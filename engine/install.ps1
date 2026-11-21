# Install Rocky — SQL transformation engine
# Usage:
#   irm https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.ps1 | iex
#
# To install a specific version, set the env var before running:
#   $env:ROCKY_VERSION = "engine-v1.0.0"
#   irm https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.ps1 | iex
#
# To install to a custom directory:
#   $env:ROCKY_INSTALL_DIR = "C:\tools"
#   irm https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.ps1 | iex

param(
    [string]$Version = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$ProgressPreference = 'SilentlyContinue'

# Install a binary with rename-then-replace. On Windows a running executable is
# locked, so overwriting it in place fails when a process is running it (most
# often the VS Code Rocky extension). Renaming an in-use binary IS allowed — the
# running process keeps its handle to the renamed file until it restarts — so try
# a normal copy first and, on a locked overwrite, move the old binary aside and
# drop the new one into place. Mirrors what install.sh does with `mv`.
function Install-RockyBinary {
    param(
        [Parameter(Mandatory)] [string]$Source,
        [Parameter(Mandatory)] [string]$Dest
    )
    $Name = [System.IO.Path]::GetFileName($Dest)
    $Dir = Split-Path -Path $Dest -Parent
    # Sweep leftover .old binaries from previous in-use updates (now unlocked).
    Get-ChildItem -Path $Dir -Filter "$Name.old*" -ErrorAction SilentlyContinue |
        Remove-Item -Force -ErrorAction SilentlyContinue
    try {
        Copy-Item -Path $Source -Destination $Dest -Force -ErrorAction Stop
    }
    catch {
        if (-not (Test-Path $Dest)) { throw }
        # A unique name avoids colliding with a still-locked .old from an earlier
        # in-use update in the same session.
        $Stale = "$Dest.old-$(Get-Random)"
        Move-Item -Path $Dest -Destination $Stale -Force
        Copy-Item -Path $Source -Destination $Dest -Force
        Write-Host "  Replaced an in-use $Name. Restart VS Code (or the Rocky extension) to use the new version." -ForegroundColor Yellow
    }
}

# Check for 32-bit Windows
if (-not [Environment]::Is64BitProcess) {
    Write-Error "Rocky does not support 32-bit Windows."
    exit 1
}

$Repo = "rocky-data/rocky"
# Tag prefix used by the engine release workflow in the rocky monorepo.
# Allows the engine to release independently of dagster-rocky / rocky-vscode.
$TagPrefix = "engine-v"
$InstallDir = if ($env:ROCKY_INSTALL_DIR) { $env:ROCKY_INSTALL_DIR } else { "$env:LOCALAPPDATA\rocky\bin" }

# Resolve the version to install.
# Precedence: $env:ROCKY_VERSION > $Version param > latest from API.
$ResolvedVersion = if ($env:ROCKY_VERSION) {
    $env:ROCKY_VERSION
} elseif ($Version) {
    if ($Version -like "engine-v*") { $Version } else { "$TagPrefix$Version" }
} else {
    ""
}

$Archive = "rocky-x86_64-pc-windows-msvc.zip"

# Return the HTTP status of a HEAD request to $Uri, after redirects, or 0 when
# no response arrives.
function Get-HttpStatus {
    param([Parameter(Mandatory)] [string]$Uri)
    try {
        $Response = Invoke-WebRequest -Uri $Uri -Method Head -UseBasicParsing -ErrorAction Stop
        return [int]$Response.StatusCode
    }
    catch {
        $Exception = $_.Exception
        if ($Exception.PSObject.Properties['Response'] -and $Exception.Response) {
            return [int]$Exception.Response.StatusCode
        }
        return 0
    }
}

# Resolve the newest engine release from the git tags, for when the API call
# fails. Mirrors resolve_version_from_tags in install.sh: a tag can exist
# without release assets (a draft release, or a deleted one), so take the
# newest of the five highest engine tags whose Windows archive answers 200.
# Only a 404 skips a tag; any other status gives up, so a transient error never
# installs an older engine. Returns the tag, or $null after printing why.
# Messages use [Console]::Error because Write-Error stops the script here.
function Resolve-VersionFromTags {
    if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
        [Console]::Error.WriteLine("Error: the GitHub API call failed, and git is not installed to read the tags instead.")
        return $null
    }
    # Run git outside any repository and with no user or system config, so it
    # cannot pick up a checkout's auth header, a credential helper or a URL
    # rewrite. Its exit code is read from $LASTEXITCODE: redirecting a native
    # command's stderr under ErrorActionPreference Stop can throw on Windows
    # PowerShell 5.1.
    $GitEnv = @{ GIT_CONFIG_GLOBAL = '/dev/null'; GIT_CONFIG_NOSYSTEM = '1'; GIT_TERMINAL_PROMPT = '0' }
    $Saved = @{}
    foreach ($Name in $GitEnv.Keys) {
        $Saved[$Name] = [Environment]::GetEnvironmentVariable($Name)
        [Environment]::SetEnvironmentVariable($Name, $GitEnv[$Name])
    }
    try {
        $Root = [System.IO.Path]::GetPathRoot([System.IO.Path]::GetTempPath())
        $Refs = @(& git -C $Root ls-remote --tags --refs "https://github.com/$Repo.git" "$TagPrefix*")
        $GitExit = $LASTEXITCODE
    }
    finally {
        foreach ($Name in $Saved.Keys) {
            [Environment]::SetEnvironmentVariable($Name, $Saved[$Name])
        }
    }
    if ($GitExit -ne 0) {
        [Console]::Error.WriteLine("Error: git ls-remote against https://github.com/$Repo.git failed.")
        return $null
    }
    $Candidates = @($Refs |
        ForEach-Object { ($_ -split "`t")[-1] -replace '^refs/tags/', '' } |
        Where-Object { $_ -match "^$TagPrefix\d+\.\d+\.\d+$" } |
        Sort-Object -Property @{Expression = { [version]($_ -replace "^$TagPrefix", "") }} -Descending |
        Select-Object -First 5)
    foreach ($Candidate in $Candidates) {
        $Status = Get-HttpStatus -Uri "https://github.com/$Repo/releases/download/$Candidate/$Archive"
        if ($Status -eq 200) {
            return $Candidate
        }
        if ($Status -ne 404) {
            [Console]::Error.WriteLine("Error: checking $Candidate returned HTTP $Status; refusing to fall back to an older engine.")
            return $null
        }
        Write-Host "  Skipping ${Candidate}: no release assets (HTTP 404)."
    }
    [Console]::Error.WriteLine("Error: no engine release with assets among the five newest $TagPrefix* tags.")
    return $null
}

if (-not $ResolvedVersion) {
    # The API call sends no token. Without one, GitHub allows 60 API requests
    # per hour per IP address, and shared CI runners can use that up, so the
    # call fails with a 403 (#2684). When the call fails, and only then, read
    # the tags over git instead, which needs no token either. Only the call is
    # in this try: an empty answer is an error, not a reason to fall back.
    $Releases = $null
    $ApiFailed = $false
    try {
        $Releases = Invoke-RestMethod -Uri "https://api.github.com/repos/$Repo/releases?per_page=30" -ErrorAction Stop
    }
    catch {
        $ApiFailed = $true
        Write-Host "The GitHub API call failed ($_). Reading the engine tags with git ls-remote instead." -ForegroundColor Yellow
    }
    if ($ApiFailed) {
        $ResolvedVersion = Resolve-VersionFromTags
        if (-not $ResolvedVersion) {
            Write-Error "Failed to get latest version from the git tags. Set ROCKY_VERSION and retry."
            exit 1
        }
    }
    else {
        try {
            # Filter by tag prefix — /releases/latest may return a non-engine tag
            # in the monorepo (dagster-v*, vscode-v*, etc.). GitHub's /releases
            # endpoint does not sort strictly by published_at across tag prefixes,
            # and lexical sort places v1.10.0 before v1.9.0 — cast the stripped
            # version to [version] and sort descending so 1.10.0 wins.
            $ResolvedVersion = ($Releases |
                Where-Object { $_.tag_name -match "^$TagPrefix\d+\.\d+\.\d+$" } |
                Sort-Object -Property @{Expression = { [version]($_.tag_name -replace "^$TagPrefix", "") }} -Descending |
                Select-Object -First 1).tag_name
            if (-not $ResolvedVersion) {
                Write-Error "Failed to find an engine release (tag prefix '$TagPrefix'). Set ROCKY_VERSION manually."
                exit 1
            }
        }
        catch {
            Write-Error "Failed to get latest version: $_"
            exit 1
        }
    }
}

$Url = "https://github.com/$Repo/releases/download/$ResolvedVersion/$Archive"
$ChecksumsUrl = "https://github.com/$Repo/releases/download/$ResolvedVersion/checksums.txt"

Write-Host "Installing Rocky $ResolvedVersion (windows/amd64)..."
Write-Host "  From: $Url"
Write-Host "  To:   $InstallDir\rocky.exe"

# Create install directory
New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null

# Download to a temp directory; always clean up with try/finally
$TmpDir = Join-Path ([System.IO.Path]::GetTempPath()) "rocky-install-$([System.Guid]::NewGuid().ToString('N').Substring(0,8))"
New-Item -ItemType Directory -Path $TmpDir -Force | Out-Null
$ZipPath = Join-Path $TmpDir $Archive

try {
    # Download the archive
    try {
        Invoke-WebRequest -Uri $Url -OutFile $ZipPath -UseBasicParsing -ErrorAction Stop
    }
    catch {
        Write-Error "Download failed: check that version $ResolvedVersion exists.`n  $Url"
        exit 1
    }

    # Compute SHA256 and verify against checksums.txt if available
    $ActualHash = (Get-FileHash -Path $ZipPath -Algorithm SHA256).Hash.ToLower()
    Write-Host "  SHA256: $ActualHash"

    # checksums.txt is published alongside every engine release. If the
    # download fails we must not silently skip verification — an attacker
    # who can strip the file would otherwise downgrade us to no integrity
    # check. Only the download leg is bypassable via ROCKY_SKIP_CHECKSUM;
    # a missing entry or hash mismatch always aborts. Mirrors install.sh.
    #
    # The download attempt is the *only* call wrapped in try/catch. Under
    # `$ErrorActionPreference = "Stop"` (set above), `Write-Error` becomes a
    # terminating error — wrapping the verification logic in the same try
    # would route mismatches/missing-entries through the catch and let
    # ROCKY_SKIP_CHECKSUM=1 silently bypass them.
    $ChecksumsResponse = $null
    try {
        $ChecksumsResponse = Invoke-WebRequest -Uri $ChecksumsUrl -UseBasicParsing -ErrorAction Stop
    }
    catch {
        Write-Host "Error: Failed to download checksums.txt from $ChecksumsUrl." -ForegroundColor Red
        Write-Host "  Refusing to install an unverified binary. Retry, or set" -ForegroundColor Yellow
        Write-Host "  ROCKY_SKIP_CHECKSUM=1 if you intentionally want to skip." -ForegroundColor Yellow
        if ($env:ROCKY_SKIP_CHECKSUM -ne "1") {
            exit 1
        }
        Write-Host "  (Checksum verification skipped - ROCKY_SKIP_CHECKSUM=1)"
    }

    if ($null -ne $ChecksumsResponse) {
        $ChecksumsText = [System.Text.Encoding]::UTF8.GetString($ChecksumsResponse.Content)
        $ExpectedLine = ($ChecksumsText -split "`n") |
            Where-Object { $_ -match [regex]::Escape($Archive) } |
            Select-Object -First 1
        if (-not $ExpectedLine) {
            Write-Host "Error: checksums.txt did not contain an entry for $Archive." -ForegroundColor Red
            exit 1
        }
        $ExpectedHash = ($ExpectedLine.Trim() -split '\s+')[0].ToLower()
        if ($ActualHash -ne $ExpectedHash) {
            Write-Host "Error: Checksum mismatch!" -ForegroundColor Red
            Write-Host "  Expected: $ExpectedHash" -ForegroundColor Red
            Write-Host "  Actual:   $ActualHash" -ForegroundColor Red
            exit 1
        }
        Write-Host "  Checksum verified."
    }

    # Extract
    Expand-Archive -Path $ZipPath -DestinationPath $TmpDir -Force

    $BinaryPath = Join-Path $TmpDir "rocky.exe"
    if (-not (Test-Path $BinaryPath)) {
        Write-Error "Archive does not contain rocky.exe"
        exit 1
    }

    # Install rocky.exe (rename-then-replace handles a locked, in-use binary).
    Install-RockyBinary -Source $BinaryPath -Dest (Join-Path $InstallDir "rocky.exe")

    # Best-effort: also install the standalone rocky-lsp binary. The VS Code
    # extension prefers a sibling rocky-lsp for the language server; when it's
    # present the extension never runs rocky.exe as the LSP, so rocky.exe is never
    # locked in the first place. Optional — if the archive is missing (older
    # release) or fails verification we skip it and the extension falls back to
    # `rocky lsp`, which the rename-then-replace install above already handles.
    $LspArchive = "rocky-lsp-x86_64-pc-windows-msvc.zip"
    $LspZip = Join-Path $TmpDir $LspArchive
    try {
        Invoke-WebRequest -Uri "https://github.com/$Repo/releases/download/$ResolvedVersion/$LspArchive" -OutFile $LspZip -UseBasicParsing -ErrorAction Stop
        if ($null -ne $ChecksumsResponse) {
            $LspHash = (Get-FileHash -Path $LspZip -Algorithm SHA256).Hash.ToLower()
            $LspLine = ($ChecksumsText -split "`n") |
                Where-Object { $_ -match [regex]::Escape($LspArchive) } |
                Select-Object -First 1
            $LspExpected = if ($LspLine) { ($LspLine.Trim() -split '\s+')[0].ToLower() } else { "" }
            if ($LspExpected -ne $LspHash) {
                throw "rocky-lsp checksum missing or mismatched; skipping."
            }
        }
        Expand-Archive -Path $LspZip -DestinationPath $TmpDir -Force
        $LspBin = Join-Path $TmpDir "rocky-lsp.exe"
        if (Test-Path $LspBin) {
            Install-RockyBinary -Source $LspBin -Dest (Join-Path $InstallDir "rocky-lsp.exe")
            Write-Host "  Installed rocky-lsp (the VS Code extension prefers it for the language server)."
        }
    }
    catch {
        Write-Host "  (Optional rocky-lsp not installed; the VS Code extension will use 'rocky lsp'.)"
    }
}
finally {
    Remove-Item -Path $TmpDir -Recurse -Force -ErrorAction SilentlyContinue
}

# Add to PATH if not already there
$CurrentPath = [Environment]::GetEnvironmentVariable("Path", "User")
if ($CurrentPath -notlike "*$InstallDir*") {
    [Environment]::SetEnvironmentVariable("Path", "$CurrentPath;$InstallDir", "User")
    $env:Path = "$env:Path;$InstallDir"
    Write-Host ""
    Write-Host "Added $InstallDir to user PATH."
}

# Verify installation
Write-Host ""
try {
    $VersionStr = & "$InstallDir\rocky.exe" --version 2>&1
    Write-Host "$([char]0x2713) $VersionStr installed to $InstallDir\rocky.exe"
}
catch {
    Write-Host "$([char]0x2713) Rocky installed to $InstallDir\rocky.exe"
}
