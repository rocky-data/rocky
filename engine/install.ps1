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

if (-not $ResolvedVersion) {
    try {
        # Filter by tag prefix — /releases/latest may return a non-engine tag
        # in the monorepo (dagster-v*, vscode-v*, etc.). GitHub's /releases
        # endpoint does not sort strictly by published_at across tag prefixes,
        # and lexical sort places v1.10.0 before v1.9.0 — cast the stripped
        # version to [version] and sort descending so 1.10.0 wins.
        $Releases = Invoke-RestMethod -Uri "https://api.github.com/repos/$Repo/releases?per_page=30" -ErrorAction Stop
        $ResolvedVersion = ($Releases
            | Where-Object { $_.tag_name -match "^$TagPrefix\d+\.\d+\.\d+$" }
            | Sort-Object -Property @{Expression = { [version]($_.tag_name -replace "^$TagPrefix", "") }} -Descending
            | Select-Object -First 1).tag_name
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

$Archive = "rocky-x86_64-pc-windows-msvc.zip"
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

    try {
        $ChecksumsContent = Invoke-WebRequest -Uri $ChecksumsUrl -UseBasicParsing -ErrorAction Stop
        $ChecksumsText = [System.Text.Encoding]::UTF8.GetString($ChecksumsContent.Content)
        $ExpectedLine = ($ChecksumsText -split "`n") |
            Where-Object { $_ -match [regex]::Escape($Archive) } |
            Select-Object -First 1
        if ($ExpectedLine) {
            $ExpectedHash = ($ExpectedLine.Trim() -split '\s+')[0].ToLower()
            if ($ActualHash -ne $ExpectedHash) {
                Write-Error "Checksum mismatch!`n  Expected: $ExpectedHash`n  Actual:   $ActualHash"
                exit 1
            }
            Write-Host "  Checksum verified."
        } else {
            Write-Error "checksums.txt did not contain an entry for $Archive."
            exit 1
        }
    }
    catch {
        # checksums.txt is published alongside every engine release. If the
        # download fails we must not silently skip verification — an attacker
        # who can strip the file would otherwise bypass the integrity check.
        Write-Error "Failed to download checksums.txt from $ChecksumsUrl."
        Write-Host "  Refusing to install an unverified binary. Set ROCKY_SKIP_CHECKSUM=1 to override." -ForegroundColor Yellow
        if ($env:ROCKY_SKIP_CHECKSUM -ne "1") {
            exit 1
        }
        Write-Host "  (Checksum verification skipped - ROCKY_SKIP_CHECKSUM=1)"
    }

    # Extract
    Expand-Archive -Path $ZipPath -DestinationPath $TmpDir -Force

    $BinaryPath = Join-Path $TmpDir "rocky.exe"
    if (-not (Test-Path $BinaryPath)) {
        Write-Error "Archive does not contain rocky.exe"
        exit 1
    }

    # Install
    Copy-Item -Path $BinaryPath -Destination (Join-Path $InstallDir "rocky.exe") -Force
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
