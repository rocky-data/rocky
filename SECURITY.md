# Security Policy

## Reporting a Vulnerability

We take the security of Rocky and its integrations seriously. If you discover a security vulnerability in any subproject of this repository (`engine/`, `integrations/dagster/`, `editors/vscode/`, `examples/playground/`), please report it through GitHub's private vulnerability reporting:

1. Go to the [Security tab](https://github.com/rocky-data/rocky/security/advisories/new) of this repository
2. Click "Report a vulnerability"
3. Fill in the details of the vulnerability, including which subproject is affected

Alternatively, you can email **security@rocky-data.dev** directly.

**Please do not report security vulnerabilities through public GitHub issues.**

## Response Timeline

- **Acknowledgment:** Within 48 hours
- **Initial assessment:** Within 1 week
- **Fix timeline:** Depends on severity, typically within 90 days

## Supported Versions

Each Rocky artifact (engine, dagster integration, VS Code extension) is versioned and released independently. Only the latest release of each is supported.

| Artifact | Tag Prefix | Supported |
|----------|------------|-----------|
| Rocky CLI engine | `engine-v*` | Latest only |
| dagster-rocky package | `dagster-v*` | Latest only |
| Rocky VS Code extension | `vscode-v*` | Latest only |

## Scope

This policy covers all code in the `rocky-data/rocky` monorepo. Out of scope: third-party dependencies (report directly to the upstream project), credentials accidentally committed by users to their own forks, and configuration mistakes in user projects that consume Rocky.
