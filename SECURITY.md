# Security Policy

## Reporting a Vulnerability

**Do not report a security vulnerability through a public GitHub issue.** Use either private channel below. Both reach the maintainers, and either one on its own is enough.

- **The private advisory form.** Open the [private advisory form](https://github.com/rocky-data/rocky/security/advisories/new). The link goes straight to GitHub's "Report a vulnerability" page for this repository.
- **Email.** Write to **security@rocky-data.dev**.

Include these details in whichever channel you choose.

1. Name the affected path, for example `engine/` or `editors/vscode/`. Every path in this repository is in scope, `.github/` and `scripts/` included. Give the closest path you know, and say so if you are unsure.
2. Describe the vulnerability. Include the version you tested, your platform, and the steps to reproduce it.

Send a partial report rather than none. We will ask for anything else we need.

## Response Timeline

| Stage | When |
|---|---|
| We acknowledge your report | Within 48 hours |
| We send an initial assessment | Within 1 week |
| We ship a fix | Depends on severity, typically within 90 days |

## Supported Versions

Each Rocky artifact is versioned and released independently. Only the latest release of each one receives security fixes.

| Artifact | Tag Prefix | Supported |
|----------|------------|-----------|
| Rocky CLI engine | `engine-v*` | Latest only |
| rocky-sdk package | `sdk-v*` | Latest only |
| dagster-rocky package | `dagster-v*` | Latest only |
| Rocky VS Code extension | `vscode-v*` | Latest only |

`@rocky-data/compiler`, the WebAssembly build of the compiler pipeline, has never been published. There is no released version of it to support.

## Scope

This policy covers all code in the `rocky-data/rocky` monorepo. Three things fall outside it:

- Third-party dependencies. Report those to the upstream project.
- Credentials a user commits to their own fork.
- Configuration mistakes in a project that consumes Rocky.
