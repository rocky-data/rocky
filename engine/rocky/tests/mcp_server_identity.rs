//! `rocky mcp` must announce the same version as `rocky --version`.
//!
//! The MCP `initialize` result carries `serverInfo`, and a client that lists
//! its servers shows that name and version to an operator. Until #1973 it was
//! the MCP library's own identity, because rmcp's
//! `Implementation::from_build_env()` expands `env!` inside rmcp. It now
//! carries `rocky-mcp`'s crate version.
//!
//! That is only the engine's version for as long as a release remembers to
//! bump `rocky-mcp` with everything else. The workspace does not share one
//! version: each `Cargo.toml` carries the number as a literal, a release edits
//! about twenty-five of them by hand, and the documented check greps for the
//! OLD version — which cannot see a crate that already fell behind. Three
//! crates have drifted off the shared version that way.
//!
//! This test is the guard that grep cannot be. It lives in the binary crate
//! because that crate's version IS what `rocky --version` prints, so the two
//! sides of the claim are compared directly rather than described.

/// `rocky mcp`'s announced version is the binary's version.
///
/// If this fails after a release bump, the fix is to bump `rocky-mcp`'s
/// `Cargo.toml` to match, not to relax the assertion: a client reading
/// `serverInfo.version` is asking which Rocky it is talking to.
#[test]
fn the_mcp_server_announces_the_binary_version() {
    assert_eq!(
        rocky_mcp::SERVER_VERSION,
        env!("CARGO_PKG_VERSION"),
        "`rocky mcp` announces {} while `rocky --version` prints {}: bump \
         engine/crates/rocky-mcp/Cargo.toml to match the release",
        rocky_mcp::SERVER_VERSION,
        env!("CARGO_PKG_VERSION"),
    );
}

/// And it announces Rocky, not the library it is built on.
#[test]
fn the_mcp_server_announces_rocky() {
    assert_eq!(rocky_mcp::SERVER_NAME, "rocky");
}
