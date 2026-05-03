//! Generate shell completion scripts.

use std::io;

use clap::CommandFactory;
use clap_complete::Shell;

/// Write the completion script for `shell` to `out`, sourced from `C`'s clap derive.
///
/// The script is generated from the live `clap::Command` graph, so it stays in
/// sync with the binary's subcommands and flags automatically — no manual
/// completion table to maintain. Intended for users to install via shell-
/// specific paths, e.g.:
///
/// ```text
/// rocky completions zsh  > ~/.zsh/completions/_rocky
/// rocky completions bash > /etc/bash_completion.d/rocky
/// ```
///
/// Write errors (e.g. closed pipe when piping into `head`) are silently
/// dropped, matching `clap_complete::generate`'s contract.
pub fn run_completions<C: CommandFactory>(shell: Shell, out: &mut impl io::Write) {
    let mut cmd = C::command();
    let bin_name = cmd.get_name().to_string();
    clap_complete::generate(shell, &mut cmd, bin_name, out);
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    #[command(name = "test_rocky")]
    struct TestCli {}

    #[test]
    fn writes_non_empty_for_each_shell() {
        for shell in [
            Shell::Bash,
            Shell::Zsh,
            Shell::Fish,
            Shell::Elvish,
            Shell::PowerShell,
        ] {
            let mut buf = Vec::<u8>::new();
            run_completions::<TestCli>(shell, &mut buf);
            assert!(!buf.is_empty(), "{shell:?} produced empty output");
        }
    }
}
