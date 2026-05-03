//! Generate shell completion scripts.

use std::io;

use clap::CommandFactory;
use clap_complete::Shell;

/// Write the completion script for `shell` to stdout.
pub fn run_completions<C: CommandFactory>(shell: Shell) {
    let mut cmd = C::command();
    let bin_name = cmd.get_name().to_string();
    clap_complete::generate(shell, &mut cmd, bin_name, &mut io::stdout());
}
