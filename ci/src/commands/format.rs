use argh::FromArgs;
use xshell::cmd;

use crate::Flag;
use crate::Prepare;
use crate::PreparedCommand;

/// Check code formatting.
#[derive(FromArgs, Default)]
#[argh(subcommand, name = "format")]
pub struct FormatCommand {}

impl Prepare for FormatCommand {
    fn prepare<'a>(&self, sh: &'a xshell::Shell, _flags: Flag) -> Vec<PreparedCommand<'a>> {
        vec![PreparedCommand::new::<Self>(
            cmd!(sh, "cargo fmt --all -- --check"),
            "Please run 'cargo fmt --all' to format your code.",
        )]
    }
}
