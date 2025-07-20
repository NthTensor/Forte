use argh::FromArgs;
use xshell::cmd;

use crate::Flag;
use crate::Prepare;
use crate::PreparedCommand;

/// Check for clippy warnings and errors.
#[derive(FromArgs, Default)]
#[argh(subcommand, name = "clippy")]
pub struct ClippyCommand {}

impl Prepare for ClippyCommand {
    fn prepare<'a>(&self, sh: &'a xshell::Shell, _flags: Flag) -> Vec<PreparedCommand<'a>> {
        vec![PreparedCommand::new::<Self>(
            cmd!(sh, "cargo clippy --workspace -- -Dwarnings"),
            "Please fix clippy errors in output above.",
        )]
    }
}
