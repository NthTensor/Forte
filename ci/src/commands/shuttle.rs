use argh::FromArgs;

use crate::Flag;
use crate::Prepare;
use crate::PreparedCommand;
use crate::commands::ShuttleCheckCommand;
use crate::commands::ShuttleClippyCommand;
use crate::commands::ShuttleTestCommand;

/// Alias for running the `loom-check`, `loom-clippy` and `loom-test` subcommands.
#[derive(FromArgs, Default)]
#[argh(subcommand, name = "loom")]
pub struct ShuttleCommand {}

impl Prepare for ShuttleCommand {
    fn prepare<'a>(&self, sh: &'a xshell::Shell, flags: Flag) -> Vec<PreparedCommand<'a>> {
        let mut commands = vec![];
        commands.append(&mut ShuttleCheckCommand::default().prepare(sh, flags));
        commands.append(&mut ShuttleClippyCommand::default().prepare(sh, flags));
        commands.append(&mut ShuttleTestCommand::default().prepare(sh, flags));
        commands
    }
}
