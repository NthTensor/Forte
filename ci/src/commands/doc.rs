use argh::FromArgs;

use crate::Flag;
use crate::Prepare;
use crate::PreparedCommand;
use crate::commands::DocCheckCommand;
use crate::commands::DocTestCommand;

/// Alias for running the `doc-test` and `doc-check` subcommands.
#[derive(FromArgs, Default)]
#[argh(subcommand, name = "doc")]
pub struct DocCommand {}

impl Prepare for DocCommand {
    fn prepare<'a>(&self, sh: &'a xshell::Shell, flags: Flag) -> Vec<PreparedCommand<'a>> {
        let mut commands = vec![];
        commands.append(&mut DocTestCommand::default().prepare(sh, flags));
        commands.append(&mut DocCheckCommand::default().prepare(sh, flags));
        commands
    }
}
