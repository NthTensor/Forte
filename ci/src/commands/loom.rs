use crate::{
    commands::{LoomCheckCommand, LoomClippyCommand, LoomTestCommand},
    Flag, Prepare, PreparedCommand,
};
use argh::FromArgs;

/// Alias for running the `loom-check`, `loom-clippy` and `loom-test` subcommands.
#[derive(FromArgs, Default)]
#[argh(subcommand, name = "loom")]
pub struct LoomCommand {}

impl Prepare for LoomCommand {
    fn prepare<'a>(&self, sh: &'a xshell::Shell, flags: Flag) -> Vec<PreparedCommand<'a>> {
        let mut commands = vec![];
        commands.append(&mut LoomCheckCommand::default().prepare(sh, flags));
        commands.append(&mut LoomClippyCommand::default().prepare(sh, flags));
        commands.append(&mut LoomTestCommand::default().prepare(sh, flags));
        commands
    }
}
