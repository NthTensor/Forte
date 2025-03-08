use argh::FromArgs;

use crate::commands::CompileCheckCommand;
use crate::Flag;
use crate::Prepare;
use crate::PreparedCommand;

/// Alias for running the `compile-fail`, `bench-check`, `example-check`, `compile-check`, and `test-check` subcommands.
#[derive(FromArgs, Default)]
#[argh(subcommand, name = "compile")]
pub struct CompileCommand {}

impl Prepare for CompileCommand {
    fn prepare<'a>(&self, sh: &'a xshell::Shell, flags: Flag) -> Vec<PreparedCommand<'a>> {
        let mut commands = vec![];
        commands.append(&mut CompileCheckCommand::default().prepare(sh, flags));
        commands
    }
}
