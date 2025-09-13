use argh::FromArgs;
use xshell::cmd;

use crate::Flag;
use crate::Prepare;
use crate::PreparedCommand;

/// Runs the loom concurrency test suite.
#[derive(FromArgs, Default)]
#[argh(subcommand, name = "shuttle-test")]
pub struct ShuttleTestCommand {}

impl Prepare for ShuttleTestCommand {
    fn prepare<'a>(&self, sh: &'a xshell::Shell, _flags: Flag) -> Vec<PreparedCommand<'a>> {
        let command = PreparedCommand::new::<Self>(
            cmd!(
                sh,
                "cargo test --test shuttle --profile shuttle --features shuttle"
            ),
            "Please fix compiler errors in output above.",
        );
        vec![command]
    }
}
