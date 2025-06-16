use argh::FromArgs;
use xshell::cmd;

use crate::{Flag, Prepare, PreparedCommand};

/// Checks for clippy warnings and errors in the loom test suite.
#[derive(FromArgs, Default)]
#[argh(subcommand, name = "loom-clippy")]
pub struct ShuttleClippyCommand {}

impl Prepare for ShuttleClippyCommand {
    fn prepare<'a>(&self, sh: &'a xshell::Shell, _flags: Flag) -> Vec<PreparedCommand<'a>> {
        let command = PreparedCommand::new::<Self>(
            cmd!(
                sh,
                "cargo clippy --test shuttle --features shuttle -- -Dwarnings"
            ),
            "Please fix clippy errors in output above.",
        );
        vec![command]
    }
}
