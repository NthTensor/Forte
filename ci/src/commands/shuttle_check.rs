use argh::FromArgs;
use xshell::cmd;

use crate::{Flag, Prepare, PreparedCommand};

/// Checks that the loom test suite compiles.
#[derive(FromArgs, Default)]
#[argh(subcommand, name = "loom-check")]
pub struct ShuttleCheckCommand {}

impl Prepare for ShuttleCheckCommand {
    fn prepare<'a>(&self, sh: &'a xshell::Shell, _flags: Flag) -> Vec<PreparedCommand<'a>> {
        let command = PreparedCommand::new::<Self>(
            cmd!(sh, "cargo check --test shuttle --features shuttle"),
            "Please fix compiler errors in output above.",
        );
        vec![command]
    }
}
