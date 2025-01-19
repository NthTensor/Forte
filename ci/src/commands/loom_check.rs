use crate::{Flag, Prepare, PreparedCommand};
use argh::FromArgs;
use xshell::cmd;

/// Checks that the loom test suite compiles.
#[derive(FromArgs, Default)]
#[argh(subcommand, name = "loom-check")]
pub struct LoomCheckCommand {}

impl Prepare for LoomCheckCommand {
    fn prepare<'a>(&self, sh: &'a xshell::Shell, _flags: Flag) -> Vec<PreparedCommand<'a>> {
        let command = PreparedCommand::new::<Self>(
            cmd!(sh, "cargo check --test loom"),
            "Please fix compiler errors in output above.",
        )
        .with_env_var("RUSTFLAGS", "--cfg loom");
        vec![command]
    }
}
