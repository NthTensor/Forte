use crate::{Flag, Prepare, PreparedCommand};
use argh::FromArgs;
use xshell::cmd;

/// Runs the loom concurrency test suite.
#[derive(FromArgs, Default)]
#[argh(subcommand, name = "loom-test")]
pub struct LoomTestCommand {}

impl Prepare for LoomTestCommand {
    fn prepare<'a>(&self, sh: &'a xshell::Shell, _flags: Flag) -> Vec<PreparedCommand<'a>> {
        let command = PreparedCommand::new::<Self>(
            cmd!(sh, "cargo test --test loom --release"),
            "Please fix compiler errors in output above.",
        )
        .with_env_var("RUSTFLAGS", "--cfg loom")
        .with_env_var("LOOM_MAX_PREEMPTIONS", "3");
        vec![command]
    }
}
