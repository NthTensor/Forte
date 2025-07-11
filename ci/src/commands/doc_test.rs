use argh::FromArgs;
use xshell::cmd;

use crate::Flag;
use crate::Prepare;
use crate::PreparedCommand;

/// Runs all doc tests.
#[derive(FromArgs, Default)]
#[argh(subcommand, name = "doc-test")]
pub struct DocTestCommand {}

impl Prepare for DocTestCommand {
    fn prepare<'a>(&self, sh: &'a xshell::Shell, flags: Flag) -> Vec<PreparedCommand<'a>> {
        let no_fail_fast = if flags.contains(Flag::KEEP_GOING) {
            "--no-fail-fast"
        } else {
            ""
        };

        vec![PreparedCommand::new::<Self>(
            cmd!(sh, "cargo test --workspace --doc {no_fail_fast}"),
            "Please fix failing doc tests in output above.",
        )]
    }
}
