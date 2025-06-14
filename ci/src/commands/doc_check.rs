use argh::FromArgs;
use xshell::cmd;

use crate::Flag;
use crate::Prepare;
use crate::PreparedCommand;

/// Checks that all docs compile.
#[derive(FromArgs, Default)]
#[argh(subcommand, name = "doc-check")]
pub struct DocCheckCommand {}

impl Prepare for DocCheckCommand {
    fn prepare<'a>(&self, sh: &'a xshell::Shell, _flags: Flag) -> Vec<PreparedCommand<'a>> {
        vec![PreparedCommand::new::<Self>(
            cmd!(
                sh,
                "cargo doc --workspace --all-features --no-deps --document-private-items --keep-going"
            ),
            "Please fix doc warnings in output above.",
        )
        .with_env_var("RUSTDOCFLAGS", "-D warnings")]
    }
}
