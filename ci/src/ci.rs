use argh::FromArgs;

use crate::{
    commands,
    prepare::{Flag, Prepare, PreparedCommand},
};

/// The CI command line tool for Forte.
#[derive(FromArgs)]
pub struct CI {
    #[argh(subcommand)]
    command: Option<Commands>,

    /// continue running commands even if one fails.
    #[argh(switch)]
    keep_going: bool,
}

impl CI {
    /// Runs the specified commands or all commands if none are specified.
    ///
    /// When run locally, results may differ from actual CI runs triggered by `.github/workflows/ci.yml`.
    /// This is usually related to differing toolchains and configuration.
    pub fn run(self) {
        let sh = xshell::Shell::new().unwrap();

        let prepared_commands = self.prepare(&sh);

        let mut failures = vec![];

        for command in prepared_commands {
            // If the CI test is to be executed in a subdirectory, we move there before running the command.
            // This will automatically move back to the original directory once dropped.
            let _subdir_hook = command.subdir.map(|path| sh.push_dir(path));

            // Execute each command, checking if it returned an error.
            if command.command.envs(command.env_vars).run().is_err() {
                let name = command.name;
                let message = command.failure_message;

                if self.keep_going {
                    // We use bullet points here because there can be more than one error.
                    failures.push(format!("- {name}: {message}"));
                } else {
                    failures.push(format!("{name}: {message}"));
                    break;
                }
            }
        }

        // Log errors at the very end.
        if !failures.is_empty() {
            let failures = failures.join("\n");

            panic!(
                "One or more CI commands failed:\n\
                {failures}"
            );
        }
    }

    fn prepare<'a>(&self, sh: &'a xshell::Shell) -> Vec<PreparedCommand<'a>> {
        let mut flags = Flag::empty();

        if self.keep_going {
            flags |= Flag::KEEP_GOING;
        }

        match &self.command {
            Some(command) => command.prepare(sh, flags),
            None => {
                // Note that we are running the subcommands directly rather than using any aliases
                let mut cmds = vec![];
                cmds.append(&mut commands::FormatCommand::default().prepare(sh, flags));
                cmds.append(&mut commands::ClippyCommand::default().prepare(sh, flags));
                cmds.append(&mut commands::LintsCommand::default().prepare(sh, flags));
                cmds.append(&mut commands::CompileCheckCommand::default().prepare(sh, flags));
                cmds.append(&mut commands::DocCheckCommand::default().prepare(sh, flags));
                cmds.append(&mut commands::DocTestCommand::default().prepare(sh, flags));
                cmds
            }
        }
    }
}

/// The subcommands that can be run by the CI script.
#[derive(FromArgs)]
#[argh(subcommand)]
enum Commands {
    // Compile commands
    Compile(commands::CompileCommand),
    CompileCheck(commands::CompileCheckCommand),
    // Documentation commands
    Doc(commands::DocCommand),
    DocCheck(commands::DocCheckCommand),
    DocTest(commands::DocTestCommand),
    // Lint commands
    Lints(commands::LintsCommand),
    Clippy(commands::ClippyCommand),
    Format(commands::FormatCommand),
    // Shuttle commands
    Shuttle(commands::ShuttleCommand),
    ShuttleCheck(commands::ShuttleCheckCommand),
    ShuttleClippy(commands::ShuttleClippyCommand),
    ShuttleTest(commands::ShuttleTestCommand),
}

impl Prepare for Commands {
    fn prepare<'a>(&self, sh: &'a xshell::Shell, flags: Flag) -> Vec<PreparedCommand<'a>> {
        match self {
            // Compile commands
            Commands::Compile(subcommand) => subcommand.prepare(sh, flags),
            Commands::CompileCheck(subcommand) => subcommand.prepare(sh, flags),
            // Documentation commands
            Commands::Doc(subcommand) => subcommand.prepare(sh, flags),
            Commands::DocCheck(subcommand) => subcommand.prepare(sh, flags),
            Commands::DocTest(subcommand) => subcommand.prepare(sh, flags),
            // Lint commands
            Commands::Lints(subcommand) => subcommand.prepare(sh, flags),
            Commands::Clippy(subcommand) => subcommand.prepare(sh, flags),
            Commands::Format(subcommand) => subcommand.prepare(sh, flags),
            // Shuttle commands
            Commands::Shuttle(subcommand) => subcommand.prepare(sh, flags),
            Commands::ShuttleCheck(subcommand) => subcommand.prepare(sh, flags),
            Commands::ShuttleClippy(subcommand) => subcommand.prepare(sh, flags),
            Commands::ShuttleTest(subcommand) => subcommand.prepare(sh, flags),
        }
    }
}
