use anyhow::Result;
use clap::{Parser, Subcommand};
use std::ffi::OsString;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Start a worker task from the current ZeroClaw coordinator session.
    Start {
        /// Short one- or two-word name for the task.
        label: String,
        /// UTF-8 task request below the coordinator workspace.
        request_file: PathBuf,
    },
    /// Report task state.
    Status { task_id: String },
    /// Print a completed task result.
    Result { task_id: String },
    /// Cancel a running task.
    Cancel { task_id: String },
    /// Run one long-lived command owned by the current task.
    Run {
        #[arg(required = true, trailing_var_arg = true, allow_hyphen_values = true)]
        command: Vec<OsString>,
    },
    /// Run the task controller inside its transient user unit.
    #[command(hide = true)]
    Controller { task_id: String },
    /// Replace the transient command unit process with the requested command.
    #[command(hide = true)]
    Exec {
        task_id: String,
        #[arg(required = true, trailing_var_arg = true, allow_hyphen_values = true)]
        command: Vec<OsString>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Start {
            label,
            request_file,
        } => bobr_task::start(&label, &request_file).await,
        Command::Status { task_id } => bobr_task::status(&task_id).await,
        Command::Result { task_id } => bobr_task::result(&task_id),
        Command::Cancel { task_id } => bobr_task::cancel(&task_id).await,
        Command::Run { command } => {
            let status = bobr_task::run(&command).await?;
            if status.success() {
                Ok(())
            } else {
                std::process::exit(status.code().unwrap_or(1));
            }
        }
        Command::Controller { task_id } => bobr_task::controller(&task_id).await,
        Command::Exec { task_id, command } => bobr_task::exec(&task_id, &command),
    }
}
