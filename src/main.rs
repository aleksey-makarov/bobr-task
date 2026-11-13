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
    /// Continue a completed task in its existing worker session.
    Continue {
        /// Exact task ID or `current`.
        task_ref: String,
        /// UTF-8 follow-up request below the coordinator workspace.
        request_file: PathBuf,
    },
    /// List tasks owned by the current coordinator dialog.
    List,
    /// Report the task selected for the current coordinator dialog.
    Current,
    /// Select an existing non-cancelled task.
    Select { task_id: String },
    /// Report task state.
    Status {
        /// Exact task ID or `current`.
        task_ref: String,
    },
    /// Print a completed task result.
    Result {
        /// Exact task ID or `current`.
        task_ref: String,
    },
    /// Print all completed turn results in order.
    History {
        /// Exact task ID or `current`.
        task_ref: String,
    },
    /// Cancel a running task.
    Cancel {
        /// Exact task ID or `current`.
        task_ref: String,
    },
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
        Command::Continue {
            task_ref,
            request_file,
        } => bobr_task::continue_task(&task_ref, &request_file).await,
        Command::List => bobr_task::list().await,
        Command::Current => bobr_task::current().await,
        Command::Select { task_id } => bobr_task::select(&task_id).await,
        Command::Status { task_ref } => bobr_task::status(&task_ref).await,
        Command::Result { task_ref } => bobr_task::result(&task_ref),
        Command::History { task_ref } => bobr_task::history(&task_ref),
        Command::Cancel { task_ref } => bobr_task::cancel(&task_ref).await,
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
