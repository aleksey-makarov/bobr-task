use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Start a worker task from the current ZeroClaw coordinator session.
    Start,
    /// Report task state.
    Status { task_id: String },
    /// Print a completed task result.
    Result { task_id: String },
    /// Cancel a running task.
    Cancel { task_id: String },
    /// Run the task controller inside its transient user unit.
    #[command(hide = true)]
    Controller { task_id: String },
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Start => bobr_task::start().await,
        Command::Status { task_id } => bobr_task::status(&task_id).await,
        Command::Result { task_id } => bobr_task::result(&task_id),
        Command::Cancel { task_id } => bobr_task::cancel(&task_id).await,
        Command::Controller { task_id } => bobr_task::controller(&task_id).await,
    }
}
