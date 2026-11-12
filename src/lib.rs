//! Core implementation for the `bobr-task` command.

mod config;
mod rpc;
mod systemd;
mod task;

use anyhow::{Context, Result, anyhow, bail};
use config::{Config, worker_base_path};
use rpc::{RpcClient, TurnComplete};
use serde::Serialize;
use std::collections::HashMap;
use std::env;
use std::ffi::OsString;
use std::fs;
use std::os::unix::process::CommandExt;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, ExitStatus};
use systemd::Systemd;
use task::{TaskResult, TaskStore, command_unit, runtime_name};

/// Environment variable installed by ZeroClaw's shell tool for the active
/// coordinator session.
pub const ZEROCLAW_SESSION_ID: &str = "ZEROCLAW_SESSION_ID";
/// Task selected by the launcher for a worker session.
pub const BOBR_TASK_ID: &str = "BOBR_TASK_ID";

#[derive(Debug, Serialize)]
struct StartOutput<'a> {
    task_id: &'a str,
    state: &'static str,
    turn: u32,
}

#[derive(Debug, Serialize)]
struct StatusOutput<'a> {
    task_id: &'a str,
    state: String,
    controller_unit: &'a str,
    worker_session_id: Option<&'a str>,
    turn: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    worker_state: Option<String>,
}

#[derive(Debug, Serialize)]
struct CancelOutput<'a> {
    task_id: &'a str,
    state: &'static str,
    turn: u32,
}

#[derive(Debug, Serialize)]
struct HistoryOutput<'a> {
    task_id: &'a str,
    current_turn: u32,
    results: Vec<TaskResult>,
}

/// Start a task from the current ZeroClaw coordinator session.
pub async fn start(label: &str, request_file: &Path) -> Result<()> {
    let config = Config::from_env()?;
    let coordinator_session_id = env::var(ZEROCLAW_SESSION_ID)
        .context("ZEROCLAW_SESSION_ID is not set; run bobr-task from a ZeroClaw session")?;
    let coordinator_workspace = env::current_dir().context("resolve coordinator workspace")?;
    let request = read_request_file(&coordinator_workspace, request_file)?;
    let worker_base_path = worker_base_path()?;

    let store = TaskStore::new(&config.task_root)?;
    let record = store.create(label, &coordinator_session_id, &request)?;
    let systemd = Systemd::new(&config);

    if let Err(error) = systemd
        .start_controller(
            &record,
            &env::current_exe().context("locate bobr-task executable")?,
            &worker_base_path,
        )
        .await
    {
        let result = TaskResult::failed(&record, format!("failed to start controller: {error:#}"));
        store.write_result(&result)?;
        return Err(error);
    }

    print_json(&StartOutput {
        task_id: &record.id,
        state: "started",
        turn: record.current_turn,
    })
}

/// Continue a completed task in its existing worker session and workspace.
pub async fn continue_task(task_id: &str, request_file: &Path) -> Result<()> {
    let config = Config::from_env()?;
    let coordinator_session_id = env::var(ZEROCLAW_SESSION_ID)
        .context("ZEROCLAW_SESSION_ID is not set; run bobr-task from a ZeroClaw session")?;
    let coordinator_workspace = env::current_dir().context("resolve coordinator workspace")?;
    let request = read_request_file(&coordinator_workspace, request_file)?;
    let worker_base_path = worker_base_path()?;
    let store = TaskStore::new(&config.task_root)?;
    let _lock = store.lock_task(task_id)?;
    let mut record = store.read_record(task_id)?;

    if record.coordinator_session_id != coordinator_session_id {
        bail!("task '{task_id}' belongs to another coordinator session");
    }
    let previous = store.read_result(&record)?.ok_or_else(|| {
        anyhow!(
            "task '{task_id}' turn {} is still active",
            record.current_turn
        )
    })?;
    if previous.outcome == "cancelled" {
        bail!("cancelled task '{task_id}' cannot be continued");
    }
    let worker_session_id = record
        .worker_session_id
        .as_deref()
        .ok_or_else(|| anyhow!("task '{task_id}' has no worker session"))?;
    let systemd = Systemd::new(&config);
    if systemd.unit_is_active(&record.controller_unit).await?
        || systemd.unit_is_active(&command_unit(task_id)).await?
    {
        bail!("task '{task_id}' still has an active unit");
    }
    let mut rpc = RpcClient::connect(&config.rpc_socket, HashMap::new()).await?;
    let worker_state = rpc
        .session_state(worker_session_id)
        .await
        .with_context(|| format!("worker session '{worker_session_id}' is unavailable"))?;
    if worker_state != "idle" {
        bail!("worker session '{worker_session_id}' is {worker_state}, not idle");
    }

    record.current_turn = record
        .current_turn
        .checked_add(1)
        .context("task turn counter overflow")?;
    record.request = request;
    store.write_record(&record)?;

    if let Err(error) = systemd
        .start_controller(
            &record,
            &env::current_exe().context("locate bobr-task executable")?,
            &worker_base_path,
        )
        .await
    {
        let result = TaskResult::failed(&record, format!("failed to start controller: {error:#}"));
        store.write_result(&result)?;
        return Err(error);
    }

    print_json(&StartOutput {
        task_id: &record.id,
        state: "started",
        turn: record.current_turn,
    })
}

/// Report task state using its result, user unit, and worker session.
pub async fn status(task_id: &str) -> Result<()> {
    let config = Config::from_env()?;
    let store = TaskStore::new(&config.task_root)?;
    let record = store.read_record(task_id)?;

    if let Some(result) = store.read_result(&record)? {
        return print_json(&StatusOutput {
            task_id,
            state: result.outcome,
            controller_unit: &record.controller_unit,
            worker_session_id: record.worker_session_id.as_deref(),
            turn: record.current_turn,
            worker_state: None,
        });
    }

    let systemd = Systemd::new(&config);
    let unit_state = systemd.unit_state(&record.controller_unit).await?;
    let worker_state = if let Some(session_id) = record.worker_session_id.as_deref() {
        match RpcClient::connect(&config.rpc_socket, HashMap::new()).await {
            Ok(mut rpc) => rpc.session_state(session_id).await.ok(),
            Err(_) => None,
        }
    } else {
        None
    };

    print_json(&StatusOutput {
        task_id,
        state: unit_state,
        controller_unit: &record.controller_unit,
        worker_session_id: record.worker_session_id.as_deref(),
        turn: record.current_turn,
        worker_state,
    })
}

/// Print the terminal worker result.
pub fn result(task_id: &str) -> Result<()> {
    let config = Config::from_env()?;
    let store = TaskStore::new(&config.task_root)?;
    let record = store.read_record(task_id)?;
    let result = store.read_result(&record)?.ok_or_else(|| {
        anyhow!(
            "task '{task_id}' turn {} is still active",
            record.current_turn
        )
    })?;
    print_json(&result)
}

/// Print all completed turn results in order.
pub fn history(task_id: &str) -> Result<()> {
    let config = Config::from_env()?;
    let store = TaskStore::new(&config.task_root)?;
    let record = store.read_record(task_id)?;
    print_json(&HistoryOutput {
        task_id,
        current_turn: record.current_turn,
        results: store.read_results(&record)?,
    })
}

/// Cancel the active command, worker turn, and controller, in that order.
pub async fn cancel(task_id: &str) -> Result<()> {
    let config = Config::from_env()?;
    let store = TaskStore::new(&config.task_root)?;
    let _lock = store.lock_task(task_id)?;
    let record = store.read_record(task_id)?;

    if store.read_result(&record)?.is_some() {
        return print_json(&CancelOutput {
            task_id,
            state: "finished",
            turn: record.current_turn,
        });
    }

    let systemd = Systemd::new(&config);
    systemd.stop_unit(&command_unit(task_id)).await?;

    if let Some(session_id) = record.worker_session_id.as_deref() {
        let mut rpc = RpcClient::connect(&config.rpc_socket, HashMap::new()).await?;
        rpc.session_abort(session_id).await?;
    }

    systemd.stop_unit(&record.controller_unit).await?;
    if store.read_result(&record)?.is_some() {
        return print_json(&CancelOutput {
            task_id,
            state: "finished",
            turn: record.current_turn,
        });
    }
    store.write_result(&TaskResult::cancelled(&record))?;

    print_json(&CancelOutput {
        task_id,
        state: "cancelled",
        turn: record.current_turn,
    })
}

/// Run one long-lived command in the current task's transient command unit.
pub async fn run(command: &[OsString]) -> Result<ExitStatus> {
    let config = Config::from_env()?;
    let task_id = env::var(BOBR_TASK_ID)
        .context("BOBR_TASK_ID is not set; run bobr-task run from a worker session")?;
    let store = TaskStore::new(&config.task_root)?;
    store.read_record(&task_id)?;
    let workspace = task_workspace(&store, &task_id)?;
    let working_directory = task_working_directory(&workspace)?;
    let path = env::var("PATH").context("PATH is not set")?;

    Systemd::new(&config)
        .run_command(
            &task_id,
            &env::current_exe().context("locate bobr-task executable")?,
            &workspace,
            &working_directory,
            &path,
            command,
        )
        .await
}

/// Replace the transient command unit's main process with the requested
/// command. This is an internal entry point used only by `bobr-task run`.
pub fn exec(task_id: &str, command: &[OsString]) -> Result<()> {
    let config = Config::from_env()?;
    let store = TaskStore::new(&config.task_root)?;
    store.read_record(task_id)?;
    let workspace = task_workspace(&store, task_id)?;
    task_working_directory(&workspace)?;
    if env::var(BOBR_TASK_ID).as_deref() != Ok(task_id) {
        bail!("BOBR_TASK_ID does not match task '{task_id}'");
    }
    let program = command.first().context("command must not be empty")?;
    let error = Command::new(program).args(&command[1..]).exec();
    Err(error).with_context(|| format!("execute '{}'", program.to_string_lossy()))
}

/// Run the long-lived worker controller. This is an internal command started
/// by `systemd-run --user`.
pub async fn controller(task_id: &str) -> Result<()> {
    let config = Config::from_env()?;
    let store = TaskStore::new(&config.task_root)?;
    let turn = controller_inner(&config, &store, task_id).await;
    // Cancellation holds this lock while stopping the controller. This keeps
    // an aborted prompt from publishing a failed result before cancellation
    // can publish the terminal cancelled result.
    let _lock = store.lock_task(task_id)?;
    match turn {
        Ok(turn) => {
            let record = store.read_record(task_id)?;
            let result = TaskResult::from_turn(&record, turn);
            let failed = result.outcome == "failed";
            if store.read_result(&record)?.is_none() {
                store.write_result(&result)?;
            }
            if failed {
                bail!("worker turn failed: {}", result.content);
            }
            Ok(())
        }
        Err(error) => {
            let record = store.read_record(task_id)?;
            if store.read_result(&record)?.is_none() {
                store.write_result(&TaskResult::failed(&record, format!("{error:#}")))?;
            }
            Err(error)
        }
    }
}

async fn controller_inner(
    config: &Config,
    store: &TaskStore,
    task_id: &str,
) -> Result<TurnComplete> {
    let mut record = store.read_record(task_id)?;
    let task_dir = store.task_dir(task_id)?;
    let workspace = store.workspace_dir(task_id)?;
    let home = workspace.join("home");
    let tmp = workspace.join("tmp");
    let path = task_path(&home, &worker_base_path()?);
    let mut rpc_env = HashMap::new();
    rpc_env.insert("HOME".to_owned(), home.display().to_string());
    rpc_env.insert("TMPDIR".to_owned(), tmp.display().to_string());
    rpc_env.insert("PATH".to_owned(), path);
    rpc_env.insert(BOBR_TASK_ID.to_owned(), task_id.to_owned());
    rpc_env.insert(
        "BOBR_TASK_ROOT".to_owned(),
        config.task_root.display().to_string(),
    );

    let mut rpc = RpcClient::connect(&config.rpc_socket, rpc_env).await?;
    let (worker_session_id, prompt) = match record.worker_session_id.clone() {
        Some(worker_session_id) => {
            let prompt = build_followup_prompt(&record.request);
            (worker_session_id, prompt)
        }
        None => {
            let worker_session_id = format!("bobr-task-{}", runtime_name(task_id));
            rpc.session_new(&config.worker_alias, &worker_session_id, &workspace)
                .await?;
            record.worker_session_id = Some(worker_session_id.clone());
            store.write_record(&record)?;
            let prompt = build_worker_prompt(&record.request);
            (worker_session_id, prompt)
        }
    };

    let log_path = task_dir.join("controller.log");
    rpc.session_prompt(&worker_session_id, &prompt, |frame| {
        task::append_json_line(&log_path, frame)
    })
    .await
}

fn task_path(home: &Path, base_path: &str) -> String {
    format!(
        "{}/.local/bin:{}/.cargo/bin:{base_path}",
        home.display(),
        home.display()
    )
}

fn read_request_file(coordinator_workspace: &Path, request_file: &Path) -> Result<String> {
    if request_file.is_absolute()
        || request_file.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        bail!("request file must be a relative path below the coordinator workspace");
    }

    let workspace = fs::canonicalize(coordinator_workspace).with_context(|| {
        format!(
            "resolve coordinator workspace '{}'",
            coordinator_workspace.display()
        )
    })?;
    let unresolved = workspace.join(request_file);
    let metadata = fs::symlink_metadata(&unresolved)
        .with_context(|| format!("inspect request file '{}'", unresolved.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!(
            "request file '{}' is not a regular file",
            unresolved.display()
        );
    }
    let resolved = fs::canonicalize(&unresolved)
        .with_context(|| format!("resolve request file '{}'", unresolved.display()))?;
    if !resolved.starts_with(&workspace) {
        bail!(
            "request file '{}' escapes the coordinator workspace",
            unresolved.display()
        );
    }

    let request = fs::read_to_string(&resolved)
        .with_context(|| format!("read request file '{}'", resolved.display()))?;
    let request = request.trim();
    if request.is_empty() {
        bail!("request file '{}' is empty", resolved.display());
    }
    Ok(request.to_owned())
}

fn build_worker_prompt(request: &str) -> String {
    format!(
        "You are bobr_worker. Your current directory, HOME, and PATH are dedicated to this task.\n\
         Work only on the task below. Do not inspect or modify parent or sibling task directories.\n\
         Run long-lived builds and tests through `bobr-task run -- <command>`.\n\
         Work non-interactively: make reasonable assumptions, and report a blocker in the final result if the task cannot be completed.\n\n\
         TASK:\n{request}\n"
    )
}

fn build_followup_prompt(request: &str) -> String {
    format!("FOLLOW-UP:\n{request}\n")
}

fn task_workspace(store: &TaskStore, task_id: &str) -> Result<PathBuf> {
    fs::canonicalize(store.workspace_dir(task_id)?)
        .with_context(|| format!("resolve workspace for task '{task_id}'"))
}

fn task_working_directory(workspace: &Path) -> Result<PathBuf> {
    let working_directory =
        fs::canonicalize(env::current_dir()?).context("resolve current working directory")?;
    if !working_directory.starts_with(workspace) {
        bail!(
            "current directory '{}' is outside task workspace '{}'",
            working_directory.display(),
            workspace.display()
        );
    }
    Ok(working_directory)
}

fn print_json(value: &impl Serialize) -> Result<()> {
    println!("{}", serde_json::to_string(value)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_a_request_below_the_coordinator_workspace() {
        let workspace = tempfile::tempdir().unwrap();
        fs::create_dir(workspace.path().join("requests")).unwrap();
        fs::write(
            workspace.path().join("requests/update-gzip.md"),
            "  Update gzip and run its tests.\n",
        )
        .unwrap();

        assert_eq!(
            read_request_file(workspace.path(), Path::new("requests/update-gzip.md")).unwrap(),
            "Update gzip and run its tests."
        );
    }

    #[test]
    fn rejects_request_paths_outside_the_coordinator_workspace() {
        let workspace = tempfile::tempdir().unwrap();
        let outside = tempfile::NamedTempFile::new().unwrap();

        assert!(read_request_file(workspace.path(), outside.path()).is_err());
        assert!(read_request_file(workspace.path(), Path::new("../request.md")).is_err());
    }

    #[test]
    fn rejects_a_symlink_request_file() {
        use std::os::unix::fs::symlink;

        let workspace = tempfile::tempdir().unwrap();
        fs::write(workspace.path().join("real.md"), "task").unwrap();
        symlink("real.md", workspace.path().join("request.md")).unwrap();

        assert!(read_request_file(workspace.path(), Path::new("request.md")).is_err());
    }

    #[test]
    fn worker_prompt_contains_only_the_explicit_request_and_preamble() {
        let prompt = build_worker_prompt("Investigate the Bobr scheduler.");
        assert!(prompt.contains("TASK:\nInvestigate the Bobr scheduler."));
        assert!(prompt.contains("bobr-task run -- <command>"));
        assert!(!prompt.contains("coordinator conversation"));
        assert!(!prompt.contains("USER:"));
    }

    #[test]
    fn task_path_uses_only_task_local_bins_and_the_explicit_base() {
        assert_eq!(
            task_path(
                Path::new("/tasks/example/workspace/home"),
                "/run/wrappers/bin:/nix/store/agent/bin"
            ),
            "/tasks/example/workspace/home/.local/bin:\
/tasks/example/workspace/home/.cargo/bin:\
/run/wrappers/bin:/nix/store/agent/bin"
        );
    }
}
