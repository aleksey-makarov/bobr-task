//! Core implementation for the `bobr-task` command.

mod config;
mod rpc;
mod systemd;
mod task;

use anyhow::{Context, Result, anyhow, bail};
use config::Config;
use rpc::{RpcClient, TurnComplete};
use serde::Serialize;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::{Component, Path};
use systemd::Systemd;
use task::{TaskResult, TaskStore, runtime_name};

/// Environment variable installed by ZeroClaw's shell tool for the active
/// coordinator session.
pub const ZEROCLAW_SESSION_ID: &str = "ZEROCLAW_SESSION_ID";

#[derive(Debug, Serialize)]
struct StartOutput<'a> {
    task_id: &'a str,
    state: &'static str,
}

#[derive(Debug, Serialize)]
struct StatusOutput<'a> {
    task_id: &'a str,
    state: String,
    controller_unit: &'a str,
    worker_session_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    worker_state: Option<String>,
}

#[derive(Debug, Serialize)]
struct CancelOutput<'a> {
    task_id: &'a str,
    state: &'static str,
}

/// Start a task from the current ZeroClaw coordinator session.
pub async fn start(label: &str, request_file: &Path) -> Result<()> {
    let config = Config::from_env()?;
    let coordinator_session_id = env::var(ZEROCLAW_SESSION_ID)
        .context("ZEROCLAW_SESSION_ID is not set; run bobr-task from a ZeroClaw session")?;
    let coordinator_workspace = env::current_dir().context("resolve coordinator workspace")?;
    let request = read_request_file(&coordinator_workspace, request_file)?;

    let store = TaskStore::new(&config.task_root)?;
    let record = store.create(label, &coordinator_session_id, &request)?;
    let systemd = Systemd::new(&config);

    if let Err(error) = systemd
        .start_controller(
            &record,
            &env::current_exe().context("locate bobr-task executable")?,
        )
        .await
    {
        let result =
            TaskResult::failed(&record.id, format!("failed to start controller: {error:#}"));
        store.write_result(&record.id, &result)?;
        return Err(error);
    }

    print_json(&StartOutput {
        task_id: &record.id,
        state: "started",
    })
}

/// Report task state using its result, user unit, and worker session.
pub async fn status(task_id: &str) -> Result<()> {
    let config = Config::from_env()?;
    let store = TaskStore::new(&config.task_root)?;
    let record = store.read_record(task_id)?;

    if let Some(result) = store.read_result(task_id)? {
        return print_json(&StatusOutput {
            task_id,
            state: result.outcome,
            controller_unit: &record.controller_unit,
            worker_session_id: record.worker_session_id.as_deref(),
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
        worker_state,
    })
}

/// Print the terminal worker result.
pub fn result(task_id: &str) -> Result<()> {
    let config = Config::from_env()?;
    let store = TaskStore::new(&config.task_root)?;
    let result = store
        .read_result(task_id)?
        .ok_or_else(|| anyhow!("task '{task_id}' has not produced a result"))?;
    print_json(&result)
}

/// Cancel the worker turn and stop its controller unit.
pub async fn cancel(task_id: &str) -> Result<()> {
    let config = Config::from_env()?;
    let store = TaskStore::new(&config.task_root)?;
    let record = store.read_record(task_id)?;

    if store.read_result(task_id)?.is_some() {
        return print_json(&CancelOutput {
            task_id,
            state: "finished",
        });
    }

    if let Some(session_id) = record.worker_session_id.as_deref() {
        let mut rpc = RpcClient::connect(&config.rpc_socket, HashMap::new()).await?;
        rpc.session_abort(session_id).await?;
    }

    Systemd::new(&config)
        .stop_unit(&record.controller_unit)
        .await?;
    if store.read_result(task_id)?.is_none() {
        store.write_result(task_id, &TaskResult::cancelled(task_id))?;
    }

    print_json(&CancelOutput {
        task_id,
        state: "cancelled",
    })
}

/// Run the long-lived worker controller. This is an internal command started
/// by `systemd-run --user`.
pub async fn controller(task_id: &str) -> Result<()> {
    let config = Config::from_env()?;
    let store = TaskStore::new(&config.task_root)?;
    match controller_inner(&config, &store, task_id).await {
        Ok(turn) => {
            let result = TaskResult::from_turn(task_id, turn);
            let failed = result.outcome == "failed";
            store.write_result(task_id, &result)?;
            if failed {
                bail!("worker turn failed: {}", result.content);
            }
            Ok(())
        }
        Err(error) => {
            store.write_result(task_id, &TaskResult::failed(task_id, format!("{error:#}")))?;
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
    let path = task_path(&home);
    let mut rpc_env = HashMap::new();
    rpc_env.insert("HOME".to_owned(), home.display().to_string());
    rpc_env.insert("PATH".to_owned(), path);

    let mut rpc = RpcClient::connect(&config.rpc_socket, rpc_env).await?;
    let worker_session_id = format!("bobr-task-{}", runtime_name(task_id));
    rpc.session_new(&config.worker_alias, &worker_session_id, &workspace)
        .await?;
    record.worker_session_id = Some(worker_session_id.clone());
    store.write_record(&record)?;

    let log_path = task_dir.join("controller.log");
    let prompt = build_worker_prompt(&record.request);
    rpc.session_prompt(&worker_session_id, &prompt, |frame| {
        task::append_json_line(&log_path, frame)
    })
    .await
}

fn task_path(home: &Path) -> String {
    let inherited = env::var("PATH").unwrap_or_else(|_| "/run/current-system/sw/bin".to_owned());
    format!("{}/.local/bin:{inherited}", home.display())
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
         Work non-interactively: make reasonable assumptions, and report a blocker in the final result if the task cannot be completed.\n\n\
         TASK:\n{request}\n"
    )
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
        assert!(!prompt.contains("coordinator conversation"));
        assert!(!prompt.contains("USER:"));
    }
}
