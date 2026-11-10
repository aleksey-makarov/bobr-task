//! Core implementation for the `bobr-task` command.

mod config;
mod rpc;
mod systemd;
mod task;

use anyhow::{Context, Result, anyhow, bail};
use config::Config;
use rpc::{Message, RpcClient, TurnComplete};
use serde::Serialize;
use std::collections::HashMap;
use std::env;
use std::path::Path;
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
pub async fn start(label: &str) -> Result<()> {
    let config = Config::from_env()?;
    let coordinator_session_id = env::var(ZEROCLAW_SESSION_ID)
        .context("ZEROCLAW_SESSION_ID is not set; run bobr-task from a ZeroClaw session")?;

    let mut rpc = RpcClient::connect(&config.rpc_socket, HashMap::new()).await?;
    let messages = rpc.session_messages(&coordinator_session_id, 20).await?;
    let request = build_worker_request(&messages)?;

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
    rpc.session_prompt(&worker_session_id, &record.request, |frame| {
        task::append_json_line(&log_path, frame)
    })
    .await
}

fn task_path(home: &Path) -> String {
    let inherited = env::var("PATH").unwrap_or_else(|_| "/run/current-system/sw/bin".to_owned());
    format!("{}/.local/bin:{inherited}", home.display())
}

fn build_worker_request(messages: &[Message]) -> Result<String> {
    let conversation: Vec<_> = messages
        .iter()
        .filter(|message| message.kind == "message")
        .filter(|message| matches!(message.role.as_str(), "user" | "assistant"))
        .filter(|message| !message.content.trim().is_empty())
        .collect();
    if conversation.is_empty() {
        bail!("the coordinator session contains no user conversation");
    }

    let mut request = String::from(
        "You are bobr_worker. Work only inside the task workspace supplied as your current directory.\n\
         The recent coordinator conversation is included below. Treat the latest exchange as the task and earlier messages only as context.\n\
         Do not access parent or sibling task directories.\n\n",
    );
    for message in conversation {
        request.push_str(&message.role.to_ascii_uppercase());
        request.push_str(":\n");
        request.push_str(message.content.trim());
        request.push_str("\n\n");
    }
    Ok(request)
}

fn print_json(value: &impl Serialize) -> Result<()> {
    println!("{}", serde_json::to_string(value)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_request_keeps_conversation_and_discards_tools() {
        let messages = vec![
            Message {
                role: "user".into(),
                content: "Investigate Bobr".into(),
                kind: "message".into(),
            },
            Message {
                role: "assistant".into(),
                content: "tool noise".into(),
                kind: "tool_call".into(),
            },
            Message {
                role: "assistant".into(),
                content: "I will check it".into(),
                kind: "message".into(),
            },
        ];
        let request = build_worker_request(&messages).unwrap();
        assert!(request.contains("USER:\nInvestigate Bobr"));
        assert!(request.contains("ASSISTANT:\nI will check it"));
        assert!(!request.contains("tool noise"));
    }
}
