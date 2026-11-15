use serde_json::{Value, json};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use tempfile::tempdir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tokio::process::Command;

fn write_task(
    task_root: &Path,
    task_id: &str,
    coordinator: &str,
    worker_session_id: Option<&str>,
    last_used_unix_ms: u128,
    outcome: Option<&str>,
) {
    let task_dir = task_root.join(task_id);
    fs::create_dir_all(task_dir.join("workspace/tmp")).unwrap();
    fs::write(
        task_dir.join("task.json"),
        serde_json::to_vec_pretty(&json!({
            "schema_version": 4,
            "id": task_id,
            "coordinator_session_id": coordinator,
            "worker_session_id": worker_session_id,
            "controller_unit": format!(
                "bobr-task-{}-controller.service",
                task_id.replace('.', "-")
            ),
            "created_unix_ms": 1,
            "last_used_unix_ms": last_used_unix_ms,
            "current_turn": 1,
            "request": "test request"
        }))
        .unwrap(),
    )
    .unwrap();
    if let Some(outcome) = outcome {
        fs::write(
            task_dir.join("result.0001.json"),
            serde_json::to_vec_pretty(&json!({
                "schema_version": 4,
                "task_id": task_id,
                "turn": 1,
                "request": "test request",
                "outcome": outcome,
                "content": "test result"
            }))
            .unwrap(),
        )
        .unwrap();
    }
}

fn write_current(task_root: &Path, task_id: &str) {
    let coordinator_dir = task_root.join(".coordinators/coordinator");
    fs::create_dir_all(&coordinator_dir).unwrap();
    fs::write(
        coordinator_dir.join("state.json"),
        serde_json::to_vec_pretty(&json!({
            "schema_version": 1,
            "zeroclaw_session_id": "coordinator",
            "current_task_id": task_id
        }))
        .unwrap(),
    )
    .unwrap();
}

fn write_incompatible_task(task_root: &Path, task_id: &str) {
    let task_dir = task_root.join(task_id);
    fs::create_dir_all(&task_dir).unwrap();
    fs::write(
        task_dir.join("task.json"),
        serde_json::to_vec_pretty(&json!({
            "schema_version": 2,
            "id": task_id,
            "coordinator_session_id": "coordinator"
        }))
        .unwrap(),
    )
    .unwrap();
}

fn fake_systemctl(directory: &Path, state: &str) -> std::path::PathBuf {
    let path = directory.join("systemctl");
    fs::write(&path, format!("#!/bin/sh\necho {state}\n")).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
    path
}

async fn send(writer: &mut tokio::net::unix::OwnedWriteHalf, value: Value) {
    let mut encoded = serde_json::to_vec(&value).unwrap();
    encoded.push(b'\n');
    writer.write_all(&encoded).await.unwrap();
    writer.flush().await.unwrap();
}

#[tokio::test]
async fn cleanup_deletes_terminal_task_and_worker_session() {
    let dir = tempdir().unwrap();
    let task_root = dir.path().join("tasks");
    let task_id = "261115120000.cleanup-one";
    write_task(
        &task_root,
        task_id,
        "coordinator",
        Some("worker-session"),
        1,
        Some("completed"),
    );
    write_current(&task_root, task_id);
    let systemctl = fake_systemctl(dir.path(), "inactive");

    let socket = dir.path().join("rpc.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (reader, mut writer) = stream.into_split();
        let mut lines = BufReader::new(reader).lines();

        let initialize: Value =
            serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
        assert_eq!(initialize["method"], "initialize");
        send(
            &mut writer,
            json!({"jsonrpc":"2.0","id":initialize["id"],"result":{}}),
        )
        .await;

        let delete: Value =
            serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
        assert_eq!(delete["method"], "session/delete");
        assert_eq!(delete["params"]["session_id"], "worker-session");
        send(
            &mut writer,
            json!({"jsonrpc":"2.0","id":delete["id"],"result":{"deleted":true}}),
        )
        .await;
    });

    let output = Command::new(env!("CARGO_BIN_EXE_bobr-task"))
        .args(["cleanup", "current"])
        .env("ZEROCLAW_SESSION_ID", "coordinator")
        .env("BOBR_TASK_ROOT", &task_root)
        .env("ZEROCLAW_RPC_SOCKET", &socket)
        .env("BOBR_TASK_SYSTEMCTL", &systemctl)
        .output()
        .await
        .unwrap();
    server.await.unwrap();

    assert!(output.status.success(), "{output:?}");
    let response: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(response["deleted"], json!([task_id]));
    assert_eq!(response["skipped_active"], json!([]));
    assert_eq!(response["skipped_incompatible"], 0);
    assert_eq!(response["current_cleared"], true);
    assert!(!task_root.join(task_id).exists());
    let state: Value = serde_json::from_slice(
        &fs::read(task_root.join(".coordinators/coordinator/state.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(state["current_task_id"], Value::Null);
}

#[tokio::test]
async fn cleanup_all_deletes_terminal_tasks_and_skips_the_rest() {
    let dir = tempdir().unwrap();
    let task_root = dir.path().join("tasks");
    let completed = "261115120000.completed";
    let failed = "261115120001.failed";
    let active = "261115120002.active";
    let foreign = "261115120003.foreign";
    let incompatible = "261115120004.incompatible";
    write_task(
        &task_root,
        completed,
        "coordinator",
        None,
        1,
        Some("completed"),
    );
    write_task(&task_root, failed, "coordinator", None, 2, Some("failed"));
    write_task(&task_root, active, "coordinator", None, 3, None);
    write_task(
        &task_root,
        foreign,
        "another-coordinator",
        None,
        4,
        Some("completed"),
    );
    write_incompatible_task(&task_root, incompatible);
    write_current(&task_root, completed);
    let systemctl = fake_systemctl(dir.path(), "inactive");

    let output = Command::new(env!("CARGO_BIN_EXE_bobr-task"))
        .args(["cleanup", "all"])
        .env("ZEROCLAW_SESSION_ID", "coordinator")
        .env("BOBR_TASK_ROOT", &task_root)
        .env("BOBR_TASK_SYSTEMCTL", &systemctl)
        .output()
        .await
        .unwrap();

    assert!(output.status.success(), "{output:?}");
    let response: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(response["deleted"], json!([failed, completed]));
    assert_eq!(response["skipped_active"], json!([active]));
    assert_eq!(response["skipped_incompatible"], 1);
    assert_eq!(response["current_cleared"], true);
    assert!(!task_root.join(completed).exists());
    assert!(!task_root.join(failed).exists());
    assert!(task_root.join(active).exists());
    assert!(task_root.join(foreign).exists());
    assert!(task_root.join(incompatible).exists());
}

#[tokio::test]
async fn cleanup_refuses_a_task_with_an_active_unit() {
    let dir = tempdir().unwrap();
    let task_root = dir.path().join("tasks");
    let task_id = "261115120000.active-unit";
    write_task(
        &task_root,
        task_id,
        "coordinator",
        None,
        1,
        Some("completed"),
    );
    let systemctl = fake_systemctl(dir.path(), "active");

    let output = Command::new(env!("CARGO_BIN_EXE_bobr-task"))
        .args(["cleanup", task_id])
        .env("ZEROCLAW_SESSION_ID", "coordinator")
        .env("BOBR_TASK_ROOT", &task_root)
        .env("BOBR_TASK_SYSTEMCTL", &systemctl)
        .output()
        .await
        .unwrap();

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("still active"));
    assert!(task_root.join(task_id).exists());
}
