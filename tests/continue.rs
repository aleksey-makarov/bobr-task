use serde_json::{Value, json};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use tempfile::tempdir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::process::Command;

fn write_task(task_root: &Path, task_id: &str, turn: u32, request: &str) {
    let task_dir = task_root.join(task_id);
    fs::create_dir_all(task_dir.join("workspace/home")).unwrap();
    fs::create_dir_all(task_dir.join("workspace/tmp")).unwrap();
    fs::write(
        task_dir.join("task.json"),
        serde_json::to_vec_pretty(&json!({
            "schema_version": 2,
            "id": task_id,
            "coordinator_session_id": "coordinator",
            "worker_session_id": "worker",
            "controller_unit": "bobr-task-261112120000-continued-controller.service",
            "created_unix_ms": 0,
            "current_turn": turn,
            "request": request
        }))
        .unwrap(),
    )
    .unwrap();
}

fn write_result(task_root: &Path, task_id: &str, turn: u32, request: &str) {
    fs::write(
        task_root
            .join(task_id)
            .join(format!("result.{turn:04}.json")),
        serde_json::to_vec_pretty(&json!({
            "schema_version": 2,
            "task_id": task_id,
            "turn": turn,
            "request": request,
            "outcome": "completed",
            "content": format!("result {turn}")
        }))
        .unwrap(),
    )
    .unwrap();
}

async fn receive(stream: &mut BufReader<UnixStream>) -> Value {
    let mut line = String::new();
    stream.read_line(&mut line).await.unwrap();
    serde_json::from_str(&line).unwrap()
}

async fn send(stream: &mut BufReader<UnixStream>, value: Value) {
    let mut encoded = serde_json::to_vec(&value).unwrap();
    encoded.push(b'\n');
    stream.get_mut().write_all(&encoded).await.unwrap();
    stream.get_mut().flush().await.unwrap();
}

#[tokio::test]
async fn continue_advances_the_turn_and_starts_the_existing_task() {
    let dir = tempdir().unwrap();
    let task_root = dir.path().join("tasks");
    let task_id = "261112120000.continued";
    write_task(&task_root, task_id, 1, "first request");
    write_result(&task_root, task_id, 1, "first request");

    let systemctl = dir.path().join("systemctl");
    fs::write(&systemctl, "#!/bin/sh\necho inactive\nexit 3\n").unwrap();
    fs::set_permissions(&systemctl, fs::Permissions::from_mode(0o755)).unwrap();
    let systemd_arguments = dir.path().join("systemd-arguments");
    let systemd_run = dir.path().join("systemd-run");
    fs::write(
        &systemd_run,
        format!(
            "#!/bin/sh\nprintf '%s\\n' \"$@\" > '{}'\n",
            systemd_arguments.display()
        ),
    )
    .unwrap();
    fs::set_permissions(&systemd_run, fs::Permissions::from_mode(0o755)).unwrap();

    let socket = dir.path().join("rpc.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut stream = BufReader::new(stream);
        let initialize = receive(&mut stream).await;
        send(
            &mut stream,
            json!({"jsonrpc":"2.0","id":initialize["id"],"result":{}}),
        )
        .await;
        let state = receive(&mut stream).await;
        assert_eq!(state["method"], "session/state");
        assert_eq!(state["params"]["session_id"], "worker");
        send(
            &mut stream,
            json!({"jsonrpc":"2.0","id":state["id"],"result":{"state":"idle"}}),
        )
        .await;
    });

    let coordinator = dir.path().join("coordinator");
    fs::create_dir(&coordinator).unwrap();
    fs::write(coordinator.join("request.md"), "second request\n").unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_bobr-task"))
        .arg("continue")
        .arg(task_id)
        .arg("request.md")
        .current_dir(&coordinator)
        .env("ZEROCLAW_SESSION_ID", "coordinator")
        .env("ZEROCLAW_RPC_SOCKET", &socket)
        .env("BOBR_TASK_ROOT", &task_root)
        .env("BOBR_TASK_SYSTEMCTL", &systemctl)
        .env("BOBR_TASK_SYSTEMD_RUN", &systemd_run)
        .env(
            "BOBR_TASK_BASE_PATH",
            "/run/wrappers/bin:/nix/store/agent/bin",
        )
        .output()
        .await
        .unwrap();
    server.await.unwrap();

    assert!(output.status.success(), "{output:?}");
    let response: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(response["task_id"], task_id);
    assert_eq!(response["turn"], 2);
    let task: Value =
        serde_json::from_slice(&fs::read(task_root.join(task_id).join("task.json")).unwrap())
            .unwrap();
    assert_eq!(task["current_turn"], 2);
    assert_eq!(task["request"], "second request");
    assert!(task_root.join(task_id).join("result.0001.json").is_file());
    assert!(!task_root.join(task_id).join("result.0002.json").exists());
    assert!(
        fs::read_to_string(systemd_arguments)
            .unwrap()
            .ends_with(&format!("controller\n{task_id}\n"))
    );

    let current_result = Command::new(env!("CARGO_BIN_EXE_bobr-task"))
        .arg("result")
        .arg(task_id)
        .env("BOBR_TASK_ROOT", &task_root)
        .output()
        .await
        .unwrap();
    assert!(!current_result.status.success());
    assert!(String::from_utf8_lossy(&current_result.stderr).contains("turn 2 is still active"));

    let history = Command::new(env!("CARGO_BIN_EXE_bobr-task"))
        .arg("history")
        .arg(task_id)
        .env("BOBR_TASK_ROOT", &task_root)
        .output()
        .await
        .unwrap();
    let history: Value = serde_json::from_slice(&history.stdout).unwrap();
    assert_eq!(history["current_turn"], 2);
    assert_eq!(history["results"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn followup_controller_reuses_the_worker_session() {
    let dir = tempdir().unwrap();
    let task_root = dir.path().join("tasks");
    let task_id = "261112120000.continued";
    write_task(&task_root, task_id, 2, "inspect the tests");
    write_result(&task_root, task_id, 1, "first request");

    let socket = dir.path().join("rpc.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut stream = BufReader::new(stream);
        let initialize = receive(&mut stream).await;
        send(
            &mut stream,
            json!({"jsonrpc":"2.0","id":initialize["id"],"result":{}}),
        )
        .await;
        let prompt = receive(&mut stream).await;
        assert_eq!(prompt["method"], "session/prompt");
        assert_eq!(prompt["params"]["session_id"], "worker");
        assert_eq!(
            prompt["params"]["prompt"],
            "FOLLOW-UP:\ninspect the tests\n"
        );
        send(
            &mut stream,
            json!({"jsonrpc":"2.0","method":"session/update","params":{
                "type":"turn_complete",
                "session_id":"worker",
                "outcome":"completed",
                "content":"tests inspected"
            }}),
        )
        .await;
    });

    let output = Command::new(env!("CARGO_BIN_EXE_bobr-task"))
        .arg("controller")
        .arg(task_id)
        .env("ZEROCLAW_RPC_SOCKET", &socket)
        .env("BOBR_TASK_ROOT", &task_root)
        .env(
            "BOBR_TASK_BASE_PATH",
            "/run/wrappers/bin:/nix/store/agent/bin",
        )
        .output()
        .await
        .unwrap();
    server.await.unwrap();

    assert!(output.status.success(), "{output:?}");
    let result: Value = serde_json::from_slice(
        &fs::read(task_root.join(task_id).join("result.0002.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(result["content"], "tests inspected");

    let history = Command::new(env!("CARGO_BIN_EXE_bobr-task"))
        .arg("history")
        .arg(task_id)
        .env("BOBR_TASK_ROOT", &task_root)
        .output()
        .await
        .unwrap();
    assert!(history.status.success(), "{history:?}");
    let history: Value = serde_json::from_slice(&history.stdout).unwrap();
    assert_eq!(history["results"].as_array().unwrap().len(), 2);
    assert_eq!(history["results"][0]["content"], "result 1");
    assert_eq!(history["results"][1]["content"], "tests inspected");
}

#[tokio::test]
async fn continue_rejects_another_coordinator() {
    let dir = tempdir().unwrap();
    let task_root = dir.path().join("tasks");
    let task_id = "261112120000.continued";
    write_task(&task_root, task_id, 1, "first request");
    write_result(&task_root, task_id, 1, "first request");
    fs::write(dir.path().join("request.md"), "second request\n").unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_bobr-task"))
        .arg("continue")
        .arg(task_id)
        .arg("request.md")
        .current_dir(dir.path())
        .env("ZEROCLAW_SESSION_ID", "other-coordinator")
        .env("BOBR_TASK_ROOT", &task_root)
        .env(
            "BOBR_TASK_BASE_PATH",
            "/run/wrappers/bin:/nix/store/agent/bin",
        )
        .output()
        .await
        .unwrap();

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("another coordinator session"));
    let task: Value =
        serde_json::from_slice(&fs::read(task_root.join(task_id).join("task.json")).unwrap())
            .unwrap();
    assert_eq!(task["current_turn"], 1);
}
