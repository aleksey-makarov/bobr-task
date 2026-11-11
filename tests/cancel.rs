use serde_json::{Value, json};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use tempfile::tempdir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tokio::process::Command;

fn append(path: &Path, line: &str) {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    writeln!(file, "{line}").unwrap();
}

async fn send(writer: &mut tokio::net::unix::OwnedWriteHalf, value: Value) {
    let mut encoded = serde_json::to_vec(&value).unwrap();
    encoded.push(b'\n');
    writer.write_all(&encoded).await.unwrap();
    writer.flush().await.unwrap();
}

#[tokio::test]
async fn cancel_stops_the_command_before_aborting_the_worker() {
    let dir = tempdir().unwrap();
    let task_root = dir.path().join("tasks");
    let task_id = "261111151608.cancel-check";
    let task_dir = task_root.join(task_id);
    fs::create_dir_all(task_dir.join("workspace")).unwrap();
    fs::write(
        task_dir.join("task.json"),
        serde_json::to_vec_pretty(&json!({
            "schema_version": 1,
            "id": task_id,
            "coordinator_session_id": "coordinator",
            "worker_session_id": "worker",
            "controller_unit": "bobr-task-261111151608-cancel-check-controller.service",
            "created_unix_ms": 0,
            "request": "test cancellation"
        }))
        .unwrap(),
    )
    .unwrap();

    let events = dir.path().join("events");
    let fake_systemctl = dir.path().join("systemctl");
    fs::write(
        &fake_systemctl,
        format!(
            "#!/bin/sh\nprintf 'systemd:%s\\n' \"$3\" >> '{}'\n",
            events.display()
        ),
    )
    .unwrap();
    fs::set_permissions(&fake_systemctl, fs::Permissions::from_mode(0o755)).unwrap();

    let socket = dir.path().join("rpc.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    let rpc_events = events.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (reader, mut writer) = stream.into_split();
        let mut lines = BufReader::new(reader).lines();

        let initialize: Value =
            serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
        append(&rpc_events, "rpc:initialize");
        send(
            &mut writer,
            json!({"jsonrpc":"2.0","id":initialize["id"],"result":{}}),
        )
        .await;

        let abort: Value =
            serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
        assert_eq!(abort["method"], "session/abort");
        assert_eq!(abort["params"]["session_id"], "worker");
        append(&rpc_events, "rpc:session/abort");
        send(
            &mut writer,
            json!({"jsonrpc":"2.0","id":abort["id"],"result":{}}),
        )
        .await;
    });

    let output = Command::new(env!("CARGO_BIN_EXE_bobr-task"))
        .arg("cancel")
        .arg(task_id)
        .env("BOBR_TASK_ROOT", &task_root)
        .env("ZEROCLAW_RPC_SOCKET", &socket)
        .env("BOBR_TASK_SYSTEMCTL", &fake_systemctl)
        .output()
        .await
        .unwrap();
    server.await.unwrap();

    assert!(output.status.success(), "{output:?}");
    assert_eq!(
        fs::read_to_string(&events).unwrap(),
        "systemd:bobr-task-261111151608-cancel-check-command.service\n\
rpc:initialize\n\
rpc:session/abort\n\
systemd:bobr-task-261111151608-cancel-check-controller.service\n"
    );
    let result: Value =
        serde_json::from_slice(&fs::read(task_dir.join("result.json")).unwrap()).unwrap();
    assert_eq!(result["outcome"], "cancelled");
}
