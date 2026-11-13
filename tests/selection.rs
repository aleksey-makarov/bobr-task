use serde_json::{Value, json};
use std::fs;
use std::path::Path;
use tempfile::tempdir;
use tokio::process::Command;

fn write_task(
    task_root: &Path,
    task_id: &str,
    coordinator: &str,
    last_used_unix_ms: u128,
    outcome: Option<&str>,
) {
    let task_dir = task_root.join(task_id);
    fs::create_dir_all(task_dir.join("workspace/home")).unwrap();
    fs::create_dir_all(task_dir.join("workspace/tmp")).unwrap();
    fs::write(
        task_dir.join("task.json"),
        serde_json::to_vec_pretty(&json!({
            "schema_version": 3,
            "id": task_id,
            "coordinator_session_id": coordinator,
            "worker_session_id": null,
            "controller_unit": format!("bobr-task-{}-controller.service", task_id.replace('.', "-")),
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
                "schema_version": 3,
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

fn write_incompatible_task(task_root: &Path, task_id: &str, coordinator: &str) {
    let task_dir = task_root.join(task_id);
    fs::create_dir_all(&task_dir).unwrap();
    fs::write(
        task_dir.join("task.json"),
        serde_json::to_vec_pretty(&json!({
            "schema_version": 2,
            "id": task_id,
            "coordinator_session_id": coordinator
        }))
        .unwrap(),
    )
    .unwrap();
}

async fn run(task_root: &Path, arguments: &[&str]) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_bobr-task"));
    command
        .args(arguments)
        .env("ZEROCLAW_SESSION_ID", "coordinator")
        .env("BOBR_TASK_ROOT", task_root);
    command.output().await.unwrap()
}

#[tokio::test]
async fn selects_and_resolves_the_current_task() {
    let dir = tempdir().unwrap();
    let task_root = dir.path().join("tasks");
    let first = "261113120000.first";
    let second = "261113130000.second";
    let cancelled = "261113140000.cancelled";
    write_task(&task_root, first, "coordinator", 10, Some("completed"));
    write_task(&task_root, second, "coordinator", 20, Some("completed"));
    write_task(&task_root, cancelled, "coordinator", 30, Some("cancelled"));
    write_task(
        &task_root,
        "261113150000.foreign",
        "other-coordinator",
        40,
        Some("completed"),
    );
    write_incompatible_task(&task_root, "261113110000.old", "coordinator");

    let current = run(&task_root, &["current"]).await;
    assert!(current.status.success(), "{current:?}");
    let current: Value = serde_json::from_slice(&current.stdout).unwrap();
    assert_eq!(current["task"], Value::Null);

    let selected = run(&task_root, &["select", first]).await;
    assert!(selected.status.success(), "{selected:?}");
    let selected: Value = serde_json::from_slice(&selected.stdout).unwrap();
    assert_eq!(selected["task_id"], first);
    assert_eq!(selected["current"], true);

    for command in ["current", "status", "result", "history"] {
        let output = if command == "current" {
            run(&task_root, &[command]).await
        } else {
            run(&task_root, &[command, "current"]).await
        };
        assert!(output.status.success(), "{command}: {output:?}");
        let value: Value = serde_json::from_slice(&output.stdout).unwrap();
        let reported_task = if command == "current" {
            &value["task"]["task_id"]
        } else {
            &value["task_id"]
        };
        assert_eq!(reported_task, first, "{command}");
    }

    let listed = run(&task_root, &["list"]).await;
    assert!(listed.status.success(), "{listed:?}");
    let listed: Value = serde_json::from_slice(&listed.stdout).unwrap();
    assert_eq!(listed["current_task_id"], first);
    assert_eq!(listed["skipped_incompatible"], 1);
    let tasks = listed["tasks"].as_array().unwrap();
    assert_eq!(tasks.len(), 3);
    assert_eq!(tasks[0]["task_id"], first);
    assert_eq!(tasks[0]["current"], true);
    assert!(
        tasks
            .iter()
            .all(|task| task["task_id"] != "261113150000.foreign")
    );

    let rejected = run(&task_root, &["select", cancelled]).await;
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("cannot be selected"));
    let current = run(&task_root, &["current"]).await;
    let current: Value = serde_json::from_slice(&current.stdout).unwrap();
    assert_eq!(current["task"]["task_id"], first);

    let foreign = run(&task_root, &["status", "261113150000.foreign"]).await;
    assert!(!foreign.status.success());
    assert!(String::from_utf8_lossy(&foreign.stderr).contains("another coordinator session"));
}

#[tokio::test]
async fn failed_start_clears_the_new_current_task() {
    let dir = tempdir().unwrap();
    let task_root = dir.path().join("tasks");
    let coordinator = dir.path().join("coordinator");
    fs::create_dir(&coordinator).unwrap();
    fs::write(coordinator.join("request.md"), "test request\n").unwrap();
    let systemd_run = dir.path().join("systemd-run");
    fs::write(&systemd_run, "#!/bin/sh\necho launch failed >&2\nexit 1\n").unwrap();
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(&systemd_run, fs::Permissions::from_mode(0o755)).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_bobr-task"))
        .arg("start")
        .arg("start-failure")
        .arg("request.md")
        .current_dir(&coordinator)
        .env("ZEROCLAW_SESSION_ID", "coordinator")
        .env("BOBR_TASK_ROOT", &task_root)
        .env("BOBR_TASK_SYSTEMD_RUN", &systemd_run)
        .env(
            "BOBR_TASK_BASE_PATH",
            "/run/wrappers/bin:/nix/store/agent/bin",
        )
        .output()
        .await
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("systemd-run failed"));

    let current = run(&task_root, &["current"]).await;
    assert!(current.status.success(), "{current:?}");
    let current: Value = serde_json::from_slice(&current.stdout).unwrap();
    assert_eq!(current["task"], Value::Null);

    let task_ids: Vec<_> = fs::read_dir(&task_root)
        .unwrap()
        .filter_map(|entry| {
            let name = entry.unwrap().file_name().into_string().unwrap();
            (!name.starts_with('.')).then_some(name)
        })
        .collect();
    assert_eq!(task_ids.len(), 1);
    let result: Value = serde_json::from_slice(
        &fs::read(task_root.join(&task_ids[0]).join("result.0001.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(result["outcome"], "failed");
}

#[tokio::test]
async fn successful_start_selects_the_new_task() {
    let dir = tempdir().unwrap();
    let task_root = dir.path().join("tasks");
    let coordinator = dir.path().join("coordinator");
    fs::create_dir(&coordinator).unwrap();
    fs::write(coordinator.join("request.md"), "test request\n").unwrap();
    let systemd_run = dir.path().join("systemd-run");
    fs::write(&systemd_run, "#!/bin/sh\nexit 0\n").unwrap();
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(&systemd_run, fs::Permissions::from_mode(0o755)).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_bobr-task"))
        .arg("start")
        .arg("start-success")
        .arg("request.md")
        .current_dir(&coordinator)
        .env("ZEROCLAW_SESSION_ID", "coordinator")
        .env("BOBR_TASK_ROOT", &task_root)
        .env("BOBR_TASK_SYSTEMD_RUN", &systemd_run)
        .env(
            "BOBR_TASK_BASE_PATH",
            "/run/wrappers/bin:/nix/store/agent/bin",
        )
        .output()
        .await
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let started: Value = serde_json::from_slice(&output.stdout).unwrap();

    let state: Value = serde_json::from_slice(
        &fs::read(task_root.join(".coordinators/coordinator/state.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(state["current_task_id"], started["task_id"]);
}

#[tokio::test]
async fn cancellation_does_not_clear_a_concurrently_selected_task() {
    let dir = tempdir().unwrap();
    let task_root = dir.path().join("tasks");
    let cancelling = "261113160000.cancelling";
    let replacement = "261113170000.replacement";
    write_task(&task_root, cancelling, "coordinator", 10, None);
    write_task(
        &task_root,
        replacement,
        "coordinator",
        20,
        Some("completed"),
    );
    write_current(&task_root, cancelling);

    let entered = dir.path().join("entered");
    let release = dir.path().join("release");
    let systemctl = dir.path().join("systemctl");
    fs::write(
        &systemctl,
        format!(
            "#!/bin/sh\ncase \"$3\" in\n  *-command.service)\n    touch '{}'\n    while [ ! -e '{}' ]; do sleep 0.01; done\n    ;;\nesac\n",
            entered.display(),
            release.display()
        ),
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(&systemctl, fs::Permissions::from_mode(0o755)).unwrap();

    let mut cancellation = Command::new(env!("CARGO_BIN_EXE_bobr-task"));
    cancellation
        .arg("cancel")
        .arg("current")
        .env("ZEROCLAW_SESSION_ID", "coordinator")
        .env("BOBR_TASK_ROOT", &task_root)
        .env("BOBR_TASK_SYSTEMCTL", &systemctl)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    let cancellation = cancellation.spawn().unwrap();

    for _ in 0..200 {
        if entered.exists() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    if !entered.exists() {
        fs::write(&release, "continue\n").unwrap();
        panic!("cancel did not enter the command stop");
    }

    let selected = run(&task_root, &["select", replacement]).await;
    fs::write(&release, "continue\n").unwrap();
    assert!(selected.status.success(), "{selected:?}");
    let cancelled = cancellation.wait_with_output().await.unwrap();
    assert!(cancelled.status.success(), "{cancelled:?}");
    let cancelled: Value = serde_json::from_slice(&cancelled.stdout).unwrap();
    assert_eq!(cancelled["current_cleared"], false);

    let current = run(&task_root, &["current"]).await;
    assert!(current.status.success(), "{current:?}");
    let current: Value = serde_json::from_slice(&current.stdout).unwrap();
    assert_eq!(current["task"]["task_id"], replacement);
}
