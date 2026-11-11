use serde_json::json;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use tempfile::tempdir;
use tokio::process::Command;

#[tokio::test]
async fn run_returns_the_command_unit_exit_status() {
    let dir = tempdir().unwrap();
    let task_root = dir.path().join("tasks");
    let task_id = "261111151608.status-check";
    let workspace = task_root.join(task_id).join("workspace");
    fs::create_dir_all(workspace.join("home")).unwrap();
    fs::create_dir_all(workspace.join("tmp")).unwrap();
    fs::write(
        task_root.join(task_id).join("task.json"),
        serde_json::to_vec_pretty(&json!({
            "schema_version": 1,
            "id": task_id,
            "coordinator_session_id": "coordinator",
            "worker_session_id": "worker",
            "controller_unit": "bobr-task-261111151608-status-check-controller.service",
            "created_unix_ms": 0,
            "request": "test status"
        }))
        .unwrap(),
    )
    .unwrap();

    let fake_systemd_run = dir.path().join("systemd-run");
    fs::write(&fake_systemd_run, "#!/bin/sh\nexit 17\n").unwrap();
    fs::set_permissions(&fake_systemd_run, fs::Permissions::from_mode(0o755)).unwrap();

    let status = Command::new(env!("CARGO_BIN_EXE_bobr-task"))
        .arg("run")
        .arg("--")
        .arg("echo")
        .arg("test")
        .current_dir(&workspace)
        .env("BOBR_TASK_ID", task_id)
        .env("BOBR_TASK_ROOT", &task_root)
        .env("BOBR_TASK_SYSTEMD_RUN", &fake_systemd_run)
        .status()
        .await
        .unwrap();

    assert_eq!(status.code(), Some(17));
}
