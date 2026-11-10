use crate::config::{Config, current_uid};
use crate::task::TaskRecord;
use anyhow::{Context, Result, bail};
use std::path::{Path, PathBuf};
use tokio::process::Command;

pub struct Systemd {
    systemd_run: PathBuf,
    systemctl: PathBuf,
    task_root: PathBuf,
    rpc_socket: PathBuf,
    worker_alias: String,
}

impl Systemd {
    pub fn new(config: &Config) -> Self {
        Self {
            systemd_run: config.systemd_run.clone(),
            systemctl: config.systemctl.clone(),
            task_root: config.task_root.clone(),
            rpc_socket: config.rpc_socket.clone(),
            worker_alias: config.worker_alias.clone(),
        }
    }

    pub async fn start_controller(&self, task: &TaskRecord, executable: &Path) -> Result<()> {
        let task_dir = self.task_root.join(&task.id);
        let home = task_dir.join("workspace/home");
        let path = std::env::var("PATH").context("PATH is not set")?;
        let mut command = self.user_command(&self.systemd_run);
        command
            .arg("--user")
            .arg("--quiet")
            .arg("--collect")
            .arg(format!("--unit={}", task.controller_unit))
            .arg("--property=Type=exec")
            .arg("--property=NoNewPrivileges=yes")
            .arg("--property=PrivateTmp=yes")
            .arg("--property=ProtectHome=yes")
            .arg("--property=ProtectSystem=strict")
            .arg(format!("--property=ReadWritePaths={}", task_dir.display()))
            .arg(format!("--working-directory={}", task_dir.display()))
            .arg(format!("--setenv=HOME={}", home.display()))
            .arg(format!("--setenv=PATH={path}"))
            .arg(format!(
                "--setenv=BOBR_TASK_ROOT={}",
                self.task_root.display()
            ))
            .arg(format!(
                "--setenv=ZEROCLAW_RPC_SOCKET={}",
                self.rpc_socket.display()
            ))
            .arg(format!(
                "--setenv=BOBR_TASK_WORKER_ALIAS={}",
                self.worker_alias
            ))
            .arg(executable)
            .arg("controller")
            .arg(&task.id);
        let output = command.output().await.context("run systemd-run --user")?;
        if !output.status.success() {
            bail!(
                "systemd-run failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        Ok(())
    }

    pub async fn unit_state(&self, unit: &str) -> Result<String> {
        let output = self
            .user_command(&self.systemctl)
            .arg("--user")
            .arg("is-active")
            .arg(unit)
            .output()
            .await
            .context("query controller unit")?;
        let state = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        if !state.is_empty() {
            Ok(state)
        } else if output.status.success() {
            Ok("unknown".to_owned())
        } else {
            bail!(
                "systemctl is-active failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
    }

    pub async fn stop_unit(&self, unit: &str) -> Result<()> {
        let output = self
            .user_command(&self.systemctl)
            .arg("--user")
            .arg("stop")
            .arg(unit)
            .output()
            .await
            .context("stop controller unit")?;
        if !output.status.success() {
            let message = String::from_utf8_lossy(&output.stderr);
            if !message.contains("not loaded") && !message.contains("not found") {
                bail!("systemctl stop failed: {}", message.trim());
            }
        }
        Ok(())
    }

    fn user_command(&self, program: &Path) -> Command {
        let mut command = Command::new(program);
        command.arg(format!("--machine={}@.host", current_uid()));
        command
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use tempfile::tempdir;

    #[test]
    fn user_manager_transport_uses_current_uid() {
        let config = Config {
            task_root: "/tasks".into(),
            rpc_socket: "/rpc.sock".into(),
            worker_alias: "worker".into(),
            systemd_run: "systemd-run".into(),
            systemctl: "systemctl".into(),
        };
        let command = Systemd::new(&config).user_command(Path::new("systemctl"));
        let arguments: Vec<_> = command
            .as_std()
            .get_args()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            arguments,
            vec![format!("--machine={}@.host", current_uid())]
        );
    }

    #[tokio::test]
    async fn start_controller_invokes_systemd_run_with_task_isolation() {
        let dir = tempdir().unwrap();
        let task_root = dir.path().join("tasks");
        let task_dir = task_root.join("task-1");
        fs::create_dir_all(task_dir.join("workspace/home")).unwrap();
        let arguments = dir.path().join("arguments");
        let fake = dir.path().join("systemd-run");
        fs::write(
            &fake,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" > '{}'\n",
                arguments.display()
            ),
        )
        .unwrap();
        fs::set_permissions(&fake, fs::Permissions::from_mode(0o755)).unwrap();
        let config = Config {
            task_root,
            rpc_socket: "/rpc.sock".into(),
            worker_alias: "worker".into(),
            systemd_run: fake,
            systemctl: "systemctl".into(),
        };
        let task = TaskRecord {
            schema_version: 1,
            id: "task-1".into(),
            coordinator_session_id: "coordinator".into(),
            worker_session_id: None,
            controller_unit: "bobr-task-task-1-controller.service".into(),
            created_unix_ms: 0,
            request: "test".into(),
        };

        Systemd::new(&config)
            .start_controller(&task, Path::new("/nix/store/bobr-task/bin/bobr-task"))
            .await
            .unwrap();

        let arguments = fs::read_to_string(arguments).unwrap();
        assert!(arguments.starts_with(&format!("--machine={}@.host\n--user\n", current_uid())));
        assert!(arguments.contains("--user\n"));
        assert!(arguments.contains("--property=ProtectSystem=strict\n"));
        assert!(arguments.contains("--property=ReadWritePaths="));
        assert!(arguments.contains("--setenv=PATH="));
        assert!(arguments.ends_with("controller\ntask-1\n"));
    }
}
