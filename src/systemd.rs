use crate::config::{Config, current_uid};
use crate::task::{TaskRecord, command_unit};
use anyhow::{Context, Result, bail};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::ExitStatus;
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

    pub async fn start_controller(
        &self,
        task: &TaskRecord,
        executable: &Path,
        worker_base_path: &str,
    ) -> Result<()> {
        let task_dir = self.task_root.join(&task.id);
        let home = task_dir.join("workspace/home");
        let tmp = task_dir.join("workspace/tmp");
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
            .arg(format!("--setenv=TMPDIR={}", tmp.display()))
            .arg(format!("--setenv=PATH={path}"))
            .arg(format!("--setenv=BOBR_TASK_BASE_PATH={worker_base_path}"))
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

    pub async fn unit_is_active(&self, unit: &str) -> Result<bool> {
        Ok(matches!(
            self.unit_state(unit).await?.as_str(),
            "active" | "activating" | "reloading" | "deactivating"
        ))
    }

    pub async fn run_command(
        &self,
        task_id: &str,
        executable: &Path,
        workspace: &Path,
        working_directory: &Path,
        path: &str,
        argv: &[OsString],
    ) -> Result<ExitStatus> {
        if argv.is_empty() {
            bail!("command must not be empty");
        }
        let unit = command_unit(task_id);
        let home = workspace.join("home");
        let tmp = workspace.join("tmp");
        let mut command = self.user_command(&self.systemd_run);
        command
            .arg("--user")
            .arg("--quiet")
            .arg("--wait")
            .arg("--pipe")
            .arg("--collect")
            .arg(format!("--unit={unit}"))
            .arg("--property=Type=exec")
            .arg("--property=KillSignal=SIGINT")
            .arg("--property=KillMode=mixed")
            .arg("--property=TimeoutStopSec=30s")
            .arg("--property=UMask=0022")
            .arg(format!(
                "--working-directory={}",
                working_directory.display()
            ))
            .arg(format!("--setenv=HOME={}", home.display()))
            .arg(format!("--setenv=TMPDIR={}", tmp.display()))
            .arg(format!("--setenv=PATH={path}"))
            .arg(format!("--setenv=BOBR_TASK_ID={task_id}"))
            .arg(format!(
                "--setenv=BOBR_TASK_ROOT={}",
                self.task_root.display()
            ))
            .arg(executable)
            .arg("exec")
            .arg(task_id)
            .arg("--")
            .args(argv);
        let status = command.status().await.context("run command unit")?;
        Ok(status)
    }

    pub async fn stop_unit(&self, unit: &str) -> Result<()> {
        let output = self
            .user_command(&self.systemctl)
            .arg("--user")
            .arg("stop")
            .arg(unit)
            .output()
            .await
            .context("stop user unit")?;
        if !output.status.success() {
            let message = String::from_utf8_lossy(&output.stderr);
            if !message.contains("not loaded") && !message.contains("not found") {
                bail!("systemctl stop failed: {}", message.trim());
            }
        }
        Ok(())
    }

    fn user_command(&self, program: &Path) -> Command {
        let uid = current_uid();
        let runtime_dir = format!("/run/user/{uid}");
        let mut command = Command::new(program);
        command.env("XDG_RUNTIME_DIR", &runtime_dir).env(
            "DBUS_SESSION_BUS_ADDRESS",
            format!("unix:path={runtime_dir}/bus"),
        );
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
    fn user_manager_environment_uses_current_uid() {
        let config = Config {
            task_root: "/tasks".into(),
            rpc_socket: "/rpc.sock".into(),
            worker_alias: "worker".into(),
            systemd_run: "systemd-run".into(),
            systemctl: "systemctl".into(),
        };
        let command = Systemd::new(&config).user_command(Path::new("systemctl"));
        let env: std::collections::HashMap<_, _> = command
            .as_std()
            .get_envs()
            .filter_map(|(key, value)| Some((key.to_owned(), value?.to_owned())))
            .collect();
        assert_eq!(
            env.get(std::ffi::OsStr::new("XDG_RUNTIME_DIR")),
            Some(&format!("/run/user/{}", current_uid()).into())
        );
    }

    #[tokio::test]
    async fn start_controller_invokes_systemd_run_with_task_isolation() {
        let dir = tempdir().unwrap();
        let task_root = dir.path().join("tasks");
        let task_id = "261110184237.test-task";
        let task_dir = task_root.join(task_id);
        fs::create_dir_all(task_dir.join("workspace/home")).unwrap();
        fs::create_dir_all(task_dir.join("workspace/tmp")).unwrap();
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
            schema_version: 2,
            id: task_id.into(),
            coordinator_session_id: "coordinator".into(),
            worker_session_id: None,
            controller_unit: "bobr-task-261110184237-test-task-controller.service".into(),
            created_unix_ms: 0,
            current_turn: 1,
            request: "test".into(),
        };

        Systemd::new(&config)
            .start_controller(
                &task,
                Path::new("/nix/store/bobr-task/bin/bobr-task"),
                "/run/wrappers/bin:/nix/store/agent/bin",
            )
            .await
            .unwrap();

        let arguments = fs::read_to_string(arguments).unwrap();
        assert!(arguments.contains("--user\n"));
        assert!(arguments.contains("--property=ProtectSystem=strict\n"));
        assert!(arguments.contains("--property=ReadWritePaths="));
        assert!(arguments.contains("--setenv=PATH="));
        assert!(arguments.contains("--setenv=TMPDIR="));
        assert!(
            arguments
                .contains("--setenv=BOBR_TASK_BASE_PATH=/run/wrappers/bin:/nix/store/agent/bin\n")
        );
        assert!(arguments.ends_with("controller\n261110184237.test-task\n"));
    }

    #[tokio::test]
    async fn run_command_creates_one_cancellable_task_unit() {
        let dir = tempdir().unwrap();
        let task_root = dir.path().join("tasks");
        let task_id = "261110184237.test-task";
        let workspace = task_root.join(task_id).join("workspace");
        let working_directory = workspace.join("bobr-recipes");
        fs::create_dir_all(&working_directory).unwrap();
        fs::create_dir_all(workspace.join("home")).unwrap();
        fs::create_dir_all(workspace.join("tmp")).unwrap();
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
            task_root: task_root.clone(),
            rpc_socket: "/rpc.sock".into(),
            worker_alias: "worker".into(),
            systemd_run: fake,
            systemctl: "systemctl".into(),
        };
        let argv = [
            OsString::from("bin/bobr-build.sh"),
            OsString::from("--target"),
            OsString::from("world"),
        ];

        assert!(
            Systemd::new(&config)
                .run_command(
                    task_id,
                    Path::new("/nix/store/bobr-task/bin/bobr-task"),
                    &workspace,
                    &working_directory,
                    "/task/home/.local/bin:/nix/store/agent/bin",
                    &argv,
                )
                .await
                .unwrap()
                .success()
        );

        let arguments = fs::read_to_string(arguments).unwrap();
        for expected in [
            "--wait\n",
            "--pipe\n",
            "--collect\n",
            "--unit=bobr-task-261110184237-test-task-command.service\n",
            "--property=KillSignal=SIGINT\n",
            "--property=KillMode=mixed\n",
            "--property=TimeoutStopSec=30s\n",
            "--property=UMask=0022\n",
            "--setenv=BOBR_TASK_ID=261110184237.test-task\n",
        ] {
            assert!(arguments.contains(expected), "missing {expected:?}");
        }
        assert!(arguments.contains(&format!(
            "--working-directory={}\n",
            working_directory.display()
        )));
        assert!(
            arguments.ends_with(
                "exec\n261110184237.test-task\n--\nbin/bobr-build.sh\n--target\nworld\n"
            )
        );
    }

    #[tokio::test]
    async fn stop_unit_uses_the_user_manager() {
        let dir = tempdir().unwrap();
        let arguments = dir.path().join("arguments");
        let fake = dir.path().join("systemctl");
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
            task_root: dir.path().join("tasks"),
            rpc_socket: "/rpc.sock".into(),
            worker_alias: "worker".into(),
            systemd_run: "systemd-run".into(),
            systemctl: fake,
        };

        Systemd::new(&config)
            .stop_unit("bobr-task-test-command.service")
            .await
            .unwrap();

        assert_eq!(
            fs::read_to_string(arguments).unwrap(),
            "--user\nstop\nbobr-task-test-command.service\n"
        );
    }
}
