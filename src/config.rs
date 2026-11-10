use anyhow::{Context, Result, bail};
use std::env;
use std::path::PathBuf;

pub const DEFAULT_TASK_ROOT: &str = "/mnt/zeroclaw/bobr-tasks";
pub const DEFAULT_RPC_SOCKET: &str = "/mnt/zeroclaw/.zeroclaw/data/daemon.sock";
pub const DEFAULT_WORKER_ALIAS: &str = "bobr_worker";

#[derive(Clone, Debug)]
pub struct Config {
    pub task_root: PathBuf,
    pub rpc_socket: PathBuf,
    pub worker_alias: String,
    pub systemd_run: PathBuf,
    pub systemctl: PathBuf,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let task_root = env_path("BOBR_TASK_ROOT", DEFAULT_TASK_ROOT);
        let rpc_socket = env_path("ZEROCLAW_RPC_SOCKET", DEFAULT_RPC_SOCKET);
        if !task_root.is_absolute() {
            bail!("BOBR_TASK_ROOT must be an absolute path");
        }
        if !rpc_socket.is_absolute() {
            bail!("ZEROCLAW_RPC_SOCKET must be an absolute path");
        }
        Ok(Self {
            task_root,
            rpc_socket,
            worker_alias: env::var("BOBR_TASK_WORKER_ALIAS")
                .unwrap_or_else(|_| DEFAULT_WORKER_ALIAS.to_owned()),
            systemd_run: env_path("BOBR_TASK_SYSTEMD_RUN", "systemd-run"),
            systemctl: env_path("BOBR_TASK_SYSTEMCTL", "systemctl"),
        })
    }
}

fn env_path(name: &str, default: &str) -> PathBuf {
    env::var_os(name)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(default))
}

pub fn validate_task_id(task_id: &str) -> Result<()> {
    if task_id.is_empty()
        || task_id.len() > 64
        || !task_id
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        bail!("invalid task ID '{task_id}'");
    }
    Ok(())
}

pub fn current_uid() -> u32 {
    // SAFETY: geteuid has no preconditions and does not dereference pointers.
    unsafe { libc::geteuid() }
}

pub fn canonical_task_root(path: &std::path::Path) -> Result<PathBuf> {
    std::fs::create_dir_all(path)
        .with_context(|| format!("create task root '{}'", path.display()))?;
    std::fs::canonicalize(path).with_context(|| format!("resolve task root '{}'", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_only_path_safe_task_ids() {
        for value in ["abc", "a1-b2", "0123456789abcdef"] {
            validate_task_id(value).unwrap();
        }
        for value in ["", "../x", "A", "a_b", "a/b", "."] {
            assert!(validate_task_id(value).is_err(), "accepted {value:?}");
        }
    }
}
