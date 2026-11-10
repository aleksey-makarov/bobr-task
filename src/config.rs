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
    let Some((timestamp, rest)) = task_id.split_once('.') else {
        bail!("invalid task ID '{task_id}'");
    };
    if timestamp.len() != 12 || !timestamp.bytes().all(|byte| byte.is_ascii_digit()) {
        bail!("invalid task ID '{task_id}'");
    }
    let (label, suffix) = match rest.split_once('.') {
        Some((label, suffix)) => (label, Some(suffix)),
        None => (rest, None),
    };
    validate_task_label(label)?;
    if suffix.is_some_and(|suffix| {
        suffix.is_empty()
            || suffix.starts_with('0')
            || !matches!(suffix.parse::<u32>(), Ok(number) if number >= 2)
    }) {
        bail!("invalid task ID '{task_id}'");
    }
    Ok(())
}

pub fn validate_task_label(label: &str) -> Result<()> {
    let words: Vec<_> = label.split('-').collect();
    if label.is_empty()
        || label.len() > 24
        || !(1..=2).contains(&words.len())
        || words.iter().any(|word| {
            word.is_empty()
                || !word
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        })
    {
        bail!("invalid task label '{label}'; expected one or two lowercase words");
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
    fn accepts_structured_task_ids() {
        for value in [
            "261110184237.release",
            "261110184237.create-hello",
            "261110184237.create-hello.2",
            "261110184237.create-hello.19",
        ] {
            validate_task_id(value).unwrap();
        }
        for value in [
            "",
            "26111018423.create-hello",
            "261110184237",
            "261110184237.Create-hello",
            "261110184237.create_three_words",
            "261110184237.create-three-words",
            "261110184237.create-hello.0",
            "261110184237.create-hello.02",
            "261110184237.create-hello.extra",
            "../261110184237.create-hello",
        ] {
            assert!(validate_task_id(value).is_err(), "accepted {value:?}");
        }
    }

    #[test]
    fn accepts_short_task_labels() {
        for value in ["release", "create-hello", "fix-tls2"] {
            validate_task_label(value).unwrap();
        }
        for value in [
            "",
            "Create-hello",
            "create_hello",
            "create-three-words",
            "this-label-is-much-too-long",
        ] {
            assert!(validate_task_label(value).is_err(), "accepted {value:?}");
        }
    }
}
