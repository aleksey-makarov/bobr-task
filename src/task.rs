use crate::config::{canonical_task_root, validate_task_id};
use crate::rpc::TurnComplete;
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

const SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TaskRecord {
    pub schema_version: u32,
    pub id: String,
    pub coordinator_session_id: String,
    pub worker_session_id: Option<String>,
    pub controller_unit: String,
    pub created_unix_ms: u128,
    pub request: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TaskResult {
    pub schema_version: u32,
    pub task_id: String,
    pub outcome: String,
    pub content: String,
}

impl TaskResult {
    pub fn from_turn(task_id: &str, turn: TurnComplete) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            task_id: task_id.to_owned(),
            outcome: turn.outcome,
            content: turn.content,
        }
    }

    pub fn failed(task_id: &str, content: String) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            task_id: task_id.to_owned(),
            outcome: "failed".to_owned(),
            content,
        }
    }

    pub fn cancelled(task_id: &str) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            task_id: task_id.to_owned(),
            outcome: "cancelled".to_owned(),
            content: "Cancelled by operator".to_owned(),
        }
    }
}

pub struct TaskStore {
    root: PathBuf,
}

impl TaskStore {
    pub fn new(root: &Path) -> Result<Self> {
        Ok(Self {
            root: canonical_task_root(root)?,
        })
    }

    pub fn create(&self, coordinator_session_id: &str, request: &str) -> Result<TaskRecord> {
        for _ in 0..8 {
            let id = Uuid::new_v4().simple().to_string();
            let task_dir = self.root.join(&id);
            let mut builder = fs::DirBuilder::new();
            builder.mode(0o700);
            match builder.create(&task_dir) {
                Ok(()) => {
                    let mut workspace_builder = fs::DirBuilder::new();
                    workspace_builder.mode(0o700).recursive(true);
                    workspace_builder
                        .create(task_dir.join("workspace/home"))
                        .context("create task workspace")?;
                    let record = TaskRecord {
                        schema_version: SCHEMA_VERSION,
                        id: id.clone(),
                        coordinator_session_id: coordinator_session_id.to_owned(),
                        worker_session_id: None,
                        controller_unit: format!("bobr-task-{id}-controller.service"),
                        created_unix_ms: SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .context("system clock precedes Unix epoch")?
                            .as_millis(),
                        request: request.to_owned(),
                    };
                    self.write_record(&record)?;
                    return Ok(record);
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error).context("create task directory"),
            }
        }
        bail!("failed to allocate a unique task ID")
    }

    pub fn task_dir(&self, task_id: &str) -> Result<PathBuf> {
        validate_task_id(task_id)?;
        let path = self.root.join(task_id);
        let metadata = fs::symlink_metadata(&path)
            .with_context(|| format!("task '{task_id}' does not exist"))?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            bail!("task path '{}' is not a directory", path.display());
        }
        let resolved = fs::canonicalize(&path)?;
        if resolved.parent() != Some(self.root.as_path()) {
            bail!("task path '{}' escapes the task root", path.display());
        }
        Ok(resolved)
    }

    pub fn read_record(&self, task_id: &str) -> Result<TaskRecord> {
        let path = self.task_dir(task_id)?.join("task.json");
        let record: TaskRecord = serde_json::from_slice(
            &fs::read(&path).with_context(|| format!("read '{}'", path.display()))?,
        )?;
        if record.id != task_id || record.schema_version != SCHEMA_VERSION {
            bail!("task metadata does not match task '{task_id}'");
        }
        Ok(record)
    }

    pub fn workspace_dir(&self, task_id: &str) -> Result<PathBuf> {
        let path = self.task_dir(task_id)?.join("workspace");
        let metadata = fs::symlink_metadata(&path)
            .with_context(|| format!("task '{task_id}' has no workspace"))?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            bail!("task workspace '{}' is not a directory", path.display());
        }
        Ok(path)
    }

    pub fn write_record(&self, record: &TaskRecord) -> Result<()> {
        let path = self.task_dir(&record.id)?.join("task.json");
        write_json_atomic(&path, record)
    }

    pub fn read_result(&self, task_id: &str) -> Result<Option<TaskResult>> {
        let path = self.task_dir(task_id)?.join("result.json");
        match fs::read(&path) {
            Ok(data) => {
                let result: TaskResult = serde_json::from_slice(&data)?;
                if result.task_id != task_id || result.schema_version != SCHEMA_VERSION {
                    bail!("task result does not match task '{task_id}'");
                }
                Ok(Some(result))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error).with_context(|| format!("read '{}'", path.display())),
        }
    }

    pub fn write_result(&self, task_id: &str, result: &TaskResult) -> Result<()> {
        let path = self.task_dir(task_id)?.join("result.json");
        write_json_atomic(&path, result)
    }
}

pub fn append_json_line(path: &Path, value: &Value) -> Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(path)
        .with_context(|| format!("open '{}'", path.display()))?;
    serde_json::to_writer(&mut file, value)?;
    file.write_all(b"\n")?;
    file.flush()?;
    Ok(())
}

fn write_json_atomic(path: &Path, value: &impl Serialize) -> Result<()> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .context("JSON path has no UTF-8 file name")?;
    let temporary = path.with_file_name(format!(".{file_name}.{}.tmp", std::process::id()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temporary)
        .with_context(|| format!("create '{}'", temporary.display()))?;
    let data = serde_json::to_vec_pretty(value)?;
    file.write_all(&data)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    drop(file);
    if let Err(error) = fs::rename(&temporary, path) {
        let _ = fs::remove_file(&temporary);
        return Err(error).with_context(|| format!("replace '{}'", path.display()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{MetadataExt, symlink};
    use tempfile::tempdir;

    #[test]
    fn creates_private_task_and_round_trips_metadata() {
        let dir = tempdir().unwrap();
        let store = TaskStore::new(dir.path()).unwrap();
        let record = store.create("coordinator", "request").unwrap();
        let loaded = store.read_record(&record.id).unwrap();
        assert_eq!(loaded.coordinator_session_id, "coordinator");
        assert_eq!(loaded.request, "request");
        assert_eq!(
            fs::metadata(store.task_dir(&record.id).unwrap())
                .unwrap()
                .mode()
                & 0o777,
            0o700
        );
    }

    #[test]
    fn rejects_symlink_task_directories() {
        let dir = tempdir().unwrap();
        let outside = tempdir().unwrap();
        symlink(outside.path(), dir.path().join("bad")).unwrap();
        let store = TaskStore::new(dir.path()).unwrap();
        assert!(store.task_dir("bad").is_err());
    }

    #[test]
    fn result_is_written_atomically() {
        let dir = tempdir().unwrap();
        let store = TaskStore::new(dir.path()).unwrap();
        let record = store.create("coordinator", "request").unwrap();
        store
            .write_result(&record.id, &TaskResult::cancelled(&record.id))
            .unwrap();
        assert_eq!(
            store.read_result(&record.id).unwrap().unwrap().outcome,
            "cancelled"
        );
    }
}
