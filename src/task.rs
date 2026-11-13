use crate::config::{
    canonical_task_root, validate_session_id, validate_task_id, validate_task_label,
};
use crate::rpc::TurnComplete;
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::ffi::CStr;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const TASK_SCHEMA_VERSION: u32 = 3;
const COORDINATOR_STATE_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TaskRecord {
    pub schema_version: u32,
    pub id: String,
    pub coordinator_session_id: String,
    pub worker_session_id: Option<String>,
    pub controller_unit: String,
    pub created_unix_ms: u128,
    pub last_used_unix_ms: u128,
    pub current_turn: u32,
    pub request: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TaskResult {
    pub schema_version: u32,
    pub task_id: String,
    pub turn: u32,
    pub request: String,
    pub outcome: String,
    pub content: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct CoordinatorState {
    schema_version: u32,
    zeroclaw_session_id: String,
    current_task_id: Option<String>,
}

#[derive(Debug)]
pub struct TaskRecords {
    pub records: Vec<TaskRecord>,
    pub skipped_incompatible: usize,
}

#[derive(Deserialize)]
struct TaskRecordHeader {
    schema_version: u32,
    id: String,
    coordinator_session_id: String,
}

impl TaskResult {
    pub fn from_turn(task: &TaskRecord, turn: TurnComplete) -> Self {
        Self {
            schema_version: TASK_SCHEMA_VERSION,
            task_id: task.id.clone(),
            turn: task.current_turn,
            request: task.request.clone(),
            outcome: turn.outcome,
            content: turn.content,
        }
    }

    pub fn failed(task: &TaskRecord, content: String) -> Self {
        Self {
            schema_version: TASK_SCHEMA_VERSION,
            task_id: task.id.clone(),
            turn: task.current_turn,
            request: task.request.clone(),
            outcome: "failed".to_owned(),
            content,
        }
    }

    pub fn cancelled(task: &TaskRecord) -> Self {
        Self {
            schema_version: TASK_SCHEMA_VERSION,
            task_id: task.id.clone(),
            turn: task.current_turn,
            request: task.request.clone(),
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

    pub fn create(
        &self,
        label: &str,
        coordinator_session_id: &str,
        request: &str,
    ) -> Result<TaskRecord> {
        validate_task_label(label)?;
        let timestamp = local_timestamp(SystemTime::now())?;
        self.create_with_timestamp(label, coordinator_session_id, request, &timestamp)
    }

    fn create_with_timestamp(
        &self,
        label: &str,
        coordinator_session_id: &str,
        request: &str,
        timestamp: &str,
    ) -> Result<TaskRecord> {
        for collision in 1..=999 {
            let id = if collision == 1 {
                format!("{timestamp}.{label}")
            } else {
                format!("{timestamp}.{label}.{collision}")
            };
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
                    workspace_builder
                        .create(task_dir.join("workspace/tmp"))
                        .context("create task temporary directory")?;
                    let now = unix_time_ms()?;
                    let record = TaskRecord {
                        schema_version: TASK_SCHEMA_VERSION,
                        id: id.clone(),
                        coordinator_session_id: coordinator_session_id.to_owned(),
                        worker_session_id: None,
                        controller_unit: format!(
                            "bobr-task-{}-controller.service",
                            runtime_name(&id)
                        ),
                        created_unix_ms: now,
                        last_used_unix_ms: now,
                        current_turn: 1,
                        request: request.to_owned(),
                    };
                    self.write_record(&record)?;
                    return Ok(record);
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error).context("create task directory"),
            }
        }
        bail!("failed to allocate a unique task ID for '{timestamp}.{label}'")
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
        if record.id != task_id || record.schema_version != TASK_SCHEMA_VERSION {
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

    pub fn touch_record(&self, record: &mut TaskRecord) -> Result<()> {
        record.last_used_unix_ms = unix_time_ms()?;
        self.write_record(record)
    }

    pub fn list_records(&self, coordinator_session_id: &str) -> Result<TaskRecords> {
        validate_session_id(coordinator_session_id)?;
        let mut records = Vec::new();
        let mut skipped_incompatible = 0;
        for entry in fs::read_dir(&self.root)
            .with_context(|| format!("read task root '{}'", self.root.display()))?
        {
            let entry = entry?;
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| anyhow::anyhow!("task root contains a non-UTF-8 entry"))?;
            if name == ".coordinators" {
                continue;
            }
            validate_task_id(&name)?;
            let file_type = entry.file_type()?;
            if file_type.is_symlink() || !file_type.is_dir() {
                bail!(
                    "task root entry '{}' is not a directory",
                    entry.path().display()
                );
            }
            let metadata_path = entry.path().join("task.json");
            let data = fs::read(&metadata_path)
                .with_context(|| format!("read '{}'", metadata_path.display()))?;
            let header: TaskRecordHeader = serde_json::from_slice(&data)
                .with_context(|| format!("decode '{}'", metadata_path.display()))?;
            if header.id != name {
                bail!("task metadata does not match task '{name}'");
            }
            if header.coordinator_session_id != coordinator_session_id {
                continue;
            }
            if header.schema_version != TASK_SCHEMA_VERSION {
                skipped_incompatible += 1;
                continue;
            }
            records.push(self.read_record(&name)?);
        }
        records.sort_by(|left, right| {
            right
                .last_used_unix_ms
                .cmp(&left.last_used_unix_ms)
                .then_with(|| right.id.cmp(&left.id))
        });
        Ok(TaskRecords {
            records,
            skipped_incompatible,
        })
    }

    pub fn lock_coordinator(&self, coordinator_session_id: &str) -> Result<CoordinatorLock> {
        let directory = self.coordinator_dir(coordinator_session_id)?;
        let path = directory.join("state.lock");
        Ok(CoordinatorLock {
            file: lock_file(&path)
                .with_context(|| format!("lock coordinator session '{coordinator_session_id}'"))?,
        })
    }

    pub fn read_current(&self, coordinator_session_id: &str) -> Result<Option<String>> {
        let path = self
            .coordinator_dir(coordinator_session_id)?
            .join("state.json");
        let data = match fs::read(&path) {
            Ok(data) => data,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error).with_context(|| format!("read '{}'", path.display())),
        };
        let state: CoordinatorState = serde_json::from_slice(&data)
            .with_context(|| format!("decode '{}'", path.display()))?;
        if state.schema_version != COORDINATOR_STATE_SCHEMA_VERSION
            || state.zeroclaw_session_id != coordinator_session_id
        {
            bail!("coordinator state does not match session '{coordinator_session_id}'");
        }
        if let Some(task_id) = state.current_task_id.as_deref() {
            validate_task_id(task_id)?;
        }
        Ok(state.current_task_id)
    }

    pub fn set_current(
        &self,
        coordinator_session_id: &str,
        current_task_id: Option<&str>,
    ) -> Result<()> {
        if let Some(task_id) = current_task_id {
            validate_task_id(task_id)?;
        }
        let path = self
            .coordinator_dir(coordinator_session_id)?
            .join("state.json");
        write_json_atomic(
            &path,
            &CoordinatorState {
                schema_version: COORDINATOR_STATE_SCHEMA_VERSION,
                zeroclaw_session_id: coordinator_session_id.to_owned(),
                current_task_id: current_task_id.map(ToOwned::to_owned),
            },
        )
    }

    pub fn clear_current_if(&self, coordinator_session_id: &str, task_id: &str) -> Result<bool> {
        if self.read_current(coordinator_session_id)?.as_deref() != Some(task_id) {
            return Ok(false);
        }
        self.set_current(coordinator_session_id, None)?;
        Ok(true)
    }

    fn coordinator_dir(&self, coordinator_session_id: &str) -> Result<PathBuf> {
        validate_session_id(coordinator_session_id)?;
        let coordinators = self.root.join(".coordinators");
        create_private_directory(&coordinators)?;
        let coordinators = fs::canonicalize(&coordinators)?;
        if coordinators.parent() != Some(self.root.as_path()) {
            bail!("coordinator state directory escapes the task root");
        }
        let directory = coordinators.join(coordinator_session_id);
        create_private_directory(&directory)?;
        let directory = fs::canonicalize(&directory)?;
        if directory.parent() != Some(coordinators.as_path()) {
            bail!("coordinator session state escapes its root");
        }
        Ok(directory)
    }

    pub fn read_result(&self, task: &TaskRecord) -> Result<Option<TaskResult>> {
        self.read_turn_result(task, task.current_turn)
    }

    pub fn read_results(&self, task: &TaskRecord) -> Result<Vec<TaskResult>> {
        let mut results = Vec::new();
        for turn in 1..=task.current_turn {
            match self.read_turn_result(task, turn)? {
                Some(result) => results.push(result),
                None if turn == task.current_turn => {}
                None => bail!("task '{}' has no result for turn {turn}", task.id),
            }
        }
        Ok(results)
    }

    fn read_turn_result(&self, task: &TaskRecord, turn: u32) -> Result<Option<TaskResult>> {
        let path = self.result_path(&task.id, turn)?;
        match fs::read(&path) {
            Ok(data) => {
                let result: TaskResult = serde_json::from_slice(&data)?;
                if result.task_id != task.id
                    || result.turn != turn
                    || (result.request != task.request && turn == task.current_turn)
                    || result.schema_version != TASK_SCHEMA_VERSION
                {
                    bail!("task result does not match task '{}' turn {turn}", task.id);
                }
                Ok(Some(result))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error).with_context(|| format!("read '{}'", path.display())),
        }
    }

    pub fn write_result(&self, result: &TaskResult) -> Result<()> {
        let task = self.read_record(&result.task_id)?;
        if result.turn != task.current_turn || result.request != task.request {
            bail!(
                "result does not match current turn {} of task '{}'",
                task.current_turn,
                task.id
            );
        }
        let path = self.result_path(&task.id, result.turn)?;
        write_json_once(&path, result)
    }

    pub fn lock_task(&self, task_id: &str) -> Result<TaskLock> {
        let path = self.task_dir(task_id)?.join("task.lock");
        Ok(TaskLock {
            file: lock_file(&path).with_context(|| format!("lock task '{task_id}'"))?,
        })
    }

    fn result_path(&self, task_id: &str, turn: u32) -> Result<PathBuf> {
        Ok(self
            .task_dir(task_id)?
            .join(format!("result.{turn:04}.json")))
    }
}

pub struct TaskLock {
    file: fs::File,
}

impl Drop for TaskLock {
    fn drop(&mut self) {
        // SAFETY: `self.file` remains open until after this method returns.
        unsafe {
            libc::flock(self.file.as_raw_fd(), libc::LOCK_UN);
        }
    }
}

pub struct CoordinatorLock {
    file: fs::File,
}

impl Drop for CoordinatorLock {
    fn drop(&mut self) {
        // SAFETY: `self.file` remains open until after this method returns.
        unsafe {
            libc::flock(self.file.as_raw_fd(), libc::LOCK_UN);
        }
    }
}

pub fn runtime_name(task_id: &str) -> String {
    task_id.replace('.', "-")
}

pub fn command_unit(task_id: &str) -> String {
    format!("bobr-task-{}-command.service", runtime_name(task_id))
}

pub fn task_label(task_id: &str) -> Result<&str> {
    validate_task_id(task_id)?;
    Ok(task_id.split('.').nth(1).expect("validated task ID"))
}

fn unix_time_ms() -> Result<u128> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock precedes Unix epoch")?
        .as_millis())
}

fn local_timestamp(now: SystemTime) -> Result<String> {
    let seconds: libc::time_t = now
        .duration_since(UNIX_EPOCH)
        .context("system clock precedes Unix epoch")?
        .as_secs()
        .try_into()
        .context("system time does not fit time_t")?;
    let mut local = std::mem::MaybeUninit::<libc::tm>::uninit();
    // SAFETY: `seconds` and `local` are valid pointers for the duration of the
    // call, and localtime_r initializes `local` on success.
    if unsafe { libc::localtime_r(&seconds, local.as_mut_ptr()) }.is_null() {
        bail!("failed to convert system time to local time");
    }
    // SAFETY: localtime_r succeeded and initialized the value above.
    let local = unsafe { local.assume_init() };
    let mut output = [0 as libc::c_char; 13];
    let format = c"%y%m%d%H%M%S";
    // SAFETY: all pointers refer to valid objects and `output` has room for
    // twelve formatted digits plus the terminating NUL byte.
    if unsafe { libc::strftime(output.as_mut_ptr(), output.len(), format.as_ptr(), &local) } != 12 {
        bail!("failed to format local task timestamp");
    }
    // SAFETY: strftime wrote a terminating NUL byte on success.
    Ok(unsafe { CStr::from_ptr(output.as_ptr()) }
        .to_str()
        .context("local task timestamp is not UTF-8")?
        .to_owned())
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

fn create_private_directory(path: &Path) -> Result<()> {
    let mut builder = fs::DirBuilder::new();
    builder.mode(0o700);
    match builder.create(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => {
            return Err(error).with_context(|| format!("create '{}'", path.display()));
        }
    }
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        bail!("'{}' is not a directory", path.display());
    }
    Ok(())
}

fn lock_file(path: &Path) -> Result<fs::File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(path)
        .with_context(|| format!("open '{}'", path.display()))?;
    // SAFETY: `file` owns a valid descriptor for the duration of the lock.
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } != 0 {
        return Err(std::io::Error::last_os_error()).context("acquire file lock");
    }
    Ok(file)
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

fn write_json_once(path: &Path, value: &impl Serialize) -> Result<()> {
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
    let write = (|| -> Result<()> {
        let data = serde_json::to_vec_pretty(value)?;
        file.write_all(&data)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        Ok(())
    })();
    drop(file);
    if let Err(error) = write {
        let _ = fs::remove_file(&temporary);
        return Err(error);
    }
    if let Err(error) = fs::hard_link(&temporary, path) {
        let _ = fs::remove_file(&temporary);
        return Err(error).with_context(|| format!("create immutable '{}'", path.display()));
    }
    fs::remove_file(&temporary).with_context(|| format!("remove '{}'", temporary.display()))?;
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
        let record = store
            .create("create-hello", "coordinator", "request")
            .unwrap();
        assert!(record.id.ends_with(".create-hello"));
        assert!(record.id[..12].bytes().all(|byte| byte.is_ascii_digit()));
        let loaded = store.read_record(&record.id).unwrap();
        assert_eq!(loaded.coordinator_session_id, "coordinator");
        assert_eq!(loaded.request, "request");
        assert_eq!(loaded.last_used_unix_ms, loaded.created_unix_ms);
        assert_eq!(
            fs::metadata(store.task_dir(&record.id).unwrap())
                .unwrap()
                .mode()
                & 0o777,
            0o700
        );
        for directory in ["workspace/home", "workspace/tmp"] {
            assert_eq!(
                fs::metadata(store.task_dir(&record.id).unwrap().join(directory))
                    .unwrap()
                    .mode()
                    & 0o777,
                0o700
            );
        }
    }

    #[test]
    fn rejects_symlink_task_directories() {
        let dir = tempdir().unwrap();
        let outside = tempdir().unwrap();
        let task_id = "261110184237.bad";
        symlink(outside.path(), dir.path().join(task_id)).unwrap();
        let store = TaskStore::new(dir.path()).unwrap();
        assert!(store.task_dir(task_id).is_err());
    }

    #[test]
    fn results_are_numbered_and_immutable() {
        let dir = tempdir().unwrap();
        let store = TaskStore::new(dir.path()).unwrap();
        let record = store
            .create("cancel-task", "coordinator", "request")
            .unwrap();
        let result = TaskResult::cancelled(&record);
        store.write_result(&result).unwrap();
        assert_eq!(
            store.read_result(&record).unwrap().unwrap().outcome,
            "cancelled"
        );
        assert!(
            store
                .task_dir(&record.id)
                .unwrap()
                .join("result.0001.json")
                .is_file()
        );
        assert!(store.write_result(&result).is_err());
    }

    #[test]
    fn reads_completed_turns_in_order() {
        let dir = tempdir().unwrap();
        let store = TaskStore::new(dir.path()).unwrap();
        let mut record = store
            .create("continued-task", "coordinator", "first")
            .unwrap();
        store
            .write_result(&TaskResult::failed(&record, "first result".into()))
            .unwrap();
        record.current_turn = 2;
        record.request = "second".into();
        store.write_record(&record).unwrap();

        assert_eq!(store.read_results(&record).unwrap().len(), 1);
        store
            .write_result(&TaskResult::failed(&record, "second result".into()))
            .unwrap();
        let results = store.read_results(&record).unwrap();
        assert_eq!(
            results.iter().map(|result| result.turn).collect::<Vec<_>>(),
            [1, 2]
        );
    }

    #[test]
    fn adds_a_numeric_suffix_to_same_second_collisions() {
        let dir = tempdir().unwrap();
        let store = TaskStore::new(dir.path()).unwrap();
        let first = store
            .create_with_timestamp("same-task", "coordinator", "one", "261110184237")
            .unwrap();
        let second = store
            .create_with_timestamp("same-task", "coordinator", "two", "261110184237")
            .unwrap();
        assert_eq!(second.id, format!("{}.2", first.id));
    }

    #[test]
    fn formats_local_time_as_twelve_digits() {
        let timestamp = local_timestamp(SystemTime::now()).unwrap();
        assert_eq!(timestamp.len(), 12);
        assert!(timestamp.bytes().all(|byte| byte.is_ascii_digit()));
    }

    #[test]
    fn runtime_names_do_not_expose_task_id_separators() {
        assert_eq!(
            runtime_name("261110184237.create-hello.2"),
            "261110184237-create-hello-2"
        );
        assert_eq!(
            command_unit("261110184237.create-hello.2"),
            "bobr-task-261110184237-create-hello-2-command.service"
        );
        assert_eq!(
            task_label("261110184237.create-hello.2").unwrap(),
            "create-hello"
        );
    }

    #[test]
    fn coordinator_state_is_private_and_clears_only_the_selected_task() {
        let dir = tempdir().unwrap();
        let store = TaskStore::new(dir.path()).unwrap();
        let session_id = "telegram_bobr_1_aleksei";
        assert_eq!(store.read_current(session_id).unwrap(), None);

        store
            .set_current(session_id, Some("261110184237.first"))
            .unwrap();
        assert_eq!(
            store.read_current(session_id).unwrap().as_deref(),
            Some("261110184237.first")
        );
        assert!(
            !store
                .clear_current_if(session_id, "261110184238.second")
                .unwrap()
        );
        assert!(
            store
                .clear_current_if(session_id, "261110184237.first")
                .unwrap()
        );
        assert_eq!(store.read_current(session_id).unwrap(), None);

        let state_dir = dir.path().join(".coordinators").join(session_id);
        assert_eq!(fs::metadata(&state_dir).unwrap().mode() & 0o777, 0o700);
        assert_eq!(
            fs::metadata(state_dir.join("state.json")).unwrap().mode() & 0o777,
            0o600
        );
    }

    #[test]
    fn lists_owned_current_schema_tasks_by_last_use() {
        let dir = tempdir().unwrap();
        let store = TaskStore::new(dir.path()).unwrap();
        let mut older = store
            .create_with_timestamp("older", "coordinator", "one", "261110184237")
            .unwrap();
        older.last_used_unix_ms = 10;
        store.write_record(&older).unwrap();
        let mut newer = store
            .create_with_timestamp("newer", "coordinator", "two", "261110184238")
            .unwrap();
        newer.last_used_unix_ms = 20;
        store.write_record(&newer).unwrap();
        let mut same_time = store
            .create_with_timestamp("same-time", "coordinator", "tie", "261110184241")
            .unwrap();
        same_time.last_used_unix_ms = 20;
        store.write_record(&same_time).unwrap();
        store
            .create_with_timestamp("foreign", "other", "three", "261110184239")
            .unwrap();
        let mut incompatible = store
            .create_with_timestamp("old-schema", "coordinator", "four", "261110184240")
            .unwrap();
        incompatible.schema_version = 2;
        store.write_record(&incompatible).unwrap();

        let listed = store.list_records("coordinator").unwrap();
        assert_eq!(listed.skipped_incompatible, 1);
        assert_eq!(
            listed
                .records
                .iter()
                .map(|record| record.id.as_str())
                .collect::<Vec<_>>(),
            [same_time.id.as_str(), newer.id.as_str(), older.id.as_str()]
        );
    }
}
