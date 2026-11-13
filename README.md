# bobr-task

`bobr-task` launches Bobr maintenance tasks from a ZeroClaw coordinator. Each
task receives its own `workspace/` directory, environment, and `bobr_worker`
session; controller metadata stays outside that workspace and the controller
runs as a transient user systemd unit. Task directories separate mutable
state, not mutually untrusted workers.

The initial interface is:

```text
bobr-task start <short-label> <request-file>
bobr-task list
bobr-task current
bobr-task select <task-id>
bobr-task continue <task-id|current> <request-file>
bobr-task status <task-id|current>
bobr-task result <task-id|current>
bobr-task history <task-id|current>
bobr-task cancel <task-id|current>
bobr-task run -- <command> [args...]
```

The coordinator supplies a one- or two-word lowercase label such as
`create-hello` and a relative path to a UTF-8 request file in its workspace.
`start` combines the label with local time to create a naturally sortable task
ID such as `261110184237.create-hello`; a same-second collision gets a numeric
suffix. It reads the current coordinator session ID from
`ZEROCLAW_SESSION_ID`, copies the explicit request into task metadata, and
never reconstructs a request from conversation history. User text is read
from the file rather than interpolated into a shell command.

`continue` sends a follow-up request to the same worker session in the same
workspace. Completed turns are kept as immutable `result.NNNN.json` files;
`result` returns the current terminal result, while `history` returns all
completed turns in order. Task operations are limited to tasks owned by the
current coordinator dialog.

`start`, an accepted `continue`, and `select` make a task current for that
dialog. `list` shows its tasks by most recent explicit use, and `current`
reports the selection. Commands that take a task accept the literal `current`
instead of an ID. Cancelling the selected task clears the selection without
falling back to an older task; cancelled tasks remain available by exact ID but
cannot be selected again.

The host must provide an explicit `BOBR_TASK_BASE_PATH` containing absolute
directories. A worker receives only its task-local `~/.local/bin` and
`~/.cargo/bin` followed by that base path; it does not inherit the daemon's
ambient `PATH`. Its `HOME` and `TMPDIR` also point inside the task workspace.
The base path must provide `systemd-run` and `systemctl`.

Workers run long-lived builds and tests through `bobr-task run`. It starts one
transient command unit for the current task and relays its output and exit
status. `cancel` sends `SIGINT` to that unit first, waits for cooperative
shutdown, and only then aborts the ZeroClaw session. Systemd uses `SIGKILL`
after 30 seconds if the command does not stop.

This project currently targets the ZeroClaw 0.8.6 local JSON-RPC interface.

## Development

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
```

NixOS builds the same source with [`package.nix`](./package.nix); CI does not
publish a separate binary artifact.

Licensed under either the MIT license or Apache License 2.0.

