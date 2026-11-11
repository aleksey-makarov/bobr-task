# bobr-task

`bobr-task` launches Bobr maintenance tasks from a ZeroClaw coordinator. Each
task receives its own `workspace/` directory, environment, and `bobr_worker`
session; controller metadata stays outside that workspace and the controller
runs as a transient user systemd unit. Task directories separate mutable
state, not mutually untrusted workers.

The initial interface is:

```text
bobr-task start <short-label> <request-file>
bobr-task status <task-id>
bobr-task result <task-id>
bobr-task cancel <task-id>
```

The coordinator supplies a one- or two-word lowercase label such as
`create-hello` and a relative path to a UTF-8 request file in its workspace.
`start` combines the label with local time to create a naturally sortable task
ID such as `261110184237.create-hello`; a same-second collision gets a numeric
suffix. It reads the current coordinator session ID from
`ZEROCLAW_SESSION_ID`, copies the explicit request into task metadata, and
never reconstructs a request from conversation history. User text is read
from the file rather than interpolated into a shell command.

The host must provide an explicit `BOBR_TASK_BASE_PATH` containing absolute
directories. A worker receives only its task-local `~/.local/bin` and
`~/.cargo/bin` followed by that base path; it does not inherit the daemon's
ambient `PATH`. Its `HOME` and `TMPDIR` also point inside the task workspace.

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

