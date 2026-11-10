# bobr-task

`bobr-task` launches isolated Bobr maintenance tasks from a ZeroClaw
coordinator. Each task receives its own `workspace/` directory and
`bobr_worker` session; controller metadata stays outside that workspace and
the controller runs as a transient user systemd unit.

The initial interface is:

```text
bobr-task start <short-label>
bobr-task status <task-id>
bobr-task result <task-id>
bobr-task cancel <task-id>
```

The coordinator supplies a one- or two-word lowercase label such as
`create-hello`. `start` combines it with local time to create a naturally
sortable task ID such as `261110184237.create-hello`; a same-second collision
gets a numeric suffix. It then reads the current coordinator session ID from
`ZEROCLAW_SESSION_ID`. It retrieves recent conversation messages over the
local ZeroClaw RPC socket, so user text is never interpolated into a shell
command.

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

