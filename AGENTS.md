# AGENTS.md

`bobr-task` is a narrow host-side adapter between a ZeroClaw coordinator,
task-local Bobr state, and transient user systemd units.

Keep these invariants explicit:

- one task owns exactly one directory below the configured task root;
- task IDs are validated before they are used in paths or unit names;
- the coordinator invokes `bobr-task start <short-label> <request-file>`; the
  label is its own one- or two-word summary, the request is a regular UTF-8 file
  below the coordinator workspace, and arbitrary user text never becomes shell
  syntax;
- ZeroClaw remains the authority for agent sessions and systemd remains the
  authority for processes;
- worker `HOME`, `TMPDIR`, and `PATH` are constructed explicitly; the base
  `PATH` comes from `BOBR_TASK_BASE_PATH`, never from the daemon's ambient
  environment;
- one task has at most one transient command unit; long-lived commands run
  through `bobr-task run`, and cancellation stops that unit before aborting the
  ZeroClaw session;
- do not add a daemon, registry, scheduler, compatibility layer, or optional
  backend without a current use case;
- RPC and systemd interactions must be covered by fakes in tests;
- no command may delete a task directory.

