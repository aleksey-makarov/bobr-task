# AGENTS.md

`bobr-task` is a narrow host-side adapter between a ZeroClaw coordinator,
isolated Bobr task directories, and transient user systemd units.

Keep these invariants explicit:

- one task owns exactly one directory below the configured task root;
- task IDs are validated before they are used in paths or unit names;
- the coordinator invokes the fixed command `bobr-task start`; arbitrary user
  text never becomes shell syntax;
- ZeroClaw remains the authority for agent sessions and systemd remains the
  authority for processes;
- do not add a daemon, registry, scheduler, compatibility layer, or optional
  backend without a current use case;
- RPC and systemd interactions must be covered by fakes in tests;
- no command may delete a task directory.

