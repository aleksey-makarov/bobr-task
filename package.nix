{
  lib,
  rustPlatform,
}:

rustPlatform.buildRustPackage {
  pname = "bobr-task";
  version = "0.1.0";

  src = lib.cleanSourceWith {
    src = ./.;
    filter = path: type: type != "directory" || baseNameOf path != "target";
  };

  cargoLock.lockFile = ./Cargo.lock;

  meta = {
    description = "Task launcher connecting ZeroClaw workers to task-local Bobr workspaces";
    homepage = "https://github.com/aleksey-makarov/bobr-task";
    license = with lib.licenses; [
      asl20
      mit
    ];
    mainProgram = "bobr-task";
    platforms = lib.platforms.linux;
  };
}
