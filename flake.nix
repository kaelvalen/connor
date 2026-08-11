{
  description = "connor — Resistance CI/CD: terminal-native pipeline runner";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = { self, nixpkgs }: let
    systems = [ "x86_64-linux" "aarch64-linux" ];
    forAllSystems = f:
      nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});
  in {
    packages = forAllSystems (pkgs:
      pkgs.rustPlatform.buildRustPackage {
        pname = "connor";
        version = "0.3.0";
        src = ./.;
        cargoLock.lockFile = ./Cargo.lock;
        meta = {
          description = "Terminal-native CI/CD pipeline runner — TOML config, DAG-parallel execution, no server";
          homepage = "https://github.com/kaelvalen/connor";
          license = nixpkgs.lib.licenses.mit;
          mainProgram = "connor";
        };
      });

    devShells = forAllSystems (pkgs:
      pkgs.mkShell {
        packages = with pkgs; [ rustc cargo rustfmt clippy ];
      });
  };
}
