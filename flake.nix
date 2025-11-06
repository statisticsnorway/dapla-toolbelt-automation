{
  description = "Provide development environment for dapla-toolbelt-automation";
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
  };

  outputs = inputs:
    inputs.flake-parts.lib.mkFlake {inherit inputs;} {
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
      ];

      perSystem = {
        pkgs,
        self',
        ...
      }: let
        inherit (pkgs) python313Packages;
      in {
        devShells.default = pkgs.mkShell {
          name = "dapla-toolbelt-automation devel";
          packages = [
            pkgs.pre-commit
            (pkgs.python313.withPackages (ps: [ps.pre-commit-hooks]))
            python313Packages.black
            python313Packages.ruff
            pkgs.uv
            self'.packages.darglint
          ];
        };
        formatter = pkgs.alejandra;
        packages.darglint = python313Packages.buildPythonPackage rec {
          pname = "darglint";
          version = "1.8.1";
          src = pkgs.fetchPypi {
            inherit pname version;
            hash = "sha256-CA1RBt8UmxmYIufufeucAStJiRU48UoRvmgQRPC7INo=";
          };
          format = "setuptools";
          doCheck = false;
        };
      };
    };
}
