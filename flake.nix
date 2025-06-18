{
  description = "Development environment for kiteticker-async";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, fenix }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
        toolchain = fenix.packages.${system}.stable.toolchain;
      in
        {
          devShells.default = pkgs.mkShell {
            buildInputs = [
              toolchain
              pkgs.rust-analyzer
              pkgs.openssl
              pkgs.pkg-config
            ];
            shellHook = ''
              export OPENSSL_DIR=${pkgs.openssl.dev}
            '';
          };
        });
}
