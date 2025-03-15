{
  description = "Rust with OpenSSL and Musl using Nix Flakes";

  inputs = {
    nixpkgs.url = github:NixOS/nixpkgs/nixpkgs-unstable;

    naersk = {
      url = github:nmattia/naersk;
      inputs.nixpkgs.follows = "nixpkgs";
    };

    fenix = {
      url = github:nix-community/fenix;
      inputs.nixpkgs.follows = "nixpkgs";
    };

    flake-utils.url = github:numtide/flake-utils;
  };

  outputs = { self, nixpkgs, naersk, fenix, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system: let
      pkgs = nixpkgs.legacyPackages.${system};

      # Define Rust toolchain with musl target
      toolchain = with fenix.packages.${system};
        combine [
          minimal.rustc
          minimal.cargo
          targets.x86_64-unknown-linux-musl.latest.rust-std
        ];

      # Use naersk with the custom Rust toolchain
      naersk-lib = naersk.lib.${system}.override {
        cargo = toolchain;
        rustc = toolchain;
      };

      # OpenSSL with Musl support
      openssl = pkgs.openssl;
      opensslStatic = pkgs.pkgsStatic.openssl;
    in rec {
      # Default package (builds Rust project with musl)
      defaultPackage = packages.x86_64-unknown-linux-musl;

      packages.x86_64-unknown-linux-musl = naersk-lib.buildPackage {
        src = ./.;

        nativeBuildInputs = with pkgs; [
          pkgsStatic.stdenv.cc  # Required for musl builds
          openssl.dev           # OpenSSL headers
          pkg-config            # Helps Rust find OpenSSL
        ];

        buildInputs = [ opensslStatic ];

        # Environment variables to help Rust find OpenSSL
        CARGO_BUILD_TARGET = "x86_64-unknown-linux-musl";
        CARGO_BUILD_RUSTFLAGS = "-C target-feature=+crt-static";
        OPENSSL_DIR = opensslStatic;
        OPENSSL_LIB_DIR = "${opensslStatic.out}/lib";
        OPENSSL_INCLUDE_DIR = "${opensslStatic.dev}/include";

        # Ensure pkg-config can locate OpenSSL
        PKG_CONFIG_PATH = "${opensslStatic.dev}/lib/pkgconfig";

        doCheck = true;
      };

      # DevShell for interactive development
      devShells.default = pkgs.mkShell {
        packages = with pkgs; [
          openssl
        ];

        shellHook = ''
          export OPENSSL_DIR="${opensslStatic}"
          export OPENSSL_LIB_DIR="${opensslStatic.out}/lib"
          export OPENSSL_INCLUDE_DIR="${opensslStatic.dev}/include"
          export PKG_CONFIG_PATH="${opensslStatic.dev}/lib/pkgconfig"
          export PS1="(rust-musl-openssl) $PS1"
        '';
      };
    });
}

