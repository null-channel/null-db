{ lib, nix-gitignore, rustPlatform, openssl, pkg-config, protobuf }:

let
  version = "0.1.0";
in

rustPlatform.buildRustPackage {
  name = "null-db-${version}";
  inherit version;
  src = nix-gitignore.gitignoreSourcePure [
    ''
      nix/*
      .git/*
    ''
    # I think this is a workaround for a bug in gitignoreSourcePure
    ../.gitignore
  ] ../.;

  cargoHash = "sha256-J5YJhxqOwub/k17RYsIhu5iZSz5AUmOSeEBWLRcU7Ck=";

  # disk_io uses some unstable feature for some reason - workaround
  # here by pretending to be compiling rustc
  RUSTC_BOOTSTRAP = 1;

  nativeBuildInputs = [ pkg-config protobuf ];
  buildInputs = [ openssl ];

  cargoBuildHook = "cd database";

  # Don't compile atm
  doCheck = false;

  postInstall = ''
    mkdir -p $out/libexec
    mv $out/bin/null-db $out/libexec
  '';
}
