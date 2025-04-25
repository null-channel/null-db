{ system ? builtins.currentSystem }:

let
  rev = "2d2a9ddbe3f2c00747398f3dc9b05f7f2ebb0f53";

  nixpkgs = builtins.fetchTarball {
    url = "https://github.com/NixOS/nixpkgs/archive/${rev}.tar.gz";
    sha256 = "sha256-B5WRZYsRlJgwVHIV6DvidFN7VX7Fg9uuwkRW9Ha8z+w=";
  };
in

import nixpkgs {
  inherit system;
  # If `config` is not provided, things in $HOME or other places get
  # picked up, causing nondeterminism :(
  #
  # This is an unfortunate historical wart that might even eventually
  # get fixed.
  config = { };
  overlays = [ (import ./nix/overlay.nix) ];
}
