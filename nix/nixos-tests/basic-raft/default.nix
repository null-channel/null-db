{ testers
, lib
  # Adjust for more nodes
, nPeers ? 3
}:
# see also:
# https://nix.dev/tutorials/nixos/integration-testing-using-virtual-machines.html
let
  peers = lib.genList (n: "peer${toString n}") nPeers;

  raft-port = 3000;
in
testers.runNixOSTest {
  name = "null-db-basic-raft";

  nodes = lib.genAttrs peers (_self: { });

  defaults = { pkgs, ... }: {
    imports = [ ../../module.nix ];

    config = {
      # Test-specific vm configuration
      virtualisation = {
        # Default is 1 - might need more for our workers
        cores = 2;
        memorySize = 512;
        # If you want to see ttys run the same stuff as stdout/stderr,
        # enable this
        graphics = false;
      };

      # Interactive debugging
      environment.defaultPackages = [ pkgs.curl ];

      programs.null-db.enable = true;

      services.null-db = {
        enable = true;
        openFirewall = true;
        args.encoding = "json";
        args.roster = lib.map (n: "${n}:${toString raft-port}") peers;
        inherit raft-port;
      };

      # If you want verbose logging, you might break a bit of an
      # abstraction boundary:
      # systemd.services.null-db.serviceConfig.Environment =
      #   "RUST_LOG=info";
    };
  };

  testScript = { nodes }: ''
    start_all()

    for peer in machines:
      peer.wait_for_unit("null-db.service")

    puts = [peer.succeed("cli put foo bar") for peer in machines]

    assert all("I'm not the leader" not in stdout for stdout in puts), f"puts: {puts}"
  '';
}
