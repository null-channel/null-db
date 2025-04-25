{ lib, pkgs, config, ... }:

# See https://nixos.org/manual/nixos/stable/#sec-writing-modules
let
  cfg = config.services.null-db;
in

{
  options.programs.null-db = {
    enable = lib.mkEnableOption "null-db" // {
      description = "Whether to install the null-db management cli";
    };
  };

  options.services.null-db = {
    enable = lib.mkEnableOption "null-db" // {
      description = "Enable the only educational database with native htmx support";
    };

    openFirewall = lib.mkOption {
      type = lib.types.bool;
      default = false;
      description = ''
        Whether to expose the api port specified in {option} `config.services.null-db.args.port`
      '';
    };

    raft-port = lib.mkOption {
      type = lib.types.port;
      default = 3030;
      description = "gRPC port used for raft consensus";
    };

    args = lib.mkOption {
      description = "Argv provided to null-db";
      type = lib.types.submodule {
        # freeformType should make this somewhat future-proof if new
        # cli args are added that are not listed here
        freeformType = lib.types.attrsOf (lib.types.oneOf [
          lib.types.bool
          lib.types.str
          lib.types.port
          (lib.types.nullOr (lib.types.listOf lib.types.str))
        ]);

        options = {
          compaction = lib.mkOption {
            type = lib.types.bool;
            default = false;
            description = "Whether to run compaction";
          };

          dir = lib.mkOption {
            type = lib.types.str;
            # See https://www.freedesktop.org/software/systemd/man/latest/systemd.unit.html#Specifiers
            defaultText = lib.literalExpression
              ''"%S/''${config.services.null-db.user}"'';
            description = "Where to store database";
          };

          roster = lib.mkOption {
            type = lib.types.nullOr (lib.types.listOf lib.types.str);
            default = null;
            example = lib.literalExpression ''[ "10.0.0.18:5050" "10.0.0.19:5151" ]'';
            description = ''
              If provided, the peers in raft configuration in hostaddr
              notation, comma separated
            '';
          };

          port = lib.mkOption {
            type = lib.types.port;
            default = 8080;
            description = "Port to listen on";
          };

          id = lib.mkOption {
            type = lib.types.str;
            defaultText = lib.literalExpression
              ''"''${config.networking.fqdnOrHostName}:''${toString config.services.null-db.raft-port}"'';
            description = ''
              The candidate identifier of this node in raft
              configuration, in hostaddr notation
            '';
            example = lib.literalExpression ''"10.0.0.20:4040"'';
          };

          encoding = lib.mkOption {
            type = lib.types.str;
            default = "html";
            description = "Database storage format";
          };
        };
      };
    };

    user = lib.mkOption {
      type = lib.types.str;
      default = "nulldb";
      description = "Service user";
    };

    group = lib.mkOption {
      type = lib.types.str;
      default = "nulldb";
      description = "Service group";
    };
  };

  config = lib.mkMerge [
    {
      # Put defaults here that use `config` in some way - it is a bit
      # of a well-known wart that options referring to config can
      # cause infinite recursion when generating docs or other static
      # information about the available options.
      services.null-db.args = {
        id = lib.mkDefault
          "${config.networking.fqdnOrHostName}:${toString cfg.raft-port}";
        dir = lib.mkDefault "%S/${cfg.user}";
      };
    }

    (lib.mkIf config.programs.null-db.enable {
      environment.defaultPackages = [ pkgs.null-db ];
    })

    (lib.mkIf cfg.enable {
      networking.firewall.allowedTCPPorts = lib.mkMerge [
        (lib.mkIf cfg.openFirewall [ cfg.args.port ])
        (lib.mkIf (cfg.openFirewall && cfg.args.roster != null) [ cfg.raft-port ])
      ];

      users = {
        users.${cfg.user} = {
          isSystemUser = true;
          group = cfg.group;
        };

        groups.${cfg.group} = { };
      };

      systemd.services.null-db = {
        enable = true;
        description = "The only educational database with native htmx support";

        wantedBy = [ "default.target" ];

        wants = [ "network-online.target" ];
        after = [ "network-online.target" ];

        serviceConfig = {
          ExecStart = "${pkgs.null-db}/libexec/null-db ${
            lib.cli.toGNUCommandLineShell {
              mkList = k: v:
                let key = if lib.stringLength k == 1 then "-${k}" else "--${k}"; in
                [ "--${k}" (lib.concatStringsSep "," v) ];
              }
              cfg.args
          }";

          StateDirectory = cfg.user;

          User = cfg.user;
          Group = cfg.group;
        };
      };
    })
  ];
}
