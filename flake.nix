{
  description = "Read Later Telegram bot";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
    }:
    let
      packageFor =
        system:
        let
          pkgs = import nixpkgs { inherit system; };
        in
        pkgs.rustPlatform.buildRustPackage {
          pname = "readlater-bot";
          version = "0.1.0";
          src = self;
          cargoLock.lockFile = ./Cargo.lock;
        };
    in
    flake-utils.lib.eachDefaultSystem (system: {
      packages.default = packageFor system;
    })
    // {
      nixosModules.default =
        {
          config,
          lib,
          pkgs,
          ...
        }:
        let
          cfg = config.services.readlater-bot;
          tomlFormat = pkgs.formats.toml { };
          defaultSettings = {
            data_dir = "/var/lib/readlater-bot";
            retry_interval_seconds = 30;
          };
          mergedSettings = defaultSettings // cfg.settings;
          settingsFile = tomlFormat.generate "readlater-bot.toml" mergedSettings;
          runtimeConfig = "/run/readlater-bot/config.toml";
          useRuntimeConfig = cfg.configFile == null;
          configPath = if useRuntimeConfig then runtimeConfig else cfg.configFile;
        in
        {
          options.services.readlater-bot = {
            enable = lib.mkEnableOption "Read Later Telegram bot";
            package = lib.mkOption {
              type = lib.types.package;
              default = self.packages.${pkgs.system}.default;
              description = "Package providing the bot binary.";
            };
            settings = lib.mkOption {
              type = tomlFormat.type;
              default = { };
              description = "TOML settings without the token.";
            };
            tokenFile = lib.mkOption {
              type = lib.types.nullOr lib.types.str;
              default = null;
              description = "Path to a file containing the Telegram bot token.";
            };
            configFile = lib.mkOption {
              type = lib.types.nullOr lib.types.str;
              default = null;
              description = "Path to a TOML config file (bypasses settings/tokenFile).";
            };
            user = lib.mkOption {
              type = lib.types.str;
              default = "readlater-bot";
              description = "User account for the bot service.";
            };
            group = lib.mkOption {
              type = lib.types.str;
              default = "readlater-bot";
              description = "Group for the bot service.";
            };
          };

          config = lib.mkIf cfg.enable {
            assertions = [
              {
                assertion = cfg.configFile != null || cfg.tokenFile != null;
                message = "services.readlater-bot: set tokenFile with settings, or provide configFile.";
              }
              {
                assertion = !(cfg.settings ? token);
                message = "services.readlater-bot: do not set settings.token; use tokenFile.";
              }
              {
                assertion = cfg.configFile == null || (cfg.settings == { } && cfg.tokenFile == null);
                message = "services.readlater-bot: when configFile is set, do not set settings or tokenFile.";
              }
            ];

            users.users = lib.mkIf (cfg.user == "readlater-bot") {
              readlater-bot = {
                isSystemUser = true;
                group = cfg.group;
              };
            };
            users.groups = lib.mkIf (cfg.group == "readlater-bot") {
              readlater-bot = { };
            };

            systemd.services.readlater-bot = {
              description = "Read Later Telegram bot";
              wantedBy = [ "multi-user.target" ];
              after = [ "network-online.target" ];
              wants = [ "network-online.target" ];
              preStart = lib.optionalString useRuntimeConfig ''
                umask 0077
                {
                  printf 'token = "%s"\n' "$(cat ${cfg.tokenFile})"
                  cat ${settingsFile}
                } > ${runtimeConfig}
              '';
              serviceConfig = {
                ExecStart = "${cfg.package}/bin/readlater-bot --config ${configPath}";
                Restart = "on-failure";
                RestartSec = 5;
                User = cfg.user;
                Group = cfg.group;
                RuntimeDirectory = "readlater-bot";
                RuntimeDirectoryMode = "0700";
                StateDirectory = "readlater-bot";
                StateDirectoryMode = "0700";
              };
            };
          };
        };
    };
}
