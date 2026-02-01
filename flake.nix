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
        in
        {
          options.services.readlater-bot = {
            enable = lib.mkEnableOption "Read Later Telegram bot";
            package = lib.mkOption {
              type = lib.types.package;
              default = self.packages.${pkgs.system}.default;
              description = "Package providing the bot binary.";
            };
            configFile = lib.mkOption {
              type = lib.types.path;
              description = "Path to TOML config file.";
            };
          };

          config = lib.mkIf cfg.enable {
            systemd.services.readlater-bot = {
              description = "Read Later Telegram bot";
              wantedBy = [ "multi-user.target" ];
              after = [ "network-online.target" ];
              wants = [ "network-online.target" ];
              serviceConfig = {
                ExecStart = "${cfg.package}/bin/readlater-bot --config ${cfg.configFile}";
                Restart = "on-failure";
                RestartSec = 5;
              };
            };
          };
        };
    };
}
