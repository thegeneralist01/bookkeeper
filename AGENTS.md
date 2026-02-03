# Repository Guidelines

## NixOS Module Notes

- `services.readlater-bot.settings` is rendered to TOML without the token; the token must come from `services.readlater-bot.tokenFile` and is combined at runtime in `/run/readlater-bot/config.toml` to keep secrets out of the Nix store.
- If you override `services.readlater-bot.user`/`group`, ensure the group exists; otherwise systemd fails at step GROUP. Defaults only auto-create the `readlater-bot` user/group when you keep the defaults.

## Build Checks

- Run `cargo check` after changes (patches, features, or other code edits).
- Create atomic commits after changes.
