# AGENTS.md

This file is the operating guide for coding agents working in this repository.

## Project Summary

- Language: Rust (edition 2021).
- Binary crate: `readlater-bot`.
- Main domain: Telegram bot for managing Read Later / Finished entries in markdown files.
- Packaging/deployment: Nix flake + NixOS module (`flake.nix`).
- Runtime model: async bot handlers + serialized filesystem writes + retry queue.

## Rule Sources Checked

- `.cursor/rules/`: not present.
- `.cursorrules`: not present.
- `.github/copilot-instructions.md`: not present.
- Therefore, this `AGENTS.md` is the canonical agent guide.

## Repository Layout (Current)

- `src/main.rs`: app wiring, shared types/state, op-application flow.
- `src/message_handlers.rs`: Telegram message and slash-command handling.
- `src/callback_handlers.rs`: callback query handling.
- `src/helpers.rs`: rendering, parsing, utility helpers, retry helpers.
- `src/integrations.rs`: git/sync/yt-dlp/python integration helpers.
- `src/tests.rs`: unit tests.
- `flake.nix`: package and NixOS module definitions.

## Required Build/Test Commands

Run these after any meaningful Rust change:

1. `cargo check`
2. `cargo test`

These are mandatory project checks for agents.

## Lint/Format Commands

Use these when touching multiple files or refactoring:

- Format: `cargo fmt --all`
- Format check: `cargo fmt --all -- --check`
- Lint: `cargo clippy --all-targets --all-features -- -D warnings`

Notes:

- `clippy` may be slower; run at least before opening PRs or large commits.
- If a lint is noisy but valid, prefer code fix over allow attributes.

## Test Commands (Including Single Test)

Full suite:

- `cargo test`

Run one test by name substring:

- `cargo test quick_select_index_supports_top_bottom_random`
- `cargo test normalize_markdown_links`

Run exact test path:

- `cargo test tests::quick_select_index_supports_top_bottom_random`

Show logs/output for a test:

- `cargo test tests::quick_select_index_supports_top_bottom_random -- --nocapture`

Useful for iterative work:

- `cargo test -- --test-threads=1` (if debugging order-sensitive behavior)

## Nix/Packaging Commands

- Flake checks: `nix flake check`
- Build package output: `nix build .#default`
- Evaluate all systems (optional): `nix flake check --all-systems`

If prompted, pass `--accept-flake-config` when trusting flake cache settings.

## Coding Style: Rust

### Formatting and Structure

- Follow `rustfmt` defaults; do not hand-format against rustfmt.
- Keep functions focused; extract helper functions instead of deeply nested branches.
- Prefer early returns (`let Some(x) = ... else { ... };`) for control flow clarity.
- Keep lock scope (`Mutex`) as short as practical.

### Imports

- Group imports in this order:
  1. `std`
  2. external crates
  3. internal modules (`crate::...` / `super::...`)
- Avoid unused imports; remove during edits.
- Avoid broad/glob imports in new modules unless already established pattern.

### Naming

- Types/enums/traits: `UpperCamelCase`.
- Functions/variables/modules: `snake_case`.
- Constants: `SCREAMING_SNAKE_CASE`.
- Use domain terms consistently (`entry`, `session`, `peeked`, `quick_seen`, `undo`).

### Types and Data Modeling

- Prefer explicit domain types already in code (`EntryBlock`, `ListSession`, `QueuedOp`).
- Use `Path`/`PathBuf` for filesystem paths, not raw strings.
- Use `Option<T>` for nullable state; avoid sentinel values.
- Keep enums exhaustive and explicit for state machines.

### Error Handling

- Use `anyhow::Result` for fallible functions.
- Add context with `.context(...)` / `.with_context(...)` around I/O/process failures.
- Do not use `unwrap`/`expect` in production code paths.
- `unwrap` is acceptable in tests when setup failures should panic.
- On user-facing failures, send concise Telegram feedback and keep logs actionable.

### Logging

- Prefer minimal, useful logs (`error!`) for failure points.
- Avoid noisy info/debug logs unless they materially help operations.

### Async and Concurrency

- Use `tokio::spawn`/`spawn_blocking` appropriately for blocking tasks.
- Never hold async mutex guards longer than needed.
- Preserve serialized write behavior via existing write lock and queue pattern.

## Bot UX Conventions to Preserve

- Ephemeral acknowledgments use `send_ephemeral(...)` and auto-delete.
- Non-ephemeral informational/error messages should include delete-button UX helpers.
- Keep `/help` command list aligned with implemented commands.
- For commands with aliases, keep behavior consistent (for example `/bottom` and `/last`).

## Data and File Safety

- Preserve markdown entry boundary behavior and preamble handling.
- Keep line-ending normalization and trailing newline behavior.
- Use existing atomic write helpers; do not replace with direct unsafe writes.
- Preserve dedupe semantics (exact full-block match).

## NixOS Module Notes (Important)

- `services.readlater-bot.settings` is rendered to TOML without token.
- Token must come from `services.readlater-bot.tokenFile`.
- Runtime config is assembled at `/run/readlater-bot/config.toml`.
- This avoids writing secrets to the Nix store.
- If overriding `services.readlater-bot.user`/`group`, ensure group exists.
- Defaults only auto-create `readlater-bot` user/group when defaults are kept.

## Git and Commit Hygiene for Agents

- Run `cargo check` after edits; run `cargo test` when behavior changes.
- Keep commits atomic and scoped to one logical change.
- Do not commit secrets (tokens, API keys, credentials, local env files).
- Do not include local scratch directories/files (for example `.sync-x-work/`).

## Change Checklist (Before Finalizing)

- Code compiles: `cargo check`.
- Tests pass: `cargo test` (or targeted tests + rationale).
- Formatting/lint considered for non-trivial edits.
- `/help` text updated if command surface changed.
- No secrets or unrelated local artifacts included.
