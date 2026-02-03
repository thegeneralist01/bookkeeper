Scope
- Build a Telegram bot (Rust) packaged via Nix flake with a NixOS module and a package.
- Manage two Obsidian markdown files: /Users/thegeneralist/obsidian/10 Read Later.md and /Users/thegeneralist/obsidian/20 Finished Reading.md.
- Single-user bot (Telegram user ID 5311210922).
- No tags, no URL parsing, treat all inputs as raw text.

Files and entry boundaries
- Entries are defined by lines beginning with - at column 1.
- An entry starts at a - line and continues until the next - line or EOF.
- Any text before the first - line is a preamble header and must be preserved unchanged.
- New entries must be written with - prefix.
- Multi-line entries are allowed; subsequent lines are written as-is (no indentation).
- Line endings normalized to LF (\n), ensure a trailing newline at EOF.
- UTF-8 encoding.

Read Later behavior
- New items are prepended (inserted immediately after any preamble).
- Deduping: exact full-block match (entire entry text). If identical block exists, skip add and inform user.
- Add acknowledgment: send Saved. and auto-delete after 5s.
- After single-item or multi-item saves, delete the user's original message.

Resources behavior
- /add <text> prompts for Reading list vs Resource.
- Add Resource is available in the selected item view; it does not change the current view.
- Resource adds prompt for a target .md file in resources_path (or a new filename).
- New resource entry is prepended to the chosen file as: `- (Auto-Resource): <message contents>`.
- Preserve additional lines after the first.
- Deduping: exact full-block match (entire entry text). If identical block exists, skip add and inform user.
- Resource acknowledgment: send Added to resources. and auto-delete after 5s.

Finished Reading behavior
- Mark Finished moves an entry: remove from Read Later, prepend to Finished (no separators).
- Acknowledge with Moved and auto-delete after 5s.
- Undo window: 30 minutes, persists across restarts.

Delete behavior
- Delete requires two sequential confirmations via inline buttons.
- Confirmation buttons expire after 5 minutes.
- After delete, offer Undo for 30 minutes.

Listing /list UX
- /list command required; also provide /start and /help.
- Initial chooser shows Top/Bottom/Random with counts (Top/Bottom buttons include count).
- Top/Bottom peek shows 3 entries at a time with previews (two-line preview).
- Under peek: buttons 1, 2, 3 to select an item, plus Prev, Next, Back, Random.
- Selecting an item shows full entry text with actions: Mark Finished, Delete, Back.
- Back from selected item returns to the same peek view.
- Random picks from all Read Later entries, avoids repeats per /list session.

Multi-item messages
- If the incoming message contains the delimiter token ---, treat it as a multi-item input.
- Split by the token wherever it appears (not just on a line by itself).
- Show an interactive picker that lets you choose which items to add; include Add selected and Cancel.

Errors and retries
- On write failure: retry 3 times immediately.
- If still failing: enqueue the operation on disk and notify user.
- Background retry interval: 30 seconds.
- Error messages are not auto-deleted.

State storage
- No hidden markers and no sidecar index.
- A configurable data_dir stores queue and undo logs.

Security
- Ignore messages from unauthorized users silently.
- Auth by Telegram user ID.

Config
- TOML config file, path provided via NixOS module option.
- Required TOML fields:
  - token (Telegram bot token)
  - user_id (Telegram user ID)
  - read_later_path (absolute path)
  - finished_path (absolute path)
  - resources_path (absolute path to resources directory)
  - data_dir (absolute path)
  - retry_interval_seconds (default 30, configurable)

Implementation
- Rust bot (teloxide or equivalent).
- Full-file rewrite on changes, with atomic write (write temp + rename).
- Serialize writes (single queue).
- Minimal logging: errors and counters only.

Nix flake outputs
- packages.<system>.default builds the bot binary.
- nixosModules.default provides the NixOS service with configFile option.
- Service runs as a systemd unit; uses the TOML config path.
