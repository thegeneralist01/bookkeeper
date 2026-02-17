# Read Later Bot

## Configuration

The bot reads a TOML config passed via `--config`. Most values are standard TOML types. The `user_id` field accepts multiple forms so it can be sourced from secrets managers.

### `user_id`

You can provide the Telegram user ID as:

- A number
- A numeric string
- A file path containing the numeric ID (useful for age/sops)
- An explicit file object

Examples:

```toml
user_id = 123456789
```

```toml
user_id = "123456789"
```

```toml
user_id = "/run/agenix/readlater-user-id"
```

```toml
user_id = { file = "/run/agenix/readlater-user-id" }
```

### `sync_x`

`/sync_x` imports X/Twitter bookmarks into Read Later.

- The bot prompts for the Cloudflare cookie header string (`auth_token` + `ct0`).
- It runs `isolate_cookies.py`, then `main.py --mode a`.
- Extracted URLs are prepended to Read Later.
- Temporary `creds.txt` / `bookmarks.txt` files are removed after import.

Config example:

```toml
[sync_x]
source_project_path = "/Users/thegeneralist/personal/bookkeeper/vendor/extract-x-bookmarks"
work_dir = "/var/lib/readlater-bot/sync-x"
python_bin = "/Users/thegeneralist/personal/extract-x-bookmarks/.venv/bin/python"
```
