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
