# discord-bonfire-bridge

Tools the [4chan Cup](https://implyingrigged.info) community uses while moving
from Discord to **Bonfire**, a self-hosted [Fluxer](https://fluxer.app)
instance (our fork: <https://github.com/the4chancup/bonfire>). Two small
Discord-bot services, one Docker image:

- **`bot.py`** (`bonfire-bot` service): the `/bonfire` slash command. An
  imported member runs it on Discord and gets a temporary Bonfire password,
  looked up in `data/accounts.json` and issued through Bonfire's admin API.
- **`relay.py`** (`discord-relay` service): a transitional one-way
  Discord → Bonfire relay that mirrors new Discord messages into the matching
  Bonfire channels while the community finishes moving.

## Assumptions

- The Bonfire guilds were migrated with **flushcord**, which reuses Discord
  snowflake IDs verbatim, so a Bonfire guild/channel/message has the *same*
  ID as its Discord original, and the relay maps channels by identity.
- `data/accounts.json` maps `discord_id → {fluxer_user_id, username}`.
  Produce it with `python export_accounts.py` from the importer's private
  state file (never commit the result; it links Discord accounts to Bonfire
  accounts).
- The temporary-password endpoint used by `bot.py`
  (`POST /api/admin/users/{id}/temporary-password`) exists only in the
  Bonfire fork, not in stock Fluxer.

## The relay

For each configured guild, new human messages (types 0 and 19) are posted to
the same-ID Bonfire channel through a per-channel webhook named
`Discord relay`, keeping the author's Discord server name and avatar:

- startup and reconnect catch-up per channel (the only thing that advances
  the channel cursor; live events queue behind it);
- Discord edits/deletes are mirrored **only for messages the relay posted**;
  migrated history is never touched by a Discord delete;
- attachments are copied only when the mapped Bonfire member is allowed to
  upload there (`SEND_MESSAGES` + `ATTACH_FILES`, not timed out: Fluxer's own
  rule) and fit `RELAY_MAX_FILE_BYTES` (default 25 MiB); a skipped file
  leaves a note with the reason, e.g. `[1 file not relayed (not a Bonfire user)]`;
- no pings: everything posts with `allowed_mentions: {"parse": [],
  "replied_user": false}`;
- `<@discord_id>` mentions become `<@fluxer_id>` via the accounts map
  (unmapped → `@name`); known custom emoji are kept, unknown become
  `:name:`; stickers → `[sticker: name]`; >4,000 chars split;
- posts are idempotent (Fluxer dedupes a repeated webhook `nonce` for five
  minutes), and edits/deletes of pending work are durable across restarts
  via a SQLite state db (`webhooks`, `messages`, `pending`, `cursors`).

Channels/threads that don't exist on Bonfire are skipped (logged once).

## Configuration

Copy the examples and fill them in (both gitignored):

- `.env` (see `.env.example`): `DISCORD_TOKEN` (shared; compose injects it
  into the relay too) and `BONFIRE_ADMIN_KEY` for `bot.py`. Optional:
  `BONFIRE_URL`, `ACCOUNTS_FILE`.
- `relay.env` (see `relay.env.example`): `BONFIRE_BOT_TOKEN` required (a bot
  application invited with Administrator). Optional overrides: `BONFIRE_URL`,
  `RELAY_GUILD_IDS`, `RELAY_CHANNEL_ALLOWLIST`, `RELAY_DRY_RUN`,
  `ACCOUNTS_FILE`, `STATE_DB`, `RELAY_BACKFILL_SINCE`, `RELAY_MAX_FILE_BYTES`.
  **Keep the trailing newline**: a missing one once glued an appended line
  onto the token and broke auth.

## Running (Docker Compose)

```sh
mkdir -p data relay-state
cp .env.example .env && cp relay.env.example relay.env   # fill in secrets
python export_accounts.py <state.json>                   # -> data/accounts.json
chown 10001:10001 relay-state                            # once, for the image user

docker compose build
docker compose up -d bonfire-bot
docker compose up -d discord-relay     # always name the service
docker compose logs -f discord-relay
```

`RELAY_DRY_RUN=1` in `relay.env` runs a read-only dry run (in-memory state,
nothing posted) that logs per-channel catch-up summaries.

## Offline tests

```sh
python test_relay.py        # pure-function checks, no network
```

## Ending the transition

When Discord is fully retired: `docker compose stop discord-relay`, remove
the `discord-relay` service from `docker-compose.yml`, delete `relay.env` and
`relay-state/`, and delete the relay's bot application on Bonfire. Relayed
messages stay; the `Discord relay` webhooks can be deleted per channel or
left.

## Security notes

Never commit `.env`, `relay.env`, `data/` or `relay-state/`:
`data/accounts.json` is personal data linking Discord to Bonfire accounts,
and the relay state contains live webhook tokens. `accounts.json` carries no
passwords (export strips them).

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <https://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <https://opensource.org/licenses/MIT>)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
