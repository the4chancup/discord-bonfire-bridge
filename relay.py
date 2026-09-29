# SPDX-License-Identifier: MIT OR Apache-2.0
"""Discord -> Bonfire transitional relay.

New Discord messages in the migrated guilds are posted to the matching Bonfire
channel through a per-channel webhook, keeping the author's Discord guild name
and avatar. Edits and deletes are mirrored only for messages this relay posted.

Per channel a single asyncio worker drains a FIFO queue: creating a worker puts
("catchup") first so live events always wait for backfill; a catch-up is also
enqueued on every gateway session (reconnect = downtime recovery). Catch-up
retries in place on failure and is the only thing that advances the channel
cursor — live-relayed messages are deduped by the state DB at the next catch-up.
The state DB records the posted text so no-op Discord edits (link unfurls)
don't mark a relayed message "(edited)".

Edits and deletes are durable: a `pending` row is inserted before the attempt
and removed only after Bonfire confirms; transient failures reschedule with
exponential backoff (60 s doubling, 10 min cap, unbounded), and every catch-up
first drains the channel's pending rows.

Failures are classified by is_transient(): network errors, 408, 429 and 5xx
retry; other 4xx are permanent (logged and skipped). Permission data that
can't be read is unresolved, not unrestricted: it raises. Posts are idempotent
via a deterministic nonce per chunk (Fluxer dedupes webhook executes by
(webhook, nonce) for 5 minutes: MessageSendService.ts ~1100,
MessageHelpers.ts MESSAGE_NONCE_TTL).

RELAY_DRY_RUN=1: read-only, in-memory state DB. Runs every read path (Bonfire
existence set, member roles/overwrites upload check, emoji list, translation)
but skips downloads, webhook creation and all writes; prints per-channel
backfill counts.

Env: DISCORD_TOKEN, BONFIRE_BOT_TOKEN, BONFIRE_URL, RELAY_GUILD_IDS,
RELAY_CHANNEL_ALLOWLIST, RELAY_DRY_RUN, ACCOUNTS_FILE, STATE_DB,
RELAY_BACKFILL_SINCE, RELAY_MAX_FILE_BYTES. Flag: --discord-only-probe.

Never logs secrets: no tokens, no full paths (safe_path masks webhook tokens),
no exception text (aiohttp errors can carry the request URL).
"""

import asyncio
import json
import logging
import os
import re
import sqlite3
import sys
import time
import zlib
from datetime import datetime

import aiohttp
import discord

log = logging.getLogger("bonfire-relay")


def _exc_name(e):
    """Exception summary safe to log: class name and HTTP status only."""
    name = type(e).__name__
    status = getattr(e, "status", None)
    return f"{name}(status={status})" if isinstance(status, int) else name


DEFAULT_BONFIRE_URL = "https://bonfire.implyingrigged.info"
DEFAULT_RELAY_GUILD_IDS = "124029625755107328,288816233015410698,365557463430463488"
DEFAULT_ACCOUNTS_FILE = "/data/accounts.json"
DEFAULT_STATE_DB = "/data/relay.db"
DEFAULT_BACKFILL_SINCE = "2026-09-27T00:00:00Z"
DEFAULT_MAX_FILE_BYTES = 25 * 1024 * 1024
WEBHOOK_NAME = "Discord relay"
MAX_ATTACHMENTS_PER_MESSAGE = 10
CONTENT_LIMIT = 4000
CACHE_TTL = 300  # 5 minutes
RETRY_SECONDS = 60
RETRY_MAX_SECONDS = 600
DISCORD_EPOCH_MS = 1420070400000
VIEW_CHANNEL = 1 << 10
SEND_MESSAGES = 1 << 11
ATTACH_FILES = 1 << 15
ADMINISTRATOR = 1 << 3
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=60)

MENTION_USER_RE = re.compile(r"<@!?(\d+)>")
EMOJI_RE = re.compile(r"<(a?):([A-Za-z0-9_]+):(\d+)>")
WEBHOOK_TOKEN_RE = re.compile(r"(/webhooks/\d+/)[^/?]+")


class TransientError(Exception):
    """Bonfire side didn't answer cleanly; the operation is worth retrying."""


# ---------- pure, tested functions ----------

def translate_content(content, account_map, known_emoji_ids, name_for_unmapped):
    """Rewrite Discord mentions/emoji for Bonfire.
    <@id>/<@!id> -> <@fluxer_id> if mapped, else @<display name> via
    name_for_unmapped(discord_id). Channel and role mentions stay as-is.
    <a?:name:id> kept only when the (numeric) id is a known Bonfire emoji id,
    else :name:."""
    def user_repl(m):
        did = m.group(1)
        fluxer_id = account_map.get(did)
        if fluxer_id is not None:
            return f"<@{fluxer_id}>"
        return "@" + (name_for_unmapped(did) or did)

    def emoji_repl(m):
        if int(m.group(3)) in known_emoji_ids:
            return m.group(0)
        return f":{m.group(2)}:"

    return EMOJI_RE.sub(emoji_repl, MENTION_USER_RE.sub(user_repl, content))


def split_content(text, limit=CONTENT_LIMIT):
    """Split on newline, then space, then hard cut. No empty chunks."""
    chunks = []
    remaining = text
    while len(remaining) > limit:
        cut = remaining.rfind("\n", 0, limit + 1)
        if cut <= 0:
            cut = remaining.rfind(" ", 0, limit + 1)
        if cut <= 0:
            cut = limit
            chunks.append(remaining[:cut])
            remaining = remaining[cut:]
        else:
            chunks.append(remaining[:cut])
            remaining = remaining[cut + 1:]
    if remaining:
        chunks.append(remaining)
    return chunks


def effective_permissions(guild_id, member_role_ids, member_user_id, roles_by_id,
                          channel_overwrites, owner_id=None):
    """Discord effective-permission algorithm.
    roles_by_id: {role_id: {"permissions": str|int}}
    channel_overwrites: list of {id, type (0=role, 1=member), allow, deny}
    with allow/deny as decimal strings (Fluxer PermissionStringType).
    The guild owner and Administrators get every permission."""
    if owner_id is not None and int(member_user_id) == int(owner_id):
        return (1 << 64) - 1
    roles_by_id = {int(k): v for k, v in roles_by_id.items()}
    base = int(roles_by_id.get(int(guild_id), {}).get("permissions", 0))
    for rid in member_role_ids:
        base |= int(roles_by_id.get(int(rid), {}).get("permissions", 0))
    if base & ADMINISTRATOR:
        return (1 << 64) - 1
    perms = base
    ow_everyone = None
    ow_role_deny = 0
    ow_role_allow = 0
    ow_member = None
    role_ids = {int(r) for r in member_role_ids}
    for ow in channel_overwrites or []:
        oid = int(ow["id"])
        deny = int(ow.get("deny", 0))
        allow = int(ow.get("allow", 0))
        if ow["type"] == 0:
            if oid == int(guild_id):
                ow_everyone = (allow, deny)
            elif oid in role_ids:
                ow_role_deny |= deny
                ow_role_allow |= allow
        elif ow["type"] == 1 and oid == int(member_user_id):
            ow_member = (allow, deny)
    if ow_everyone:
        perms = (perms & ~ow_everyone[1]) | ow_everyone[0]
    perms = (perms & ~ow_role_deny) | ow_role_allow
    if ow_member:
        perms = (perms & ~ow_member[1]) | ow_member[0]
    return perms


def upload_allowed(perms, member, now_ms):
    """Mirror of Bonfire's upload gate (BaseChannelAuthService + AttachmentUploadService.
    getMemberUploadChannel): channel access is authenticated first, so the member
    needs VIEW_CHANNEL; uploads need SEND_MESSAGES | ATTACH_FILES; and a member
    whose communication_disabled_until is still in the future is refused
    (isGuildMemberTimedOut, GuildModel.ts). member may be None (not a member)."""
    if member is None:
        return False
    if (perms & VIEW_CHANNEL) == 0 or (perms & SEND_MESSAGES) == 0 or (perms & ATTACH_FILES) == 0:
        return False
    until = member.get("communication_disabled_until")
    if until:
        try:
            if datetime.fromisoformat(until.replace("Z", "+00:00")).timestamp() * 1000 > now_ms:
                return False
        except (ValueError, AttributeError):
            pass
    return True


def is_transient(status):
    """None = network error; 408/429/5xx retry; other 4xx are permanent."""
    if status is None:
        return True
    return status == 408 or status == 429 or 500 <= status < 600


def safe_path(path):
    """Mask the token segment of webhook URLs for logging."""
    return WEBHOOK_TOKEN_RE.sub(r"\1<token>", path)


def plan_attachments(attachments, allowed, max_bytes, limit=MAX_ATTACHMENTS_PER_MESSAGE):
    """Decide which attachments to relay. attachments: [{filename, size}, ...].
    Returns (to_download, skipped_count)."""
    to_download = []
    skipped = 0
    for att in attachments:
        if len(to_download) >= limit:
            skipped += 1
        elif not allowed or att["size"] > max_bytes:
            skipped += 1
        else:
            to_download.append(att)
    return to_download, skipped


def attachment_ids_to_keep(relayed_pairs, current_discord_ids):
    """relayed_pairs: [{d: discord_attachment_id, b: bonfire_attachment_id}].
    current_discord_ids: attachment ids still on the Discord message.
    Returns (kept_pairs, removed_pairs)."""
    current = {int(i) for i in current_discord_ids}
    kept = [p for p in relayed_pairs if int(p["d"]) in current]
    removed = [p for p in relayed_pairs if int(p["d"]) not in current]
    return kept, removed


def forward_text(snapshot_contents):
    """Bonfire body for a Discord forward: '[Forwarded]' + each snapshot's text."""
    parts = ["[Forwarded]"]
    parts.extend(c for c in snapshot_contents if c)
    return "\n".join(parts)


def poll_text(question, answers):
    """Bonfire body for a Discord poll."""
    lines = [f"[Poll] {question}"]
    lines.extend(f"- {a}" for a in answers)
    return "\n".join(lines)


def has_relayable(full, files):
    """Whether a message produces anything on Bonfire: rendered text (translated
    body + suffix: sticker lines, the file note) or at least one file."""
    return bool(split_content(full)) or bool(files)


def chunk_nonce(discord_id, chunk_index, version=""):
    """Deterministic nonce: 'did:i[:version]' — max 19+1+2+1+8 = 31 chars, inside
    Fluxer's 1-32 limit (MessageRequestSchemas.ts MessageNonceRequest). The version
    makes a repost (after an attachment removal) differ from the original post:
    Fluxer answers a repeated nonce within 5 minutes with the earlier message, which
    the repost has just deleted."""
    return f"{discord_id}:{chunk_index}" + (f":{version}" if version else "")


def post_version(chunks, discord_file_ids):
    """8-hex fingerprint of what a post contains (text and which Discord files)."""
    text = "\x00".join(chunks) + "|" + ",".join(str(i) for i in sorted(discord_file_ids))
    return f"{zlib.crc32(text.encode()):08x}"


def is_stale_echo(returned_content, sent_chunk):
    """True when the webhook answered a different (earlier) message for this nonce.
    Surrounding whitespace is ignored: the server may trim it, and a false match would
    PATCH (and mark as edited) every relayed message."""
    return returned_content is not None and returned_content.strip() != sent_chunk.strip()


def relayed_channel_type(t):
    """Channel types we relay. t is an int ChannelType value:
    0 text, 5 news, 10 news_thread, 11 public_thread, 12 private_thread."""
    return t in (0, 5, 10, 11, 12)


def snowflake_from_iso(iso_ts):
    dt = datetime.fromisoformat(iso_ts.replace("Z", "+00:00"))
    return (int(dt.timestamp() * 1000) - DISCORD_EPOCH_MS) << 22


def display_name_for(member_or_user):
    """nick > global name > username, trimmed to 1-80 chars."""
    name = getattr(member_or_user, "display_name", None) or getattr(member_or_user, "global_name", None) or member_or_user.name
    return (name or "Discord user").strip()[:80] or "Discord user"


def avatar_url_for(member_or_user):
    ga = getattr(member_or_user, "guild_avatar", None)
    if ga is not None:
        return str(ga.url)
    return str(member_or_user.display_avatar.url)


# ---------- state ----------

class StateDB:
    def __init__(self, path):
        self.db = sqlite3.connect(path)
        self.db.execute("CREATE TABLE IF NOT EXISTS webhooks (channel_id INTEGER PRIMARY KEY, webhook_id INTEGER, token TEXT)")
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS messages ("
            "discord_id INTEGER PRIMARY KEY, bonfire_ids TEXT, channel_id INTEGER, "
            "webhook_id INTEGER, token TEXT, content TEXT, suffix TEXT, "
            "attachments TEXT, author_name TEXT, author_avatar TEXT, author_discord_id INTEGER, kind TEXT)"
        )
        self.db.execute("CREATE TABLE IF NOT EXISTS cursors (channel_id INTEGER PRIMARY KEY, last_id INTEGER)")
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS pending ("
            "discord_id INTEGER PRIMARY KEY, channel_id INTEGER, kind TEXT, attempt INTEGER DEFAULT 0)"
        )

    def get_cursor(self, channel_id):
        row = self.db.execute("SELECT last_id FROM cursors WHERE channel_id=?", (channel_id,)).fetchone()
        return row[0] if row else None

    def set_cursor(self, channel_id, last_id):
        self.db.execute("INSERT OR REPLACE INTO cursors VALUES (?,?)", (channel_id, last_id))
        self.db.commit()

    def get_webhook(self, channel_id):
        return self.db.execute("SELECT webhook_id, token FROM webhooks WHERE channel_id=?", (channel_id,)).fetchone()

    def set_webhook(self, channel_id, webhook_id, token):
        if not token:
            raise ValueError("refusing to store a webhook without a token")
        self.db.execute("INSERT OR REPLACE INTO webhooks VALUES (?,?,?)", (channel_id, webhook_id, token))
        self.db.commit()

    def add_message(self, discord_id, bonfire_ids, channel_id, webhook_id, token,
                    content, suffix, attachments, author_name, author_avatar, author_discord_id,
                    kind="normal"):
        self.db.execute(
            "INSERT OR REPLACE INTO messages VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (discord_id, json.dumps(bonfire_ids), channel_id, webhook_id, token,
             content, suffix, json.dumps(attachments), author_name, author_avatar,
             author_discord_id, kind),
        )
        self.db.commit()

    def get_message(self, discord_id):
        row = self.db.execute(
            "SELECT bonfire_ids, channel_id, webhook_id, token, content, suffix, "
            "attachments, author_name, author_avatar, author_discord_id, kind FROM messages WHERE discord_id=?",
            (discord_id,),
        ).fetchone()
        return (
            {"bonfire_ids": json.loads(row[0]), "channel_id": row[1], "webhook_id": row[2],
             "token": row[3], "content": row[4], "suffix": row[5],
             "attachments": json.loads(row[6] or "[]"),
             "author_name": row[7], "author_avatar": row[8], "author_discord_id": row[9],
             "kind": row[10]}
            if row else None
        )

    def update_message(self, discord_id, bonfire_ids, content, attachments=None):
        if attachments is None:
            self.db.execute(
                "UPDATE messages SET bonfire_ids=?, content=? WHERE discord_id=?",
                (json.dumps(bonfire_ids), content, discord_id),
            )
        else:
            self.db.execute(
                "UPDATE messages SET bonfire_ids=?, content=?, attachments=? WHERE discord_id=?",
                (json.dumps(bonfire_ids), content, json.dumps(attachments), discord_id),
            )
        self.db.commit()

    def del_message(self, discord_id):
        self.db.execute("DELETE FROM messages WHERE discord_id=?", (discord_id,))
        self.db.commit()

    def add_pending(self, discord_id, channel_id, kind):
        self.db.execute(
            "INSERT OR IGNORE INTO pending (discord_id, channel_id, kind) VALUES (?,?,?)",
            (discord_id, channel_id, kind),
        )
        self.db.commit()

    def pending_for_channel(self, channel_id):
        return self.db.execute(
            "SELECT discord_id, kind, attempt FROM pending WHERE channel_id=? ORDER BY discord_id",
            (channel_id,),
        ).fetchall()

    def bump_pending(self, discord_id):
        self.db.execute("UPDATE pending SET attempt=attempt+1 WHERE discord_id=?", (discord_id,))
        self.db.commit()

    def del_pending(self, discord_id):
        self.db.execute("DELETE FROM pending WHERE discord_id=?", (discord_id,))
        self.db.commit()


# ---------- Bonfire REST ----------

class BonfireAPI:
    """Bot-token REST client for the Fluxer API. Auth: 'Authorization: Bot <token>'
    (fluxer_api/src/api/openapi/openapi.json securitySchemes.botToken)."""

    def __init__(self, base_url, token):
        self.base = base_url.rstrip("/") + "/api"
        self.headers = {"Authorization": f"Bot {token}"} if token else {}

    async def _req(self, method, path, session=None, **kw):
        sess = session or aiohttp.ClientSession(timeout=REQUEST_TIMEOUT)
        own = session is None
        try:
            while True:
                async with sess.request(method, self.base + path, headers=self.headers, **kw) as resp:
                    if resp.status == 429:
                        try:
                            wait = float((await resp.json()).get("retry_after", 1))
                        except Exception:
                            wait = 1.0
                        log.warning("bonfire 429 on %s %s: retry in %.1fs", method, safe_path(path), wait)
                        await asyncio.sleep(wait)
                        continue
                    return resp.status, await resp.read()
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            raise TransientError(_exc_name(e)) from None
        finally:
            if own:
                await sess.close()

    async def get_json(self, path):
        """Any non-200 raises: an unreadable answer is unresolved, never cached
        as empty (see TTLCache)."""
        status, text = await self._req("GET", path)
        if status != 200:
            raise TransientError(f"GET {safe_path(path)} -> {status}")
        return status, json.loads(text)

    async def channel_status(self, channel_id):
        status, _ = await self._req("GET", f"/channels/{channel_id}")
        return status

    async def bonfire_message_ids_after(self, channel_id, after_snowflake):
        """All Bonfire message ids after the given id. Fluxer returns the oldest
        `limit` matches after that id, newest-first; paginate on the max id
        (MessageDataRepository.listMessagesAfter). Any non-200 page raises so the
        caller retries rather than working from a partial set."""
        ids = set()
        after = after_snowflake
        while True:
            status, data = await self.get_json(f"/channels/{channel_id}/messages?after={after}&limit=100")
            if not data:
                break
            for m in data:
                ids.add(int(m["id"]))
                after = max(after, int(m["id"]))
            if len(data) < 100:
                break
        return ids

    async def message_exists(self, channel_id, message_id):
        status, _ = await self._req("GET", f"/channels/{channel_id}/messages/{message_id}")
        return status == 200

    async def get_or_create_webhook(self, channel_id):
        _, hooks = await self.get_json(f"/channels/{channel_id}/webhooks")
        if hooks:
            for h in hooks:
                if h.get("name") == WEBHOOK_NAME and h.get("token"):
                    return int(h["id"]), h["token"]
        status, text = await self._req("POST", f"/channels/{channel_id}/webhooks", json={"name": WEBHOOK_NAME})
        if status not in (200, 201):
            if is_transient(status):
                raise TransientError(f"webhook create in {channel_id} -> {status}")
            log.warning("webhook create in %s -> status %s", channel_id, status)
            return None, None
        data = json.loads(text)
        if not data.get("token"):
            raise TransientError(f"webhook create in {channel_id} returned no token")
        return int(data["id"]), data["token"]

    async def execute_webhook(self, webhook_id, token, payload, files=None):
        url = f"/webhooks/{webhook_id}/{token}?wait=true"
        if files:
            form = aiohttp.FormData()
            form.add_field("payload_json", json.dumps(payload), content_type="application/json")
            for i, f in enumerate(files):
                # f = (filename, bytes, content_type, discord_attachment_id)
                form.add_field(f"files[{i}]", f[1], filename=f[0],
                               content_type=f[2] or "application/octet-stream")
            status, text = await self._req("POST", url, data=form)
        else:
            status, text = await self._req("POST", url, json=payload)
        if status in (200, 201) and text:
            try:
                return status, json.loads(text)
            except Exception:
                return status, None
        return status, None

    async def edit_webhook_message(self, webhook_id, token, message_id, payload):
        return await self._req("PATCH", f"/webhooks/{webhook_id}/{token}/messages/{message_id}", json=payload)

    async def delete_webhook_message(self, webhook_id, token, message_id):
        return await self._req("DELETE", f"/webhooks/{webhook_id}/{token}/messages/{message_id}")

    async def guild_owner_id(self, guild_id):
        _, data = await self.get_json(f"/guilds/{guild_id}")
        return int(data["owner_id"]) if data and data.get("owner_id") else None

    async def guild_roles(self, guild_id):
        _, data = await self.get_json(f"/guilds/{guild_id}/roles")
        return {int(r["id"]): r for r in data} if data else None

    async def guild_member(self, guild_id, fluxer_user_id):
        """404 (not a member) -> None; any other non-200 raises."""
        status, text = await self._req("GET", f"/guilds/{guild_id}/members/{fluxer_user_id}")
        if status == 404:
            return None
        if status != 200:
            raise TransientError(f"member {fluxer_user_id} in {guild_id} -> {status}")
        return json.loads(text)

    async def channel_overwrites(self, channel_id):
        _, data = await self.get_json(f"/channels/{channel_id}")
        return (data.get("permission_overwrites") or []) if data else []

    async def guild_emoji_ids(self, guild_id):
        _, data = await self.get_json(f"/guilds/{guild_id}/emojis")
        return {int(e["id"]) for e in data} if data else set()


class TTLCache:
    def __init__(self):
        self._v = {}

    async def get(self, key, fetch):
        v = self._v.get(key)
        if v and v[1] > time.monotonic():
            return v[0]
        val = await fetch()  # exceptions propagate: failures are never cached
        self._v[key] = (val, time.monotonic() + CACHE_TTL)
        return val


# ---------- relay ----------

class Relay:
    def __init__(self, bonfire, state, account_map, dry_run):
        self.bonfire = bonfire
        self.state = state
        self.accounts = account_map  # {discord_id_str: {"fluxer_user_id": ..., "username": ...}}
        self.dry_run = dry_run
        self.queues = {}          # channel_id -> asyncio.Queue
        self.workers = {}         # channel_id -> asyncio.Task
        self.channels = {}        # channel_id -> discord channel
        self.skipped_channels = set()  # not on Bonfire (403/404)
        self.cache = TTLCache()
        self.member_cache = {}    # (guild_id, discord_id) -> Member or None (per run)
        self.known_message_ids = set()  # Bonfire ids confirmed to exist (replies)

    def fluxer_id_for(self, discord_id):
        acc = self.accounts.get(str(discord_id))
        return acc["fluxer_user_id"] if acc else None

    def mention_name_lookup(self, message=None, payload_mentions=None):
        names = {}
        if message is not None:
            names = {str(m.id): m.display_name for m in message.mentions}
        if payload_mentions:
            for m in payload_mentions:
                member = m.get("member") or {}
                names[str(m["id"])] = member.get("nick") or m.get("global_name") or m.get("username") or str(m["id"])
        return lambda did: names.get(did)

    async def author_can_upload(self, guild_id, channel_id, discord_id):
        fluxer_id = self.fluxer_id_for(discord_id)
        if fluxer_id is None:
            return False
        member = await self.cache.get(
            ("member", guild_id, fluxer_id), lambda: self.bonfire.guild_member(guild_id, fluxer_id))
        if member is None:  # 404: not a member -> can't upload (cacheable)
            return False
        roles = await self.cache.get(("roles", guild_id), lambda: self.bonfire.guild_roles(guild_id))
        overwrites = await self.cache.get(("ow", channel_id), lambda: self.bonfire.channel_overwrites(channel_id))
        owner = await self.cache.get(("owner", guild_id), lambda: self.bonfire.guild_owner_id(guild_id))
        if roles is None:
            raise TransientError(f"roles for {guild_id} unreadable")
        perms = effective_permissions(guild_id, member.get("roles") or [], int(fluxer_id), roles, overwrites, owner)
        return upload_allowed(perms, member, time.time() * 1000)

    async def webhook_for(self, channel_id):
        row = self.state.get_webhook(channel_id)
        if row and row[1]:
            return row[0], row[1]
        if self.dry_run:
            return None, None
        wid, token = await self.bonfire.get_or_create_webhook(channel_id)  # raises on transient/no token
        if wid and token:
            self.state.set_webhook(channel_id, wid, token)
        return wid, token

    async def resolve_reference(self, channel_id, discord_message_id):
        row = self.state.get_message(discord_message_id)
        candidate = row["bonfire_ids"][0] if row and row["bonfire_ids"] else discord_message_id
        if candidate in self.known_message_ids:
            return candidate
        if await self.bonfire.message_exists(channel_id, candidate):
            self.known_message_ids.add(candidate)
            return candidate
        return None

    async def member_for(self, guild, user):
        """Get a Member for guild nick/avatar; channel.history() returns Users.
        Cached for the run; a 404 (left guild) falls back to the User."""
        if isinstance(user, discord.Member):
            return user
        key = (guild.id, user.id)
        if key not in self.member_cache:
            try:
                self.member_cache[key] = await guild.fetch_member(user.id)
            except discord.NotFound:
                self.member_cache[key] = None
            except Exception as e:
                log.warning("fetch_member %s in %s failed exc=%s", user.id, guild.id, _exc_name(e))
                self.member_cache[key] = None
        return self.member_cache[key] or user

    def special_body(self, message):
        """Forward/poll text for a message, or ''. Reads discord.py attrs only."""
        snaps = getattr(message, "message_snapshots", None) or []
        if snaps and not (message.content or ""):
            return forward_text([s.content or "" for s in snaps])
        poll = getattr(message, "poll", None)
        if poll is not None:
            return poll_text(poll.question, [a.text for a in poll.answers])
        return ""

    async def build_content(self, message):
        """Returns (translated_content, attachments_plan_inputs, sticker_suffix).
        The file-note part of the suffix is appended after downloads, so it can
        count attachments that vanished from Discord in the meantime."""
        content = "\n".join(p for p in (message.content or "", self.special_body(message)) if p)
        suffix = ""
        for sticker in getattr(message, "stickers", []) or []:
            suffix += f"\n[sticker: {sticker.name}]"
        allowed = await self.author_can_upload(message.guild.id, message.channel.id, message.author.id)
        att_inputs = [{"filename": a.filename, "size": a.size, "id": a.id, "_att": a}
                      for a in message.attachments]
        for snap in getattr(message, "message_snapshots", None) or []:
            att_inputs += [{"filename": a.filename, "size": a.size, "id": a.id, "_att": a}
                           for a in getattr(snap, "attachments", []) or []]
        plan = plan_attachments(att_inputs, allowed, self.max_file_bytes)
        emoji_ids = await self.known_emoji_ids(message.guild.id)
        lookup = self.mention_name_lookup(message=message)
        translated = translate_content(content, self.mention_map(), emoji_ids, lookup)
        return translated, suffix, plan, emoji_ids, lookup

    def mention_map(self):
        return {did: acc["fluxer_user_id"] for did, acc in self.accounts.items()}

    async def known_emoji_ids(self, guild_id):
        return await self.cache.get(("emoji", guild_id), lambda: self.bonfire.guild_emoji_ids(guild_id))

    async def download_files(self, plan):
        """Returns (files, gone_count). files: [(filename, bytes, content_type, discord_att_id)].
        A file that Discord no longer serves (NotFound/Forbidden) is permanent:
        counted, not fatal. Other failures are transient."""
        files = []
        gone = 0
        for item in plan:
            try:
                data = await item["_att"].read()
                files.append((item["filename"], data, item["_att"].content_type, item["id"]))
            except (discord.NotFound, discord.Forbidden):
                gone += 1
            except Exception as e:
                raise TransientError(f"attachment {item['filename']}: {_exc_name(e)}")
        return files, gone

    async def download_url(self, url):
        """Discord CDN fetch for reposts where only the raw payload url is known."""
        async with aiohttp.ClientSession(timeout=REQUEST_TIMEOUT) as sess:
            try:
                async with sess.get(url) as resp:
                    if resp.status != 200:
                        if is_transient(resp.status):
                            raise TransientError(f"attachment cdn -> {resp.status}")
                        return None
                    return await resp.read()
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                raise TransientError(_exc_name(e))

    async def _post_chunks(self, channel_id, wid, token, discord_id, chunks, files,
                           author_name, author_avatar, ref, version=None):
        """Post chunks through the webhook; deterministic nonce makes a retry of a
        failed chunk return the earlier message instead of duplicating.
        Returns (posted_ids, att_pairs, outcome)."""
        posted_ids = []
        att_pairs = []
        chunk_list = chunks or [""]
        # original posts use the content-independent nonce so a retry after a
        # lost response still dedupes; reposts pass a version so they don't
        # collide with the originals they just deleted
        for i, chunk in enumerate(chunk_list):
            payload = {
                "content": chunk,
                "username": author_name,
                "avatar_url": author_avatar,
                "allowed_mentions": {"parse": [], "replied_user": False},
                "nonce": chunk_nonce(discord_id, i, version or ""),
            }
            if i == 0 and ref:
                payload["message_reference"] = {"message_id": str(ref)}
            last = i == len(chunk_list) - 1
            if last and files:
                payload["attachments"] = [{"id": j, "filename": f[0]} for j, f in enumerate(files)]
            try:
                status, data = await self.bonfire.execute_webhook(
                    wid, token, payload, files=files if last else None)
            except TransientError as e:
                status, data = None, None
                log.warning("webhook execute msg=%s chunk=%s transient: %s", discord_id, i, e)
            if data and data.get("id"):
                # includes a nonce-deduped earlier post: still "posted"
                posted_ids.append(int(data["id"]))
                self.known_message_ids.add(int(data["id"]))
                for j, att in enumerate(data.get("attachments") or []):
                    # response attachments follow the order of the files sent
                    if j < len(files):
                        att_pairs.append({"d": int(files[j][3]), "b": int(att["id"])})
                if is_stale_echo(data.get("content"), chunk):
                    # Fluxer returned the earlier message for this nonce: fix it
                    try:
                        st, _ = await self.bonfire.edit_webhook_message(
                            wid, token, int(data["id"]), {"content": chunk})
                    except TransientError:
                        st = None
                    if st not in (200, 204):
                        log.warning("nonce echo repair PATCH status=%s bonfire_msg=%s msg=%s",
                                    st, data.get("id"), discord_id)
                        return posted_ids, att_pairs, "transient"
                continue
            if status is None or is_transient(status):
                if posted_ids:
                    log.warning("msg %s: %d/%d chunk(s) posted, tail lost to transient failure",
                                discord_id, len(posted_ids), len(chunk_list))
                return posted_ids, att_pairs, "transient"
            log.warning("webhook execute status=%s msg=%s chunk=%s: permanent, skipping message",
                        status, discord_id, i)
            return posted_ids, att_pairs, "permanent"
        return posted_ids, att_pairs, "ok"

    async def relay_message(self, message):
        """Returns 'ok', 'permanent' (non-transient 4xx: logged + skipped) or
        'transient' (retry later via catch-up)."""
        channel_id = message.channel.id
        try:
            translated, sticker_suffix, (to_download, skipped), _, _ = await self.build_content(message)
        except TransientError as e:
            log.warning("transient preparing msg %s: %s", message.id, e)
            return "transient"
        if self.dry_run:
            full = translated + sticker_suffix + (f"\n[{skipped} file(s) not relayed]" if skipped else "")
            log.info("DRY-RUN would relay msg %s in #%s (%d chunk(s), %d file(s) copied, %d noted)",
                     message.id, channel_id, len(split_content(full)) or 1, len(to_download), skipped)
            return "ok"
        # sticker-only and file-only messages still relay: judge the rendered
        # body (translated text + suffix), not just the translated text
        if not has_relayable(translated + sticker_suffix, to_download) and not skipped:
            log.info("msg %s produced nothing to relay (system-less content)", message.id)
            return "ok"
        try:
            wid, token = await self.webhook_for(channel_id)
            files, gone = await self.download_files(to_download)
            ref = None
            if message.type == discord.MessageType.reply and message.reference and message.reference.message_id:
                ref = await self.resolve_reference(channel_id, message.reference.message_id)
            author = await self.member_for(message.guild, message.author)
        except TransientError as e:
            log.warning("transient preparing msg %s: %s", message.id, e)
            return "transient"
        if not wid or not token:
            log.warning("no webhook for channel %s; msg %s transient", channel_id, message.id)
            return "transient"
        suffix = sticker_suffix
        not_relayed = skipped + gone
        if not_relayed:
            suffix += f"\n[{not_relayed} file(s) not relayed]"
        full = translated + suffix
        chunks = split_content(full)
        if not has_relayable(full, files):
            log.info("msg %s produced nothing to relay (system-less content)", message.id)
            return "ok"
        kind = ("poll" if getattr(message, "poll", None) is not None
                else "forward" if getattr(message, "message_snapshots", None) and not (message.content or "")
                else "normal")
        posted_ids, att_pairs, outcome = await self._post_chunks(
            channel_id, wid, token, message.id, chunks, files,
            display_name_for(author), avatar_url_for(author), ref)
        if posted_ids:
            self.state.add_message(message.id, posted_ids, channel_id, wid, token,
                                   full, suffix, att_pairs,
                                   display_name_for(author), avatar_url_for(author),
                                   message.author.id, kind)
        return outcome

    async def _repost(self, channel_id, discord_id, row, content, current_atts, author_name, author_avatar):
        """Attachments can't be edited (WebhookMessageEditRequest has none): prepare the
        replacement fully, then delete the relayed messages and post it. Only then
        is the mapping updated; nothing is deleted if the replacement is empty."""
        # re-check the author's upload rule and download what survives it
        allowed = False
        channel = self.channels.get(channel_id)
        if channel is not None and row.get("author_discord_id") is not None:
            allowed = await self.author_can_upload(channel.guild.id, channel_id, row["author_discord_id"])
        files = []
        missing = 0
        for att in current_atts or []:
            if not allowed or int(att.get("size", 0)) > self.max_file_bytes:
                missing += 1
                continue
            data = await self.download_url(att["url"])
            if data is None:
                missing += 1
                continue
            files.append((att.get("filename", "file"), data, att.get("content_type"), int(att["id"])))
        new_content = content
        if missing:
            new_content += f"\n[{missing} file(s) not relayed]"
        if not has_relayable(new_content, files):
            log.info("repost of discord msg %s would be empty; keeping the old copy", discord_id)
            return True
        # everything is ready: delete the old copy, post the replacement
        for bid in row["bonfire_ids"]:
            try:
                status, _ = await self.bonfire.delete_webhook_message(row["webhook_id"], row["token"], bid)
            except TransientError as e:
                raise TransientError(f"delete {bid}: {e}")
            if status not in (200, 204, 404):
                if is_transient(status):
                    raise TransientError(f"delete {bid} -> {status}")
                log.warning("webhook delete status=%s bonfire_msg=%s: permanent", status, bid)
        chunks = split_content(new_content) or [""]
        version = post_version(chunks, [f[3] for f in files])
        posted_ids, att_pairs, outcome = await self._post_chunks(
            channel_id, row["webhook_id"], row["token"], discord_id, chunks, files,
            author_name, author_avatar, None, version=version)
        if outcome == "ok":
            self.state.update_message(discord_id, posted_ids, new_content, att_pairs)
            return True
        if outcome == "permanent":
            # the old copy is already deleted; don't leave the mapping pointing at gone ids
            if posted_ids:
                self.state.update_message(discord_id, posted_ids, new_content, att_pairs)
            else:
                self.state.del_message(discord_id)
            log.warning("repost of discord msg %s failed permanently (%d chunk(s) posted)",
                        discord_id, len(posted_ids))
            return True
        if posted_ids:
            self.state.update_message(discord_id, posted_ids, new_content, att_pairs)
        return False

    async def do_edit(self, channel_id, discord_id, content, payload_mentions, current_attachments):
        """Apply an edit (or attachment removal) to a relayed message.
        Returns True when Bonfire confirms success. current_attachments: raw
        Discord attachment dicts currently on the message."""
        row = self.state.get_message(int(discord_id))
        if not row:
            return True  # not ours: nothing pending
        if row.get("kind") in ("forward", "poll"):
            return True  # Discord can't edit these; a MESSAGE_UPDATE is a pin/unfurl
        emoji_ids = set()
        channel = self.channels.get(row["channel_id"])
        if channel is not None:
            emoji_ids = await self.known_emoji_ids(channel.guild.id)
        translated = translate_content(
            content or "", self.mention_map(), emoji_ids,
            self.mention_name_lookup(payload_mentions=payload_mentions or []))
        new_full = translated + (row["suffix"] or "")
        kept_pairs, removed = attachment_ids_to_keep(
            row["attachments"], [a.get("id") for a in current_attachments or []])
        if removed or (not split_content(new_full) and kept_pairs):
            # webhook edits can't set attachments -> delete + repost.
            # (also covers a caption removed while a file stays: PATCH {"content": ""}
            # without attachments is rejected with CANNOT_SEND_EMPTY_MESSAGE)
            return await self._repost(
                row["channel_id"], discord_id, row, new_full,
                current_attachments or [], row["author_name"], row["author_avatar"])
        if new_full == row["content"]:
            return True  # link unfurl or other no-op edit
        chunks = split_content(new_full) or [""]
        kept = row["bonfire_ids"][: len(chunks)]
        surplus = row["bonfire_ids"][len(chunks):]
        for bid, chunk in zip(kept, chunks):
            try:
                status, _ = await self.bonfire.edit_webhook_message(
                    row["webhook_id"], row["token"], bid, {"content": chunk})
            except TransientError as e:
                raise TransientError(f"edit {bid}: {e}")
            if status not in (200, 204):
                if is_transient(status):
                    raise TransientError(f"edit {bid} -> {status}")
                log.warning("webhook edit status=%s bonfire_msg=%s (discord %s)",
                            status, bid, discord_id)
                return True  # permanent: give up on this edit, clear pending
        # if the edit needs more chunks than exist, the tail is dropped (kept
        # simple); if it needs fewer, delete the surplus webhook messages —
        # survivors stay in the mapping so a later delete still finds them.
        if len(chunks) > len(row["bonfire_ids"]):
            log.warning("edit of discord msg %s grew from %d to %d chunks; tail dropped",
                        discord_id, len(row["bonfire_ids"]), len(chunks))
        survivors = list(kept)
        for bid in surplus:
            try:
                status, _ = await self.bonfire.delete_webhook_message(row["webhook_id"], row["token"], bid)
            except TransientError:
                status = None
            if status in (200, 204, 404):
                continue
            survivors.append(bid)  # keep it tracked
            log.warning("webhook delete status=%s bonfire_msg=%s; kept in mapping", status, bid)
        self.state.update_message(int(discord_id), survivors, new_full, kept_pairs)
        return True

    async def do_delete(self, discord_id):
        """Delete the relayed Bonfire messages. Returns True when all are gone
        (2xx or 404); False leaves the mapping and the pending row."""
        row = self.state.get_message(int(discord_id))
        if not row:
            return True
        survivors = []
        for bid in row["bonfire_ids"]:
            try:
                status, _ = await self.bonfire.delete_webhook_message(row["webhook_id"], row["token"], bid)
            except TransientError:
                status = None
            if status in (200, 204, 404):
                continue
            survivors.append(bid)
            if status is None or is_transient(status):
                continue
            log.warning("webhook delete status=%s bonfire_msg=%s: permanent, dropping", status, bid)
            survivors.pop()  # permanent failure: stop tracking it
        if survivors:
            self.state.update_message(int(discord_id), survivors, row["content"], row["attachments"])
            return False
        self.state.del_message(int(discord_id))
        return True

    async def retry_pending(self, channel_id, discord_id):
        """Bump the pending row and schedule another try with backoff."""
        self.state.bump_pending(discord_id)
        row = self.state.db.execute(
            "SELECT attempt FROM pending WHERE discord_id=?", (discord_id,)).fetchone()
        self._delayed_item(channel_id, ("retry", discord_id), self._retry_delay(row[0] if row else 0))

    async def drive_pending(self, channel_id, discord_id, live_payload=None):
        """Run one pending row to completion. Returns True on success.
        live_payload: a raw edit event (obj.data) when the row is fresh from the
        gateway; otherwise the current Discord message is fetched, and a 404
        turns an edit into a delete."""
        row = self.state.db.execute(
            "SELECT kind FROM pending WHERE discord_id=?", (discord_id,)).fetchone()
        if not row:
            return True
        if row[0] == "delete":
            return await self.do_delete(discord_id)
        if live_payload is not None:
            data = live_payload.data or {}
            return await self.do_edit(
                channel_id, discord_id, data.get("content"),
                data.get("mentions") or [], data.get("attachments") or [])
        channel = self.channels.get(channel_id)
        try:
            msg = await channel.fetch_message(discord_id)
        except discord.NotFound:
            return await self.do_delete(discord_id)
        except Exception as e:
            raise TransientError(f"fetch_message {discord_id}: {_exc_name(e)}")
        return await self.do_edit(
            channel_id, discord_id, msg.content,
            [self._user_payload(m) for m in msg.mentions],
            [{"id": a.id, "size": a.size, "url": a.url,
              "filename": a.filename, "content_type": a.content_type}
             for a in msg.attachments])

    async def process_pending(self, channel_id):
        """Drain the durable pending list for this channel (runs inside catch-up).
        A row that still can't be applied is rescheduled with backoff, never
        dropped silently."""
        for discord_id, _kind, _attempt in self.state.pending_for_channel(channel_id):
            if self.dry_run:
                self.state.del_pending(discord_id)
                continue
            try:
                ok = await self.drive_pending(channel_id, discord_id)
            except TransientError:
                ok = False
            if ok:
                self.state.del_pending(discord_id)
            else:
                await self.retry_pending(channel_id, discord_id)

    @staticmethod
    def _user_payload(member_or_user):
        return {"id": str(member_or_user.id),
                "username": getattr(member_or_user, "name", None),
                "global_name": getattr(member_or_user, "global_name", None),
                "member": {"nick": getattr(member_or_user, "nick", None)}}

    def _delayed_item(self, channel_id, item, seconds):
        async def put():
            await asyncio.sleep(seconds)
            if channel_id in self.queues:
                self.queues[channel_id].put_nowait(item)
        asyncio.create_task(put())

    def _retry_delay(self, attempt):
        return min(RETRY_SECONDS * (2 ** attempt), RETRY_MAX_SECONDS)

    def enqueue(self, channel, kind, obj):
        if channel.id not in self.queues:
            self.queues[channel.id] = asyncio.Queue()
        self.channels[channel.id] = channel
        new_worker = channel.id not in self.workers or self.workers[channel.id].done()
        if new_worker:
            self.workers[channel.id] = asyncio.create_task(self.channel_worker(channel.id))
            # a brand-new worker must catch up before touching any live item
            if kind != "catchup":
                self.queues[channel.id].put_nowait(("catchup", None))
        self.queues[channel.id].put_nowait((kind, obj))

    async def channel_worker(self, channel_id):
        while True:
            kind, obj = await self.queues[channel_id].get()
            try:
                if kind == "catchup":
                    await self.catchup(channel_id)
                elif kind == "message":
                    if channel_id not in self.skipped_channels and not self.state.get_message(obj.id):
                        outcome = await self.relay_message(obj)
                        # live path never advances the cursor. A transient failure
                        # runs catch-up inline so newer messages can't overtake it;
                        # catch-up re-finds the message via the state DB.
                        if outcome == "transient":
                            await self.catchup(channel_id)
                elif kind in ("edit", "delete", "retry"):
                    discord_id = int(obj.message_id) if kind == "edit" else int(obj)
                    if kind != "retry":
                        self.state.add_pending(discord_id, channel_id, kind)
                    if self.dry_run:
                        log.info("DRY-RUN would %s Bonfire ids for Discord msg %s", kind, discord_id)
                        self.state.del_pending(discord_id)
                        continue
                    try:
                        ok = await self.drive_pending(
                            channel_id, discord_id, live_payload=obj if kind == "edit" else None)
                    except TransientError:
                        ok = False
                    if ok:
                        self.state.del_pending(discord_id)
                    else:
                        await self.retry_pending(channel_id, discord_id)
            except Exception as e:
                log.error("worker item %s failed channel=%s id=%s exc=%s",
                          kind, channel_id, getattr(obj, "id", getattr(obj, "message_id", "?")), _exc_name(e))
                if kind in ("edit", "delete", "retry"):
                    # an unexpected error must not leave a pending row without a retry
                    discord_id = int(obj.message_id) if kind == "edit" else int(obj)
                    await self.retry_pending(channel_id, discord_id)

    async def catchup(self, channel_id):
        # Retry in place: the channel's live items wait behind the catch-up, so they
        # can never move the cursor past messages the catch-up hasn't handled yet.
        while True:
            try:
                return await self._catchup_once(channel_id)
            except Exception as e:
                log.error("catch-up failed channel=%s exc=%s; retrying in %ds",
                          channel_id, _exc_name(e), RETRY_SECONDS)
                await asyncio.sleep(RETRY_SECONDS)

    async def _catchup_once(self, channel_id):
        channel = self.channels.get(channel_id)
        if channel is None:
            return
        status = await self.bonfire.channel_status(channel_id)
        if status in (403, 404):
            log.info("channel #%s (%s): not on Bonfire, skipped", channel.name, channel_id)
            self.skipped_channels.add(channel_id)
            return
        if status != 200:
            raise RuntimeError(f"channel check status {status}")
        await self.process_pending(channel_id)
        cursor = self.state.get_cursor(channel_id) or snowflake_from_iso(self.backfill_since)
        bonfire_ids = await self.bonfire.bonfire_message_ids_after(channel_id, cursor)
        n_seen = n_existing = n_relayed = n_files = n_noted = 0
        async for msg in channel.history(after=discord.Object(id=cursor), oldest_first=True, limit=None):
            n_seen += 1
            if self.state.get_message(msg.id) or msg.id in bonfire_ids:
                n_existing += 1
                self.state.set_cursor(channel_id, msg.id)
                continue
            if msg.author.bot or getattr(msg, "webhook_id", None):
                n_existing += 1
                self.state.set_cursor(channel_id, msg.id)
                continue
            if msg.type not in (discord.MessageType.default, discord.MessageType.reply):
                n_existing += 1
                self.state.set_cursor(channel_id, msg.id)
                continue
            if self.dry_run:
                _, _, (to_dl, skipped), _, _ = await self.build_content(msg)
                n_files += len(to_dl)
                n_noted += skipped
                n_relayed += 1
            else:
                outcome = await self.relay_message(msg)
                if outcome == "transient":
                    raise TransientError(f"transient relay of {msg.id}")
                if outcome == "ok":
                    n_relayed += 1
                    posted = self.state.get_message(msg.id)
                    if posted:
                        n_files += len(posted["attachments"])
                        noted = re.search(r"\[(\d+) file\(s\) not relayed\]", posted["suffix"] or "")
                        n_noted += int(noted.group(1)) if noted else 0
                # 'permanent' is logged inside relay_message and skipped
            self.state.set_cursor(channel_id, msg.id)
        log.info("channel #%s (%s): %d Discord message(s) since cursor, %d already on Bonfire, "
                 "%d relayed (%d file(s) copied, %d noted)",
                 channel.name, channel_id, n_seen, n_existing, n_relayed, n_files, n_noted)

    async def probe(self, guilds):
        """--discord-only-probe: list channels + count messages since backfill, then exit."""
        dt = datetime.fromisoformat(self.backfill_since.replace("Z", "+00:00"))
        for guild in guilds:
            log.info("guild %s (%s):", guild.name, guild.id)
            total = 0
            for ch in list(guild.channels) + list(getattr(guild, "threads", [])):
                if not relayed_channel_type(getattr(ch.type, "value", ch.type)):
                    continue
                try:
                    n = 0
                    async for _ in ch.history(after=dt, oldest_first=True, limit=None):
                        n += 1
                    total += n
                    log.info("  #%s (%s): %d message(s) since %s", ch.name, ch.id, n, self.backfill_since)
                except Exception as e:
                    log.warning("  #%s (%s): history failed exc=%s", ch.name, ch.id, _exc_name(e))
            log.info("guild %s total: %d message(s)", guild.name, total)


def load_accounts(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except OSError:
        log.warning("accounts file %s missing/unreadable; mention translation and upload checks disabled", path)
        return {}


def main():
    logging.basicConfig(level=logging.INFO)
    probe_only = "--discord-only-probe" in sys.argv
    token = os.environ["DISCORD_TOKEN"]
    bot_token = os.environ.get("BONFIRE_BOT_TOKEN")
    bonfire_url = os.environ.get("BONFIRE_URL") or DEFAULT_BONFIRE_URL
    guild_ids = {int(x) for x in (os.environ.get("RELAY_GUILD_IDS") or DEFAULT_RELAY_GUILD_IDS).split(",") if x.strip()}
    allowlist = {int(x) for x in (os.environ.get("RELAY_CHANNEL_ALLOWLIST") or "").split(",") if x.strip()}
    dry_run = os.environ.get("RELAY_DRY_RUN") == "1"
    accounts_file = os.environ.get("ACCOUNTS_FILE") or DEFAULT_ACCOUNTS_FILE
    state_db = os.environ.get("STATE_DB") or DEFAULT_STATE_DB

    # A dry run never touches the real state: its cursors would make the real run skip the backfill.
    relay = Relay(BonfireAPI(bonfire_url, bot_token), StateDB(":memory:" if dry_run else state_db),
                  load_accounts(accounts_file), dry_run)
    relay.backfill_since = os.environ.get("RELAY_BACKFILL_SINCE") or DEFAULT_BACKFILL_SINCE
    relay.max_file_bytes = int(os.environ.get("RELAY_MAX_FILE_BYTES") or DEFAULT_MAX_FILE_BYTES)

    intents = discord.Intents.none()
    intents.guilds = True
    intents.guild_messages = True
    intents.message_content = True
    client = discord.Client(intents=intents)

    def channel_ok(ch):
        return (
            ch.guild and ch.guild.id in guild_ids
            and (not allowlist or ch.id in allowlist)
            and relayed_channel_type(getattr(ch.type, "value", ch.type))
        )

    @client.event
    async def on_ready():
        log.info("relay connected as %s (dry_run=%s)", client.user, dry_run)
        if probe_only:
            await relay.probe([g for g in client.guilds if g.id in guild_ids])
            await client.close()
            return
        # Re-runs after a reconnect: a catchup is (re)enqueued for every channel;
        # enqueue() itself puts catchup first when it creates a new worker.
        for guild in client.guilds:
            if guild.id not in guild_ids:
                continue
            for ch in list(guild.channels) + list(getattr(guild, "threads", [])):
                if channel_ok(ch):
                    relay.enqueue(ch, "catchup", None)

    @client.event
    async def on_message(message):
        if probe_only or not channel_ok(message.channel):
            return
        if message.author.bot or getattr(message, "webhook_id", None):
            return
        if message.type not in (discord.MessageType.default, discord.MessageType.reply):
            return
        relay.enqueue(message.channel, "message", message)

    @client.event
    async def on_raw_message_edit(payload):
        if probe_only:
            return
        if int(payload.channel_id) in relay.queues:
            relay.state.add_pending(int(payload.message_id), int(payload.channel_id), "edit")
            relay.queues[int(payload.channel_id)].put_nowait(("edit", payload))

    @client.event
    async def on_raw_message_delete(payload):
        if probe_only:
            return
        if int(payload.channel_id) in relay.queues:
            relay.state.add_pending(int(payload.message_id), int(payload.channel_id), "delete")
            relay.queues[int(payload.channel_id)].put_nowait(("delete", int(payload.message_id)))

    @client.event
    async def on_raw_bulk_message_delete(payload):
        if probe_only:
            return
        if int(payload.channel_id) in relay.queues:
            for mid in payload.message_ids:
                relay.state.add_pending(int(mid), int(payload.channel_id), "delete")
                relay.queues[int(payload.channel_id)].put_nowait(("delete", int(mid)))

    client.run(token)


if __name__ == "__main__":
    main()
