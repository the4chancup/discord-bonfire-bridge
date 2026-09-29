# SPDX-License-Identifier: MIT OR Apache-2.0
"""Bonfire account handout bot: /bonfire gives an imported member a temp password."""

import asyncio
import json
import logging
import os
import sys
import time

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands

log = logging.getLogger("bonfire-bot")


def _exc_name(e):
    """Exception summary safe to log: class name and HTTP status only, never the message
    (Discord webhook error text can embed the interaction token in the request URL)."""
    name = type(e).__name__
    status = getattr(e, "status", None)
    return f"{name}(status={status})" if isinstance(status, int) else name

DEFAULT_BONFIRE_URL = "https://bonfire.implyingrigged.info"
DEFAULT_ACCOUNTS_FILE = "/data/accounts.json"
COOLDOWN_SECONDS = 15 * 60
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=15)


class BonfireService:
    """Decision logic for /bonfire, testable without a Discord connection."""

    def __init__(self, accounts_file, bonfire_url, admin_key, cooldown_seconds=COOLDOWN_SECONDS, now=time.monotonic):
        self._accounts_file = accounts_file
        self._bonfire_url = bonfire_url.rstrip("/")
        self._admin_key = admin_key
        self._cooldown_seconds = cooldown_seconds
        self._now = now
        self._accounts_mtime = None
        self._accounts = {}
        self._accounts_loaded = False
        self._issued_at = {}
        self._locks = {}

    def _load_accounts(self):
        try:
            mtime = os.path.getmtime(self._accounts_file)
        except OSError:
            if self._accounts_mtime is not None:
                log.warning("accounts file %s unreadable, keeping previous copy", self._accounts_file)
            return
        if mtime == self._accounts_mtime:
            return
        try:
            with open(self._accounts_file, "r", encoding="utf-8") as f:
                self._accounts = json.load(f)
            self._accounts_mtime = mtime
            self._accounts_loaded = True
        except (OSError, json.JSONDecodeError):
            log.exception("failed to load accounts file %s", self._accounts_file)

    def load_accounts(self):
        """Attempt a load; return True if the accounts file has ever loaded successfully."""
        self._load_accounts()
        return self._accounts_loaded

    async def _request_password(self, fluxer_user_id, discord_id):
        """POST the temporary-password endpoint. Returns (status, password_or_None)."""
        url = f"{self._bonfire_url}/api/admin/users/{fluxer_user_id}/temporary-password"
        headers = {
            "Authorization": f"Admin {self._admin_key}",
            "X-Audit-Log-Reason": f"Discord /bonfire by {discord_id}",
        }
        async with aiohttp.ClientSession(timeout=REQUEST_TIMEOUT) as session:
            async with session.post(url, headers=headers) as resp:
                if resp.status != 200:
                    return resp.status, None
                return resp.status, await resp.json()

    def clear_cooldown(self, discord_id):
        self._issued_at.pop(discord_id, None)

    async def handle(self, discord_id):
        """Return (reply_text, issued) for a /bonfire invocation by this Discord user id."""
        self._load_accounts()
        if not self._accounts_loaded:
            log.error("/bonfire discord_id=%s outcome=error accounts unavailable (%s)", discord_id, self._accounts_file)
            return "Couldn't get your password right now, please try again in a few minutes.", False
        account = self._accounts.get(str(discord_id))
        if account is None:
            log.info("/bonfire discord_id=%s outcome=unknown", discord_id)
            return (
                "There's no imported Bonfire account for you: only members who appear in the "
                f"backed-up channels got one. You can create your own at {self._bonfire_url}",
                False,
            )

        lock = self._locks.setdefault(discord_id, asyncio.Lock())
        async with lock:
            fluxer_user_id = account["fluxer_user_id"]
            last = self._issued_at.get(discord_id)
            if last is not None:
                elapsed = self._now() - last
                if elapsed < self._cooldown_seconds:
                    remaining = -(-int(self._cooldown_seconds - elapsed) // 60)
                    ago = int(elapsed // 60)
                    log.info("/bonfire discord_id=%s fluxer_id=%s outcome=cooldown", discord_id, fluxer_user_id)
                    return (
                        f"You already got a password {ago} minutes ago; use that one. "
                        f"You can ask again in {remaining} minutes.",
                        False,
                    )

            try:
                status, data = await self._request_password(fluxer_user_id, discord_id)
            except Exception as e:
                log.error(
                    "/bonfire discord_id=%s fluxer_id=%s outcome=error exc=%s",
                    discord_id, fluxer_user_id, type(e).__name__,
                )
                return "Couldn't get your password right now, please try again in a few minutes.", False

            password = (data or {}).get("password") if status == 200 else None
            if not password:
                log.warning(
                    "/bonfire discord_id=%s fluxer_id=%s outcome=error status=%s", discord_id, fluxer_user_id, status
                )
                return "Couldn't get your password right now, please try again in a few minutes.", False

            username = data.get("username") or account["username"]
            self._issued_at[discord_id] = self._now()
            log.info("/bonfire discord_id=%s fluxer_id=%s outcome=issued", discord_id, fluxer_user_id)
            return (
                f"Your Bonfire account:\nUsername: `{username}`\n"
                f"Temporary password: `{password}`\n"
                f"Sign in at {self._bonfire_url}/login, then change the password in Settings → Account. "
                "Running /bonfire again replaces this password and logs you out of Bonfire.",
                True,
            )


class BonfireBot(commands.Bot):
    def __init__(self, service):
        super().__init__(command_prefix="!", intents=discord.Intents.none())
        self.service = service

    async def setup_hook(self):
        command = self.tree.get_command("bonfire")
        await self.http.upsert_global_command(self.application_id, command.to_dict(self.tree))


def main():
    logging.basicConfig(level=logging.INFO)
    token = os.environ["DISCORD_TOKEN"]
    admin_key = os.environ["BONFIRE_ADMIN_KEY"]
    bonfire_url = os.environ.get("BONFIRE_URL") or DEFAULT_BONFIRE_URL
    accounts_file = os.environ.get("ACCOUNTS_FILE") or DEFAULT_ACCOUNTS_FILE

    service = BonfireService(accounts_file, bonfire_url, admin_key)
    if not service.load_accounts():
        log.error("accounts file %s is missing or unreadable, exiting", accounts_file)
        sys.exit(f"bonfire-bot: cannot load accounts file {accounts_file}")
    bot = BonfireBot(service)

    @bot.tree.command(name="bonfire", description="Get your Bonfire login (only you can see the reply)")
    @app_commands.guild_only()
    async def bonfire(interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)
        text, issued = await service.handle(interaction.user.id)
        try:
            await interaction.followup.send(text, ephemeral=True)
        except Exception as e:
            if issued:
                service.clear_cooldown(interaction.user.id)
            log.error(
                "/bonfire discord_id=%s followup send failed, issued=%s exc=%s",
                interaction.user.id, issued, _exc_name(e),
            )

    @bot.tree.error
    async def on_tree_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
        log.error(
            "/bonfire discord_id=%s command error exc=%s",
            interaction.user.id if interaction.user else "?",
            _exc_name(error),
        )

    bot.run(token)


if __name__ == "__main__":
    main()
