# SPDX-License-Identifier: MIT OR Apache-2.0
"""Export the imported-account map for the bonfire bot.

Reads the importer's private state file (discord id -> fluxer account with
password) and writes accounts.json with discord id -> {fluxer_user_id,
username} and NO password material.

Usage: python export_accounts.py [STATE_FILE] [OUT_FILE]
Defaults: STATE_FILE = ../bonfire-users-state.json (relative to this script),
          OUT_FILE = ./data/accounts.json.
"""

import json
import os
import sys

DEFAULT_STATE = os.path.normpath(os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "bonfire-users-state.json"))
DEFAULT_OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "accounts.json")


def main() -> int:
    state_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_STATE
    out_path = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_OUT

    with open(state_path, "r", encoding="utf-8") as f:
        state = json.load(f)

    state_accounts = state.get("accounts") if isinstance(state, dict) else None
    if not isinstance(state_accounts, dict):
        print(f"export_accounts: {state_path} has no \"accounts\" object (old or unknown format)", file=sys.stderr)
        return 1

    accounts = {
        discord_id: {
            "fluxer_user_id": str(entry["fluxer_user_id"]),
            "username": entry["username"],
        }
        for discord_id, entry in state_accounts.items()
    }

    out_dir = os.path.dirname(out_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    tmp_path = out_path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(accounts, f, ensure_ascii=False, indent=1)
        f.write("\n")
    os.replace(tmp_path, out_path)

    print(len(accounts))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
