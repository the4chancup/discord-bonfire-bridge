# SPDX-License-Identifier: MIT OR Apache-2.0
"""Offline tests for relay.py's pure functions. Stdlib only; stubs out
discord.py and aiohttp (not installed on this machine, only in the image)."""

import asyncio
import importlib.util
import json
import os
import sys
import types

RELAY = os.path.join(os.path.dirname(os.path.abspath(__file__)), "relay.py")

for name in ("discord", "aiohttp"):
    if name not in sys.modules:
        stub = types.ModuleType(name)
        sys.modules[name] = stub
sys.modules["aiohttp"].ClientTimeout = lambda total=0: None
sys.modules["aiohttp"].ClientSession = object
sys.modules["aiohttp"].FormData = object

spec = importlib.util.spec_from_file_location("relay", RELAY)
relay = importlib.util.module_from_spec(spec)
spec.loader.exec_module(relay)

CHECKS = []


def check(label, cond):
    CHECKS.append((label, cond))
    print(("PASS" if cond else "FAIL"), label)


def eq(label, got, want):
    check(f"{label} (got {got!r})", got == want)


GUILD = 124029625755107328
ATTACH = 1 << 15
ADMIN = 1 << 3
SEND = 1 << 11


def roles(*perms_for_roles, everyone=0):
    """roles_by_id with @everyone = guild id."""
    r = {str(GUILD): {"permissions": str(everyone)}}
    for rid, p in perms_for_roles:
        r[str(rid)] = {"permissions": str(p)}
    return r


def ow(oid, otype, allow=0, deny=0):
    return {"id": str(oid), "type": otype, "allow": str(allow), "deny": str(deny)}


# ---- translate_content ----
amap = {"111": "9001"}
names = {"222": "Mister X"}
emoji = {55555}

t = relay.translate_content("hi <@111> and <@!111>", amap, emoji, names.get)
eq("mapped mention + nickname form", t, "hi <@9001> and <@9001>")

t = relay.translate_content("hi <@222>", amap, emoji, names.get)
eq("unmapped mention -> @name", t, "hi @Mister X")

t = relay.translate_content("hi <@333>", amap, emoji, names.get)
eq("unmapped unknown name -> @id", t, "hi @333")

t = relay.translate_content("chan <#444> role <@&666>", amap, emoji, names.get)
eq("channel/role mentions untouched", t, "chan <#444> role <@&666>")

t = relay.translate_content("<:pepe:55555> <a:spin:55555>", amap, emoji, names.get)
eq("known static+animated emoji kept", t, "<:pepe:55555> <a:spin:55555>")

t = relay.translate_content("<:nope:777> <a:nope2:888>", amap, emoji, names.get)
eq("unknown emoji -> :name:", t, ":nope: :nope2:")

# ---- split_content ----
eq("short text one chunk", relay.split_content("hello"), ["hello"])
eq("empty text -> no chunks", relay.split_content(""), [])
long = "a" * 4000 + " " + "b" * 100
c = relay.split_content(long)
check("space split: 2 chunks, none empty", len(c) == 2 and all(c) and c[0].endswith("a") and c[1] == "b" * 100)
big = "x" * 9000
c = relay.split_content(big)
check("hard cut 9000 -> 4000+4000+1000", [len(x) for x in c] == [4000, 4000, 1000])
nl = "a" * 2000 + "\n" + "b" * 3000
c = relay.split_content(nl)
check("newline split keeps both", len(c) == 2 and c[0] == "a" * 2000 and c[1] == "b" * 3000)
check("exact limit unsplit", relay.split_content("z" * 4000) == ["z" * 4000])

# ---- effective_permissions ----
r = roles((1, SEND | ATTACH), everyone=SEND)
check("member with attach role", bool(relay.effective_permissions(GUILD, [1], 42, r, []) & ATTACH))
check("member with no roles (everyone only)", not relay.effective_permissions(GUILD, [], 42, r, []) & ATTACH)

r = roles((1, SEND | ATTACH), everyone=SEND)
ows = [ow(GUILD, 0, deny=ATTACH)]
check("everyone deny beats role grant", not relay.effective_permissions(GUILD, [1], 42, r, ows) & ATTACH)

r2 = roles((1, SEND | ATTACH), (2, SEND), everyone=SEND)
ows = [ow(GUILD, 0, deny=ATTACH), ow(2, 0, allow=ATTACH)]
check("role overwrite allow beats @everyone deny", bool(relay.effective_permissions(GUILD, [1, 2], 42, r2, ows) & ATTACH))

ows = [ow(2, 0, allow=ATTACH), ow(42, 1, deny=ATTACH)]
check("member deny beats role allow", not relay.effective_permissions(GUILD, [2], 42, r2, ows) & ATTACH)

r3 = roles((3, ADMIN), everyone=0)
eq("administrator -> all bits", relay.effective_permissions(GUILD, [3], 42, r3, []) == (1 << 64) - 1, True)
check("admin beats member deny", bool(relay.effective_permissions(GUILD, [3], 42, r3, [ow(42, 1, deny=ATTACH)]) & ATTACH))

r4 = roles(everyone=SEND | ATTACH)
check("everyone can attach, no member roles", bool(relay.effective_permissions(GUILD, [], 42, r4, []) & ATTACH))
ows = [ow(9, 0, deny=ATTACH)]  # role the member doesn't have
check("overwrite for role member lacks: no-op", bool(relay.effective_permissions(GUILD, [], 42, r4, ows) & ATTACH))

# guild owner has everything, even over an explicit member deny
check("owner uploads despite member deny",
   bool(relay.effective_permissions(GUILD, [], 77, r4, [ow(77, 1, deny=ATTACH)], owner_id=77) & ATTACH))
check("non-owner unaffected by owner_id",
   not relay.effective_permissions(GUILD, [], 42, r4, [ow(42, 1, deny=ATTACH)], owner_id=77) & ATTACH)

# ---- plan_attachments ----
A = lambda i, sz=100: {"filename": f"f{i}.txt", "size": sz}
atts = [A(i) for i in range(3)]
dl, sk = relay.plan_attachments(atts, True, 1000)
check("allowed, all fit", len(dl) == 3 and sk == 0)
dl, sk = relay.plan_attachments(atts, False, 1000)
check("not allowed, all skipped", dl == [] and sk == 3)
dl, sk = relay.plan_attachments([A(1), A(2, 5000), A(3)], True, 1000)
check("oversized skipped, rest kept", [x["filename"] for x in dl] == ["f1.txt", "f3.txt"] and sk == 1)
dl, sk = relay.plan_attachments([A(i) for i in range(12)], True, 1000)
check("over limit: first 10 kept", len(dl) == 10 and sk == 2)
dl, sk = relay.plan_attachments([], True, 1000)
check("empty attachments", dl == [] and sk == 0)
dl, sk = relay.plan_attachments([A(1, 5000), A(2, 100), *[A(i) for i in range(3, 13)]], True, 400)
check("oversize + over limit mix", [x["filename"] for x in dl] == ["f2.txt"] + [f"f{i}.txt" for i in range(3, 12)] and sk == 2)

# ---- is_transient ----
check("network error is transient", relay.is_transient(None))
check("429 transient", relay.is_transient(429))
check("503 transient", relay.is_transient(503))
check("408 transient", relay.is_transient(408))
check("400 permanent", not relay.is_transient(400))
check("403 permanent", not relay.is_transient(403))
check("404 permanent", not relay.is_transient(404))
check("200 not transient", not relay.is_transient(200))

# ---- upload_allowed (SEND|ATTACH + not timed out) ----
BOTH = (1 << 10) | SEND | ATTACH  # VIEW|SEND|ATTACH, as Bonfire requires
member = {"roles": [], "communication_disabled_until": None}
eq("no member -> no upload", relay.upload_allowed(BOTH, None, 0), False)
eq("attach without send_messages", relay.upload_allowed(ATTACH, member, 0), False)
eq("send without attach", relay.upload_allowed(SEND, member, 0), False)
eq("both bits, no timeout", relay.upload_allowed(BOTH, member, 0), True)
future = {"communication_disabled_until": "2999-01-01T00:00:00Z"}
eq("timed out until future", relay.upload_allowed((1 << 64) - 1, future, 0), False)
past = {"communication_disabled_until": "2000-01-01T00:00:00Z"}
eq("expired timeout allowed", relay.upload_allowed(BOTH, past, 9999999999999), True)
eq("admin/owner-level perms upload", relay.upload_allowed((1 << 64) - 1, member, 0), True)

# ---- safe_path ----
eq("webhook token masked",
   relay.safe_path("/webhooks/12345/AbCdEf_sEcReT/messages/999?wait=true"),
   "/webhooks/12345/<token>/messages/999?wait=true")
eq("non-webhook path unchanged",
   relay.safe_path("/channels/42/messages?after=1&limit=100"),
   "/channels/42/messages?after=1&limit=100")



# ---- VIEW_CHANNEL required for upload ----
VIEW = 1 << 10
eq("send+attach without view -> refused", relay.upload_allowed(SEND | ATTACH, member, 0), False)
eq("all three -> allowed", relay.upload_allowed(VIEW | SEND | ATTACH, member, 0), True)
eq("timed out with all three", relay.upload_allowed(VIEW | SEND | ATTACH, future, 0), False)
eq("expired timeout with all three", relay.upload_allowed(VIEW | SEND | ATTACH, past, 9999999999999), True)
eq("view only no upload", relay.upload_allowed(VIEW, member, 0), False)

# ---- attachment_ids_to_keep ----
pairs = [{"d": 1, "b": 11}, {"d": 2, "b": 12}, {"d": 3, "b": 13}]
kept, removed = relay.attachment_ids_to_keep(pairs, [1, "3"])
eq("kept ids", [p["b"] for p in kept], [11, 13])
eq("removed ids", [p["b"] for p in removed], [12])
kept, removed = relay.attachment_ids_to_keep(pairs, [1, 2, 3])
eq("nothing removed", (len(kept), len(removed)), (3, 0))
kept, removed = relay.attachment_ids_to_keep([], [])
eq("empty pairs", (kept, removed), ([], []))

# ---- forward_text / poll_text / chunk_nonce ----
eq("forward", relay.forward_text(["snap text"]), "[Forwarded]\nsnap text")
eq("forward multi-snapshot", relay.forward_text(["a", "b"]), "[Forwarded]\na\nb")
eq("forward empty snapshot", relay.forward_text([""]), "[Forwarded]")
eq("poll", relay.poll_text("best map?", ["a", "b"]), "[Poll] best map?\n- a\n- b")
eq("poll no answers", relay.poll_text("q", []), "[Poll] q")
eq("nonce", relay.chunk_nonce(12345, 0), "12345:0")
check("nonce <=32 chars", len(relay.chunk_nonce(9999999999999999999, 9)) <= 32)
check("nonces unique", relay.chunk_nonce(1, 0) != relay.chunk_nonce(1, 1))
v = relay.post_version(["x" * 4000, "y"], [111, 222])
check("versioned nonce <=32 chars", len(relay.chunk_nonce(9999999999999999999, 99, v)) <= 32)
check("repost after a file removal gets a new nonce",
      relay.post_version(["text"], [111, 222]) != relay.post_version(["text"], [111]))
eq("same post, same version", relay.post_version(["text"], [222, 111]), relay.post_version(["text"], [111, 222]))

# ---- execute_webhook multipart (4-tuples) ----
class _FF:
    def __init__(self): self.fields = []
    def add_field(self, *a, **k): self.fields.append((a, k))
_ff = _FF()
_saved_form = relay.aiohttp.FormData
relay.aiohttp.FormData = lambda: _ff
_api = relay.BonfireAPI("http://example.invalid", "tok")
async def _fake_req(method, path, **kw): return 200, json.dumps({"id": "9"}).encode()
_api._req = _fake_req
_st, _d = asyncio.run(_api.execute_webhook(1, "whtok", {"content": "c"},
                                         files=[("f.txt", b"x", "text/plain", 7)]))
relay.aiohttp.FormData = _saved_form
check("multipart 4-tuple form fields",
      [a[0] for a, k in _ff.fields] == ["payload_json", "files[0]"] and _d["id"] == "9")

# ---- nonce versioning ----
eq("original nonce has no version", relay.chunk_nonce(12345, 2), "12345:2")
eq("repost nonce is versioned", relay.chunk_nonce(12345, 2, "abcd1234"), "12345:2:abcd1234")
check("repost nonce differs from original",
      relay.chunk_nonce(12345, 2) != relay.chunk_nonce(12345, 2, "abcd1234"))
check("nonce <=32 chars versioned",
      len(relay.chunk_nonce(9999999999999999999, 9, "ffffffff")) <= 32)

# ---- has_relayable ----
check("sticker-only is relayable", relay.has_relayable("[sticker: x]", []))
check("file-only is relayable", relay.has_relayable("", [("f", 1)]))
check("empty is not relayable", not relay.has_relayable("", []))
check("plain text relayable", relay.has_relayable("hi", []))



# ---- relayed_channel_type ----
check("text relayed", relay.relayed_channel_type(0))
check("news relayed", relay.relayed_channel_type(5))
check("news_thread relayed", relay.relayed_channel_type(10))
check("public_thread relayed", relay.relayed_channel_type(11))
check("private_thread relayed", relay.relayed_channel_type(12))
check("voice not relayed", not relay.relayed_channel_type(2))
check("category not relayed", not relay.relayed_channel_type(4))
check("forum not relayed", not relay.relayed_channel_type(15))

# ---- is_stale_echo ----
check("same content not stale", not relay.is_stale_echo("hi", "hi"))
check("old content is stale", relay.is_stale_echo("old", "new"))
check("no content field not stale", not relay.is_stale_echo(None, "new"))

n_fails = sum(1 for _, ok in CHECKS if not ok)
print(f"\n{len(CHECKS) - n_fails}/{len(CHECKS)} checks passed")
sys.exit(1 if n_fails else 0)
