"""
NOMOLO Slack Collector
Parses Slack workspace export (JSON files per channel) into JSONL vault.

Slack export: workspace.slack.com > Settings > Import/Export Data > Export
Produces: ZIP with channels/ folders containing date-based JSON files.

Usage:
  Extract the ZIP and upload the folder through the Nomolo web UI.
"""

import hashlib
import json
import logging
import os
from datetime import datetime

from core.vault import flush_entries, load_processed_ids, append_processed_ids

logger = logging.getLogger("nomolo.slack")


def _make_id(*parts):
    raw = ":".join(str(p) for p in parts)
    return "slack:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]


def _load_users(export_dir):
    """Load user ID -> name mapping from users.json."""
    users_file = os.path.join(export_dir, "users.json")
    if not os.path.exists(users_file):
        return {}
    try:
        with open(users_file, "r", encoding="utf-8") as f:
            users = json.load(f)
        return {u["id"]: u.get("real_name") or u.get("name", u["id"]) for u in users}
    except Exception:
        return {}


def _load_channels(export_dir):
    """Load channel ID -> name mapping from channels.json."""
    channels = {}
    for fname in ("channels.json", "groups.json", "dms.json", "mpims.json"):
        fpath = os.path.join(export_dir, fname)
        if not os.path.exists(fpath):
            continue
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                data = json.load(f)
            for ch in data:
                channels[ch.get("id", "")] = ch.get("name", ch.get("id", ""))
        except Exception:
            continue
    return channels


def parse_export(export_dir):
    """Parse a Slack workspace export directory. Returns list of message dicts."""
    users = _load_users(export_dir)
    messages = []

    # Iterate channel directories
    for item in sorted(os.listdir(export_dir)):
        channel_dir = os.path.join(export_dir, item)
        if not os.path.isdir(channel_dir):
            continue
        if item.startswith(".") or item in ("__MACOSX",):
            continue

        channel_name = item

        # Each file is a date (YYYY-MM-DD.json)
        for fname in sorted(os.listdir(channel_dir)):
            if not fname.endswith(".json"):
                continue

            fpath = os.path.join(channel_dir, fname)
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    day_messages = json.load(f)
            except Exception:
                continue

            for msg in day_messages:
                if msg.get("subtype") in ("channel_join", "channel_leave", "channel_topic", "channel_purpose"):
                    continue

                text = msg.get("text", "")
                if not text or not text.strip():
                    continue

                ts = msg.get("ts", "")
                user_id = msg.get("user", "")
                sender = users.get(user_id, user_id)

                # Convert Slack timestamp to ISO
                date_str = ""
                if ts:
                    try:
                        dt = datetime.fromtimestamp(float(ts))
                        date_str = dt.isoformat()
                    except (ValueError, OSError):
                        date_str = ts

                entry = {
                    "id": _make_id(channel_name, ts),
                    "date": date_str,
                    "sender": sender,
                    "channel": channel_name,
                    "body": text.strip(),
                    "body_for_embedding": f"{sender} in #{channel_name}: {text.strip()}",
                    "type": "message",
                }

                if msg.get("files"):
                    entry["has_files"] = True
                    entry["file_count"] = len(msg["files"])
                if msg.get("reactions"):
                    entry["reactions"] = [
                        {"name": r["name"], "count": r.get("count", 1)}
                        for r in msg["reactions"]
                    ]
                if msg.get("thread_ts") and msg["thread_ts"] != ts:
                    entry["thread_ts"] = msg["thread_ts"]
                    entry["type"] = "thread_reply"

                messages.append(entry)

    return messages


def run_import(export_dir, vault_root=None, config=None):
    """Import a Slack workspace export into the vault."""
    config = config or {}

    if not os.path.isdir(export_dir):
        return {"status": "error", "message": f"Directory not found: {export_dir}"}

    vault_path = os.path.join(vault_root or "vaults", "Slack")
    os.makedirs(vault_path, exist_ok=True)

    seen = load_processed_ids(vault_path)
    messages = parse_export(export_dir)
    new_messages = [m for m in messages if m["id"] not in seen]

    if not new_messages:
        logger.info("No new Slack messages (all %d already processed)", len(messages))
        return {"status": "completed", "records": 0, "total_parsed": len(messages)}

    flush_entries(new_messages, vault_path, "messages.jsonl")
    append_processed_ids(vault_path, [m["id"] for m in new_messages])

    logger.info("Imported %d Slack messages", len(new_messages))
    return {"status": "completed", "records": len(new_messages), "total_parsed": len(messages)}
