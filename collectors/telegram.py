"""
NOMOLO Telegram Collector
Parses Telegram Desktop JSON export into JSONL vault.

Telegram Desktop: Settings > Advanced > Export Telegram Data
Produces: result.json with chats, messages, contacts.

Usage:
  Upload the result.json file through the Nomolo web UI.
"""

import hashlib
import json
import logging
import os
from datetime import datetime

from core.vault import flush_entries, load_processed_ids, append_processed_ids

logger = logging.getLogger("nomolo.telegram")


def _make_id(*parts):
    raw = ":".join(str(p) for p in parts)
    return "telegram:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]


def _extract_text(msg):
    """Extract plain text from a Telegram message (handles rich text arrays)."""
    text = msg.get("text", "")
    if isinstance(text, str):
        return text
    if isinstance(text, list):
        parts = []
        for part in text:
            if isinstance(part, str):
                parts.append(part)
            elif isinstance(part, dict):
                parts.append(part.get("text", ""))
        return "".join(parts)
    return str(text)


def parse_export(file_path):
    """Parse a Telegram Desktop JSON export. Returns list of message dicts."""
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    messages = []

    for chat in data.get("chats", {}).get("list", []):
        chat_name = chat.get("name", "Unknown")
        chat_type = chat.get("type", "personal_chat")

        for msg in chat.get("messages", []):
            if msg.get("type") != "message":
                continue

            text = _extract_text(msg)
            if not text or not text.strip():
                continue

            msg_id = msg.get("id", "")
            date_str = msg.get("date", "")
            sender = msg.get("from", msg.get("actor", ""))

            entry = {
                "id": _make_id(chat_name, msg_id),
                "date": date_str,
                "sender": sender,
                "chat": chat_name,
                "chat_type": chat_type,
                "body": text.strip(),
                "body_for_embedding": f"{sender} in {chat_name}: {text.strip()}",
                "type": "message",
            }

            if msg.get("photo"):
                entry["has_photo"] = True
            if msg.get("file"):
                entry["has_file"] = True
            if msg.get("forwarded_from"):
                entry["forwarded_from"] = msg["forwarded_from"]

            messages.append(entry)

    return messages


def run_import(file_path, vault_root=None, config=None):
    """Import a Telegram export into the vault."""
    config = config or {}

    if not os.path.exists(file_path):
        return {"status": "error", "message": f"File not found: {file_path}"}

    vault_path = os.path.join(vault_root or "vaults", "Telegram")
    os.makedirs(vault_path, exist_ok=True)

    seen = load_processed_ids(vault_path)
    messages = parse_export(file_path)
    new_messages = [m for m in messages if m["id"] not in seen]

    if not new_messages:
        logger.info("No new Telegram messages (all %d already processed)", len(messages))
        return {"status": "completed", "records": 0, "total_parsed": len(messages)}

    flush_entries(new_messages, vault_path, "messages.jsonl")
    append_processed_ids(vault_path, [m["id"] for m in new_messages])

    logger.info("Imported %d Telegram messages", len(new_messages))
    return {"status": "completed", "records": len(new_messages), "total_parsed": len(messages)}
