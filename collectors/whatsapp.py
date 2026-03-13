"""
NOMOLO WhatsApp Collector
Parses WhatsApp chat exports (text file format) into JSONL vault.

WhatsApp export: Settings > Chats > Export Chat > Without Media
Produces: _chat.txt files with format "DD/MM/YYYY, HH:MM - Sender: Message"

Usage:
  Upload the _chat.txt file through the Nomolo web UI.
"""

import hashlib
import json
import logging
import os
import re
from datetime import datetime

from core.vault import flush_entries, load_processed_ids, append_processed_ids

logger = logging.getLogger("nomolo.whatsapp")

# WhatsApp message patterns (handles multiple date formats)
_MSG_PATTERNS = [
    # DD/MM/YYYY, HH:MM - Sender: Message
    re.compile(r"(\d{1,2}/\d{1,2}/\d{2,4}),?\s+(\d{1,2}:\d{2}(?::\d{2})?(?:\s*[AP]M)?)\s*[-\u2013]\s*(.+?):\s(.+)"),
    # [DD/MM/YYYY, HH:MM:SS] Sender: Message (alternative format)
    re.compile(r"\[(\d{1,2}/\d{1,2}/\d{2,4}),?\s+(\d{1,2}:\d{2}(?::\d{2})?(?:\s*[AP]M)?)\]\s*(.+?):\s(.+)"),
    # MM/DD/YY, HH:MM - Sender: Message (US format)
    re.compile(r"(\d{1,2}/\d{1,2}/\d{2,4}),?\s+(\d{1,2}:\d{2}(?::\d{2})?)\s*[-\u2013]\s*(.+?):\s(.+)"),
]

_SYSTEM_MSG = re.compile(
    r"(\d{1,2}/\d{1,2}/\d{2,4}),?\s+(\d{1,2}:\d{2}(?::\d{2})?(?:\s*[AP]M)?)\s*[-\u2013]\s*(.+)"
)


def _make_id(*parts):
    raw = ":".join(str(p) for p in parts)
    return "whatsapp:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]


def _parse_date(date_str, time_str):
    """Parse WhatsApp date/time into datetime."""
    combined = f"{date_str} {time_str}".strip()
    for fmt in (
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y %H:%M:%S",
        "%m/%d/%y %H:%M",
        "%m/%d/%y %H:%M:%S",
        "%d/%m/%y %H:%M",
        "%d/%m/%y %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%d/%m/%Y %I:%M %p",
        "%d/%m/%Y %I:%M:%S %p",
    ):
        try:
            return datetime.strptime(combined, fmt)
        except ValueError:
            continue
    return None


def parse_chat_export(file_path):
    """Parse a WhatsApp chat export file. Returns list of message dicts."""
    messages = []
    current_msg = None

    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue

            matched = False
            for pattern in _MSG_PATTERNS:
                m = pattern.match(line)
                if m:
                    # Save previous message
                    if current_msg:
                        messages.append(current_msg)

                    date_str, time_str, sender, text = m.groups()
                    dt = _parse_date(date_str, time_str)

                    current_msg = {
                        "id": _make_id(date_str, time_str, sender, text[:50]),
                        "date": dt.isoformat() if dt else f"{date_str} {time_str}",
                        "sender": sender.strip(),
                        "body": text.strip(),
                        "body_for_embedding": f"{sender}: {text}",
                        "type": "message",
                    }
                    matched = True
                    break

            if not matched:
                # System message or continuation of previous message
                if current_msg:
                    current_msg["body"] += "\n" + line
                    current_msg["body_for_embedding"] += " " + line

    if current_msg:
        messages.append(current_msg)

    return messages


def run_import(file_path, vault_root=None, config=None):
    """Import a WhatsApp chat export into the vault."""
    config = config or {}

    if not os.path.exists(file_path):
        logger.error("File not found: %s", file_path)
        return {"status": "error", "message": f"File not found: {file_path}"}

    vault_path = os.path.join(vault_root or "vaults", "WhatsApp")
    os.makedirs(vault_path, exist_ok=True)

    # Load already-processed IDs
    seen = load_processed_ids(vault_path)

    messages = parse_chat_export(file_path)
    new_messages = [m for m in messages if m["id"] not in seen]

    if not new_messages:
        logger.info("No new WhatsApp messages to import (all %d already processed)", len(messages))
        return {"status": "completed", "records": 0, "total_parsed": len(messages)}

    # Extract chat name from filename
    chat_name = os.path.basename(file_path).replace("WhatsApp Chat with ", "").replace(".txt", "").replace("_chat", "")
    for msg in new_messages:
        msg["chat"] = chat_name

    # Flush to vault
    flush_entries(new_messages, vault_path, f"{chat_name}.jsonl")
    append_processed_ids(vault_path, [m["id"] for m in new_messages])

    logger.info("Imported %d WhatsApp messages from %s", len(new_messages), chat_name)
    return {"status": "completed", "records": len(new_messages), "total_parsed": len(messages), "chat": chat_name}
