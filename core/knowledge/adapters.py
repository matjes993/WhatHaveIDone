"""
Nomolo Knowledge Graph — Vault-to-Canonical Adapters

Each adapter reads raw vault JSONL entries (as dicts) and yields
CanonicalRecords for the GraphBuilder. Adding a new data source =
writing one adapter function here. Zero changes to graph builder.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Iterator

from core.knowledge.schema import CanonicalRecord, EntityType


# ---------------------------------------------------------------------------
# Gmail adapter
# ---------------------------------------------------------------------------

def adapt_gmail(entries: Iterator[dict]) -> Iterator[CanonicalRecord]:
    """Convert raw Gmail vault entries to canonical MESSAGE records."""
    for entry in entries:
        sender = entry.get("from", "")
        to_raw = entry.get("to", "")
        cc_raw = entry.get("cc", "")

        yield CanonicalRecord(
            record_type=EntityType.MESSAGE,
            source_name="gmail",
            source_id=entry.get("id", ""),
            data={
                "subject": entry.get("subject", ""),
                "body": entry.get("body_clean", entry.get("body_raw", "")),
                "sender": _parse_email_header(sender),
                "recipients": _parse_email_list(to_raw),
                "cc": _parse_email_list(cc_raw),
                "date": entry.get("date", ""),
                "thread_id": entry.get("threadId", ""),
                "attachments": entry.get("attachments", []),
                "is_automated": _is_automated(sender),
            },
            raw=entry,
        )


# ---------------------------------------------------------------------------
# Google Contacts adapter
# ---------------------------------------------------------------------------

def adapt_google_contacts(entries: Iterator[dict]) -> Iterator[CanonicalRecord]:
    """Convert Google Contacts vault entries to canonical PERSON records."""
    for entry in entries:
        name_obj = entry.get("name", {})
        emails = entry.get("emails", [])
        phones = entry.get("phones", [])
        orgs = entry.get("organizations", [])

        yield CanonicalRecord(
            record_type=EntityType.PERSON,
            source_name="google_contacts",
            source_id=entry.get("id", entry.get("source_id", "")),
            data={
                "name": name_obj.get("display", "") if isinstance(name_obj, dict) else str(name_obj),
                "given_name": name_obj.get("given", "") if isinstance(name_obj, dict) else "",
                "family_name": name_obj.get("family", "") if isinstance(name_obj, dict) else "",
                "emails": emails,
                "phones": phones,
                "organizations": orgs,
                "nicknames": entry.get("nicknames", []),
                "addresses": entry.get("addresses", []),
                "birthdays": entry.get("birthdays", []),
                "urls": entry.get("urls", []),
            },
            raw=entry,
        )


# ---------------------------------------------------------------------------
# Mac Contacts adapter
# ---------------------------------------------------------------------------

def adapt_mac_contacts(entries: Iterator[dict]) -> Iterator[CanonicalRecord]:
    """Convert Mac Contacts vault entries to canonical PERSON records."""
    for entry in entries:
        name_obj = entry.get("name", {})
        emails = entry.get("emails", [])
        phones = entry.get("phones", [])
        orgs = entry.get("organizations", [])

        yield CanonicalRecord(
            record_type=EntityType.PERSON,
            source_name="apple_contacts",
            source_id=entry.get("id", entry.get("source_id", "")),
            data={
                "name": name_obj.get("display", "") if isinstance(name_obj, dict) else str(name_obj),
                "given_name": name_obj.get("given", "") if isinstance(name_obj, dict) else "",
                "family_name": name_obj.get("family", "") if isinstance(name_obj, dict) else "",
                "emails": emails,
                "phones": phones,
                "organizations": orgs,
                "nicknames": entry.get("nicknames", []),
                "addresses": entry.get("addresses", []),
            },
            raw=entry,
        )


# ---------------------------------------------------------------------------
# Calendar adapter
# ---------------------------------------------------------------------------

def adapt_calendar(entries: Iterator[dict]) -> Iterator[CanonicalRecord]:
    """Convert Calendar vault entries to canonical EVENT records."""
    for entry in entries:
        attendees_raw = entry.get("attendees", [])
        attendees = []
        for att in attendees_raw:
            if isinstance(att, dict):
                email = att.get("email", "")
                name = att.get("displayName", att.get("name", ""))
                if email:
                    attendees.append({"email": email, "name": name})
            elif isinstance(att, str):
                attendees.append(att)

        yield CanonicalRecord(
            record_type=EntityType.EVENT,
            source_name="google_calendar",
            source_id=entry.get("id", ""),
            data={
                "title": entry.get("summary", entry.get("title", "")),
                "description": entry.get("description", ""),
                "start": _extract_datetime(entry.get("start", {})),
                "end": _extract_datetime(entry.get("end", {})),
                "location": entry.get("location", ""),
                "attendees": attendees,
                "status": entry.get("status", ""),
            },
            raw=entry,
        )


# ---------------------------------------------------------------------------
# iMessage adapter
# ---------------------------------------------------------------------------

def adapt_imessage(entries: Iterator[dict]) -> Iterator[CanonicalRecord]:
    """Convert iMessage vault entries to canonical MESSAGE records."""
    for entry in entries:
        contact = entry.get("contact", "")
        is_from_me = entry.get("is_from_me", False)

        if is_from_me:
            sender = "me"
            recipients = [contact]
        else:
            sender = contact
            recipients = ["me"]

        yield CanonicalRecord(
            record_type=EntityType.MESSAGE,
            source_name="apple_imessage",
            source_id=entry.get("id", ""),
            data={
                "body": entry.get("text", ""),
                "sender": sender,
                "recipients": recipients,
                "date": entry.get("date", ""),
            },
            raw=entry,
        )


# ---------------------------------------------------------------------------
# WhatsApp adapter
# ---------------------------------------------------------------------------

def adapt_whatsapp(entries: Iterator[dict]) -> Iterator[CanonicalRecord]:
    """Convert WhatsApp vault entries to canonical MESSAGE records."""
    for entry in entries:
        sender = entry.get("sender", entry.get("from", ""))
        chat = entry.get("chat", entry.get("group", ""))

        yield CanonicalRecord(
            record_type=EntityType.MESSAGE,
            source_name="whatsapp",
            source_id=entry.get("id", ""),
            data={
                "body": entry.get("text", entry.get("message", "")),
                "sender": sender,
                "recipients": [chat] if chat else [],
                "date": entry.get("date", entry.get("timestamp", "")),
            },
            raw=entry,
        )


# ---------------------------------------------------------------------------
# Telegram adapter
# ---------------------------------------------------------------------------

def adapt_telegram(entries: Iterator[dict]) -> Iterator[CanonicalRecord]:
    """Convert Telegram vault entries to canonical MESSAGE records."""
    for entry in entries:
        sender = entry.get("from", entry.get("sender", ""))

        yield CanonicalRecord(
            record_type=EntityType.MESSAGE,
            source_name="telegram",
            source_id=entry.get("id", ""),
            data={
                "body": entry.get("text", ""),
                "sender": sender,
                "recipients": [],
                "date": entry.get("date", ""),
            },
            raw=entry,
        )


# ---------------------------------------------------------------------------
# Slack adapter
# ---------------------------------------------------------------------------

def adapt_slack(entries: Iterator[dict]) -> Iterator[CanonicalRecord]:
    """Convert Slack vault entries to canonical MESSAGE records."""
    for entry in entries:
        yield CanonicalRecord(
            record_type=EntityType.MESSAGE,
            source_name="slack",
            source_id=entry.get("id", ""),
            data={
                "body": entry.get("text", ""),
                "sender": entry.get("sender", entry.get("user", entry.get("from", ""))),
                "recipients": [],
                "date": entry.get("date", entry.get("ts", "")),
            },
            raw=entry,
        )


# ---------------------------------------------------------------------------
# Browser History adapter
# ---------------------------------------------------------------------------

def adapt_browser_history(entries: Iterator[dict]) -> Iterator[CanonicalRecord]:
    """Convert browser history vault entries to canonical BOOKMARK records."""
    for entry in entries:
        yield CanonicalRecord(
            record_type=EntityType.BOOKMARK,
            source_name="browser_history",
            source_id=entry.get("id", ""),
            data={
                "url": entry.get("url", ""),
                "title": entry.get("title", ""),
                "created": entry.get("last_visit", entry.get("date", "")),
            },
            raw=entry,
        )


# ---------------------------------------------------------------------------
# Bookmarks adapter
# ---------------------------------------------------------------------------

def adapt_bookmarks(entries: Iterator[dict]) -> Iterator[CanonicalRecord]:
    """Convert bookmarks vault entries to canonical BOOKMARK records."""
    for entry in entries:
        yield CanonicalRecord(
            record_type=EntityType.BOOKMARK,
            source_name="chrome_bookmarks",
            source_id=entry.get("id", ""),
            data={
                "url": entry.get("url", ""),
                "title": entry.get("title", entry.get("name", "")),
                "created": entry.get("date_added", entry.get("date", "")),
                "tags": entry.get("tags", []),
            },
            raw=entry,
        )


# ---------------------------------------------------------------------------
# Notes adapter
# ---------------------------------------------------------------------------

def adapt_notes(entries: Iterator[dict]) -> Iterator[CanonicalRecord]:
    """Convert Notes vault entries to canonical NOTE records."""
    for entry in entries:
        yield CanonicalRecord(
            record_type=EntityType.NOTE,
            source_name="apple_notes",
            source_id=entry.get("id", ""),
            data={
                "title": entry.get("title", entry.get("subject", "")),
                "body": entry.get("body", entry.get("text", "")),
                "created": entry.get("created", entry.get("date", "")),
                "modified": entry.get("modified", entry.get("updated_at", "")),
            },
            raw=entry,
        )


# ---------------------------------------------------------------------------
# Deep Scan adapter
# ---------------------------------------------------------------------------

def adapt_deep_scan(entries: Iterator[dict]) -> Iterator[CanonicalRecord]:
    """Convert Deep Scan vault entries to canonical FILE records."""
    for entry in entries:
        yield CanonicalRecord(
            record_type=EntityType.FILE,
            source_name="deep_scan",
            source_id=entry.get("id", ""),
            data={
                "name": entry.get("filename", ""),
                "path": entry.get("path", ""),
                "mime_type": entry.get("mime_type", ""),
                "size_bytes": entry.get("size_bytes", 0),
                "created": entry.get("created", ""),
                "modified": entry.get("modified", ""),
                "media_type": entry.get("file_type", ""),
            },
            raw=entry,
        )


# ---------------------------------------------------------------------------
# Vault reader — reads all sources from a vault directory
# ---------------------------------------------------------------------------

def read_vault_jsonl(vault_root: str) -> dict[str, list[dict]]:
    """
    Read all JSONL files from a vault directory, grouped by source type.
    Returns a dict like {"gmail": [...], "contacts": [...], ...}
    """
    import json
    from pathlib import Path

    vault = Path(vault_root)
    result: dict[str, list[dict]] = {}

    source_dirs = {
        "gmail": "Gmail_Primary",
        "google_contacts": "Contacts_Google",
        "mac_contacts": "Contacts",
        "calendar": "Calendar",
        "imessage": "Messages",
        "whatsapp": "WhatsApp",
        "telegram": "Telegram",
        "slack": "Slack",
        "browser_history": "Browser",
        "bookmarks": "Bookmarks",
        "notes": "Notes",
        "deep_scan": "DeepScan",
    }

    for source_key, path_spec in source_dirs.items():
        entries = []
        full_path = vault / path_spec

        if full_path.is_file() and full_path.suffix == ".jsonl":
            entries.extend(_read_jsonl_file(full_path))
        elif full_path.is_dir():
            for jsonl_file in sorted(full_path.rglob("*.jsonl*")):
                entries.extend(_read_jsonl_file(jsonl_file))

        if entries:
            result[source_key] = entries

    return result


def _read_jsonl_file(path) -> list[dict]:
    """Read a single JSONL or JSONL.ZST file and return list of dicts."""
    import json

    entries = []
    fh = None
    path_str = str(path)

    if path_str.endswith(".zst"):
        try:
            import zstandard as zstd
            import io
            dctx = zstd.ZstdDecompressor()
            raw = open(path_str, "rb")
            reader = dctx.stream_reader(raw)
            fh = io.TextIOWrapper(reader, encoding="utf-8")
        except ImportError:
            return entries
    else:
        fh = open(path_str, "r", encoding="utf-8")

    if fh is None:
        return entries

    with fh:
        for line in fh:
            line = line.strip()
            if line:
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return entries


def adapt_all(vault_data: dict[str, list[dict]]) -> Iterator[CanonicalRecord]:
    """
    Route vault data through the correct adapter based on source key.
    Yields all CanonicalRecords from all sources.
    """
    adapters = {
        "gmail": adapt_gmail,
        "google_contacts": adapt_google_contacts,
        "mac_contacts": adapt_mac_contacts,
        "calendar": adapt_calendar,
        "imessage": adapt_imessage,
        "whatsapp": adapt_whatsapp,
        "telegram": adapt_telegram,
        "slack": adapt_slack,
        "browser_history": adapt_browser_history,
        "bookmarks": adapt_bookmarks,
        "notes": adapt_notes,
        "deep_scan": adapt_deep_scan,
    }

    for source_key, entries in vault_data.items():
        adapter = adapters.get(source_key)
        if adapter:
            yield from adapter(iter(entries))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")


def _parse_email_header(header: str) -> str:
    """Extract email from a 'Name <email>' header. Returns email or raw string."""
    if not header:
        return ""
    match = _EMAIL_RE.search(header)
    return match.group(0).lower() if match else header.strip()


def _parse_email_list(header: str) -> list[str]:
    """Parse a comma-separated email header into a list of email addresses."""
    if not header:
        return []
    return [_parse_email_header(h) for h in header.split(",") if h.strip()]


def _is_automated(sender: str) -> bool:
    """Heuristic: is this sender an automated notification?"""
    if not sender:
        return False
    lower = sender.lower()
    return any(kw in lower for kw in ["noreply", "no-reply", "notification", "newsletter", "donotreply"])


def _extract_datetime(dt_obj) -> str:
    """Extract datetime string from Google Calendar dateTime/date object."""
    if isinstance(dt_obj, dict):
        return dt_obj.get("dateTime", dt_obj.get("date", ""))
    return str(dt_obj) if dt_obj else ""
