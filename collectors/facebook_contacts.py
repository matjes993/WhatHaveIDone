"""
WHID Facebook Contacts Collector
Parses a Facebook data export (JSON format) to extract friends and
address-book contacts into a local JSONL vault.

Supports two Facebook export structures:
  - Friends list:  friends_v2 array
  - Address book:  address_book.address_book_v2 array
"""

import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from core.vault import append_processed_ids, flush_entries, load_processed_ids

logger = logging.getLogger("whid.facebook_contacts")

# Possible locations of the friends JSON inside a Facebook export directory.
FRIENDS_CANDIDATES = [
    "friends_and_followers/friends.json",
    "friends/friends.json",
]

ADDRESS_BOOK_CANDIDATES = [
    "about_you/your_address_books.json",
    "about_you/address_book.json",
]


# ── helpers ──────────────────────────────────────────────────────────

def _decode_fb_name(name: str) -> str:
    """Decode Facebook's escaped non-ASCII encoding.

    Facebook JSON exports encode non-ASCII characters as latin-1 byte
    sequences.  Re-encoding to latin-1 and decoding as UTF-8 recovers
    the original string.
    """
    try:
        return name.encode("latin-1").decode("utf-8")
    except (UnicodeDecodeError, UnicodeEncodeError):
        return name


def _make_id(name: str) -> str:
    """Deterministic entry ID from a contact name (12-char hex hash)."""
    digest = hashlib.sha256(name.encode("utf-8")).hexdigest()[:12]
    return f"contacts:facebook:{digest}"


def _ts_to_iso(ts) -> str:
    """Convert a Unix timestamp to an ISO-8601 string, or empty string."""
    if ts is None:
        return ""
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()
    except (ValueError, TypeError, OSError):
        return ""


def _file_mtime_iso(path: str) -> str:
    """Return a file's modification time as an ISO-8601 string."""
    try:
        mtime = os.path.getmtime(path)
        return datetime.fromtimestamp(mtime, tz=timezone.utc).isoformat()
    except OSError:
        return ""


# ── parsers ──────────────────────────────────────────────────────────

def _parse_friends(data: dict, file_path: str) -> list[dict]:
    """Parse the friends_v2 structure and return vault entries."""
    friends = data.get("friends_v2", [])
    if not friends:
        return []

    updated_at = _file_mtime_iso(file_path)
    entries = []

    for friend in friends:
        raw_name = friend.get("name", "")
        if not raw_name:
            continue
        name = _decode_fb_name(raw_name)
        entries.append({
            "id": _make_id(name),
            "sources": ["facebook"],
            "source_id": "",
            "name": {"display": name},
            "emails": [],
            "phones": [],
            "connected_on": _ts_to_iso(friend.get("timestamp")),
            "updated_at": updated_at,
        })

    return entries


def _parse_address_book(data: dict, file_path: str) -> list[dict]:
    """Parse the address_book_v2 structure and return vault entries."""
    # The structure may be nested under an "address_book" key or at the top.
    ab_data = data.get("address_book", data)
    contacts = ab_data.get("address_book_v2", [])
    if not contacts:
        return []

    updated_at = _file_mtime_iso(file_path)
    entries = []

    for contact in contacts:
        raw_name = contact.get("name", "")
        if not raw_name:
            continue
        name = _decode_fb_name(raw_name)

        emails = []
        phones = []
        for detail in contact.get("details", []):
            cp = detail.get("contact_point", "")
            if not cp:
                continue
            if "@" in cp:
                emails.append(cp)
            else:
                phones.append(cp)

        entries.append({
            "id": _make_id(name),
            "sources": ["facebook"],
            "source_id": "",
            "name": {"display": name},
            "emails": emails,
            "phones": phones,
            "connected_on": _ts_to_iso(contact.get("timestamp")),
            "updated_at": updated_at,
        })

    return entries


# ── file discovery ───────────────────────────────────────────────────

def _find_files(export_path: str) -> list[tuple[str, str]]:
    """Return a list of (file_path, kind) tuples found in export_path.

    kind is either "friends" or "address_book".
    If export_path points directly to a JSON file, return it as-is.
    """
    p = Path(export_path)

    if p.is_file() and p.suffix == ".json":
        return [(str(p), "auto")]

    if not p.is_dir():
        return []

    found = []
    for candidate in FRIENDS_CANDIDATES:
        full = p / candidate
        if full.is_file():
            found.append((str(full), "friends"))

    for candidate in ADDRESS_BOOK_CANDIDATES:
        full = p / candidate
        if full.is_file():
            found.append((str(full), "address_book"))

    return found


# ── main entry point ─────────────────────────────────────────────────

def run_import(export_path: str, config: dict | None = None):
    """Import Facebook contacts from a data export into the vault.

    Args:
        export_path: Path to a Facebook export JSON file or the root
                     directory of an extracted Facebook data download.
        config:      Configuration dict; uses ``vault_root`` key to
                     determine where the vault lives.
    """
    config = config or {}
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    vault_root_base = config.get("vault_root", os.path.join(project_root, "vaults"))
    vault_root_base = os.path.expanduser(vault_root_base)
    if not os.path.isabs(vault_root_base):
        vault_root_base = os.path.join(project_root, vault_root_base)
    vault_path = os.path.join(vault_root_base, "Contacts")

    os.makedirs(vault_path, exist_ok=True)

    print(f"\n  Facebook Contacts Import")
    print(f"  Export: {export_path}")
    print(f"  Saving to: {vault_path}")

    # Discover files to process
    files = _find_files(export_path)
    if not files:
        print("  No Facebook export files found.")
        print(f"  Expected friends.json or address book JSON in: {export_path}")
        return

    print(f"  Found {len(files)} file(s) to process")

    # Load already-processed IDs
    processed_ids = load_processed_ids(vault_path)
    if processed_ids:
        print(f"  Already processed: {len(processed_ids):,}")

    all_entries: list[dict] = []

    for file_path, kind in files:
        try:
            with open(file_path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
        except (json.JSONDecodeError, OSError) as exc:
            logger.error("Failed to read %s: %s", file_path, exc)
            continue

        entries: list[dict] = []

        if kind == "friends":
            entries = _parse_friends(data, file_path)
        elif kind == "address_book":
            entries = _parse_address_book(data, file_path)
        elif kind == "auto":
            entries = _parse_friends(data, file_path)
            entries.extend(_parse_address_book(data, file_path))

        logger.info("  Parsed %d entries from %s", len(entries), file_path)
        all_entries.extend(entries)

    # Deduplicate by ID (keep first occurrence)
    seen: set[str] = set()
    unique_entries: list[dict] = []
    for entry in all_entries:
        if entry["id"] not in seen:
            seen.add(entry["id"])
            unique_entries.append(entry)

    # Filter out already-processed
    new_entries = [e for e in unique_entries if e["id"] not in processed_ids]

    if not new_entries:
        print("  Nothing new -- vault is up to date.")
        return

    flush_entries(new_entries, vault_path, "contacts.jsonl")
    append_processed_ids(vault_path, [e["id"] for e in new_entries])

    print()
    print(f"  {'=' * 40}")
    print(f"    New contacts added: {len(new_entries):,}")
    print(f"    Saved to: {vault_path}")
    print(f"  {'=' * 40}")
