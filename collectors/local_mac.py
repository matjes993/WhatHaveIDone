"""
NOMOLO Local Mac Collector
Collects personal data from local macOS databases into the vault system.

Two-step approach:
  1. Quick glimpse — fast preview stats + identity snapshot (seconds)
  2. Deep collection — full vault write in background

Uses the same read-only-copy technique as the browser collector to avoid
locking active databases.
"""

import hashlib
import json
import logging
import os
import shutil
import sqlite3
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from core.vault import flush_entries, load_processed_ids, append_processed_ids

logger = logging.getLogger("nomolo.local_mac")

HOME = Path.home()

# Core Foundation absolute reference date: 2001-01-01 00:00:00 UTC
_CF_EPOCH = datetime(2001, 1, 1)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _copy_db(src: Path) -> Path | None:
    """Copy a SQLite database + WAL/SHM to temp dir."""
    if not src.exists() or not os.access(str(src), os.R_OK):
        return None
    try:
        tmp_dir = Path(tempfile.mkdtemp(prefix="nomolo_"))
        tmp = tmp_dir / src.name
        shutil.copy2(src, tmp)
        for suffix in ["-wal", "-shm"]:
            wal = src.parent / (src.name + suffix)
            if wal.exists():
                shutil.copy2(wal, tmp_dir / (src.name + suffix))
        return tmp
    except OSError as e:
        logger.warning("Could not copy %s: %s", src, e)
        return None


def _query(db_path: Path, sql: str, params: tuple = ()) -> list[dict]:
    """Run a read-only query on a temp copy of the database."""
    tmp = _copy_db(db_path)
    if not tmp:
        return []
    try:
        conn = sqlite3.connect(f"file:{tmp}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        rows = [dict(r) for r in conn.execute(sql, params)]
        conn.close()
        return rows
    except sqlite3.Error as e:
        logger.warning("Query on %s failed: %s", db_path, e)
        return []
    finally:
        try:
            tmp.unlink()
            for suffix in ["-wal", "-shm"]:
                f = tmp.parent / (tmp.name + suffix)
                if f.exists():
                    f.unlink()
            tmp.parent.rmdir()
        except OSError:
            pass


def _make_id(source: str, *parts: str) -> str:
    """Deterministic ID for deduplication."""
    raw = ":".join([source] + list(parts))
    return f"local:{source}:{hashlib.md5(raw.encode()).hexdigest()[:12]}"


def _cf_to_datetime(cf_timestamp: float | None) -> datetime | None:
    """Convert Core Foundation timestamp to datetime."""
    if cf_timestamp is None or cf_timestamp == 0:
        return None
    try:
        return _CF_EPOCH + timedelta(seconds=cf_timestamp)
    except (OverflowError, ValueError, OSError):
        return None


# ---------------------------------------------------------------------------
# Individual collectors — write to vault
# ---------------------------------------------------------------------------

def collect_contacts(vault_root: str) -> dict:
    """Collect macOS Contacts into vault."""
    vault_path = os.path.join(vault_root, "Contacts")
    processed = load_processed_ids(vault_path)

    ab_dir = HOME / "Library" / "Application Support" / "AddressBook"
    db_path = None
    if (ab_dir / "AddressBook-v22.abcddb").exists():
        db_path = ab_dir / "AddressBook-v22.abcddb"
    else:
        sources_dir = ab_dir / "Sources"
        if sources_dir.exists():
            for src_dir in sources_dir.iterdir():
                candidate = src_dir / "AddressBook-v22.abcddb"
                if candidate.exists():
                    db_path = candidate
                    break

    if not db_path:
        return {"status": "skipped", "reason": "no_database", "records": 0}

    rows = _query(db_path, """
        SELECT
            ZFIRSTNAME, ZLASTNAME, ZORGANIZATION, ZJOBTITLE,
            ZNICKNAME, ZBIRTHDAY, ZMODIFICATIONDATE, ZCREATIONDATE,
            Z_PK
        FROM ZABCDRECORD
        WHERE ZFIRSTNAME IS NOT NULL OR ZLASTNAME IS NOT NULL
        ORDER BY ZMODIFICATIONDATE DESC
    """)

    entries = []
    new_ids = []
    now = datetime.now().isoformat()

    for r in rows:
        first = r.get("ZFIRSTNAME") or ""
        last = r.get("ZLASTNAME") or ""
        name = f"{first} {last}".strip()
        if not name:
            continue

        entry_id = _make_id("contacts", str(r.get("Z_PK", "")), name)
        if entry_id in processed:
            continue

        org = r.get("ZORGANIZATION") or ""
        job = r.get("ZJOBTITLE") or ""
        modified = _cf_to_datetime(r.get("ZMODIFICATIONDATE"))
        created = _cf_to_datetime(r.get("ZCREATIONDATE"))
        birthday = _cf_to_datetime(r.get("ZBIRTHDAY"))

        entry = {
            "id": entry_id,
            "sources": ["mac_contacts"],
            "type": "contact",
            "name": name,
            "first_name": first,
            "last_name": last,
            "organization": org,
            "job_title": job,
            "birthday": birthday.strftime("%Y-%m-%d") if birthday else None,
            "created_at": created.isoformat() if created else None,
            "modified_at": modified.isoformat() if modified else None,
            "updated_at": now,
            "contact_for_embedding": f"Contact: {name}" + (f" at {org}" if org else "") + (f", {job}" if job else ""),
        }
        entries.append(entry)
        new_ids.append(entry_id)

    if entries:
        flush_entries(entries, vault_path, "contacts.jsonl")
        append_processed_ids(vault_path, new_ids)

    return {"status": "completed", "records": len(entries), "total": len(rows)}


def collect_chrome_bookmarks(vault_root: str) -> dict:
    """Collect Chrome bookmarks into vault."""
    vault_path = os.path.join(vault_root, "Bookmarks")
    processed = load_processed_ids(vault_path)

    bm_path = HOME / "Library" / "Application Support" / "Google" / "Chrome" / "Default" / "Bookmarks"
    if not bm_path.exists():
        return {"status": "skipped", "reason": "no_file", "records": 0}

    try:
        with open(bm_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return {"status": "error", "reason": "parse_error", "records": 0}

    entries = []
    new_ids = []
    now = datetime.now().isoformat()

    def _walk(node, folder_path=""):
        if node.get("type") == "url":
            url = node.get("url", "")
            name = node.get("name", "")
            entry_id = _make_id("chrome_bm", url)
            if entry_id in processed:
                return

            # Chrome timestamps: microseconds since 1601-01-01
            added_raw = node.get("date_added", "0")
            try:
                ts = int(added_raw)
                added = datetime(1601, 1, 1) + timedelta(microseconds=ts) if ts > 0 else None
            except (ValueError, OverflowError, OSError):
                added = None

            from urllib.parse import urlparse
            domain = urlparse(url).netloc if url else ""

            entry = {
                "id": entry_id,
                "sources": ["chrome_bookmarks"],
                "type": "bookmark",
                "name": name,
                "url": url,
                "domain": domain,
                "folder": folder_path,
                "date_added": added.isoformat() if added else None,
                "updated_at": now,
                "bookmark_for_embedding": f"Bookmark: '{name}' ({domain}) in {folder_path}" if folder_path else f"Bookmark: '{name}' ({domain})",
            }
            entries.append(entry)
            new_ids.append(entry_id)

        elif node.get("type") == "folder":
            child_path = f"{folder_path}/{node.get('name', '')}" if folder_path else node.get("name", "")
            for child in node.get("children", []):
                _walk(child, child_path)

    roots = data.get("roots", {})
    for root_name, root_node in roots.items():
        if isinstance(root_node, dict) and "children" in root_node:
            _walk(root_node, root_name)

    if entries:
        flush_entries(entries, vault_path, "chrome_bookmarks.jsonl")
        append_processed_ids(vault_path, new_ids)

    return {"status": "completed", "records": len(entries), "total": len(entries) + len(processed)}


def collect_safari_bookmarks(vault_root: str) -> dict:
    """Collect Safari bookmarks from Bookmarks.plist."""
    vault_path = os.path.join(vault_root, "Bookmarks")
    processed = load_processed_ids(vault_path)

    plist_path = HOME / "Library" / "Safari" / "Bookmarks.plist"
    if not plist_path.exists() or not os.access(str(plist_path), os.R_OK):
        return {"status": "skipped", "reason": "not_accessible", "records": 0}

    import plistlib
    try:
        with open(plist_path, "rb") as f:
            data = plistlib.load(f)
    except Exception:
        return {"status": "error", "reason": "parse_error", "records": 0}

    entries = []
    new_ids = []
    now = datetime.now().isoformat()

    def _walk(node, folder=""):
        if "URLString" in node:
            url = node["URLString"]
            title = node.get("URIDictionary", {}).get("title", "") or node.get("Title", "")
            entry_id = _make_id("safari_bm", url)
            if entry_id in processed:
                return

            from urllib.parse import urlparse
            domain = urlparse(url).netloc if url else ""

            entry = {
                "id": entry_id,
                "sources": ["safari_bookmarks"],
                "type": "bookmark",
                "name": title,
                "url": url,
                "domain": domain,
                "folder": folder,
                "updated_at": now,
                "bookmark_for_embedding": f"Safari bookmark: '{title}' ({domain})" + (f" in {folder}" if folder else ""),
            }
            entries.append(entry)
            new_ids.append(entry_id)

        for child in node.get("Children", []):
            child_folder = f"{folder}/{child.get('Title', '')}" if folder else child.get("Title", "")
            _walk(child, child_folder)

    _walk(data)

    if entries:
        flush_entries(entries, vault_path, "safari_bookmarks.jsonl")
        append_processed_ids(vault_path, new_ids)

    return {"status": "completed", "records": len(entries), "total": len(entries) + len(processed)}


def collect_calendar(vault_root: str) -> dict:
    """Collect macOS Calendar events into vault."""
    vault_path = os.path.join(vault_root, "Calendar")
    processed = load_processed_ids(vault_path)

    db_path = HOME / "Library" / "Calendars" / "Calendar.sqlitedb"
    if not db_path.exists() or not os.access(str(db_path), os.R_OK):
        return {"status": "skipped", "reason": "not_accessible", "records": 0}

    rows = _query(db_path, """
        SELECT
            ci.ROWID, ci.summary, ci.description, ci.location_id,
            ci.start_date, ci.end_date, ci.all_day,
            ci.calendar_id, c.title as calendar_name
        FROM CalendarItem ci
        LEFT JOIN Calendar c ON ci.calendar_id = c.ROWID
        WHERE ci.summary IS NOT NULL AND ci.summary != ''
        ORDER BY ci.start_date DESC
    """)

    entries = []
    new_ids = []
    now = datetime.now().isoformat()

    for r in rows:
        summary = r.get("summary") or ""
        entry_id = _make_id("calendar", str(r.get("ROWID", "")))
        if entry_id in processed:
            continue

        start = _cf_to_datetime(r.get("start_date"))
        end = _cf_to_datetime(r.get("end_date"))
        cal_name = r.get("calendar_name") or ""

        entry = {
            "id": entry_id,
            "sources": ["mac_calendar"],
            "type": "event",
            "title": summary,
            "description": (r.get("description") or "")[:500],
            "calendar": cal_name,
            "all_day": bool(r.get("all_day")),
            "start_date": start.isoformat() if start else None,
            "end_date": end.isoformat() if end else None,
            "updated_at": now,
            "event_for_embedding": f"Event: '{summary}'" + (f" on {start.strftime('%Y-%m-%d')}" if start else "") + (f" ({cal_name})" if cal_name else ""),
        }
        entries.append(entry)
        new_ids.append(entry_id)

    if entries:
        flush_entries(entries, vault_path, "calendar.jsonl")
        append_processed_ids(vault_path, new_ids)

    return {"status": "completed", "records": len(entries), "total": len(rows)}


def collect_imessage(vault_root: str) -> dict:
    """Collect iMessage conversations into vault."""
    vault_path = os.path.join(vault_root, "Messages")
    processed = load_processed_ids(vault_path)

    db_path = HOME / "Library" / "Messages" / "chat.db"
    if not db_path.exists() or not os.access(str(db_path), os.R_OK):
        return {"status": "skipped", "reason": "not_accessible", "records": 0}

    rows = _query(db_path, """
        SELECT
            m.ROWID, m.text, m.date, m.is_from_me,
            m.service,
            h.id as contact_id, h.uncanonicalized_id as contact_name
        FROM message m
        LEFT JOIN handle h ON m.handle_id = h.ROWID
        WHERE m.text IS NOT NULL AND m.text != ''
        ORDER BY m.date DESC
        LIMIT 10000
    """)

    entries = []
    new_ids = []
    now = datetime.now().isoformat()

    for r in rows:
        text = r.get("text") or ""
        entry_id = _make_id("imessage", str(r.get("ROWID", "")))
        if entry_id in processed:
            continue

        # iMessage dates: nanoseconds since 2001-01-01 (after ~2020ish)
        # or seconds since 2001-01-01 (older)
        raw_date = r.get("date")
        msg_date = None
        if raw_date:
            if raw_date > 1_000_000_000_000:  # nanoseconds
                msg_date = _cf_to_datetime(raw_date / 1_000_000_000)
            else:
                msg_date = _cf_to_datetime(raw_date)

        contact = r.get("contact_name") or r.get("contact_id") or "Unknown"
        is_from_me = bool(r.get("is_from_me"))

        entry = {
            "id": entry_id,
            "sources": ["mac_imessage"],
            "type": "message",
            "text": text[:1000],  # Cap for vault storage
            "contact": contact,
            "is_from_me": is_from_me,
            "service": r.get("service") or "iMessage",
            "date": msg_date.isoformat() if msg_date else None,
            "updated_at": now,
            "message_for_embedding": f"{'Sent to' if is_from_me else 'From'} {contact}: {text[:200]}",
        }
        entries.append(entry)
        new_ids.append(entry_id)

    if entries:
        flush_entries(entries, vault_path, "imessage.jsonl")
        append_processed_ids(vault_path, new_ids)

    return {"status": "completed", "records": len(entries), "total": len(rows)}


def collect_notes(vault_root: str) -> dict:
    """Collect Apple Notes into vault."""
    vault_path = os.path.join(vault_root, "Notes")
    processed = load_processed_ids(vault_path)

    db_path = HOME / "Library" / "Group Containers" / "group.com.apple.notes" / "NoteStore.sqlite"
    if not db_path.exists() or not os.access(str(db_path), os.R_OK):
        return {"status": "skipped", "reason": "not_accessible", "records": 0}

    rows = _query(db_path, """
        SELECT
            n.Z_PK, n.ZTITLE, n.ZSNIPPET, n.ZMODIFICATIONDATE, n.ZCREATIONDATE,
            f.ZTITLE2 as folder_name
        FROM ZICCLOUDSYNCINGOBJECT n
        LEFT JOIN ZICCLOUDSYNCINGOBJECT f ON n.ZFOLDER = f.Z_PK
        WHERE n.ZTITLE IS NOT NULL AND n.ZTITLE != ''
        AND n.ZMARKEDFORDELETION != 1
        ORDER BY n.ZMODIFICATIONDATE DESC
    """)

    entries = []
    new_ids = []
    now = datetime.now().isoformat()

    for r in rows:
        title = r.get("ZTITLE") or ""
        entry_id = _make_id("notes", str(r.get("Z_PK", "")))
        if entry_id in processed:
            continue

        modified = _cf_to_datetime(r.get("ZMODIFICATIONDATE"))
        created = _cf_to_datetime(r.get("ZCREATIONDATE"))
        snippet = (r.get("ZSNIPPET") or "")[:500]
        folder = r.get("folder_name") or ""

        entry = {
            "id": entry_id,
            "sources": ["mac_notes"],
            "type": "note",
            "title": title,
            "snippet": snippet,
            "folder": folder,
            "created_at": created.isoformat() if created else None,
            "modified_at": modified.isoformat() if modified else None,
            "updated_at": now,
            "note_for_embedding": f"Note: '{title}'" + (f" in {folder}" if folder else "") + (f" — {snippet[:100]}" if snippet else ""),
        }
        entries.append(entry)
        new_ids.append(entry_id)

    if entries:
        flush_entries(entries, vault_path, "notes.jsonl")
        append_processed_ids(vault_path, new_ids)

    return {"status": "completed", "records": len(entries), "total": len(rows)}


def collect_safari_history(vault_root: str) -> dict:
    """Collect Safari browsing history into vault."""
    vault_path = os.path.join(vault_root, "Browser")
    processed = load_processed_ids(vault_path)

    db_path = HOME / "Library" / "Safari" / "History.db"
    if not db_path.exists() or not os.access(str(db_path), os.R_OK):
        return {"status": "skipped", "reason": "not_accessible", "records": 0}

    rows = _query(db_path, """
        SELECT
            hi.id, hi.url, hi.domain_expansion,
            hv.title, hv.visit_time,
            COUNT(*) as visit_count
        FROM history_items hi
        JOIN history_visits hv ON hi.id = hv.history_item
        WHERE hi.url IS NOT NULL
        GROUP BY hi.id
        ORDER BY MAX(hv.visit_time) DESC
        LIMIT 50000
    """)

    entries = []
    new_ids = []
    now = datetime.now().isoformat()

    for r in rows:
        url = r.get("url") or ""
        entry_id = _make_id("safari", str(r.get("id", "")))
        if entry_id in processed:
            continue

        visit_time = _cf_to_datetime(r.get("visit_time"))
        from urllib.parse import urlparse
        domain = r.get("domain_expansion") or urlparse(url).netloc

        entry = {
            "id": entry_id,
            "sources": ["safari"],
            "type": "browse",
            "url": url,
            "title": r.get("title") or "",
            "domain": domain,
            "visit_count": r.get("visit_count", 1),
            "last_visit": visit_time.isoformat() if visit_time else None,
            "updated_at": now,
            "browse_for_embedding": f"Visited '{r.get('title', '')}' ({domain}) — {r.get('visit_count', 1)} visits",
        }
        entries.append(entry)
        new_ids.append(entry_id)

    if entries:
        flush_entries(entries, vault_path, "safari_history.jsonl")
        append_processed_ids(vault_path, new_ids)

    return {"status": "completed", "records": len(entries), "total": len(rows)}


def collect_photos_metadata(vault_root: str) -> dict:
    """Collect Photos metadata (not actual images) into vault."""
    vault_path = os.path.join(vault_root, "Photos")
    processed = load_processed_ids(vault_path)

    db_path = HOME / "Pictures" / "Photos Library.photoslibrary" / "database" / "Photos.sqlite"
    if not db_path.exists() or not os.access(str(db_path), os.R_OK):
        return {"status": "skipped", "reason": "not_accessible", "records": 0}

    rows = _query(db_path, """
        SELECT
            Z_PK, ZDATECREATED,
            ZLATITUDE, ZLONGITUDE, ZDURATION,
            ZWIDTH, ZHEIGHT
        FROM ZASSET
        WHERE ZTRASHEDSTATE = 0
        ORDER BY ZDATECREATED DESC
        LIMIT 50000
    """)

    entries = []
    new_ids = []
    now = datetime.now().isoformat()

    for r in rows:
        entry_id = _make_id("photos", str(r.get("Z_PK", "")))
        if entry_id in processed:
            continue

        created = _cf_to_datetime(r.get("ZDATECREATED"))
        filename = r.get("ZORIGINALFILENAME") or r.get("ZFILENAME") or ""
        lat = r.get("ZLATITUDE")
        lon = r.get("ZLONGITUDE")
        has_location = lat is not None and lon is not None and lat != 0 and lon != 0

        entry = {
            "id": entry_id,
            "sources": ["mac_photos"],
            "type": "photo",
            "filename": filename,
            "date": created.isoformat() if created else None,
            "has_location": has_location,
            "latitude": lat if has_location else None,
            "longitude": lon if has_location else None,
            "width": r.get("ZWIDTH"),
            "height": r.get("ZHEIGHT"),
            "duration": r.get("ZDURATION"),
            "updated_at": now,
            "photo_for_embedding": f"Photo: {filename}" + (f" taken {created.strftime('%Y-%m-%d')}" if created else "") + (" with location" if has_location else ""),
        }
        entries.append(entry)
        new_ids.append(entry_id)

    if entries:
        flush_entries(entries, vault_path, "photos.jsonl")
        append_processed_ids(vault_path, new_ids)

    return {"status": "completed", "records": len(entries), "total": len(rows)}


def collect_mail_metadata(vault_root: str) -> dict:
    """Collect Apple Mail metadata (envelope index) into vault."""
    vault_path = os.path.join(vault_root, "Mail")
    processed = load_processed_ids(vault_path)

    # Mail stores an Envelope Index SQLite database
    mail_dir = HOME / "Library" / "Mail"
    envelope_db = mail_dir / "V10" / "Envelope Index" if (mail_dir / "V10").exists() else None
    if not envelope_db:
        # Try other versions
        for v in ["V9", "V8", "V7"]:
            candidate = mail_dir / v / "Envelope Index"
            if candidate.exists():
                envelope_db = candidate
                break

    if not envelope_db or not envelope_db.exists() or not os.access(str(envelope_db), os.R_OK):
        return {"status": "skipped", "reason": "not_accessible", "records": 0}

    rows = _query(envelope_db, """
        SELECT
            m.ROWID, m.subject, m.sender, m.date_sent, m.date_received,
            m.read, m.flagged
        FROM messages m
        WHERE m.subject IS NOT NULL AND m.subject != ''
        ORDER BY m.date_received DESC
        LIMIT 50000
    """)

    entries = []
    new_ids = []
    now = datetime.now().isoformat()

    for r in rows:
        entry_id = _make_id("mail", str(r.get("ROWID", "")))
        if entry_id in processed:
            continue

        date_received = r.get("date_received")
        # Mail dates are Unix timestamps
        received_dt = None
        if date_received:
            try:
                received_dt = datetime.fromtimestamp(date_received)
            except (ValueError, OSError):
                pass

        sender = r.get("sender") or ""

        entry = {
            "id": entry_id,
            "sources": ["mac_mail"],
            "type": "email",
            "subject": (r.get("subject") or "")[:300],
            "sender": sender,
            "date": received_dt.isoformat() if received_dt else None,
            "read": bool(r.get("read")),
            "flagged": bool(r.get("flagged")),
            "updated_at": now,
            "email_for_embedding": f"Email from {sender}: '{(r.get('subject') or '')[:100]}'" + (f" on {received_dt.strftime('%Y-%m-%d')}" if received_dt else ""),
        }
        entries.append(entry)
        new_ids.append(entry_id)

    if entries:
        flush_entries(entries, vault_path, "apple_mail.jsonl")
        append_processed_ids(vault_path, new_ids)

    return {"status": "completed", "records": len(entries), "total": len(rows)}


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

_COLLECTORS = {
    "contacts": collect_contacts,
    "chrome_bookmarks": collect_chrome_bookmarks,
    "safari_bookmarks": collect_safari_bookmarks,
    "calendar": collect_calendar,
    "imessage": collect_imessage,
    "notes": collect_notes,
    "safari_history": collect_safari_history,
    "photos": collect_photos_metadata,
    "mail": collect_mail_metadata,
}


def collect_all_local(vault_root: str, sources: list[str] | None = None) -> dict:
    """
    Run all (or specified) local Mac collectors.

    Returns:
        {
            "status": "completed",
            "results": { "contacts": {...}, "calendar": {...}, ... },
            "total_records": int,
            "sources_collected": int,
            "sources_skipped": int,
        }
    """
    targets = sources or list(_COLLECTORS.keys())
    results = {}
    total = 0
    collected = 0
    skipped = 0

    for name in targets:
        collector = _COLLECTORS.get(name)
        if not collector:
            results[name] = {"status": "error", "reason": f"unknown collector: {name}"}
            continue

        try:
            result = collector(vault_root)
            results[name] = result
            if result.get("status") == "completed":
                total += result.get("records", 0)
                collected += 1
            else:
                skipped += 1
        except Exception as e:
            logger.error("Collector %s failed: %s", name, e)
            results[name] = {"status": "error", "reason": str(e)}
            skipped += 1

    return {
        "status": "completed",
        "results": results,
        "total_records": total,
        "sources_collected": collected,
        "sources_skipped": skipped,
    }


# ---------------------------------------------------------------------------
# Identity Snapshot — the "magic moment"
# ---------------------------------------------------------------------------

def generate_identity_snapshot(vault_root: str, scan_data: dict | None = None) -> dict:
    """
    Generate a quick identity snapshot from whatever data we have.
    Combines scan preview data + any vault data for a punchy summary.

    Returns insights like:
    - "Your oldest data is from 2014"
    - "327 contacts, 12,000 messages"
    - "Most active platform: YouTube (5,200 visits)"
    - "You have photos from 23 countries"
    """
    insights = []
    stats = {}

    # From scan data (preview)
    if scan_data:
        sources = scan_data.get("sources", {})
        summary = scan_data.get("summary", {})

        total_records = summary.get("total_records", 0)
        if total_records > 0:
            stats["local_records"] = total_records

        # Contact count
        contacts = sources.get("contacts", {})
        if contacts.get("found"):
            count = contacts.get("total", 0)
            if count > 0:
                insights.append({
                    "icon": "\U0001f465",
                    "text": f"{count:,} contact{'s' if count != 1 else ''} in your address book",
                    "category": "people",
                })

        # Messages
        imessage = sources.get("imessage", {})
        if imessage.get("found"):
            count = imessage.get("total", 0)
            if count > 100:
                insights.append({
                    "icon": "\U0001f4ac",
                    "text": f"{count:,} messages in your iMessage history",
                    "category": "messaging",
                })

        # Photos
        photos = sources.get("photos", {})
        if photos.get("found"):
            count = photos.get("total", 0)
            if count > 0:
                insights.append({
                    "icon": "\U0001f5bc\ufe0f",
                    "text": f"{count:,} photos in your library",
                    "category": "media",
                })

        # Notes
        notes = sources.get("notes", {})
        if notes.get("found"):
            count = notes.get("total", 0)
            if count > 0:
                insights.append({
                    "icon": "\U0001f4dd",
                    "text": f"{count:,} notes across your notebooks",
                    "category": "notes",
                })

    # From vault data (already collected) — build the leaderboard
    browser_vault = os.path.join(vault_root, "Browser")
    leaderboard = []
    if os.path.exists(browser_vault):
        urls = 0
        oldest_date = None
        newest_date = None
        top_domains: dict[str, int] = {}
        categories: dict[str, int] = {}
        unique_domains = set()

        for fname in os.listdir(browser_vault):
            if not fname.endswith(".jsonl"):
                continue
            fpath = os.path.join(browser_vault, fname)
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    for line in f:
                        try:
                            record = json.loads(line)
                            urls += 1
                            domain = record.get("domain", "")
                            if domain:
                                # Strip www. for cleaner display
                                clean = domain.removeprefix("www.")
                                top_domains[clean] = top_domains.get(clean, 0) + record.get("visit_count", 1)
                                unique_domains.add(clean)

                            for date_field in ["first_visit", "last_visit", "date"]:
                                dt_str = record.get(date_field)
                                if dt_str:
                                    try:
                                        dt = datetime.fromisoformat(dt_str)
                                        if oldest_date is None or dt < oldest_date:
                                            oldest_date = dt
                                        if newest_date is None or dt > newest_date:
                                            newest_date = dt
                                    except ValueError:
                                        pass
                        except json.JSONDecodeError:
                            pass
            except OSError:
                pass

        # Build the top 5 leaderboard — the viral centerpiece
        if top_domains:
            sorted_domains = sorted(top_domains.items(), key=lambda x: x[1], reverse=True)
            _MEDALS = ["\U0001f947", "\U0001f948", "\U0001f949", "4\ufe0f\u20e3", "5\ufe0f\u20e3"]
            for i, (domain, visits) in enumerate(sorted_domains[:5]):
                leaderboard.append({
                    "rank": i + 1,
                    "medal": _MEDALS[i],
                    "domain": domain,
                    "visits": visits,
                })

            # Total visits for the top 5 combined
            top5_total = sum(d["visits"] for d in leaderboard)
            stats["top5_visits"] = top5_total

        if urls > 0:
            stats["browser_urls"] = urls
            stats["unique_domains"] = len(unique_domains)

        # Timeline insight — always compelling
        if oldest_date:
            years = (datetime.now() - oldest_date).days // 365
            months = (datetime.now() - oldest_date).days // 30
            stats["oldest_data"] = oldest_date.strftime("%Y-%m-%d")
            stats["years_of_history"] = years
            if years > 0:
                insights.append({
                    "icon": "\u23f3",
                    "text": f"Your digital trail goes back to {oldest_date.strftime('%B %Y')} — {years} years",
                    "category": "timeline",
                })
            elif months > 0:
                insights.append({
                    "icon": "\u23f3",
                    "text": f"Your history starts {oldest_date.strftime('%B %Y')} — {months} months of data",
                    "category": "timeline",
                })

        # Big numbers insight
        if urls > 0 and len(unique_domains) > 0:
            insights.append({
                "icon": "\U0001f310",
                "text": f"{urls:,} page visits across {len(unique_domains):,} different sites",
                "category": "browsing",
            })

    # Sort: timeline first, then browsing, then local sources
    priority = {"timeline": 0, "browsing": 1, "people": 2, "messaging": 3, "media": 4, "notes": 5}
    insights.sort(key=lambda x: priority.get(x.get("category", ""), 99))

    return {
        "leaderboard": leaderboard,  # Top 5 sites — the viral hook
        "insights": insights[:4],     # Supporting stats (keep it tight)
        "stats": stats,
        "has_data": len(leaderboard) > 0 or len(insights) > 0,
    }
