"""
NOMOLO Local Mac Scanner — Preview Mode

Scans local macOS databases in READ-ONLY mode to discover what personal data
exists on the user's machine. Returns structured preview data for the knowledge
graph WITHOUT collecting or storing anything.

Collection only happens after explicit user consent.

Requires: macOS Full Disk Access for Terminal/app to read protected databases.
"""

from __future__ import annotations

import logging
import os
import plistlib
import shutil
import sqlite3
import tempfile
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

logger = logging.getLogger("nomolo.local_scanner")

# ---------------------------------------------------------------------------
# macOS data source paths
# ---------------------------------------------------------------------------

HOME = Path.home()

# Each source: (name, path_pattern, description)
_SOURCES = {
    "contacts": {
        "name": "Contacts",
        "icon": "\U0001f465",
        "category": "people",
        "color": "#a855f7",
        "paths": [
            HOME / "Library" / "Application Support" / "AddressBook" / "Sources",
            HOME / "Library" / "Application Support" / "AddressBook" / "AddressBook-v22.abcddb",
        ],
    },
    "calendar": {
        "name": "Calendar",
        "icon": "\U0001f4c5",
        "category": "events",
        "color": "#00d4ff",
        "paths": [
            HOME / "Library" / "Calendars" / "Calendar.sqlitedb",
        ],
    },
    "imessage": {
        "name": "iMessage",
        "icon": "\U0001f4ac",
        "category": "messaging",
        "color": "#00ff88",
        "paths": [
            HOME / "Library" / "Messages" / "chat.db",
        ],
    },
    "notes": {
        "name": "Apple Notes",
        "icon": "\U0001f4dd",
        "category": "notes",
        "color": "#ffd700",
        "paths": [
            HOME / "Library" / "Group Containers" / "group.com.apple.notes" / "NoteStore.sqlite",
        ],
    },
    "mail": {
        "name": "Apple Mail",
        "icon": "\u2709\ufe0f",
        "category": "email",
        "color": "#00d4ff",
        "paths": [
            HOME / "Library" / "Mail",
        ],
    },
    "photos": {
        "name": "Photos",
        "icon": "\U0001f5bc\ufe0f",
        "category": "media",
        "color": "#ff6b9d",
        "paths": [
            HOME / "Pictures" / "Photos Library.photoslibrary" / "database" / "Photos.sqlite",
        ],
    },
    "safari_history": {
        "name": "Safari History",
        "icon": "\U0001f310",
        "category": "browsing",
        "color": "#87ceeb",
        "paths": [
            HOME / "Library" / "Safari" / "History.db",
        ],
    },
    "safari_bookmarks": {
        "name": "Safari Bookmarks",
        "icon": "\U0001f516",
        "category": "browsing",
        "color": "#87ceeb",
        "paths": [
            HOME / "Library" / "Safari" / "Bookmarks.plist",
        ],
    },
    "chrome_bookmarks": {
        "name": "Chrome Bookmarks",
        "icon": "\U0001f516",
        "category": "browsing",
        "color": "#87ceeb",
        "paths": [
            HOME / "Library" / "Application Support" / "Google" / "Chrome" / "Default" / "Bookmarks",
        ],
    },
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _check_path(path: Path) -> str:
    """Check a path's status: 'readable', 'needs_fda', or 'missing'."""
    if not path.exists():
        return "missing"
    if not os.access(str(path), os.R_OK):
        return "needs_fda"
    return "readable"


def _copy_db_to_temp(src: Path) -> Path | None:
    """Copy a SQLite database to temp so we don't conflict with locks."""
    if not src.exists() or not os.access(str(src), os.R_OK):
        return None
    try:
        tmp = Path(tempfile.mkdtemp()) / src.name
        shutil.copy2(src, tmp)
        # Also copy WAL and SHM files if they exist
        for suffix in ["-wal", "-shm"]:
            wal = src.parent / (src.name + suffix)
            if wal.exists():
                shutil.copy2(wal, tmp.parent / (tmp.name + suffix))
        return tmp
    except (OSError, shutil.SameFileError) as e:
        logger.warning("Could not copy %s: %s", src, e)
        return None


def _safe_query(db_path: Path, sql: str, params: tuple = ()) -> list[dict]:
    """Run a read-only SQL query on a copied database."""
    tmp = _copy_db_to_temp(db_path)
    if not tmp:
        return []
    try:
        conn = sqlite3.connect(f"file:{tmp}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        rows = [dict(r) for r in conn.execute(sql, params)]
        conn.close()
        return rows
    except sqlite3.Error as e:
        logger.warning("Query failed on %s: %s", db_path, e)
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


# ---------------------------------------------------------------------------
# Individual source scanners — preview only, no data stored
# ---------------------------------------------------------------------------

def _scan_contacts() -> dict[str, Any]:
    """Scan macOS Contacts (AddressBook) — return preview stats."""
    # Try the newer path format first
    ab_dir = HOME / "Library" / "Application Support" / "AddressBook"
    db_path = None

    # Look for AddressBook database
    if (ab_dir / "AddressBook-v22.abcddb").exists():
        db_path = ab_dir / "AddressBook-v22.abcddb"
    else:
        # Search in Sources subdirectories
        sources_dir = ab_dir / "Sources"
        if sources_dir.exists():
            for src_dir in sources_dir.iterdir():
                candidate = src_dir / "AddressBook-v22.abcddb"
                if candidate.exists():
                    db_path = candidate
                    break

    if not db_path:
        return {"found": False, "reason": "no_database"}

    # Count contacts
    rows = _safe_query(db_path, "SELECT COUNT(*) as cnt FROM ZABCDRECORD WHERE ZFIRSTNAME IS NOT NULL OR ZLASTNAME IS NOT NULL")
    total = rows[0]["cnt"] if rows else 0

    if total == 0:
        return {"found": False, "reason": "empty"}

    # Get sample names for graph (top 10 most recently modified)
    sample_rows = _safe_query(db_path, """
        SELECT ZFIRSTNAME, ZLASTNAME, ZORGANIZATION, ZMODIFICATIONDATE
        FROM ZABCDRECORD
        WHERE ZFIRSTNAME IS NOT NULL OR ZLASTNAME IS NOT NULL
        ORDER BY ZMODIFICATIONDATE DESC
        LIMIT 10
    """)

    sample_names = []
    for r in sample_rows:
        first = r.get("ZFIRSTNAME") or ""
        last = r.get("ZLASTNAME") or ""
        name = f"{first} {last}".strip()
        if name:
            sample_names.append(name)

    # Count with organizations
    org_rows = _safe_query(db_path, """
        SELECT ZORGANIZATION, COUNT(*) as cnt
        FROM ZABCDRECORD
        WHERE ZORGANIZATION IS NOT NULL AND ZORGANIZATION != ''
        GROUP BY ZORGANIZATION
        ORDER BY cnt DESC
        LIMIT 5
    """)
    top_orgs = [{"name": r["ZORGANIZATION"], "count": r["cnt"]} for r in org_rows]

    return {
        "found": True,
        "total": total,
        "sample_names": sample_names,
        "top_organizations": top_orgs,
        "graph_nodes": [{"id": f"contact_{i}", "label": n, "type": "person"} for i, n in enumerate(sample_names[:5])],
    }


def _scan_calendar() -> dict[str, Any]:
    """Scan macOS Calendar — return preview stats."""
    db_path = HOME / "Library" / "Calendars" / "Calendar.sqlitedb"

    if not db_path.exists():
        return {"found": False, "reason": "no_database"}

    # Count events
    rows = _safe_query(db_path, "SELECT COUNT(*) as cnt FROM CalendarItem")
    total = rows[0]["cnt"] if rows else 0

    if total == 0:
        return {"found": False, "reason": "empty"}

    # Date range
    range_rows = _safe_query(db_path, """
        SELECT MIN(start_date) as earliest, MAX(start_date) as latest
        FROM CalendarItem
        WHERE start_date IS NOT NULL
    """)

    # macOS Calendar uses Core Data timestamps (seconds since 2001-01-01)
    _CD_EPOCH = datetime(2001, 1, 1)
    earliest = None
    latest = None
    if range_rows and range_rows[0]["earliest"]:
        try:
            earliest = (_CD_EPOCH + timedelta(seconds=range_rows[0]["earliest"])).strftime("%Y-%m-%d")
            latest = (_CD_EPOCH + timedelta(seconds=range_rows[0]["latest"])).strftime("%Y-%m-%d")
        except (ValueError, OverflowError):
            pass

    # Sample recent events
    sample_rows = _safe_query(db_path, """
        SELECT summary, start_date
        FROM CalendarItem
        WHERE summary IS NOT NULL AND summary != ''
        ORDER BY start_date DESC
        LIMIT 8
    """)
    sample_events = [r["summary"] for r in sample_rows if r.get("summary")]

    return {
        "found": True,
        "total": total,
        "earliest": earliest,
        "latest": latest,
        "sample_events": sample_events[:5],
        "graph_nodes": [{"id": f"event_{i}", "label": e[:30], "type": "event"} for i, e in enumerate(sample_events[:3])],
    }


def _scan_imessage() -> dict[str, Any]:
    """Scan iMessage/SMS database — return preview stats."""
    db_path = HOME / "Library" / "Messages" / "chat.db"

    if not db_path.exists():
        return {"found": False, "reason": "no_database"}

    # Count messages
    rows = _safe_query(db_path, "SELECT COUNT(*) as cnt FROM message")
    total = rows[0]["cnt"] if rows else 0

    if total == 0:
        return {"found": False, "reason": "empty"}

    # Count unique conversations
    chat_rows = _safe_query(db_path, "SELECT COUNT(*) as cnt FROM chat")
    total_chats = chat_rows[0]["cnt"] if chat_rows else 0

    # Top conversations by message count
    top_rows = _safe_query(db_path, """
        SELECT
            h.id as handle_id,
            COUNT(m.ROWID) as msg_count
        FROM message m
        JOIN handle h ON m.handle_id = h.ROWID
        GROUP BY h.id
        ORDER BY msg_count DESC
        LIMIT 10
    """)

    top_conversations = []
    for r in top_rows:
        handle = r.get("handle_id", "")
        # Anonymize phone numbers for preview (show last 4 digits)
        if handle.startswith("+") or handle.replace("-", "").replace(" ", "").isdigit():
            display = f"***{handle[-4:]}" if len(handle) >= 4 else handle
        else:
            display = handle  # email or name
        top_conversations.append({"contact": display, "messages": r["msg_count"]})

    # Date range
    range_rows = _safe_query(db_path, """
        SELECT MIN(date) as earliest, MAX(date) as latest
        FROM message WHERE date > 0
    """)

    # iMessage dates: nanoseconds since 2001-01-01 (after ~2017) or seconds
    _CD_EPOCH = datetime(2001, 1, 1)
    earliest = None
    latest = None
    if range_rows and range_rows[0]["earliest"]:
        try:
            ts = range_rows[0]["earliest"]
            # Detect nanoseconds vs seconds
            if ts > 1_000_000_000_000:
                ts = ts / 1_000_000_000
            earliest = (_CD_EPOCH + timedelta(seconds=ts)).strftime("%Y-%m-%d")
            ts2 = range_rows[0]["latest"]
            if ts2 > 1_000_000_000_000:
                ts2 = ts2 / 1_000_000_000
            latest = (_CD_EPOCH + timedelta(seconds=ts2)).strftime("%Y-%m-%d")
        except (ValueError, OverflowError):
            pass

    return {
        "found": True,
        "total": total,
        "total_chats": total_chats,
        "earliest": earliest,
        "latest": latest,
        "top_conversations": top_conversations[:5],
        "graph_nodes": [{"id": f"chat_{i}", "label": c["contact"], "type": "person", "weight": c["messages"]}
                        for i, c in enumerate(top_conversations[:5])],
    }


def _scan_notes() -> dict[str, Any]:
    """Scan Apple Notes database — return preview stats."""
    db_path = HOME / "Library" / "Group Containers" / "group.com.apple.notes" / "NoteStore.sqlite"

    if not db_path.exists():
        return {"found": False, "reason": "no_database"}

    # Count notes
    rows = _safe_query(db_path, """
        SELECT COUNT(*) as cnt
        FROM ZICCLOUDSYNCINGOBJECT
        WHERE ZTITLE IS NOT NULL AND ZMARKEDFORDELETION != 1
    """)
    total = rows[0]["cnt"] if rows else 0

    if total == 0:
        # Try alternative table structure
        rows = _safe_query(db_path, "SELECT COUNT(*) as cnt FROM ZICNOTEDATA")
        total = rows[0]["cnt"] if rows else 0

    if total == 0:
        return {"found": False, "reason": "empty"}

    # Sample note titles
    sample_rows = _safe_query(db_path, """
        SELECT ZTITLE, ZMODIFICATIONDATE1
        FROM ZICCLOUDSYNCINGOBJECT
        WHERE ZTITLE IS NOT NULL AND ZMARKEDFORDELETION != 1
        ORDER BY ZMODIFICATIONDATE1 DESC
        LIMIT 5
    """)
    sample_titles = [r["ZTITLE"] for r in sample_rows if r.get("ZTITLE")]

    # Count folders
    folder_rows = _safe_query(db_path, """
        SELECT COUNT(DISTINCT ZFOLDER) as cnt
        FROM ZICCLOUDSYNCINGOBJECT
        WHERE ZFOLDER IS NOT NULL AND ZMARKEDFORDELETION != 1
    """)
    folder_count = folder_rows[0]["cnt"] if folder_rows else 0

    return {
        "found": True,
        "total": total,
        "folders": folder_count,
        "sample_titles": sample_titles,
        "graph_nodes": [{"id": f"note_{i}", "label": t[:25], "type": "note"} for i, t in enumerate(sample_titles[:3])],
    }


def _scan_mail() -> dict[str, Any]:
    """Scan Apple Mail — count emlx files without reading content."""
    mail_dir = HOME / "Library" / "Mail"

    if not mail_dir.exists():
        return {"found": False, "reason": "no_directory"}

    # Count emlx files (each is one email)
    total = 0
    mailbox_count = 0
    try:
        for root, dirs, files in os.walk(str(mail_dir)):
            emlx_count = sum(1 for f in files if f.endswith(".emlx"))
            if emlx_count > 0:
                total += emlx_count
                mailbox_count += 1
    except PermissionError:
        return {"found": False, "reason": "permission_denied"}

    if total == 0:
        return {"found": False, "reason": "empty"}

    return {
        "found": True,
        "total": total,
        "mailboxes": mailbox_count,
        "graph_nodes": [],
    }


def _scan_photos() -> dict[str, Any]:
    """Scan Photos library metadata — count without reading images."""
    db_path = HOME / "Pictures" / "Photos Library.photoslibrary" / "database" / "Photos.sqlite"

    if not db_path.exists():
        return {"found": False, "reason": "no_database"}

    rows = _safe_query(db_path, "SELECT COUNT(*) as cnt FROM ZASSET WHERE ZTRASHEDSTATE = 0")
    total = rows[0]["cnt"] if rows else 0

    if total == 0:
        return {"found": False, "reason": "empty"}

    # Date range
    _CD_EPOCH = datetime(2001, 1, 1)
    range_rows = _safe_query(db_path, """
        SELECT MIN(ZDATECREATED) as earliest, MAX(ZDATECREATED) as latest
        FROM ZASSET WHERE ZTRASHEDSTATE = 0 AND ZDATECREATED IS NOT NULL
    """)

    earliest = None
    latest = None
    if range_rows and range_rows[0]["earliest"]:
        try:
            earliest = (_CD_EPOCH + timedelta(seconds=range_rows[0]["earliest"])).strftime("%Y-%m-%d")
            latest = (_CD_EPOCH + timedelta(seconds=range_rows[0]["latest"])).strftime("%Y-%m-%d")
        except (ValueError, OverflowError):
            pass

    # Count videos vs photos
    video_rows = _safe_query(db_path, "SELECT COUNT(*) as cnt FROM ZASSET WHERE ZTRASHEDSTATE = 0 AND ZKIND = 1")
    video_count = video_rows[0]["cnt"] if video_rows else 0

    return {
        "found": True,
        "total": total,
        "photos": total - video_count,
        "videos": video_count,
        "earliest": earliest,
        "latest": latest,
        "graph_nodes": [],
    }


def _scan_safari_history() -> dict[str, Any]:
    """Scan Safari browsing history."""
    db_path = HOME / "Library" / "Safari" / "History.db"

    if not db_path.exists():
        return {"found": False, "reason": "no_database"}

    rows = _safe_query(db_path, "SELECT COUNT(*) as cnt FROM history_items")
    total = rows[0]["cnt"] if rows else 0

    if total == 0:
        return {"found": False, "reason": "empty"}

    visit_rows = _safe_query(db_path, "SELECT COUNT(*) as cnt FROM history_visits")
    total_visits = visit_rows[0]["cnt"] if visit_rows else 0

    return {
        "found": True,
        "total": total,
        "total_visits": total_visits,
        "graph_nodes": [],
    }


def _scan_safari_bookmarks() -> dict[str, Any]:
    """Scan Safari bookmarks plist."""
    plist_path = HOME / "Library" / "Safari" / "Bookmarks.plist"

    if not plist_path.exists():
        return {"found": False, "reason": "no_file"}

    try:
        with open(plist_path, "rb") as f:
            data = plistlib.load(f)

        # Count bookmarks recursively
        def count_bookmarks(node):
            count = 0
            if node.get("WebBookmarkType") == "WebBookmarkTypeLeaf":
                count += 1
            for child in node.get("Children", []):
                count += count_bookmarks(child)
            return count

        total = count_bookmarks(data)
        return {
            "found": True,
            "total": total,
            "graph_nodes": [],
        }
    except Exception as e:
        logger.warning("Safari bookmarks scan failed: %s", e)
        return {"found": False, "reason": "parse_error"}


def _scan_chrome_bookmarks() -> dict[str, Any]:
    """Scan Chrome bookmarks JSON."""
    bm_path = HOME / "Library" / "Application Support" / "Google" / "Chrome" / "Default" / "Bookmarks"

    if not bm_path.exists():
        return {"found": False, "reason": "no_file"}

    try:
        import json
        with open(bm_path) as f:
            data = json.load(f)

        def count_nodes(node):
            count = 0
            if node.get("type") == "url":
                count += 1
            for child in node.get("children", []):
                count += count_nodes(child)
            return count

        total = 0
        for root_key in data.get("roots", {}):
            root = data["roots"][root_key]
            if isinstance(root, dict):
                total += count_nodes(root)

        return {
            "found": True,
            "total": total,
            "graph_nodes": [],
        }
    except Exception as e:
        logger.warning("Chrome bookmarks scan failed: %s", e)
        return {"found": False, "reason": "parse_error"}


# ---------------------------------------------------------------------------
# Main scanner — runs all source scanners in preview mode
# ---------------------------------------------------------------------------

_SCANNERS = {
    "contacts": _scan_contacts,
    "calendar": _scan_calendar,
    "imessage": _scan_imessage,
    "notes": _scan_notes,
    "mail": _scan_mail,
    "photos": _scan_photos,
    "safari_history": _scan_safari_history,
    "safari_bookmarks": _scan_safari_bookmarks,
    "chrome_bookmarks": _scan_chrome_bookmarks,
}


def scan_local_mac() -> dict[str, Any]:
    """
    Scan all local macOS data sources in preview mode.

    Returns a dict with:
      - sources: dict of source_id -> preview data
      - summary: high-level stats
      - graph_nodes: combined nodes for knowledge graph
      - graph_edges: edges connecting nodes to "You"
      - needs_full_disk_access: True if some sources failed due to permissions
    """
    results = {}
    all_graph_nodes = []
    all_graph_edges = []
    total_records = 0
    needs_fda = False
    sources_found = 0

    sources_locked = 0  # exist but need Full Disk Access

    for source_id, scanner_fn in _SCANNERS.items():
        meta = _SOURCES.get(source_id, {})

        # Pre-check: does the file exist but lack permission?
        paths = meta.get("paths", [])
        path_status = "missing"
        for p in paths:
            s = _check_path(p)
            if s == "readable":
                path_status = "readable"
                break
            elif s == "needs_fda":
                path_status = "needs_fda"

        if path_status == "needs_fda":
            result = {"found": False, "reason": "needs_fda", "exists": True}
            needs_fda = True
            sources_locked += 1
        elif path_status == "missing":
            result = {"found": False, "reason": "missing"}
        else:
            try:
                result = scanner_fn()
            except PermissionError:
                result = {"found": False, "reason": "needs_fda", "exists": True}
                needs_fda = True
                sources_locked += 1
            except Exception as e:
                logger.warning("Scanner %s failed: %s", source_id, e)
                result = {"found": False, "reason": str(e)}

        result["source_id"] = source_id
        result["name"] = meta.get("name", source_id)
        result["icon"] = meta.get("icon", "")
        result["category"] = meta.get("category", "other")
        result["color"] = meta.get("color", "#888")
        results[source_id] = result

        if result.get("found"):
            sources_found += 1
            count = result.get("total", 0)
            total_records += count

            # Add source as a graph node (found = solid)
            all_graph_nodes.append({
                "id": source_id,
                "label": result["name"],
                "icon": result["icon"],
                "type": "source",
                "category": result["category"],
                "color": result["color"],
                "count": count,
            })
            all_graph_edges.append({
                "source": "user",
                "target": source_id,
                "weight": count,
            })

            # Add detail nodes (people, events, etc.)
            for node in result.get("graph_nodes", []):
                node["parent"] = source_id
                node["color"] = result["color"]
                all_graph_nodes.append(node)
                all_graph_edges.append({
                    "source": source_id,
                    "target": node["id"],
                    "weight": node.get("weight", 1),
                })

        elif result.get("exists"):
            # Source exists but locked — show as locked node in graph
            all_graph_nodes.append({
                "id": source_id,
                "label": result["name"],
                "icon": result["icon"],
                "type": "source_locked",
                "category": result["category"],
                "color": result["color"],
                "count": 0,
            })
            all_graph_edges.append({
                "source": "user",
                "target": source_id,
                "weight": 0,
            })

    return {
        "sources": results,
        "summary": {
            "total_records": total_records,
            "sources_found": sources_found,
            "sources_locked": sources_locked,
            "sources_scanned": len(_SCANNERS),
            "needs_full_disk_access": needs_fda,
        },
        "graph_nodes": [{"id": "user", "label": "You", "type": "person", "color": "#00d4ff"}] + all_graph_nodes,
        "graph_edges": all_graph_edges,
    }


# ---------------------------------------------------------------------------
# CLI convenience
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import json
    result = scan_local_mac()
    print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
