"""
Chrome browser history analyzer for Nomolo.

Reads the user's Chrome history database, identifies which platforms/services
they actively use, and returns structured data suitable for building a
knowledge graph visualization of their digital footprint.
"""

from __future__ import annotations

import os
import shutil
import sqlite3
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


# ---------------------------------------------------------------------------
# Chrome timestamp helpers
# ---------------------------------------------------------------------------

_CHROME_EPOCH = datetime(1601, 1, 1)


def chrome_time_to_datetime(chrome_timestamp: int) -> datetime:
    """Convert a Chrome/WebKit timestamp (microseconds since 1601-01-01) to a Python datetime."""
    return _CHROME_EPOCH + timedelta(microseconds=chrome_timestamp)


def _format_date(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Platform detection map
# ---------------------------------------------------------------------------

_PLATFORM_ENTRY = dict[str, str | bool]

PLATFORM_MAP: dict[str, _PLATFORM_ENTRY] = {
    # Email
    "mail.google.com": {"source": "gmail", "name": "Gmail", "icon": "\u2709\ufe0f", "category": "email"},
    "gmail.com": {"source": "gmail", "name": "Gmail", "icon": "\u2709\ufe0f", "category": "email"},
    # Google services
    "contacts.google.com": {"source": "contacts-google", "name": "Google Contacts", "icon": "\U0001f465", "category": "social"},
    "calendar.google.com": {"source": "calendar", "name": "Google Calendar", "icon": "\U0001f4c5", "category": "productivity"},
    "drive.google.com": {"source": "google-drive", "name": "Google Drive", "icon": "\U0001f4be", "category": "cloud"},
    "maps.google.com": {"source": "maps", "name": "Google Maps", "icon": "\U0001f4cd", "category": "location"},
    "docs.google.com": {"source": "google-docs", "name": "Google Docs", "icon": "\U0001f4c4", "category": "productivity"},
    "sheets.google.com": {"source": "google-sheets", "name": "Google Sheets", "icon": "\U0001f4ca", "category": "productivity"},
    "photos.google.com": {"source": "google-photos", "name": "Google Photos", "icon": "\U0001f5bc\ufe0f", "category": "media"},
    # YouTube
    "youtube.com": {"source": "youtube", "name": "YouTube", "icon": "\U0001f3ac", "category": "media"},
    "www.youtube.com": {"source": "youtube", "name": "YouTube", "icon": "\U0001f3ac", "category": "media"},
    "music.youtube.com": {"source": "youtube-music", "name": "YouTube Music", "icon": "\U0001f3b5", "category": "media"},
    # LinkedIn
    "linkedin.com": {"source": "contacts-linkedin", "name": "LinkedIn", "icon": "\U0001f4bc", "category": "social"},
    "www.linkedin.com": {"source": "contacts-linkedin", "name": "LinkedIn", "icon": "\U0001f4bc", "category": "social"},
    # Facebook / Meta
    "facebook.com": {"source": "contacts-facebook", "name": "Facebook", "icon": "\U0001f465", "category": "social"},
    "www.facebook.com": {"source": "contacts-facebook", "name": "Facebook", "icon": "\U0001f465", "category": "social"},
    "instagram.com": {"source": "contacts-instagram", "name": "Instagram", "icon": "\U0001f4f7", "category": "social"},
    "www.instagram.com": {"source": "contacts-instagram", "name": "Instagram", "icon": "\U0001f4f7", "category": "social"},
    # Twitter / X
    "twitter.com": {"source": "twitter", "name": "Twitter/X", "icon": "\U0001f426", "category": "social"},
    "x.com": {"source": "twitter", "name": "Twitter/X", "icon": "\U0001f426", "category": "social"},
    # Spotify
    "open.spotify.com": {"source": "music-spotify", "name": "Spotify", "icon": "\U0001f3b5", "category": "media"},
    "spotify.com": {"source": "music-spotify", "name": "Spotify", "icon": "\U0001f3b5", "category": "media"},
    # Amazon (multi-region)
    "amazon.com": {"source": "shopping-amazon", "name": "Amazon", "icon": "\U0001f6d2", "category": "shopping"},
    "www.amazon.com": {"source": "shopping-amazon", "name": "Amazon", "icon": "\U0001f6d2", "category": "shopping"},
    "amazon.de": {"source": "shopping-amazon", "name": "Amazon", "icon": "\U0001f6d2", "category": "shopping"},
    "www.amazon.de": {"source": "shopping-amazon", "name": "Amazon", "icon": "\U0001f6d2", "category": "shopping"},
    "amazon.co.uk": {"source": "shopping-amazon", "name": "Amazon", "icon": "\U0001f6d2", "category": "shopping"},
    "www.amazon.co.uk": {"source": "shopping-amazon", "name": "Amazon", "icon": "\U0001f6d2", "category": "shopping"},
    "amazon.fr": {"source": "shopping-amazon", "name": "Amazon", "icon": "\U0001f6d2", "category": "shopping"},
    "www.amazon.fr": {"source": "shopping-amazon", "name": "Amazon", "icon": "\U0001f6d2", "category": "shopping"},
    "amazon.es": {"source": "shopping-amazon", "name": "Amazon", "icon": "\U0001f6d2", "category": "shopping"},
    "www.amazon.es": {"source": "shopping-amazon", "name": "Amazon", "icon": "\U0001f6d2", "category": "shopping"},
    "amazon.it": {"source": "shopping-amazon", "name": "Amazon", "icon": "\U0001f6d2", "category": "shopping"},
    "www.amazon.it": {"source": "shopping-amazon", "name": "Amazon", "icon": "\U0001f6d2", "category": "shopping"},
    "amazon.nl": {"source": "shopping-amazon", "name": "Amazon", "icon": "\U0001f6d2", "category": "shopping"},
    "www.amazon.nl": {"source": "shopping-amazon", "name": "Amazon", "icon": "\U0001f6d2", "category": "shopping"},
    "amazon.co.jp": {"source": "shopping-amazon", "name": "Amazon", "icon": "\U0001f6d2", "category": "shopping"},
    "www.amazon.co.jp": {"source": "shopping-amazon", "name": "Amazon", "icon": "\U0001f6d2", "category": "shopping"},
    "amazon.ca": {"source": "shopping-amazon", "name": "Amazon", "icon": "\U0001f6d2", "category": "shopping"},
    "www.amazon.ca": {"source": "shopping-amazon", "name": "Amazon", "icon": "\U0001f6d2", "category": "shopping"},
    # PayPal
    "paypal.com": {"source": "finance-paypal", "name": "PayPal", "icon": "\U0001f4b3", "category": "finance"},
    "www.paypal.com": {"source": "finance-paypal", "name": "PayPal", "icon": "\U0001f4b3", "category": "finance"},
    # Messaging
    "web.whatsapp.com": {"source": "whatsapp", "name": "WhatsApp", "icon": "\U0001f4ac", "category": "messaging"},
    "web.telegram.org": {"source": "telegram", "name": "Telegram", "icon": "\u2708\ufe0f", "category": "messaging"},
    "discord.com": {"source": "discord", "name": "Discord", "icon": "\U0001f3ae", "category": "messaging"},
    "slack.com": {"source": "slack", "name": "Slack", "icon": "\U0001f4ac", "category": "messaging"},
    # Reddit
    "reddit.com": {"source": "reddit", "name": "Reddit", "icon": "\U0001f517", "category": "social"},
    "www.reddit.com": {"source": "reddit", "name": "Reddit", "icon": "\U0001f517", "category": "social"},
    # Productivity
    "github.com": {"source": "github", "name": "GitHub", "icon": "\U0001f4bb", "category": "productivity"},
    "notion.so": {"source": "notion", "name": "Notion", "icon": "\U0001f4d3", "category": "productivity"},
    "www.notion.so": {"source": "notion", "name": "Notion", "icon": "\U0001f4d3", "category": "productivity"},
    "trello.com": {"source": "trello", "name": "Trello", "icon": "\U0001f4cb", "category": "productivity"},
    # Media / Entertainment
    "netflix.com": {"source": "netflix", "name": "Netflix", "icon": "\U0001f3ac", "category": "media"},
    "www.netflix.com": {"source": "netflix", "name": "Netflix", "icon": "\U0001f3ac", "category": "media"},
    # Books
    "goodreads.com": {"source": "books-goodreads", "name": "Goodreads", "icon": "\U0001f4da", "category": "media"},
    "www.goodreads.com": {"source": "books-goodreads", "name": "Goodreads", "icon": "\U0001f4da", "category": "media"},
    "audible.com": {"source": "books-audible", "name": "Audible", "icon": "\U0001f3a7", "category": "media"},
    "www.audible.com": {"source": "books-audible", "name": "Audible", "icon": "\U0001f3a7", "category": "media"},
}

# Which sources can be collected via the Nomolo API
_COLLECTABLE_SOURCES: set[str] = {
    "gmail",
    "contacts-google",
    "calendar",
    "google-drive",
    "google-docs",
    "google-sheets",
    "google-photos",
    "contacts-linkedin",
    "contacts-facebook",
    "contacts-instagram",
    "twitter",
    "github",
    "whatsapp",
    "telegram",
    "discord",
    "slack",
    "notion",
    "trello",
    "youtube",
    "youtube-music",
    "music-spotify",
    "shopping-amazon",
    "finance-paypal",
    "reddit",
    "maps",
    "netflix",
    "books-goodreads",
    "books-audible",
}

# Rough difficulty / time estimates per source (for suggestions)
_SOURCE_META: dict[str, dict[str, str]] = {
    "gmail": {"difficulty": "easy", "estimated_time": "2 minutes"},
    "contacts-google": {"difficulty": "easy", "estimated_time": "2 minutes"},
    "calendar": {"difficulty": "easy", "estimated_time": "2 minutes"},
    "google-drive": {"difficulty": "easy", "estimated_time": "2 minutes"},
    "google-docs": {"difficulty": "easy", "estimated_time": "2 minutes"},
    "google-sheets": {"difficulty": "easy", "estimated_time": "2 minutes"},
    "google-photos": {"difficulty": "easy", "estimated_time": "3 minutes"},
    "contacts-linkedin": {"difficulty": "medium", "estimated_time": "5 minutes"},
    "contacts-facebook": {"difficulty": "medium", "estimated_time": "5 minutes"},
    "contacts-instagram": {"difficulty": "medium", "estimated_time": "5 minutes"},
    "twitter": {"difficulty": "medium", "estimated_time": "5 minutes"},
    "github": {"difficulty": "easy", "estimated_time": "2 minutes"},
    "whatsapp": {"difficulty": "hard", "estimated_time": "10 minutes"},
    "telegram": {"difficulty": "medium", "estimated_time": "5 minutes"},
    "discord": {"difficulty": "medium", "estimated_time": "5 minutes"},
    "slack": {"difficulty": "medium", "estimated_time": "5 minutes"},
    "notion": {"difficulty": "easy", "estimated_time": "3 minutes"},
    "trello": {"difficulty": "easy", "estimated_time": "3 minutes"},
    "youtube": {"difficulty": "easy", "estimated_time": "2 minutes"},
    "youtube-music": {"difficulty": "easy", "estimated_time": "2 minutes"},
    "music-spotify": {"difficulty": "easy", "estimated_time": "3 minutes"},
    "shopping-amazon": {"difficulty": "hard", "estimated_time": "10 minutes"},
    "finance-paypal": {"difficulty": "hard", "estimated_time": "10 minutes"},
    "reddit": {"difficulty": "medium", "estimated_time": "5 minutes"},
    "maps": {"difficulty": "easy", "estimated_time": "2 minutes"},
    "netflix": {"difficulty": "medium", "estimated_time": "5 minutes"},
    "books-goodreads": {"difficulty": "medium", "estimated_time": "5 minutes"},
    "books-audible": {"difficulty": "medium", "estimated_time": "5 minutes"},
}

# Priority ordering: higher = suggest first (API-easy and data-rich first)
_SOURCE_PRIORITY: dict[str, int] = {
    "gmail": 100,
    "contacts-google": 95,
    "calendar": 90,
    "google-drive": 85,
    "github": 80,
    "contacts-linkedin": 75,
    "notion": 70,
    "slack": 65,
    "discord": 60,
    "telegram": 55,
    "twitter": 50,
    "youtube": 45,
    "music-spotify": 40,
    "reddit": 35,
    "contacts-facebook": 30,
    "contacts-instagram": 28,
    "google-docs": 25,
    "google-sheets": 24,
    "google-photos": 23,
    "youtube-music": 22,
    "trello": 20,
    "maps": 18,
    "whatsapp": 15,
    "netflix": 12,
    "books-goodreads": 10,
    "books-audible": 8,
    "shopping-amazon": 5,
    "finance-paypal": 3,
}


# ---------------------------------------------------------------------------
# Chrome history DB helpers
# ---------------------------------------------------------------------------

def _chrome_history_path() -> Path:
    """Return the default Chrome history DB path on macOS."""
    return Path.home() / "Library" / "Application Support" / "Google" / "Chrome" / "Default" / "History"


def _copy_history_to_temp(src: Path) -> Path:
    """
    Copy the Chrome history DB to a temp file so we don't conflict with
    Chrome's lock on the live database.
    """
    tmp = Path(tempfile.mkdtemp()) / "History_copy"
    shutil.copy2(src, tmp)
    return tmp


def _extract_domain(url: str) -> str:
    """
    Extract the hostname from a URL string. Returns empty string on failure.
    """
    try:
        parsed = urlparse(url)
        return parsed.hostname or ""
    except Exception:
        return ""


# ---------------------------------------------------------------------------
# SQL queries — aggregation happens in SQLite for speed
# ---------------------------------------------------------------------------

_SQL_DOMAIN_VISITS = """
    SELECT
        url,
        visit_count,
        last_visit_time
    FROM urls
    WHERE visit_count > 0
    ORDER BY visit_count DESC
"""

_SQL_DATE_RANGE = """
    SELECT
        MIN(last_visit_time) AS earliest,
        MAX(last_visit_time) AS latest
    FROM urls
    WHERE last_visit_time > 0
"""

_SQL_TOTALS = """
    SELECT
        COUNT(*) AS total_urls,
        COALESCE(SUM(visit_count), 0) AS total_visits
    FROM urls
"""


# ---------------------------------------------------------------------------
# Core analysis
# ---------------------------------------------------------------------------

def _format_visits_label(count: int) -> str:
    """Human-friendly label like '12.4k visits'."""
    if count >= 1_000_000:
        return f"{count / 1_000_000:.1f}M visits"
    if count >= 1_000:
        return f"{count / 1_000:.1f}k visits"
    return f"{count} visits"


def _node_size(visits: int, max_visits: int) -> int:
    """Map visit count to a node size between 8 and 28 for graph visualisation."""
    if max_visits == 0:
        return 12
    ratio = visits / max_visits
    return int(8 + 20 * ratio)


def analyze_chrome_history() -> dict[str, Any]:
    """
    Read Chrome history and return a structured analysis.

    Returns a dict with keys: success, total_urls, total_visits, date_range,
    platforms, top_domains, stats, graph_nodes, graph_edges.
    On failure returns {"success": False, "error": "<error_code>", "message": "..."}.
    """
    history_path = _chrome_history_path()

    # --- Pre-flight checks ---------------------------------------------------
    if not history_path.parent.exists():
        return {
            "success": False,
            "error": "chrome_not_found",
            "message": "Chrome data directory not found. Is Google Chrome installed?",
        }

    if not history_path.exists():
        return {
            "success": False,
            "error": "chrome_not_found",
            "message": f"Chrome history database not found at {history_path}",
        }

    if not os.access(history_path, os.R_OK):
        return {
            "success": False,
            "error": "permission_denied",
            "message": (
                "Cannot read Chrome history. Grant Terminal full-disk access in "
                "System Settings > Privacy & Security > Full Disk Access."
            ),
        }

    # --- Copy DB to temp so Chrome's lock doesn't block us --------------------
    try:
        tmp_db = _copy_history_to_temp(history_path)
    except Exception as exc:
        return {
            "success": False,
            "error": "db_locked",
            "message": f"Failed to copy Chrome history database: {exc}",
        }

    try:
        conn = sqlite3.connect(f"file:{tmp_db}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # Totals
        cur.execute(_SQL_TOTALS)
        row = cur.fetchone()
        total_urls: int = row["total_urls"]
        total_visits: int = row["total_visits"]

        if total_urls == 0:
            conn.close()
            return {
                "success": False,
                "error": "empty_history",
                "message": "Chrome history exists but contains no entries.",
            }

        # Date range
        cur.execute(_SQL_DATE_RANGE)
        dr = cur.fetchone()
        earliest_dt = chrome_time_to_datetime(dr["earliest"])
        latest_dt = chrome_time_to_datetime(dr["latest"])

        # Aggregate visits by domain in Python (SQLite has no built-in URL parser)
        cur.execute(_SQL_DOMAIN_VISITS)
        domain_visits: dict[str, int] = {}
        domain_last_visit: dict[str, int] = {}

        for row in cur:
            domain = _extract_domain(row["url"])
            if not domain:
                continue
            vc: int = row["visit_count"]
            lvt: int = row["last_visit_time"]
            domain_visits[domain] = domain_visits.get(domain, 0) + vc
            if lvt > domain_last_visit.get(domain, 0):
                domain_last_visit[domain] = lvt

        conn.close()
    except Exception as exc:
        return {
            "success": False,
            "error": "db_locked",
            "message": f"Failed to read Chrome history database: {exc}",
        }
    finally:
        # Clean up temp file
        try:
            tmp_db.unlink()
            tmp_db.parent.rmdir()
        except OSError:
            pass

    # --- Build platform list --------------------------------------------------
    # Deduplicate by source (multiple domains can map to same source)
    source_agg: dict[str, dict[str, Any]] = {}

    for domain, info in PLATFORM_MAP.items():
        if domain not in domain_visits:
            continue
        src = str(info["source"])
        visits = domain_visits[domain]
        lvt = domain_last_visit.get(domain, 0)

        if src in source_agg:
            source_agg[src]["visits"] += visits
            if lvt > source_agg[src]["_last_visit_raw"]:
                source_agg[src]["_last_visit_raw"] = lvt
        else:
            source_agg[src] = {
                "source": src,
                "name": info["name"],
                "icon": info["icon"],
                "category": info["category"],
                "visits": visits,
                "_last_visit_raw": lvt,
                "nomolo_collectable": src in _COLLECTABLE_SOURCES,
                "collection_command": f"nomolo collect {src}" if src in _COLLECTABLE_SOURCES else None,
            }

    # Convert last-visit timestamps and sort by visits descending
    platforms: list[dict[str, Any]] = []
    for p in sorted(source_agg.values(), key=lambda x: x["visits"], reverse=True):
        p["last_visit"] = _format_date(chrome_time_to_datetime(p.pop("_last_visit_raw")))
        platforms.append(p)

    # --- Top domains ----------------------------------------------------------
    sorted_domains = sorted(domain_visits.items(), key=lambda kv: kv[1], reverse=True)[:50]
    platform_domains = set(PLATFORM_MAP.keys())
    top_domains = [
        {
            "domain": d,
            "visits": v,
            "is_platform": d in platform_domains,
        }
        for d, v in sorted_domains
    ]

    # --- Stats ----------------------------------------------------------------
    years_of_history = max(1, round((latest_dt - earliest_dt).days / 365.25, 1))
    collectable_count = sum(1 for p in platforms if p["nomolo_collectable"])

    stats = {
        "years_of_history": years_of_history,
        "unique_domains": len(domain_visits),
        "platforms_detected": len(platforms),
        "collectable_platforms": collectable_count,
    }

    # --- Graph nodes & edges --------------------------------------------------
    max_platform_visits = platforms[0]["visits"] if platforms else 1

    graph_nodes: list[dict[str, Any]] = [
        {"id": "user", "type": "person", "label": "You", "size": 30},
    ]
    graph_edges: list[dict[str, Any]] = []

    for p in platforms:
        graph_nodes.append({
            "id": p["source"],
            "type": "platform",
            "label": p["name"],
            "icon": p["icon"],
            "visits": p["visits"],
            "size": _node_size(p["visits"], max_platform_visits),
        })
        graph_edges.append({
            "source": "user",
            "target": p["source"],
            "weight": p["visits"],
            "label": _format_visits_label(p["visits"]),
        })

    return {
        "success": True,
        "total_urls": total_urls,
        "total_visits": total_visits,
        "date_range": {
            "earliest": _format_date(earliest_dt),
            "latest": _format_date(latest_dt),
        },
        "platforms": platforms,
        "top_domains": top_domains,
        "stats": stats,
        "graph_nodes": graph_nodes,
        "graph_edges": graph_edges,
    }


# ---------------------------------------------------------------------------
# Next-step suggestion
# ---------------------------------------------------------------------------

def get_suggested_next_step(
    analysis: dict[str, Any],
    existing_vaults: list[str] | None = None,
) -> dict[str, Any] | None:
    """
    Based on Chrome analysis and what's already collected, suggest the single
    best next action for the user.

    Priority logic:
      1. Highest-visit collectable platform not yet collected
      2. Prefer API-based (easy difficulty) over export-based
      3. Prefer platforms with richest personal data (via _SOURCE_PRIORITY)

    Returns a suggestion dict, or None if everything is already collected.
    """
    if not analysis.get("success") or not analysis.get("platforms"):
        return None

    collected: set[str] = set(existing_vaults or [])

    # Build candidates: collectable, not yet collected
    candidates: list[dict[str, Any]] = [
        p for p in analysis["platforms"]
        if p.get("nomolo_collectable") and p["source"] not in collected
    ]

    if not candidates:
        return None

    # Score each candidate: combine visit weight with source priority
    max_visits = max(c["visits"] for c in candidates) or 1

    def _score(p: dict[str, Any]) -> float:
        visit_score = p["visits"] / max_visits  # 0..1
        priority_score = _SOURCE_PRIORITY.get(p["source"], 0) / 100  # 0..1
        return 0.4 * visit_score + 0.6 * priority_score

    best = max(candidates, key=_score)

    meta = _SOURCE_META.get(best["source"], {"difficulty": "medium", "estimated_time": "5 minutes"})

    return {
        "source": best["source"],
        "name": best["name"],
        "reason": (
            f"You've visited {best['name']} {best['visits']:,} times "
            f"— it's your {'most-used' if best == candidates[0] else 'highly-used'} platform"
        ),
        "action": f"Connect {best['name']}",
        "difficulty": meta["difficulty"],
        "estimated_time": meta["estimated_time"],
        "command": f"nomolo setup {best['source']}",
    }


# ---------------------------------------------------------------------------
# CLI convenience
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import json

    result = analyze_chrome_history()
    print(json.dumps(result, indent=2, ensure_ascii=False))

    if result.get("success"):
        suggestion = get_suggested_next_step(result, existing_vaults=[])
        if suggestion:
            print("\n--- Suggested next step ---")
            print(json.dumps(suggestion, indent=2, ensure_ascii=False))
