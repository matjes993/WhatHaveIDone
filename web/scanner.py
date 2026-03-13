"""
NOMOLO Data Source Discovery Engine

Scans the user's machine to discover where their personal data lives.
Returns structured results for every detectable data source with metadata
about extraction difficulty, estimated record counts, and next steps.

Usage:
    from web.scanner import scan, get_life_score

    results = await scan(vault_root="vaults", project_root="/path/to/nomolo")
    score = get_life_score(results)
"""

from __future__ import annotations

import asyncio
import logging
import os
import sqlite3
import shutil
import tempfile
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Callable, Coroutine, Optional

logger = logging.getLogger("nomolo.scanner")

# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

ProgressCallback = Optional[Callable[[str, int, int], Coroutine[Any, Any, None]]]


@dataclass
class DataSource:
    id: str
    name: str
    category: str
    status: str  # "discovered" | "already_collected" | "ready"
    location: str
    nomolo_grade: str  # A+ through F
    estimated_records: int
    time_to_collect: str
    icon: str
    description: str
    action: str

    def to_dict(self) -> dict:
        return asdict(self)


# ---------------------------------------------------------------------------
# Path constants
# ---------------------------------------------------------------------------

HOME = Path.home()
DOWNLOADS = HOME / "Downloads"
LIBRARY = HOME / "Library"

CHROME_HISTORY = LIBRARY / "Application Support" / "Google" / "Chrome" / "Default" / "History"
SAFARI_HISTORY = LIBRARY / "Safari" / "History.db"
FIREFOX_PROFILES = LIBRARY / "Application Support" / "Firefox" / "Profiles"

ICLOUD_FOLDER = LIBRARY / "Mobile Documents"
WHATSAPP_CONTAINERS = LIBRARY / "Group Containers"

# Cloud storage base
CLOUD_STORAGE = LIBRARY / "CloudStorage"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_stat(path: Path) -> Optional[os.stat_result]:
    """Return stat result or None if inaccessible."""
    try:
        return path.stat()
    except (OSError, PermissionError):
        return None


def _path_exists(path: Path) -> bool:
    """Check path existence without raising."""
    try:
        return path.exists()
    except (OSError, PermissionError):
        return False


def _count_lines(path: Path) -> int:
    """Count lines in a file. Returns 0 on any error."""
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            return sum(1 for _ in f)
    except (OSError, PermissionError):
        return 0


def _count_jsonl(path: Path) -> int:
    """Count non-empty lines in a JSONL file."""
    try:
        count = 0
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                if line.strip():
                    count += 1
        return count
    except (OSError, PermissionError):
        return 0


def _sqlite_row_count(db_path: Path, table: str) -> int:
    """
    Count rows in an SQLite table. Copies the DB to a temp file first
    to avoid locking issues with live databases.
    """
    if not _path_exists(db_path):
        return 0
    tmp = None
    try:
        fd, tmp = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        shutil.copy2(str(db_path), tmp)
        conn = sqlite3.connect(f"file:{tmp}?mode=ro", uri=True)
        cursor = conn.execute(f"SELECT COUNT(*) FROM {table}")  # noqa: S608
        count = cursor.fetchone()[0]
        conn.close()
        return count
    except (sqlite3.Error, OSError, PermissionError):
        return 0
    finally:
        if tmp:
            try:
                os.unlink(tmp)
            except OSError:
                pass


def _glob_downloads(*patterns: str) -> list[Path]:
    """Return all files matching any of the glob patterns in ~/Downloads."""
    results: list[Path] = []
    if not _path_exists(DOWNLOADS):
        return results
    for pattern in patterns:
        try:
            results.extend(DOWNLOADS.glob(pattern))
        except (OSError, PermissionError):
            pass
    return results


def _find_google_drive_folder() -> Optional[Path]:
    """Locate Google Drive folder under CloudStorage."""
    if not _path_exists(CLOUD_STORAGE):
        return None
    try:
        for entry in CLOUD_STORAGE.iterdir():
            if entry.name.startswith("GoogleDrive-") and entry.is_dir():
                return entry
    except (OSError, PermissionError):
        pass
    return None


def _find_dropbox_folder() -> Optional[Path]:
    """Locate Dropbox folder."""
    candidates = [
        HOME / "Dropbox",
        CLOUD_STORAGE / "Dropbox",
    ]
    # Also check for any Dropbox-prefixed folder in CloudStorage
    if _path_exists(CLOUD_STORAGE):
        try:
            for entry in CLOUD_STORAGE.iterdir():
                if entry.name.startswith("Dropbox") and entry.is_dir():
                    candidates.append(entry)
        except (OSError, PermissionError):
            pass
    for c in candidates:
        if _path_exists(c):
            return c
    return None


# ---------------------------------------------------------------------------
# Vault inspection (what's already collected)
# ---------------------------------------------------------------------------

def _get_vault_stats(vault_root: Path) -> dict[str, int]:
    """
    Scan existing vaults and return {vault_name: record_count}.
    Uses processed_ids.txt line count, falling back to .jsonl line count.
    """
    stats: dict[str, int] = {}
    if not _path_exists(vault_root):
        return stats

    try:
        for entry in vault_root.iterdir():
            if not entry.is_dir():
                continue

            vault_name = entry.name
            count = 0

            # Primary: processed_ids.txt
            ids_file = entry / "processed_ids.txt"
            if _path_exists(ids_file):
                count = _count_lines(ids_file)

            # Fallback: sum all .jsonl files
            if count == 0:
                try:
                    for jf in entry.glob("*.jsonl"):
                        count += _count_jsonl(jf)
                except (OSError, PermissionError):
                    pass

            if count > 0:
                stats[vault_name] = count
    except (OSError, PermissionError):
        pass

    return stats


# Mapping from vault directory names to source IDs
_VAULT_TO_SOURCE: dict[str, str] = {
    "Browser": "chrome_history",
    "Gmail": "gmail",
    "Contacts": "google_contacts",
    "Calendar": "google_calendar",
    "Maps": "google_maps",
    "YouTube": "youtube_history",
    "Music": "spotify",
    "Health": "apple_health",
    "Finance": "finance_csv",
    "Shopping": "amazon_orders",
    "Books": "books",
    "Podcasts": "podcasts",
    "Notes": "notes_markdown",
    "LinkedIn": "linkedin_csv",
    "Instagram": "instagram_json",
    "Facebook": "facebook_json",
}


# ---------------------------------------------------------------------------
# Category scanners
# ---------------------------------------------------------------------------

async def _scan_browsers(vault_stats: dict[str, int]) -> list[DataSource]:
    """Detect browser history databases."""
    sources: list[DataSource] = []

    # Chrome
    chrome_exists = _path_exists(CHROME_HISTORY)
    chrome_count = 0
    if chrome_exists:
        chrome_count = await asyncio.to_thread(
            _sqlite_row_count, CHROME_HISTORY, "urls"
        )
    already = "Browser" in vault_stats
    sources.append(DataSource(
        id="chrome_history",
        name="Chrome Browser History",
        category="browsers",
        status="already_collected" if already else ("ready" if chrome_exists else "discovered"),
        location=str(CHROME_HISTORY) if chrome_exists else "Not found — Chrome may not be installed",
        nomolo_grade="A+",
        estimated_records=vault_stats.get("Browser", chrome_count),
        time_to_collect="10 seconds",
        icon="\U0001f310",  # globe
        description="Every website you've visited in Chrome with timestamps and visit counts",
        action="nomolo collect browser" if not already else "Already in vault",
    ))

    # Safari
    safari_exists = _path_exists(SAFARI_HISTORY)
    safari_count = 0
    if safari_exists:
        safari_count = await asyncio.to_thread(
            _sqlite_row_count, SAFARI_HISTORY, "history_items"
        )
    sources.append(DataSource(
        id="safari_history",
        name="Safari Browser History",
        category="browsers",
        status="ready" if safari_exists else "discovered",
        location=str(SAFARI_HISTORY) if safari_exists else "Not found",
        nomolo_grade="B+",
        estimated_records=safari_count,
        time_to_collect="10 seconds" if safari_exists else "requires setup",
        icon="\U0001f9ed",  # compass
        description="Safari browsing history including timestamps and visit frequency",
        action="nomolo collect safari" if safari_exists else "Grant Full Disk Access in System Settings",
    ))

    # Firefox
    firefox_profiles: list[Path] = []
    if _path_exists(FIREFOX_PROFILES):
        try:
            firefox_profiles = list(FIREFOX_PROFILES.glob("*/places.sqlite"))
        except (OSError, PermissionError):
            pass
    firefox_exists = len(firefox_profiles) > 0
    firefox_count = 0
    if firefox_exists:
        firefox_count = await asyncio.to_thread(
            _sqlite_row_count, firefox_profiles[0], "moz_places"
        )
    sources.append(DataSource(
        id="firefox_history",
        name="Firefox Browser History",
        category="browsers",
        status="ready" if firefox_exists else "discovered",
        location=str(firefox_profiles[0]) if firefox_exists else "Not found — Firefox may not be installed",
        nomolo_grade="A",
        estimated_records=firefox_count,
        time_to_collect="10 seconds" if firefox_exists else "requires setup",
        icon="\U0001f98a",  # fox
        description="Firefox browsing history, bookmarks, and page metadata",
        action="nomolo collect firefox" if firefox_exists else "Install Firefox or locate profile manually",
    ))

    return sources


async def _scan_google(
    vault_stats: dict[str, int],
    project_root: Path,
) -> list[DataSource]:
    """Detect Google-related data sources."""
    sources: list[DataSource] = []

    # Gmail credentials
    creds = project_root / "credentials.json"
    token = project_root / "token.json"
    creds_exists = _path_exists(creds)
    token_exists = _path_exists(token)
    gmail_collected = "Gmail" in vault_stats

    sources.append(DataSource(
        id="gmail",
        name="Gmail",
        category="email",
        status=(
            "already_collected" if gmail_collected
            else "ready" if (creds_exists and token_exists)
            else "discovered"
        ),
        location=(
            f"{vault_stats.get('Gmail', 0):,} emails in vault" if gmail_collected
            else str(creds) if creds_exists
            else "credentials.json not found in project directory"
        ),
        nomolo_grade="A+",
        estimated_records=vault_stats.get("Gmail", 0),
        time_to_collect="5-30 minutes depending on mailbox size",
        icon="\U00002709",  # envelope
        description="Your complete Gmail archive: every email, thread, label, and attachment metadata",
        action=(
            "Already in vault" if gmail_collected
            else "nomolo collect gmail" if (creds_exists and token_exists)
            else "Set up Google OAuth credentials first: see docs/gmail-setup.md"
        ),
    ))

    # Google Contacts
    contacts_collected = "Contacts" in vault_stats
    sources.append(DataSource(
        id="google_contacts",
        name="Google Contacts",
        category="social",
        status=(
            "already_collected" if contacts_collected
            else "ready" if (creds_exists and token_exists)
            else "discovered"
        ),
        location=(
            f"{vault_stats.get('Contacts', 0):,} contacts in vault" if contacts_collected
            else "Uses same Google OAuth credentials"
        ),
        nomolo_grade="A+",
        estimated_records=vault_stats.get("Contacts", 0),
        time_to_collect="30 seconds",
        icon="\U0001f4c7",  # rolodex
        description="All your Google contacts with phone numbers, emails, and metadata",
        action=(
            "Already in vault" if contacts_collected
            else "nomolo collect contacts" if creds_exists
            else "Set up Google OAuth credentials first"
        ),
    ))

    # Google Calendar
    cal_collected = "Calendar" in vault_stats
    sources.append(DataSource(
        id="google_calendar",
        name="Google Calendar",
        category="google",
        status="already_collected" if cal_collected else "discovered",
        location=(
            f"{vault_stats.get('Calendar', 0):,} events in vault" if cal_collected
            else "Requires Google OAuth"
        ),
        nomolo_grade="A",
        estimated_records=vault_stats.get("Calendar", 0),
        time_to_collect="1 minute",
        icon="\U0001f4c5",  # calendar
        description="All calendar events, meetings, and appointments",
        action="Already in vault" if cal_collected else "nomolo collect calendar",
    ))

    # Google Maps / Location History
    maps_collected = "Maps" in vault_stats
    sources.append(DataSource(
        id="google_maps",
        name="Google Maps Timeline",
        category="google",
        status="already_collected" if maps_collected else "discovered",
        location=(
            f"{vault_stats.get('Maps', 0):,} locations in vault" if maps_collected
            else "Requires Google Takeout export"
        ),
        nomolo_grade="B",
        estimated_records=vault_stats.get("Maps", 0),
        time_to_collect="2 minutes (after Takeout download)",
        icon="\U0001f4cd",  # pin
        description="Your location history: every place you've been with timestamps",
        action="Already in vault" if maps_collected else "nomolo collect maps",
    ))

    # Google Drive folder
    gdrive = _find_google_drive_folder()
    sources.append(DataSource(
        id="google_drive",
        name="Google Drive",
        category="cloud",
        status="ready" if gdrive else "discovered",
        location=str(gdrive) if gdrive else "Google Drive for Desktop not detected",
        nomolo_grade="C+",
        estimated_records=0,
        time_to_collect="requires setup",
        icon="\U0001f4be",  # floppy
        description="Files synced from Google Drive to your Mac",
        action="Scan Google Drive folder for documents" if gdrive else "Install Google Drive for Desktop",
    ))

    # Google Takeout zips in Downloads
    takeout_zips = _glob_downloads("takeout-*.zip", "Takeout/*.zip", "takeout*.zip")
    sources.append(DataSource(
        id="google_takeout",
        name="Google Takeout Export",
        category="google",
        status="ready" if takeout_zips else "discovered",
        location=(
            f"{len(takeout_zips)} Takeout zip(s) in Downloads" if takeout_zips
            else "No Takeout exports found in Downloads"
        ),
        nomolo_grade="A",
        estimated_records=0,
        time_to_collect="5-15 minutes" if takeout_zips else "requires setup",
        icon="\U0001f4e6",  # package
        description="Bulk Google data export: YouTube, Maps, Chrome, Fit, and more",
        action=(
            f"nomolo import takeout {takeout_zips[0]}" if takeout_zips
            else "Go to takeout.google.com to request your data export"
        ),
    ))

    return sources


async def _scan_social() -> list[DataSource]:
    """Detect social media exports."""
    sources: list[DataSource] = []

    # LinkedIn
    linkedin_csvs = _glob_downloads(
        "Connections.csv", "LinkedIn*.csv", "linkedin*.csv",
        "**/Connections.csv",
    )
    sources.append(DataSource(
        id="linkedin_csv",
        name="LinkedIn Connections",
        category="social",
        status="ready" if linkedin_csvs else "discovered",
        location=(
            str(linkedin_csvs[0]) if linkedin_csvs
            else "No LinkedIn export found in Downloads"
        ),
        nomolo_grade="A+",
        estimated_records=_count_lines(linkedin_csvs[0]) - 1 if linkedin_csvs else 0,
        time_to_collect="10 seconds" if linkedin_csvs else "requires setup",
        icon="\U0001f4bc",  # briefcase
        description="Your professional network: names, companies, positions, connection dates",
        action=(
            f"nomolo collect linkedin {linkedin_csvs[0]}" if linkedin_csvs
            else "Export from linkedin.com/mypreferences/d/download-my-data"
        ),
    ))

    # Facebook
    facebook_exports = _glob_downloads(
        "facebook-*.zip", "facebook-*.json",
        "**/facebook_data/**/*.json",
    )
    # Also check for unzipped Facebook export directories
    fb_dirs: list[Path] = []
    try:
        fb_dirs = [
            d for d in DOWNLOADS.iterdir()
            if d.is_dir() and "facebook" in d.name.lower()
        ] if _path_exists(DOWNLOADS) else []
    except (OSError, PermissionError):
        pass

    fb_found = len(facebook_exports) > 0 or len(fb_dirs) > 0
    sources.append(DataSource(
        id="facebook_json",
        name="Facebook Data Export",
        category="social",
        status="ready" if fb_found else "discovered",
        location=(
            str(facebook_exports[0]) if facebook_exports
            else str(fb_dirs[0]) if fb_dirs
            else "No Facebook export found in Downloads"
        ),
        nomolo_grade="B+",
        estimated_records=0,
        time_to_collect="2 minutes" if fb_found else "requires setup",
        icon="\U0001f465",  # people
        description="Facebook posts, messages, friends, photos metadata, and activity",
        action=(
            f"nomolo collect facebook {facebook_exports[0]}" if facebook_exports
            else "Export from facebook.com/dyi — choose JSON format"
        ),
    ))

    # Instagram
    instagram_exports = _glob_downloads(
        "instagram-*.zip", "instagram-*.json",
        "**/instagram_data/**/*.json",
    )
    ig_dirs: list[Path] = []
    try:
        ig_dirs = [
            d for d in DOWNLOADS.iterdir()
            if d.is_dir() and "instagram" in d.name.lower()
        ] if _path_exists(DOWNLOADS) else []
    except (OSError, PermissionError):
        pass

    ig_found = len(instagram_exports) > 0 or len(ig_dirs) > 0
    sources.append(DataSource(
        id="instagram_json",
        name="Instagram Data Export",
        category="social",
        status="ready" if ig_found else "discovered",
        location=(
            str(instagram_exports[0]) if instagram_exports
            else str(ig_dirs[0]) if ig_dirs
            else "No Instagram export found in Downloads"
        ),
        nomolo_grade="B+",
        estimated_records=0,
        time_to_collect="2 minutes" if ig_found else "requires setup",
        icon="\U0001f4f7",  # camera
        description="Instagram posts, stories, messages, followers, and likes",
        action=(
            f"nomolo collect instagram {instagram_exports[0]}" if instagram_exports
            else "Export from instagram.com — Settings > Your Activity > Download Your Information"
        ),
    ))

    return sources


async def _scan_messaging() -> list[DataSource]:
    """Detect messaging app data."""
    sources: list[DataSource] = []

    # WhatsApp
    whatsapp_found = False
    whatsapp_location = "Not found"

    # Check Group Containers for WhatsApp
    if _path_exists(WHATSAPP_CONTAINERS):
        try:
            for entry in WHATSAPP_CONTAINERS.iterdir():
                if "whatsapp" in entry.name.lower():
                    whatsapp_found = True
                    whatsapp_location = str(entry)
                    break
        except (OSError, PermissionError):
            pass

    # Check Downloads for WhatsApp exports
    wa_exports = _glob_downloads(
        "WhatsApp Chat*.txt", "WhatsApp Chat*.zip",
        "whatsapp*.txt", "whatsapp*.zip",
    )
    if wa_exports:
        whatsapp_found = True
        whatsapp_location = str(wa_exports[0])

    sources.append(DataSource(
        id="whatsapp",
        name="WhatsApp Messages",
        category="messaging",
        status="ready" if wa_exports else ("discovered" if whatsapp_found else "discovered"),
        location=whatsapp_location,
        nomolo_grade="C+" if wa_exports else "D",
        estimated_records=0,
        time_to_collect="5 minutes" if wa_exports else "requires setup",
        icon="\U0001f4ac",  # speech bubble
        description="WhatsApp chat history, group messages, and media metadata",
        action=(
            f"nomolo collect whatsapp {wa_exports[0]}" if wa_exports
            else "Export chats from WhatsApp > Settings > Chats > Export Chat"
        ),
    ))

    # Telegram
    telegram_exports = _glob_downloads(
        "telegram_export*", "ChatExport*",
        "DataExport*",
    )
    tg_dirs: list[Path] = []
    try:
        tg_dirs = [
            d for d in DOWNLOADS.iterdir()
            if d.is_dir() and ("telegram" in d.name.lower() or "chatexport" in d.name.lower())
        ] if _path_exists(DOWNLOADS) else []
    except (OSError, PermissionError):
        pass

    tg_found = len(telegram_exports) > 0 or len(tg_dirs) > 0
    sources.append(DataSource(
        id="telegram",
        name="Telegram Messages",
        category="messaging",
        status="ready" if tg_found else "discovered",
        location=(
            str(telegram_exports[0]) if telegram_exports
            else str(tg_dirs[0]) if tg_dirs
            else "No Telegram export found"
        ),
        nomolo_grade="B" if tg_found else "C",
        estimated_records=0,
        time_to_collect="5 minutes" if tg_found else "requires setup",
        icon="\U00002708",  # airplane (Telegram-ish)
        description="Telegram chat messages, channels, groups, and media",
        action=(
            f"nomolo collect telegram {telegram_exports[0]}" if telegram_exports
            else "Use Telegram Desktop > Settings > Export Telegram Data"
        ),
    ))

    return sources


async def _scan_media(vault_stats: dict[str, int]) -> list[DataSource]:
    """Detect media-related data sources."""
    sources: list[DataSource] = []

    # Spotify
    spotify_collected = "Music" in vault_stats
    spotify_exports = _glob_downloads(
        "my_spotify_data*", "Spotify*.zip",
        "StreamingHistory*.json", "endsong*.json",
    )
    spotify_dirs: list[Path] = []
    try:
        spotify_dirs = [
            d for d in DOWNLOADS.iterdir()
            if d.is_dir() and "spotify" in d.name.lower()
        ] if _path_exists(DOWNLOADS) else []
    except (OSError, PermissionError):
        pass

    sp_found = len(spotify_exports) > 0 or len(spotify_dirs) > 0
    sources.append(DataSource(
        id="spotify",
        name="Spotify Listening History",
        category="media",
        status=(
            "already_collected" if spotify_collected
            else "ready" if sp_found
            else "discovered"
        ),
        location=(
            f"{vault_stats.get('Music', 0):,} tracks in vault" if spotify_collected
            else str(spotify_exports[0]) if spotify_exports
            else "No Spotify export found in Downloads"
        ),
        nomolo_grade="A" if sp_found else "B",
        estimated_records=vault_stats.get("Music", 0),
        time_to_collect="1 minute" if sp_found else "requires setup",
        icon="\U0001f3b5",  # music note
        description="Every song you've listened to on Spotify with timestamps and play duration",
        action=(
            "Already in vault" if spotify_collected
            else f"nomolo collect music {spotify_exports[0]}" if spotify_exports
            else "Request data at spotify.com/account/privacy"
        ),
    ))

    # YouTube
    yt_collected = "YouTube" in vault_stats
    yt_exports = _glob_downloads(
        "**/YouTube/**/*.json", "watch-history.json",
        "**/Takeout/**/YouTube/**",
    )
    sources.append(DataSource(
        id="youtube_history",
        name="YouTube Watch History",
        category="media",
        status=(
            "already_collected" if yt_collected
            else "ready" if yt_exports
            else "discovered"
        ),
        location=(
            f"{vault_stats.get('YouTube', 0):,} videos in vault" if yt_collected
            else str(yt_exports[0]) if yt_exports
            else "Include YouTube in your Google Takeout"
        ),
        nomolo_grade="A" if yt_exports else "B",
        estimated_records=vault_stats.get("YouTube", 0),
        time_to_collect="1 minute" if yt_exports else "requires setup",
        icon="\U0001f3ac",  # clapper
        description="Every YouTube video you've watched, liked, and subscribed to",
        action=(
            "Already in vault" if yt_collected
            else f"nomolo collect youtube {yt_exports[0]}" if yt_exports
            else "nomolo collect youtube (or include in Google Takeout)"
        ),
    ))

    # Photos
    photos_lib = HOME / "Pictures" / "Photos Library.photoslibrary"
    photos_exists = _path_exists(photos_lib)
    sources.append(DataSource(
        id="apple_photos",
        name="Apple Photos Library",
        category="media",
        status="ready" if photos_exists else "discovered",
        location=str(photos_lib) if photos_exists else "No Photos library found",
        nomolo_grade="C",
        estimated_records=0,
        time_to_collect="requires setup",
        icon="\U0001f5bc",  # framed picture
        description="Photo and video metadata: dates, locations, albums, faces",
        action="Coming soon — Photos library scanning" if photos_exists else "Not available",
    ))

    # Podcasts
    pod_collected = "Podcasts" in vault_stats
    sources.append(DataSource(
        id="podcasts",
        name="Podcast History",
        category="media",
        status="already_collected" if pod_collected else "discovered",
        location=(
            f"{vault_stats.get('Podcasts', 0):,} episodes in vault" if pod_collected
            else "Requires podcast app export"
        ),
        nomolo_grade="B",
        estimated_records=vault_stats.get("Podcasts", 0),
        time_to_collect="1 minute",
        icon="\U0001f3a7",  # headphones
        description="Podcast subscriptions and listening history",
        action="Already in vault" if pod_collected else "nomolo collect podcasts",
    ))

    # Books
    books_collected = "Books" in vault_stats
    sources.append(DataSource(
        id="books",
        name="Books & Reading",
        category="media",
        status="already_collected" if books_collected else "discovered",
        location=(
            f"{vault_stats.get('Books', 0):,} books in vault" if books_collected
            else "Requires Goodreads/Kindle export"
        ),
        nomolo_grade="B+",
        estimated_records=vault_stats.get("Books", 0),
        time_to_collect="1 minute",
        icon="\U0001f4da",  # books
        description="Books you've read, ratings, and reading history",
        action="Already in vault" if books_collected else "nomolo collect books",
    ))

    return sources


async def _scan_health(vault_stats: dict[str, int]) -> list[DataSource]:
    """Detect health data exports."""
    sources: list[DataSource] = []

    health_collected = "Health" in vault_stats

    # Apple Health export
    health_exports = _glob_downloads(
        "export.xml", "apple_health_export*",
        "**/apple_health_export/**/*.xml",
    )
    # Also check for zip containing health data
    health_zips = _glob_downloads("export*.zip")
    health_found = len(health_exports) > 0 or len(health_zips) > 0

    sources.append(DataSource(
        id="apple_health",
        name="Apple Health Data",
        category="health",
        status=(
            "already_collected" if health_collected
            else "ready" if health_found
            else "discovered"
        ),
        location=(
            f"{vault_stats.get('Health', 0):,} records in vault" if health_collected
            else str(health_exports[0]) if health_exports
            else "No Apple Health export found in Downloads"
        ),
        nomolo_grade="A" if health_found else "B",
        estimated_records=vault_stats.get("Health", 0),
        time_to_collect="2 minutes" if health_found else "requires setup",
        icon="\U00002764",  # heart
        description="Steps, heart rate, workouts, sleep, nutrition — your complete health record",
        action=(
            "Already in vault" if health_collected
            else f"nomolo collect health {health_exports[0]}" if health_exports
            else "Open Apple Health app > Profile > Export All Health Data"
        ),
    ))

    return sources


async def _scan_finance(vault_stats: dict[str, int]) -> list[DataSource]:
    """Detect financial data exports."""
    sources: list[DataSource] = []

    finance_collected = "Finance" in vault_stats

    # Bank/PayPal CSVs
    finance_csvs = _glob_downloads(
        "*statement*.csv", "*transaction*.csv",
        "*paypal*.csv", "*PayPal*.csv",
        "*bank*.csv", "*Bank*.csv",
        "*chase*.csv", "*Chase*.csv",
        "*revolut*.csv", "*Revolut*.csv",
        "*wise*.csv", "*Wise*.csv",
    )
    sources.append(DataSource(
        id="finance_csv",
        name="Bank & Payment Statements",
        category="finance",
        status=(
            "already_collected" if finance_collected
            else "ready" if finance_csvs
            else "discovered"
        ),
        location=(
            f"{vault_stats.get('Finance', 0):,} transactions in vault" if finance_collected
            else f"{len(finance_csvs)} CSV file(s) in Downloads" if finance_csvs
            else "No financial CSVs found in Downloads"
        ),
        nomolo_grade="A" if finance_csvs else "C",
        estimated_records=vault_stats.get("Finance", 0),
        time_to_collect="1 minute" if finance_csvs else "requires setup",
        icon="\U0001f4b3",  # credit card
        description="Transaction history from banks and payment providers",
        action=(
            "Already in vault" if finance_collected
            else f"nomolo collect finance {finance_csvs[0]}" if finance_csvs
            else "Download CSV statements from your bank's website"
        ),
    ))

    return sources


async def _scan_shopping(vault_stats: dict[str, int]) -> list[DataSource]:
    """Detect shopping data exports."""
    sources: list[DataSource] = []

    shopping_collected = "Shopping" in vault_stats

    amazon_csvs = _glob_downloads(
        "*amazon*order*.csv", "*Amazon*Order*.csv",
        "*amazon*.csv", "*Amazon*.csv",
        "01-Jan-*_to_*.csv",  # Amazon's default export filename pattern
    )
    sources.append(DataSource(
        id="amazon_orders",
        name="Amazon Order History",
        category="shopping",
        status=(
            "already_collected" if shopping_collected
            else "ready" if amazon_csvs
            else "discovered"
        ),
        location=(
            f"{vault_stats.get('Shopping', 0):,} orders in vault" if shopping_collected
            else str(amazon_csvs[0]) if amazon_csvs
            else "No Amazon order CSVs found in Downloads"
        ),
        nomolo_grade="A" if amazon_csvs else "C",
        estimated_records=vault_stats.get("Shopping", 0),
        time_to_collect="1 minute" if amazon_csvs else "requires setup",
        icon="\U0001f6d2",  # shopping cart
        description="Your complete Amazon order history with items, prices, and dates",
        action=(
            "Already in vault" if shopping_collected
            else f"nomolo collect shopping {amazon_csvs[0]}" if amazon_csvs
            else "Export from amazon.com/gp/b2b/reports"
        ),
    ))

    return sources


async def _scan_cloud() -> list[DataSource]:
    """Detect cloud storage services."""
    sources: list[DataSource] = []

    # iCloud
    icloud_exists = _path_exists(ICLOUD_FOLDER)
    sources.append(DataSource(
        id="icloud",
        name="iCloud Drive",
        category="cloud",
        status="ready" if icloud_exists else "discovered",
        location=str(ICLOUD_FOLDER) if icloud_exists else "iCloud Drive not found",
        nomolo_grade="C",
        estimated_records=0,
        time_to_collect="requires setup",
        icon="\U00002601",  # cloud
        description="Files stored in iCloud Drive including Documents, Desktop sync",
        action="Scan iCloud for documents" if icloud_exists else "Enable iCloud Drive in System Settings",
    ))

    # Dropbox
    dropbox = _find_dropbox_folder()
    sources.append(DataSource(
        id="dropbox",
        name="Dropbox",
        category="cloud",
        status="ready" if dropbox else "discovered",
        location=str(dropbox) if dropbox else "Dropbox not detected",
        nomolo_grade="C",
        estimated_records=0,
        time_to_collect="requires setup",
        icon="\U0001f4e5",  # inbox tray
        description="Files synced from Dropbox to your Mac",
        action="Scan Dropbox folder for documents" if dropbox else "Install Dropbox or locate folder",
    ))

    return sources


async def _scan_email(vault_stats: dict[str, int]) -> list[DataSource]:
    """Detect email sources beyond Gmail."""
    sources: list[DataSource] = []

    # .mbox files
    mbox_files = _glob_downloads("*.mbox")
    # Also check common locations
    mail_dir = HOME / "Library" / "Mail"
    mail_exists = _path_exists(mail_dir)

    if mbox_files:
        sources.append(DataSource(
            id="mbox_import",
            name="Email Archive (.mbox)",
            category="email",
            status="ready",
            location=str(mbox_files[0]),
            nomolo_grade="B",
            estimated_records=0,
            time_to_collect="5 minutes",
            icon="\U0001f4ec",  # mailbox with mail
            description="Archived email in standard mbox format",
            action=f"nomolo collect mbox {mbox_files[0]}",
        ))

    if mail_exists:
        sources.append(DataSource(
            id="apple_mail",
            name="Apple Mail",
            category="email",
            status="discovered",
            location=str(mail_dir),
            nomolo_grade="C+",
            estimated_records=0,
            time_to_collect="requires setup",
            icon="\U0001f4e8",  # incoming envelope
            description="Emails from Apple Mail (all configured accounts)",
            action="Coming soon — Apple Mail integration",
        ))

    return sources


async def _scan_notes(vault_stats: dict[str, int]) -> list[DataSource]:
    """Detect notes and knowledge bases."""
    sources: list[DataSource] = []

    notes_collected = "Notes" in vault_stats

    # Markdown files in common locations
    md_locations = [
        HOME / "Documents",
        HOME / "Desktop",
        HOME / "Obsidian",
        HOME / "Notes",
    ]
    md_count = 0
    md_root: Optional[Path] = None
    for loc in md_locations:
        if _path_exists(loc):
            try:
                found = list(loc.rglob("*.md"))
                if len(found) > md_count:
                    md_count = len(found)
                    md_root = loc
            except (OSError, PermissionError):
                pass

    sources.append(DataSource(
        id="notes_markdown",
        name="Markdown Notes",
        category="notes",
        status=(
            "already_collected" if notes_collected
            else "ready" if md_count > 0
            else "discovered"
        ),
        location=(
            f"{vault_stats.get('Notes', 0):,} notes in vault" if notes_collected
            else f"{md_count} .md files found under {md_root}" if md_root
            else "No markdown files found in common locations"
        ),
        nomolo_grade="A",
        estimated_records=vault_stats.get("Notes", md_count),
        time_to_collect="30 seconds" if md_count > 0 else "requires setup",
        icon="\U0001f4dd",  # memo
        description="Personal notes, journals, and knowledge base in Markdown format",
        action=(
            "Already in vault" if notes_collected
            else f"nomolo collect notes {md_root}" if md_root
            else "Point Nomolo at your notes folder"
        ),
    ))

    # Notion exports
    notion_exports = _glob_downloads(
        "Notion*.zip", "notion*.zip",
        "**/notion-export*",
    )
    notion_dirs: list[Path] = []
    try:
        notion_dirs = [
            d for d in DOWNLOADS.iterdir()
            if d.is_dir() and "notion" in d.name.lower()
        ] if _path_exists(DOWNLOADS) else []
    except (OSError, PermissionError):
        pass

    notion_found = len(notion_exports) > 0 or len(notion_dirs) > 0
    sources.append(DataSource(
        id="notion_export",
        name="Notion Export",
        category="notes",
        status="ready" if notion_found else "discovered",
        location=(
            str(notion_exports[0]) if notion_exports
            else str(notion_dirs[0]) if notion_dirs
            else "No Notion export found in Downloads"
        ),
        nomolo_grade="B+",
        estimated_records=0,
        time_to_collect="2 minutes" if notion_found else "requires setup",
        icon="\U0001f4d3",  # notebook
        description="Pages, databases, and workspace content exported from Notion",
        action=(
            f"nomolo collect notion {notion_exports[0]}" if notion_exports
            else "Export from Notion: Settings > Export all workspace content"
        ),
    ))

    return sources


# ---------------------------------------------------------------------------
# Main scan function
# ---------------------------------------------------------------------------

async def scan(
    vault_root: str = "vaults",
    project_root: str = ".",
    progress: ProgressCallback = None,
) -> dict:
    """
    Run the full data source discovery scan.

    Args:
        vault_root: Path to the vaults directory (relative or absolute).
        project_root: Path to the Nomolo project root.
        progress: Optional async callback(category, done, total) for live updates.

    Returns:
        Dict with keys:
            sources: list of DataSource dicts
            vault_stats: dict of vault name -> record count
            categories: dict of category -> list of source IDs
            summary: dict with totals
    """
    project = Path(project_root).resolve()
    vaults = Path(vault_root)
    if not vaults.is_absolute():
        vaults = project / vault_root
    vaults = vaults.resolve()

    # Pre-load vault stats (fast, synchronous)
    vault_stats = _get_vault_stats(vaults)

    categories_to_scan = [
        ("browsers", _scan_browsers(vault_stats)),
        ("google", _scan_google(vault_stats, project)),
        ("social", _scan_social()),
        ("messaging", _scan_messaging()),
        ("media", _scan_media(vault_stats)),
        ("health", _scan_health(vault_stats)),
        ("finance", _scan_finance(vault_stats)),
        ("shopping", _scan_shopping(vault_stats)),
        ("cloud", _scan_cloud()),
        ("email", _scan_email(vault_stats)),
        ("notes", _scan_notes(vault_stats)),
    ]

    total = len(categories_to_scan)
    all_sources: list[DataSource] = []
    category_map: dict[str, list[str]] = {}

    # Run all category scans concurrently
    tasks = [coro for _, coro in categories_to_scan]
    category_names = [name for name, _ in categories_to_scan]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    for i, (cat_name, result) in enumerate(zip(category_names, results)):
        if isinstance(result, Exception):
            logger.warning("Scanner error in %s: %s", cat_name, result)
            continue

        for source in result:
            all_sources.append(source)
            category_map.setdefault(source.category, []).append(source.id)

        if progress:
            try:
                await progress(cat_name, i + 1, total)
            except Exception:
                pass  # Don't let callback errors break the scan

    # Build summary
    total_sources = len(all_sources)
    ready = sum(1 for s in all_sources if s.status == "ready")
    collected = sum(1 for s in all_sources if s.status == "already_collected")
    discovered = sum(1 for s in all_sources if s.status == "discovered")
    total_records = sum(s.estimated_records for s in all_sources)

    return {
        "sources": [s.to_dict() for s in all_sources],
        "vault_stats": vault_stats,
        "categories": category_map,
        "summary": {
            "total_sources": total_sources,
            "ready_to_import": ready,
            "already_collected": collected,
            "discovered": discovered,
            "total_records": total_records,
        },
    }


# ---------------------------------------------------------------------------
# Life Archive Score
# ---------------------------------------------------------------------------

# Category weights for scoring — higher weight = more important to a
# complete life archive
_CATEGORY_WEIGHTS: dict[str, float] = {
    "browsers": 8.0,
    "email": 15.0,
    "google": 10.0,
    "social": 10.0,
    "messaging": 10.0,
    "media": 8.0,
    "health": 8.0,
    "finance": 8.0,
    "shopping": 5.0,
    "cloud": 3.0,
    "notes": 8.0,
}

# Minimum sources per category to get full marks
_CATEGORY_TARGETS: dict[str, int] = {
    "browsers": 1,
    "email": 1,
    "google": 2,
    "social": 2,
    "messaging": 1,
    "media": 2,
    "health": 1,
    "finance": 1,
    "shopping": 1,
    "cloud": 1,
    "notes": 1,
}


def get_life_score(scan_results: dict) -> dict:
    """
    Calculate a Life Archive Score from scan results.

    Returns:
        Dict with keys:
            overall: int 0-100
            grade: str (A+ through F)
            categories: dict of category -> {score, max, collected, suggestions}
            suggestions: list of top improvement suggestions
    """
    sources = scan_results.get("sources", [])

    # Group sources by category
    by_category: dict[str, list[dict]] = {}
    for s in sources:
        by_category.setdefault(s["category"], []).append(s)

    category_scores: dict[str, dict] = {}
    weighted_total = 0.0
    weight_sum = 0.0
    all_suggestions: list[dict] = []

    for cat, weight in _CATEGORY_WEIGHTS.items():
        cat_sources = by_category.get(cat, [])
        target = _CATEGORY_TARGETS.get(cat, 1)

        collected_count = sum(
            1 for s in cat_sources
            if s["status"] == "already_collected"
        )
        ready_count = sum(
            1 for s in cat_sources
            if s["status"] == "ready"
        )

        # Score: full marks for collected, half marks for ready (discoverable)
        effective = collected_count + (ready_count * 0.3)
        cat_score = min(effective / target, 1.0) * 100

        category_scores[cat] = {
            "score": round(cat_score),
            "max": 100,
            "weight": weight,
            "collected": collected_count,
            "ready": ready_count,
            "total": len(cat_sources),
        }

        weighted_total += cat_score * weight
        weight_sum += weight

        # Generate suggestions for uncollected categories
        if cat_score < 100:
            # Find best ready-to-import source in this category
            ready_sources = [
                s for s in cat_sources if s["status"] == "ready"
            ]
            discovered_sources = [
                s for s in cat_sources if s["status"] == "discovered"
            ]

            if ready_sources:
                best = ready_sources[0]
                all_suggestions.append({
                    "category": cat,
                    "impact": weight * (1.0 - cat_score / 100),
                    "source": best["name"],
                    "action": best["action"],
                    "time": best["time_to_collect"],
                    "message": f"Import {best['name']} — {best['time_to_collect']}, file already on your machine",
                })
            elif discovered_sources:
                best = discovered_sources[0]
                all_suggestions.append({
                    "category": cat,
                    "impact": weight * (1.0 - cat_score / 100) * 0.5,
                    "source": best["name"],
                    "action": best["action"],
                    "time": best["time_to_collect"],
                    "message": f"Set up {best['name']} — {best['action']}",
                })

    # Overall score
    overall = round(weighted_total / weight_sum) if weight_sum > 0 else 0

    # Letter grade
    if overall >= 97:
        grade = "A+"
    elif overall >= 93:
        grade = "A"
    elif overall >= 90:
        grade = "A-"
    elif overall >= 87:
        grade = "B+"
    elif overall >= 83:
        grade = "B"
    elif overall >= 80:
        grade = "B-"
    elif overall >= 77:
        grade = "C+"
    elif overall >= 73:
        grade = "C"
    elif overall >= 70:
        grade = "C-"
    elif overall >= 67:
        grade = "D+"
    elif overall >= 63:
        grade = "D"
    elif overall >= 60:
        grade = "D-"
    else:
        grade = "F"

    # Sort suggestions by impact (highest first)
    all_suggestions.sort(key=lambda s: s["impact"], reverse=True)

    return {
        "overall": overall,
        "grade": grade,
        "categories": category_scores,
        "suggestions": all_suggestions[:5],  # Top 5 improvements
    }
