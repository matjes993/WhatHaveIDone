"""
NOMOLO YouTube Collector
Parses YouTube watch history and search history from Google Takeout export
into the unified YouTube JSONL vault.

Google Takeout path: Takeout/YouTube and YouTube Music/history/watch-history.json
Optional search:    Takeout/YouTube and YouTube Music/history/search-history.json

Usage:
  nomolo collect youtube ~/Downloads/Takeout/YouTube\ and\ YouTube\ Music/history/
  nomolo collect youtube ~/Downloads/Takeout/.../watch-history.json
"""

import hashlib
import json
import logging
import os
import re
from datetime import datetime

from core.vault import flush_entries, load_processed_ids, append_processed_ids

logger = logging.getLogger("nomolo.youtube")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_id(entry_type, *parts):
    """Generate a deterministic 12-char hex ID from type and key parts."""
    raw = ":".join(str(p) for p in parts)
    return f"youtube:{entry_type}:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]


def _extract_video_id(url):
    """Extract the video ID from a YouTube URL."""
    if not url:
        return ""
    match = re.search(r"[?&]v=([a-zA-Z0-9_-]{11})", url)
    if match:
        return match.group(1)
    # Short URL format: youtu.be/xxxxx
    match = re.search(r"youtu\.be/([a-zA-Z0-9_-]{11})", url)
    if match:
        return match.group(1)
    return ""


def _parse_timestamp(ts_str):
    """
    Parse a timestamp string into a datetime object.
    Handles ISO 8601 with various suffixes.
    Returns None on failure.
    """
    if not ts_str:
        return None
    # Strip trailing Z and microseconds for consistent parsing
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
    ):
        try:
            return datetime.strptime(ts_str, fmt)
        except ValueError:
            continue
    logger.warning("Could not parse timestamp: %r", ts_str)
    return None


def _find_history_files(export_path):
    """
    Locate watch-history.json and search-history.json from an export path.

    Accepts:
      - Direct path to watch-history.json
      - Path to the YouTube history directory
      - Path to the YouTube and YouTube Music directory
      - Path to the Takeout root directory

    Returns (watch_history_path, search_history_path) — either may be None.
    """
    export_path = os.path.expanduser(export_path)

    # If it's a direct file path
    if os.path.isfile(export_path):
        parent = os.path.dirname(export_path)
        basename = os.path.basename(export_path).lower()

        if "watch-history" in basename or "watch_history" in basename:
            watch_path = export_path
        else:
            watch_path = None

        # Look for search history in the same directory
        search_path = None
        for name in ("search-history.json", "search_history.json"):
            candidate = os.path.join(parent, name)
            if os.path.isfile(candidate):
                search_path = candidate
                break

        return watch_path, search_path

    # If it's a directory, search for the files
    if os.path.isdir(export_path):
        watch_path = None
        search_path = None

        # Candidate paths relative to export_path
        watch_candidates = [
            "watch-history.json",
            "history/watch-history.json",
            "YouTube and YouTube Music/history/watch-history.json",
            "Takeout/YouTube and YouTube Music/history/watch-history.json",
        ]
        search_candidates = [
            "search-history.json",
            "history/search-history.json",
            "YouTube and YouTube Music/history/search-history.json",
            "Takeout/YouTube and YouTube Music/history/search-history.json",
        ]

        for candidate in watch_candidates:
            full = os.path.join(export_path, candidate)
            if os.path.isfile(full):
                watch_path = full
                break

        for candidate in search_candidates:
            full = os.path.join(export_path, candidate)
            if os.path.isfile(full):
                search_path = full
                break

        return watch_path, search_path

    logger.error("Export path not found: %s", export_path)
    return None, None


# ---------------------------------------------------------------------------
# Entry parsers
# ---------------------------------------------------------------------------

def _parse_watch_entry(item):
    """
    Parse a single watch history JSON entry into a vault entry dict.
    Returns None if the entry is not a valid watch entry.
    """
    title = item.get("title", "")
    if not title:
        return None

    # Strip "Watched " prefix added by Google Takeout
    if title.startswith("Watched "):
        title = title[len("Watched "):]

    url = item.get("titleUrl", "")
    video_id = _extract_video_id(url)
    ts = item.get("time", "")

    # Extract channel name from subtitles
    channel = ""
    subtitles = item.get("subtitles", [])
    if subtitles and isinstance(subtitles, list):
        channel = subtitles[0].get("name", "")

    dt = _parse_timestamp(ts)
    year = dt.year if dt else 0
    month = dt.month if dt else 0
    date_str = dt.strftime("%Y-%m-%d") if dt else ""

    entry_id = _make_id("watch", url or title, ts)

    # Build embedding text
    embedding_parts = [f"Watched '{title}'"]
    if channel:
        embedding_parts[0] += f" by {channel}"
    if date_str:
        embedding_parts[0] += f" on {date_str}"

    return {
        "id": entry_id,
        "sources": ["youtube"],
        "type": "watch",
        "title": title,
        "channel": channel,
        "url": url,
        "video_id": video_id,
        "watched_at": ts,
        "year": year,
        "month": month,
        "updated_at": datetime.now().isoformat(),
        "youtube_for_embedding": embedding_parts[0],
    }


def _parse_search_entry(item):
    """
    Parse a single search history JSON entry into a vault entry dict.
    Returns None if the entry is not a valid search entry.
    """
    title = item.get("title", "")
    if not title:
        return None

    # Strip "Searched for " prefix added by Google Takeout
    query = title
    if query.startswith("Searched for "):
        query = query[len("Searched for "):]

    ts = item.get("time", "")
    dt = _parse_timestamp(ts)
    year = dt.year if dt else 0
    month = dt.month if dt else 0
    date_str = dt.strftime("%Y-%m-%d") if dt else ""

    entry_id = _make_id("search", query, ts)

    # Build embedding text
    embedding = f"Searched for '{query}'"
    if date_str:
        embedding += f" on {date_str}"

    return {
        "id": entry_id,
        "sources": ["youtube"],
        "type": "search",
        "query": query,
        "searched_at": ts,
        "year": year,
        "month": month,
        "updated_at": datetime.now().isoformat(),
        "youtube_for_embedding": embedding,
    }


def _load_json(file_path):
    """Load and parse a JSON file with encoding fallback."""
    encodings = ["utf-8-sig", "utf-8", "latin-1"]

    for encoding in encodings:
        try:
            with open(file_path, "r", encoding=encoding) as f:
                return json.load(f)
        except UnicodeDecodeError:
            continue
        except json.JSONDecodeError as e:
            logger.error("Invalid JSON in %s: %s", file_path, e)
            raise
        except FileNotFoundError:
            logger.error("File not found: %s", file_path)
            raise
        except OSError as e:
            logger.error("Cannot read %s: %s", file_path, e)
            raise

    logger.error("Could not decode %s with any supported encoding", file_path)
    raise ValueError(f"Cannot decode JSON file: {file_path}")


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_import(export_path, config=None):
    """
    Import YouTube history from a Google Takeout export into the vault.

    Accepts path to watch-history.json file or the Takeout YouTube directory.

    Args:
        export_path: Path to JSON file or YouTube export directory.
        config: Dict with optional 'vault_root' key.
    """
    config = config or {}
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    vault_root_base = config.get("vault_root", os.path.join(project_root, "vaults"))
    vault_root_base = os.path.expanduser(vault_root_base)
    if not os.path.isabs(vault_root_base):
        vault_root_base = os.path.join(project_root, vault_root_base)
    vault_path = os.path.join(vault_root_base, "YouTube")

    print(f"\n  NOMOLO YouTube Collector")
    print(f"  {'=' * 45}")
    print(f"  Path: {export_path}")
    print(f"  Vault: {vault_path}")

    # Find history files
    watch_path, search_path = _find_history_files(export_path)

    if watch_path is None and search_path is None:
        print("  Error: No watch-history.json or search-history.json found.")
        print("  Provide the path to watch-history.json or the YouTube export directory.")
        return

    if watch_path:
        print(f"  Watch history: {watch_path}")
    if search_path:
        print(f"  Search history: {search_path}")

    # Load already-processed IDs
    processed_ids = load_processed_ids(vault_path)
    if processed_ids:
        print(f"  Already processed: {len(processed_ids):,}")

    new_entries = []
    skipped_duplicate = 0
    skipped_invalid = 0
    watch_count = 0
    search_count = 0

    # Parse watch history
    if watch_path:
        try:
            watch_data = _load_json(watch_path)
        except (ValueError, OSError) as e:
            print(f"  Error reading watch history: {e}")
            watch_data = []

        if not isinstance(watch_data, list):
            logger.warning("watch-history.json is not a list, skipping")
            watch_data = []

        print(f"  Watch entries found: {len(watch_data):,}")

        for item in watch_data:
            try:
                entry = _parse_watch_entry(item)
            except Exception as e:
                logger.warning("Skipping watch entry: %s", e)
                skipped_invalid += 1
                continue

            if entry is None:
                skipped_invalid += 1
                continue
            if entry["id"] in processed_ids:
                skipped_duplicate += 1
                continue

            new_entries.append(entry)
            watch_count += 1

    # Parse search history
    if search_path:
        try:
            search_data = _load_json(search_path)
        except (ValueError, OSError) as e:
            print(f"  Error reading search history: {e}")
            search_data = []

        if not isinstance(search_data, list):
            logger.warning("search-history.json is not a list, skipping")
            search_data = []

        print(f"  Search entries found: {len(search_data):,}")

        for item in search_data:
            try:
                entry = _parse_search_entry(item)
            except Exception as e:
                logger.warning("Skipping search entry: %s", e)
                skipped_invalid += 1
                continue

            if entry is None:
                skipped_invalid += 1
                continue
            if entry["id"] in processed_ids:
                skipped_duplicate += 1
                continue

            new_entries.append(entry)
            search_count += 1

    if not new_entries:
        print("  Nothing new — vault is up to date.")
        return

    # Flush to vault
    flush_entries(new_entries, vault_path, "youtube.jsonl")
    new_ids = [e["id"] for e in new_entries]
    append_processed_ids(vault_path, new_ids)

    # Collect year stats
    year_counts = {}
    for e in new_entries:
        y = e.get("year", 0)
        if y:
            year_counts[y] = year_counts.get(y, 0) + 1

    # Collect unique channels
    channels = set()
    for e in new_entries:
        ch = e.get("channel", "")
        if ch:
            channels.add(ch)

    print()
    print(f"  {'=' * 45}")
    print(f"  Done! {len(new_entries):,} entries saved")
    print(f"  {'=' * 45}")
    print(f"    Watch entries:   {watch_count:,}")
    print(f"    Search entries:  {search_count:,}")
    print(f"    Unique channels: {len(channels):,}")
    if year_counts:
        for year in sorted(year_counts.keys()):
            print(f"    {year}: {year_counts[year]:,}")
    if skipped_duplicate:
        print(f"    Skipped (dupe):  {skipped_duplicate:,}")
    if skipped_invalid:
        print(f"    Skipped (invalid): {skipped_invalid:,}")
    print()

    logger.info(
        "YouTube import complete: %d watch, %d search, %d duplicate, %d invalid",
        watch_count, search_count, skipped_duplicate, skipped_invalid,
    )
