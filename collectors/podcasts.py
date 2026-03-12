"""
WHID Podcasts Collector
Parses podcast listening history from Podcast Addict SQLite backup or
generic CSV export into the unified Podcasts JSONL vault.

Supports two import modes:
  1. Podcast Addict backup.db: whid collect podcasts ~/Downloads/backup.db
  2. CSV export:               whid collect podcasts ~/Downloads/podcast_history.csv

Both write to the Podcasts/ vault directory with unified schema.
"""

import csv
import hashlib
import logging
import os
import sqlite3
from datetime import datetime

from core.vault import flush_entries, load_processed_ids, append_processed_ids

logger = logging.getLogger("whid.podcasts")


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def _normalize_columns(header_row):
    """
    Build a case-insensitive, whitespace-stripped mapping from normalized
    column name to its actual index.
    """
    mapping = {}
    for idx, col in enumerate(header_row):
        key = col.strip().lower()
        mapping[key] = idx
    return mapping


def _get(row, col_map, *names, default=""):
    """Return the first matching column value from row, or default."""
    for name in names:
        idx = col_map.get(name.lower())
        if idx is not None and idx < len(row):
            val = row[idx].strip()
            if val:
                return val
    return default


def _read_csv(export_path):
    """
    Read a CSV file, handling BOM and encoding issues.
    Returns (column_map, rows) where column_map maps normalized header
    names to column indices.
    """
    encodings = ["utf-8-sig", "utf-8", "latin-1"]

    for encoding in encodings:
        try:
            with open(export_path, "r", encoding=encoding, newline="") as f:
                reader = csv.reader(f)
                header = next(reader, None)
                if header is None:
                    logger.error("CSV file is empty: %s", export_path)
                    return None, []

                col_map = _normalize_columns(header)
                rows = list(reader)
                logger.info(
                    "Read %d rows from %s (encoding=%s)",
                    len(rows), export_path, encoding,
                )
                return col_map, rows
        except UnicodeDecodeError:
            continue
        except FileNotFoundError:
            logger.error("CSV file not found: %s", export_path)
            raise
        except OSError as e:
            logger.error("Cannot read CSV file %s: %s", export_path, e)
            raise

    logger.error(
        "Could not decode %s with any supported encoding (tried %s)",
        export_path, ", ".join(encodings),
    )
    raise ValueError(f"Cannot decode CSV file: {export_path}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_id(*parts):
    """Generate a deterministic 12-char hex ID from key parts."""
    raw = ":".join(str(p) for p in parts)
    return "podcasts:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]


def _seconds_to_readable(seconds):
    """
    Convert seconds to a human-readable duration string.
    Examples: 2730 -> "45:30", 3661 -> "1:01:01"
    """
    if not seconds or seconds <= 0:
        return "0:00"

    seconds = int(seconds)
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60

    if hours > 0:
        return f"{hours}:{minutes:02d}:{secs:02d}"
    return f"{minutes}:{secs:02d}"


def _safe_int(value, default=0):
    """Convert a string to int, returning default on failure."""
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return default


def _safe_float(value, default=0.0):
    """Convert a string to float, returning default on failure."""
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def _parse_date(date_str):
    """
    Parse a date string into a datetime object.
    Returns None on failure.
    """
    if not date_str:
        return None

    for fmt in (
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d",
        "%d/%m/%Y",
        "%m/%d/%Y",
    ):
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue

    logger.warning("Could not parse date: %r", date_str)
    return None


def _duration_str_to_seconds(duration_str):
    """
    Convert a duration string like "45:30" or "1:05:30" to seconds.
    Returns 0 on failure.
    """
    if not duration_str:
        return 0

    parts = duration_str.strip().split(":")
    try:
        if len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
        elif len(parts) == 2:
            return int(parts[0]) * 60 + int(parts[1])
        elif len(parts) == 1:
            return int(parts[0])
    except (ValueError, TypeError):
        pass
    return 0


# ---------------------------------------------------------------------------
# CSV episode parser
# ---------------------------------------------------------------------------

def _parse_episode_row(row, col_map):
    """
    Convert a single CSV row to a vault entry dict.
    Expected CSV columns: Podcast, Episode, Date Listened, Duration, URL
    Returns None if the row lacks an episode title.
    """
    episode_title = _get(row, col_map, "episode", "episode title", "title")
    if not episode_title:
        return None

    podcast_name = _get(row, col_map, "podcast", "podcast name", "show")
    date_listened = _get(row, col_map, "date listened", "date", "listened date")
    duration_str = _get(row, col_map, "duration", "length")
    url = _get(row, col_map, "url", "episode url", "link")
    author = _get(row, col_map, "author", "artist", "creator")
    description = _get(row, col_map, "description", "summary")

    dt = _parse_date(date_listened)
    date_str = dt.strftime("%Y-%m-%d") if dt else date_listened
    year = dt.year if dt else 0
    month = dt.month if dt else 0

    duration_seconds = _duration_str_to_seconds(duration_str)
    if not duration_str and duration_seconds == 0:
        readable_duration = ""
    elif not duration_str:
        readable_duration = _seconds_to_readable(duration_seconds)
    else:
        readable_duration = duration_str

    entry_id = _make_id(podcast_name, episode_title, date_str)

    # Build embedding text
    embedding = f"Listened to '{episode_title}' from {podcast_name}"
    if author:
        embedding += f" by {author}"
    if date_str:
        embedding += f" on {date_str}"
    if readable_duration:
        embedding += f" — {readable_duration}"

    return {
        "id": entry_id,
        "sources": ["csv"],
        "podcast_name": podcast_name,
        "episode_title": episode_title,
        "description": description,
        "listened_at": date_str,
        "duration": readable_duration,
        "duration_seconds": duration_seconds,
        "progress": 1.0,
        "url": url,
        "author": author,
        "year": year,
        "month": month,
        "updated_at": datetime.now().isoformat(),
        "podcast_for_embedding": embedding,
    }


# ---------------------------------------------------------------------------
# SQLite (Podcast Addict) parser
# ---------------------------------------------------------------------------

def _parse_db_episode(episode_row, podcast_info):
    """
    Convert a Podcast Addict DB episode row + podcast info into a vault entry dict.

    episode_row: dict-like with keys from the episodes table
    podcast_info: dict with podcast-level info (name, author)

    Returns None if the episode lacks a name.
    """
    episode_title = episode_row.get("name", "")
    if not episode_title:
        return None

    podcast_name = podcast_info.get("name", "")
    author = podcast_info.get("author", "")
    description = episode_row.get("description", "") or ""
    url = episode_row.get("url", "") or ""

    # Parse date
    date_published = episode_row.get("date_published", "")
    if isinstance(date_published, (int, float)) and date_published > 0:
        # Timestamp in milliseconds
        try:
            dt = datetime.fromtimestamp(date_published / 1000)
        except (OSError, ValueError):
            dt = None
    elif isinstance(date_published, str):
        dt = _parse_date(date_published)
    else:
        dt = None

    date_str = dt.strftime("%Y-%m-%d") if dt else ""
    year = dt.year if dt else 0
    month = dt.month if dt else 0

    # Duration and progress
    duration = _safe_int(episode_row.get("duration", 0))
    duration_seconds = duration // 1000 if duration > 1000 else duration
    playback_position = _safe_int(episode_row.get("playback_position", 0))
    playback_seconds = playback_position // 1000 if playback_position > 1000 else playback_position

    if duration_seconds > 0:
        progress = min(playback_seconds / duration_seconds, 1.0)
    else:
        is_played = episode_row.get("is_played", 0)
        progress = 1.0 if is_played else 0.0

    readable_duration = _seconds_to_readable(duration_seconds)

    entry_id = _make_id(podcast_name, episode_title, date_str)

    # Build embedding text
    embedding = f"Listened to '{episode_title}' from {podcast_name}"
    if author:
        embedding += f" by {author}"
    if date_str:
        embedding += f" on {date_str}"
    if readable_duration:
        embedding += f" — {readable_duration}"

    return {
        "id": entry_id,
        "sources": ["podcast-addict"],
        "podcast_name": podcast_name,
        "episode_title": episode_title,
        "description": description[:500] if description else "",
        "listened_at": date_str,
        "duration": readable_duration,
        "duration_seconds": duration_seconds,
        "progress": round(progress, 2),
        "url": url,
        "author": author,
        "year": year,
        "month": month,
        "updated_at": datetime.now().isoformat(),
        "podcast_for_embedding": embedding,
    }


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_import(export_path, config=None):
    """
    Import podcast listening history into the vault.

    Accepts path to a Podcast Addict backup.db or a CSV file.

    Args:
        export_path: Path to backup.db or CSV file.
        config: Dict with optional 'vault_root' key.
    """
    config = config or {}
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    vault_root_base = config.get("vault_root", os.path.join(project_root, "vaults"))
    vault_root_base = os.path.expanduser(vault_root_base)
    if not os.path.isabs(vault_root_base):
        vault_root_base = os.path.join(project_root, vault_root_base)
    vault_path = os.path.join(vault_root_base, "Podcasts")

    export_path = os.path.expanduser(export_path)

    # Detect format by extension
    is_sqlite = export_path.lower().endswith((".db", ".sqlite", ".sqlite3"))

    if is_sqlite:
        _import_from_db(export_path, vault_path)
    else:
        _import_from_csv(export_path, vault_path)


def _import_from_csv(export_path, vault_path):
    """Import podcast history from a CSV file."""
    print(f"\n  WHID Podcasts Collector — CSV")
    print(f"  {'=' * 45}")
    print(f"  CSV: {export_path}")
    print(f"  Vault: {vault_path}")

    # Read CSV
    col_map, rows = _read_csv(export_path)
    if col_map is None:
        return

    print(f"  Episodes found: {len(rows)}")

    # Load already-processed IDs
    processed_ids = load_processed_ids(vault_path)
    if processed_ids:
        print(f"  Already processed: {len(processed_ids):,}")

    # Convert rows to entries
    new_entries = []
    skipped_empty = 0
    skipped_duplicate = 0

    for row in rows:
        try:
            entry = _parse_episode_row(row, col_map)
        except Exception as e:
            logger.warning("Skipping row: %s", e)
            skipped_empty += 1
            continue

        if entry is None:
            skipped_empty += 1
            continue
        if entry["id"] in processed_ids:
            skipped_duplicate += 1
            continue

        new_entries.append(entry)

    if not new_entries:
        print("  Nothing new — vault is up to date.")
        return

    # Flush to vault
    flush_entries(new_entries, vault_path, "podcasts.jsonl")
    new_ids = [e["id"] for e in new_entries]
    append_processed_ids(vault_path, new_ids)

    # Summary stats
    unique_podcasts = set(e["podcast_name"] for e in new_entries if e.get("podcast_name"))
    total_seconds = sum(e.get("duration_seconds", 0) for e in new_entries)
    total_hours = total_seconds / 3600

    print()
    print(f"  {'=' * 45}")
    print(f"  Done! {len(new_entries):,} episodes saved")
    print(f"  {'=' * 45}")
    print(f"    Unique podcasts: {len(unique_podcasts):,}")
    print(f"    Total listening: {total_hours:.1f} hours")
    if skipped_duplicate:
        print(f"    Skipped (dupe):  {skipped_duplicate:,}")
    if skipped_empty:
        print(f"    Skipped (empty): {skipped_empty:,}")
    print()

    logger.info(
        "CSV podcast import complete: %d new, %d duplicate, %d empty",
        len(new_entries), skipped_duplicate, skipped_empty,
    )


def _import_from_db(export_path, vault_path):
    """Import podcast history from a Podcast Addict SQLite backup."""
    print(f"\n  WHID Podcasts Collector — Podcast Addict")
    print(f"  {'=' * 45}")
    print(f"  Database: {export_path}")
    print(f"  Vault: {vault_path}")

    if not os.path.isfile(export_path):
        print(f"  Error: File not found: {export_path}")
        return

    try:
        conn = sqlite3.connect(f"file:{export_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
    except sqlite3.Error as e:
        print(f"  Error opening database: {e}")
        return

    try:
        # Load podcast info into a lookup dict
        podcast_lookup = {}
        try:
            cursor = conn.execute("SELECT _id, name, author, url, description FROM podcasts")
            for row in cursor:
                podcast_lookup[row["_id"]] = {
                    "name": row["name"] or "",
                    "author": row["author"] or "",
                    "url": row["url"] or "",
                    "description": row["description"] or "",
                }
        except sqlite3.Error as e:
            logger.warning("Could not read podcasts table: %s", e)

        # Query episodes that have been played
        try:
            cursor = conn.execute(
                "SELECT _id, name, description, url, podcast_id, "
                "date_published, duration, playback_position, is_played "
                "FROM episodes WHERE is_played = 1 OR playback_position > 0"
            )
            episode_rows = [dict(row) for row in cursor]
        except sqlite3.Error as e:
            print(f"  Error querying episodes: {e}")
            return

        print(f"  Played episodes found: {len(episode_rows)}")
        print(f"  Podcasts in library: {len(podcast_lookup)}")

        # Load already-processed IDs
        processed_ids = load_processed_ids(vault_path)
        if processed_ids:
            print(f"  Already processed: {len(processed_ids):,}")

        new_entries = []
        skipped_empty = 0
        skipped_duplicate = 0

        for ep in episode_rows:
            podcast_id = ep.get("podcast_id")
            podcast_info = podcast_lookup.get(podcast_id, {"name": "", "author": ""})

            try:
                entry = _parse_db_episode(ep, podcast_info)
            except Exception as e:
                logger.warning("Skipping episode: %s", e)
                skipped_empty += 1
                continue

            if entry is None:
                skipped_empty += 1
                continue
            if entry["id"] in processed_ids:
                skipped_duplicate += 1
                continue

            new_entries.append(entry)

        if not new_entries:
            print("  Nothing new — vault is up to date.")
            return

        # Flush to vault
        flush_entries(new_entries, vault_path, "podcasts.jsonl")
        new_ids = [e["id"] for e in new_entries]
        append_processed_ids(vault_path, new_ids)

        # Summary stats
        unique_podcasts = set(e["podcast_name"] for e in new_entries if e.get("podcast_name"))
        fully_played = sum(1 for e in new_entries if e.get("progress", 0) >= 0.9)
        total_seconds = sum(e.get("duration_seconds", 0) for e in new_entries)
        total_hours = total_seconds / 3600

        print()
        print(f"  {'=' * 45}")
        print(f"  Done! {len(new_entries):,} episodes saved")
        print(f"  {'=' * 45}")
        print(f"    Unique podcasts: {len(unique_podcasts):,}")
        print(f"    Fully played:    {fully_played:,}")
        print(f"    Total listening: {total_hours:.1f} hours")
        if skipped_duplicate:
            print(f"    Skipped (dupe):  {skipped_duplicate:,}")
        if skipped_empty:
            print(f"    Skipped (empty): {skipped_empty:,}")
        print()

        logger.info(
            "Podcast Addict import complete: %d new, %d duplicate, %d empty",
            len(new_entries), skipped_duplicate, skipped_empty,
        )

    finally:
        conn.close()
