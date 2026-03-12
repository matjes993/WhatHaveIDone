"""
NOMOLO Music Collector
Parses Spotify extended streaming history JSON into the unified Music JSONL vault.

Spotify data export includes StreamingHistory_music_0.json, StreamingHistory_music_1.json,
etc. (extended format) or older StreamingHistory0.json (legacy format).

Usage:
  nomolo collect music ~/Downloads/my_spotify_data/
  nomolo collect music ~/Downloads/StreamingHistory_music_0.json
"""

import hashlib
import json
import logging
import os
import re
from datetime import datetime

from core.vault import flush_entries, load_processed_ids, append_processed_ids

logger = logging.getLogger("nomolo.music")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_id(*parts):
    """Generate a deterministic 12-char hex ID from key parts."""
    raw = ":".join(str(p) for p in parts)
    return "music:spotify:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]


def _ms_to_readable(ms):
    """
    Convert milliseconds to a human-readable duration string.
    Examples: 213000 -> "3:33", 3600000 -> "1:00:00"
    """
    if not ms or ms <= 0:
        return "0:00"

    total_seconds = int(ms / 1000)
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60

    if hours > 0:
        return f"{hours}:{minutes:02d}:{seconds:02d}"
    return f"{minutes}:{seconds:02d}"


def _parse_timestamp(ts_str):
    """
    Parse a timestamp string into a datetime object.
    Handles both extended and legacy Spotify formats.
    Returns None on failure.
    """
    if not ts_str:
        return None

    for fmt in (
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
    ):
        try:
            return datetime.strptime(ts_str, fmt)
        except ValueError:
            continue

    logger.warning("Could not parse timestamp: %r", ts_str)
    return None


def _find_streaming_files(export_path):
    """
    Locate Spotify streaming history JSON files from an export path.

    Accepts:
      - Direct path to a StreamingHistory JSON file
      - Path to the Spotify data export directory

    Returns list of file paths sorted by name.
    """
    export_path = os.path.expanduser(export_path)

    # If it's a direct file path
    if os.path.isfile(export_path):
        return [export_path]

    # If it's a directory, search for streaming history files
    if os.path.isdir(export_path):
        files = []
        for entry in os.listdir(export_path):
            lower = entry.lower()
            if lower.endswith(".json") and (
                "streaminghistory" in lower
                or "streaming_history" in lower
            ):
                files.append(os.path.join(export_path, entry))

        # Also check subdirectories (Spotify sometimes nests data)
        for subdir_name in (
            "Spotify Account Data",
            "Spotify Extended Streaming History",
            "MyData",
        ):
            subdir = os.path.join(export_path, subdir_name)
            if os.path.isdir(subdir):
                for entry in os.listdir(subdir):
                    lower = entry.lower()
                    if lower.endswith(".json") and (
                        "streaminghistory" in lower
                        or "streaming_history" in lower
                    ):
                        files.append(os.path.join(subdir, entry))

        return sorted(set(files))

    logger.error("Export path not found: %s", export_path)
    return []


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


def _is_extended_format(item):
    """Detect whether a JSON entry is in extended or legacy format."""
    return "ts" in item or "master_metadata_track_name" in item


# ---------------------------------------------------------------------------
# Entry parsers
# ---------------------------------------------------------------------------

def _parse_extended_entry(item):
    """
    Parse a single extended streaming history JSON entry into a vault entry dict.
    Returns None if the entry lacks track metadata.
    """
    track = item.get("master_metadata_track_name", "")
    if not track:
        return None

    artist = item.get("master_metadata_album_artist_name", "")
    album = item.get("master_metadata_album_album_name", "")
    ts = item.get("ts", "")
    ms_played = item.get("ms_played", 0)
    platform = item.get("platform", "")
    skipped = item.get("skipped", False)
    spotify_uri = item.get("spotify_track_uri", "")

    dt = _parse_timestamp(ts)
    year = dt.year if dt else 0
    month = dt.month if dt else 0
    date_str = dt.strftime("%Y-%m-%d") if dt else ""

    duration_readable = _ms_to_readable(ms_played)
    entry_id = _make_id(track, artist, ts)

    # Build embedding text
    embedding = f"Listened to '{track}' by {artist}"
    if album:
        embedding += f" ({album})"
    if date_str:
        embedding += f" on {date_str}"
    embedding += f" — {duration_readable}"

    return {
        "id": entry_id,
        "sources": ["spotify"],
        "track": track,
        "artist": artist,
        "album": album,
        "played_at": ts,
        "duration_ms": ms_played,
        "duration_readable": duration_readable,
        "platform": platform,
        "skipped": skipped if skipped is not None else False,
        "year": year,
        "month": month,
        "updated_at": datetime.now().isoformat(),
        "listen_for_embedding": embedding,
    }


def _parse_legacy_entry(item):
    """
    Parse a single legacy streaming history JSON entry into a vault entry dict.
    Returns None if the entry lacks track metadata.
    """
    track = item.get("trackName", "")
    if not track:
        return None

    artist = item.get("artistName", "")
    ts = item.get("endTime", "")
    ms_played = item.get("msPlayed", 0)

    dt = _parse_timestamp(ts)
    year = dt.year if dt else 0
    month = dt.month if dt else 0
    date_str = dt.strftime("%Y-%m-%d") if dt else ""

    duration_readable = _ms_to_readable(ms_played)
    entry_id = _make_id(track, artist, ts)

    # Build embedding text
    embedding = f"Listened to '{track}' by {artist}"
    if date_str:
        embedding += f" on {date_str}"
    embedding += f" — {duration_readable}"

    return {
        "id": entry_id,
        "sources": ["spotify"],
        "track": track,
        "artist": artist,
        "album": "",
        "played_at": ts,
        "duration_ms": ms_played,
        "duration_readable": duration_readable,
        "platform": "",
        "skipped": False,
        "year": year,
        "month": month,
        "updated_at": datetime.now().isoformat(),
        "listen_for_embedding": embedding,
    }


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_import(export_path, config=None):
    """
    Import Spotify streaming history into the vault.

    Accepts path to a JSON file or the Spotify export directory.

    Args:
        export_path: Path to JSON file or Spotify export directory.
        config: Dict with optional 'vault_root' key.
    """
    config = config or {}
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    vault_root_base = config.get("vault_root", os.path.join(project_root, "vaults"))
    vault_root_base = os.path.expanduser(vault_root_base)
    if not os.path.isabs(vault_root_base):
        vault_root_base = os.path.join(project_root, vault_root_base)
    vault_path = os.path.join(vault_root_base, "Music")

    print(f"\n  NOMOLO Music Collector — Spotify")
    print(f"  {'=' * 45}")
    print(f"  Path: {export_path}")
    print(f"  Vault: {vault_path}")

    # Find streaming history files
    history_files = _find_streaming_files(export_path)

    if not history_files:
        print("  Error: No streaming history JSON files found.")
        print("  Provide the path to a StreamingHistory JSON file or Spotify export directory.")
        return

    print(f"  History files found: {len(history_files)}")
    for f in history_files:
        print(f"    - {os.path.basename(f)}")

    # Load already-processed IDs
    processed_ids = load_processed_ids(vault_path)
    if processed_ids:
        print(f"  Already processed: {len(processed_ids):,}")

    new_entries = []
    skipped_duplicate = 0
    skipped_invalid = 0
    total_raw = 0
    extended_count = 0
    legacy_count = 0

    for file_path in history_files:
        try:
            data = _load_json(file_path)
        except (ValueError, OSError) as e:
            print(f"  Error reading {os.path.basename(file_path)}: {e}")
            continue

        if not isinstance(data, list):
            logger.warning("%s is not a list, skipping", file_path)
            continue

        total_raw += len(data)
        basename = os.path.basename(file_path)

        for item in data:
            try:
                # Auto-detect format
                if _is_extended_format(item):
                    entry = _parse_extended_entry(item)
                    if entry:
                        extended_count += 1
                else:
                    entry = _parse_legacy_entry(item)
                    if entry:
                        legacy_count += 1
            except Exception as e:
                logger.warning("Skipping entry in %s: %s", basename, e)
                skipped_invalid += 1
                continue

            if entry is None:
                skipped_invalid += 1
                continue
            if entry["id"] in processed_ids:
                skipped_duplicate += 1
                continue

            new_entries.append(entry)

    print(f"  Total raw entries: {total_raw:,}")

    if not new_entries:
        print("  Nothing new — vault is up to date.")
        return

    # Flush to vault
    flush_entries(new_entries, vault_path, "music.jsonl")
    new_ids = [e["id"] for e in new_entries]
    append_processed_ids(vault_path, new_ids)

    # Collect stats
    unique_tracks = set()
    unique_artists = set()
    year_counts = {}
    total_ms = 0

    for e in new_entries:
        unique_tracks.add(f"{e['track']}:{e['artist']}")
        if e.get("artist"):
            unique_artists.add(e["artist"])
        y = e.get("year", 0)
        if y:
            year_counts[y] = year_counts.get(y, 0) + 1
        total_ms += e.get("duration_ms", 0)

    total_hours = total_ms / (1000 * 60 * 60)

    print()
    print(f"  {'=' * 45}")
    print(f"  Done! {len(new_entries):,} listens saved")
    print(f"  {'=' * 45}")
    print(f"    Unique tracks:   {len(unique_tracks):,}")
    print(f"    Unique artists:  {len(unique_artists):,}")
    print(f"    Total listening:  {total_hours:.1f} hours")
    if extended_count:
        print(f"    Extended format: {extended_count:,}")
    if legacy_count:
        print(f"    Legacy format:   {legacy_count:,}")
    if year_counts:
        for year in sorted(year_counts.keys()):
            print(f"    {year}: {year_counts[year]:,}")
    if skipped_duplicate:
        print(f"    Skipped (dupe):  {skipped_duplicate:,}")
    if skipped_invalid:
        print(f"    Skipped (invalid): {skipped_invalid:,}")
    print()

    logger.info(
        "Spotify import complete: %d new, %d duplicate, %d invalid",
        len(new_entries), skipped_duplicate, skipped_invalid,
    )
