"""
NOMOLO Notes Collector
Scans a directory of markdown/text files, audio recordings, and video
recordings and imports them as notes into the unified Notes JSONL vault.

Text notes (.md, .txt) get their full body imported.
Audio/video recordings get metadata entries (filename, duration, timestamps)
with the actual media files staying in place — only linked from the vault.

Usage:
  nomolo collect notes ~/Documents/notes/
  nomolo collect notes ~/Documents/journal/
"""

import hashlib
import logging
import os
import re
import yaml
from datetime import datetime

from core.vault import flush_entries, load_processed_ids, append_processed_ids

logger = logging.getLogger("nomolo.notes")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

NOTE_EXTENSIONS = {".md", ".txt", ".markdown"}

AUDIO_EXTENSIONS = {".mp3", ".m4a", ".wav", ".ogg", ".flac", ".aac", ".wma", ".opus"}

VIDEO_EXTENSIONS = {".mp4", ".mov", ".avi", ".mkv", ".webm", ".m4v", ".wmv", ".flv"}

MEDIA_EXTENSIONS = AUDIO_EXTENSIONS | VIDEO_EXTENSIONS


def _make_id(file_path):
    """Generate a deterministic 12-char hex ID from the file path."""
    return "notes:" + hashlib.sha256(file_path.encode("utf-8")).hexdigest()[:12]


def _extract_title(content, filename):
    """
    Extract title from markdown content.
    Looks for the first heading (# Title) or falls back to filename.
    """
    if content:
        # Match first ATX heading (# Title)
        match = re.search(r"^#\s+(.+)$", content, re.MULTILINE)
        if match:
            return match.group(1).strip()

    # Fallback: use filename without extension
    name = os.path.splitext(filename)[0]
    # Replace common separators with spaces
    name = name.replace("-", " ").replace("_", " ")
    return name


def _extract_frontmatter_tags(content):
    """
    Extract tags from YAML frontmatter if present.
    Expects format:
      ---
      tags: [a, b, c]
      ---
    or:
      ---
      tags:
        - a
        - b
      ---

    Returns a list of tag strings, or an empty list.
    """
    if not content or not content.startswith("---"):
        return []

    # Find the closing ---
    end_match = re.search(r"\n---\s*\n", content[3:])
    if not end_match:
        return []

    frontmatter_str = content[3 : 3 + end_match.start()]

    try:
        frontmatter = yaml.safe_load(frontmatter_str)
    except Exception:
        logger.debug("Could not parse YAML frontmatter")
        return []

    if not isinstance(frontmatter, dict):
        return []

    tags = frontmatter.get("tags", [])
    if isinstance(tags, list):
        return [str(t).strip() for t in tags if t]
    if isinstance(tags, str):
        # Handle comma-separated string: "a, b, c"
        return [t.strip() for t in tags.split(",") if t.strip()]
    return []


def _strip_frontmatter(content):
    """Remove YAML frontmatter from content if present."""
    if not content or not content.startswith("---"):
        return content

    end_match = re.search(r"\n---\s*\n", content[3:])
    if not end_match:
        return content

    return content[3 + end_match.end() :].lstrip()


def _read_file(file_path):
    """Read a text file with encoding fallback."""
    encodings = ["utf-8-sig", "utf-8", "latin-1"]

    for encoding in encodings:
        try:
            with open(file_path, "r", encoding=encoding) as f:
                return f.read()
        except UnicodeDecodeError:
            continue
        except OSError as e:
            logger.warning("Cannot read file %s: %s", file_path, e)
            return None

    logger.warning("Could not decode %s with any supported encoding", file_path)
    return None


def _parse_note_file(file_path, base_dir):
    """
    Parse a single note file into a vault entry dict.
    Returns None if the file cannot be read or is empty.

    Args:
        file_path: Absolute path to the note file.
        base_dir: Base directory for computing relative paths.
    """
    content = _read_file(file_path)
    if content is None:
        return None

    content = content.strip()
    if not content:
        return None

    filename = os.path.basename(file_path)
    rel_path = os.path.relpath(file_path, base_dir)

    # Extract metadata
    tags = _extract_frontmatter_tags(content)
    body = _strip_frontmatter(content)
    title = _extract_title(body, filename)
    word_count = len(body.split())

    # File timestamps
    stat = os.stat(file_path)
    modified_at = datetime.fromtimestamp(stat.st_mtime).isoformat()

    # Try to get creation time (macOS has st_birthtime, Linux falls back to mtime)
    try:
        created_ts = stat.st_birthtime
    except AttributeError:
        created_ts = stat.st_mtime
    created_at = datetime.fromtimestamp(created_ts).isoformat()

    # Parse year/month from created_at
    try:
        dt = datetime.fromisoformat(created_at)
        year = dt.year
        month = dt.month
    except (ValueError, TypeError):
        year, month = 0, 0

    entry_id = _make_id(rel_path)

    # Build embedding text — title + body (truncated for very large files)
    embedding_body = body[:2000] if len(body) > 2000 else body
    embedding = f"{title} — {embedding_body}"

    return {
        "id": entry_id,
        "sources": ["local"],
        "title": title,
        "body": body,
        "tags": tags,
        "file_path": rel_path,
        "created_at": created_at,
        "modified_at": modified_at,
        "word_count": word_count,
        "year": year,
        "month": month,
        "updated_at": datetime.now().isoformat(),
        "note_for_embedding": embedding,
    }


# ---------------------------------------------------------------------------
# Media file parser (audio/video recordings)
# ---------------------------------------------------------------------------

def _parse_media_file(file_path, base_dir):
    """
    Parse an audio or video recording file into a vault entry dict.
    Only captures metadata — the actual file stays in place and is linked.
    Returns None if the file cannot be stat'd.
    """
    try:
        stat = os.stat(file_path)
    except OSError as e:
        logger.warning("Cannot stat %s: %s", file_path, e)
        return None

    filename = os.path.basename(file_path)
    rel_path = os.path.relpath(file_path, base_dir)
    ext = os.path.splitext(filename)[1].lower()

    if ext in AUDIO_EXTENSIONS:
        media_type = "audio-recording"
    elif ext in VIDEO_EXTENSIONS:
        media_type = "video-recording"
    else:
        return None

    # Title from filename
    title = os.path.splitext(filename)[0]
    title = title.replace("-", " ").replace("_", " ")

    # File timestamps
    modified_at = datetime.fromtimestamp(stat.st_mtime).isoformat()
    try:
        created_ts = stat.st_birthtime
    except AttributeError:
        created_ts = stat.st_mtime
    created_at = datetime.fromtimestamp(created_ts).isoformat()

    try:
        dt = datetime.fromtimestamp(created_ts)
        year = dt.year
        month = dt.month
    except (ValueError, TypeError):
        year, month = 0, 0

    size_mb = round(stat.st_size / (1024 * 1024), 1)
    entry_id = _make_id(rel_path)

    # Build embedding text
    type_label = "Audio recording" if media_type == "audio-recording" else "Video recording"
    date_str = datetime.fromtimestamp(created_ts).strftime("%Y-%m-%d")
    embedding = f"{type_label}: {title} — {date_str} — {size_mb} MB"

    return {
        "id": entry_id,
        "sources": ["local"],
        "title": title,
        "body": "",
        "tags": [media_type],
        "media_type": media_type,
        "media_path": os.path.abspath(file_path),
        "file_path": rel_path,
        "file_size_mb": size_mb,
        "format": ext.lstrip("."),
        "created_at": created_at,
        "modified_at": modified_at,
        "word_count": 0,
        "year": year,
        "month": month,
        "updated_at": datetime.now().isoformat(),
        "note_for_embedding": embedding,
    }


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_import(export_path, config=None):
    """
    Import notes from a directory of markdown/text files into the vault.

    Recursively scans for .md, .txt, and .markdown files.

    Args:
        export_path: Path to the directory containing note files.
        config: Dict with optional 'vault_root' key.
    """
    config = config or {}
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    vault_root_base = config.get("vault_root", os.path.join(project_root, "vaults"))
    vault_root_base = os.path.expanduser(vault_root_base)
    if not os.path.isabs(vault_root_base):
        vault_root_base = os.path.join(project_root, vault_root_base)
    vault_path = os.path.join(vault_root_base, "Notes")

    export_path = os.path.expanduser(export_path)

    print(f"\n  NOMOLO Notes Collector")
    print(f"  {'=' * 45}")
    print(f"  Directory: {export_path}")
    print(f"  Vault: {vault_path}")

    if not os.path.isdir(export_path):
        print(f"  Error: Directory not found: {export_path}")
        return

    # Recursively find note files and media recordings
    note_files = []
    media_files = []
    for dirpath, _dirnames, filenames in os.walk(export_path):
        for fname in filenames:
            ext = os.path.splitext(fname)[1].lower()
            if ext in NOTE_EXTENSIONS:
                note_files.append(os.path.join(dirpath, fname))
            elif ext in MEDIA_EXTENSIONS:
                media_files.append(os.path.join(dirpath, fname))

    note_files.sort()
    media_files.sort()

    if not note_files and not media_files:
        print("  No note files or media recordings found.")
        return

    print(f"  Text notes found: {len(note_files)}")
    if media_files:
        audio_count = sum(1 for f in media_files if os.path.splitext(f)[1].lower() in AUDIO_EXTENSIONS)
        video_count = len(media_files) - audio_count
        print(f"  Audio recordings: {audio_count}")
        print(f"  Video recordings: {video_count}")

    # Load already-processed IDs
    processed_ids = load_processed_ids(vault_path)
    if processed_ids:
        print(f"  Already processed: {len(processed_ids):,}")

    # Parse files into entries
    new_entries = []
    skipped_empty = 0
    skipped_duplicate = 0

    for file_path in note_files:
        try:
            entry = _parse_note_file(file_path, export_path)
        except Exception as e:
            logger.warning("Skipping %s: %s", file_path, e)
            skipped_empty += 1
            continue

        if entry is None:
            skipped_empty += 1
            continue
        if entry["id"] in processed_ids:
            skipped_duplicate += 1
            continue

        new_entries.append(entry)

    # Parse media files into entries
    skipped_media_error = 0
    for file_path in media_files:
        try:
            entry = _parse_media_file(file_path, export_path)
        except Exception as e:
            logger.warning("Skipping %s: %s", file_path, e)
            skipped_media_error += 1
            continue

        if entry is None:
            skipped_media_error += 1
            continue
        if entry["id"] in processed_ids:
            skipped_duplicate += 1
            continue

        new_entries.append(entry)

    if not new_entries:
        print("  Nothing new — vault is up to date.")
        return

    # Flush to vault
    flush_entries(new_entries, vault_path, "notes.jsonl")
    new_ids = [e["id"] for e in new_entries]
    append_processed_ids(vault_path, new_ids)

    # Summary stats
    total_words = sum(e["word_count"] for e in new_entries)
    with_tags = sum(1 for e in new_entries if e.get("tags"))
    all_tags = set()
    for e in new_entries:
        all_tags.update(e.get("tags", []))
    year_counts = {}
    for e in new_entries:
        y = e.get("year", 0)
        if y:
            year_counts[y] = year_counts.get(y, 0) + 1

    # Count media entries
    audio_saved = sum(1 for e in new_entries if e.get("media_type") == "audio-recording")
    video_saved = sum(1 for e in new_entries if e.get("media_type") == "video-recording")
    text_saved = len(new_entries) - audio_saved - video_saved

    print()
    print(f"  {'=' * 45}")
    print(f"  Done! {len(new_entries):,} entries saved")
    print(f"  {'=' * 45}")
    if text_saved:
        print(f"    Text notes:      {text_saved:,}")
    if audio_saved:
        print(f"    Audio recordings:{audio_saved:,}")
    if video_saved:
        print(f"    Video recordings:{video_saved:,}")
    print(f"    Total words:     {total_words:,}")
    print(f"    With tags:       {with_tags:,}")
    if all_tags:
        print(f"    Unique tags:     {len(all_tags)}")
    if year_counts:
        for year in sorted(year_counts.keys()):
            print(f"    {year}: {year_counts[year]:,}")
    if skipped_duplicate:
        print(f"    Skipped (dupe):  {skipped_duplicate:,}")
    if skipped_empty:
        print(f"    Skipped (empty): {skipped_empty:,}")
    if skipped_media_error:
        print(f"    Skipped (media): {skipped_media_error:,}")
    print()

    logger.info(
        "Notes import complete: %d new, %d duplicate, %d empty",
        len(new_entries), skipped_duplicate, skipped_empty,
    )
