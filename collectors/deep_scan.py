"""
Nomolo Deep Computer Scanner

Walks the user's entire home directory and extracts metadata + content
from all discoverable files. The ultimate local-first collector.

Architecture:
  1. Discovery — fast os.walk() with skip rules, stat only
  2. Classification — group by extension/MIME into value tiers
  3. Metadata extraction — Spotlight attrs via mdls (fast, no file opens)
  4. Content extraction — PDFs, images, docs, code (batched)
  5. Dedup — partial hash for duplicates across locations
  6. Flush — write to vault JSONL

Safety:
  - Never reads secrets (.env, credentials, keychains, SSH keys)
  - Skips system dirs, caches, build artifacts, app bundles
  - Files > 100MB logged but not content-extracted
  - Read-only — never modifies any file on disk
"""

from __future__ import annotations

import hashlib
import json
import logging
import mimetypes
import os
import subprocess
import stat
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from core.vault import flush_entries, load_processed_ids, append_processed_ids

logger = logging.getLogger("nomolo.deep_scan")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Directories to always skip (relative to home or absolute)
SKIP_DIRS = {
    # System / framework
    "Library", ".Trash", ".fseventsd", ".Spotlight-V100",
    ".DocumentRevisions-V100", ".TemporaryItems",
    # Development artifacts
    "node_modules", ".git", "__pycache__", ".tox", ".venv", "venv",
    ".cache", ".npm", ".yarn", ".cargo", ".rustup", ".gradle",
    ".m2", "target", "build", "dist", ".next", ".nuxt",
    ".angular", "bower_components", "Pods", "DerivedData",
    ".build", ".swiftpm",
    # App bundles
    "Applications",
    # Container/VM
    ".docker", ".vagrant", ".lima",
    # Large media managed by apps
    "Photos Library.photoslibrary",
    "Music Library.musiclibrary",
    # Nomolo's own vault
    "vaults", ".nomolo",
}

# File extensions that are NEVER secrets but we skip anyway (binaries, etc.)
SKIP_EXTENSIONS = {
    # Executables / binaries
    ".dylib", ".so", ".o", ".a", ".class", ".pyc", ".pyo",
    ".wasm", ".dSYM",
    # Disk images / archives we can't parse
    ".dmg", ".iso", ".vmdk", ".qcow2", ".vdi",
    # Database files (we handle specific ones separately)
    ".sqlite", ".sqlite3", ".db", ".sqlite-wal", ".sqlite-shm",
    # Lock files
    ".lock", ".lck",
}

# Secret / sensitive file patterns — NEVER read content
SECRET_PATTERNS = {
    ".env", ".env.local", ".env.production", ".env.development",
    ".netrc", ".npmrc", ".pypirc",
    "credentials.json", "token.json", "service_account.json",
    "id_rsa", "id_ed25519", "id_ecdsa", "id_dsa",
    ".pem", ".key", ".p12", ".pfx", ".keystore",
    "config.yaml",  # Nomolo's own config
}

# Max file size for content extraction (100 MB)
MAX_CONTENT_SIZE = 100 * 1024 * 1024

# Max text content to extract per file (first N chars)
MAX_TEXT_EXTRACT = 2000

# How many bytes to hash for dedup (first 4KB)
HASH_PREFIX_SIZE = 4096

# ---------------------------------------------------------------------------
# File classification
# ---------------------------------------------------------------------------

# Extension → semantic file_type mapping
EXTENSION_TYPE_MAP = {
    # Documents
    ".pdf": "document", ".doc": "document", ".docx": "document",
    ".odt": "document", ".rtf": "document", ".pages": "document",
    ".tex": "document", ".epub": "document",
    # Spreadsheets
    ".xls": "spreadsheet", ".xlsx": "spreadsheet",
    ".csv": "spreadsheet", ".tsv": "spreadsheet",
    ".numbers": "spreadsheet", ".ods": "spreadsheet",
    # Presentations
    ".ppt": "presentation", ".pptx": "presentation",
    ".key": "presentation", ".odp": "presentation",
    # Text / Markdown
    ".txt": "text", ".md": "text", ".markdown": "text",
    ".rst": "text", ".org": "text", ".adoc": "text",
    # Code
    ".py": "code", ".js": "code", ".ts": "code", ".tsx": "code",
    ".jsx": "code", ".rb": "code", ".go": "code", ".rs": "code",
    ".java": "code", ".kt": "code", ".swift": "code", ".m": "code",
    ".c": "code", ".cpp": "code", ".h": "code", ".hpp": "code",
    ".cs": "code", ".php": "code", ".r": "code", ".R": "code",
    ".scala": "code", ".clj": "code", ".ex": "code", ".erl": "code",
    ".hs": "code", ".lua": "code", ".pl": "code", ".sh": "code",
    ".bash": "code", ".zsh": "code", ".fish": "code",
    ".sql": "code", ".graphql": "code",
    # Config / Data
    ".json": "config", ".yaml": "config", ".yml": "config",
    ".toml": "config", ".ini": "config", ".cfg": "config",
    ".xml": "config", ".plist": "config",
    # Web
    ".html": "web", ".htm": "web", ".css": "web",
    ".scss": "web", ".sass": "web", ".less": "web",
    # Images
    ".jpg": "image", ".jpeg": "image", ".png": "image",
    ".gif": "image", ".bmp": "image", ".tiff": "image",
    ".tif": "image", ".webp": "image", ".svg": "image",
    ".heic": "image", ".heif": "image", ".raw": "image",
    ".cr2": "image", ".nef": "image", ".arw": "image",
    ".ico": "image",
    # Audio
    ".mp3": "audio", ".wav": "audio", ".flac": "audio",
    ".aac": "audio", ".ogg": "audio", ".wma": "audio",
    ".m4a": "audio", ".opus": "audio", ".aiff": "audio",
    # Video
    ".mp4": "video", ".mov": "video", ".avi": "video",
    ".mkv": "video", ".wmv": "video", ".flv": "video",
    ".webm": "video", ".m4v": "video",
    # Archives
    ".zip": "archive", ".tar": "archive", ".gz": "archive",
    ".bz2": "archive", ".xz": "archive", ".7z": "archive",
    ".rar": "archive", ".tgz": "archive",
    # Fonts
    ".ttf": "font", ".otf": "font", ".woff": "font",
    ".woff2": "font",
    # 3D / Design
    ".sketch": "design", ".fig": "design", ".xd": "design",
    ".ai": "design", ".psd": "design", ".blend": "design",
    ".stl": "design", ".obj": "design",
}

# Value tiers for prioritization
TIER_1_TYPES = {"document", "spreadsheet", "presentation", "text"}
TIER_2_TYPES = {"code", "config", "web", "image"}
TIER_3_TYPES = {"audio", "video", "archive", "font", "design"}


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

def discover_files(
    root: str | None = None,
    max_files: int = 500_000,
    progress_fn=None,
) -> list[dict]:
    """
    Walk the file system and collect file metadata.
    Returns list of file info dicts, sorted by value tier then size.
    """
    if root is None:
        root = str(Path.home())

    files = []
    dirs_scanned = 0
    files_found = 0
    files_skipped = 0
    start = time.time()

    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        # Skip excluded directories (modify in-place to prevent descent)
        dirnames[:] = [
            d for d in dirnames
            if d not in SKIP_DIRS
            and not d.startswith(".")  # Skip hidden dirs by default
            and not d.endswith(".app")  # Skip app bundles
            and not d.endswith(".photoslibrary")
            and not d.endswith(".musiclibrary")
        ]

        # Allow specific hidden dirs we care about
        rel = os.path.relpath(dirpath, root)
        if rel == ".":
            # At home dir, re-add useful hidden dirs
            for d in [".ssh", ".config"]:
                if d not in dirnames and os.path.isdir(os.path.join(dirpath, d)):
                    dirnames.append(d)

        dirs_scanned += 1

        for filename in filenames:
            if files_found >= max_files:
                break

            # Skip hidden files (except specific ones)
            if filename.startswith(".") and filename not in {".zshrc", ".bashrc", ".gitconfig", ".zsh_history", ".bash_history"}:
                files_skipped += 1
                continue

            ext = os.path.splitext(filename)[1].lower()

            # Skip binary / uninteresting extensions
            if ext in SKIP_EXTENSIONS:
                files_skipped += 1
                continue

            filepath = os.path.join(dirpath, filename)

            # Skip symlinks
            if os.path.islink(filepath):
                files_skipped += 1
                continue

            try:
                st = os.stat(filepath)
            except (OSError, PermissionError):
                files_skipped += 1
                continue

            # Skip non-regular files
            if not stat.S_ISREG(st.st_mode):
                files_skipped += 1
                continue

            file_type = EXTENSION_TYPE_MAP.get(ext, "other")
            mime_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"

            # Determine tier
            if file_type in TIER_1_TYPES:
                tier = 1
            elif file_type in TIER_2_TYPES:
                tier = 2
            elif file_type in TIER_3_TYPES:
                tier = 3
            else:
                tier = 4

            # Determine relative path from home
            try:
                rel_path = os.path.relpath(filepath, root)
            except ValueError:
                rel_path = filepath

            # Semantic location from path
            location = _classify_location(rel_path)

            files.append({
                "path": filepath,
                "rel_path": rel_path,
                "filename": filename,
                "extension": ext,
                "file_type": file_type,
                "mime_type": mime_type,
                "tier": tier,
                "size_bytes": st.st_size,
                "created": _ts_to_iso(st.st_birthtime) if hasattr(st, "st_birthtime") else _ts_to_iso(st.st_ctime),
                "modified": _ts_to_iso(st.st_mtime),
                "accessed": _ts_to_iso(st.st_atime),
                "location": location,
            })
            files_found += 1

        if files_found >= max_files:
            break

        if progress_fn and dirs_scanned % 500 == 0:
            progress_fn(dirs_scanned, files_found, files_skipped)

    elapsed = time.time() - start
    logger.info(
        "Discovery: %d files found, %d skipped, %d dirs scanned in %.1fs",
        files_found, files_skipped, dirs_scanned, elapsed,
    )

    # Sort: tier 1 first, then by size descending within tier
    files.sort(key=lambda f: (f["tier"], -f["size_bytes"]))
    return files


# ---------------------------------------------------------------------------
# Metadata extraction
# ---------------------------------------------------------------------------

def extract_metadata_batch(files: list[dict], batch_size: int = 200) -> list[dict]:
    """
    Enrich file dicts with Spotlight metadata via mdls.
    Batches calls for efficiency.
    """
    enriched = []
    for i in range(0, len(files), batch_size):
        batch = files[i:i + batch_size]
        paths = [f["path"] for f in batch]

        # Get Spotlight metadata for batch
        spotlight_data = _mdls_batch(paths)

        for file_info, md in zip(batch, spotlight_data):
            file_info["spotlight"] = md
            enriched.append(file_info)

    return enriched


def _mdls_batch(paths: list[str]) -> list[dict]:
    """Run mdls on a batch of files and parse results."""
    results = []
    for path in paths:
        try:
            output = subprocess.run(
                ["mdls", "-plist", "-", path],
                capture_output=True, text=True, timeout=5,
            )
            if output.returncode == 0:
                md = _parse_mdls_plist(output.stdout)
            else:
                md = {}
        except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
            md = {}
        results.append(md)
    return results


def _parse_mdls_plist(plist_str: str) -> dict:
    """Parse mdls plist output into a flat dict of interesting fields."""
    import plistlib
    try:
        data = plistlib.loads(plist_str.encode("utf-8"))
    except Exception:
        return {}

    # Extract fields we care about
    result = {}
    field_map = {
        "kMDItemTitle": "md_title",
        "kMDItemAuthors": "md_authors",
        "kMDItemCreator": "md_creator",
        "kMDItemDescription": "md_description",
        "kMDItemComment": "md_comment",
        "kMDItemKeywords": "md_keywords",
        "kMDItemNumberOfPages": "md_pages",
        "kMDItemPixelHeight": "md_height",
        "kMDItemPixelWidth": "md_width",
        "kMDItemDurationSeconds": "md_duration",
        "kMDItemCodecs": "md_codecs",
        "kMDItemAudioBitRate": "md_audio_bitrate",
        "kMDItemMusicalGenre": "md_genre",
        "kMDItemAlbum": "md_album",
        "kMDItemComposer": "md_composer",
        "kMDItemLatitude": "md_latitude",
        "kMDItemLongitude": "md_longitude",
        "kMDItemAltitude": "md_altitude",
        "kMDItemWhereFroms": "md_download_urls",
        "kMDItemFinderComment": "md_finder_comment",
        "kMDItemUserTags": "md_tags",
        "kMDItemContentType": "md_content_type",
        "kMDItemEncodingApplications": "md_encoding_app",
    }

    for plist_key, our_key in field_map.items():
        val = data.get(plist_key)
        if val is not None and val != "" and val != []:
            # Convert dates to ISO strings
            if isinstance(val, datetime):
                val = val.isoformat()
            elif isinstance(val, list):
                val = [v.isoformat() if isinstance(v, datetime) else v for v in val]
            result[our_key] = val

    return result


# ---------------------------------------------------------------------------
# Content extraction (tier-specific)
# ---------------------------------------------------------------------------

def extract_content(file_info: dict) -> dict | None:
    """Extract text content from a file based on its type. Returns enriched dict."""
    path = file_info["path"]
    size = file_info["size_bytes"]
    file_type = file_info["file_type"]

    # Never extract secrets
    filename = file_info["filename"]
    if _is_secret(filename):
        file_info["content_skipped"] = "secret_file"
        return file_info

    # Skip very large files
    if size > MAX_CONTENT_SIZE:
        file_info["content_skipped"] = "too_large"
        return file_info

    try:
        if file_type == "text" or file_type == "code" or file_type == "config" or file_type == "web":
            content = _read_text_file(path)
            if content:
                file_info["content_preview"] = content[:MAX_TEXT_EXTRACT]
                file_info["line_count"] = content.count("\n") + 1
                file_info["char_count"] = len(content)
                if file_type == "code":
                    file_info["language"] = _detect_language(file_info["extension"])
                    file_info["imports"] = _extract_imports(content, file_info["extension"])

        elif file_type == "document":
            if file_info["extension"] == ".pdf":
                text = _extract_pdf_text(path)
                if text:
                    file_info["content_preview"] = text[:MAX_TEXT_EXTRACT]

        elif file_type == "image":
            exif = _extract_exif(path)
            if exif:
                file_info["exif"] = exif

    except Exception as e:
        logger.debug("Content extraction failed for %s: %s", path, e)
        file_info["content_error"] = str(e)

    return file_info


def _read_text_file(path: str) -> str | None:
    """Read a text file, handling encoding gracefully."""
    for encoding in ("utf-8", "latin-1"):
        try:
            with open(path, "r", encoding=encoding, errors="replace") as f:
                return f.read(MAX_TEXT_EXTRACT + 100)
        except (OSError, PermissionError):
            return None
    return None


def _detect_language(ext: str) -> str:
    """Map extension to programming language name."""
    lang_map = {
        ".py": "Python", ".js": "JavaScript", ".ts": "TypeScript",
        ".tsx": "TypeScript", ".jsx": "JavaScript", ".rb": "Ruby",
        ".go": "Go", ".rs": "Rust", ".java": "Java", ".kt": "Kotlin",
        ".swift": "Swift", ".c": "C", ".cpp": "C++", ".h": "C/C++",
        ".cs": "C#", ".php": "PHP", ".r": "R", ".R": "R",
        ".scala": "Scala", ".sh": "Shell", ".bash": "Shell",
        ".zsh": "Shell", ".sql": "SQL", ".lua": "Lua",
    }
    return lang_map.get(ext, "Unknown")


def _extract_imports(content: str, ext: str) -> list[str]:
    """Extract import/require statements from code."""
    imports = []
    lines = content.split("\n")[:100]  # First 100 lines only

    for line in lines:
        line = line.strip()
        if ext in (".py",):
            if line.startswith("import ") or line.startswith("from "):
                imports.append(line)
        elif ext in (".js", ".ts", ".tsx", ".jsx"):
            if line.startswith("import ") or ("require(" in line):
                imports.append(line)
        elif ext in (".go",):
            if line.startswith("import "):
                imports.append(line)
        elif ext in (".java", ".kt", ".scala"):
            if line.startswith("import "):
                imports.append(line)
        elif ext in (".rs",):
            if line.startswith("use "):
                imports.append(line)

    return imports[:20]  # Cap at 20


def _extract_pdf_text(path: str) -> str | None:
    """Extract text from PDF using macOS built-in tools."""
    try:
        result = subprocess.run(
            ["mdimport", "-d2", path],
            capture_output=True, text=True, timeout=10,
        )
        # mdimport doesn't output text directly, fall back to textutil
        result = subprocess.run(
            ["textutil", "-convert", "txt", "-stdout", path],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0 and result.stdout:
            return result.stdout
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass
    return None


def _extract_exif(path: str) -> dict | None:
    """Extract EXIF data from image using macOS sips."""
    try:
        result = subprocess.run(
            ["sips", "-g", "all", path],
            capture_output=True, text=True, timeout=5,
        )
        if result.returncode != 0:
            return None

        exif = {}
        for line in result.stdout.split("\n"):
            line = line.strip()
            if ":" in line:
                key, _, val = line.partition(":")
                key = key.strip()
                val = val.strip()
                if key in ("pixelWidth", "pixelHeight", "dpiWidth", "dpiHeight",
                           "hasAlpha", "space", "profile", "orientation"):
                    exif[key] = val
        return exif if exif else None
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return None


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

def compute_partial_hash(path: str) -> str | None:
    """Hash the first 4KB of a file for fast dedup."""
    try:
        with open(path, "rb") as f:
            data = f.read(HASH_PREFIX_SIZE)
        return hashlib.sha256(data).hexdigest()[:16]
    except (OSError, PermissionError):
        return None


# ---------------------------------------------------------------------------
# Vault entry builder
# ---------------------------------------------------------------------------

def build_vault_entry(file_info: dict) -> dict:
    """Convert a file_info dict into a vault JSONL entry."""
    entry_id = _make_id(file_info["rel_path"])

    entry = {
        "id": entry_id,
        "sources": ["deep_scan"],
        "type": "file",
        "filename": file_info["filename"],
        "path": file_info["rel_path"],
        "extension": file_info["extension"],
        "file_type": file_info["file_type"],
        "mime_type": file_info["mime_type"],
        "size_bytes": file_info["size_bytes"],
        "created": file_info["created"],
        "modified": file_info["modified"],
        "location": file_info["location"],
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

    # Spotlight metadata
    spotlight = file_info.get("spotlight", {})
    if spotlight:
        for key, val in spotlight.items():
            entry[key] = val

    # Content extraction results
    if "content_preview" in file_info:
        entry["content_preview"] = file_info["content_preview"]
    if "line_count" in file_info:
        entry["line_count"] = file_info["line_count"]
    if "char_count" in file_info:
        entry["char_count"] = file_info["char_count"]
    if "language" in file_info:
        entry["language"] = file_info["language"]
    if "imports" in file_info:
        entry["imports"] = file_info["imports"]
    if "exif" in file_info:
        entry["exif"] = file_info["exif"]

    # Dedup hash
    if "partial_hash" in file_info:
        entry["partial_hash"] = file_info["partial_hash"]

    # Build embedding text
    parts = [file_info["filename"]]
    if spotlight.get("md_title"):
        parts.append(str(spotlight["md_title"]))
    if spotlight.get("md_description"):
        parts.append(str(spotlight["md_description"]))
    if spotlight.get("md_comment"):
        parts.append(str(spotlight["md_comment"]))
    if spotlight.get("md_tags"):
        parts.append(" ".join(spotlight["md_tags"]))
    if "content_preview" in file_info:
        parts.append(file_info["content_preview"][:500])
    entry["file_for_embedding"] = " | ".join(parts)

    return entry


# ---------------------------------------------------------------------------
# Main scan orchestrator
# ---------------------------------------------------------------------------

def deep_scan(
    vault_root: str,
    scan_root: str | None = None,
    max_files: int = 500_000,
    extract_content_flag: bool = True,
    extract_metadata_flag: bool = True,
    progress_fn=None,
) -> dict:
    """
    Run the full deep scan pipeline.

    Args:
        vault_root: Path to Nomolo vault root
        scan_root: Directory to scan (default: user home)
        max_files: Maximum files to discover
        extract_content_flag: Whether to extract file content
        extract_metadata_flag: Whether to extract Spotlight metadata
        progress_fn: Callback(phase, current, total, message)

    Returns:
        Stats dict with counts.
    """
    vault_path = os.path.join(vault_root, "DeepScan")
    processed = load_processed_ids(vault_path)

    stats = {
        "discovered": 0,
        "new": 0,
        "skipped_processed": 0,
        "skipped_secret": 0,
        "content_extracted": 0,
        "metadata_enriched": 0,
        "entries_written": 0,
        "elapsed_seconds": 0,
    }

    start = time.time()

    # Phase 1: Discovery
    if progress_fn:
        progress_fn("discovery", 0, 0, "Scanning file system...")

    files = discover_files(
        root=scan_root,
        max_files=max_files,
        progress_fn=lambda d, f, s: progress_fn("discovery", f, max_files, f"Scanned {d} dirs, found {f} files") if progress_fn else None,
    )
    stats["discovered"] = len(files)

    if progress_fn:
        progress_fn("discovery", len(files), len(files), f"Found {len(files)} files")

    # Filter out already-processed, secret, and overlapping files
    new_files = []
    stats["skipped_overlap"] = 0
    for f in files:
        entry_id = _make_id(f["rel_path"])
        if entry_id in processed:
            stats["skipped_processed"] += 1
            continue
        if _is_secret(f["filename"]):
            stats["skipped_secret"] += 1
            continue
        if _is_overlap(f["rel_path"], f["extension"]):
            stats["skipped_overlap"] += 1
            continue
        new_files.append(f)

    stats["new"] = len(new_files)

    if not new_files:
        stats["elapsed_seconds"] = time.time() - start
        return stats

    # Phase 2: Metadata extraction (Spotlight)
    if extract_metadata_flag:
        if progress_fn:
            progress_fn("metadata", 0, len(new_files), "Extracting Spotlight metadata...")

        new_files = extract_metadata_batch(new_files)
        stats["metadata_enriched"] = len(new_files)

        if progress_fn:
            progress_fn("metadata", len(new_files), len(new_files), "Metadata extracted")

    # Phase 3: Content extraction (text, code, PDFs, images)
    if extract_content_flag:
        if progress_fn:
            progress_fn("content", 0, len(new_files), "Extracting content...")

        for i, file_info in enumerate(new_files):
            if file_info["tier"] <= 2:  # Only tiers 1-2 get content extraction
                extract_content(file_info)
                stats["content_extracted"] += 1

            # Compute partial hash for dedup
            ph = compute_partial_hash(file_info["path"])
            if ph:
                file_info["partial_hash"] = ph

            if progress_fn and (i + 1) % 100 == 0:
                progress_fn("content", i + 1, len(new_files), f"Extracted {i + 1}/{len(new_files)}")

    # Phase 4: Build entries and flush
    if progress_fn:
        progress_fn("flush", 0, len(new_files), "Writing to vault...")

    entries = []
    new_ids = []
    batch_size = 1000

    for i, file_info in enumerate(new_files):
        entry = build_vault_entry(file_info)
        entries.append(entry)
        new_ids.append(entry["id"])

        # Flush in batches
        if len(entries) >= batch_size:
            flush_entries(entries, vault_path, "files.jsonl")
            append_processed_ids(vault_path, new_ids)
            stats["entries_written"] += len(entries)
            entries = []
            new_ids = []

            if progress_fn:
                progress_fn("flush", stats["entries_written"], len(new_files),
                           f"Written {stats['entries_written']}/{len(new_files)}")

    # Flush remaining
    if entries:
        flush_entries(entries, vault_path, "files.jsonl")
        append_processed_ids(vault_path, new_ids)
        stats["entries_written"] += len(entries)

    stats["elapsed_seconds"] = round(time.time() - start, 1)

    if progress_fn:
        progress_fn("done", stats["entries_written"], stats["entries_written"],
                    f"Done! {stats['entries_written']} files archived in {stats['elapsed_seconds']}s")

    return stats


# ---------------------------------------------------------------------------
# Overlap detection — skip files already captured by other collectors
# ---------------------------------------------------------------------------

# Paths managed by other collectors — deep scan should not duplicate these
OVERLAP_PATH_PATTERNS = [
    # Mail.app stores downloaded emails — already covered by Gmail/Mail collector
    "Library/Mail",
    # Photos managed by Photos.app — already covered by Photos collector
    "Pictures/Photos Library.photoslibrary",
    # Contacts managed by Contacts.app — already covered by Contacts collector
    "Library/Application Support/AddressBook",
    # Calendar managed by Calendar.app — already covered by Calendar collector
    "Library/Calendars",
    # Notes managed by Notes.app — already covered by Notes collector
    "Library/Group Containers/group.com.apple.notes",
    # Messages managed by Messages.app — already covered by iMessage collector
    "Library/Messages",
    # Safari managed by Safari — already covered by Safari/Browser collector
    "Library/Safari",
    # Chrome managed by Chrome — already covered by Chrome collector
    "Library/Application Support/Google/Chrome",
    # Nomolo's own vault files
    "vaults/",
    "Nomolo",
]

# File extensions that are just container/DB formats for other collectors
OVERLAP_EXTENSIONS = {
    ".emlx",  # Mail.app email files — Gmail collector handles these
    ".mbox",  # Mailbox format — Gmail collector handles
    ".ics",   # Calendar files — Calendar collector handles
    ".vcf",   # Contact cards — Contacts collector handles
    ".abcddb",  # Address Book DB
}


def _is_overlap(rel_path: str, extension: str) -> bool:
    """Check if a file path overlaps with data already collected by other collectors."""
    # Check path-based overlaps
    for pattern in OVERLAP_PATH_PATTERNS:
        if rel_path.startswith(pattern) or f"/{pattern}" in rel_path:
            return True

    # Check extension-based overlaps
    if extension.lower() in OVERLAP_EXTENSIONS:
        return True

    return False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_id(rel_path: str) -> str:
    """Deterministic ID from relative path."""
    h = hashlib.md5(rel_path.encode("utf-8")).hexdigest()[:12]
    return f"local:deep_scan:{h}"


def _ts_to_iso(ts: float) -> str:
    """Convert Unix timestamp to ISO 8601 string."""
    try:
        return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
    except (OSError, ValueError, OverflowError):
        return ""


def _classify_location(rel_path: str) -> str:
    """Classify file location semantically from its path."""
    parts = rel_path.lower().split(os.sep)

    if "desktop" in parts:
        return "desktop"
    elif "documents" in parts:
        if any(w in parts for w in ["work", "projects", "business"]):
            return "work"
        elif any(w in parts for w in ["personal", "private"]):
            return "personal"
        return "documents"
    elif "downloads" in parts:
        return "downloads"
    elif "pictures" in parts or "photos" in parts:
        return "photos"
    elif "movies" in parts or "videos" in parts:
        return "videos"
    elif "music" in parts:
        return "music"
    elif any(w in parts for w in ["code", "projects", "repos", "src", "dev", "github"]):
        return "code"
    elif ".config" in parts or ".ssh" in parts:
        return "system_config"
    else:
        return "other"


def _is_secret(filename: str) -> bool:
    """Check if a filename matches secret/sensitive patterns."""
    lower = filename.lower()
    for pattern in SECRET_PATTERNS:
        if lower == pattern.lower() or lower.endswith(pattern.lower()):
            return True
    return False


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------

def main():
    """Run deep scan from command line."""
    import argparse

    parser = argparse.ArgumentParser(description="Nomolo Deep Computer Scanner")
    parser.add_argument("--vault-root", default="vaults", help="Vault root directory")
    parser.add_argument("--scan-root", default=None, help="Directory to scan (default: home)")
    parser.add_argument("--max-files", type=int, default=500_000, help="Max files to discover")
    parser.add_argument("--no-content", action="store_true", help="Skip content extraction")
    parser.add_argument("--no-metadata", action="store_true", help="Skip Spotlight metadata")
    parser.add_argument("--quiet", action="store_true", help="Suppress progress output")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.INFO,
        format="%(message)s",
    )

    def progress(phase, current, total, message):
        if not args.quiet:
            print(f"\r  [{phase}] {message}", end="", flush=True)
            if current == total and total > 0:
                print()

    print("Deep Computer Scanner")
    print("=" * 50)

    stats = deep_scan(
        vault_root=args.vault_root,
        scan_root=args.scan_root,
        max_files=args.max_files,
        extract_content_flag=not args.no_content,
        extract_metadata_flag=not args.no_metadata,
        progress_fn=progress,
    )

    print(f"\nResults:")
    print(f"  Files discovered: {stats['discovered']:,}")
    print(f"  New files:        {stats['new']:,}")
    print(f"  Already scanned:  {stats['skipped_processed']:,}")
    print(f"  Overlap skipped:  {stats.get('skipped_overlap', 0):,}")
    print(f"  Secrets skipped:  {stats['skipped_secret']:,}")
    print(f"  Content extracted: {stats['content_extracted']:,}")
    print(f"  Entries written:  {stats['entries_written']:,}")
    print(f"  Time:             {stats['elapsed_seconds']}s")
    rate = stats["entries_written"] / max(stats["elapsed_seconds"], 0.1)
    print(f"  Speed:            {rate:,.0f} files/sec")


if __name__ == "__main__":
    main()
