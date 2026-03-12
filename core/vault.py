"""
WHID Vault I/O Utilities
Shared read/write helpers for vault JSONL files.
Thread-safe writes, entry counting, integrity checks.
Supports both plain .jsonl and compressed .jsonl.zst files transparently.
"""

import os
import json
import tempfile
import threading
import logging
from collections import defaultdict

logger = logging.getLogger("whid.vault")

try:
    import zstandard as zstd
    HAS_ZSTD = True
except ImportError:
    HAS_ZSTD = False

# Global write lock for all vault file operations
_write_lock = threading.Lock()

# Month names for file naming (01_January.jsonl, etc.)
_MONTH_NAMES = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]


def _open_jsonl(file_path):
    """Open a .jsonl or .jsonl.zst file for reading. Returns a context manager yielding text lines."""
    if file_path.endswith(".zst"):
        if not HAS_ZSTD:
            logger.warning("Cannot read %s — install zstandard: pip install zstandard", file_path)
            return None
        import io
        dctx = zstd.ZstdDecompressor()
        fh = open(file_path, "rb")
        reader = dctx.stream_reader(fh)
        return io.TextIOWrapper(reader, encoding="utf-8")
    return open(file_path, "r", encoding="utf-8")


def _find_jsonl_files(vault_path):
    """Find all .jsonl and .jsonl.zst files under vault_path."""
    for root, _dirs, files in os.walk(vault_path):
        for f in sorted(files):
            if f.endswith(".jsonl") or f.endswith(".jsonl.zst"):
                yield os.path.join(root, f)


def flush_entries(entries, vault_path, file_name):
    """Append JSONL entries to a specific file in vault_path. Thread-safe."""
    if not entries:
        return

    os.makedirs(vault_path, exist_ok=True)
    file_path = os.path.join(vault_path, file_name)

    with _write_lock:
        try:
            with open(file_path, "a", encoding="utf-8") as f:
                for entry in entries:
                    f.write(json.dumps(entry) + "\n")
        except PermissionError:
            logger.error(
                "Permission denied writing to %s — check folder permissions.",
                file_path,
            )
            raise
        except OSError as e:
            if "No space left" in str(e) or e.errno == 28:
                logger.error(
                    "Disk full — cannot write to %s. Free up space and re-run.",
                    file_path,
                )
            else:
                logger.error("Failed to write %s: %s", file_path, e)
            raise


def flush_entries_by_date(entries, vault_path, parse_date_fn):
    """
    Group entries by year/month and write to JSONL files.
    Uses parse_date_fn(date_str) to parse the "date" field.
    Entries with unparseable dates go to _unknown/unknown_date.jsonl.
    Thread-safe.
    """
    if not entries:
        return

    file_groups = defaultdict(list)

    for entry in entries:
        dt = parse_date_fn(entry.get("date", ""))

        if dt:
            year_dir = os.path.join(vault_path, dt.strftime("%Y"))
            month_num = dt.month
            filename = f"{month_num:02d}_{_MONTH_NAMES[month_num - 1]}.jsonl"
        else:
            year_dir = os.path.join(vault_path, "_unknown")
            filename = "unknown_date.jsonl"

        file_path = os.path.join(year_dir, filename)
        file_groups[file_path].append((year_dir, entry))

    with _write_lock:
        for file_path, items in file_groups.items():
            try:
                os.makedirs(items[0][0], exist_ok=True)
                with open(file_path, "a", encoding="utf-8") as f:
                    for _, entry in items:
                        f.write(json.dumps(entry) + "\n")
            except PermissionError:
                logger.error(
                    "Permission denied writing to %s — check folder permissions.",
                    file_path,
                )
                raise
            except OSError as e:
                if "No space left" in str(e) or e.errno == 28:
                    logger.error(
                        "Disk full — cannot write to %s. Free up space and re-run.",
                        file_path,
                    )
                else:
                    logger.error("Failed to write %s: %s", file_path, e)
                raise


def count_entries(vault_path):
    """Count total entries across all JSONL/JSONL.ZST files. Returns (total_entries, num_files)."""
    total = 0
    num_files = 0

    for file_path in _find_jsonl_files(vault_path):
        num_files += 1
        try:
            fh = _open_jsonl(file_path)
            if fh is None:
                continue
            with fh:
                for line in fh:
                    if line.strip():
                        total += 1
        except (OSError, PermissionError) as e:
            logger.warning("Cannot read %s: %s", file_path, e)

    return total, num_files


def read_all_entries(vault_path):
    """Generator that yields all entries from all JSONL/JSONL.ZST files. Skips malformed JSON."""
    for file_path in _find_jsonl_files(vault_path):
        try:
            fh = _open_jsonl(file_path)
            if fh is None:
                continue
            with fh:
                for line_num, line in enumerate(fh, 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        yield json.loads(line)
                    except json.JSONDecodeError:
                        logger.warning(
                            "Skipping malformed JSON in %s line %d",
                            file_path,
                            line_num,
                        )
        except (OSError, PermissionError) as e:
            logger.warning("Cannot read %s: %s", file_path, e)


def read_entry_ids(vault_path):
    """Return set of all entry IDs found in JSONL files."""
    ids = set()
    for entry in read_all_entries(vault_path):
        if "id" in entry:
            ids.add(entry["id"])
    return ids


def verify_integrity(vault_path):
    """
    Compare entries on disk with processed_ids.txt.
    Returns dict with: disk_ids, log_ids, missing, duplicates.
    Prints a status line.
    """
    disk_ids = set()
    disk_entries = 0

    for file_path in _find_jsonl_files(vault_path):
        try:
            fh = _open_jsonl(file_path)
            if fh is None:
                continue
            with fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                        disk_entries += 1
                        if "id" in entry:
                            disk_ids.add(entry["id"])
                    except json.JSONDecodeError:
                        pass
        except (OSError, PermissionError):
            pass

    log_ids = load_processed_ids(vault_path)
    missing = log_ids - disk_ids
    duplicates = disk_entries - len(disk_ids)

    if not missing and duplicates == 0:
        print(f"  Integrity OK ({len(disk_ids):,} entries, all accounted for)")
    else:
        if missing:
            print(f"  WARNING: {len(missing):,} IDs in log but missing from disk")
        if duplicates > 0:
            print(f"  INFO: {duplicates:,} duplicate entries found")

    return {
        "disk_ids": disk_ids,
        "log_ids": log_ids,
        "missing": missing,
        "duplicates": duplicates,
    }


def load_processed_ids(vault_path):
    """Load processed_ids.txt into a set."""
    processed_log = os.path.join(vault_path, "processed_ids.txt")
    if not os.path.exists(processed_log):
        return set()

    with open(processed_log, "r") as f:
        return {line.strip() for line in f if line.strip()}


def append_processed_ids(vault_path, ids):
    """Append IDs to processed_ids.txt."""
    if not ids:
        return

    os.makedirs(vault_path, exist_ok=True)
    processed_log = os.path.join(vault_path, "processed_ids.txt")

    with open(processed_log, "a") as f:
        for entry_id in ids:
            f.write(f"{entry_id}\n")


def atomic_write(file_path, lines):
    """Write lines to file atomically via temp file + rename."""
    dir_name = os.path.dirname(file_path)
    try:
        fd, tmp_path = tempfile.mkstemp(dir=dir_name, suffix=".tmp")
    except PermissionError:
        logger.error("Permission denied creating temp file in %s", dir_name)
        raise
    except OSError as e:
        if e.errno == 28:
            logger.error("Disk full — cannot write to %s.", dir_name)
        else:
            logger.error("Cannot create temp file in %s: %s", dir_name, e)
        raise

    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            for line in lines:
                f.write(line)
        os.replace(tmp_path, file_path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def read_entries_by_file(vault_path):
    """Return dict: {file_path: [entry, ...]} for all JSONL/JSONL.ZST files.
    Skips malformed JSON lines with a warning.
    """
    result = {}
    for file_path in _find_jsonl_files(vault_path):
        entries = []
        try:
            fh = _open_jsonl(file_path)
            if fh is None:
                continue
            with fh:
                for line_num, line in enumerate(fh, 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entries.append(json.loads(line))
                    except json.JSONDecodeError:
                        logger.warning(
                            "Skipping malformed JSON in %s line %d",
                            file_path, line_num,
                        )
        except (OSError, PermissionError) as e:
            logger.warning("Cannot read %s: %s", file_path, e)
            continue
        if entries:
            result[file_path] = entries
    return result


def rewrite_file_entries(file_path, entries):
    """Atomically rewrite a JSONL file with the given entries."""
    lines = [json.dumps(e) + "\n" for e in entries]
    atomic_write(file_path, lines)


def compress_vault(vault_path, level=9):
    """
    Compress all .jsonl files to .jsonl.zst using Zstandard.
    Verifies the compressed file before deleting the original.
    Returns (files_compressed, bytes_saved).
    """
    if not HAS_ZSTD:
        raise ImportError("zstandard is required: pip install zstandard")

    cctx = zstd.ZstdCompressor(level=level)
    files_compressed = 0
    bytes_saved = 0

    for file_path in list(_find_jsonl_files(vault_path)):
        if file_path.endswith(".zst"):
            continue  # already compressed

        zst_path = file_path + ".zst"
        original_size = os.path.getsize(file_path)

        # Compress
        with open(file_path, "rb") as f_in:
            with open(zst_path, "wb") as f_out:
                cctx.copy_stream(f_in, f_out)

        compressed_size = os.path.getsize(zst_path)

        # Verify: count lines in compressed file match original
        orig_lines = 0
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    orig_lines += 1

        comp_lines = 0
        fh = _open_jsonl(zst_path)
        with fh:
            for line in fh:
                if line.strip():
                    comp_lines += 1

        if comp_lines != orig_lines:
            logger.error(
                "Verification failed for %s: %d vs %d lines. Keeping original.",
                file_path, orig_lines, comp_lines,
            )
            os.remove(zst_path)
            continue

        # Safe to remove original
        os.remove(file_path)
        files_compressed += 1
        bytes_saved += original_size - compressed_size

    return files_compressed, bytes_saved
