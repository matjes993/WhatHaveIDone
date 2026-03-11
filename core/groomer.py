"""
WHID Vault Groomer
Sorts, deduplicates, and validates JSONL vault files.
Implements the Sniper Mechanism: identifies ghost IDs (records logged as
processed but missing from disk) and writes missing_ids.txt for collectors
to recover them.
"""

import os
import json
import sys
import logging
import tempfile
from datetime import datetime

logger = logging.getLogger("whid.groomer")


def parse_date(date_str):
    """Parse email-style or ISO date strings. Returns None on failure."""
    if not date_str:
        return None

    # Try RFC 2822 style: "Mon, 01 Jan 2024 12:00:00 +0000 (UTC)"
    clean = date_str.split(" (")[0].strip()
    try:
        return datetime.strptime(clean, "%a, %d %b %Y %H:%M:%S %z")
    except ValueError:
        pass

    # Try ISO format
    try:
        return datetime.fromisoformat(date_str)
    except ValueError:
        pass

    logger.warning("Unparseable date: %s", date_str)
    return None


def _sort_key(entry):
    """Sort key that puts entries with unparseable dates at the end."""
    dt = parse_date(entry.get("date", ""))
    if dt is None:
        return datetime.max.replace(tzinfo=None)
    return dt.replace(tzinfo=None)


def _atomic_write(file_path, lines):
    """Write lines to file atomically via temp file + rename."""
    dir_name = os.path.dirname(file_path)
    fd, tmp_path = tempfile.mkstemp(dir=dir_name, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            for line in lines:
                f.write(line)
        os.replace(tmp_path, file_path)
    except Exception:
        os.unlink(tmp_path)
        raise


def groom_vault(vault_path):
    """
    Groom a vault directory:
    1. Deduplicate entries by ID within each JSONL file
    2. Sort entries chronologically
    3. Detect ghost IDs and write missing_ids.txt (Sniper Mechanism)
    """
    if not os.path.exists(vault_path):
        logger.error("Vault path does not exist: %s", vault_path)
        return

    processed_log = os.path.join(vault_path, "processed_ids.txt")
    missing_log = os.path.join(vault_path, "missing_ids.txt")

    # Load previously processed IDs
    old_log_ids = set()
    if os.path.exists(processed_log):
        with open(processed_log, "r") as f:
            old_log_ids = {line.strip() for line in f if line.strip()}

    all_found_ids = set()
    files_processed = 0
    entries_deduped = 0

    for root, _dirs, files in os.walk(vault_path):
        for file in files:
            if not file.endswith(".jsonl"):
                continue

            file_path = os.path.join(root, file)
            entries = []

            with open(file_path, "r", encoding="utf-8") as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                    except json.JSONDecodeError as e:
                        logger.warning(
                            "Skipping malformed JSON in %s line %d: %s",
                            file_path, line_num, e,
                        )
                        continue

                    if "id" not in entry:
                        logger.warning(
                            "Skipping entry without 'id' in %s line %d",
                            file_path, line_num,
                        )
                        continue

                    entries.append(entry)

            if not entries:
                continue

            # Deduplicate by ID (last occurrence wins)
            before_count = len(entries)
            unique = {e["id"]: e for e in entries}
            entries_deduped += before_count - len(unique)

            # Sort chronologically
            sorted_entries = sorted(unique.values(), key=_sort_key)

            # Atomic write back
            lines = [json.dumps(e) + "\n" for e in sorted_entries]
            _atomic_write(file_path, lines)

            for e in sorted_entries:
                all_found_ids.add(e["id"])

            files_processed += 1

    # Sniper Mechanism: detect ghosts
    ghost_ids = old_log_ids - all_found_ids
    if ghost_ids:
        logger.info(
            "Sniper: found %d ghost IDs — writing missing_ids.txt", len(ghost_ids)
        )
        _atomic_write(missing_log, [f"{gid}\n" for gid in sorted(ghost_ids)])
    elif os.path.exists(missing_log):
        os.remove(missing_log)

    # Update processed log
    _atomic_write(processed_log, [f"{mid}\n" for mid in sorted(all_found_ids)])

    logger.info(
        "Groomed %s: %d files, %d unique entries, %d duplicates removed, %d ghosts",
        vault_path, files_processed, len(all_found_ids), entries_deduped, len(ghost_ids),
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    if len(sys.argv) < 2:
        print("Usage: python -m core.groomer <vault_path>")
        sys.exit(1)

    groom_vault(sys.argv[1])
