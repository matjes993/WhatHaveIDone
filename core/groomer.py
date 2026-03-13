"""
NOMOLO Vault Groomer
Sorts, deduplicates, and validates JSONL vault files.
Implements the Sniper Mechanism: identifies ghost IDs (records logged as
processed but missing from disk) and writes missing_ids.txt for collectors
to recover them.
"""

import os
import json
import sys
import logging
from datetime import datetime

from core.vault import atomic_write as _atomic_write, _open_jsonl, _find_jsonl_files, HAS_ZSTD

logger = logging.getLogger("nomolo.groomer")


def parse_date(date_str):
    """Parse email-style or ISO date strings. Returns None on failure."""
    if not date_str:
        return None

    clean = date_str.split(" (")[0].strip()

    # Replace named timezones with numeric offsets
    clean = clean.replace(" GMT", " +0000")
    clean = clean.replace(" UTC", " +0000")
    if clean.endswith(" UT"):
        clean = clean[:-3] + " +0000"
    clean = clean.replace(" EST", " -0500")
    clean = clean.replace(" EDT", " -0400")
    clean = clean.replace(" CST", " -0600")
    clean = clean.replace(" CDT", " -0500")
    clean = clean.replace(" MST", " -0700")
    clean = clean.replace(" MDT", " -0600")
    clean = clean.replace(" PST", " -0800")
    clean = clean.replace(" PDT", " -0700")
    clean = clean.replace(" CET", " +0100")
    clean = clean.replace(" CEST", " +0200")

    # RFC 2822: "Mon, 01 Jan 2024 12:00:00 +0000"
    try:
        return datetime.strptime(clean, "%a, %d %b %Y %H:%M:%S %z")
    except ValueError:
        pass

    # Without day-of-week: "01 Jan 2024 12:00:00 +0000"
    try:
        return datetime.strptime(clean, "%d %b %Y %H:%M:%S %z")
    except ValueError:
        pass

    # ISO format
    try:
        return datetime.fromisoformat(date_str)
    except ValueError:
        pass

    # ctime format: "Fri Mar  6 09:19:53 2026"
    try:
        return datetime.strptime(clean, "%a %b %d %H:%M:%S %Y")
    except ValueError:
        pass

    # No timezone: "Mon, 27 May 2013 13:51:26"
    try:
        return datetime.strptime(clean, "%a, %d %b %Y %H:%M:%S")
    except ValueError:
        pass

    # 2-digit year: "01 Feb 21 11:02:06 +0100"
    try:
        return datetime.strptime(clean, "%d %b %y %H:%M:%S %z")
    except ValueError:
        pass

    # 2-digit year without seconds: "04 Dec 14 00:45 +0000"
    try:
        return datetime.strptime(clean, "%d %b %y %H:%M %z")
    except ValueError:
        pass

    logger.debug("Unparseable date: %s", date_str)
    return None


def _sort_key(entry):
    """Sort key that puts entries with unparseable dates at the end."""
    dt = parse_date(entry.get("date", ""))
    if dt is None:
        return datetime.max.replace(tzinfo=None)
    return dt.replace(tzinfo=None)


def groom_vault(vault_path):
    """
    Groom a vault directory:
    1. Deduplicate entries by ID within each JSONL file
    2. Sort entries chronologically
    3. Detect ghost IDs and write missing_ids.txt (Sniper Mechanism)
    """
    if not os.path.exists(vault_path):
        logger.error("🏴‍☠️ Blimey! No treasure chest found at: %s", vault_path)
        print(f"\n🏴‍☠️ Arrr! No vault found at: {vault_path}")
        print("Run 'nomolo collect gmail' first to fill yer treasure chest.")
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
    entries_skipped = 0
    unparseable_dates = 0
    total_entries = 0

    for file_path in _find_jsonl_files(vault_path):
        is_compressed = file_path.endswith(".zst")
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
                        entry = json.loads(line)
                    except json.JSONDecodeError as e:
                        logger.warning(
                            "🏴‍☠️ Blimey! Barnacle-encrusted JSON in %s line %d: %s — tossin' it overboard",
                            file_path,
                            line_num,
                            e,
                        )
                        entries_skipped += 1
                        continue

                    if "id" not in entry:
                        logger.warning(
                            "🏴‍☠️ Found an unmarked treasure in %s line %d — no 'id' tag, tossin' it overboard",
                            file_path,
                            line_num,
                        )
                        entries_skipped += 1
                        continue

                    entries.append(entry)
        except PermissionError:
            logger.error("🏴‍☠️ Blimey! The lock on %s won't budge — skipping", file_path)
            continue
        except OSError as e:
            logger.error("🏴‍☠️ Blimey! Can't read the scroll %s: %s — skipping", file_path, e)
            continue

        if not entries:
            continue

        # Deduplicate by ID (last occurrence wins)
        before_count = len(entries)
        unique = {e["id"]: e for e in entries}
        entries_deduped += before_count - len(unique)
        total_entries += len(unique)

        # Sort chronologically and count unparseable dates
        sorted_entries = sorted(unique.values(), key=_sort_key)
        for e in sorted_entries:
            if parse_date(e.get("date", "")) is None and e.get("date", ""):
                unparseable_dates += 1

        # Write back — plain JSONL (even if source was compressed)
        # The user can re-compress afterwards with `nomolo compress`
        write_path = file_path[:-4] if is_compressed else file_path  # strip .zst
        lines = [json.dumps(e) + "\n" for e in sorted_entries]
        try:
            _atomic_write(write_path, lines)
            # Remove old compressed file if we wrote a new plain one
            if is_compressed and os.path.exists(file_path):
                os.remove(file_path)
        except (PermissionError, OSError) as e:
            logger.error(
                "🏴‍☠️ Blimey! Failed to stash the groomed scroll %s: %s — original treasure untouched",
                write_path,
                e,
            )
            continue

        for e in sorted_entries:
            all_found_ids.add(e["id"])

        files_processed += 1

    # Sniper Mechanism: detect ghosts
    ghost_ids = old_log_ids - all_found_ids
    if ghost_ids:
        logger.info(
            "🎯 The Sniper spotted %d missing treasures — writing missing_ids.txt", len(ghost_ids)
        )
        logger.info(
            "⚔️ Run 'nomolo collect gmail' to hunt 'em down and recover the loot!"
        )
        try:
            _atomic_write(missing_log, [f"{gid}\n" for gid in sorted(ghost_ids)])
        except (PermissionError, OSError) as e:
            logger.error("🏴‍☠️ Blimey! Failed to write the missing treasure map: %s", e)
    elif os.path.exists(missing_log):
        try:
            os.remove(missing_log)
        except OSError as e:
            logger.warning("🏴‍☠️ Could not scuttle the old missing_ids.txt: %s", e)

    # Update processed log
    try:
        _atomic_write(processed_log, [f"{mid}\n" for mid in sorted(all_found_ids)])
    except (PermissionError, OSError) as e:
        logger.error("🏴‍☠️ Blimey! Failed to update the ship's log: %s", e)

    logger.info(
        "📜 Scrolls sorted by stardate. The archive is shipshape! "
        "Groomed %s: %d files, %d unique entries, %d duplicates removed, %d ghosts",
        vault_path,
        files_processed,
        len(all_found_ids),
        entries_deduped,
        len(ghost_ids),
    )

    if entries_skipped > 0:
        logger.warning(
            "🏴‍☠️ %d barnacle-encrusted entries were tossed overboard (malformed JSON or missing ID). "
            "Check the captain's log above for details.",
            entries_skipped,
        )

    if entries_deduped > 0:
        logger.info("🧹 The Groomer found %d duplicate doubloons. Tossed 'em overboard.", entries_deduped)

    if unparseable_dates > 0:
        logger.info(
            "🗺️ %d entries have dates even a cartographer couldn't read (filed under _unknown/). "
            "These are barnacle-covered date headers from the sender — not recoverable.",
            unparseable_dates,
        )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    if len(sys.argv) < 2:
        print("Usage: python -m core.groomer <vault_path>")
        print("   or: nomolo groom gmail")
        sys.exit(1)

    groom_vault(os.path.expanduser(sys.argv[1]))
