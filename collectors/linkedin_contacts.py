"""
WHID LinkedIn Contacts Collector
Parses LinkedIn's exported Connections CSV into a local JSONL vault.

LinkedIn exports contacts via: Settings > Data Privacy > Get a copy of your data > Connections.
The CSV typically has columns: First Name, Last Name, Email Address, Company, Position, Connected On.
"""

import os
import csv
import hashlib
import logging
from datetime import datetime

from core.vault import flush_entries, load_processed_ids, append_processed_ids

logger = logging.getLogger("whid.linkedin_contacts")


def _normalize_columns(header_row):
    """
    Build a case-insensitive, whitespace-stripped mapping from normalized
    column name to its actual index.  Handles minor variations in LinkedIn's
    CSV headers across export versions.
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


def _make_id(first_name, last_name, email):
    """Generate a deterministic 12-char hex ID from name + email."""
    raw = f"{first_name}:{last_name}:{email}"
    return "contacts:linkedin:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]


def _parse_connected_on(date_str):
    """
    Parse LinkedIn's 'Connected On' date.  Common formats:
      - '15 Jan 2024'
      - '2024-01-15'
      - '01/15/2024'
    Returns ISO date string or empty string.
    """
    date_str = date_str.strip()
    if not date_str:
        return ""

    for fmt in ("%d %b %Y", "%Y-%m-%d", "%m/%d/%Y", "%b %d, %Y"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue

    logger.warning("Could not parse connected_on date: %r", date_str)
    return date_str


def _read_csv(export_path):
    """
    Read a LinkedIn Connections CSV, handling BOM and encoding issues.
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


def _row_to_entry(row, col_map, file_mtime_iso):
    """Convert a single CSV row to a vault entry dict. Returns None if row is empty."""
    first = _get(row, col_map, "first name", "first_name", "firstname")
    last = _get(row, col_map, "last name", "last_name", "lastname")

    if not first and not last:
        return None

    email = _get(row, col_map, "email address", "email_address", "email")
    company = _get(row, col_map, "company", "organization", "org")
    position = _get(row, col_map, "position", "title", "job title", "job_title")
    connected_on = _get(row, col_map, "connected on", "connected_on", "date connected")
    profile_url = _get(row, col_map, "url", "profile url", "profile_url", "linkedin url")

    entry_id = _make_id(first, last, email)
    display_name = f"{first} {last}".strip()

    return {
        "id": entry_id,
        "source": "linkedin",
        "source_id": profile_url,
        "name": {
            "display": display_name,
            "given": first,
            "family": last,
        },
        "emails": [email] if email else [],
        "phones": [],
        "organization": company,
        "title": position,
        "connected_on": _parse_connected_on(connected_on),
        "updated_at": file_mtime_iso,
    }


def run_import(export_path, config=None):
    """
    Main entry point: parse a LinkedIn Connections CSV and flush to vault.

    Args:
        export_path: Path to the LinkedIn Connections CSV file.
        config: Dict with at least 'vault_root' key.
    """
    config = config or {}
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    vault_root_base = config.get("vault_root", os.path.join(project_root, "vaults"))
    vault_root_base = os.path.expanduser(vault_root_base)
    if not os.path.isabs(vault_root_base):
        vault_root_base = os.path.join(project_root, vault_root_base)
    vault_path = os.path.join(vault_root_base, "Contacts_LinkedIn")

    logger.info("LinkedIn contacts import starting")
    logger.info("  CSV: %s", export_path)
    logger.info("  Vault: %s", vault_path)

    # Get file modification time for updated_at
    try:
        mtime = os.path.getmtime(export_path)
        file_mtime_iso = datetime.fromtimestamp(mtime).isoformat()
    except OSError as e:
        logger.error("Cannot stat CSV file %s: %s", export_path, e)
        raise

    # Read CSV
    col_map, rows = _read_csv(export_path)
    if col_map is None:
        return

    print(f"\n  LinkedIn Contacts Import")
    print(f"  CSV: {export_path}")
    print(f"  Rows in file: {len(rows)}")

    # Load already-processed IDs
    processed_ids = load_processed_ids(vault_path)
    if processed_ids:
        print(f"  Already processed: {len(processed_ids):,}")

    # Convert rows to entries, skipping duplicates
    new_entries = []
    skipped_empty = 0
    skipped_duplicate = 0

    for row in rows:
        entry = _row_to_entry(row, col_map, file_mtime_iso)
        if entry is None:
            skipped_empty += 1
            continue
        if entry["id"] in processed_ids:
            skipped_duplicate += 1
            continue
        new_entries.append(entry)

    if not new_entries:
        print("  Nothing new -- vault is up to date.")
        return

    # Flush to vault
    flush_entries(new_entries, vault_path, "contacts.jsonl")

    # Update processed IDs
    new_ids = [e["id"] for e in new_entries]
    append_processed_ids(vault_path, new_ids)

    # Summary
    print()
    print(f"  {'=' * 40}")
    print(f"    New contacts added: {len(new_entries):,}")
    if skipped_duplicate:
        print(f"    Skipped (already processed): {skipped_duplicate:,}")
    if skipped_empty:
        print(f"    Skipped (empty name): {skipped_empty:,}")
    print(f"    Saved to: {vault_path}")
    print(f"  {'=' * 40}")

    logger.info(
        "Import complete: %d new, %d duplicate, %d empty",
        len(new_entries), skipped_duplicate, skipped_empty,
    )
