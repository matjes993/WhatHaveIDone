"""
WHID Browser Collector
Reads Chrome browsing history from the local SQLite database or from
a CSV export into the unified Browser JSONL vault.

Chrome stores history at:
  macOS: ~/Library/Application Support/Google/Chrome/<Profile>/History
  Linux: ~/.config/google-chrome/<Profile>/History

The collector copies the database to a temp file first (Chrome locks it
while running), then reads from the copy.

Also supports CSV exports from browser history export extensions.

Usage:
  whid collect browser
  whid collect browser --profile="Profile 1"
  whid collect browser-csv ~/Downloads/chrome_history.csv
"""

import csv
import hashlib
import logging
import os
import platform
import shutil
import sqlite3
import tempfile
from datetime import datetime, timedelta
from urllib.parse import urlparse

from core.vault import flush_entries, load_processed_ids, append_processed_ids

logger = logging.getLogger("whid.browser")


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# WebKit epoch: 1601-01-01 00:00:00 UTC
# Difference between Unix epoch (1970-01-01) and WebKit epoch in microseconds
_WEBKIT_EPOCH_OFFSET = 11644473600 * 1_000_000


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
    return "browser:chrome:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]


def _safe_int(value, default=0):
    """Convert a string to int, returning default on failure."""
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return default


def _webkit_to_datetime(webkit_ts):
    """
    Convert a WebKit timestamp (microseconds since 1601-01-01 00:00:00 UTC)
    to a Python datetime object.

    Returns None if the timestamp is invalid or zero.
    """
    if not webkit_ts or webkit_ts <= 0:
        return None

    try:
        # Convert WebKit microseconds to Unix microseconds
        unix_us = webkit_ts - _WEBKIT_EPOCH_OFFSET
        if unix_us < 0:
            return None
        unix_seconds = unix_us / 1_000_000
        return datetime.utcfromtimestamp(unix_seconds)
    except (OSError, ValueError, OverflowError):
        return None


def _extract_domain(url):
    """
    Extract the domain from a URL.
    e.g. "https://www.example.com/page" -> "example.com"
    """
    if not url:
        return ""

    try:
        parsed = urlparse(url)
        hostname = parsed.hostname or ""
        # Strip www. prefix
        if hostname.startswith("www."):
            hostname = hostname[4:]
        return hostname
    except Exception:
        return ""


def _get_chrome_history_path(profile="Default"):
    """
    Return the path to Chrome's History SQLite database for the current platform.
    """
    system = platform.system()

    if system == "Darwin":
        base = os.path.expanduser(
            "~/Library/Application Support/Google/Chrome"
        )
    elif system == "Linux":
        base = os.path.expanduser("~/.config/google-chrome")
    elif system == "Windows":
        base = os.path.expandvars(
            r"%LOCALAPPDATA%\Google\Chrome\User Data"
        )
    else:
        logger.error("Unsupported platform: %s", system)
        return None

    history_path = os.path.join(base, profile, "History")
    return history_path


def _copy_to_temp(source_path):
    """
    Copy a file to a temporary location and return the temp path.
    This is needed because Chrome locks its database while running.
    """
    fd, temp_path = tempfile.mkstemp(suffix=".sqlite")
    os.close(fd)

    try:
        shutil.copy2(source_path, temp_path)
        return temp_path
    except (OSError, shutil.SameFileError) as e:
        logger.error("Could not copy %s to temp: %s", source_path, e)
        # Clean up on failure
        try:
            os.unlink(temp_path)
        except OSError:
            pass
        raise


# ---------------------------------------------------------------------------
# URL row parser
# ---------------------------------------------------------------------------

def _parse_url_row(row):
    """
    Convert a Chrome urls table row (dict-like) into a vault entry dict.
    Returns None if the row lacks a URL.
    """
    url = row.get("url", "")
    if not url:
        return None

    # Skip internal Chrome URLs
    if url.startswith(("chrome://", "chrome-extension://", "devtools://", "about:")):
        return None

    title = row.get("title", "") or ""
    visit_count = _safe_int(row.get("visit_count", 0))
    typed_count = _safe_int(row.get("typed_count", 0))
    last_visit_time = row.get("last_visit_time", 0) or 0

    domain = _extract_domain(url)
    last_dt = _webkit_to_datetime(last_visit_time)
    last_visit_str = last_dt.strftime("%Y-%m-%dT%H:%M:%S") if last_dt else ""
    year = last_dt.year if last_dt else 0
    month = last_dt.month if last_dt else 0

    entry_id = _make_id(url)

    # Build embedding text
    display_title = title if title else domain
    embedding = f"Visited '{display_title}' ({domain})"
    if visit_count > 1:
        embedding += f" — {visit_count} visits"
    if last_visit_str:
        date_only = last_visit_str[:10]
        embedding += f", last on {date_only}"

    return {
        "id": entry_id,
        "sources": ["chrome"],
        "url": url,
        "title": title,
        "domain": domain,
        "visit_count": visit_count,
        "typed_count": typed_count,
        "last_visit": last_visit_str,
        "first_visit": "",
        "year": year,
        "month": month,
        "updated_at": datetime.now().isoformat(),
        "browse_for_embedding": embedding,
    }


def _parse_csv_row(row, col_map):
    """
    Convert a CSV row into a vault entry dict.
    Returns None if the row lacks a URL.
    """
    url = _get(row, col_map, "url", "page url", "address", "link")
    if not url:
        return None

    # Skip internal Chrome URLs
    if url.startswith(("chrome://", "chrome-extension://", "devtools://", "about:")):
        return None

    title = _get(row, col_map, "title", "page title", "name")
    visit_count = _safe_int(_get(row, col_map, "visit count", "visits", "visit_count"))
    typed_count = _safe_int(_get(row, col_map, "typed count", "typed_count"))
    last_visit = _get(row, col_map, "last visit", "last visited", "last_visit", "date", "timestamp")

    domain = _extract_domain(url)

    # Parse last visit date
    last_dt = None
    if last_visit:
        for fmt in (
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
            "%m/%d/%Y %H:%M:%S",
            "%m/%d/%Y",
        ):
            try:
                last_dt = datetime.strptime(last_visit, fmt)
                break
            except ValueError:
                continue

    last_visit_str = last_dt.strftime("%Y-%m-%dT%H:%M:%S") if last_dt else last_visit
    year = last_dt.year if last_dt else 0
    month = last_dt.month if last_dt else 0

    # Default visit_count to 1 if not provided
    if visit_count == 0:
        visit_count = 1

    entry_id = _make_id(url)

    # Build embedding text
    display_title = title if title else domain
    embedding = f"Visited '{display_title}' ({domain})"
    if visit_count > 1:
        embedding += f" — {visit_count} visits"
    if last_visit_str:
        date_only = last_visit_str[:10]
        embedding += f", last on {date_only}"

    return {
        "id": entry_id,
        "sources": ["chrome"],
        "url": url,
        "title": title,
        "domain": domain,
        "visit_count": visit_count,
        "typed_count": typed_count,
        "last_visit": last_visit_str,
        "first_visit": "",
        "year": year,
        "month": month,
        "updated_at": datetime.now().isoformat(),
        "browse_for_embedding": embedding,
    }


# ---------------------------------------------------------------------------
# Main entry points
# ---------------------------------------------------------------------------

def run_import(config=None, profile="Default"):
    """
    Import Chrome browsing history from the local SQLite database into the vault.

    Copies the database to a temp file first (Chrome locks it while running),
    then reads from the copy.

    Args:
        config: Dict with optional 'vault_root' key.
        profile: Chrome profile name (default: "Default").
    """
    config = config or {}
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    vault_root_base = config.get("vault_root", os.path.join(project_root, "vaults"))
    vault_root_base = os.path.expanduser(vault_root_base)
    if not os.path.isabs(vault_root_base):
        vault_root_base = os.path.join(project_root, vault_root_base)
    vault_path = os.path.join(vault_root_base, "Browser")

    print(f"\n  WHID Browser Collector — Chrome")
    print(f"  {'=' * 45}")
    print(f"  Profile: {profile}")
    print(f"  Vault: {vault_path}")

    # Locate Chrome history database
    history_path = _get_chrome_history_path(profile)
    if history_path is None:
        print("  Error: Could not determine Chrome history path for this platform.")
        return

    if not os.path.isfile(history_path):
        print(f"  Error: Chrome history not found at {history_path}")
        print("  Make sure Chrome is installed and the profile name is correct.")
        return

    print(f"  History DB: {history_path}")

    # Copy to temp location (Chrome locks the DB while running)
    print("  Copying database to temp location...")
    try:
        temp_path = _copy_to_temp(history_path)
    except OSError as e:
        print(f"  Error: Could not copy database: {e}")
        print("  Try closing Chrome and running again.")
        return

    try:
        _import_from_sqlite(temp_path, vault_path)
    finally:
        # Clean up temp file
        try:
            os.unlink(temp_path)
        except OSError:
            pass


def run_import_csv(export_path, config=None):
    """
    Import browser history from a CSV export into the vault.

    Args:
        export_path: Path to the CSV file.
        config: Dict with optional 'vault_root' key.
    """
    config = config or {}
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    vault_root_base = config.get("vault_root", os.path.join(project_root, "vaults"))
    vault_root_base = os.path.expanduser(vault_root_base)
    if not os.path.isabs(vault_root_base):
        vault_root_base = os.path.join(project_root, vault_root_base)
    vault_path = os.path.join(vault_root_base, "Browser")

    export_path = os.path.expanduser(export_path)

    print(f"\n  WHID Browser Collector — CSV")
    print(f"  {'=' * 45}")
    print(f"  CSV: {export_path}")
    print(f"  Vault: {vault_path}")

    # Read CSV
    col_map, rows = _read_csv(export_path)
    if col_map is None:
        return

    print(f"  URLs found: {len(rows)}")

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
            entry = _parse_csv_row(row, col_map)
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
    flush_entries(new_entries, vault_path, "browser.jsonl")
    new_ids = [e["id"] for e in new_entries]
    append_processed_ids(vault_path, new_ids)

    # Summary stats
    unique_domains = set(e["domain"] for e in new_entries if e.get("domain"))
    total_visits = sum(e.get("visit_count", 0) for e in new_entries)

    print()
    print(f"  {'=' * 45}")
    print(f"  Done! {len(new_entries):,} URLs saved")
    print(f"  {'=' * 45}")
    print(f"    Unique domains:  {len(unique_domains):,}")
    print(f"    Total visits:    {total_visits:,}")
    if skipped_duplicate:
        print(f"    Skipped (dupe):  {skipped_duplicate:,}")
    if skipped_empty:
        print(f"    Skipped (empty): {skipped_empty:,}")
    print()

    logger.info(
        "CSV browser import complete: %d new, %d duplicate, %d empty",
        len(new_entries), skipped_duplicate, skipped_empty,
    )


def _import_from_sqlite(db_path, vault_path):
    """Import browser history from a Chrome SQLite database copy."""
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
    except sqlite3.Error as e:
        print(f"  Error opening database: {e}")
        return

    try:
        # Query URLs with visit info
        try:
            cursor = conn.execute(
                "SELECT u.url, u.title, u.visit_count, u.typed_count, "
                "u.last_visit_time, "
                "MIN(v.visit_time) as first_visit_time "
                "FROM urls u "
                "LEFT JOIN visits v ON v.url = u.id "
                "GROUP BY u.id "
                "ORDER BY u.last_visit_time DESC"
            )
            url_rows = [dict(row) for row in cursor]
        except sqlite3.Error as e:
            print(f"  Error querying database: {e}")
            return

        print(f"  URLs found: {len(url_rows):,}")

        # Load already-processed IDs
        processed_ids = load_processed_ids(vault_path)
        if processed_ids:
            print(f"  Already processed: {len(processed_ids):,}")

        new_entries = []
        skipped_empty = 0
        skipped_duplicate = 0

        for row in url_rows:
            try:
                entry = _parse_url_row(row)
            except Exception as e:
                logger.warning("Skipping URL: %s", e)
                skipped_empty += 1
                continue

            if entry is None:
                skipped_empty += 1
                continue

            # Add first_visit from the joined visits table
            first_visit_time = row.get("first_visit_time", 0) or 0
            first_dt = _webkit_to_datetime(first_visit_time)
            if first_dt:
                entry["first_visit"] = first_dt.strftime("%Y-%m-%dT%H:%M:%S")

            if entry["id"] in processed_ids:
                skipped_duplicate += 1
                continue

            new_entries.append(entry)

        if not new_entries:
            print("  Nothing new — vault is up to date.")
            return

        # Flush to vault
        flush_entries(new_entries, vault_path, "browser.jsonl")
        new_ids = [e["id"] for e in new_entries]
        append_processed_ids(vault_path, new_ids)

        # Summary stats
        unique_domains = set(e["domain"] for e in new_entries if e.get("domain"))
        total_visits = sum(e.get("visit_count", 0) for e in new_entries)

        # Top domains
        domain_counts = {}
        for e in new_entries:
            d = e.get("domain", "")
            if d:
                domain_counts[d] = domain_counts.get(d, 0) + e.get("visit_count", 0)
        top_domains = sorted(domain_counts.items(), key=lambda x: x[1], reverse=True)[:10]

        year_counts = {}
        for e in new_entries:
            y = e.get("year", 0)
            if y:
                year_counts[y] = year_counts.get(y, 0) + 1

        print()
        print(f"  {'=' * 45}")
        print(f"  Done! {len(new_entries):,} URLs saved")
        print(f"  {'=' * 45}")
        print(f"    Unique domains:  {len(unique_domains):,}")
        print(f"    Total visits:    {total_visits:,}")
        if top_domains:
            print(f"    Top domains:")
            for domain, count in top_domains:
                print(f"      {domain}: {count:,}")
        if year_counts:
            for year in sorted(year_counts.keys()):
                print(f"    {year}: {year_counts[year]:,}")
        if skipped_duplicate:
            print(f"    Skipped (dupe):  {skipped_duplicate:,}")
        if skipped_empty:
            print(f"    Skipped (internal): {skipped_empty:,}")
        print()

        logger.info(
            "Chrome import complete: %d new, %d duplicate, %d internal",
            len(new_entries), skipped_duplicate, skipped_empty,
        )

    finally:
        conn.close()
