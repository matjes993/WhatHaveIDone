"""
NOMOLO Books Collector
Parses book data from Goodreads CSV export and Audible library CSV
into the unified Books JSONL vault.

Supports two import modes:
  1. Goodreads CSV: nomolo collect books-goodreads ~/Downloads/goodreads_library_export.csv
  2. Audible CSV:   nomolo collect books-audible ~/Downloads/audible_library.csv

Both write to the Books/ vault directory with unified schema.
"""

import csv
import hashlib
import logging
import os
from datetime import datetime

from core.vault import flush_entries, load_processed_ids, append_processed_ids

logger = logging.getLogger("nomolo.books")


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


def _make_id(source, *parts):
    """Generate a deterministic 12-char hex ID from source and key parts."""
    raw = ":".join(str(p) for p in parts)
    return f"books:{source}:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]


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


def _parse_shelves(shelves_str):
    """Parse a comma-separated shelves string into a list."""
    if not shelves_str:
        return []
    return [s.strip() for s in shelves_str.split(",") if s.strip()]


# ---------------------------------------------------------------------------
# Goodreads parser
# ---------------------------------------------------------------------------

def _parse_goodreads_row(row, col_map):
    """
    Convert a single Goodreads CSV row to a vault entry dict.
    Returns None if the row lacks a title.
    """
    title = _get(row, col_map, "title")
    if not title:
        return None

    author = _get(row, col_map, "author", "author l-f")
    additional_authors = _get(row, col_map, "additional authors")
    isbn = _get(row, col_map, "isbn").strip("= \"'")
    isbn13 = _get(row, col_map, "isbn13").strip("= \"'")
    my_rating = _safe_int(_get(row, col_map, "my rating"))
    average_rating = _safe_float(_get(row, col_map, "average rating"))
    publisher = _get(row, col_map, "publisher")
    binding = _get(row, col_map, "binding")
    pages = _safe_int(_get(row, col_map, "number of pages"))
    year_published = _safe_int(
        _get(row, col_map, "original publication year", "year published")
    )
    date_read = _get(row, col_map, "date read")
    date_added = _get(row, col_map, "date added")
    shelves_str = _get(row, col_map, "bookshelves")
    exclusive_shelf = _get(row, col_map, "exclusive shelf")
    review = _get(row, col_map, "my review")
    book_id = _get(row, col_map, "book id")

    shelves = _parse_shelves(shelves_str)
    if exclusive_shelf and exclusive_shelf not in shelves:
        shelves.insert(0, exclusive_shelf)

    # Map exclusive shelf to status
    status_map = {
        "read": "read",
        "currently-reading": "currently-reading",
        "to-read": "to-read",
    }
    status = status_map.get(exclusive_shelf, exclusive_shelf or "unknown")

    # Build embedding text
    embedding_parts = [f"{title} by {author}"]
    if year_published:
        embedding_parts[0] += f" ({year_published}"
        if pages:
            embedding_parts[0] += f", {pages} pages"
        embedding_parts[0] += ")"
    if my_rating:
        embedding_parts.append(f"Rating: {my_rating}/5")
    if shelves:
        embedding_parts.append(f"Shelves: {', '.join(shelves)}")
    if review:
        embedding_parts.append(f"Review excerpt: {review[:150]}")

    entry_id = _make_id("goodreads", book_id or title, author)

    return {
        "id": entry_id,
        "sources": ["goodreads"],
        "title": title,
        "author": author,
        "additional_authors": additional_authors,
        "isbn": isbn,
        "isbn13": isbn13,
        "my_rating": my_rating,
        "average_rating": average_rating,
        "publisher": publisher,
        "format": binding.lower() if binding else "",
        "pages": pages,
        "year_published": year_published,
        "date_read": date_read,
        "date_added": date_added,
        "shelves": shelves,
        "status": status,
        "review": review,
        "narrator": "",
        "duration": "",
        "updated_at": datetime.now().isoformat(),
        "book_for_embedding": " — ".join(embedding_parts),
    }


# ---------------------------------------------------------------------------
# Audible parser
# ---------------------------------------------------------------------------

def _parse_audible_row(row, col_map):
    """
    Convert a single Audible CSV row to a vault entry dict.
    Returns None if the row lacks a title.
    """
    title = _get(row, col_map, "title")
    if not title:
        return None

    author = _get(row, col_map, "author", "authors")
    narrator = _get(row, col_map, "narrator", "narrators")
    duration = _get(row, col_map, "length", "duration")
    date_added = _get(row, col_map, "date added", "date_added")
    date_purchased = _get(row, col_map, "date purchased", "date_purchased", "purchase date")
    rating = _safe_int(_get(row, col_map, "rating", "my rating"))

    # Use date_purchased as date_added fallback
    if not date_added and date_purchased:
        date_added = date_purchased

    # Build embedding text
    embedding_parts = [f"{title} by {author}"]
    if duration:
        embedding_parts[0] += f" ({duration})"
    embedding_parts[0] += " [audiobook]"
    if narrator:
        embedding_parts.append(f"Narrated by {narrator}")
    if rating:
        embedding_parts.append(f"Rating: {rating}/5")

    entry_id = _make_id("audible", title, author)

    return {
        "id": entry_id,
        "sources": ["audible"],
        "title": title,
        "author": author,
        "additional_authors": "",
        "isbn": "",
        "isbn13": "",
        "my_rating": rating,
        "average_rating": 0.0,
        "publisher": "",
        "format": "audiobook",
        "pages": 0,
        "year_published": 0,
        "date_read": "",
        "date_added": date_added,
        "shelves": [],
        "status": "read",
        "review": "",
        "narrator": narrator,
        "duration": duration,
        "updated_at": datetime.now().isoformat(),
        "book_for_embedding": " — ".join(embedding_parts),
    }


# ---------------------------------------------------------------------------
# Main entry points
# ---------------------------------------------------------------------------

def run_import_goodreads(export_path, config=None):
    """
    Import books from a Goodreads CSV export into the vault.

    Args:
        export_path: Path to the Goodreads library export CSV.
        config: Dict with optional 'vault_root' key.
    """
    config = config or {}
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    vault_root_base = config.get("vault_root", os.path.join(project_root, "vaults"))
    vault_root_base = os.path.expanduser(vault_root_base)
    if not os.path.isabs(vault_root_base):
        vault_root_base = os.path.join(project_root, vault_root_base)
    vault_path = os.path.join(vault_root_base, "Books")

    print(f"\n  NOMOLO Books Collector — Goodreads")
    print(f"  {'=' * 45}")
    print(f"  CSV: {export_path}")
    print(f"  Vault: {vault_path}")

    # Read CSV
    col_map, rows = _read_csv(export_path)
    if col_map is None:
        return

    print(f"  Books found: {len(rows)}")

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
            entry = _parse_goodreads_row(row, col_map)
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
    flush_entries(new_entries, vault_path, "books.jsonl")
    new_ids = [e["id"] for e in new_entries]
    append_processed_ids(vault_path, new_ids)

    # Summary stats
    with_rating = sum(1 for e in new_entries if e.get("my_rating"))
    with_review = sum(1 for e in new_entries if e.get("review"))
    status_counts = {}
    for e in new_entries:
        s = e.get("status", "unknown")
        status_counts[s] = status_counts.get(s, 0) + 1

    print()
    print(f"  {'=' * 45}")
    print(f"  Done! {len(new_entries):,} books saved")
    print(f"  {'=' * 45}")
    print(f"    With rating:     {with_rating:,}")
    print(f"    With review:     {with_review:,}")
    for status, count in sorted(status_counts.items()):
        print(f"    {status}: {count:,}")
    if skipped_duplicate:
        print(f"    Skipped (dupe):  {skipped_duplicate:,}")
    if skipped_empty:
        print(f"    Skipped (empty): {skipped_empty:,}")
    print()

    logger.info(
        "Goodreads import complete: %d new, %d duplicate, %d empty",
        len(new_entries), skipped_duplicate, skipped_empty,
    )


def run_import_audible(export_path, config=None):
    """
    Import books from an Audible library CSV export into the vault.

    Args:
        export_path: Path to the Audible library CSV.
        config: Dict with optional 'vault_root' key.
    """
    config = config or {}
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    vault_root_base = config.get("vault_root", os.path.join(project_root, "vaults"))
    vault_root_base = os.path.expanduser(vault_root_base)
    if not os.path.isabs(vault_root_base):
        vault_root_base = os.path.join(project_root, vault_root_base)
    vault_path = os.path.join(vault_root_base, "Books")

    print(f"\n  NOMOLO Books Collector — Audible")
    print(f"  {'=' * 45}")
    print(f"  CSV: {export_path}")
    print(f"  Vault: {vault_path}")

    # Read CSV
    col_map, rows = _read_csv(export_path)
    if col_map is None:
        return

    print(f"  Audiobooks found: {len(rows)}")

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
            entry = _parse_audible_row(row, col_map)
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
    flush_entries(new_entries, vault_path, "books.jsonl")
    new_ids = [e["id"] for e in new_entries]
    append_processed_ids(vault_path, new_ids)

    # Summary stats
    with_rating = sum(1 for e in new_entries if e.get("my_rating"))
    with_narrator = sum(1 for e in new_entries if e.get("narrator"))

    print()
    print(f"  {'=' * 45}")
    print(f"  Done! {len(new_entries):,} audiobooks saved")
    print(f"  {'=' * 45}")
    print(f"    With rating:     {with_rating:,}")
    print(f"    With narrator:   {with_narrator:,}")
    if skipped_duplicate:
        print(f"    Skipped (dupe):  {skipped_duplicate:,}")
    if skipped_empty:
        print(f"    Skipped (empty): {skipped_empty:,}")
    print()

    logger.info(
        "Audible import complete: %d new, %d duplicate, %d empty",
        len(new_entries), skipped_duplicate, skipped_empty,
    )
