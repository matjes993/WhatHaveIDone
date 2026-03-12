"""
WHID Shopping Collector
Parses Amazon order history CSV into the unified Shopping JSONL vault.

Usage:
  whid collect shopping-amazon ~/Downloads/amazon_order_history.csv
"""

import csv
import hashlib
import logging
import os
from datetime import datetime

from core.vault import flush_entries, load_processed_ids, append_processed_ids

logger = logging.getLogger("whid.shopping")


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


def _make_id(*parts):
    """Generate a deterministic 12-char hex ID from key parts."""
    raw = ":".join(str(p) for p in parts)
    return "shopping:amazon:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]


def _safe_float(value, default=0.0):
    """Convert a string to float, returning default on failure."""
    if not value:
        return default
    cleaned = value.replace(",", ".").strip()
    cleaned = cleaned.replace("€", "").replace("$", "").replace("£", "").strip()
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return default


def _safe_int(value, default=0):
    """Convert a string to int, returning default on failure."""
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return default


def _parse_date(date_str):
    """
    Parse a date string into YYYY-MM-DD format.
    Handles common date formats from Amazon exports.
    Returns the original string if no format matches.
    """
    if not date_str:
        return ""

    for fmt in (
        "%m/%d/%Y",
        "%d/%m/%Y",
        "%Y-%m-%d",
        "%d.%m.%Y",
        "%Y/%m/%d",
        "%B %d, %Y",
        "%b %d, %Y",
    ):
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue

    return date_str.strip()


# ---------------------------------------------------------------------------
# Amazon parser
# ---------------------------------------------------------------------------

def _parse_amazon_row(row, col_map):
    """
    Convert a single Amazon order history CSV row to a vault entry dict.
    Returns None if the row lacks a title or order ID.
    """
    title = _get(row, col_map, "title", "product name", "item title")
    order_id = _get(row, col_map, "order id", "order number")

    if not title:
        return None

    order_date_raw = _get(row, col_map, "order date", "date")
    order_date = _parse_date(order_date_raw)
    category = _get(row, col_map, "category", "product category")
    asin = _get(row, col_map, "asin/isbn", "asin", "isbn")
    quantity = _safe_int(_get(row, col_map, "quantity"), default=1)
    price = _safe_float(_get(row, col_map, "item total", "price", "total"))
    currency = _get(row, col_map, "currency", default="EUR")
    seller = _get(row, col_map, "seller", "sold by")
    status = _get(row, col_map, "order status", "status", default="Delivered")
    shipping_address = _get(row, col_map, "shipping address", "address")

    # Parse year/month from normalized date
    year, month = 0, 0
    if order_date and len(order_date) >= 7:
        try:
            dt = datetime.strptime(order_date, "%Y-%m-%d")
            year = dt.year
            month = dt.month
        except ValueError:
            pass

    entry_id = _make_id(order_id or title, asin or title)

    # Build embedding text
    embedding = f"{order_date} — Bought '{title}'"
    if category:
        embedding += f" ({category})"
    embedding += f" for {price} {currency}"
    if seller:
        embedding += f" from {seller}"

    return {
        "id": entry_id,
        "sources": ["amazon"],
        "order_date": order_date,
        "order_id": order_id,
        "title": title,
        "category": category,
        "asin": asin,
        "quantity": quantity,
        "price": price,
        "currency": currency,
        "seller": seller,
        "status": status,
        "year": year,
        "month": month,
        "updated_at": datetime.now().isoformat(),
        "order_for_embedding": embedding,
    }


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_import_amazon(export_path, config=None):
    """
    Import orders from an Amazon order history CSV export into the vault.

    Args:
        export_path: Path to the Amazon order history CSV file.
        config: Dict with optional 'vault_root' key.
    """
    config = config or {}
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    vault_root_base = config.get("vault_root", os.path.join(project_root, "vaults"))
    vault_root_base = os.path.expanduser(vault_root_base)
    if not os.path.isabs(vault_root_base):
        vault_root_base = os.path.join(project_root, vault_root_base)
    vault_path = os.path.join(vault_root_base, "Shopping")

    print(f"\n  WHID Shopping Collector — Amazon")
    print(f"  {'=' * 45}")
    print(f"  CSV: {export_path}")
    print(f"  Vault: {vault_path}")

    # Read CSV
    col_map, rows = _read_csv(export_path)
    if col_map is None:
        return

    print(f"  Orders found: {len(rows)}")

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
            entry = _parse_amazon_row(row, col_map)
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
    flush_entries(new_entries, vault_path, "shopping.jsonl")
    new_ids = [e["id"] for e in new_entries]
    append_processed_ids(vault_path, new_ids)

    # Summary stats
    total_spent = sum(e["price"] for e in new_entries)
    total_items = sum(e["quantity"] for e in new_entries)
    categories = {}
    sellers = {}
    for e in new_entries:
        cat = e.get("category") or "Uncategorized"
        categories[cat] = categories.get(cat, 0) + 1
        sel = e.get("seller") or "Unknown"
        sellers[sel] = sellers.get(sel, 0) + 1

    print()
    print(f"  {'=' * 45}")
    print(f"  Done! {len(new_entries):,} orders saved")
    print(f"  {'=' * 45}")
    print(f"    Total spent:     {total_spent:,.2f}")
    print(f"    Total items:     {total_items:,}")
    if categories:
        print(f"    Categories:      {len(categories)}")
        for cat, count in sorted(categories.items(), key=lambda x: -x[1])[:5]:
            print(f"      {cat}: {count:,}")
    if len(sellers) > 1:
        print(f"    Sellers:         {len(sellers)}")
    if skipped_duplicate:
        print(f"    Skipped (dupe):  {skipped_duplicate:,}")
    if skipped_empty:
        print(f"    Skipped (empty): {skipped_empty:,}")
    print()

    logger.info(
        "Amazon import complete: %d new, %d duplicate, %d empty",
        len(new_entries), skipped_duplicate, skipped_empty,
    )
