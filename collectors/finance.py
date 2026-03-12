"""
WHID Finance Collector
Parses financial transactions from PayPal CSV export and generic bank CSV
into the unified Finance JSONL vault.

Supports two import modes:
  1. PayPal CSV:  whid collect finance-paypal ~/Downloads/paypal_export.csv
  2. Bank CSV:    whid collect finance-bank ~/Downloads/bank_export.csv --bank deutschebank

Both write to the Finance/ vault directory with unified schema.
"""

import csv
import hashlib
import logging
import os
from datetime import datetime

from core.vault import flush_entries, load_processed_ids, append_processed_ids

logger = logging.getLogger("whid.finance")


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
    return f"finance:{source}:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]


def _safe_float(value, default=0.0):
    """Convert a string to float, returning default on failure."""
    if not value:
        return default
    # Handle comma-as-decimal and currency symbols
    cleaned = value.replace(",", ".").strip()
    cleaned = cleaned.replace("€", "").replace("$", "").replace("£", "").strip()
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return default


def _parse_date(date_str):
    """
    Parse a date string into YYYY-MM-DD format.
    Handles common date formats from financial exports.
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
        "%m-%d-%Y",
        "%d-%m-%Y",
    ):
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue

    return date_str.strip()


# ---------------------------------------------------------------------------
# PayPal parser
# ---------------------------------------------------------------------------

def _parse_paypal_row(row, col_map):
    """
    Convert a single PayPal CSV row to a vault entry dict.
    Returns None if the row lacks essential data.
    """
    date_raw = _get(row, col_map, "date")
    if not date_raw:
        return None

    date = _parse_date(date_raw)
    time = _get(row, col_map, "time")
    name = _get(row, col_map, "name", "counterparty")
    txn_type = _get(row, col_map, "type")
    status = _get(row, col_map, "status")
    currency = _get(row, col_map, "currency")
    gross = _safe_float(_get(row, col_map, "gross", "amount"))
    fee = _safe_float(_get(row, col_map, "fee"))
    net = _safe_float(_get(row, col_map, "net"))
    from_email = _get(row, col_map, "from email address", "from email")
    to_email = _get(row, col_map, "to email address", "to email")
    txn_id = _get(row, col_map, "transaction id")
    item_title = _get(row, col_map, "item title", "subject")
    balance = _safe_float(_get(row, col_map, "balance"))

    description = item_title or txn_type or ""
    counterparty_email = to_email or from_email

    # Parse year/month from normalized date
    year, month = 0, 0
    if date and len(date) >= 7:
        try:
            dt = datetime.strptime(date, "%Y-%m-%d")
            year = dt.year
            month = dt.month
        except ValueError:
            pass

    entry_id = _make_id("paypal", txn_id or f"{date}:{time}:{gross}:{name}")

    # Build embedding text
    embedding = f"{date} — Payment of {gross} {currency} to {name}"
    if description:
        embedding += f" — {description}"

    return {
        "id": entry_id,
        "sources": ["paypal"],
        "date": date,
        "time": time,
        "description": description,
        "amount": gross,
        "currency": currency,
        "fee": fee,
        "balance": balance,
        "counterparty": name,
        "counterparty_email": counterparty_email,
        "transaction_id": txn_id,
        "type": txn_type.lower() if txn_type else "",
        "status": status.lower() if status else "",
        "category": "",
        "year": year,
        "month": month,
        "updated_at": datetime.now().isoformat(),
        "transaction_for_embedding": embedding,
    }


# ---------------------------------------------------------------------------
# Bank parser
# ---------------------------------------------------------------------------

def _parse_bank_row(row, col_map, bank_name="bank"):
    """
    Convert a single generic bank CSV row to a vault entry dict.
    Returns None if the row lacks essential data.
    """
    date_raw = _get(row, col_map, "date", "booking date", "buchungstag", "valuta")
    if not date_raw:
        return None

    date = _parse_date(date_raw)
    description = _get(
        row, col_map,
        "description", "payee", "verwendungszweck", "buchungstext",
        "memo", "reference", "details",
    )
    amount = _safe_float(_get(row, col_map, "amount", "betrag", "value"))
    balance = _safe_float(_get(row, col_map, "balance", "saldo"))
    currency = _get(row, col_map, "currency", "währung", default="EUR")
    category = _get(row, col_map, "category", "kategorie")
    counterparty = _get(
        row, col_map,
        "payee", "counterparty", "empfänger", "auftraggeber",
        "beguenstigter/zahlungspflichtiger",
    )

    # If no separate counterparty, try to use description
    if not counterparty:
        counterparty = description

    # Parse year/month from normalized date
    year, month = 0, 0
    if date and len(date) >= 7:
        try:
            dt = datetime.strptime(date, "%Y-%m-%d")
            year = dt.year
            month = dt.month
        except ValueError:
            pass

    entry_id = _make_id("bank", f"{bank_name}:{date}:{amount}:{description}")

    # Build embedding text
    embedding = f"{date} — Payment of {amount} {currency} to {counterparty}"
    if description and description != counterparty:
        embedding += f" — {description}"

    return {
        "id": entry_id,
        "sources": [f"bank-{bank_name}"],
        "date": date,
        "time": "",
        "description": description,
        "amount": amount,
        "currency": currency,
        "fee": 0.0,
        "balance": balance,
        "counterparty": counterparty,
        "counterparty_email": "",
        "transaction_id": "",
        "type": "",
        "status": "completed",
        "category": category,
        "year": year,
        "month": month,
        "updated_at": datetime.now().isoformat(),
        "transaction_for_embedding": embedding,
    }


# ---------------------------------------------------------------------------
# Main entry points
# ---------------------------------------------------------------------------

def run_import_paypal(export_path, config=None):
    """
    Import transactions from a PayPal CSV export into the vault.

    Args:
        export_path: Path to the PayPal CSV export file.
        config: Dict with optional 'vault_root' key.
    """
    config = config or {}
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    vault_root_base = config.get("vault_root", os.path.join(project_root, "vaults"))
    vault_root_base = os.path.expanduser(vault_root_base)
    if not os.path.isabs(vault_root_base):
        vault_root_base = os.path.join(project_root, vault_root_base)
    vault_path = os.path.join(vault_root_base, "Finance")

    print(f"\n  WHID Finance Collector — PayPal")
    print(f"  {'=' * 45}")
    print(f"  CSV: {export_path}")
    print(f"  Vault: {vault_path}")

    # Read CSV
    col_map, rows = _read_csv(export_path)
    if col_map is None:
        return

    print(f"  Transactions found: {len(rows)}")

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
            entry = _parse_paypal_row(row, col_map)
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
    flush_entries(new_entries, vault_path, "finance.jsonl")
    new_ids = [e["id"] for e in new_entries]
    append_processed_ids(vault_path, new_ids)

    # Summary stats
    total_in = sum(e["amount"] for e in new_entries if e["amount"] > 0)
    total_out = sum(e["amount"] for e in new_entries if e["amount"] < 0)
    total_fees = sum(e["fee"] for e in new_entries if e["fee"])
    currencies = set(e["currency"] for e in new_entries if e["currency"])

    print()
    print(f"  {'=' * 45}")
    print(f"  Done! {len(new_entries):,} transactions saved")
    print(f"  {'=' * 45}")
    print(f"    Income:          {total_in:,.2f}")
    print(f"    Spending:        {total_out:,.2f}")
    print(f"    Fees:            {total_fees:,.2f}")
    print(f"    Currencies:      {', '.join(sorted(currencies))}")
    if skipped_duplicate:
        print(f"    Skipped (dupe):  {skipped_duplicate:,}")
    if skipped_empty:
        print(f"    Skipped (empty): {skipped_empty:,}")
    print()

    logger.info(
        "PayPal import complete: %d new, %d duplicate, %d empty",
        len(new_entries), skipped_duplicate, skipped_empty,
    )


def run_import_bank(export_path, config=None, bank_name="bank"):
    """
    Import transactions from a generic bank CSV export into the vault.

    Args:
        export_path: Path to the bank CSV export file.
        config: Dict with optional 'vault_root' key.
        bank_name: Name of the bank, used in source tag (e.g. 'deutschebank').
    """
    config = config or {}
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    vault_root_base = config.get("vault_root", os.path.join(project_root, "vaults"))
    vault_root_base = os.path.expanduser(vault_root_base)
    if not os.path.isabs(vault_root_base):
        vault_root_base = os.path.join(project_root, vault_root_base)
    vault_path = os.path.join(vault_root_base, "Finance")

    print(f"\n  WHID Finance Collector — Bank ({bank_name})")
    print(f"  {'=' * 45}")
    print(f"  CSV: {export_path}")
    print(f"  Vault: {vault_path}")

    # Read CSV
    col_map, rows = _read_csv(export_path)
    if col_map is None:
        return

    print(f"  Transactions found: {len(rows)}")

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
            entry = _parse_bank_row(row, col_map, bank_name=bank_name)
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
    flush_entries(new_entries, vault_path, "finance.jsonl")
    new_ids = [e["id"] for e in new_entries]
    append_processed_ids(vault_path, new_ids)

    # Summary stats
    total_in = sum(e["amount"] for e in new_entries if e["amount"] > 0)
    total_out = sum(e["amount"] for e in new_entries if e["amount"] < 0)
    currencies = set(e["currency"] for e in new_entries if e["currency"])

    print()
    print(f"  {'=' * 45}")
    print(f"  Done! {len(new_entries):,} transactions saved")
    print(f"  {'=' * 45}")
    print(f"    Income:          {total_in:,.2f}")
    print(f"    Spending:        {total_out:,.2f}")
    print(f"    Currencies:      {', '.join(sorted(currencies))}")
    if skipped_duplicate:
        print(f"    Skipped (dupe):  {skipped_duplicate:,}")
    if skipped_empty:
        print(f"    Skipped (empty): {skipped_empty:,}")
    print()

    logger.info(
        "Bank (%s) import complete: %d new, %d duplicate, %d empty",
        bank_name, len(new_entries), skipped_duplicate, skipped_empty,
    )
