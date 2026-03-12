"""
WHID RAG Cleaner
Post-processing pass that enriches vault entries with RAG-optimized fields:
- body_clean (stripped of quotes, signatures, forwarded blocks)
- body_for_embedding (contextual text for vector search)
- Parsed contacts, entities, thread metadata, automation detection
"""

import os
import re
import sys
import json
import time
import logging
from datetime import datetime, timezone
from email.utils import parseaddr, getaddresses

from core.vault import read_entries_by_file, rewrite_file_entries

logger = logging.getLogger("whid.cleaner")


# ---------------------------------------------------------------------------
# Contact parsing
# ---------------------------------------------------------------------------

def parse_contact(addr_string):
    """Parse 'John Doe <john@ex.com>' into (name, email).
    Handles: bare emails, quoted names, 'Last, First' format.
    """
    if not addr_string:
        return ("", "")
    name, email = parseaddr(addr_string)
    return (name.strip(), email.strip().lower())


def parse_contact_list(addr_string):
    """Parse comma-separated address list into [{name, email}, ...]."""
    if not addr_string:
        return []
    pairs = getaddresses([addr_string])
    return [{"name": n.strip(), "email": e.strip().lower()} for n, e in pairs if e]


# ---------------------------------------------------------------------------
# Quote / signature stripping
# ---------------------------------------------------------------------------

# "On ... wrote:" patterns (Gmail, Apple Mail, Thunderbird)
_RE_ON_WROTE = re.compile(
    r"^On\s+.{10,80}\s+wrote:\s*$", re.IGNORECASE
)

# Outlook-style "From: ... Sent: ... To: ... Subject: ..." block
_RE_OUTLOOK_HEADER = re.compile(
    r"^-*\s*From:\s+.+", re.IGNORECASE
)

# Forwarded message markers
_RE_FORWARDED = re.compile(
    r"^-{3,}\s*(Forwarded message|Original Message)\s*-{3,}",
    re.IGNORECASE,
)

# Apple Mail forwarded
_RE_APPLE_FWD = re.compile(
    r"^Begin forwarded message:\s*$", re.IGNORECASE
)

# Signature separator (RFC 3676: "-- " on its own line)
_RE_SIG_SEPARATOR = re.compile(r"^--\s*$")


def strip_quotes_and_signatures(body):
    """Remove quoted replies, forwarded blocks, and signatures from email body.
    Returns only the new content the sender actually wrote.
    """
    if not body:
        return ""

    lines = body.split("\n")
    result_lines = []
    i = 0

    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        # Stop at signature separator
        if _RE_SIG_SEPARATOR.match(stripped):
            break

        # Stop at "On ... wrote:" (Gmail/Thunderbird quote intro)
        if _RE_ON_WROTE.match(stripped):
            break

        # Stop at Outlook-style reply header block
        if _RE_OUTLOOK_HEADER.match(stripped):
            # Verify it's a real header block (has Sent: nearby)
            lookahead = "\n".join(lines[i:i + 5])
            if re.search(r"Sent:", lookahead, re.IGNORECASE):
                break

        # Stop at forwarded message markers
        if _RE_FORWARDED.match(stripped):
            break

        # Stop at Apple Mail forwarded
        if _RE_APPLE_FWD.match(stripped):
            break

        # Skip individual quoted lines (> prefix)
        if stripped.startswith(">"):
            i += 1
            continue

        result_lines.append(line)
        i += 1

    return "\n".join(result_lines).strip()


# ---------------------------------------------------------------------------
# Automation / newsletter detection
# ---------------------------------------------------------------------------

_AUTOMATED_FROM_PATTERNS = [
    r"noreply@", r"no-reply@", r"no_reply@",
    r"donotreply@", r"do-not-reply@",
    r"mailer-daemon@", r"postmaster@",
    r"notifications?@", r"alerts?@", r"updates?@",
    r"bounce[s]?@", r"auto@",
]

_AUTOMATED_FROM_RE = re.compile(
    "|".join(_AUTOMATED_FROM_PATTERNS), re.IGNORECASE
)


def detect_automated(entry):
    """Heuristic: is this an automated/bulk email?"""
    from_addr = entry.get("from", "").lower()
    if _AUTOMATED_FROM_RE.search(from_addr):
        return True

    if entry.get("list_unsubscribe"):
        return True

    tags = entry.get("tags", [])
    automated_labels = {
        "CATEGORY_PROMOTIONS", "CATEGORY_UPDATES",
        "CATEGORY_FORUMS", "CATEGORY_SOCIAL",
    }
    if automated_labels.intersection(tags):
        return True

    return False


# ---------------------------------------------------------------------------
# Entity extraction
# ---------------------------------------------------------------------------

_RE_URL = re.compile(r"https?://[^\s<>\"'\])+]+")
_RE_EMAIL = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
_RE_AMOUNT = re.compile(
    r"[$€£]\s?\d[\d,]*\.?\d*|\d[\d,]*\.?\d*\s?(?:USD|EUR|GBP|CHF)"
)
_RE_PHONE = re.compile(r"\+?\d[\d\s\-()/\.]{7,}\d")


def extract_entities(text):
    """Extract URLs, email addresses, monetary amounts, phone numbers."""
    if not text:
        return {"urls": [], "emails_mentioned": [], "amounts": [], "phone_numbers": []}

    return {
        "urls": _RE_URL.findall(text),
        "emails_mentioned": list(set(_RE_EMAIL.findall(text))),
        "amounts": _RE_AMOUNT.findall(text),
        "phone_numbers": _RE_PHONE.findall(text),
    }


# ---------------------------------------------------------------------------
# Language detection (simple heuristic)
# ---------------------------------------------------------------------------

_LANG_MARKERS = {
    "fr": {"le", "la", "les", "de", "des", "du", "un", "une", "est", "dans", "pour", "que", "qui", "nous", "vous"},
    "de": {"der", "die", "das", "und", "ist", "ein", "eine", "nicht", "auf", "mit", "ich", "wir", "sie", "haben"},
    "es": {"el", "la", "los", "las", "de", "en", "es", "por", "para", "con", "que", "una", "como", "pero"},
    "nl": {"de", "het", "een", "van", "en", "is", "dat", "voor", "met", "op", "niet", "zijn", "naar", "ook"},
    "pt": {"o", "a", "os", "as", "de", "em", "um", "uma", "para", "com", "que", "como", "mais", "por"},
    "it": {"il", "la", "le", "di", "in", "un", "una", "che", "per", "con", "sono", "come", "non", "anche"},
}


def detect_language(text):
    """Simple heuristic language detection."""
    if not text or len(text) < 20:
        return "unknown"

    sample = text[:500]
    sample_len = max(len(sample), 1)

    # Japanese (Hiragana/Katakana) — check before CJK since kanji overlaps
    jp = sum(1 for c in sample if '\u3040' <= c <= '\u30ff')
    if jp / sample_len > 0.05:
        return "ja"

    # Korean (Hangul)
    ko = sum(1 for c in sample if '\uac00' <= c <= '\ud7af')
    if ko / sample_len > 0.1:
        return "ko"

    # CJK characters (Chinese)
    cjk = sum(1 for c in sample if '\u4e00' <= c <= '\u9fff')
    if cjk / sample_len > 0.1:
        return "zh"

    # Cyrillic
    cyr = sum(1 for c in sample if '\u0400' <= c <= '\u04ff')
    if cyr / sample_len > 0.2:
        return "ru"

    # Arabic
    arab = sum(1 for c in sample if '\u0600' <= c <= '\u06ff')
    if arab / sample_len > 0.2:
        return "ar"

    # European languages by word frequency
    words = re.findall(r"[a-zA-ZÀ-ÿ]+", text.lower()[:1000])
    word_set = set(words[:200])

    scores = {lang: len(word_set & markers) for lang, markers in _LANG_MARKERS.items()}
    best = max(scores, key=scores.get)
    if scores[best] >= 3:
        return best

    return "en"


# ---------------------------------------------------------------------------
# Core entry cleaning
# ---------------------------------------------------------------------------

def clean_entry(entry, thread_index=None):
    """Add all RAG fields to a single entry. Mutates and returns the entry."""
    body_raw = entry.get("body_raw", "")

    # Strip quotes and signatures
    entry["body_clean"] = strip_quotes_and_signatures(body_raw)

    # Parse contacts
    entry["from_name"], entry["from_email"] = parse_contact(entry.get("from", ""))
    entry["to_list"] = parse_contact_list(entry.get("to", ""))
    entry["cc_list"] = parse_contact_list(entry.get("cc", ""))

    # Date fields from internalDate (milliseconds since epoch)
    internal_date = entry.get("internalDate", "")
    if internal_date:
        try:
            dt = datetime.fromtimestamp(int(internal_date) / 1000, tz=timezone.utc)
            entry["year"] = dt.year
            entry["month"] = dt.month
        except (ValueError, OSError, OverflowError):
            _set_year_month_fallback(entry)
    else:
        _set_year_month_fallback(entry)

    # Thread position and depth
    if thread_index and entry.get("threadId"):
        thread = thread_index.get(entry["threadId"], [])
        entry["thread_depth"] = len(thread)
        entry["thread_position"] = _find_thread_position(entry["id"], thread)
    else:
        entry["thread_position"] = 1
        entry["thread_depth"] = 1

    # Automation detection
    entry["is_automated"] = detect_automated(entry)

    # Attachments
    attachments = entry.get("attachments", [])
    entry["has_attachments"] = len(attachments) > 0
    entry["attachment_names"] = [a.get("filename", "") for a in attachments if a.get("filename")]

    # Entity extraction (from clean body)
    entry["entities"] = extract_entities(entry["body_clean"])

    # Stats
    entry["word_count"] = len(entry["body_clean"].split()) if entry["body_clean"] else 0
    entry["lang"] = detect_language(entry["body_clean"])

    # Embedding text — contextual wrapper for vector search
    from_display = entry["from_name"] or entry["from_email"]
    to_names = [c["name"] or c["email"] for c in entry["to_list"][:3]]
    to_display = ", ".join(to_names) if to_names else ""
    date_str = entry.get("date", "")
    subject = entry.get("subject", "")
    entry["body_for_embedding"] = (
        f"From {from_display} to {to_display} on {date_str} "
        f"re: {subject}: {entry['body_clean']}"
    )

    return entry


def _set_year_month_fallback(entry):
    """Set year/month from date header string as fallback."""
    date_str = entry.get("date", "")
    if not date_str:
        entry["year"] = 0
        entry["month"] = 0
        return

    # Try to extract year from common patterns
    year_match = re.search(r"\b(19|20)\d{2}\b", date_str)
    if year_match:
        entry["year"] = int(year_match.group())
    else:
        entry["year"] = 0

    months = {
        "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
        "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    }
    month_match = re.search(r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*\b", date_str, re.IGNORECASE)
    if month_match:
        entry["month"] = months.get(month_match.group()[:3].lower(), 0)
    else:
        entry["month"] = 0


def _find_thread_position(msg_id, thread_entries):
    """Find 1-indexed position of msg_id in a sorted thread."""
    for i, (tid, _) in enumerate(thread_entries, 1):
        if tid == msg_id:
            return i
    return 1


# ---------------------------------------------------------------------------
# Thread index builder
# ---------------------------------------------------------------------------

def build_thread_index(all_entries):
    """Build {threadId: [(msg_id, sort_key), ...]} sorted chronologically.
    Uses internalDate when available, falls back to date header.
    """
    from collections import defaultdict
    threads = defaultdict(list)

    for entry in all_entries:
        thread_id = entry.get("threadId", "")
        if not thread_id:
            continue

        msg_id = entry.get("id", "")
        internal_date = entry.get("internalDate", "")

        if internal_date:
            try:
                sort_key = int(internal_date)
            except ValueError:
                sort_key = 0
        else:
            sort_key = 0

        threads[thread_id].append((msg_id, sort_key))

    # Sort each thread chronologically
    for thread_id in threads:
        threads[thread_id].sort(key=lambda x: x[1])

    return dict(threads)


# ---------------------------------------------------------------------------
# Progress display
# ---------------------------------------------------------------------------

def _print_progress(processed, total, start_time, skipped=0):
    """Print a progress bar with ETA."""
    elapsed = time.time() - start_time
    rate = processed / elapsed if elapsed > 0 else 0

    pct = int(processed / total * 100) if total > 0 else 100
    bar_width = 25
    filled = int(bar_width * processed / total) if total > 0 else bar_width
    bar = "#" * filled + "." * (bar_width - filled)

    eta = ""
    if rate > 0 and processed < total:
        remaining = (total - processed) / rate
        if remaining > 60:
            eta = f" | ~{remaining / 60:.0f}m left"
        else:
            eta = f" | ~{remaining:.0f}s left"

    skip_str = f" | {skipped} skipped" if skipped > 0 else ""

    print(
        f"\r  [{bar}] {pct:3d}% | "
        f"{processed:,}/{total:,} | "
        f"{rate:.0f}/s{skip_str}{eta}   ",
        end="", flush=True,
    )


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_clean(vault_name="Primary", config=None):
    """RAG-optimized cleaning pass for Gmail vault entries."""
    config = config or {}
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    vault_root_base = config.get("vault_root", os.path.join(project_root, "vaults"))
    vault_root_base = os.path.expanduser(vault_root_base)
    if not os.path.isabs(vault_root_base):
        vault_root_base = os.path.join(project_root, vault_root_base)
    vault_path = os.path.join(vault_root_base, f"Gmail_{vault_name}")

    if not os.path.exists(vault_path):
        print(f"\nError: Vault not found: {vault_path}")
        print("Run 'whid collect gmail' first.")
        sys.exit(1)

    print(f"\n  WHID RAG Cleaner")
    print(f"  {'=' * 45}")
    print(f"  Vault: {vault_path}")

    # Phase 1: Load all entries
    print("\n  Phase 1: Loading vault entries...")
    file_entries = read_entries_by_file(vault_path)

    total_entries = sum(len(entries) for entries in file_entries.values())
    total_files = len(file_entries)
    print(f"  Found {total_entries:,} entries across {total_files} files")

    if total_entries == 0:
        print("  Nothing to clean.")
        return

    # Count already-cleaned entries
    already_clean = sum(
        1 for entries in file_entries.values()
        for e in entries if "body_clean" in e
    )
    to_clean = total_entries - already_clean

    if to_clean == 0:
        print("  All entries already cleaned — nothing to do.")
        return

    if already_clean > 0:
        print(f"  Already cleaned: {already_clean:,} (skipping)")
    print(f"  To clean: {to_clean:,}")

    # Phase 2: Build thread index from ALL entries (including already cleaned)
    print("\n  Phase 2: Building thread index...")
    all_entries = [e for entries in file_entries.values() for e in entries]
    thread_index = build_thread_index(all_entries)
    unique_threads = len(thread_index)
    print(f"  {unique_threads:,} threads indexed")
    del all_entries  # free memory

    # Phase 3: Clean entries file by file
    print(f"\n  Phase 3: Cleaning entries...")
    start_time = time.time()
    processed = 0
    cleaned = 0
    skipped = 0
    errors = 0
    files_written = 0

    for file_path, entries in file_entries.items():
        file_changed = False

        for entry in entries:
            processed += 1

            if "body_clean" in entry:
                skipped += 1
                if processed % 500 == 0:
                    _print_progress(processed, total_entries, start_time, skipped)
                continue

            try:
                clean_entry(entry, thread_index)
                cleaned += 1
                file_changed = True
            except Exception as e:
                logger.error(
                    "Error cleaning entry %s: %s", entry.get("id", "unknown"), e
                )
                errors += 1

            if processed % 500 == 0:
                _print_progress(processed, total_entries, start_time, skipped)

        if file_changed:
            try:
                rewrite_file_entries(file_path, entries)
                files_written += 1
            except (PermissionError, OSError) as e:
                logger.error("Failed to write %s: %s", file_path, e)
                errors += 1

    _print_progress(processed, total_entries, start_time, skipped)
    print()  # newline after progress bar

    elapsed = time.time() - start_time

    print(f"\n  {'=' * 45}")
    print(f"  Done! Cleaned {cleaned:,} entries in {elapsed:.1f}s")
    print(f"  Files rewritten: {files_written}")
    if skipped > 0:
        print(f"  Skipped (already clean): {skipped:,}")
    if errors > 0:
        print(f"  Errors: {errors} (check logs)")
    print(f"  {'=' * 45}")

    # Summary of what was added
    print(f"\n  New fields per entry:")
    print(f"    body_clean         — reply quotes and signatures stripped")
    print(f"    body_for_embedding — contextual text for vector search")
    print(f"    from_name/email    — parsed sender")
    print(f"    to_list/cc_list    — parsed recipients")
    print(f"    year/month         — numeric date fields for filtering")
    print(f"    thread_position    — position within conversation")
    print(f"    thread_depth       — total messages in thread")
    print(f"    is_automated       — newsletter/notification flag")
    print(f"    entities           — URLs, emails, amounts, phones")
    print(f"    word_count/lang    — text stats")
    print()
