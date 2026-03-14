"""
Alias extraction — discovers the user's identities across vault data.
Scans Gmail, Contacts, and config for email addresses, names, and nicknames.
"""

import json
import os
import re
import time
import logging
from collections import Counter

from core.vault import read_all_entries

logger = logging.getLogger("nomolo.aliases")

_EMAIL_RE = re.compile(r"[\w.\-+]+@[\w.\-]+\.\w+")
_CACHE_FILENAME = ".nomolo_aliases.json"
_CACHE_MAX_AGE_SECS = 3600  # 1 hour


def load_cached_aliases(vault_root: str) -> dict | None:
    """Load cached aliases if fresh (< 1 hour old). Public API."""
    return _load_cache(vault_root)


def save_cached_aliases(vault_root: str, data: dict):
    """Save alias data to cache file. Public API."""
    _save_cache(vault_root, data)


def extract_user_aliases(vault_root: str, user_name: str | None = None) -> dict:
    """
    Scan vault directories for the user's own identities.

    Returns dict with primary_name, primary_email, and aliases list.
    Each alias has type, value, source, and optionally usage_count.
    """
    # --- Check cache first ---
    cached = _load_cache(vault_root)
    if cached is not None:
        return cached

    aliases = []

    # --- 1. Gmail: find user emails from SENT messages ---
    gmail_dir = os.path.join(vault_root, "Gmail_Primary")
    if os.path.isdir(gmail_dir):
        aliases.extend(_scan_gmail(gmail_dir))

    # --- 2. Read config.yaml for user_name (if not provided) ---
    if not user_name:
        user_name = _read_config_user_name()

    # --- 3. Contacts: find user's own card ---
    contacts_dir = os.path.join(vault_root, "Contacts")
    if os.path.isdir(contacts_dir):
        # Collect known user emails from Gmail scan for matching
        known_emails = {a["value"] for a in aliases if a["type"] == "email"}
        aliases.extend(_scan_contacts(contacts_dir, known_emails, user_name))

    # --- 4. Google Contacts ---
    google_contacts_dir = os.path.join(vault_root, "Contacts_Google")
    if os.path.isdir(google_contacts_dir):
        known_emails = {a["value"] for a in aliases if a["type"] == "email"}
        aliases.extend(_scan_contacts(google_contacts_dir, known_emails, user_name))

    # --- 5. Config fallback: include user_name as a name alias ---
    if user_name:
        aliases.append({
            "type": "name",
            "value": user_name,
            "source": "Config",
            "usage_count": 0,
        })

    # Ensure every alias has usage_count (default 0)
    for a in aliases:
        a.setdefault("usage_count", 0)

    # Deduplicate by (type, value) — keep highest usage_count
    aliases = _deduplicate(aliases)

    # Sort: emails first by usage_count desc, then names, then nicknames
    type_order = {"email": 0, "name": 1, "nickname": 2}
    aliases.sort(key=lambda a: (
        type_order.get(a["type"], 99),
        -a.get("usage_count", 0),
        a["value"],
    ))

    # Determine primaries — prefer name with highest usage count
    primary_email = None
    primary_name = None
    for a in aliases:
        if a["type"] == "email" and primary_email is None:
            primary_email = a["value"]
        if a["type"] == "name" and primary_name is None:
            # Prefer high-usage name over config name
            if a.get("usage_count", 0) > 0:
                primary_name = a["value"]
    if not primary_name:
        primary_name = user_name

    result = {
        "primary_name": primary_name,
        "primary_email": primary_email,
        "aliases": aliases,
    }

    _save_cache(vault_root, result)
    return result


# ── Gmail scanning ────────────────────────────────────────────────────────

def _scan_gmail(gmail_dir):
    """Scan Gmail JSONL entries for the user's own email addresses and names.

    Strategy: the user's email appears most often as a TO recipient
    (they receive mail to their own address). Also extract names from
    the to_list entries matching the primary email.
    """
    to_counter = Counter()  # email → count as recipient
    to_names = {}           # email → Counter of display names
    reply_to_emails = set()

    for entry in read_all_entries(gmail_dir):
        if not isinstance(entry, dict):
            continue

        # Count TO recipients — user's own email is the most frequent
        to_list = entry.get("to_list", [])
        if isinstance(to_list, list):
            for addr in to_list:
                if isinstance(addr, dict):
                    email = addr.get("email", "")
                    name = addr.get("name", "")
                elif isinstance(addr, str):
                    found = _EMAIL_RE.findall(addr)
                    email = found[0] if found else ""
                    name = ""
                else:
                    continue
                if email:
                    email = email.lower()
                    to_counter[email] += 1
                    if name and name.strip():
                        to_names.setdefault(email, Counter())[name.strip()] += 1

        # Fallback: parse "to" string if to_list is empty
        if not to_list:
            to_val = entry.get("to", "")
            if isinstance(to_val, str):
                for email in _EMAIL_RE.findall(to_val):
                    to_counter[email.lower()] += 1

        # Collect reply_to emails
        reply_to = entry.get("reply_to", "")
        if isinstance(reply_to, str):
            for rt_email in _EMAIL_RE.findall(reply_to):
                reply_to_emails.add(rt_email.lower())

    if not to_counter:
        return []

    # The user's email(s) dominate the TO field — pick the top ones
    # that appear in >10% of messages
    total = sum(to_counter.values())
    threshold = max(total * 0.05, 50)  # at least 50 appearances

    aliases = []
    for email, count in to_counter.most_common():
        if count >= threshold:
            aliases.append({
                "type": "email",
                "value": email,
                "source": "Gmail",
                "usage_count": count,
            })
        else:
            break

    # Extract names the user goes by from to_list entries.
    # to_names maps email → Counter of display names. Only keep names
    # that appear frequently (they're how the user is addressed).
    user_emails = {a["value"] for a in aliases}
    name_counts = Counter()
    for email in user_emails:
        name_counts.update(to_names.get(email, Counter()))

    for name, count in name_counts.most_common():
        if count < 20:
            break  # Noise threshold — must appear in 20+ emails
        # Skip email-like or very short
        if "@" in name or len(name) < 3:
            continue
        # Skip names with digits (customer IDs)
        if any(c.isdigit() for c in name):
            continue
        # Skip names with >3 words (event titles)
        if len(name.split()) > 3:
            continue
        aliases.append({
            "type": "name",
            "value": name,
            "source": "Gmail",
            "usage_count": count,
        })

    # Add reply_to if they match user emails
    for email in reply_to_emails:
        if email in user_emails and email not in {a["value"] for a in aliases if a["type"] == "email"}:
            aliases.append({
                "type": "email",
                "value": email,
                "source": "Gmail",
                "usage_count": 0,
            })

    return aliases


# ── Contacts scanning ─────────────────────────────────────────────────────

def _scan_contacts(contacts_dir, known_user_emails, user_name=None):
    """Scan Contacts JSONL for the user's own card. Extract names/nicknames."""
    aliases = []
    source = "Google Contacts" if "Google" in contacts_dir else "Contacts"

    for entry in read_all_entries(contacts_dir):
        if not isinstance(entry, dict):
            continue

        if not _is_user_card(entry, known_user_emails, user_name):
            continue

        # Extract names
        for key in ("name", "displayName", "display_name", "full_name"):
            val = entry.get(key)
            if isinstance(val, str) and val.strip():
                aliases.append({"type": "name", "value": val.strip(), "source": source})

        names = entry.get("names", [])
        if isinstance(names, list):
            for nm in names:
                display = nm.get("displayName", nm) if isinstance(nm, dict) else nm
                if isinstance(display, str) and display.strip():
                    aliases.append({"type": "name", "value": display.strip(), "source": source})

        # Extract nicknames
        nicknames = entry.get("nicknames", [])
        if isinstance(nicknames, list):
            for nn in nicknames:
                val = nn.get("value", nn) if isinstance(nn, dict) else nn
                if isinstance(val, str) and val.strip():
                    aliases.append({"type": "nickname", "value": val.strip(), "source": source})

        nickname = entry.get("nickname")
        if isinstance(nickname, str) and nickname.strip():
            aliases.append({"type": "nickname", "value": nickname.strip(), "source": source})

        # Extract emails from contact card
        emails = entry.get("emails", [])
        if isinstance(emails, list):
            for em in emails:
                val = em.get("value", em) if isinstance(em, dict) else em
                if isinstance(val, str) and _EMAIL_RE.match(val):
                    aliases.append({"type": "email", "value": val.lower(), "source": source})

    return aliases


def _is_user_card(contact, known_emails, user_name):
    """Check if a contact record is likely the user's own card."""
    # Explicit own-card markers
    if contact.get("type") == "own_card":
        return True
    if contact.get("is_me") or contact.get("isMe"):
        return True

    # Google profile source marker
    metadata = contact.get("metadata", {})
    if isinstance(metadata, dict):
        sources = metadata.get("sources", [])
        if isinstance(sources, list):
            for src in sources:
                if isinstance(src, dict) and src.get("type") == "PROFILE":
                    return True

    # Match by email
    emails = contact.get("emails", [])
    if isinstance(emails, list):
        for em in emails:
            val = em.get("value", em) if isinstance(em, dict) else em
            if isinstance(val, str) and val.lower() in known_emails:
                return True

    # Match by name
    if user_name:
        name_lower = user_name.lower()
        for key in ("name", "displayName", "display_name", "full_name"):
            val = contact.get(key, "")
            if isinstance(val, str) and name_lower in val.lower():
                return True

    return False


# ── Config ─────────────────────────────────────────────────────────────────

def _read_config_user_name():
    """Read user_name from config.yaml in project root."""
    config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config.yaml")
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            # Simple YAML parsing for 'user_name: value' — no PyYAML dependency
            for line in f:
                line = line.strip()
                if line.startswith("user_name:"):
                    return line.split(":", 1)[1].strip()
    except (OSError, PermissionError):
        pass
    return None


# ── Cache ──────────────────────────────────────────────────────────────────

def _load_cache(vault_root):
    """Load cached aliases if fresh (< 1 hour old)."""
    cache_path = os.path.join(vault_root, _CACHE_FILENAME)
    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        cached_at = data.get("cached_at", 0)
        if time.time() - cached_at < _CACHE_MAX_AGE_SECS:
            logger.debug("Using cached aliases from %s", cache_path)
            return data
    except (FileNotFoundError, json.JSONDecodeError, OSError, KeyError, TypeError):
        pass
    return None


def _save_cache(vault_root, data):
    """Save alias data to cache file with timestamp."""
    cache_path = os.path.join(vault_root, _CACHE_FILENAME)
    data["cached_at"] = time.time()
    try:
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except (OSError, PermissionError) as e:
        logger.warning("Could not save alias cache: %s", e)


# ── Helpers ────────────────────────────────────────────────────────────────

def _deduplicate(aliases):
    """Deduplicate by (type, lowercase value). Keep highest usage_count."""
    seen = {}
    for alias in aliases:
        key = (alias["type"], alias["value"].lower())
        existing = seen.get(key)
        if existing is None or alias.get("usage_count", 0) > existing.get("usage_count", 0):
            seen[key] = alias
    return list(seen.values())


if __name__ == "__main__":
    import sys
    result = extract_user_aliases(sys.argv[1] if len(sys.argv) > 1 else "vaults")
    print(json.dumps(result, indent=2))
