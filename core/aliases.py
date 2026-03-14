"""
Alias extraction — discovers the user's identities across platforms.
Scans vault data for email addresses, names, nicknames, and usernames.
"""

import json
import os
import re
import logging
from collections import Counter
from datetime import datetime, timezone

logger = logging.getLogger("nomolo.aliases")

# Regex for email extraction
_EMAIL_RE = re.compile(r'[\w\.-]+@[\w\.-]+\.\w+')

# Map vault directory names to Flatcloud villain IDs
VAULT_TO_VILLAIN = {
    "Gmail_Primary": "omniscient_eye",
    "Contacts_Google": "omniscient_eye",
    "Contacts": "walled_garden",
    "Messages": "walled_garden",
    "WhatsApp": "hydra_of_faces",
    "LinkedIn": "professional_masque",
    "Twitter": "chaos_herald",
    "Telegram": "shadow_courier",
    "Slack": "corporate_hive",
}

# Map vault directory names to human-readable source names
VAULT_TO_SOURCE = {
    "Gmail_Primary": "Gmail",
    "Contacts_Google": "Google Contacts",
    "Contacts": "Contacts",
    "Messages": "iMessage",
    "WhatsApp": "WhatsApp",
    "LinkedIn": "LinkedIn",
    "Twitter": "Twitter",
    "Telegram": "Telegram",
    "Slack": "Slack",
}

# Max files to scan per vault directory (performance guard)
_MAX_FILES_PER_VAULT = 1000

# Cache file name
_CACHE_FILENAME = ".nomolo_aliases.json"


def extract_user_aliases(vault_root, user_name=None):
    """
    Scan vault directories for the user's own identities.

    Args:
        vault_root: Path to the vaults directory.
        user_name: Optional known user name to match against contacts.

    Returns dict with:
        primary_name: str or None
        primary_email: str or None
        aliases: list of dicts with type, value, source, villain, usage_count
    """
    aliases = []

    # --- 1. Gmail vault: find user's primary email from "from" fields ---
    gmail_dir = os.path.join(vault_root, "Gmail_Primary")
    if os.path.isdir(gmail_dir):
        aliases.extend(_scan_gmail(gmail_dir))

    # --- 2. Google Contacts: find user's own card ---
    google_contacts_dir = os.path.join(vault_root, "Contacts_Google")
    if os.path.isdir(google_contacts_dir):
        aliases.extend(_scan_contacts(google_contacts_dir, "Contacts_Google", user_name))

    # --- 3. Local Contacts (macOS "Me" card) ---
    contacts_dir = os.path.join(vault_root, "Contacts")
    if os.path.isdir(contacts_dir):
        aliases.extend(_scan_contacts(contacts_dir, "Contacts", user_name))

    # --- 4. Messages: frequent sender is likely the user ---
    messages_dir = os.path.join(vault_root, "Messages")
    if os.path.isdir(messages_dir):
        aliases.extend(_scan_messages(messages_dir))

    # --- 5. Config fallback: always include user_name if provided ---
    if user_name:
        aliases.append({
            "type": "name",
            "value": user_name,
            "source": "Config",
            "villain": None,
            "usage_count": 0,
        })

    # Deduplicate by (type, value) — keep the one with highest usage_count
    aliases = _deduplicate(aliases)

    # Sort: emails first (by usage_count desc), then names, then nicknames
    type_order = {"email": 0, "name": 1, "nickname": 2, "organization": 3}
    aliases.sort(key=lambda a: (type_order.get(a["type"], 99), -a["usage_count"], a["value"]))

    # Determine primary identity
    primary_email = None
    primary_name = None

    for a in aliases:
        if a["type"] == "email" and primary_email is None:
            primary_email = a["value"]
        if a["type"] == "name" and primary_name is None:
            primary_name = a["value"]

    if primary_name is None and user_name:
        primary_name = user_name

    return {
        "primary_name": primary_name,
        "primary_email": primary_email,
        "aliases": aliases,
    }


# ── Gmail scanning ────────────────────────────────────────────────────────

def _scan_gmail(gmail_dir):
    """Scan Gmail JSON files for sender emails. Most common 'from' is the user."""
    from_counter = Counter()
    reply_to_counter = Counter()
    files_scanned = 0

    for file_path in _walk_json_files(gmail_dir):
        if files_scanned >= _MAX_FILES_PER_VAULT:
            break
        files_scanned += 1

        try:
            data = _read_json_file(file_path)
            if data is None:
                continue

            entries = data if isinstance(data, list) else [data]
            for entry in entries:
                if not isinstance(entry, dict):
                    continue

                # Extract from field
                from_val = entry.get("from", "")
                if isinstance(from_val, str):
                    for email in _EMAIL_RE.findall(from_val):
                        from_counter[email.lower()] += 1

                # Extract reply_to field
                reply_to = entry.get("reply_to", "")
                if isinstance(reply_to, str):
                    for email in _EMAIL_RE.findall(reply_to):
                        reply_to_counter[email.lower()] += 1

        except Exception:
            continue

    aliases = []
    source = VAULT_TO_SOURCE["Gmail_Primary"]
    villain = VAULT_TO_VILLAIN["Gmail_Primary"]

    # The most common "from" email is likely the user's
    for email, count in from_counter.most_common(5):
        aliases.append({
            "type": "email",
            "value": email,
            "source": source,
            "villain": villain,
            "usage_count": count,
        })

    # Also check reply_to for additional email identities
    for email, count in reply_to_counter.most_common(3):
        if not any(a["value"] == email for a in aliases):
            aliases.append({
                "type": "email",
                "value": email,
                "source": source,
                "villain": villain,
                "usage_count": count,
            })

    return aliases


# ── Contacts scanning ─────────────────────────────────────────────────────

def _scan_contacts(contacts_dir, vault_key, user_name=None):
    """Scan contact JSON files for emails, names, nicknames."""
    aliases = []
    source = VAULT_TO_SOURCE.get(vault_key, vault_key)
    villain = VAULT_TO_VILLAIN.get(vault_key)
    files_scanned = 0

    for file_path in _walk_json_files(contacts_dir):
        if files_scanned >= _MAX_FILES_PER_VAULT:
            break
        files_scanned += 1

        try:
            data = _read_json_file(file_path)
            if data is None:
                continue

            contacts = data if isinstance(data, list) else [data]
            for contact in contacts:
                if not isinstance(contact, dict):
                    continue

                # Check if this is the user's own card
                is_user_card = _is_user_card(contact, user_name)
                if not is_user_card and user_name:
                    continue

                # Extract emails
                emails = contact.get("emails", [])
                if isinstance(emails, list):
                    for em in emails:
                        val = em.get("value", em) if isinstance(em, dict) else em
                        if isinstance(val, str) and _EMAIL_RE.match(val):
                            aliases.append({
                                "type": "email",
                                "value": val.lower(),
                                "source": source,
                                "villain": villain,
                                "usage_count": 0,
                            })

                # Extract names
                names = contact.get("names", [])
                if isinstance(names, list):
                    for nm in names:
                        display = nm.get("displayName", nm) if isinstance(nm, dict) else nm
                        if isinstance(display, str) and display.strip():
                            aliases.append({
                                "type": "name",
                                "value": display.strip(),
                                "source": source,
                                "villain": villain,
                                "usage_count": 0,
                            })
                elif isinstance(names, str) and names.strip():
                    aliases.append({
                        "type": "name",
                        "value": names.strip(),
                        "source": source,
                        "villain": villain,
                        "usage_count": 0,
                    })

                # Check for name in top-level fields
                for name_key in ("name", "displayName", "display_name", "full_name"):
                    name_val = contact.get(name_key)
                    if isinstance(name_val, str) and name_val.strip():
                        aliases.append({
                            "type": "name",
                            "value": name_val.strip(),
                            "source": source,
                            "villain": villain,
                            "usage_count": 0,
                        })

                # Extract nicknames
                nicknames = contact.get("nicknames", [])
                if isinstance(nicknames, list):
                    for nn in nicknames:
                        val = nn.get("value", nn) if isinstance(nn, dict) else nn
                        if isinstance(val, str) and val.strip():
                            aliases.append({
                                "type": "nickname",
                                "value": val.strip(),
                                "source": source,
                                "villain": villain,
                                "usage_count": 0,
                            })

                # Extract organizations
                orgs = contact.get("organizations", [])
                if isinstance(orgs, list):
                    for org in orgs:
                        org_name = org.get("name", org) if isinstance(org, dict) else org
                        if isinstance(org_name, str) and org_name.strip():
                            aliases.append({
                                "type": "organization",
                                "value": org_name.strip(),
                                "source": source,
                                "villain": villain,
                                "usage_count": 0,
                            })

        except Exception:
            continue

    return aliases


def _is_user_card(contact, user_name):
    """Check if a contact record is likely the user's own card."""
    if not user_name:
        # Without a user_name, check for "me" card markers
        metadata = contact.get("metadata", {})
        if isinstance(metadata, dict):
            sources = metadata.get("sources", [])
            if isinstance(sources, list):
                for src in sources:
                    if isinstance(src, dict) and src.get("type") == "PROFILE":
                        return True

        # macOS "Me" card detection
        if contact.get("is_me") or contact.get("isMe"):
            return True

        return False

    # Match against user_name (case-insensitive partial match)
    name_lower = user_name.lower()
    for key in ("name", "displayName", "display_name", "full_name"):
        val = contact.get(key, "")
        if isinstance(val, str) and name_lower in val.lower():
            return True

    names = contact.get("names", [])
    if isinstance(names, list):
        for nm in names:
            display = nm.get("displayName", nm) if isinstance(nm, dict) else nm
            if isinstance(display, str) and name_lower in display.lower():
                return True

    return False


# ── Messages scanning ─────────────────────────────────────────────────────

def _scan_messages(messages_dir):
    """Scan Messages for the most frequent sender (likely the user)."""
    sender_counter = Counter()
    files_scanned = 0

    for file_path in _walk_json_files(messages_dir):
        if files_scanned >= _MAX_FILES_PER_VAULT:
            break
        files_scanned += 1

        try:
            data = _read_json_file(file_path)
            if data is None:
                continue

            messages = data if isinstance(data, list) else [data]
            for msg in messages:
                if not isinstance(msg, dict):
                    continue

                # Check "is_from_me" flag (iMessage style)
                if msg.get("is_from_me") or msg.get("isFromMe"):
                    sender = msg.get("sender") or msg.get("handle") or msg.get("from", "")
                    if isinstance(sender, str) and sender.strip():
                        sender_counter[sender.strip()] += 1

        except Exception:
            continue

    aliases = []
    source = VAULT_TO_SOURCE["Messages"]
    villain = VAULT_TO_VILLAIN["Messages"]

    for sender, count in sender_counter.most_common(3):
        alias_type = "email" if _EMAIL_RE.match(sender) else "name"
        aliases.append({
            "type": alias_type,
            "value": sender.lower() if alias_type == "email" else sender,
            "source": source,
            "villain": villain,
            "usage_count": count,
        })

    return aliases


# ── Helpers ───────────────────────────────────────────────────────────────

def _walk_json_files(directory):
    """Walk directory and yield paths to .json files."""
    try:
        for root, _dirs, files in os.walk(directory):
            for f in sorted(files):
                if f.endswith(".json") and not f.startswith("."):
                    yield os.path.join(root, f)
    except (OSError, PermissionError):
        pass


def _read_json_file(file_path):
    """Read and parse a JSON file. Returns None on failure."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError, PermissionError, UnicodeDecodeError):
        return None


def _deduplicate(aliases):
    """Deduplicate aliases by (type, value). Keep highest usage_count."""
    seen = {}
    for alias in aliases:
        key = (alias["type"], alias["value"].lower())
        if key not in seen or alias["usage_count"] > seen[key]["usage_count"]:
            seen[key] = alias
    return list(seen.values())


# ── Cache ─────────────────────────────────────────────────────────────────

def load_cached_aliases(vault_root):
    """Load cached alias data from disk. Returns dict or None."""
    cache_path = os.path.join(vault_root, _CACHE_FILENAME)
    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, OSError, PermissionError):
        return None


def save_cached_aliases(vault_root, data):
    """Save alias data to disk cache with timestamp."""
    cache_path = os.path.join(vault_root, _CACHE_FILENAME)
    data["cached_at"] = datetime.now(timezone.utc).isoformat()
    try:
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except (OSError, PermissionError) as e:
        logger.warning("Could not save alias cache to %s: %s", cache_path, e)
