"""
NOMOLO Instagram Contacts Collector
Parses Instagram data export JSON files (followers/following) into a
local JSONL vault as a flat file.

Supports both old-format and new-format Instagram data exports.
No external dependencies beyond stdlib.
"""

import os
import json
import glob
import logging
from datetime import datetime, timezone

from core.vault import flush_entries, load_processed_ids, append_processed_ids

logger = logging.getLogger("nomolo.instagram_contacts")


def _decode_facebook_text(text):
    """
    Decode Instagram/Facebook export text encoding.

    Instagram (owned by Meta) uses the same broken encoding as Facebook:
    non-ASCII characters are stored as escaped UTF-8 byte sequences
    interpreted as Latin-1.  e.g. "Ren\\u00c3\\u00a9" -> "Rene" with accent.
    """
    if not text or not isinstance(text, str):
        return text or ""
    try:
        return text.encode("latin-1").decode("utf-8")
    except (UnicodeDecodeError, UnicodeEncodeError):
        return text


def _parse_old_format(data):
    """
    Parse old Instagram export format (list of objects with string_list_data).

    Structure:
    [
      {
        "title": "",
        "media_list_data": [],
        "string_list_data": [
          {"href": "https://...", "value": "username", "timestamp": 123}
        ]
      }
    ]

    Returns dict mapping username -> timestamp.
    """
    contacts = {}
    if not isinstance(data, list):
        return contacts

    for item in data:
        for entry in item.get("string_list_data", []):
            username = _decode_facebook_text(entry.get("value", "")).strip()
            if not username:
                continue
            timestamp = entry.get("timestamp", 0)
            contacts[username.lower()] = {
                "username": username,
                "timestamp": timestamp,
            }

    return contacts


def _parse_new_format(data, key):
    """
    Parse new Instagram export format (dict with a top-level key).

    Structure:
    {
      "relationships_followers": [
        {
          "title": "username",
          "string_list_data": [
            {"value": "username", "timestamp": 123}
          ]
        }
      ]
    }

    Returns dict mapping username -> timestamp.
    """
    contacts = {}
    if not isinstance(data, dict):
        return contacts

    items = data.get(key, [])
    if not isinstance(items, list):
        return contacts

    for item in items:
        # Try string_list_data first, fall back to title
        username = None
        timestamp = 0

        for entry in item.get("string_list_data", []):
            username = _decode_facebook_text(entry.get("value", "")).strip()
            timestamp = entry.get("timestamp", 0)
            if username:
                break

        if not username:
            username = _decode_facebook_text(item.get("title", "")).strip()

        if not username:
            continue

        contacts[username.lower()] = {
            "username": username,
            "timestamp": timestamp,
        }

    return contacts


def _load_json(file_path):
    """Load and return parsed JSON from a file. Returns None on failure."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Could not read %s: %s", file_path, e)
        return None


def _parse_contacts_file(file_path, role):
    """
    Parse a single followers or following JSON file.
    role is "follower" or "following".

    Returns dict mapping username -> {username, timestamp}.
    """
    data = _load_json(file_path)
    if data is None:
        return {}

    # Try old format (top-level list)
    if isinstance(data, list):
        result = _parse_old_format(data)
        if result:
            logger.info(
                "Parsed %d %s entries from %s (old format)",
                len(result), role, file_path,
            )
            return result

    # Try new format (top-level dict with known keys)
    if isinstance(data, dict):
        for key in ("relationships_followers", "relationships_following",
                     "followers", "following"):
            result = _parse_new_format(data, key)
            if result:
                logger.info(
                    "Parsed %d %s entries from %s (new format, key=%s)",
                    len(result), role, file_path, key,
                )
                return result

    logger.warning("Unrecognized format in %s — no contacts extracted.", file_path)
    return {}


def _find_export_files(export_path):
    """
    Auto-detect followers and following JSON files from an export directory.
    Returns (followers_files, following_files).
    """
    followers_files = []
    following_files = []

    # Common locations within Instagram exports
    search_dirs = [
        export_path,
        os.path.join(export_path, "followers_and_following"),
        os.path.join(export_path, "connections", "followers_and_following"),
    ]

    for search_dir in search_dirs:
        if not os.path.isdir(search_dir):
            continue

        for pattern in ("followers*.json", "followers_*.json"):
            followers_files.extend(glob.glob(os.path.join(search_dir, pattern)))

        for pattern in ("following*.json", "following_*.json"):
            following_files.extend(glob.glob(os.path.join(search_dir, pattern)))

    # Deduplicate while preserving order
    followers_files = list(dict.fromkeys(followers_files))
    following_files = list(dict.fromkeys(following_files))

    return followers_files, following_files


def _timestamp_to_iso(ts):
    """Convert a Unix timestamp to ISO 8601 string. Returns empty string if 0."""
    if not ts:
        return ""
    try:
        return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
    except (OSError, ValueError, OverflowError):
        return ""


def _build_entry(username, relationship, timestamp, updated_at):
    """Build a vault entry dict for a single contact."""
    return {
        "id": f"contacts:instagram:{username.lower()}",
        "sources": ["instagram"],
        "source_id": username,
        "name": {"display": username},
        "handles": {"instagram": username},
        "relationship": relationship,
        "connected_on": _timestamp_to_iso(timestamp),
        "updated_at": updated_at,
    }


def run_import(export_path, config=None):
    """
    Main entry point: parse Instagram export and save contacts to vault.

    Args:
        export_path: Path to the Instagram export directory or a specific JSON file.
        config: Optional config dict with vault_root and other settings.
    """
    config = config or {}
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    vault_root_base = config.get("vault_root", os.path.join(project_root, "vaults"))
    vault_root_base = os.path.expanduser(vault_root_base)
    if not os.path.isabs(vault_root_base):
        vault_root_base = os.path.join(project_root, vault_root_base)
    vault_root = os.path.join(vault_root_base, "Contacts")

    export_path = os.path.expanduser(export_path)

    if not os.path.exists(export_path):
        logger.error("Export path does not exist: %s", export_path)
        print(f"\nError: Path not found: {export_path}")
        return

    print(f"\nSaving to: {vault_root}")

    # Collect followers and following data
    followers = {}
    following = {}

    if os.path.isfile(export_path):
        # Single file mode — guess role from filename
        basename = os.path.basename(export_path).lower()
        if "following" in basename:
            following = _parse_contacts_file(export_path, "following")
        else:
            followers = _parse_contacts_file(export_path, "follower")
        updated_at = datetime.fromtimestamp(
            os.path.getmtime(export_path), tz=timezone.utc
        ).isoformat()
    elif os.path.isdir(export_path):
        followers_files, following_files = _find_export_files(export_path)

        if not followers_files and not following_files:
            logger.error(
                "No followers*.json or following*.json files found in %s",
                export_path,
            )
            print(
                f"\nError: No Instagram export files found in {export_path}\n"
                "Expected followers*.json and/or following*.json files in:\n"
                f"  {export_path}\n"
                f"  {os.path.join(export_path, 'followers_and_following')}"
            )
            return

        print(
            f"  Found {len(followers_files)} followers file(s), "
            f"{len(following_files)} following file(s)"
        )

        for fp in followers_files:
            followers.update(_parse_contacts_file(fp, "follower"))

        for fp in following_files:
            following.update(_parse_contacts_file(fp, "following"))

        # Use the most recent file modification time
        all_files = followers_files + following_files
        latest_mtime = max(os.path.getmtime(f) for f in all_files)
        updated_at = datetime.fromtimestamp(
            latest_mtime, tz=timezone.utc
        ).isoformat()
    else:
        logger.error("Export path is not a file or directory: %s", export_path)
        print(f"\nError: {export_path} is not a file or directory.")
        return

    # Determine relationships
    follower_usernames = set(followers.keys())
    following_usernames = set(following.keys())
    mutual = follower_usernames & following_usernames

    all_usernames = follower_usernames | following_usernames

    if not all_usernames:
        print("  No contacts found in export files.")
        return

    print(
        f"  Parsed {len(all_usernames):,} unique contacts "
        f"({len(follower_usernames):,} followers, "
        f"{len(following_usernames):,} following, "
        f"{len(mutual):,} mutual)"
    )

    # Load already-processed IDs
    processed_ids = load_processed_ids(vault_root)
    if processed_ids:
        print(f"  + Already vaulted: {len(processed_ids):,} contacts")

    # Build entries, skipping already processed
    entries = []
    for username_key in sorted(all_usernames):
        entry_id = f"contacts:instagram:{username_key}"
        if entry_id in processed_ids:
            continue

        if username_key in mutual:
            relationship = "mutual"
            info = followers[username_key]
        elif username_key in follower_usernames:
            relationship = "follower"
            info = followers[username_key]
        else:
            relationship = "following"
            info = following[username_key]

        entry = _build_entry(
            username=info["username"],
            relationship=relationship,
            timestamp=info["timestamp"],
            updated_at=updated_at,
        )
        entries.append(entry)

    if not entries:
        print("  + Nothing new — vault is up to date.")
        return

    print(f"  + Processing {len(entries):,} new contacts...")

    # Flush to vault
    flush_entries(entries, vault_root, "contacts.jsonl")
    new_ids = [e["id"] for e in entries]
    append_processed_ids(vault_root, new_ids)

    print()
    print("  " + "=" * 45)
    print(f"    Done! {len(entries):,} contacts saved")
    print(f"    Saved to: {vault_root}")
    print("  " + "=" * 45)
