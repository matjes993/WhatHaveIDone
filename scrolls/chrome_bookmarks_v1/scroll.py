"""Chrome Bookmarks Extractor — a SAFE community scroll.

Reads Chrome's Bookmarks JSON file (no network, no dependencies)
and writes structured bookmark entries to the vault.

Returns metrics dict for the scrolls engine to record.
"""
import json
import os
import time
from pathlib import Path
from urllib.parse import urlparse


CHROME_BOOKMARKS_PATH = Path.home() / "Library" / "Application Support" / "Google" / "Chrome" / "Default" / "Bookmarks"


def extract(vault_root: str, **kwargs) -> dict:
    """Extract Chrome bookmarks into vault.

    Args:
        vault_root: Path to the vault root directory.

    Returns:
        Metrics dict with extraction statistics.
    """
    start = time.time()
    bookmarks_file = kwargs.get("bookmarks_path", CHROME_BOOKMARKS_PATH)
    bookmarks_file = Path(bookmarks_file)

    if not bookmarks_file.exists():
        return {
            "records_extracted": 0,
            "manual_steps": 0,
            "human_wait_seconds": 0,
            "fields_per_record": 0,
            "unique_field_names": 0,
            "has_timestamps": False,
            "has_relationships": False,
            "total_bytes": 0,
            "compression_ratio": 1.0,
        }

    with open(bookmarks_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    entries = []
    all_fields = set()

    # Chrome stores bookmarks in roots: bookmark_bar, other, synced
    roots = data.get("roots", {})
    for root_name, root_node in roots.items():
        if isinstance(root_node, dict):
            _walk_bookmarks(root_node, root_name, entries, all_fields)

    # Write to vault
    vault_dir = os.path.join(vault_root, "Bookmarks")
    os.makedirs(vault_dir, exist_ok=True)
    output_file = os.path.join(vault_dir, "chrome_bookmarks.jsonl")

    total_bytes = 0
    with open(output_file, "w", encoding="utf-8") as f:
        for entry in entries:
            line = json.dumps(entry, ensure_ascii=False)
            f.write(line + "\n")
            total_bytes += len(line.encode("utf-8"))

    duration = time.time() - start
    n = len(entries)

    return {
        "records_extracted": n,
        "manual_steps": 0,
        "human_wait_seconds": 0.0,
        "fields_per_record": len(all_fields),
        "unique_field_names": len(all_fields),
        "has_timestamps": True,
        "has_relationships": False,
        "total_bytes": total_bytes,
        "compression_ratio": 1.0,
    }


def _walk_bookmarks(node, folder_path, entries, all_fields):
    """Recursively walk Chrome bookmark tree."""
    if not isinstance(node, dict):
        return

    node_type = node.get("type", "")

    if node_type == "url":
        url = node.get("url", "")
        name = node.get("name", "")
        date_added = node.get("date_added", "")

        # Chrome timestamps are microseconds since 1601-01-01
        timestamp = None
        if date_added and date_added.isdigit():
            # Convert Chrome timestamp to Unix timestamp
            chrome_epoch = int(date_added)
            unix_ts = (chrome_epoch - 11644473600000000) / 1000000
            if unix_ts > 0:
                timestamp = unix_ts

        domain = ""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc
        except Exception:
            pass

        entry = {
            "id": f"chrome_bm_{hash(url) & 0xFFFFFFFF:08x}",
            "type": "bookmark",
            "title": name,
            "url": url,
            "domain": domain,
            "folder": folder_path,
            "date_added": timestamp,
            "sources": ["chrome_bookmarks"],
            "bookmark_for_embedding": f"Bookmark: '{name}' ({domain}) in {folder_path}",
        }

        entries.append(entry)
        all_fields.update(entry.keys())

    elif node_type == "folder":
        children = node.get("children", [])
        folder_name = node.get("name", "")
        child_path = f"{folder_path}/{folder_name}" if folder_name else folder_path
        for child in children:
            _walk_bookmarks(child, child_path, entries, all_fields)

    # Also check "children" even if type is not folder (for root nodes)
    elif "children" in node:
        for child in node.get("children", []):
            _walk_bookmarks(child, folder_path, entries, all_fields)
