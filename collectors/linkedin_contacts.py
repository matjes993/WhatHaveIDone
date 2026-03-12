"""
NOMOLO LinkedIn Contacts Collector
Parses LinkedIn's full data export into the unified contacts JSONL vault.

Supports two modes:
  1. Single CSV file: nomolo collect contacts-linkedin ~/Downloads/Connections.csv
  2. Full export directory: nomolo collect contacts-linkedin ~/Downloads/linkedin-export/
     Parses Connections.csv + enriches with messages, endorsements, recommendations.

LinkedIn exports via: Settings > Data Privacy > Get a copy of your data.
"""

import csv
import hashlib
import json
import logging
import os
from datetime import datetime
from pathlib import Path

from core.vault import flush_entries, load_processed_ids, append_processed_ids

logger = logging.getLogger("nomolo.linkedin_contacts")


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

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
# Full export parsers — enrich contacts with data from other export files
# ---------------------------------------------------------------------------

def _find_csv(export_dir, *candidates):
    """Find the first existing CSV file from candidate filenames."""
    for name in candidates:
        path = os.path.join(export_dir, name)
        if os.path.isfile(path):
            return path
    return None


def _find_json(export_dir, *candidates):
    """Find the first existing JSON file from candidate filenames."""
    for name in candidates:
        path = os.path.join(export_dir, name)
        if os.path.isfile(path):
            return path
    return None


def _parse_messages(export_dir):
    """
    Parse messages.csv to build per-contact message stats.
    Returns dict: {lowercase_name: {count, last_date, sent_count, received_count}}.

    LinkedIn message CSV columns vary but typically include:
    CONVERSATION ID, CONVERSATION TITLE, FROM, TO, DATE, SUBJECT, CONTENT
    """
    stats = {}
    msg_file = _find_csv(
        export_dir,
        "messages.csv",
        "Messages.csv",
        os.path.join("messages", "messages.csv"),
    )
    if not msg_file:
        return stats

    try:
        col_map, rows = _read_csv(msg_file)
    except (ValueError, OSError) as e:
        logger.warning("Could not read messages file: %s", e)
        return stats

    if col_map is None:
        return stats

    for row in rows:
        sender = _get(row, col_map, "from", "sender", "sender name")
        date_str = _get(row, col_map, "date", "sent date", "date sent")

        if not sender:
            continue

        key = sender.strip().lower()
        if key not in stats:
            stats[key] = {
                "message_count": 0,
                "last_message_date": "",
            }

        stats[key]["message_count"] += 1

        if date_str and date_str > stats[key]["last_message_date"]:
            stats[key]["last_message_date"] = date_str

    logger.info("Parsed message stats for %d contacts from %s", len(stats), msg_file)
    return stats


def _parse_endorsements(export_dir):
    """
    Parse Endorsement_Received_Info.csv.
    Returns dict: {lowercase_endorser_name: [skill1, skill2, ...]}.
    """
    endorsements = {}
    endo_file = _find_csv(
        export_dir,
        "Endorsement_Received_Info.csv",
        "endorsement_received_info.csv",
        "Endorsements_Received.csv",
        "endorsements_received.csv",
    )
    if not endo_file:
        return endorsements

    try:
        col_map, rows = _read_csv(endo_file)
    except (ValueError, OSError) as e:
        logger.warning("Could not read endorsements file: %s", e)
        return endorsements

    if col_map is None:
        return endorsements

    for row in rows:
        endorser = _get(
            row, col_map,
            "endorser first name", "endorser_first_name", "first name",
        )
        endorser_last = _get(
            row, col_map,
            "endorser last name", "endorser_last_name", "last name",
        )
        skill = _get(row, col_map, "skill name", "skill_name", "skill")

        if not endorser:
            continue

        name = f"{endorser} {endorser_last}".strip().lower()
        if name not in endorsements:
            endorsements[name] = []
        if skill:
            endorsements[name].append(skill)

    logger.info("Parsed endorsements from %d contacts", len(endorsements))
    return endorsements


def _parse_recommendations(export_dir):
    """
    Parse Recommendations_Received.csv.
    Returns dict: {lowercase_recommender_name: recommendation_text}.
    """
    recs = {}
    rec_file = _find_csv(
        export_dir,
        "Recommendations_Received.csv",
        "recommendations_received.csv",
        "Recommendations.csv",
    )
    if not rec_file:
        return recs

    try:
        col_map, rows = _read_csv(rec_file)
    except (ValueError, OSError) as e:
        logger.warning("Could not read recommendations file: %s", e)
        return recs

    if col_map is None:
        return recs

    for row in rows:
        first = _get(row, col_map, "first name", "first_name", "recommender first name")
        last = _get(row, col_map, "last name", "last_name", "recommender last name")
        text = _get(row, col_map, "recommendation text", "recommendation", "text", "body")
        status = _get(row, col_map, "status")

        if not first:
            continue

        name = f"{first} {last}".strip().lower()
        if text:
            recs[name] = {
                "text": text,
                "status": status,
            }

    logger.info("Parsed %d recommendations", len(recs))
    return recs


def _parse_invitations(export_dir):
    """
    Parse Invitations.csv for pending/sent invitation data.
    Returns dict: {lowercase_name: {direction, sent_at, message}}.
    """
    invites = {}
    inv_file = _find_csv(
        export_dir,
        "Invitations.csv",
        "invitations.csv",
    )
    if not inv_file:
        return invites

    try:
        col_map, rows = _read_csv(inv_file)
    except (ValueError, OSError) as e:
        logger.warning("Could not read invitations file: %s", e)
        return invites

    if col_map is None:
        return invites

    for row in rows:
        sender = _get(row, col_map, "from", "sender", "inviter name")
        to = _get(row, col_map, "to", "recipient", "invitee name")
        message = _get(row, col_map, "message", "invitation message")
        date_str = _get(row, col_map, "sent at", "sent_at", "date", "sent date")
        direction = _get(row, col_map, "direction")

        # Determine the contact name and direction
        if direction.lower() == "incoming" and sender:
            name = sender.strip().lower()
            dir_val = "received"
        elif direction.lower() == "outgoing" and to:
            name = to.strip().lower()
            dir_val = "sent"
        elif sender:
            name = sender.strip().lower()
            dir_val = "received"
        elif to:
            name = to.strip().lower()
            dir_val = "sent"
        else:
            continue

        invites[name] = {
            "direction": dir_val,
            "sent_at": date_str,
            "message": message,
        }

    logger.info("Parsed %d invitations", len(invites))
    return invites


def _parse_skills(export_dir):
    """
    Parse Skills.csv — the user's own skills, but useful for context.
    Returns list of skill strings.
    """
    skills_file = _find_csv(export_dir, "Skills.csv", "skills.csv")
    if not skills_file:
        return []

    try:
        col_map, rows = _read_csv(skills_file)
    except (ValueError, OSError):
        return []

    if col_map is None:
        return []

    return [
        _get(row, col_map, "name", "skill name", "skill")
        for row in rows
        if _get(row, col_map, "name", "skill name", "skill")
    ]


# ---------------------------------------------------------------------------
# Entry builder
# ---------------------------------------------------------------------------

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

    # Build embedding text
    embedding_parts = [display_name]
    if position and company:
        embedding_parts.append(f"{position} at {company}")
    elif company:
        embedding_parts.append(company)
    elif position:
        embedding_parts.append(position)
    if email:
        embedding_parts.append(email)

    emails = []
    if email:
        emails.append({"value": email.lower(), "type": "linkedin", "primary": True})

    organizations = []
    if company or position:
        organizations.append({
            "name": company,
            "title": position,
            "current": True,
        })

    return {
        "id": entry_id,
        "sources": ["linkedin"],
        "source_id": profile_url,
        "name": {
            "display": display_name,
            "given": first,
            "family": last,
        },
        "emails": emails,
        "phones": [],
        "addresses": [],
        "organizations": organizations,
        "urls": [{"value": profile_url, "type": "linkedin"}] if profile_url else [],
        "connected_on": _parse_connected_on(connected_on),
        "updated_at": file_mtime_iso,
        "contact_for_embedding": " — ".join(embedding_parts),
    }


def _enrich_entry(entry, messages, endorsements, recommendations, invitations):
    """Enrich a contact entry with data from other LinkedIn export files."""
    display = entry["name"]["display"].lower()

    # Message stats
    msg_stats = messages.get(display)
    if msg_stats:
        entry["linkedin_messages"] = msg_stats

    # Endorsements
    endorsed_skills = endorsements.get(display)
    if endorsed_skills:
        entry["linkedin_endorsed_skills"] = endorsed_skills

    # Recommendations
    rec = recommendations.get(display)
    if rec:
        entry["linkedin_recommendation"] = rec
        # Add to embedding for RAG
        if rec.get("text"):
            entry["contact_for_embedding"] += " — " + rec["text"][:200]

    # Invitations
    invite = invitations.get(display)
    if invite:
        entry["linkedin_invitation"] = invite

    return entry


# ---------------------------------------------------------------------------
# Export directory detection
# ---------------------------------------------------------------------------

def _detect_export_dir(export_path):
    """
    Detect whether export_path is a single CSV or a full LinkedIn export dir.
    Returns (connections_csv_path, export_dir_or_none).
    """
    p = Path(export_path)

    if p.is_file() and p.suffix.lower() == ".csv":
        # Check if it's inside a full export directory
        parent = p.parent
        # If the parent has other LinkedIn export files, treat it as full export
        indicators = [
            "messages.csv", "Messages.csv",
            "Invitations.csv", "invitations.csv",
            "Skills.csv", "skills.csv",
            "Profile.csv", "profile.csv",
        ]
        for indicator in indicators:
            if (parent / indicator).is_file():
                logger.info("Detected full export directory: %s", parent)
                return str(p), str(parent)

        return str(p), None

    if p.is_dir():
        # Look for Connections.csv in the directory
        for name in ["Connections.csv", "connections.csv"]:
            csv_path = p / name
            if csv_path.is_file():
                return str(csv_path), str(p)

        # No Connections.csv found
        logger.error("No Connections.csv found in %s", export_path)
        return None, str(p)

    return None, None


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_import(export_path, config=None):
    """
    Main entry point: parse LinkedIn export and flush to vault.

    Accepts either:
      - Path to a Connections.csv file
      - Path to a full LinkedIn data export directory

    Args:
        export_path: Path to CSV file or export directory.
        config: Dict with at least 'vault_root' key.
    """
    config = config or {}
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    vault_root_base = config.get("vault_root", os.path.join(project_root, "vaults"))
    vault_root_base = os.path.expanduser(vault_root_base)
    if not os.path.isabs(vault_root_base):
        vault_root_base = os.path.join(project_root, vault_root_base)
    vault_path = os.path.join(vault_root_base, "Contacts")

    logger.info("LinkedIn contacts import starting")
    logger.info("  Path: %s", export_path)
    logger.info("  Vault: %s", vault_path)

    # Detect export type
    csv_path, export_dir = _detect_export_dir(export_path)

    if csv_path is None and export_dir is not None:
        print(f"\n  Error: No Connections.csv found in {export_path}")
        print("  Make sure your LinkedIn export includes Connections data.")
        return
    elif csv_path is None:
        print(f"\n  Error: Path not found or not a CSV/directory: {export_path}")
        return

    # Get file modification time for updated_at
    try:
        mtime = os.path.getmtime(csv_path)
        file_mtime_iso = datetime.fromtimestamp(mtime).isoformat()
    except OSError as e:
        logger.error("Cannot stat CSV file %s: %s", csv_path, e)
        raise

    # Read Connections CSV
    col_map, rows = _read_csv(csv_path)
    if col_map is None:
        return

    is_full_export = export_dir is not None

    print(f"\n  LinkedIn Contacts Import")
    print(f"  {'=' * 45}")
    if is_full_export:
        print(f"  Mode: Full data export")
        print(f"  Directory: {export_dir}")
    else:
        print(f"  Mode: Connections CSV only")
        print(f"  CSV: {csv_path}")
    print(f"  Connections found: {len(rows)}")

    # Parse enrichment data from full export
    messages = {}
    endorsements = {}
    recommendations = {}
    invitations = {}
    enrichment_stats = []

    if is_full_export:
        print(f"\n  Scanning export for enrichment data...")

        messages = _parse_messages(export_dir)
        if messages:
            total_msgs = sum(s["message_count"] for s in messages.values())
            enrichment_stats.append(f"Messages: {total_msgs:,} across {len(messages)} contacts")

        endorsements = _parse_endorsements(export_dir)
        if endorsements:
            total_endo = sum(len(v) for v in endorsements.values())
            enrichment_stats.append(f"Endorsements: {total_endo:,} from {len(endorsements)} contacts")

        recommendations = _parse_recommendations(export_dir)
        if recommendations:
            enrichment_stats.append(f"Recommendations: {len(recommendations)}")

        invitations = _parse_invitations(export_dir)
        if invitations:
            enrichment_stats.append(f"Invitations: {len(invitations)}")

        if enrichment_stats:
            for stat in enrichment_stats:
                print(f"    + {stat}")
        else:
            print(f"    (no additional export files found)")

    # Load already-processed IDs
    processed_ids = load_processed_ids(vault_path)
    if processed_ids:
        print(f"  Already processed: {len(processed_ids):,}")

    # Convert rows to entries, skipping duplicates
    new_entries = []
    skipped_empty = 0
    skipped_duplicate = 0
    enriched_count = 0

    for row in rows:
        entry = _row_to_entry(row, col_map, file_mtime_iso)
        if entry is None:
            skipped_empty += 1
            continue
        if entry["id"] in processed_ids:
            skipped_duplicate += 1
            continue

        # Enrich with data from other export files
        if is_full_export:
            before = len(entry)
            entry = _enrich_entry(entry, messages, endorsements, recommendations, invitations)
            if len(entry) > before:
                enriched_count += 1

        new_entries.append(entry)

    if not new_entries:
        print("  Nothing new -- vault is up to date.")
        return

    # Flush to vault
    flush_entries(new_entries, vault_path, "contacts.jsonl")

    # Update processed IDs
    new_ids = [e["id"] for e in new_entries]
    append_processed_ids(vault_path, new_ids)

    # Summary stats
    with_email = sum(1 for e in new_entries if e.get("emails"))
    with_org = sum(1 for e in new_entries if e.get("organizations"))
    with_url = sum(1 for e in new_entries if e.get("urls"))
    with_msgs = sum(1 for e in new_entries if e.get("linkedin_messages"))

    print()
    print(f"  {'=' * 45}")
    print(f"  Done! {len(new_entries):,} contacts saved")
    print(f"  {'=' * 45}")
    print(f"    With email:      {with_email:,}")
    print(f"    With company:    {with_org:,}")
    print(f"    With profile URL:{with_url:,}")
    if with_msgs:
        print(f"    With messages:   {with_msgs:,}")
    if enriched_count:
        print(f"    Enriched:        {enriched_count:,}")
    if skipped_duplicate:
        print(f"    Skipped (dupe):  {skipped_duplicate:,}")
    if skipped_empty:
        print(f"    Skipped (empty): {skipped_empty:,}")
    print(f"    Saved to: {vault_path}")
    print(f"  {'=' * 45}")

    logger.info(
        "Import complete: %d new, %d duplicate, %d empty, %d enriched",
        len(new_entries), skipped_duplicate, skipped_empty, enriched_count,
    )
