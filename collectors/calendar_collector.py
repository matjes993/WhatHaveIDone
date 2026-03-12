"""
NOMOLO Google Calendar Collector
Exports Google Calendar events to a local JSONL vault.
Uses calendar.readonly scope — your calendar is never modified.

Supports two modes:
  - API-based: fetches events directly from Google Calendar API v3
  - ICS import: parses .ics files as a fallback

Usage:
  nomolo collect calendar                         # API-based export
  nomolo collect calendar --ics ~/calendar.ics    # ICS file import
"""

import hashlib
import json
import logging
import os
import re
import sys
import time
from datetime import datetime

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Suppress "file_cache is only supported with oauth2client<4.0.0" warning
logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR)

from core.auth import get_google_credentials
from core.vault import flush_entries, load_processed_ids, append_processed_ids

logger = logging.getLogger("nomolo.calendar")

SCOPES = ["https://www.googleapis.com/auth/calendar.readonly"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_id(*parts):
    """Generate a deterministic 12-char hex ID from key parts."""
    raw = ":".join(str(p) for p in parts)
    return "calendar:google:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]


def _unfold_ics(text):
    """Unfold ICS line folding: lines starting with a space are continuations."""
    return re.sub(r"\r?\n[ \t]", "", text)


def _parse_ics_datetime(value):
    """
    Parse an ICS DTSTART/DTEND value into (iso_string, is_all_day).

    Handles:
      - 20240115T100000Z       (UTC datetime)
      - 20240115T100000        (local datetime)
      - 20240115               (all-day date)
      - TZID=Europe/Berlin:20240115T100000  (timezone-qualified)
    """
    # Strip any TZID= prefix (e.g. TZID=Europe/Berlin:20240115T100000)
    if ":" in value and not value.startswith("2"):
        _, value = value.rsplit(":", 1)

    value = value.strip()

    # All-day: YYYYMMDD (8 digits, no T)
    if len(value) == 8 and value.isdigit():
        try:
            dt = datetime.strptime(value, "%Y%m%d")
            return dt.strftime("%Y-%m-%d"), True
        except ValueError:
            return value, True

    # Datetime with Z suffix: 20240115T100000Z
    if value.endswith("Z"):
        try:
            dt = datetime.strptime(value, "%Y%m%dT%H%M%SZ")
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ"), False
        except ValueError:
            return value, False

    # Datetime without timezone: 20240115T100000
    if "T" in value:
        try:
            dt = datetime.strptime(value, "%Y%m%dT%H%M%S")
            return dt.strftime("%Y-%m-%dT%H:%M:%S"), False
        except ValueError:
            return value, False

    return value, False


def _parse_ics_event(lines):
    """
    Parse a list of unfolded lines from a single VEVENT block into a dict.
    Returns None if the event lacks essential fields.
    """
    props = {}
    for line in lines:
        # ICS lines are KEY;params:VALUE or KEY:VALUE
        match = re.match(r"^([A-Z][A-Z0-9_-]*)([;][^:]*)?:(.*)", line)
        if not match:
            continue
        key = match.group(1)
        params = match.group(2) or ""
        value = match.group(3)

        if key == "ATTENDEE":
            # Accumulate attendees
            attendees = props.get("ATTENDEE", [])
            # Extract CN= from params
            cn_match = re.search(r"CN=([^;:\"]+)", params + ":" + value)
            cn = cn_match.group(1).strip() if cn_match else ""
            # Extract email from mailto:
            email_match = re.search(r"mailto:([^\s;]+)", value, re.IGNORECASE)
            email = email_match.group(1) if email_match else ""
            # Extract PARTSTAT
            status_match = re.search(r"PARTSTAT=([^;:]+)", params)
            status = status_match.group(1).strip().lower() if status_match else ""
            attendees.append({"email": email, "name": cn, "status": status})
            props["ATTENDEE"] = attendees
        elif key in ("DTSTART", "DTEND"):
            # Preserve params for TZID parsing
            props[key] = (params + ":" + value).lstrip(";:")
            if params:
                props[key] = params.lstrip(";") + ":" + value
            else:
                props[key] = value
        else:
            props[key] = value

    uid = props.get("UID", "")
    summary = props.get("SUMMARY", "")
    if not uid and not summary:
        return None

    # Parse start/end times
    start_raw = props.get("DTSTART", "")
    end_raw = props.get("DTEND", "")
    start, start_all_day = _parse_ics_datetime(start_raw) if start_raw else ("", False)
    end, end_all_day = _parse_ics_datetime(end_raw) if end_raw else ("", False)
    all_day = start_all_day

    # Parse year/month from start
    year = 0
    month = 0
    if start:
        try:
            dt_start = datetime.fromisoformat(start.replace("Z", "+00:00"))
            year = dt_start.year
            month = dt_start.month
        except (ValueError, TypeError):
            date_match = re.match(r"(\d{4})-(\d{2})", start)
            if date_match:
                year = int(date_match.group(1))
                month = int(date_match.group(2))

    description = props.get("DESCRIPTION", "").replace("\\n", "\n").replace("\\,", ",").strip()
    location = props.get("LOCATION", "").replace("\\,", ",").strip()
    organizer_raw = props.get("ORGANIZER", "")
    organizer_match = re.search(r"mailto:([^\s;]+)", organizer_raw, re.IGNORECASE)
    organizer = organizer_match.group(1) if organizer_match else organizer_raw
    status = props.get("STATUS", "").lower()
    rrule = props.get("RRULE", "")
    attendees = props.get("ATTENDEE", [])

    # Build embedding
    embedding_parts = [summary or "(no title)"]
    if start:
        start_date = start[:10]
        start_time = start[11:16] if len(start) > 10 else ""
        end_time = end[11:16] if len(end) > 10 else ""
        if start_time and end_time:
            embedding_parts.append(f"on {start_date} {start_time}-{end_time}")
        elif start_time:
            embedding_parts.append(f"on {start_date} {start_time}")
        else:
            embedding_parts.append(f"on {start_date}")
    if location:
        embedding_parts.append(f"at {location}")
    if description:
        embedding_parts.append(description[:200])
    if attendees:
        names = [a.get("name") or a.get("email", "") for a in attendees if a.get("name") or a.get("email")]
        if names:
            embedding_parts.append(f"Attendees: {', '.join(names[:10])}")

    entry_id = _make_id(uid or summary, start)

    return {
        "id": entry_id,
        "sources": ["google-calendar"],
        "title": summary,
        "description": description,
        "location": location,
        "start": start,
        "end": end,
        "all_day": all_day,
        "attendees": attendees,
        "organizer": organizer,
        "status": status,
        "recurring": bool(rrule),
        "url": "",
        "year": year,
        "month": month,
        "updated_at": datetime.now().isoformat(),
        "event_for_embedding": " — ".join(embedding_parts),
    }


def _event_to_entry(event):
    """
    Convert a Google Calendar API event resource into a vault entry dict.
    Returns None if the event lacks essential fields.
    """
    event_id = event.get("id", "")
    summary = event.get("summary", "")

    if not event_id:
        return None

    # Parse start/end
    start_obj = event.get("start", {})
    end_obj = event.get("end", {})

    # All-day events use "date", timed events use "dateTime"
    start = start_obj.get("dateTime", start_obj.get("date", ""))
    end = end_obj.get("dateTime", end_obj.get("date", ""))
    all_day = "date" in start_obj and "dateTime" not in start_obj

    # Parse year/month from start
    year = 0
    month = 0
    if start:
        try:
            dt_start = datetime.fromisoformat(start.replace("Z", "+00:00"))
            year = dt_start.year
            month = dt_start.month
        except (ValueError, TypeError):
            date_match = re.match(r"(\d{4})-(\d{2})", start)
            if date_match:
                year = int(date_match.group(1))
                month = int(date_match.group(2))

    description = event.get("description", "")
    location = event.get("location", "")
    organizer = event.get("organizer", {}).get("email", "")
    status = event.get("status", "")
    recurring = bool(event.get("recurringEventId"))
    url = event.get("htmlLink", "")
    updated_at = event.get("updated", "")

    # Parse attendees
    attendees = []
    for att in event.get("attendees", []):
        attendees.append({
            "email": att.get("email", ""),
            "name": att.get("displayName", ""),
            "status": att.get("responseStatus", ""),
        })

    # Build embedding text
    embedding_parts = [summary or "(no title)"]
    if start:
        start_date = start[:10]
        start_time = start[11:16] if len(start) > 10 else ""
        end_time = end[11:16] if len(end) > 10 else ""
        if start_time and end_time:
            embedding_parts.append(f"on {start_date} {start_time}-{end_time}")
        elif start_time:
            embedding_parts.append(f"on {start_date} {start_time}")
        else:
            embedding_parts.append(f"on {start_date}")
    if location:
        embedding_parts.append(f"at {location}")
    if description:
        embedding_parts.append(description[:200])
    if attendees:
        names = [a.get("name") or a.get("email", "") for a in attendees if a.get("name") or a.get("email")]
        if names:
            embedding_parts.append(f"Attendees: {', '.join(names[:10])}")

    entry_id = f"calendar:google:{event_id}"

    return {
        "id": entry_id,
        "sources": ["google-calendar"],
        "title": summary,
        "description": description,
        "location": location,
        "start": start,
        "end": end,
        "all_day": all_day,
        "attendees": attendees,
        "organizer": organizer,
        "status": status,
        "recurring": recurring,
        "url": url,
        "year": year,
        "month": month,
        "updated_at": updated_at,
        "event_for_embedding": " — ".join(embedding_parts),
    }


def get_credentials(credentials_file, token_file, scopes):
    """Wrap core.auth.get_google_credentials for use by setup."""
    return get_google_credentials(credentials_file, token_file, scopes)


def _print_progress(fetched, start_time):
    """Print inline progress."""
    elapsed = time.time() - start_time
    rate = fetched / elapsed if elapsed > 0 else 0
    print(
        f"\r  Fetching events... {fetched:,} ({rate:.0f}/s)",
        end="", flush=True,
    )


# ---------------------------------------------------------------------------
# API-based export
# ---------------------------------------------------------------------------

def run_export(config=None):
    """
    Main API entry point: fetch all Google Calendar events and save to vault.

    Args:
        config: Dict with optional 'vault_root' and 'calendar' config section.
    """
    config = config or {}
    calendar_config = config.get("calendar", {})
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    vault_root_base = config.get("vault_root", os.path.join(project_root, "vaults"))
    vault_root_base = os.path.expanduser(vault_root_base)
    if not os.path.isabs(vault_root_base):
        vault_root_base = os.path.join(project_root, vault_root_base)
    vault_path = os.path.join(vault_root_base, "Calendar")

    credentials_file = calendar_config.get("credentials_file", "credentials.json")
    token_file = calendar_config.get("token_file", "token_calendar.json")
    scopes = [calendar_config.get("scope", SCOPES[0])]

    try:
        os.makedirs(vault_path, exist_ok=True)
    except PermissionError:
        print(f"\nError: Permission denied creating vault directory: {vault_path}")
        sys.exit(1)
    except OSError as e:
        print(f"\nError: Cannot create vault directory {vault_path}: {e}")
        sys.exit(1)

    print(f"\n  NOMOLO Google Calendar Collector")
    print(f"  {'=' * 45}")
    print(f"  Vault: {vault_path}")

    # Authenticate
    try:
        creds = get_credentials(credentials_file, token_file, scopes)
    except FileNotFoundError as e:
        print(f"\n{e}")
        sys.exit(1)

    # Build Calendar API service
    try:
        service = build("calendar", "v3", credentials=creds)
    except Exception as e:
        print(f"\nError: Could not connect to Calendar API: {e}")
        print("Check your internet connection and try again.")
        sys.exit(1)

    # Load already-processed IDs
    processed_ids = load_processed_ids(vault_path)
    if processed_ids:
        print(f"  Already vaulted: {len(processed_ids):,} events")

    # Fetch all events with pagination
    all_events = []
    page_token = None
    start_time = time.time()

    print()

    while True:
        try:
            kwargs = {
                "calendarId": "primary",
                "maxResults": 2500,
                "singleEvents": True,
                "orderBy": "startTime",
                "timeMin": "2000-01-01T00:00:00Z",
            }
            if page_token:
                kwargs["pageToken"] = page_token

            results = service.events().list(**kwargs).execute()
        except HttpError as e:
            status = e.resp.status
            if status == 403:
                print(
                    "\n\nError: Calendar API access denied (403).\n"
                    "Make sure the Google Calendar API is enabled in Google Cloud Console\n"
                    "and your OAuth token has the calendar.readonly scope.\n"
                    "Try deleting token_calendar.json and re-authorizing."
                )
            elif status == 429:
                print(
                    "\n\nError: Rate limited by Calendar API.\n"
                    "Wait a few minutes and try again."
                )
            elif status == 401:
                print(
                    "\n\nError: Authentication failed.\n"
                    "Delete token_calendar.json and re-authorize."
                )
            else:
                print(f"\n\nCalendar API error (HTTP {status}): {e}")
            sys.exit(1)
        except Exception as e:
            print(f"\n\nError fetching events: {e}")
            sys.exit(1)

        events = results.get("items", [])
        all_events.extend(events)

        _print_progress(len(all_events), start_time)

        page_token = results.get("nextPageToken")
        if not page_token:
            break

    elapsed = time.time() - start_time
    print(f"\r  Fetched {len(all_events):,} events in {elapsed:.1f}s        ")

    # Filter out already-processed events
    new_events = []
    for event in all_events:
        event_id = event.get("id", "")
        entry_id = f"calendar:google:{event_id}"
        if entry_id not in processed_ids:
            new_events.append(event)

    if not new_events:
        print("  Nothing new — vault is up to date.")
        return

    print(f"  Processing {len(new_events):,} new events...")

    # Convert to entries
    entries = []
    skipped = 0
    for event in new_events:
        try:
            entry = _event_to_entry(event)
            if entry is None:
                skipped += 1
                continue
            entries.append(entry)
        except Exception as e:
            event_id = event.get("id", "unknown")
            logger.warning("Skipping event %s: %s", event_id, e)
            skipped += 1

    # Flush to vault
    if entries:
        flush_entries(entries, vault_path, "calendar.jsonl")
        new_ids = [e["id"] for e in entries]
        append_processed_ids(vault_path, new_ids)

    # Summary stats
    all_day_count = sum(1 for e in entries if e.get("all_day"))
    recurring_count = sum(1 for e in entries if e.get("recurring"))
    with_attendees = sum(1 for e in entries if e.get("attendees"))
    with_location = sum(1 for e in entries if e.get("location"))

    year_counts = {}
    for e in entries:
        y = e.get("year", 0)
        if y:
            year_counts[y] = year_counts.get(y, 0) + 1

    print()
    print(f"  {'=' * 45}")
    print(f"  Done! {len(entries):,} events saved")
    print(f"  {'=' * 45}")
    print(f"    All-day events:  {all_day_count:,}")
    print(f"    Recurring:       {recurring_count:,}")
    print(f"    With attendees:  {with_attendees:,}")
    print(f"    With location:   {with_location:,}")
    if year_counts:
        for year in sorted(year_counts.keys()):
            print(f"    {year}: {year_counts[year]:,}")
    if skipped:
        print(f"    Skipped:         {skipped}")
    print()


# ---------------------------------------------------------------------------
# ICS file import
# ---------------------------------------------------------------------------

def run_import_ics(export_path, config=None):
    """
    Import calendar events from an ICS file into the vault.

    Args:
        export_path: Path to an .ics file.
        config: Dict with optional 'vault_root' key.
    """
    config = config or {}
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    vault_root_base = config.get("vault_root", os.path.join(project_root, "vaults"))
    vault_root_base = os.path.expanduser(vault_root_base)
    if not os.path.isabs(vault_root_base):
        vault_root_base = os.path.join(project_root, vault_root_base)
    vault_path = os.path.join(vault_root_base, "Calendar")

    export_path = os.path.expanduser(export_path)

    print(f"\n  NOMOLO Calendar Collector — ICS Import")
    print(f"  {'=' * 45}")
    print(f"  Path: {export_path}")
    print(f"  Vault: {vault_path}")

    if not os.path.isfile(export_path):
        print(f"  Error: File not found: {export_path}")
        return

    # Read and unfold ICS
    try:
        with open(export_path, "r", encoding="utf-8") as f:
            raw = f.read()
    except UnicodeDecodeError:
        with open(export_path, "r", encoding="latin-1") as f:
            raw = f.read()

    unfolded = _unfold_ics(raw)
    lines = unfolded.split("\n")

    # Extract VEVENT blocks
    events = []
    current_event = None
    for line in lines:
        stripped = line.strip()
        if stripped == "BEGIN:VEVENT":
            current_event = []
        elif stripped == "END:VEVENT":
            if current_event is not None:
                events.append(current_event)
            current_event = None
        elif current_event is not None:
            current_event.append(stripped)

    print(f"  VEVENT blocks found: {len(events):,}")

    if not events:
        print("  No events found in ICS file.")
        return

    # Load already-processed IDs
    processed_ids = load_processed_ids(vault_path)
    if processed_ids:
        print(f"  Already processed: {len(processed_ids):,}")

    new_entries = []
    skipped_duplicate = 0
    skipped_invalid = 0

    for event_lines in events:
        try:
            entry = _parse_ics_event(event_lines)
        except Exception as e:
            logger.warning("Skipping ICS event: %s", e)
            skipped_invalid += 1
            continue

        if entry is None:
            skipped_invalid += 1
            continue
        if entry["id"] in processed_ids:
            skipped_duplicate += 1
            continue

        new_entries.append(entry)

    if not new_entries:
        print("  Nothing new — vault is up to date.")
        return

    # Flush to vault
    flush_entries(new_entries, vault_path, "calendar.jsonl")
    new_ids = [e["id"] for e in new_entries]
    append_processed_ids(vault_path, new_ids)

    # Summary stats
    all_day_count = sum(1 for e in new_entries if e.get("all_day"))
    recurring_count = sum(1 for e in new_entries if e.get("recurring"))
    with_attendees = sum(1 for e in new_entries if e.get("attendees"))
    with_location = sum(1 for e in new_entries if e.get("location"))

    year_counts = {}
    for e in new_entries:
        y = e.get("year", 0)
        if y:
            year_counts[y] = year_counts.get(y, 0) + 1

    print()
    print(f"  {'=' * 45}")
    print(f"  Done! {len(new_entries):,} events saved")
    print(f"  {'=' * 45}")
    print(f"    All-day events:  {all_day_count:,}")
    print(f"    Recurring:       {recurring_count:,}")
    print(f"    With attendees:  {with_attendees:,}")
    print(f"    With location:   {with_location:,}")
    if year_counts:
        for year in sorted(year_counts.keys()):
            print(f"    {year}: {year_counts[year]:,}")
    if skipped_duplicate:
        print(f"    Skipped (dupe):  {skipped_duplicate:,}")
    if skipped_invalid:
        print(f"    Skipped (invalid): {skipped_invalid:,}")
    print()

    logger.info(
        "ICS import complete: %d new, %d duplicate, %d invalid",
        len(new_entries), skipped_duplicate, skipped_invalid,
    )
