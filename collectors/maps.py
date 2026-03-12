"""
NOMOLO Google Maps Location History Collector
Parses Google Takeout Semantic Location History into the unified Maps JSONL vault.

Google Takeout exports location history as per-month JSON files in:
  Takeout/Location History (Timeline)/Semantic Location History/YYYY/YYYY-MONTH.json

This collector parses place visits and activity segments from those files.
Raw location records (Records.json) are skipped as too granular.

Usage:
  nomolo collect maps ~/Downloads/Takeout/Location\ History\ (Timeline)/Semantic\ Location\ History/
  nomolo collect maps ~/Downloads/2024_JANUARY.json
"""

import hashlib
import json
import logging
import os
import re
from datetime import datetime

from core.vault import flush_entries, load_processed_ids, append_processed_ids

logger = logging.getLogger("nomolo.maps")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_id(prefix, *parts):
    """Generate a deterministic 12-char hex ID from key parts."""
    raw = ":".join(str(p) for p in parts)
    return f"maps:{prefix}:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]


def _e7_to_decimal(e7_value):
    """Convert latitudeE7/longitudeE7 to decimal degrees."""
    if e7_value is None:
        return 0.0
    return e7_value / 1e7


def _normalize_activity_type(raw_type):
    """
    Normalize a Google Maps activity type string to a human-readable label.

    Examples:
      IN_PASSENGER_VEHICLE -> driving
      WALKING -> walking
      IN_BUS -> bus
      CYCLING -> cycling
    """
    if not raw_type:
        return "unknown"

    mapping = {
        "IN_PASSENGER_VEHICLE": "driving",
        "IN_VEHICLE": "driving",
        "DRIVING": "driving",
        "WALKING": "walking",
        "ON_FOOT": "walking",
        "RUNNING": "running",
        "CYCLING": "cycling",
        "ON_BICYCLE": "cycling",
        "IN_BUS": "bus",
        "IN_TRAIN": "train",
        "IN_TRAM": "tram",
        "IN_SUBWAY": "subway",
        "IN_FERRY": "ferry",
        "FLYING": "flying",
        "IN_TAXI": "taxi",
        "MOTORCYCLING": "motorcycle",
        "SAILING": "sailing",
        "SKIING": "skiing",
        "STILL": "stationary",
        "UNKNOWN_ACTIVITY_TYPE": "unknown",
    }

    normalized = mapping.get(raw_type.upper())
    if normalized:
        return normalized

    # Fallback: strip prefixes and lowercase
    clean = raw_type.replace("IN_", "").replace("ON_", "").lower()
    return clean


def _normalize_semantic_type(raw_type):
    """
    Normalize a Google Maps semantic type string.

    Examples:
      TYPE_CAFE -> cafe
      TYPE_HOME -> home
      TYPE_WORK -> work
    """
    if not raw_type:
        return ""

    # Strip TYPE_ prefix and lowercase
    if raw_type.startswith("TYPE_"):
        return raw_type[5:].lower()
    return raw_type.lower()


def _parse_duration_minutes(start_str, end_str):
    """Calculate duration in minutes between two ISO timestamp strings."""
    if not start_str or not end_str:
        return 0

    try:
        start_dt = datetime.fromisoformat(start_str.replace("Z", "+00:00").replace(".000Z", "+00:00"))
        end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00").replace(".000Z", "+00:00"))
        delta = end_dt - start_dt
        return max(0, int(delta.total_seconds() / 60))
    except (ValueError, TypeError):
        return 0


def _parse_year_month(timestamp_str):
    """Extract year and month from an ISO timestamp string."""
    if not timestamp_str:
        return 0, 0
    match = re.match(r"(\d{4})-(\d{2})", timestamp_str)
    if match:
        return int(match.group(1)), int(match.group(2))
    return 0, 0


def _find_timeline_files(export_path):
    """
    Locate Semantic Location History JSON files from an export path.

    Accepts:
      - Direct path to a single month JSON file
      - Path to the Semantic Location History directory (recursively finds all JSONs)
      - Path to a Takeout root directory

    Returns list of file paths sorted by name.
    """
    export_path = os.path.expanduser(export_path)

    # If it's a direct file path
    if os.path.isfile(export_path):
        return [export_path]

    # If it's a directory, search recursively for JSON files
    if os.path.isdir(export_path):
        files = []
        for root, _dirs, filenames in os.walk(export_path):
            for fname in filenames:
                if fname.lower().endswith(".json"):
                    # Skip Records.json (raw location records, too granular)
                    if fname.lower() in ("records.json", "settings.json", "tombstones.json"):
                        continue
                    full_path = os.path.join(root, fname)
                    files.append(full_path)

        return sorted(files)

    logger.error("Export path not found: %s", export_path)
    return []


def _load_json(file_path):
    """Load and parse a JSON file with encoding fallback."""
    encodings = ["utf-8-sig", "utf-8", "latin-1"]

    for encoding in encodings:
        try:
            with open(file_path, "r", encoding=encoding) as f:
                return json.load(f)
        except UnicodeDecodeError:
            continue
        except json.JSONDecodeError as e:
            logger.error("Invalid JSON in %s: %s", file_path, e)
            raise
        except FileNotFoundError:
            logger.error("File not found: %s", file_path)
            raise
        except OSError as e:
            logger.error("Cannot read %s: %s", file_path, e)
            raise

    logger.error("Could not decode %s with any supported encoding", file_path)
    raise ValueError(f"Cannot decode JSON file: {file_path}")


# ---------------------------------------------------------------------------
# Entry parsers
# ---------------------------------------------------------------------------

def _parse_place_visit(visit_obj):
    """
    Parse a placeVisit timeline object into a vault entry dict.
    Returns None if the visit lacks essential fields.
    """
    location = visit_obj.get("location", {})
    duration = visit_obj.get("duration", {})

    start_time = duration.get("startTimestamp", "")
    end_time = duration.get("endTimestamp", "")

    if not start_time:
        return None

    place_name = location.get("name", "")
    address = location.get("address", "")
    place_id = location.get("placeId", "")
    lat = _e7_to_decimal(location.get("latitudeE7"))
    lng = _e7_to_decimal(location.get("longitudeE7"))
    confidence = visit_obj.get("placeConfidence", "")
    semantic_type = _normalize_semantic_type(location.get("semanticType", ""))

    duration_minutes = _parse_duration_minutes(start_time, end_time)
    year, month = _parse_year_month(start_time)

    entry_id = _make_id("visit", place_id or place_name or address, start_time)

    # Build embedding text
    embedding_parts = []
    label = place_name or address or "Unknown location"
    embedding_parts.append(f"Visited {label}")
    if place_name and address:
        embedding_parts[0] = f"Visited {place_name} ({address})"
    if start_time:
        start_date = start_time[:10]
        start_hm = start_time[11:16] if len(start_time) > 11 else ""
        end_hm = end_time[11:16] if len(end_time) > 11 else ""
        if start_hm and end_hm:
            embedding_parts.append(f"on {start_date} {start_hm}-{end_hm}")
        elif start_hm:
            embedding_parts.append(f"on {start_date} {start_hm}")
        else:
            embedding_parts.append(f"on {start_date}")
    if duration_minutes:
        embedding_parts.append(f"{duration_minutes} min")

    return {
        "id": entry_id,
        "sources": ["google-maps"],
        "type": "visit",
        "place_name": place_name,
        "address": address,
        "place_id": place_id,
        "lat": lat,
        "lng": lng,
        "start_time": start_time,
        "end_time": end_time,
        "duration_minutes": duration_minutes,
        "confidence": confidence,
        "semantic_type": semantic_type,
        "year": year,
        "month": month,
        "updated_at": datetime.now().isoformat(),
        "location_for_embedding": " — ".join(embedding_parts),
    }


def _parse_activity_segment(segment_obj):
    """
    Parse an activitySegment timeline object into a vault entry dict.
    Returns None if the segment lacks essential fields.
    """
    duration = segment_obj.get("duration", {})
    start_time = duration.get("startTimestamp", "")
    end_time = duration.get("endTimestamp", "")

    if not start_time:
        return None

    raw_activity_type = segment_obj.get("activityType", "")
    activity_type = _normalize_activity_type(raw_activity_type)
    distance_meters = segment_obj.get("distance", 0)
    duration_minutes = _parse_duration_minutes(start_time, end_time)
    year, month = _parse_year_month(start_time)

    entry_id = _make_id("activity", activity_type, start_time, end_time)

    # Build embedding text
    # Capitalize first letter for readability
    activity_label = activity_type.capitalize()
    if activity_type == "driving":
        activity_label = "Drove"
    elif activity_type == "walking":
        activity_label = "Walked"
    elif activity_type == "cycling":
        activity_label = "Cycled"
    elif activity_type == "running":
        activity_label = "Ran"
    elif activity_type == "bus":
        activity_label = "Took bus"
    elif activity_type == "train":
        activity_label = "Took train"
    elif activity_type == "flying":
        activity_label = "Flew"

    embedding_parts = []
    if distance_meters and distance_meters >= 1000:
        km = distance_meters / 1000
        embedding_parts.append(f"{activity_label} {km:.1f} km")
    elif distance_meters:
        embedding_parts.append(f"{activity_label} {distance_meters} m")
    else:
        embedding_parts.append(activity_label)

    if start_time:
        start_date = start_time[:10]
        embedding_parts.append(f"on {start_date}")

    if duration_minutes:
        embedding_parts.append(f"{duration_minutes} min")

    return {
        "id": entry_id,
        "sources": ["google-maps"],
        "type": "activity",
        "activity_type": activity_type,
        "distance_meters": distance_meters,
        "start_time": start_time,
        "end_time": end_time,
        "duration_minutes": duration_minutes,
        "year": year,
        "month": month,
        "updated_at": datetime.now().isoformat(),
        "location_for_embedding": " — ".join(embedding_parts),
    }


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_import(export_path, config=None):
    """
    Import Google Maps Semantic Location History into the vault.

    Accepts path to the Semantic Location History directory or a single
    month JSON file.

    Args:
        export_path: Path to JSON file or Semantic Location History directory.
        config: Dict with optional 'vault_root' key.
    """
    config = config or {}
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    vault_root_base = config.get("vault_root", os.path.join(project_root, "vaults"))
    vault_root_base = os.path.expanduser(vault_root_base)
    if not os.path.isabs(vault_root_base):
        vault_root_base = os.path.join(project_root, vault_root_base)
    vault_path = os.path.join(vault_root_base, "Maps")

    print(f"\n  NOMOLO Maps Collector — Google Location History")
    print(f"  {'=' * 45}")
    print(f"  Path: {export_path}")
    print(f"  Vault: {vault_path}")

    # Find timeline JSON files
    timeline_files = _find_timeline_files(export_path)

    if not timeline_files:
        print("  Error: No Semantic Location History JSON files found.")
        print("  Provide the path to a month JSON file or the Semantic Location History directory.")
        return

    print(f"  Timeline files found: {len(timeline_files)}")
    if len(timeline_files) <= 10:
        for f in timeline_files:
            print(f"    - {os.path.basename(f)}")
    else:
        for f in timeline_files[:5]:
            print(f"    - {os.path.basename(f)}")
        print(f"    ... and {len(timeline_files) - 5} more")

    # Load already-processed IDs
    processed_ids = load_processed_ids(vault_path)
    if processed_ids:
        print(f"  Already processed: {len(processed_ids):,}")

    new_entries = []
    skipped_duplicate = 0
    skipped_invalid = 0
    total_visits = 0
    total_activities = 0
    total_objects = 0

    for file_path in timeline_files:
        try:
            data = _load_json(file_path)
        except (ValueError, OSError) as e:
            print(f"  Error reading {os.path.basename(file_path)}: {e}")
            continue

        if not isinstance(data, dict):
            logger.warning("%s is not a dict, skipping", file_path)
            continue

        timeline_objects = data.get("timelineObjects", [])
        total_objects += len(timeline_objects)
        basename = os.path.basename(file_path)

        for obj in timeline_objects:
            entry = None
            try:
                if "placeVisit" in obj:
                    entry = _parse_place_visit(obj["placeVisit"])
                    if entry:
                        total_visits += 1
                elif "activitySegment" in obj:
                    entry = _parse_activity_segment(obj["activitySegment"])
                    if entry:
                        total_activities += 1
            except Exception as e:
                logger.warning("Skipping entry in %s: %s", basename, e)
                skipped_invalid += 1
                continue

            if entry is None:
                skipped_invalid += 1
                continue
            if entry["id"] in processed_ids:
                skipped_duplicate += 1
                continue

            new_entries.append(entry)

        print(f"\r  Processing... {len(new_entries):,} entries from {total_objects:,} objects", end="", flush=True)

    print()

    print(f"  Total timeline objects: {total_objects:,}")

    if not new_entries:
        print("  Nothing new — vault is up to date.")
        return

    # Flush to vault
    flush_entries(new_entries, vault_path, "maps.jsonl")
    new_ids = [e["id"] for e in new_entries]
    append_processed_ids(vault_path, new_ids)

    # Collect stats
    visit_entries = [e for e in new_entries if e.get("type") == "visit"]
    activity_entries = [e for e in new_entries if e.get("type") == "activity"]

    total_distance_km = sum(e.get("distance_meters", 0) for e in activity_entries) / 1000
    total_visit_minutes = sum(e.get("duration_minutes", 0) for e in visit_entries)
    total_travel_minutes = sum(e.get("duration_minutes", 0) for e in activity_entries)

    unique_places = set()
    for e in visit_entries:
        place = e.get("place_name") or e.get("address") or e.get("place_id", "")
        if place:
            unique_places.add(place)

    year_counts = {}
    for e in new_entries:
        y = e.get("year", 0)
        if y:
            year_counts[y] = year_counts.get(y, 0) + 1

    # Activity type breakdown
    activity_types = {}
    for e in activity_entries:
        at = e.get("activity_type", "unknown")
        activity_types[at] = activity_types.get(at, 0) + 1

    print()
    print(f"  {'=' * 45}")
    print(f"  Done! {len(new_entries):,} entries saved")
    print(f"  {'=' * 45}")
    print(f"    Place visits:    {len(visit_entries):,}")
    print(f"    Activities:      {len(activity_entries):,}")
    print(f"    Unique places:   {len(unique_places):,}")
    print(f"    Total distance:  {total_distance_km:,.1f} km")
    print(f"    Time at places:  {total_visit_minutes / 60:,.1f} hours")
    print(f"    Time traveling:  {total_travel_minutes / 60:,.1f} hours")
    if activity_types:
        print(f"    Activity breakdown:")
        for at in sorted(activity_types, key=activity_types.get, reverse=True):
            print(f"      {at}: {activity_types[at]:,}")
    if year_counts:
        for year in sorted(year_counts.keys()):
            print(f"    {year}: {year_counts[year]:,}")
    if skipped_duplicate:
        print(f"    Skipped (dupe):    {skipped_duplicate:,}")
    if skipped_invalid:
        print(f"    Skipped (invalid): {skipped_invalid:,}")
    print()

    logger.info(
        "Maps import complete: %d new (%d visits, %d activities), %d duplicate, %d invalid",
        len(new_entries), len(visit_entries), len(activity_entries),
        skipped_duplicate, skipped_invalid,
    )
