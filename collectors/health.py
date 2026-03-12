"""
WHID Health Collector
Parses Apple Health XML export (export.xml) into the unified Health JSONL vault.

Uses iterative XML parsing (iterparse) to handle large exports efficiently
without loading the entire file into memory.

Imports:
  - Workouts (full detail)
  - Daily step count summaries (aggregated by day)
  - Sleep analysis
  - Body measurements (weight, height)
  - Heart rate daily summaries (min, max, avg per day)

Usage:
  whid collect health ~/Downloads/apple_health_export/export.xml
  whid collect health ~/Downloads/apple_health_export/
"""

import hashlib
import logging
import os
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime

from core.vault import flush_entries, load_processed_ids, append_processed_ids

logger = logging.getLogger("whid.health")


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Record types we care about
STEP_TYPE = "HKQuantityTypeIdentifierStepCount"
HEART_RATE_TYPE = "HKQuantityTypeIdentifierHeartRate"
WEIGHT_TYPE = "HKQuantityTypeIdentifierBodyMass"
HEIGHT_TYPE = "HKQuantityTypeIdentifierHeight"
SLEEP_TYPE = "HKCategoryTypeIdentifierSleepAnalysis"

RECORD_TYPES_OF_INTEREST = {
    STEP_TYPE,
    HEART_RATE_TYPE,
    WEIGHT_TYPE,
    HEIGHT_TYPE,
    SLEEP_TYPE,
}

# Activity type normalization map
ACTIVITY_TYPE_MAP = {
    "HKWorkoutActivityTypeRunning": "running",
    "HKWorkoutActivityTypeWalking": "walking",
    "HKWorkoutActivityTypeCycling": "cycling",
    "HKWorkoutActivityTypeSwimming": "swimming",
    "HKWorkoutActivityTypeHiking": "hiking",
    "HKWorkoutActivityTypeYoga": "yoga",
    "HKWorkoutActivityTypeFunctionalStrengthTraining": "strength-training",
    "HKWorkoutActivityTypeTraditionalStrengthTraining": "strength-training",
    "HKWorkoutActivityTypeHighIntensityIntervalTraining": "hiit",
    "HKWorkoutActivityTypeCrossTraining": "cross-training",
    "HKWorkoutActivityTypePilates": "pilates",
    "HKWorkoutActivityTypeDance": "dance",
    "HKWorkoutActivityTypeElliptical": "elliptical",
    "HKWorkoutActivityTypeRowing": "rowing",
    "HKWorkoutActivityTypeStairClimbing": "stair-climbing",
    "HKWorkoutActivityTypeCoreTraining": "core-training",
    "HKWorkoutActivityTypeMixedCardio": "mixed-cardio",
    "HKWorkoutActivityTypeSoccer": "soccer",
    "HKWorkoutActivityTypeTennis": "tennis",
    "HKWorkoutActivityTypeBasketball": "basketball",
    "HKWorkoutActivityTypePlay": "play",
    "HKWorkoutActivityTypeCooldown": "cooldown",
    "HKWorkoutActivityTypeMindAndBody": "mind-and-body",
    "HKWorkoutActivityTypeFlexibility": "flexibility",
    "HKWorkoutActivityTypeOther": "other",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_id(entry_type, *parts):
    """Generate a deterministic 12-char hex ID from type and key parts."""
    raw = ":".join(str(p) for p in parts)
    return f"health:{entry_type}:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]


def _normalize_activity_type(raw_type):
    """
    Convert Apple Health workout activity type to a human-readable name.
    e.g. "HKWorkoutActivityTypeRunning" -> "running"
    """
    if raw_type in ACTIVITY_TYPE_MAP:
        return ACTIVITY_TYPE_MAP[raw_type]

    # Fallback: strip prefix and lowercase
    prefix = "HKWorkoutActivityType"
    if raw_type.startswith(prefix):
        return raw_type[len(prefix):].lower()
    return raw_type.lower()


def _safe_float(value, default=0.0):
    """Convert a string to float, returning default on failure."""
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def _parse_health_date(date_str):
    """
    Parse an Apple Health date string into a datetime object.
    Format: "2024-01-15 10:00:00 +0100"
    Returns None on failure.
    """
    if not date_str:
        return None

    for fmt in (
        "%Y-%m-%d %H:%M:%S %z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    ):
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue

    logger.warning("Could not parse health date: %r", date_str)
    return None


def _find_export_xml(export_path):
    """
    Locate the export.xml file from the given path.

    Accepts:
      - Direct path to export.xml
      - Path to the apple_health_export directory
      - Path to a directory containing the apple_health_export directory

    Returns the path to export.xml, or None if not found.
    """
    export_path = os.path.expanduser(export_path)

    # Direct file
    if os.path.isfile(export_path):
        return export_path

    # Directory: look for export.xml in common locations
    if os.path.isdir(export_path):
        candidates = [
            "export.xml",
            "apple_health_export/export.xml",
        ]
        for candidate in candidates:
            full = os.path.join(export_path, candidate)
            if os.path.isfile(full):
                return full

    logger.error("Could not find export.xml at %s", export_path)
    return None


# ---------------------------------------------------------------------------
# Record parsers
# ---------------------------------------------------------------------------

def _parse_workout(attrib):
    """
    Parse a Workout XML element's attributes into a vault entry dict.
    Returns None if essential data is missing.
    """
    activity_raw = attrib.get("workoutActivityType", "")
    if not activity_raw:
        return None

    activity = _normalize_activity_type(activity_raw)
    duration = _safe_float(attrib.get("duration", 0))
    distance = _safe_float(attrib.get("totalDistance", 0))
    calories = _safe_float(attrib.get("totalEnergyBurned", 0))
    source = attrib.get("sourceName", "")

    start_str = attrib.get("startDate", "")
    end_str = attrib.get("endDate", "")
    start_dt = _parse_health_date(start_str)
    end_dt = _parse_health_date(end_str)

    year = start_dt.year if start_dt else 0
    month = start_dt.month if start_dt else 0
    date_str = start_dt.strftime("%Y-%m-%d") if start_dt else ""

    start_iso = start_dt.isoformat() if start_dt else start_str
    end_iso = end_dt.isoformat() if end_dt else end_str

    entry_id = _make_id("workout", activity, start_str)

    # Build embedding text
    embedding = f"{activity.replace('-', ' ').title()} workout on {date_str}"
    parts = []
    if duration:
        parts.append(f"{duration:.1f} min")
    if distance:
        parts.append(f"{distance:.1f} km")
    if calories:
        parts.append(f"{calories:.0f} kcal")
    if parts:
        embedding += " — " + ", ".join(parts)

    return {
        "id": entry_id,
        "sources": ["apple-health"],
        "type": "workout",
        "activity": activity,
        "duration_minutes": round(duration, 1),
        "distance_km": round(distance, 1),
        "calories": round(calories, 0),
        "start_date": start_iso,
        "end_date": end_iso,
        "source_device": source,
        "year": year,
        "month": month,
        "updated_at": datetime.now().isoformat(),
        "health_for_embedding": embedding,
    }


def _parse_record(attrib):
    """
    Parse a Record XML element's attributes into a partial data dict
    used for aggregation. Returns (record_type, data_dict) or (None, None)
    if the record type is not of interest.
    """
    record_type = attrib.get("type", "")
    if record_type not in RECORD_TYPES_OF_INTEREST:
        return None, None

    value = _safe_float(attrib.get("value", 0))
    unit = attrib.get("unit", "")
    source = attrib.get("sourceName", "")
    start_str = attrib.get("startDate", "")
    end_str = attrib.get("endDate", "")
    start_dt = _parse_health_date(start_str)

    date_str = start_dt.strftime("%Y-%m-%d") if start_dt else ""

    return record_type, {
        "value": value,
        "unit": unit,
        "source": source,
        "start_date": start_str,
        "end_date": end_str,
        "date": date_str,
        "dt": start_dt,
    }


def _aggregate_daily_steps(step_records):
    """
    Aggregate individual step count records into daily summaries.

    Args:
        step_records: list of record dicts from _parse_record

    Returns:
        list of vault entry dicts, one per day
    """
    daily = defaultdict(float)
    for rec in step_records:
        day = rec.get("date", "")
        if day:
            daily[day] += rec["value"]

    entries = []
    for day, total_steps in sorted(daily.items()):
        steps = int(total_steps)
        dt = _parse_health_date(day + " 00:00:00 +0000") if day else None
        year = dt.year if dt else 0
        month = dt.month if dt else 0

        entry_id = _make_id("daily", day)

        entries.append({
            "id": entry_id,
            "sources": ["apple-health"],
            "type": "daily_summary",
            "date": day,
            "steps": steps,
            "year": year,
            "month": month,
            "updated_at": datetime.now().isoformat(),
            "health_for_embedding": f"{day} — {steps:,} steps",
        })

    return entries


def _aggregate_daily_heart_rate(hr_records):
    """
    Aggregate individual heart rate records into daily summaries (min, max, avg).

    Returns list of vault entry dicts, one per day.
    """
    daily = defaultdict(list)
    for rec in hr_records:
        day = rec.get("date", "")
        if day and rec["value"] > 0:
            daily[day].append(rec["value"])

    entries = []
    for day, values in sorted(daily.items()):
        if not values:
            continue

        avg_hr = round(sum(values) / len(values), 1)
        min_hr = round(min(values), 1)
        max_hr = round(max(values), 1)

        dt = _parse_health_date(day + " 00:00:00 +0000") if day else None
        year = dt.year if dt else 0
        month = dt.month if dt else 0

        entry_id = _make_id("hr", day)

        entries.append({
            "id": entry_id,
            "sources": ["apple-health"],
            "type": "heart_rate_daily",
            "date": day,
            "avg_bpm": avg_hr,
            "min_bpm": min_hr,
            "max_bpm": max_hr,
            "readings": len(values),
            "year": year,
            "month": month,
            "updated_at": datetime.now().isoformat(),
            "health_for_embedding": f"{day} — heart rate avg {avg_hr} bpm (min {min_hr}, max {max_hr})",
        })

    return entries


def _build_sleep_entry(attrib):
    """
    Parse a sleep analysis record into a vault entry.
    Returns None if essential data is missing.
    """
    value = attrib.get("value", "")
    source = attrib.get("sourceName", "")
    start_str = attrib.get("startDate", "")
    end_str = attrib.get("endDate", "")

    start_dt = _parse_health_date(start_str)
    end_dt = _parse_health_date(end_str)

    if not start_dt or not end_dt:
        return None

    date_str = start_dt.strftime("%Y-%m-%d")
    year = start_dt.year
    month = start_dt.month

    duration_hours = (end_dt - start_dt).total_seconds() / 3600

    # Normalize sleep type
    sleep_type = "unknown"
    if "InBed" in str(value):
        sleep_type = "in-bed"
    elif "Asleep" in str(value) or "Core" in str(value):
        sleep_type = "asleep"
    elif "Deep" in str(value):
        sleep_type = "deep"
    elif "REM" in str(value):
        sleep_type = "rem"
    elif "Awake" in str(value):
        sleep_type = "awake"

    start_iso = start_dt.isoformat()
    end_iso = end_dt.isoformat()
    entry_id = _make_id("sleep", start_str, end_str)

    return {
        "id": entry_id,
        "sources": ["apple-health"],
        "type": "sleep",
        "sleep_type": sleep_type,
        "duration_hours": round(duration_hours, 2),
        "start_date": start_iso,
        "end_date": end_iso,
        "source_device": source,
        "year": year,
        "month": month,
        "updated_at": datetime.now().isoformat(),
        "health_for_embedding": f"{date_str} — sleep ({sleep_type}) {duration_hours:.1f} hours",
    }


def _build_body_measurement_entry(record_type, data):
    """
    Build a vault entry for a body measurement (weight or height).
    """
    date_str = data.get("date", "")
    dt = data.get("dt")
    year = dt.year if dt else 0
    month = dt.month if dt else 0
    value = data["value"]
    unit = data.get("unit", "")

    if record_type == WEIGHT_TYPE:
        measurement = "weight"
        label = f"{value} {unit}"
    elif record_type == HEIGHT_TYPE:
        measurement = "height"
        label = f"{value} {unit}"
    else:
        measurement = "body"
        label = f"{value} {unit}"

    entry_id = _make_id("body", measurement, date_str, str(value))

    return {
        "id": entry_id,
        "sources": ["apple-health"],
        "type": f"body_{measurement}",
        "measurement": measurement,
        "value": value,
        "unit": unit,
        "date": date_str,
        "source_device": data.get("source", ""),
        "year": year,
        "month": month,
        "updated_at": datetime.now().isoformat(),
        "health_for_embedding": f"{date_str} — {measurement}: {label}",
    }


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_import(export_path, config=None):
    """
    Import Apple Health data from an export.xml into the vault.

    Uses iterative XML parsing to handle large files efficiently.

    Args:
        export_path: Path to export.xml or the apple_health_export directory.
        config: Dict with optional 'vault_root' key.
    """
    config = config or {}
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    vault_root_base = config.get("vault_root", os.path.join(project_root, "vaults"))
    vault_root_base = os.path.expanduser(vault_root_base)
    if not os.path.isabs(vault_root_base):
        vault_root_base = os.path.join(project_root, vault_root_base)
    vault_path = os.path.join(vault_root_base, "Health")

    print(f"\n  WHID Health Collector — Apple Health")
    print(f"  {'=' * 45}")
    print(f"  Path: {export_path}")
    print(f"  Vault: {vault_path}")

    # Find export.xml
    xml_path = _find_export_xml(export_path)
    if xml_path is None:
        print("  Error: Could not find export.xml.")
        print("  Provide the path to export.xml or the apple_health_export directory.")
        return

    print(f"  XML file: {xml_path}")

    # Load already-processed IDs
    processed_ids = load_processed_ids(vault_path)
    if processed_ids:
        print(f"  Already processed: {len(processed_ids):,}")

    # Collect raw data via iterative parsing
    workouts = []
    step_records = []
    hr_records = []
    sleep_entries = []
    body_entries = []
    total_records = 0

    print("  Parsing XML (this may take a while for large exports)...")

    try:
        for event, elem in ET.iterparse(xml_path, events=("end",)):
            tag = elem.tag

            if tag == "Workout":
                total_records += 1
                try:
                    entry = _parse_workout(elem.attrib)
                    if entry:
                        workouts.append(entry)
                except Exception as e:
                    logger.warning("Skipping workout: %s", e)
                elem.clear()

            elif tag == "Record":
                total_records += 1
                record_type = elem.attrib.get("type", "")

                if record_type == SLEEP_TYPE:
                    try:
                        entry = _build_sleep_entry(elem.attrib)
                        if entry:
                            sleep_entries.append(entry)
                    except Exception as e:
                        logger.warning("Skipping sleep record: %s", e)

                elif record_type in (WEIGHT_TYPE, HEIGHT_TYPE):
                    rec_type, data = _parse_record(elem.attrib)
                    if data:
                        try:
                            entry = _build_body_measurement_entry(rec_type, data)
                            body_entries.append(entry)
                        except Exception as e:
                            logger.warning("Skipping body measurement: %s", e)

                elif record_type == STEP_TYPE:
                    _, data = _parse_record(elem.attrib)
                    if data:
                        step_records.append(data)

                elif record_type == HEART_RATE_TYPE:
                    _, data = _parse_record(elem.attrib)
                    if data:
                        hr_records.append(data)

                elem.clear()

            else:
                # Clear elements we don't need to free memory
                elem.clear()

    except ET.ParseError as e:
        print(f"  Error parsing XML: {e}")
        logger.error("XML parse error in %s: %s", xml_path, e)
        return
    except FileNotFoundError:
        print(f"  Error: File not found: {xml_path}")
        return

    print(f"  Total XML elements processed: {total_records:,}")
    print(f"  Workouts: {len(workouts):,}")
    print(f"  Step readings: {len(step_records):,}")
    print(f"  Heart rate readings: {len(hr_records):,}")
    print(f"  Sleep records: {len(sleep_entries):,}")
    print(f"  Body measurements: {len(body_entries):,}")

    # Aggregate daily summaries
    daily_step_entries = _aggregate_daily_steps(step_records)
    daily_hr_entries = _aggregate_daily_heart_rate(hr_records)

    print(f"  Daily step summaries: {len(daily_step_entries):,}")
    print(f"  Daily HR summaries: {len(daily_hr_entries):,}")

    # Combine all entries
    all_entries = workouts + daily_step_entries + daily_hr_entries + sleep_entries + body_entries

    # Filter out already-processed
    new_entries = []
    skipped_duplicate = 0

    for entry in all_entries:
        if entry["id"] in processed_ids:
            skipped_duplicate += 1
            continue
        new_entries.append(entry)

    if not new_entries:
        print("  Nothing new — vault is up to date.")
        return

    # Flush to vault
    flush_entries(new_entries, vault_path, "health.jsonl")
    new_ids = [e["id"] for e in new_entries]
    append_processed_ids(vault_path, new_ids)

    # Summary stats by type
    type_counts = {}
    for e in new_entries:
        t = e.get("type", "unknown")
        type_counts[t] = type_counts.get(t, 0) + 1

    year_counts = {}
    for e in new_entries:
        y = e.get("year", 0)
        if y:
            year_counts[y] = year_counts.get(y, 0) + 1

    print()
    print(f"  {'=' * 45}")
    print(f"  Done! {len(new_entries):,} entries saved")
    print(f"  {'=' * 45}")
    for entry_type, count in sorted(type_counts.items()):
        print(f"    {entry_type}: {count:,}")
    if year_counts:
        for year in sorted(year_counts.keys()):
            print(f"    {year}: {year_counts[year]:,}")
    if skipped_duplicate:
        print(f"    Skipped (dupe):  {skipped_duplicate:,}")
    print()

    logger.info(
        "Apple Health import complete: %d new, %d duplicate",
        len(new_entries), skipped_duplicate,
    )
