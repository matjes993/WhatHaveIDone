"""Tests for the Health collector — Apple Health XML parsers."""

import json
import os
import tempfile

import pytest

from collectors.health import (
    _make_id,
    _normalize_activity_type,
    _safe_float,
    _parse_health_date,
    _find_export_xml,
    _parse_workout,
    _parse_record,
    _aggregate_daily_steps,
    _aggregate_daily_heart_rate,
    _build_sleep_entry,
    _build_body_measurement_entry,
    STEP_TYPE,
    HEART_RATE_TYPE,
    WEIGHT_TYPE,
    HEIGHT_TYPE,
    SLEEP_TYPE,
    run_import,
)


# ═══════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════

class TestMakeId:
    def test_deterministic(self):
        assert _make_id("workout", "running", "ts") == _make_id("workout", "running", "ts")

    def test_prefix(self):
        assert _make_id("workout", "x").startswith("health:workout:")

    def test_different_types(self):
        assert _make_id("workout", "x") != _make_id("daily", "x")


class TestNormalizeActivityType:
    def test_known_types(self):
        assert _normalize_activity_type("HKWorkoutActivityTypeRunning") == "running"
        assert _normalize_activity_type("HKWorkoutActivityTypeSwimming") == "swimming"
        assert _normalize_activity_type("HKWorkoutActivityTypeYoga") == "yoga"

    def test_strength_training_variants(self):
        assert _normalize_activity_type("HKWorkoutActivityTypeFunctionalStrengthTraining") == "strength-training"
        assert _normalize_activity_type("HKWorkoutActivityTypeTraditionalStrengthTraining") == "strength-training"

    def test_unknown_type_fallback(self):
        result = _normalize_activity_type("HKWorkoutActivityTypeSnowboarding")
        assert result == "snowboarding"

    def test_no_prefix_fallback(self):
        result = _normalize_activity_type("SomeWeirdActivity")
        assert result == "someweirdactivity"


class TestSafeFloat:
    def test_valid(self):
        assert _safe_float("3.14") == pytest.approx(3.14)

    def test_invalid(self):
        assert _safe_float("abc") == 0.0

    def test_none(self):
        assert _safe_float(None) == 0.0


class TestParseHealthDate:
    def test_apple_format(self):
        dt = _parse_health_date("2024-01-15 10:00:00 +0100")
        assert dt is not None
        assert dt.year == 2024

    def test_iso_format(self):
        dt = _parse_health_date("2024-01-15T10:00:00")
        assert dt is not None

    def test_empty(self):
        assert _parse_health_date("") is None

    def test_none(self):
        assert _parse_health_date(None) is None

    def test_garbage(self):
        assert _parse_health_date("not-a-date") is None


class TestFindExportXml:
    def test_direct_file(self):
        with tempfile.NamedTemporaryFile(suffix=".xml", delete=False) as f:
            f.write(b"<xml/>")
            path = f.name
        try:
            assert _find_export_xml(path) == path
        finally:
            os.unlink(path)

    def test_directory_with_export(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            xml_path = os.path.join(tmpdir, "export.xml")
            with open(xml_path, "w") as f:
                f.write("<xml/>")
            assert _find_export_xml(tmpdir) == xml_path

    def test_nested_directory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            nested = os.path.join(tmpdir, "apple_health_export")
            os.makedirs(nested)
            xml_path = os.path.join(nested, "export.xml")
            with open(xml_path, "w") as f:
                f.write("<xml/>")
            assert _find_export_xml(tmpdir) == xml_path

    def test_not_found(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            assert _find_export_xml(tmpdir) is None

    def test_nonexistent_path(self):
        assert _find_export_xml("/nonexistent/path") is None


# ═══════════════════════════════════════════════════════════════════════
# Record Parsers
# ═══════════════════════════════════════════════════════════════════════

class TestParseWorkout:
    def test_full_workout(self):
        attrib = {
            "workoutActivityType": "HKWorkoutActivityTypeRunning",
            "duration": "30.5",
            "totalDistance": "5.2",
            "totalEnergyBurned": "350",
            "sourceName": "Apple Watch",
            "startDate": "2024-01-15 10:00:00 +0100",
            "endDate": "2024-01-15 10:30:30 +0100",
        }
        entry = _parse_workout(attrib)
        assert entry is not None
        assert entry["activity"] == "running"
        assert entry["duration_minutes"] == pytest.approx(30.5)
        assert entry["distance_km"] == pytest.approx(5.2)
        assert entry["type"] == "workout"
        assert entry["year"] == 2024

    def test_no_activity_type(self):
        attrib = {"duration": "30", "totalDistance": "5"}
        assert _parse_workout(attrib) is None

    def test_missing_values_default_zero(self):
        attrib = {
            "workoutActivityType": "HKWorkoutActivityTypeYoga",
            "startDate": "2024-01-15 10:00:00 +0100",
        }
        entry = _parse_workout(attrib)
        assert entry is not None
        assert entry["duration_minutes"] == 0.0
        assert entry["distance_km"] == 0.0


class TestParseRecord:
    def test_step_record(self):
        attrib = {
            "type": STEP_TYPE,
            "value": "1234",
            "unit": "count",
            "sourceName": "iPhone",
            "startDate": "2024-01-15 10:00:00 +0100",
            "endDate": "2024-01-15 11:00:00 +0100",
        }
        record_type, data = _parse_record(attrib)
        assert record_type == STEP_TYPE
        assert data["value"] == 1234.0

    def test_uninteresting_type(self):
        attrib = {"type": "HKQuantityTypeIdentifierBloodPressureSystolic", "value": "120"}
        record_type, data = _parse_record(attrib)
        assert record_type is None
        assert data is None


class TestAggregateDailySteps:
    def test_aggregation(self):
        records = [
            {"date": "2024-01-15", "value": 3000},
            {"date": "2024-01-15", "value": 5000},
            {"date": "2024-01-16", "value": 8000},
        ]
        entries = _aggregate_daily_steps(records)
        assert len(entries) == 2
        day_15 = next(e for e in entries if e["date"] == "2024-01-15")
        assert day_15["steps"] == 8000
        assert day_15["type"] == "daily_summary"

    def test_empty(self):
        assert _aggregate_daily_steps([]) == []


class TestAggregateDailyHeartRate:
    def test_aggregation(self):
        records = [
            {"date": "2024-01-15", "value": 60},
            {"date": "2024-01-15", "value": 80},
            {"date": "2024-01-15", "value": 100},
        ]
        entries = _aggregate_daily_heart_rate(records)
        assert len(entries) == 1
        assert entries[0]["min_bpm"] == 60.0
        assert entries[0]["max_bpm"] == 100.0
        assert entries[0]["avg_bpm"] == 80.0
        assert entries[0]["readings"] == 3

    def test_empty(self):
        assert _aggregate_daily_heart_rate([]) == []

    def test_zero_values_skipped(self):
        records = [
            {"date": "2024-01-15", "value": 0},
            {"date": "2024-01-15", "value": 0},
        ]
        entries = _aggregate_daily_heart_rate(records)
        assert len(entries) == 0


class TestBuildSleepEntry:
    def test_full_sleep(self):
        attrib = {
            "value": "HKCategoryValueSleepAnalysisAsleepCore",
            "sourceName": "Apple Watch",
            "startDate": "2024-01-15 23:00:00 +0100",
            "endDate": "2024-01-16 07:00:00 +0100",
        }
        entry = _build_sleep_entry(attrib)
        assert entry is not None
        assert entry["type"] == "sleep"
        assert entry["sleep_type"] == "asleep"
        assert entry["duration_hours"] == pytest.approx(8.0, abs=0.1)

    def test_rem_sleep(self):
        attrib = {
            "value": "HKCategoryValueSleepAnalysisREM",
            "startDate": "2024-01-15 02:00:00 +0100",
            "endDate": "2024-01-15 03:00:00 +0100",
        }
        entry = _build_sleep_entry(attrib)
        assert entry["sleep_type"] == "rem"

    def test_deep_sleep(self):
        attrib = {
            "value": "HKCategoryValueSleepAnalysisAsleepDeep",
            "startDate": "2024-01-15 02:00:00 +0100",
            "endDate": "2024-01-15 03:00:00 +0100",
        }
        entry = _build_sleep_entry(attrib)
        # "Asleep" matches before "Deep" in the collector logic
        assert entry["sleep_type"] == "asleep"

    def test_in_bed(self):
        attrib = {
            "value": "HKCategoryValueSleepAnalysisInBed",
            "startDate": "2024-01-15 22:00:00 +0100",
            "endDate": "2024-01-16 06:00:00 +0100",
        }
        entry = _build_sleep_entry(attrib)
        assert entry["sleep_type"] == "in-bed"

    def test_missing_dates(self):
        attrib = {"value": "HKCategoryValueSleepAnalysisInBed"}
        assert _build_sleep_entry(attrib) is None


class TestBuildBodyMeasurementEntry:
    def test_weight(self):
        from datetime import datetime
        data = {
            "value": 75.5,
            "unit": "kg",
            "source": "iPhone",
            "date": "2024-01-15",
            "dt": datetime(2024, 1, 15),
        }
        entry = _build_body_measurement_entry(WEIGHT_TYPE, data)
        assert entry["measurement"] == "weight"
        assert entry["value"] == 75.5
        assert entry["unit"] == "kg"
        assert entry["type"] == "body_weight"

    def test_height(self):
        from datetime import datetime
        data = {
            "value": 180,
            "unit": "cm",
            "source": "iPhone",
            "date": "2024-01-15",
            "dt": datetime(2024, 1, 15),
        }
        entry = _build_body_measurement_entry(HEIGHT_TYPE, data)
        assert entry["measurement"] == "height"
        assert entry["type"] == "body_height"


# ═══════════════════════════════════════════════════════════════════════
# End-to-End Import
# ═══════════════════════════════════════════════════════════════════════

class TestRunImport:
    def _write_xml(self, path, records="", workouts=""):
        with open(path, "w") as f:
            f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
            f.write("<HealthData>\n")
            f.write(records)
            f.write(workouts)
            f.write("</HealthData>\n")

    def test_workout_import(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            xml_path = os.path.join(tmpdir, "export.xml")
            vault_root = os.path.join(tmpdir, "vaults")

            self._write_xml(
                xml_path,
                workouts=(
                    '<Workout workoutActivityType="HKWorkoutActivityTypeRunning" '
                    'duration="30" totalDistance="5" totalEnergyBurned="300" '
                    'sourceName="Watch" startDate="2024-01-15 10:00:00 +0100" '
                    'endDate="2024-01-15 10:30:00 +0100"/>\n'
                ),
            )

            run_import(xml_path, config={"vault_root": vault_root})

            jsonl = os.path.join(vault_root, "Health", "health.jsonl")
            assert os.path.isfile(jsonl)
            with open(jsonl) as f:
                entries = [json.loads(line) for line in f if line.strip()]
            assert len(entries) == 1
            assert entries[0]["activity"] == "running"

    def test_step_aggregation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            xml_path = os.path.join(tmpdir, "export.xml")
            vault_root = os.path.join(tmpdir, "vaults")

            records = ""
            for i in range(3):
                records += (
                    f'<Record type="{STEP_TYPE}" value="1000" unit="count" '
                    f'sourceName="iPhone" startDate="2024-01-15 {10+i}:00:00 +0000" '
                    f'endDate="2024-01-15 {11+i}:00:00 +0000"/>\n'
                )
            self._write_xml(xml_path, records=records)

            run_import(xml_path, config={"vault_root": vault_root})

            jsonl = os.path.join(vault_root, "Health", "health.jsonl")
            with open(jsonl) as f:
                entries = [json.loads(line) for line in f if line.strip()]
            # 3 step records on same day => 1 daily summary
            assert len(entries) == 1
            assert entries[0]["steps"] == 3000

    def test_deduplication(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            xml_path = os.path.join(tmpdir, "export.xml")
            vault_root = os.path.join(tmpdir, "vaults")

            self._write_xml(
                xml_path,
                workouts=(
                    '<Workout workoutActivityType="HKWorkoutActivityTypeYoga" '
                    'duration="60" startDate="2024-01-15 08:00:00 +0100" '
                    'endDate="2024-01-15 09:00:00 +0100"/>\n'
                ),
            )

            run_import(xml_path, config={"vault_root": vault_root})
            run_import(xml_path, config={"vault_root": vault_root})

            jsonl = os.path.join(vault_root, "Health", "health.jsonl")
            with open(jsonl) as f:
                entries = [json.loads(line) for line in f if line.strip()]
            assert len(entries) == 1

    def test_empty_xml(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            xml_path = os.path.join(tmpdir, "export.xml")
            vault_root = os.path.join(tmpdir, "vaults")
            self._write_xml(xml_path)

            run_import(xml_path, config={"vault_root": vault_root})

            jsonl = os.path.join(vault_root, "Health", "health.jsonl")
            assert not os.path.isfile(jsonl)
