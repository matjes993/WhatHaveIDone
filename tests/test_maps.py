"""Tests for the Maps collector — Google Semantic Location History parsers."""

import json
import os
import tempfile

import pytest

from collectors.maps import (
    _make_id,
    _e7_to_decimal,
    _normalize_activity_type,
    _normalize_semantic_type,
    _parse_duration_minutes,
    _parse_year_month,
    _find_timeline_files,
    _load_json,
    _parse_place_visit,
    _parse_activity_segment,
    run_import,
)


# ═══════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════

class TestMakeId:
    def test_deterministic(self):
        assert _make_id("visit", "place", "ts") == _make_id("visit", "place", "ts")

    def test_prefix(self):
        assert _make_id("visit", "x").startswith("maps:visit:")

    def test_different_types(self):
        assert _make_id("visit", "x") != _make_id("activity", "x")


class TestE7ToDecimal:
    def test_positive(self):
        result = _e7_to_decimal(523456789)
        assert result == pytest.approx(52.3456789)

    def test_negative(self):
        result = _e7_to_decimal(-1234567)
        assert result == pytest.approx(-0.1234567)

    def test_zero(self):
        assert _e7_to_decimal(0) == 0.0

    def test_none(self):
        assert _e7_to_decimal(None) == 0.0


class TestNormalizeActivityType:
    def test_known_types(self):
        assert _normalize_activity_type("IN_PASSENGER_VEHICLE") == "driving"
        assert _normalize_activity_type("WALKING") == "walking"
        assert _normalize_activity_type("CYCLING") == "cycling"
        assert _normalize_activity_type("IN_BUS") == "bus"
        assert _normalize_activity_type("IN_TRAIN") == "train"
        assert _normalize_activity_type("FLYING") == "flying"

    def test_case_insensitive(self):
        assert _normalize_activity_type("walking") == "walking"
        assert _normalize_activity_type("Walking") == "walking"

    def test_unknown_fallback(self):
        result = _normalize_activity_type("IN_KAYAK")
        assert result == "kayak"

    def test_empty(self):
        assert _normalize_activity_type("") == "unknown"

    def test_none(self):
        assert _normalize_activity_type(None) == "unknown"


class TestNormalizeSemanticType:
    def test_type_prefix(self):
        assert _normalize_semantic_type("TYPE_CAFE") == "cafe"
        assert _normalize_semantic_type("TYPE_HOME") == "home"
        assert _normalize_semantic_type("TYPE_WORK") == "work"

    def test_no_prefix(self):
        assert _normalize_semantic_type("RESTAURANT") == "restaurant"

    def test_empty(self):
        assert _normalize_semantic_type("") == ""

    def test_none(self):
        assert _normalize_semantic_type(None) == ""


class TestParseDurationMinutes:
    def test_basic(self):
        result = _parse_duration_minutes("2024-01-15T10:00:00Z", "2024-01-15T10:30:00Z")
        assert result == 30

    def test_cross_day(self):
        result = _parse_duration_minutes("2024-01-15T23:00:00Z", "2024-01-16T01:00:00Z")
        assert result == 120

    def test_empty_start(self):
        assert _parse_duration_minutes("", "2024-01-15T10:00:00Z") == 0

    def test_empty_end(self):
        assert _parse_duration_minutes("2024-01-15T10:00:00Z", "") == 0

    def test_invalid(self):
        assert _parse_duration_minutes("invalid", "also-invalid") == 0

    def test_millisecond_format(self):
        result = _parse_duration_minutes("2024-01-15T10:00:00.000Z", "2024-01-15T10:30:00.000Z")
        assert result == 30


class TestParseYearMonth:
    def test_valid(self):
        assert _parse_year_month("2024-01-15T10:00:00Z") == (2024, 1)

    def test_date_only(self):
        assert _parse_year_month("2024-06-15") == (2024, 6)

    def test_empty(self):
        assert _parse_year_month("") == (0, 0)

    def test_none(self):
        assert _parse_year_month(None) == (0, 0)


# ═══════════════════════════════════════════════════════════════════════
# File Finder
# ═══════════════════════════════════════════════════════════════════════

class TestFindTimelineFiles:
    def test_direct_file(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            f.write(b"{}")
            path = f.name
        try:
            result = _find_timeline_files(path)
            assert result == [path]
        finally:
            os.unlink(path)

    def test_directory_with_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            for name in ("2024_JANUARY.json", "2024_FEBRUARY.json"):
                with open(os.path.join(tmpdir, name), "w") as f:
                    f.write("{}")
            result = _find_timeline_files(tmpdir)
            assert len(result) == 2

    def test_skips_records_json(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "Records.json"), "w") as f:
                f.write("[]")
            with open(os.path.join(tmpdir, "2024_JANUARY.json"), "w") as f:
                f.write("{}")
            result = _find_timeline_files(tmpdir)
            assert len(result) == 1
            assert "Records" not in result[0]

    def test_skips_settings_json(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "Settings.json"), "w") as f:
                f.write("{}")
            result = _find_timeline_files(tmpdir)
            assert len(result) == 0

    def test_nonexistent(self):
        result = _find_timeline_files("/nonexistent/path")
        assert result == []

    def test_recursive(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            subdir = os.path.join(tmpdir, "2024")
            os.makedirs(subdir)
            with open(os.path.join(subdir, "2024_MARCH.json"), "w") as f:
                f.write("{}")
            result = _find_timeline_files(tmpdir)
            assert len(result) == 1


# ═══════════════════════════════════════════════════════════════════════
# Entry Parsers
# ═══════════════════════════════════════════════════════════════════════

class TestParsePlaceVisit:
    def test_full_visit(self):
        visit = {
            "location": {
                "name": "Central Park",
                "address": "New York, NY",
                "placeId": "ChIJ123",
                "latitudeE7": 407812800,
                "longitudeE7": -739066100,
                "semanticType": "TYPE_PARK",
            },
            "duration": {
                "startTimestamp": "2024-01-15T10:00:00Z",
                "endTimestamp": "2024-01-15T12:00:00Z",
            },
            "placeConfidence": "HIGH",
        }
        entry = _parse_place_visit(visit)
        assert entry is not None
        assert entry["type"] == "visit"
        assert entry["place_name"] == "Central Park"
        assert entry["lat"] == pytest.approx(40.78128)
        assert entry["duration_minutes"] == 120
        assert entry["year"] == 2024

    def test_no_start_time(self):
        visit = {
            "location": {"name": "Somewhere"},
            "duration": {"endTimestamp": "2024-01-15T12:00:00Z"},
        }
        assert _parse_place_visit(visit) is None

    def test_minimal_visit(self):
        visit = {
            "location": {},
            "duration": {"startTimestamp": "2024-01-15T10:00:00Z"},
        }
        entry = _parse_place_visit(visit)
        assert entry is not None
        assert entry["place_name"] == ""


class TestParseActivitySegment:
    def test_driving(self):
        segment = {
            "activityType": "IN_PASSENGER_VEHICLE",
            "distance": 15000,
            "duration": {
                "startTimestamp": "2024-01-15T10:00:00Z",
                "endTimestamp": "2024-01-15T10:30:00Z",
            },
        }
        entry = _parse_activity_segment(segment)
        assert entry is not None
        assert entry["type"] == "activity"
        assert entry["activity_type"] == "driving"
        assert entry["distance_meters"] == 15000
        assert entry["duration_minutes"] == 30

    def test_walking(self):
        segment = {
            "activityType": "WALKING",
            "distance": 2000,
            "duration": {
                "startTimestamp": "2024-01-15T10:00:00Z",
                "endTimestamp": "2024-01-15T10:20:00Z",
            },
        }
        entry = _parse_activity_segment(segment)
        assert entry["activity_type"] == "walking"

    def test_no_start_time(self):
        segment = {
            "activityType": "WALKING",
            "duration": {"endTimestamp": "2024-01-15T10:30:00Z"},
        }
        assert _parse_activity_segment(segment) is None

    def test_no_distance(self):
        segment = {
            "activityType": "WALKING",
            "duration": {
                "startTimestamp": "2024-01-15T10:00:00Z",
                "endTimestamp": "2024-01-15T10:30:00Z",
            },
        }
        entry = _parse_activity_segment(segment)
        assert entry is not None
        assert entry["distance_meters"] == 0


# ═══════════════════════════════════════════════════════════════════════
# End-to-End Import
# ═══════════════════════════════════════════════════════════════════════

class TestRunImport:
    def test_basic_import(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            json_path = os.path.join(tmpdir, "2024_JANUARY.json")
            vault_root = os.path.join(tmpdir, "vaults")

            data = {
                "timelineObjects": [
                    {
                        "placeVisit": {
                            "location": {"name": "Coffee Shop", "placeId": "place1"},
                            "duration": {
                                "startTimestamp": "2024-01-15T09:00:00Z",
                                "endTimestamp": "2024-01-15T09:30:00Z",
                            },
                        }
                    },
                    {
                        "activitySegment": {
                            "activityType": "WALKING",
                            "distance": 1500,
                            "duration": {
                                "startTimestamp": "2024-01-15T09:30:00Z",
                                "endTimestamp": "2024-01-15T09:45:00Z",
                            },
                        }
                    },
                ]
            }
            with open(json_path, "w") as f:
                json.dump(data, f)

            run_import(json_path, config={"vault_root": vault_root})

            jsonl = os.path.join(vault_root, "Maps", "maps.jsonl")
            assert os.path.isfile(jsonl)
            with open(jsonl) as fh:
                entries = [json.loads(line) for line in fh if line.strip()]
            assert len(entries) == 2
            types = {e["type"] for e in entries}
            assert "visit" in types
            assert "activity" in types

    def test_deduplication(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            json_path = os.path.join(tmpdir, "2024_JANUARY.json")
            vault_root = os.path.join(tmpdir, "vaults")

            data = {
                "timelineObjects": [
                    {
                        "placeVisit": {
                            "location": {"name": "Home", "placeId": "home1"},
                            "duration": {
                                "startTimestamp": "2024-01-15T18:00:00Z",
                                "endTimestamp": "2024-01-16T08:00:00Z",
                            },
                        }
                    },
                ]
            }
            with open(json_path, "w") as f:
                json.dump(data, f)

            run_import(json_path, config={"vault_root": vault_root})
            run_import(json_path, config={"vault_root": vault_root})

            jsonl = os.path.join(vault_root, "Maps", "maps.jsonl")
            with open(jsonl) as fh:
                entries = [json.loads(line) for line in fh if line.strip()]
            assert len(entries) == 1

    def test_multiple_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            vault_root = os.path.join(tmpdir, "vaults")

            for month_name, start_date in [("2024_JANUARY", "2024-01-15"), ("2024_FEBRUARY", "2024-02-15")]:
                data = {
                    "timelineObjects": [
                        {
                            "placeVisit": {
                                "location": {"name": f"Place in {month_name}"},
                                "duration": {
                                    "startTimestamp": f"{start_date}T10:00:00Z",
                                    "endTimestamp": f"{start_date}T11:00:00Z",
                                },
                            }
                        }
                    ]
                }
                with open(os.path.join(tmpdir, f"{month_name}.json"), "w") as f:
                    json.dump(data, f)

            run_import(tmpdir, config={"vault_root": vault_root})

            jsonl = os.path.join(vault_root, "Maps", "maps.jsonl")
            with open(jsonl) as fh:
                entries = [json.loads(line) for line in fh if line.strip()]
            assert len(entries) == 2

    def test_empty_timeline(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            json_path = os.path.join(tmpdir, "empty.json")
            vault_root = os.path.join(tmpdir, "vaults")

            with open(json_path, "w") as f:
                json.dump({"timelineObjects": []}, f)

            run_import(json_path, config={"vault_root": vault_root})

            jsonl = os.path.join(vault_root, "Maps", "maps.jsonl")
            assert not os.path.isfile(jsonl)
