"""Tests for the Podcasts collector — CSV and SQLite (Podcast Addict) parsers."""

import csv
import json
import os
import sqlite3
import tempfile

import pytest

from collectors.podcasts import (
    _normalize_columns,
    _get,
    _read_csv,
    _make_id,
    _seconds_to_readable,
    _safe_int,
    _safe_float,
    _parse_date,
    _duration_str_to_seconds,
    _parse_episode_row,
    _parse_db_episode,
    run_import,
)


# ═══════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════

class TestMakeId:
    def test_deterministic(self):
        assert _make_id("podcast", "episode", "date") == _make_id("podcast", "episode", "date")

    def test_prefix(self):
        assert _make_id("x").startswith("podcasts:")


class TestSecondsToReadable:
    def test_minutes_seconds(self):
        assert _seconds_to_readable(2730) == "45:30"

    def test_with_hours(self):
        assert _seconds_to_readable(3661) == "1:01:01"

    def test_zero(self):
        assert _seconds_to_readable(0) == "0:00"

    def test_negative(self):
        assert _seconds_to_readable(-10) == "0:00"

    def test_none(self):
        assert _seconds_to_readable(None) == "0:00"

    def test_under_minute(self):
        assert _seconds_to_readable(45) == "0:45"


class TestDurationStrToSeconds:
    def test_mm_ss(self):
        assert _duration_str_to_seconds("45:30") == 2730

    def test_hh_mm_ss(self):
        assert _duration_str_to_seconds("1:05:30") == 3930

    def test_seconds_only(self):
        assert _duration_str_to_seconds("120") == 120

    def test_empty(self):
        assert _duration_str_to_seconds("") == 0

    def test_none(self):
        assert _duration_str_to_seconds(None) == 0

    def test_garbage(self):
        assert _duration_str_to_seconds("abc") == 0


class TestParseDate:
    def test_iso(self):
        dt = _parse_date("2024-01-15")
        assert dt is not None
        assert dt.year == 2024

    def test_iso_with_time(self):
        dt = _parse_date("2024-01-15T10:30:00")
        assert dt is not None

    def test_empty(self):
        assert _parse_date("") is None

    def test_none(self):
        assert _parse_date(None) is None

    def test_garbage(self):
        assert _parse_date("not-a-date") is None


# ═══════════════════════════════════════════════════════════════════════
# CSV Episode Parser
# ═══════════════════════════════════════════════════════════════════════

class TestParseEpisodeRow:
    @pytest.fixture
    def col_map(self):
        headers = ["Podcast", "Episode", "Date Listened", "Duration", "URL", "Author"]
        return _normalize_columns(headers)

    def test_full_row(self, col_map):
        row = ["The Daily", "Episode 101", "2024-01-15", "30:00", "https://example.com/ep101", "NYT"]
        entry = _parse_episode_row(row, col_map)
        assert entry is not None
        assert entry["podcast_name"] == "The Daily"
        assert entry["episode_title"] == "Episode 101"
        assert entry["duration_seconds"] == 1800
        assert entry["year"] == 2024

    def test_no_episode_title(self, col_map):
        row = ["Podcast", "", "2024-01-15", "30:00", "", ""]
        assert _parse_episode_row(row, col_map) is None

    def test_no_duration(self, col_map):
        row = ["Podcast", "Episode", "2024-01-15", "", "", ""]
        entry = _parse_episode_row(row, col_map)
        assert entry is not None
        assert entry["duration_seconds"] == 0


# ═══════════════════════════════════════════════════════════════════════
# SQLite Episode Parser
# ═══════════════════════════════════════════════════════════════════════

class TestParseDbEpisode:
    def test_full_episode(self):
        episode = {
            "name": "Great Episode",
            "description": "A great description",
            "url": "https://example.com/ep",
            "date_published": "2024-01-15",
            "duration": 1800000,  # 1800 seconds in ms
            "playback_position": 1800000,
            "is_played": 1,
        }
        podcast = {"name": "My Podcast", "author": "Host Name"}
        entry = _parse_db_episode(episode, podcast)
        assert entry is not None
        assert entry["podcast_name"] == "My Podcast"
        assert entry["episode_title"] == "Great Episode"
        assert entry["progress"] == 1.0
        assert entry["sources"] == ["podcast-addict"]

    def test_no_name(self):
        episode = {"name": "", "duration": 1000, "playback_position": 0, "is_played": 0}
        podcast = {"name": "P", "author": "A"}
        assert _parse_db_episode(episode, podcast) is None

    def test_timestamp_ms_date(self):
        # 1705305600000 ms = 2024-01-15T12:00:00Z approx
        episode = {
            "name": "Ep",
            "date_published": 1705305600000,
            "duration": 60000,
            "playback_position": 0,
            "is_played": 0,
        }
        podcast = {"name": "P", "author": "A"}
        entry = _parse_db_episode(episode, podcast)
        assert entry is not None
        assert entry["year"] == 2024

    def test_partial_playback(self):
        episode = {
            "name": "Half Listen",
            "date_published": "2024-01-15",
            "duration": 60000,
            "playback_position": 30000,
            "is_played": 0,
        }
        podcast = {"name": "P", "author": "A"}
        entry = _parse_db_episode(episode, podcast)
        assert entry["progress"] == pytest.approx(0.5, abs=0.05)

    def test_description_truncation(self):
        episode = {
            "name": "Ep",
            "description": "x" * 1000,
            "date_published": "2024-01-15",
            "duration": 1000,
            "playback_position": 0,
            "is_played": 1,
        }
        podcast = {"name": "P", "author": "A"}
        entry = _parse_db_episode(episode, podcast)
        assert len(entry["description"]) == 500


# ═══════════════════════════════════════════════════════════════════════
# End-to-End Import
# ═══════════════════════════════════════════════════════════════════════

class TestRunImportCsv:
    def test_csv_import(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, "podcasts.csv")
            vault_root = os.path.join(tmpdir, "vaults")

            with open(csv_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["Podcast", "Episode", "Date Listened", "Duration"])
                writer.writerow(["Show A", "Ep 1", "2024-01-15", "30:00"])
                writer.writerow(["Show B", "Ep 2", "2024-01-16", "45:00"])

            run_import(csv_path, config={"vault_root": vault_root})

            jsonl = os.path.join(vault_root, "Podcasts", "podcasts.jsonl")
            assert os.path.isfile(jsonl)
            with open(jsonl) as f:
                entries = [json.loads(line) for line in f if line.strip()]
            assert len(entries) == 2

    def test_csv_deduplication(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, "podcasts.csv")
            vault_root = os.path.join(tmpdir, "vaults")

            with open(csv_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["Podcast", "Episode", "Date Listened"])
                writer.writerow(["Show", "Ep", "2024-01-15"])

            run_import(csv_path, config={"vault_root": vault_root})
            run_import(csv_path, config={"vault_root": vault_root})

            jsonl = os.path.join(vault_root, "Podcasts", "podcasts.jsonl")
            with open(jsonl) as f:
                entries = [json.loads(line) for line in f if line.strip()]
            assert len(entries) == 1


class TestRunImportDb:
    def _create_test_db(self, db_path):
        conn = sqlite3.connect(db_path)
        conn.execute(
            "CREATE TABLE podcasts ("
            "_id INTEGER PRIMARY KEY, name TEXT, author TEXT, url TEXT, description TEXT)"
        )
        conn.execute(
            "CREATE TABLE episodes ("
            "_id INTEGER PRIMARY KEY, name TEXT, description TEXT, url TEXT, "
            "podcast_id INTEGER, date_published TEXT, duration INTEGER, "
            "playback_position INTEGER, is_played INTEGER)"
        )
        conn.execute(
            "INSERT INTO podcasts VALUES (1, 'Test Podcast', 'Host', 'http://example.com', 'Desc')"
        )
        conn.execute(
            "INSERT INTO episodes VALUES (1, 'Episode 1', 'Ep desc', 'http://ep.com', 1, "
            "'2024-01-15', 1800000, 1800000, 1)"
        )
        conn.commit()
        conn.close()

    def test_db_import(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "backup.db")
            vault_root = os.path.join(tmpdir, "vaults")
            self._create_test_db(db_path)

            run_import(db_path, config={"vault_root": vault_root})

            jsonl = os.path.join(vault_root, "Podcasts", "podcasts.jsonl")
            assert os.path.isfile(jsonl)
            with open(jsonl) as f:
                entries = [json.loads(line) for line in f if line.strip()]
            assert len(entries) == 1
            assert entries[0]["podcast_name"] == "Test Podcast"

    def test_db_deduplication(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "backup.db")
            vault_root = os.path.join(tmpdir, "vaults")
            self._create_test_db(db_path)

            run_import(db_path, config={"vault_root": vault_root})
            run_import(db_path, config={"vault_root": vault_root})

            jsonl = os.path.join(vault_root, "Podcasts", "podcasts.jsonl")
            with open(jsonl) as f:
                entries = [json.loads(line) for line in f if line.strip()]
            assert len(entries) == 1
