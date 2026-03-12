"""Tests for the YouTube collector — watch and search history parsers."""

import hashlib
import json
import os
import tempfile

import pytest

from collectors.youtube import (
    _make_id,
    _extract_video_id,
    _parse_timestamp,
    _find_history_files,
    _parse_watch_entry,
    _parse_search_entry,
    _load_json,
    run_import,
)


# ═══════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════

class TestMakeId:
    def test_deterministic(self):
        assert _make_id("watch", "url", "ts") == _make_id("watch", "url", "ts")

    def test_prefix(self):
        assert _make_id("watch", "url").startswith("youtube:watch:")

    def test_different_types(self):
        assert _make_id("watch", "x") != _make_id("search", "x")


class TestExtractVideoId:
    def test_standard_url(self):
        assert _extract_video_id("https://www.youtube.com/watch?v=dQw4w9WgXcQ") == "dQw4w9WgXcQ"

    def test_short_url(self):
        assert _extract_video_id("https://youtu.be/dQw4w9WgXcQ") == "dQw4w9WgXcQ"

    def test_url_with_params(self):
        assert _extract_video_id("https://www.youtube.com/watch?v=dQw4w9WgXcQ&t=42s") == "dQw4w9WgXcQ"

    def test_empty(self):
        assert _extract_video_id("") == ""

    def test_none(self):
        assert _extract_video_id(None) == ""

    def test_no_id(self):
        assert _extract_video_id("https://www.youtube.com/") == ""


class TestParseTimestamp:
    def test_iso_with_z(self):
        dt = _parse_timestamp("2024-01-15T10:30:00Z")
        assert dt is not None
        assert dt.year == 2024
        assert dt.month == 1
        assert dt.hour == 10

    def test_iso_with_microseconds(self):
        dt = _parse_timestamp("2024-01-15T10:30:00.123456Z")
        assert dt is not None

    def test_iso_without_z(self):
        dt = _parse_timestamp("2024-01-15T10:30:00")
        assert dt is not None

    def test_empty(self):
        assert _parse_timestamp("") is None

    def test_none(self):
        assert _parse_timestamp(None) is None

    def test_garbage(self):
        assert _parse_timestamp("not-a-date") is None


# ═══════════════════════════════════════════════════════════════════════
# File Finder
# ═══════════════════════════════════════════════════════════════════════

class TestFindHistoryFiles:
    def test_direct_watch_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            watch_file = os.path.join(tmpdir, "watch-history.json")
            with open(watch_file, "w") as f:
                f.write("[]")
            watch, search = _find_history_files(watch_file)
            assert watch == watch_file
            assert search is None

    def test_directory_with_both(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            watch_file = os.path.join(tmpdir, "watch-history.json")
            search_file = os.path.join(tmpdir, "search-history.json")
            for fpath in (watch_file, search_file):
                with open(fpath, "w") as f:
                    f.write("[]")
            watch, search = _find_history_files(tmpdir)
            assert watch is not None
            assert search is not None

    def test_nested_directory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            history_dir = os.path.join(tmpdir, "history")
            os.makedirs(history_dir)
            watch_file = os.path.join(history_dir, "watch-history.json")
            with open(watch_file, "w") as f:
                f.write("[]")
            watch, search = _find_history_files(tmpdir)
            assert watch is not None

    def test_nonexistent_path(self):
        watch, search = _find_history_files("/nonexistent/path")
        assert watch is None
        assert search is None

    def test_search_sibling(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            watch_file = os.path.join(tmpdir, "watch-history.json")
            search_file = os.path.join(tmpdir, "search-history.json")
            with open(watch_file, "w") as f:
                f.write("[]")
            with open(search_file, "w") as f:
                f.write("[]")
            watch, search = _find_history_files(watch_file)
            assert watch == watch_file
            assert search == search_file


# ═══════════════════════════════════════════════════════════════════════
# Entry Parsers
# ═══════════════════════════════════════════════════════════════════════

class TestParseWatchEntry:
    def test_full_entry(self):
        item = {
            "title": "Watched Some Video",
            "titleUrl": "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
            "time": "2024-01-15T10:30:00Z",
            "subtitles": [{"name": "Channel Name"}],
        }
        entry = _parse_watch_entry(item)
        assert entry is not None
        assert entry["title"] == "Some Video"  # "Watched " stripped
        assert entry["video_id"] == "dQw4w9WgXcQ"
        assert entry["channel"] == "Channel Name"
        assert entry["type"] == "watch"
        assert entry["year"] == 2024

    def test_no_title(self):
        assert _parse_watch_entry({"time": "2024-01-15T10:00:00Z"}) is None

    def test_no_watched_prefix(self):
        item = {"title": "Some Video", "titleUrl": "", "time": ""}
        entry = _parse_watch_entry(item)
        assert entry is not None
        assert entry["title"] == "Some Video"

    def test_no_subtitles(self):
        item = {"title": "Watched Test", "titleUrl": "", "time": ""}
        entry = _parse_watch_entry(item)
        assert entry["channel"] == ""


class TestParseSearchEntry:
    def test_full_entry(self):
        item = {
            "title": "Searched for python tutorial",
            "time": "2024-01-15T10:30:00Z",
        }
        entry = _parse_search_entry(item)
        assert entry is not None
        assert entry["query"] == "python tutorial"
        assert entry["type"] == "search"
        assert entry["year"] == 2024

    def test_no_title(self):
        assert _parse_search_entry({"time": "2024-01-01T00:00:00Z"}) is None

    def test_no_prefix(self):
        item = {"title": "raw query", "time": ""}
        entry = _parse_search_entry(item)
        assert entry["query"] == "raw query"


# ═══════════════════════════════════════════════════════════════════════
# End-to-End Import
# ═══════════════════════════════════════════════════════════════════════

class TestRunImport:
    def test_basic_import(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            vault_root = os.path.join(tmpdir, "vaults")
            watch_file = os.path.join(tmpdir, "watch-history.json")

            data = [
                {
                    "title": "Watched Video One",
                    "titleUrl": "https://www.youtube.com/watch?v=abcdefghijk",
                    "time": "2024-01-15T10:00:00Z",
                    "subtitles": [{"name": "Channel A"}],
                },
                {
                    "title": "Watched Video Two",
                    "titleUrl": "https://www.youtube.com/watch?v=xyzxyzxyzxy",
                    "time": "2024-01-16T11:00:00Z",
                },
            ]
            with open(watch_file, "w") as f:
                json.dump(data, f)

            run_import(watch_file, config={"vault_root": vault_root})

            vault_path = os.path.join(vault_root, "YouTube")
            jsonl = os.path.join(vault_path, "youtube.jsonl")
            assert os.path.isfile(jsonl)

            with open(jsonl) as f:
                entries = [json.loads(line) for line in f if line.strip()]
            assert len(entries) == 2

    def test_deduplication(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            vault_root = os.path.join(tmpdir, "vaults")
            watch_file = os.path.join(tmpdir, "watch-history.json")

            data = [
                {
                    "title": "Watched Video",
                    "titleUrl": "https://www.youtube.com/watch?v=abcdefghijk",
                    "time": "2024-01-15T10:00:00Z",
                },
            ]
            with open(watch_file, "w") as f:
                json.dump(data, f)

            run_import(watch_file, config={"vault_root": vault_root})
            run_import(watch_file, config={"vault_root": vault_root})

            jsonl = os.path.join(vault_root, "YouTube", "youtube.jsonl")
            with open(jsonl) as f:
                entries = [json.loads(line) for line in f if line.strip()]
            assert len(entries) == 1

    def test_with_search_history(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            vault_root = os.path.join(tmpdir, "vaults")
            watch_file = os.path.join(tmpdir, "watch-history.json")
            search_file = os.path.join(tmpdir, "search-history.json")

            with open(watch_file, "w") as f:
                json.dump([{"title": "Watched V", "titleUrl": "", "time": "2024-01-01T00:00:00Z"}], f)
            with open(search_file, "w") as f:
                json.dump([{"title": "Searched for test", "time": "2024-01-01T00:00:00Z"}], f)

            run_import(tmpdir, config={"vault_root": vault_root})

            jsonl = os.path.join(vault_root, "YouTube", "youtube.jsonl")
            with open(jsonl) as f:
                entries = [json.loads(line) for line in f if line.strip()]
            assert len(entries) == 2
            types = {e["type"] for e in entries}
            assert "watch" in types
            assert "search" in types
