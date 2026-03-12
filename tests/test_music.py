"""Tests for the Music collector — Spotify streaming history parsers."""

import json
import os
import tempfile

import pytest

from collectors.music import (
    _make_id,
    _ms_to_readable,
    _parse_timestamp,
    _find_streaming_files,
    _load_json,
    _is_extended_format,
    _parse_extended_entry,
    _parse_legacy_entry,
    run_import,
)


# ═══════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════

class TestMakeId:
    def test_deterministic(self):
        assert _make_id("track", "artist", "ts") == _make_id("track", "artist", "ts")

    def test_prefix(self):
        assert _make_id("x").startswith("music:spotify:")

    def test_different(self):
        assert _make_id("a") != _make_id("b")


class TestMsToReadable:
    def test_standard(self):
        assert _ms_to_readable(213000) == "3:33"

    def test_with_hours(self):
        assert _ms_to_readable(3600000) == "1:00:00"

    def test_zero(self):
        assert _ms_to_readable(0) == "0:00"

    def test_negative(self):
        assert _ms_to_readable(-1000) == "0:00"

    def test_none(self):
        assert _ms_to_readable(None) == "0:00"

    def test_under_minute(self):
        assert _ms_to_readable(30000) == "0:30"


class TestParseTimestamp:
    def test_extended_format(self):
        dt = _parse_timestamp("2024-01-15T10:30:00Z")
        assert dt is not None
        assert dt.year == 2024

    def test_legacy_format(self):
        dt = _parse_timestamp("2024-01-15 10:30")
        assert dt is not None

    def test_empty(self):
        assert _parse_timestamp("") is None

    def test_none(self):
        assert _parse_timestamp(None) is None

    def test_garbage(self):
        assert _parse_timestamp("garbage") is None


class TestIsExtendedFormat:
    def test_extended_with_ts(self):
        assert _is_extended_format({"ts": "2024-01-01T00:00:00Z"})

    def test_extended_with_track_name(self):
        assert _is_extended_format({"master_metadata_track_name": "Song"})

    def test_legacy(self):
        assert not _is_extended_format({"trackName": "Song", "endTime": "2024-01-01"})


# ═══════════════════════════════════════════════════════════════════════
# File Finder
# ═══════════════════════════════════════════════════════════════════════

class TestFindStreamingFiles:
    def test_direct_file(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            f.write(b"[]")
            path = f.name
        try:
            result = _find_streaming_files(path)
            assert result == [path]
        finally:
            os.unlink(path)

    def test_directory_with_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            for name in ("StreamingHistory_music_0.json", "StreamingHistory_music_1.json"):
                with open(os.path.join(tmpdir, name), "w") as f:
                    f.write("[]")
            result = _find_streaming_files(tmpdir)
            assert len(result) == 2

    def test_directory_no_history(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "other.json"), "w") as f:
                f.write("[]")
            result = _find_streaming_files(tmpdir)
            assert len(result) == 0

    def test_nonexistent(self):
        result = _find_streaming_files("/nonexistent/path")
        assert result == []

    def test_subdirectory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            subdir = os.path.join(tmpdir, "Spotify Account Data")
            os.makedirs(subdir)
            with open(os.path.join(subdir, "StreamingHistory0.json"), "w") as f:
                f.write("[]")
            result = _find_streaming_files(tmpdir)
            assert len(result) == 1


# ═══════════════════════════════════════════════════════════════════════
# Entry Parsers
# ═══════════════════════════════════════════════════════════════════════

class TestParseExtendedEntry:
    def test_full_entry(self):
        item = {
            "master_metadata_track_name": "Bohemian Rhapsody",
            "master_metadata_album_artist_name": "Queen",
            "master_metadata_album_album_name": "A Night at the Opera",
            "ts": "2024-01-15T10:30:00Z",
            "ms_played": 354000,
            "platform": "android",
            "skipped": False,
            "spotify_track_uri": "spotify:track:abc123",
        }
        entry = _parse_extended_entry(item)
        assert entry is not None
        assert entry["track"] == "Bohemian Rhapsody"
        assert entry["artist"] == "Queen"
        assert entry["album"] == "A Night at the Opera"
        assert entry["duration_ms"] == 354000
        assert entry["year"] == 2024

    def test_no_track_name(self):
        item = {"ts": "2024-01-15T10:30:00Z", "ms_played": 100}
        assert _parse_extended_entry(item) is None

    def test_skipped(self):
        item = {
            "master_metadata_track_name": "Song",
            "master_metadata_album_artist_name": "Artist",
            "ts": "2024-01-15T10:30:00Z",
            "ms_played": 5000,
            "skipped": True,
        }
        entry = _parse_extended_entry(item)
        assert entry["skipped"] is True


class TestParseLegacyEntry:
    def test_full_entry(self):
        item = {
            "trackName": "Hotel California",
            "artistName": "Eagles",
            "endTime": "2024-01-15 10:30",
            "msPlayed": 391000,
        }
        entry = _parse_legacy_entry(item)
        assert entry is not None
        assert entry["track"] == "Hotel California"
        assert entry["artist"] == "Eagles"
        assert entry["album"] == ""

    def test_no_track(self):
        item = {"artistName": "Eagles", "endTime": "2024-01-15 10:30"}
        assert _parse_legacy_entry(item) is None


# ═══════════════════════════════════════════════════════════════════════
# End-to-End Import
# ═══════════════════════════════════════════════════════════════════════

class TestRunImport:
    def test_extended_import(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            vault_root = os.path.join(tmpdir, "vaults")
            history_file = os.path.join(tmpdir, "StreamingHistory_music_0.json")

            data = [
                {
                    "master_metadata_track_name": "Song A",
                    "master_metadata_album_artist_name": "Artist A",
                    "master_metadata_album_album_name": "Album A",
                    "ts": "2024-01-15T10:30:00Z",
                    "ms_played": 200000,
                },
            ]
            with open(history_file, "w") as f:
                json.dump(data, f)

            run_import(history_file, config={"vault_root": vault_root})

            jsonl = os.path.join(vault_root, "Music", "music.jsonl")
            assert os.path.isfile(jsonl)
            with open(jsonl) as f:
                entries = [json.loads(line) for line in f if line.strip()]
            assert len(entries) == 1

    def test_deduplication(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            vault_root = os.path.join(tmpdir, "vaults")
            history_file = os.path.join(tmpdir, "StreamingHistory_music_0.json")

            data = [
                {
                    "master_metadata_track_name": "Song",
                    "master_metadata_album_artist_name": "Artist",
                    "ts": "2024-01-15T10:30:00Z",
                    "ms_played": 200000,
                },
            ]
            with open(history_file, "w") as f:
                json.dump(data, f)

            run_import(history_file, config={"vault_root": vault_root})
            run_import(history_file, config={"vault_root": vault_root})

            jsonl = os.path.join(vault_root, "Music", "music.jsonl")
            with open(jsonl) as f:
                entries = [json.loads(line) for line in f if line.strip()]
            assert len(entries) == 1

    def test_legacy_import(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            vault_root = os.path.join(tmpdir, "vaults")
            history_file = os.path.join(tmpdir, "StreamingHistory0.json")

            data = [
                {"trackName": "Legacy Song", "artistName": "Legacy Artist", "endTime": "2024-01-15 10:30", "msPlayed": 100000},
            ]
            with open(history_file, "w") as f:
                json.dump(data, f)

            run_import(history_file, config={"vault_root": vault_root})

            jsonl = os.path.join(vault_root, "Music", "music.jsonl")
            with open(jsonl) as f:
                entries = [json.loads(line) for line in f if line.strip()]
            assert len(entries) == 1
            assert entries[0]["track"] == "Legacy Song"
