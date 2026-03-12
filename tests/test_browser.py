"""Tests for the Browser collector — Chrome SQLite and CSV parsers."""

import csv
import json
import os
import sqlite3
import tempfile
from datetime import datetime

import pytest

from collectors.browser import (
    _normalize_columns,
    _get,
    _read_csv,
    _make_id,
    _safe_int,
    _webkit_to_datetime,
    _extract_domain,
    _get_chrome_history_path,
    _copy_to_temp,
    _parse_url_row,
    _parse_csv_row,
    _WEBKIT_EPOCH_OFFSET,
    run_import_csv,
)


# ═══════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════

class TestMakeId:
    def test_deterministic(self):
        assert _make_id("http://example.com") == _make_id("http://example.com")

    def test_prefix(self):
        assert _make_id("x").startswith("browser:chrome:")

    def test_different(self):
        assert _make_id("a") != _make_id("b")


class TestSafeInt:
    def test_valid(self):
        assert _safe_int("42") == 42

    def test_invalid(self):
        assert _safe_int("abc") == 0

    def test_none(self):
        assert _safe_int(None) == 0


class TestWebkitToDatetime:
    def test_known_timestamp(self):
        # 2024-01-15T12:00:00Z in WebKit microseconds
        unix_ts = 1705320000  # approx
        webkit_ts = (unix_ts * 1_000_000) + _WEBKIT_EPOCH_OFFSET
        dt = _webkit_to_datetime(webkit_ts)
        assert dt is not None
        assert dt.year == 2024

    def test_zero(self):
        assert _webkit_to_datetime(0) is None

    def test_negative(self):
        assert _webkit_to_datetime(-1) is None

    def test_none(self):
        assert _webkit_to_datetime(None) is None


class TestExtractDomain:
    def test_standard_url(self):
        assert _extract_domain("https://www.example.com/page") == "example.com"

    def test_no_www(self):
        assert _extract_domain("https://example.com/page") == "example.com"

    def test_subdomain(self):
        assert _extract_domain("https://mail.google.com") == "mail.google.com"

    def test_empty(self):
        assert _extract_domain("") == ""

    def test_none(self):
        assert _extract_domain(None) == ""

    def test_invalid_url(self):
        # urlparse handles most strings gracefully
        result = _extract_domain("not a url")
        assert isinstance(result, str)


class TestGetChromeHistoryPath:
    def test_returns_string(self):
        path = _get_chrome_history_path()
        # On macOS or Linux this should return a path; on unknown platforms None
        if path is not None:
            assert "History" in path

    def test_custom_profile(self):
        path = _get_chrome_history_path(profile="Profile 1")
        if path is not None:
            assert "Profile 1" in path


class TestCopyToTemp:
    def test_successful_copy(self):
        with tempfile.NamedTemporaryFile(delete=False) as src:
            src.write(b"test data")
            src_path = src.name
        try:
            temp_path = _copy_to_temp(src_path)
            assert os.path.isfile(temp_path)
            with open(temp_path, "rb") as f:
                assert f.read() == b"test data"
            os.unlink(temp_path)
        finally:
            os.unlink(src_path)

    def test_nonexistent_source(self):
        with pytest.raises(OSError):
            _copy_to_temp("/nonexistent/file")


# ═══════════════════════════════════════════════════════════════════════
# URL Row Parsers
# ═══════════════════════════════════════════════════════════════════════

class TestParseUrlRow:
    def test_full_row(self):
        # Build a WebKit timestamp for 2024-01-15
        unix_ts = 1705320000
        webkit_ts = (unix_ts * 1_000_000) + _WEBKIT_EPOCH_OFFSET

        row = {
            "url": "https://www.example.com/page",
            "title": "Example Page",
            "visit_count": 5,
            "typed_count": 2,
            "last_visit_time": webkit_ts,
        }
        entry = _parse_url_row(row)
        assert entry is not None
        assert entry["url"] == "https://www.example.com/page"
        assert entry["domain"] == "example.com"
        assert entry["visit_count"] == 5
        assert entry["year"] == 2024

    def test_no_url(self):
        assert _parse_url_row({"url": "", "title": "Test"}) is None

    def test_chrome_internal_url(self):
        assert _parse_url_row({"url": "chrome://settings/"}) is None
        assert _parse_url_row({"url": "chrome-extension://abc/popup.html"}) is None
        assert _parse_url_row({"url": "about:blank"}) is None
        assert _parse_url_row({"url": "devtools://devtools/index.html"}) is None

    def test_no_title(self):
        row = {"url": "https://example.com", "title": "", "visit_count": 1, "typed_count": 0, "last_visit_time": 0}
        entry = _parse_url_row(row)
        assert entry is not None
        assert entry["title"] == ""


class TestParseCsvRow:
    @pytest.fixture
    def col_map(self):
        headers = ["URL", "Title", "Visit Count", "Last Visit"]
        return _normalize_columns(headers)

    def test_full_row(self, col_map):
        row = ["https://www.example.com/page", "Example", "5", "2024-01-15T10:30:00"]
        entry = _parse_csv_row(row, col_map)
        assert entry is not None
        assert entry["domain"] == "example.com"
        assert entry["visit_count"] == 5
        assert entry["year"] == 2024

    def test_no_url(self, col_map):
        row = ["", "Title", "1", "2024-01-15"]
        assert _parse_csv_row(row, col_map) is None

    def test_chrome_internal(self, col_map):
        row = ["chrome://history", "History", "1", ""]
        assert _parse_csv_row(row, col_map) is None

    def test_default_visit_count(self, col_map):
        row = ["https://example.com", "Test", "", "2024-01-15"]
        entry = _parse_csv_row(row, col_map)
        assert entry["visit_count"] == 1


# ═══════════════════════════════════════════════════════════════════════
# End-to-End Import
# ═══════════════════════════════════════════════════════════════════════

class TestRunImportCsv:
    def test_basic_import(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, "chrome.csv")
            vault_root = os.path.join(tmpdir, "vaults")

            with open(csv_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["URL", "Title", "Visit Count", "Last Visit"])
                writer.writerow(["https://www.example.com", "Example", "3", "2024-01-15"])
                writer.writerow(["https://www.python.org", "Python", "10", "2024-01-16"])

            run_import_csv(csv_path, config={"vault_root": vault_root})

            jsonl = os.path.join(vault_root, "Browser", "browser.jsonl")
            assert os.path.isfile(jsonl)
            with open(jsonl) as f:
                entries = [json.loads(line) for line in f if line.strip()]
            assert len(entries) == 2

    def test_deduplication(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, "chrome.csv")
            vault_root = os.path.join(tmpdir, "vaults")

            with open(csv_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["URL", "Title"])
                writer.writerow(["https://example.com", "Example"])

            run_import_csv(csv_path, config={"vault_root": vault_root})
            run_import_csv(csv_path, config={"vault_root": vault_root})

            jsonl = os.path.join(vault_root, "Browser", "browser.jsonl")
            with open(jsonl) as f:
                entries = [json.loads(line) for line in f if line.strip()]
            assert len(entries) == 1

    def test_internal_urls_skipped(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, "chrome.csv")
            vault_root = os.path.join(tmpdir, "vaults")

            with open(csv_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["URL", "Title"])
                writer.writerow(["chrome://settings", "Settings"])
                writer.writerow(["https://example.com", "Good URL"])

            run_import_csv(csv_path, config={"vault_root": vault_root})

            jsonl = os.path.join(vault_root, "Browser", "browser.jsonl")
            with open(jsonl) as f:
                entries = [json.loads(line) for line in f if line.strip()]
            assert len(entries) == 1
