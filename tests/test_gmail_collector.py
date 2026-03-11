"""Tests for collectors/gmail_collector.py — Gmail collection logic.

These tests use mocks and do NOT call the real Gmail API.
"""

import base64
import json
import os
import tempfile

import pytest

from collectors.gmail_collector import (
    clean_html_to_text,
    _parse_message_date,
    _msg_to_entry,
    _flush_entries_to_vault,
    AdaptiveThrottle,
)


class TestParseDateCollector:
    def test_rfc2822(self):
        dt, valid = _parse_message_date("Tue, 15 Oct 2024 08:30:00 +0200")
        assert valid is True
        assert dt.year == 2024
        assert dt.month == 10

    def test_rfc2822_with_tz_name(self):
        dt, valid = _parse_message_date("Tue, 15 Oct 2024 08:30:00 +0200 (CEST)")
        assert valid is True

    def test_iso_format(self):
        dt, valid = _parse_message_date("2024-03-20T14:00:00+00:00")
        assert valid is True

    def test_empty_string(self):
        dt, valid = _parse_message_date("")
        assert valid is False
        assert dt is None

    def test_none(self):
        dt, valid = _parse_message_date(None)
        assert valid is False

    def test_garbage(self):
        dt, valid = _parse_message_date("not-a-date")
        assert valid is False


class TestCleanHtmlToText:
    def test_plain_text(self):
        data = base64.urlsafe_b64encode(b"Hello World").decode()
        payload = {
            "parts": [
                {"mimeType": "text/plain", "body": {"data": data}}
            ]
        }
        assert clean_html_to_text(payload) == "Hello World"

    def test_html_stripping(self):
        html = "<html><body><p>Hello <b>World</b></p><script>evil()</script></body></html>"
        data = base64.urlsafe_b64encode(html.encode()).decode()
        payload = {
            "parts": [
                {"mimeType": "text/html", "body": {"data": data}}
            ]
        }
        result = clean_html_to_text(payload)
        assert "Hello" in result
        assert "World" in result
        assert "evil" not in result
        assert "<script>" not in result

    def test_nested_parts(self):
        data = base64.urlsafe_b64encode(b"Nested content").decode()
        payload = {
            "parts": [
                {
                    "mimeType": "multipart/alternative",
                    "parts": [
                        {"mimeType": "text/plain", "body": {"data": data}}
                    ],
                }
            ]
        }
        assert "Nested content" in clean_html_to_text(payload)

    def test_no_parts(self):
        data = base64.urlsafe_b64encode(b"Direct body").decode()
        payload = {"body": {"data": data}}
        assert clean_html_to_text(payload) == "Direct body"

    def test_empty_payload(self):
        assert clean_html_to_text({}) == ""

    def test_whitespace_normalization(self):
        data = base64.urlsafe_b64encode(b"  lots   of    spaces  ").decode()
        payload = {"body": {"data": data}}
        assert clean_html_to_text(payload) == "lots of spaces"


class TestMsgToEntry:
    def test_basic_conversion(self):
        msg = {
            "threadId": "thread123",
            "labelIds": ["INBOX", "UNREAD"],
            "payload": {
                "headers": [
                    {"name": "Date", "value": "Mon, 01 Jan 2024 12:00:00 +0000"},
                    {"name": "Subject", "value": "Test Email"},
                    {"name": "From", "value": "alice@example.com"},
                    {"name": "To", "value": "bob@example.com"},
                ],
                "body": {"data": base64.urlsafe_b64encode(b"Body text").decode()},
            },
        }
        entry = _msg_to_entry("msg123", msg)

        assert entry["id"] == "msg123"
        assert entry["threadId"] == "thread123"
        assert entry["subject"] == "Test Email"
        assert entry["from"] == "alice@example.com"
        assert entry["to"] == "bob@example.com"
        assert entry["tags"] == ["INBOX", "UNREAD"]
        assert "Body text" in entry["body_raw"]

    def test_missing_headers(self):
        msg = {"payload": {"headers": [], "body": {"data": ""}}}
        entry = _msg_to_entry("msg123", msg)
        assert entry["subject"] == "No Subject"
        assert entry["from"] == ""

    def test_empty_payload(self):
        msg = {}
        entry = _msg_to_entry("msg123", msg)
        assert entry["id"] == "msg123"
        assert entry["subject"] == "No Subject"


class TestFlushEntriesToVault:
    def test_writes_to_correct_year_month(self, tmp_path):
        vault = str(tmp_path / "vault")
        entries = [
            {
                "id": "msg1",
                "date": "Mon, 15 Jan 2024 12:00:00 +0000",
                "subject": "January email",
            },
            {
                "id": "msg2",
                "date": "Thu, 20 Jun 2024 12:00:00 +0000",
                "subject": "June email",
            },
        ]
        _flush_entries_to_vault(entries, vault)

        jan_file = os.path.join(vault, "2024", "01_January.jsonl")
        jun_file = os.path.join(vault, "2024", "06_June.jsonl")
        assert os.path.exists(jan_file)
        assert os.path.exists(jun_file)

        with open(jan_file) as f:
            jan_entry = json.loads(f.read().strip())
        assert jan_entry["id"] == "msg1"

    def test_unknown_date_goes_to_unknown_folder(self, tmp_path):
        vault = str(tmp_path / "vault")
        entries = [{"id": "msg1", "date": "garbage-date", "subject": "Bad date"}]
        _flush_entries_to_vault(entries, vault)

        unknown_file = os.path.join(vault, "_unknown", "unknown_date.jsonl")
        assert os.path.exists(unknown_file)

    def test_empty_date_goes_to_unknown(self, tmp_path):
        vault = str(tmp_path / "vault")
        entries = [{"id": "msg1", "date": "", "subject": "No date"}]
        _flush_entries_to_vault(entries, vault)

        unknown_file = os.path.join(vault, "_unknown", "unknown_date.jsonl")
        assert os.path.exists(unknown_file)

    def test_appends_to_existing(self, tmp_path):
        vault = str(tmp_path / "vault")
        entries1 = [
            {"id": "msg1", "date": "Mon, 15 Jan 2024 12:00:00 +0000", "subject": "First"}
        ]
        entries2 = [
            {"id": "msg2", "date": "Tue, 16 Jan 2024 12:00:00 +0000", "subject": "Second"}
        ]
        _flush_entries_to_vault(entries1, vault)
        _flush_entries_to_vault(entries2, vault)

        jan_file = os.path.join(vault, "2024", "01_January.jsonl")
        with open(jan_file) as f:
            lines = [json.loads(l) for l in f if l.strip()]
        assert len(lines) == 2


class TestAdaptiveThrottle:
    def test_initial_values(self):
        t = AdaptiveThrottle(5, 50)
        assert t.max_workers == 5
        assert t.batch_size == 50

    def test_rate_limit_reduces_workers(self):
        t = AdaptiveThrottle(5, 50)
        t.on_rate_limit()
        assert t.max_workers < 5
        assert t.batch_size < 50

    def test_multiple_rate_limits(self):
        t = AdaptiveThrottle(8, 50)
        t.on_rate_limit()  # 8->4, 50->25
        t.on_rate_limit()  # 4->2, 25->12
        t.on_rate_limit()  # 2->1, 12->10 (min)
        assert t.max_workers >= 1
        assert t.batch_size >= 10

    def test_never_goes_below_minimums(self):
        t = AdaptiveThrottle(2, 10)
        for _ in range(10):
            t.on_rate_limit()
        assert t.max_workers >= 1
        assert t.batch_size >= 10

    def test_success_streak_ramps_up(self):
        t = AdaptiveThrottle(5, 50)
        t.on_rate_limit()  # reduce
        reduced_workers = t.max_workers

        for _ in range(25):
            t.on_success()

        assert t.max_workers >= reduced_workers

    def test_rate_limit_resets_streak(self):
        t = AdaptiveThrottle(5, 50)
        for _ in range(15):
            t.on_success()
        t.on_rate_limit()
        assert t.success_streak == 0


class TestEndToEnd:
    """Test the full collect->groom->sniper pipeline with mocked data."""

    def test_full_pipeline(self, tmp_path):
        from core.groomer import groom_vault

        vault = str(tmp_path / "Gmail_Test")
        os.makedirs(vault)

        # Simulate collection output
        year_dir = os.path.join(vault, "2024")
        os.makedirs(year_dir)

        entries = [
            {"id": f"msg{i}", "date": f"Mon, {i:02d} Jan 2024 12:00:00 +0000", "subject": f"Email {i}"}
            for i in range(1, 11)
        ]
        # Add duplicates
        entries.extend([
            {"id": "msg1", "date": "Mon, 01 Jan 2024 12:00:00 +0000", "subject": "Email 1 dup"},
            {"id": "msg5", "date": "Mon, 05 Jan 2024 12:00:00 +0000", "subject": "Email 5 dup"},
        ])

        with open(os.path.join(year_dir, "01_January.jsonl"), "w") as f:
            for e in entries:
                f.write(json.dumps(e) + "\n")

        # Simulate processed_ids.txt with an extra ghost ID
        with open(os.path.join(vault, "processed_ids.txt"), "w") as f:
            for i in range(1, 12):  # msg11 doesn't exist on disk
                f.write(f"msg{i}\n")

        # Run groomer
        groom_vault(vault)

        # Check dedup worked
        with open(os.path.join(year_dir, "01_January.jsonl")) as f:
            lines = [json.loads(l) for l in f if l.strip()]
        assert len(lines) == 10  # 12 -> 10 after dedup

        # Check sorted
        ids = [l["id"] for l in lines]
        assert ids == [f"msg{i}" for i in range(1, 11)]

        # Check ghost detected
        missing = os.path.join(vault, "missing_ids.txt")
        assert os.path.exists(missing)
        with open(missing) as f:
            ghosts = [l.strip() for l in f]
        assert "msg11" in ghosts
