"""Tests for collectors/gmail_collector.py — Gmail collection logic.

These tests use mocks and do NOT call the real Gmail API.
"""

import base64
import json
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from collectors.gmail_collector import (
    clean_html_to_text,
    _parse_message_date,
    _msg_to_entry,
    _flush_entries_to_vault,
    _fetch_batch,
    _handle_api_error,
    AdaptiveThrottle,
)


# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------
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

    def test_gmt_timezone(self):
        dt, valid = _parse_message_date("Sat, 14 Feb 2026 22:18:48 GMT")
        assert valid is True
        assert dt.year == 2026
        assert dt.month == 2

    def test_utc_timezone(self):
        dt, valid = _parse_message_date("Mon, 01 Jan 2024 12:00:00 UTC")
        assert valid is True

    def test_without_day_of_week(self):
        dt, valid = _parse_message_date("01 Jun 2025 13:00:15 -0000")
        assert valid is True
        assert dt.month == 6

    def test_without_day_of_week_gmt(self):
        dt, valid = _parse_message_date("14 Feb 2026 22:18:48 GMT")
        assert valid is True

    def test_pst_timezone(self):
        dt, valid = _parse_message_date("Mon, 01 Jan 2024 12:00:00 PST")
        assert valid is True

    def test_cet_timezone(self):
        dt, valid = _parse_message_date("Mon, 01 Jan 2024 12:00:00 CET")
        assert valid is True

    def test_unix_timestamp_string(self):
        dt, valid = _parse_message_date("1700000000")
        assert valid is False

    def test_partial_date(self):
        dt, valid = _parse_message_date("2024-01")
        assert valid is False


# ---------------------------------------------------------------------------
# HTML cleaning
# ---------------------------------------------------------------------------
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

    def test_style_tags_removed(self):
        html = "<html><style>.red { color: red; }</style><body>Content</body></html>"
        data = base64.urlsafe_b64encode(html.encode()).decode()
        payload = {
            "parts": [{"mimeType": "text/html", "body": {"data": data}}]
        }
        result = clean_html_to_text(payload)
        assert "Content" in result
        assert ".red" not in result

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

    def test_unicode_body(self):
        text = "Héllo Wörld 你好 🎉"
        data = base64.urlsafe_b64encode(text.encode("utf-8")).decode()
        payload = {"body": {"data": data}}
        result = clean_html_to_text(payload)
        assert "Héllo" in result
        assert "Wörld" in result
        assert "你好" in result

    def test_empty_body_data(self):
        payload = {"body": {"data": ""}}
        assert clean_html_to_text(payload) == ""

    def test_body_without_data_key(self):
        payload = {"body": {}}
        assert clean_html_to_text(payload) == ""

    def test_multipart_prefers_plain_text(self):
        """When both plain and HTML parts exist, plain text should be included."""
        plain = base64.urlsafe_b64encode(b"Plain version").decode()
        html = base64.urlsafe_b64encode(b"<p>HTML version</p>").decode()
        payload = {
            "parts": [
                {"mimeType": "text/plain", "body": {"data": plain}},
                {"mimeType": "text/html", "body": {"data": html}},
            ]
        }
        result = clean_html_to_text(payload)
        assert "Plain version" in result


# ---------------------------------------------------------------------------
# Message to entry conversion
# ---------------------------------------------------------------------------
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

    def test_no_labels(self):
        msg = {"payload": {"headers": [], "body": {"data": ""}}}
        entry = _msg_to_entry("msg123", msg)
        assert entry["tags"] == []

    def test_no_thread_id(self):
        msg = {"payload": {"headers": [], "body": {"data": ""}}}
        entry = _msg_to_entry("msg123", msg)
        assert entry["threadId"] == ""

    def test_unicode_subject(self):
        msg = {
            "payload": {
                "headers": [
                    {"name": "Subject", "value": "Ré: Ünïcödé 你好"},
                ],
                "body": {"data": ""},
            }
        }
        entry = _msg_to_entry("msg_unicode", msg)
        assert entry["subject"] == "Ré: Ünïcödé 你好"

    def test_case_insensitive_headers(self):
        """Gmail headers should be matched case-insensitively."""
        msg = {
            "payload": {
                "headers": [
                    {"name": "DATE", "value": "Mon, 01 Jan 2024 12:00:00 +0000"},
                    {"name": "SUBJECT", "value": "Upper Case Headers"},
                ],
                "body": {"data": ""},
            }
        }
        entry = _msg_to_entry("msg_case", msg)
        # Headers are lowercased in _msg_to_entry
        assert entry["date"] == "Mon, 01 Jan 2024 12:00:00 +0000"
        assert entry["subject"] == "Upper Case Headers"

    def test_all_required_fields_present(self):
        """Entry must have all required fields for vault storage."""
        msg = {}
        entry = _msg_to_entry("msg_fields", msg)
        required = {"id", "threadId", "date", "subject", "from", "to", "tags", "body_raw"}
        assert required.issubset(set(entry.keys()))


# ---------------------------------------------------------------------------
# Flush entries to vault
# ---------------------------------------------------------------------------
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

    def test_multiple_years(self, tmp_path):
        vault = str(tmp_path / "vault")
        entries = [
            {"id": "msg1", "date": "Mon, 15 Jan 2020 12:00:00 +0000", "subject": "2020"},
            {"id": "msg2", "date": "Wed, 15 Jun 2022 12:00:00 +0000", "subject": "2022"},
            {"id": "msg3", "date": "Fri, 15 Mar 2024 12:00:00 +0000", "subject": "2024"},
        ]
        _flush_entries_to_vault(entries, vault)

        assert os.path.isdir(os.path.join(vault, "2020"))
        assert os.path.isdir(os.path.join(vault, "2022"))
        assert os.path.isdir(os.path.join(vault, "2024"))

    def test_empty_entries_list(self, tmp_path):
        vault = str(tmp_path / "vault")
        _flush_entries_to_vault([], vault)
        # Should not create any directories
        assert not os.path.exists(vault)

    def test_entries_are_valid_json_lines(self, tmp_path):
        vault = str(tmp_path / "vault")
        entries = [
            {"id": f"msg{i}", "date": "Mon, 15 Jan 2024 12:00:00 +0000", "subject": f"Test {i}"}
            for i in range(10)
        ]
        _flush_entries_to_vault(entries, vault)

        jan_file = os.path.join(vault, "2024", "01_January.jsonl")
        with open(jan_file) as f:
            for line in f:
                entry = json.loads(line)  # should not raise
                assert "id" in entry


# ---------------------------------------------------------------------------
# Adaptive throttle
# ---------------------------------------------------------------------------
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

    def test_ramp_up_does_not_exceed_original(self):
        t = AdaptiveThrottle(5, 50)
        t.on_rate_limit()
        # Ramp up many times
        for _ in range(200):
            t.on_success()
        assert t.max_workers <= 5
        assert t.batch_size <= 50

    def test_thread_safety(self):
        """Throttle should be safe to use from multiple threads."""
        import threading

        t = AdaptiveThrottle(10, 100)
        errors = []

        def rate_limit_loop():
            try:
                for _ in range(50):
                    t.on_rate_limit()
                    t.on_success()
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=rate_limit_loop) for _ in range(5)]
        for th in threads:
            th.start()
        for th in threads:
            th.join()

        assert len(errors) == 0
        assert t.max_workers >= 1
        assert t.batch_size >= 10


# ---------------------------------------------------------------------------
# Fetch batch
# ---------------------------------------------------------------------------
class TestFetchBatch:
    def test_successful_fetch(self):
        from tests.mock_gmail import MockGmailInbox
        inbox = MockGmailInbox(num_messages=5)
        service = inbox.build_service()

        ids = [f"msg_{i:06d}" for i in range(5)]
        entries, failed, rate_limited = _fetch_batch(service, ids)

        assert len(entries) == 5
        assert len(failed) == 0
        assert len(rate_limited) == 0

    def test_404_goes_to_failed(self):
        from tests.mock_gmail import MockGmailInbox
        inbox = MockGmailInbox(num_messages=3)
        service = inbox.build_service()

        # Mix real and nonexistent IDs
        ids = ["msg_000000", "nonexistent_id", "msg_000001"]
        entries, failed, rate_limited = _fetch_batch(service, ids)

        assert len(entries) == 2
        assert "nonexistent_id" in failed
        assert len(rate_limited) == 0

    def test_rate_limited_separated(self):
        from tests.mock_gmail import MockGmailInbox
        inbox = MockGmailInbox(num_messages=10, rate_limit_after=5, rate_limit_count=3)
        service = inbox.build_service()

        ids = [f"msg_{i:06d}" for i in range(10)]
        entries, failed, rate_limited = _fetch_batch(service, ids)

        assert len(entries) + len(failed) + len(rate_limited) == 10
        assert len(rate_limited) > 0

    def test_empty_batch(self):
        from tests.mock_gmail import MockGmailInbox
        inbox = MockGmailInbox(num_messages=5)
        service = inbox.build_service()

        entries, failed, rate_limited = _fetch_batch(service, [])
        assert len(entries) == 0
        assert len(failed) == 0
        assert len(rate_limited) == 0


# ---------------------------------------------------------------------------
# Handle API error
# ---------------------------------------------------------------------------
class TestHandleApiError:
    def test_403_api_not_enabled(self, capsys):
        from googleapiclient.errors import HttpError
        resp = MagicMock()
        resp.status = 403
        error = HttpError(resp, b'{"error": {"message": "Gmail API has not been used"}}')

        with pytest.raises(SystemExit):
            _handle_api_error(error)

        output = capsys.readouterr().out
        assert "not enabled" in output or "Enable" in output

    def test_429_rate_limit(self, capsys):
        from googleapiclient.errors import HttpError
        resp = MagicMock()
        resp.status = 429
        error = HttpError(resp, b'{"error": {"message": "Rate limit"}}')

        with pytest.raises(SystemExit):
            _handle_api_error(error)

        output = capsys.readouterr().out
        assert "Rate limited" in output

    def test_401_auth_failed(self, capsys):
        from googleapiclient.errors import HttpError
        resp = MagicMock()
        resp.status = 401
        error = HttpError(resp, b'{"error": {"message": "Unauthorized"}}')

        with pytest.raises(SystemExit):
            _handle_api_error(error)

        output = capsys.readouterr().out
        assert "Authentication failed" in output

    def test_non_http_error(self, capsys):
        with pytest.raises(SystemExit):
            _handle_api_error(RuntimeError("Something broke"))

        output = capsys.readouterr().out
        assert "Unexpected error" in output


# ---------------------------------------------------------------------------
# End-to-end unit test
# ---------------------------------------------------------------------------
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
