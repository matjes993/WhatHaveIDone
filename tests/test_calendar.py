"""Tests for the Calendar collector — ICS parser and API event converter."""

import json
import os
import tempfile

import pytest

# Calendar collector imports Google API libs at module level; we need to handle that.
# The helper functions and ICS parser don't need the API, but the module-level import
# of googleapiclient will fail if it's not installed. We import selectively.
from collectors.calendar_collector import (
    _make_id,
    _unfold_ics,
    _parse_ics_datetime,
    _parse_ics_event,
    _event_to_entry,
    run_import_ics,
)


# ═══════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════

class TestMakeId:
    def test_deterministic(self):
        assert _make_id("uid", "start") == _make_id("uid", "start")

    def test_prefix(self):
        assert _make_id("x").startswith("calendar:google:")

    def test_different(self):
        assert _make_id("a") != _make_id("b")


class TestUnfoldIcs:
    def test_basic_folding(self):
        text = "SUMMARY:This is a very long\n line that continues"
        result = _unfold_ics(text)
        assert result == "SUMMARY:This is a very longline that continues"

    def test_no_folding(self):
        text = "SUMMARY:Short line"
        assert _unfold_ics(text) == "SUMMARY:Short line"

    def test_tab_continuation(self):
        text = "DESCRIPTION:Line one\n\tcontinued"
        result = _unfold_ics(text)
        assert result == "DESCRIPTION:Line onecontinued"


class TestParseIcsDatetime:
    def test_utc_datetime(self):
        iso, all_day = _parse_ics_datetime("20240115T100000Z")
        assert iso == "2024-01-15T10:00:00Z"
        assert all_day is False

    def test_local_datetime(self):
        iso, all_day = _parse_ics_datetime("20240115T100000")
        assert iso == "2024-01-15T10:00:00"
        assert all_day is False

    def test_all_day(self):
        iso, all_day = _parse_ics_datetime("20240115")
        assert iso == "2024-01-15"
        assert all_day is True

    def test_with_tzid(self):
        iso, all_day = _parse_ics_datetime("TZID=Europe/Berlin:20240115T100000")
        assert iso == "2024-01-15T10:00:00"
        assert all_day is False

    def test_empty(self):
        iso, all_day = _parse_ics_datetime("")
        assert iso == ""
        assert all_day is False


# ═══════════════════════════════════════════════════════════════════════
# ICS Event Parser
# ═══════════════════════════════════════════════════════════════════════

class TestParseIcsEvent:
    def test_full_event(self):
        lines = [
            "UID:unique-123",
            "SUMMARY:Team Meeting",
            "DTSTART:20240115T100000Z",
            "DTEND:20240115T110000Z",
            "LOCATION:Room 101",
            "DESCRIPTION:Weekly sync meeting",
            "STATUS:CONFIRMED",
            "RRULE:FREQ=WEEKLY",
        ]
        entry = _parse_ics_event(lines)
        assert entry is not None
        assert entry["title"] == "Team Meeting"
        assert entry["location"] == "Room 101"
        assert entry["recurring"] is True
        assert entry["status"] == "confirmed"
        assert entry["year"] == 2024

    def test_all_day_event(self):
        lines = [
            "UID:allday-1",
            "SUMMARY:Holiday",
            "DTSTART;VALUE=DATE:20240115",
            "DTEND;VALUE=DATE:20240116",
        ]
        entry = _parse_ics_event(lines)
        assert entry is not None
        assert entry["all_day"] is True

    def test_with_attendees(self):
        lines = [
            "UID:att-1",
            "SUMMARY:Meeting",
            "DTSTART:20240115T100000Z",
            "DTEND:20240115T110000Z",
            "ATTENDEE;CN=John Doe;PARTSTAT=ACCEPTED:mailto:john@example.com",
            "ATTENDEE;CN=Jane Smith;PARTSTAT=TENTATIVE:mailto:jane@example.com",
        ]
        entry = _parse_ics_event(lines)
        assert entry is not None
        assert len(entry["attendees"]) == 2
        assert entry["attendees"][0]["name"] == "John Doe"
        assert entry["attendees"][0]["email"] == "john@example.com"

    def test_no_uid_no_summary(self):
        lines = ["DTSTART:20240115T100000Z", "DTEND:20240115T110000Z"]
        assert _parse_ics_event(lines) is None

    def test_summary_without_uid(self):
        lines = ["SUMMARY:Important Event", "DTSTART:20240115T100000Z"]
        entry = _parse_ics_event(lines)
        assert entry is not None
        assert entry["title"] == "Important Event"

    def test_escaped_characters(self):
        lines = [
            "UID:esc-1",
            "SUMMARY:Lunch\\, Team",
            "DESCRIPTION:Line one\\nLine two\\, continued",
            "DTSTART:20240115T120000Z",
        ]
        entry = _parse_ics_event(lines)
        assert entry is not None
        assert "Line one\nLine two, continued" in entry["description"]


# ═══════════════════════════════════════════════════════════════════════
# API Event Converter
# ═══════════════════════════════════════════════════════════════════════

class TestEventToEntry:
    def test_timed_event(self):
        event = {
            "id": "evt_123",
            "summary": "Standup",
            "start": {"dateTime": "2024-01-15T09:00:00+01:00"},
            "end": {"dateTime": "2024-01-15T09:15:00+01:00"},
            "location": "Zoom",
            "description": "Daily standup",
            "organizer": {"email": "boss@example.com"},
            "status": "confirmed",
            "attendees": [
                {"email": "a@x.com", "displayName": "Alice", "responseStatus": "accepted"},
            ],
            "htmlLink": "https://calendar.google.com/event/123",
            "updated": "2024-01-15T10:00:00Z",
        }
        entry = _event_to_entry(event)
        assert entry is not None
        assert entry["id"] == "calendar:google:evt_123"
        assert entry["title"] == "Standup"
        assert entry["all_day"] is False
        assert entry["year"] == 2024
        assert len(entry["attendees"]) == 1

    def test_all_day_event(self):
        event = {
            "id": "evt_allday",
            "summary": "Public Holiday",
            "start": {"date": "2024-01-15"},
            "end": {"date": "2024-01-16"},
        }
        entry = _event_to_entry(event)
        assert entry is not None
        assert entry["all_day"] is True

    def test_recurring_event(self):
        event = {
            "id": "evt_rec",
            "summary": "Weekly",
            "start": {"dateTime": "2024-01-15T10:00:00Z"},
            "end": {"dateTime": "2024-01-15T11:00:00Z"},
            "recurringEventId": "rec_parent",
        }
        entry = _event_to_entry(event)
        assert entry["recurring"] is True

    def test_no_id(self):
        event = {"summary": "No ID Event", "start": {"date": "2024-01-15"}}
        assert _event_to_entry(event) is None

    def test_no_summary(self):
        event = {
            "id": "evt_nosummary",
            "start": {"dateTime": "2024-01-15T10:00:00Z"},
            "end": {"dateTime": "2024-01-15T11:00:00Z"},
        }
        entry = _event_to_entry(event)
        assert entry is not None
        assert entry["title"] == ""


# ═══════════════════════════════════════════════════════════════════════
# End-to-End ICS Import
# ═══════════════════════════════════════════════════════════════════════

class TestRunImportIcs:
    def test_basic_ics_import(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ics_path = os.path.join(tmpdir, "calendar.ics")
            vault_root = os.path.join(tmpdir, "vaults")

            ics_content = (
                "BEGIN:VCALENDAR\n"
                "BEGIN:VEVENT\n"
                "UID:test-1\n"
                "SUMMARY:Meeting One\n"
                "DTSTART:20240115T100000Z\n"
                "DTEND:20240115T110000Z\n"
                "END:VEVENT\n"
                "BEGIN:VEVENT\n"
                "UID:test-2\n"
                "SUMMARY:Meeting Two\n"
                "DTSTART:20240116T140000Z\n"
                "DTEND:20240116T150000Z\n"
                "END:VEVENT\n"
                "END:VCALENDAR\n"
            )
            with open(ics_path, "w") as f:
                f.write(ics_content)

            run_import_ics(ics_path, config={"vault_root": vault_root})

            jsonl = os.path.join(vault_root, "Calendar", "calendar.jsonl")
            assert os.path.isfile(jsonl)
            with open(jsonl) as f:
                entries = [json.loads(line) for line in f if line.strip()]
            assert len(entries) == 2

    def test_ics_deduplication(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ics_path = os.path.join(tmpdir, "calendar.ics")
            vault_root = os.path.join(tmpdir, "vaults")

            ics_content = (
                "BEGIN:VCALENDAR\n"
                "BEGIN:VEVENT\n"
                "UID:dedupe-1\n"
                "SUMMARY:Event\n"
                "DTSTART:20240115T100000Z\n"
                "DTEND:20240115T110000Z\n"
                "END:VEVENT\n"
                "END:VCALENDAR\n"
            )
            with open(ics_path, "w") as f:
                f.write(ics_content)

            run_import_ics(ics_path, config={"vault_root": vault_root})
            run_import_ics(ics_path, config={"vault_root": vault_root})

            jsonl = os.path.join(vault_root, "Calendar", "calendar.jsonl")
            with open(jsonl) as f:
                entries = [json.loads(line) for line in f if line.strip()]
            assert len(entries) == 1

    def test_ics_file_not_found(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            vault_root = os.path.join(tmpdir, "vaults")
            # Should not raise, just prints error
            run_import_ics("/nonexistent/calendar.ics", config={"vault_root": vault_root})
