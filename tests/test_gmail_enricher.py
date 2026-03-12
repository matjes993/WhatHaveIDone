"""Tests for collectors/gmail_enricher.py — metadata backfill logic.

These tests use mocks and do NOT call the real Gmail API.
"""

import json
import os
import tempfile

import pytest
from unittest.mock import MagicMock, patch

from collectors.gmail_enricher import (
    _metadata_to_patch,
    _fetch_metadata_batch,
)


# ---------------------------------------------------------------------------
# Metadata parsing
# ---------------------------------------------------------------------------
class TestMetadataToPatch:
    def test_all_headers_present(self):
        msg = {
            "internalDate": "1704067200000",
            "sizeEstimate": 5432,
            "payload": {
                "headers": [
                    {"name": "Cc", "value": "carol@ex.com"},
                    {"name": "Bcc", "value": "secret@ex.com"},
                    {"name": "Reply-To", "value": "reply@ex.com"},
                    {"name": "List-Unsubscribe", "value": "<https://unsub.link>"},
                    {"name": "Message-ID", "value": "<msg001@mail.ex.com>"},
                    {"name": "In-Reply-To", "value": "<parent@mail.ex.com>"},
                    {"name": "References", "value": "<ref1@mail.ex.com> <ref2@mail.ex.com>"},
                ],
            },
        }
        patch = _metadata_to_patch(msg)
        assert patch["cc"] == "carol@ex.com"
        assert patch["bcc"] == "secret@ex.com"
        assert patch["reply_to"] == "reply@ex.com"
        assert patch["list_unsubscribe"] == "<https://unsub.link>"
        assert patch["message_id"] == "<msg001@mail.ex.com>"
        assert patch["in_reply_to"] == "<parent@mail.ex.com>"
        assert patch["references"] == "<ref1@mail.ex.com> <ref2@mail.ex.com>"
        assert patch["internalDate"] == "1704067200000"
        assert patch["sizeEstimate"] == 5432

    def test_no_headers(self):
        msg = {
            "internalDate": "1704067200000",
            "sizeEstimate": 100,
            "payload": {"headers": []},
        }
        patch = _metadata_to_patch(msg)
        assert patch["cc"] == ""
        assert patch["bcc"] == ""
        assert patch["reply_to"] == ""
        assert patch["internalDate"] == "1704067200000"

    def test_missing_payload(self):
        msg = {"internalDate": "1704067200000", "sizeEstimate": 0}
        patch = _metadata_to_patch(msg)
        assert patch["cc"] == ""
        assert patch["internalDate"] == "1704067200000"

    def test_case_insensitive_headers(self):
        msg = {
            "internalDate": "1000",
            "sizeEstimate": 0,
            "payload": {
                "headers": [
                    {"name": "CC", "value": "UPPER@EX.COM"},
                    {"name": "message-id", "value": "<lower@ex.com>"},
                ],
            },
        }
        patch = _metadata_to_patch(msg)
        assert patch["cc"] == "UPPER@EX.COM"
        assert patch["message_id"] == "<lower@ex.com>"


# ---------------------------------------------------------------------------
# Batch metadata fetching
# ---------------------------------------------------------------------------
class TestFetchMetadataBatch:
    def _make_mock_service(self, responses):
        """Create a mock Gmail service that returns metadata responses."""
        service = MagicMock()
        users = MagicMock()
        messages = MagicMock()
        service.users.return_value = users
        users.messages.return_value = messages

        def mock_get(userId="me", id=None, format=None, metadataHeaders=None):
            mock_request = MagicMock()
            if id in responses:
                mock_request.execute.return_value = responses[id]
            else:
                from googleapiclient.errors import HttpError
                resp = MagicMock()
                resp.status = 404
                mock_request.execute.side_effect = HttpError(
                    resp, b'{"error": {"message": "Not found"}}'
                )
            return mock_request

        messages.get = mock_get

        def mock_batch(callback=None):
            batch = MockBatchRequest(callback, messages)
            return batch

        service.new_batch_http_request = mock_batch
        return service

    def test_basic_fetch(self):
        responses = {
            "msg_001": {
                "id": "msg_001",
                "internalDate": "1704067200000",
                "sizeEstimate": 1000,
                "payload": {
                    "headers": [
                        {"name": "Cc", "value": "carol@ex.com"},
                        {"name": "Message-ID", "value": "<001@ex.com>"},
                    ]
                },
            },
            "msg_002": {
                "id": "msg_002",
                "internalDate": "1704070800000",
                "sizeEstimate": 2000,
                "payload": {"headers": []},
            },
        }
        service = self._make_mock_service(responses)
        patches, failed, rate_limited = _fetch_metadata_batch(
            service, ["msg_001", "msg_002"]
        )

        assert len(patches) == 2
        assert patches["msg_001"]["cc"] == "carol@ex.com"
        assert patches["msg_001"]["internalDate"] == "1704067200000"
        assert patches["msg_002"]["internalDate"] == "1704070800000"
        assert len(failed) == 0
        assert len(rate_limited) == 0

    def test_404_counted_as_failed(self):
        service = self._make_mock_service({})
        patches, failed, rate_limited = _fetch_metadata_batch(
            service, ["nonexistent"]
        )
        assert len(patches) == 0
        # 404 is silently skipped in the callback, counted as failed
        assert len(failed) == 1

    def test_empty_batch(self):
        service = self._make_mock_service({})
        patches, failed, rate_limited = _fetch_metadata_batch(service, [])
        assert len(patches) == 0
        assert len(failed) == 0


class MockBatchRequest:
    """Simple mock for batch requests in tests."""
    def __init__(self, callback, messages_resource):
        self.callback = callback
        self.messages_resource = messages_resource
        self.requests = []

    def add(self, request, request_id=None):
        self.requests.append((request, request_id))

    def execute(self):
        from googleapiclient.errors import HttpError
        for request, request_id in self.requests:
            try:
                response = request.execute()
                self.callback(request_id, response, None)
            except HttpError as e:
                self.callback(request_id, None, e)
            except Exception as e:
                self.callback(request_id, None, e)


# ---------------------------------------------------------------------------
# Integration: vault file patching
# ---------------------------------------------------------------------------
class TestEnrichVaultPatching:
    """Test that patches are correctly applied to vault files."""

    def test_patch_applied_to_entries(self, tmp_path):
        """Simulate the patch application logic."""
        from core.vault import read_entries_by_file, rewrite_file_entries

        vault_path = os.path.join(str(tmp_path), "Gmail_Primary", "2024")
        os.makedirs(vault_path)

        # Write entries without enrichment fields
        entries = [
            {"id": "msg_001", "threadId": "t1", "date": "Mon, 01 Jan 2024 00:00:00 +0000",
             "subject": "Test", "from": "alice@ex.com", "to": "bob@ex.com",
             "tags": ["INBOX"], "body_raw": "Hello"},
            {"id": "msg_002", "threadId": "t2", "date": "Tue, 02 Jan 2024 00:00:00 +0000",
             "subject": "Test 2", "from": "bob@ex.com", "to": "alice@ex.com",
             "tags": ["INBOX"], "body_raw": "World"},
        ]
        file_path = os.path.join(vault_path, "01_January.jsonl")
        with open(file_path, "w") as f:
            for e in entries:
                f.write(json.dumps(e) + "\n")

        # Simulate patches from API
        patches = {
            "msg_001": {
                "cc": "carol@ex.com",
                "bcc": "",
                "reply_to": "",
                "list_unsubscribe": "",
                "message_id": "<001@ex.com>",
                "in_reply_to": "",
                "references": "",
                "internalDate": "1704067200000",
                "sizeEstimate": 1500,
            },
            "msg_002": {
                "cc": "",
                "bcc": "",
                "reply_to": "",
                "list_unsubscribe": "<https://unsub.link>",
                "message_id": "<002@ex.com>",
                "in_reply_to": "<001@ex.com>",
                "references": "<001@ex.com>",
                "internalDate": "1704153600000",
                "sizeEstimate": 2000,
            },
        }

        # Apply patches
        parent_path = os.path.join(str(tmp_path), "Gmail_Primary")
        file_entries = read_entries_by_file(parent_path)

        for fp, fe_entries in file_entries.items():
            changed = False
            for entry in fe_entries:
                msg_id = entry.get("id", "")
                if msg_id in patches:
                    entry.update(patches[msg_id])
                    changed = True
            if changed:
                rewrite_file_entries(fp, fe_entries)

        # Verify
        result_entries = read_entries_by_file(parent_path)
        result_list = list(result_entries.values())[0]

        assert result_list[0]["internalDate"] == "1704067200000"
        assert result_list[0]["cc"] == "carol@ex.com"
        assert result_list[0]["body_raw"] == "Hello"  # original preserved

        assert result_list[1]["internalDate"] == "1704153600000"
        assert result_list[1]["list_unsubscribe"] == "<https://unsub.link>"
        assert result_list[1]["in_reply_to"] == "<001@ex.com>"

    def test_resume_skips_enriched(self, tmp_path):
        """Entries with internalDate should not be re-fetched."""
        vault_path = os.path.join(str(tmp_path), "Gmail_Primary", "2024")
        os.makedirs(vault_path)

        entries = [
            {"id": "msg_enriched", "threadId": "t1",
             "internalDate": "1704067200000", "sizeEstimate": 1000,
             "body_raw": "Already enriched"},
            {"id": "msg_needs_enrich", "threadId": "t2",
             "body_raw": "Needs enrichment"},
        ]
        file_path = os.path.join(vault_path, "01_January.jsonl")
        with open(file_path, "w") as f:
            for e in entries:
                f.write(json.dumps(e) + "\n")

        # Scan for unenriched
        parent_path = os.path.join(str(tmp_path), "Gmail_Primary")
        from core.vault import read_entries_by_file
        file_entries = read_entries_by_file(parent_path)

        unenriched_ids = []
        for fp, fe_entries in file_entries.items():
            for entry in fe_entries:
                if not entry.get("internalDate"):
                    unenriched_ids.append(entry.get("id"))

        assert unenriched_ids == ["msg_needs_enrich"]
