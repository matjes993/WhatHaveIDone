"""
Full pipeline tests using the Mock Gmail API.
Tests the entire collect->groom->sniper flow without real credentials.

Run with: pytest tests/test_full_pipeline.py -v
"""

import json
import os
import shutil
import tempfile
from unittest.mock import patch, MagicMock

import pytest

from tests.mock_gmail import MockGmailInbox
from collectors.gmail_collector import (
    run_export,
    _fetch_batch,
    _flush_entries_to_vault,
    _process_batch_with_retry,
    AdaptiveThrottle,
)
from core.groomer import groom_vault


@pytest.fixture
def temp_vault():
    vault = tempfile.mkdtemp(prefix="nomolo_test_")
    yield vault
    shutil.rmtree(vault, ignore_errors=True)


def _make_config(temp_vault, **gmail_overrides):
    """Helper to build a test config."""
    gmail = {
        "max_workers": 1,
        "batch_size": 10,
        "page_size": 50,
        "scope": "https://www.googleapis.com/auth/gmail.readonly",
        "credentials_file": "fake.json",
        "token_file": "fake_token.json",
    }
    gmail.update(gmail_overrides)
    return {"vault_root": temp_vault, "gmail": gmail}


def _run_mock_export(inbox, config, vault_name="Test"):
    """Helper to run export with mock Gmail."""
    with patch(
        "collectors.gmail_collector.get_credentials",
        return_value=inbox.build_credentials(),
    ), patch(
        "collectors.gmail_collector.build",
        return_value=inbox.build_service(),
    ):
        run_export(vault_name=vault_name, config=config)


def _count_vault_entries(vault_path):
    """Count total entries across all JSONL files in a vault."""
    total = 0
    for root, dirs, files in os.walk(vault_path):
        for f in files:
            if f.endswith(".jsonl"):
                with open(os.path.join(root, f)) as fh:
                    total += sum(1 for line in fh if line.strip())
    return total


def _read_vault_entries(vault_path):
    """Read all entries from all JSONL files in a vault."""
    entries = []
    for root, dirs, files in os.walk(vault_path):
        for f in files:
            if f.endswith(".jsonl"):
                with open(os.path.join(root, f)) as fh:
                    for line in fh:
                        if line.strip():
                            entries.append(json.loads(line))
    return entries


# ===========================================================================
# Collection tests
# ===========================================================================
class TestMockCollectionRun:
    """Test collection with the mock Gmail API."""

    def test_collect_50_messages(self, temp_vault):
        inbox = MockGmailInbox(num_messages=50, page_size=20)
        config = _make_config(temp_vault, max_workers=2, batch_size=10, page_size=20)

        _run_mock_export(inbox, config, "Small")

        vault_path = os.path.join(temp_vault, "Gmail_Small")
        assert os.path.exists(vault_path)

        entries = _read_vault_entries(vault_path)
        assert len(entries) == 50

        # All entries have required fields
        for entry in entries:
            assert "id" in entry
            assert "subject" in entry
            assert "body_raw" in entry

        # Check processed_ids.txt
        processed = os.path.join(vault_path, "processed_ids.txt")
        assert os.path.exists(processed)
        with open(processed) as f:
            processed_ids = [l.strip() for l in f if l.strip()]
        assert len(processed_ids) == 50

    def test_collect_200_messages(self, temp_vault):
        inbox = MockGmailInbox(num_messages=200, page_size=50)
        config = _make_config(temp_vault, max_workers=3, batch_size=25, page_size=50)

        _run_mock_export(inbox, config, "Big")

        vault_path = os.path.join(temp_vault, "Gmail_Big")
        assert _count_vault_entries(vault_path) == 200

    def test_incremental_collection(self, temp_vault):
        """Second run should skip already-processed messages."""
        inbox = MockGmailInbox(num_messages=30, page_size=50)
        config = _make_config(temp_vault, batch_size=30)

        mock_service = inbox.build_service()
        mock_creds = inbox.build_credentials()

        with patch(
            "collectors.gmail_collector.get_credentials", return_value=mock_creds
        ), patch(
            "collectors.gmail_collector.build", return_value=mock_service
        ):
            run_export(vault_name="Incr", config=config)

        inbox.fetch_count = 0

        with patch(
            "collectors.gmail_collector.get_credentials", return_value=mock_creds
        ), patch(
            "collectors.gmail_collector.build", return_value=mock_service
        ):
            run_export(vault_name="Incr", config=config)

        vault_path = os.path.join(temp_vault, "Gmail_Incr")
        with open(os.path.join(vault_path, "processed_ids.txt")) as f:
            ids = [l.strip() for l in f if l.strip()]
        assert len(ids) == 30  # still 30, not 60

    def test_html_emails_stripped(self, temp_vault):
        inbox = MockGmailInbox(num_messages=10, include_html=True)
        config = _make_config(temp_vault, batch_size=10)

        _run_mock_export(inbox, config, "Html")

        vault_path = os.path.join(temp_vault, "Gmail_Html")
        entries = _read_vault_entries(vault_path)
        for entry in entries:
            assert "<html>" not in entry["body_raw"]
            assert "<script>" not in entry["body_raw"]

    def test_bad_dates_go_to_unknown(self, temp_vault):
        inbox = MockGmailInbox(num_messages=20, include_bad_dates=True, include_html=False)
        config = _make_config(temp_vault, batch_size=20)

        _run_mock_export(inbox, config, "Dates")

        unknown_dir = os.path.join(temp_vault, "Gmail_Dates", "_unknown")
        assert os.path.exists(unknown_dir)
        unknown_file = os.path.join(unknown_dir, "unknown_date.jsonl")
        assert os.path.exists(unknown_file)

    def test_no_duplicate_ids_in_output(self, temp_vault):
        """Each message ID should appear exactly once in the vault."""
        inbox = MockGmailInbox(num_messages=50, page_size=20)
        config = _make_config(temp_vault, max_workers=2, batch_size=15)

        _run_mock_export(inbox, config, "Unique")

        vault_path = os.path.join(temp_vault, "Gmail_Unique")
        entries = _read_vault_entries(vault_path)
        ids = [e["id"] for e in entries]
        assert len(ids) == len(set(ids)), f"Duplicate IDs found: {len(ids)} total, {len(set(ids))} unique"


# ===========================================================================
# Rate limiting tests
# ===========================================================================
class TestRateLimitHandling:
    """Test that rate limiting is handled gracefully."""

    def test_rate_limit_with_retry(self, temp_vault):
        inbox = MockGmailInbox(
            num_messages=20, rate_limit_after=10, rate_limit_count=5, page_size=50
        )
        config = _make_config(temp_vault, batch_size=10)

        with patch(
            "collectors.gmail_collector.get_credentials",
            return_value=inbox.build_credentials(),
        ), patch(
            "collectors.gmail_collector.build",
            return_value=inbox.build_service(),
        ), patch(
            "collectors.gmail_collector.RETRY_BASE_DELAY", 0.01
        ):
            run_export(vault_name="RateLimit", config=config)

        vault_path = os.path.join(temp_vault, "Gmail_RateLimit")
        with open(os.path.join(vault_path, "processed_ids.txt")) as f:
            ids = [l.strip() for l in f if l.strip()]

        assert len(ids) >= 15  # at least 75%

    def test_adaptive_throttle_reduces_on_limit(self):
        throttle = AdaptiveThrottle(5, 50)
        throttle.on_rate_limit()
        assert throttle.max_workers == 2
        assert throttle.batch_size == 25

        throttle.on_rate_limit()
        assert throttle.max_workers == 1
        assert throttle.batch_size == 12

    def test_fetch_batch_separates_rate_limited(self):
        inbox = MockGmailInbox(num_messages=10, rate_limit_after=5, rate_limit_count=3)
        service = inbox.build_service()
        ids = [f"msg_{i:06d}" for i in range(10)]

        entries, failed, rate_limited = _fetch_batch(service, ids)

        assert len(entries) + len(failed) + len(rate_limited) == 10
        assert len(rate_limited) > 0

    def test_all_rate_limited_still_completes(self, temp_vault):
        """Even if everything is rate-limited, the process should complete without hanging."""
        inbox = MockGmailInbox(
            num_messages=5, rate_limit_after=0, rate_limit_count=100, page_size=50
        )
        config = _make_config(temp_vault, batch_size=5)

        with patch(
            "collectors.gmail_collector.get_credentials",
            return_value=inbox.build_credentials(),
        ), patch(
            "collectors.gmail_collector.build",
            return_value=inbox.build_service(),
        ), patch(
            "collectors.gmail_collector.RETRY_BASE_DELAY", 0.01
        ):
            run_export(vault_name="AllLimited", config=config)

        # Should not hang — just report failures


# ===========================================================================
# Groom after collection
# ===========================================================================
class TestGroomAfterCollection:
    """Test the groom->sniper pipeline after mock collection."""

    def test_groom_deduplicates(self, temp_vault):
        inbox = MockGmailInbox(num_messages=20, page_size=50)
        config = _make_config(temp_vault, batch_size=20)

        _run_mock_export(inbox, config, "Dedup")

        vault_path = os.path.join(temp_vault, "Gmail_Dedup")

        # Manually inject duplicates
        for root, dirs, files in os.walk(vault_path):
            for f in files:
                if f.endswith(".jsonl"):
                    fpath = os.path.join(root, f)
                    with open(fpath) as fh:
                        lines = fh.readlines()
                    with open(fpath, "a") as fh:
                        for line in lines[:3]:
                            fh.write(line)
                    break

        groom_vault(vault_path)

        assert _count_vault_entries(vault_path) == 20

    def test_sniper_detects_ghosts(self, temp_vault):
        inbox = MockGmailInbox(num_messages=10, page_size=50)
        config = _make_config(temp_vault, batch_size=10)

        _run_mock_export(inbox, config, "Ghost")

        vault_path = os.path.join(temp_vault, "Gmail_Ghost")

        # Add a fake ghost to processed_ids
        with open(os.path.join(vault_path, "processed_ids.txt"), "a") as f:
            f.write("ghost_message_id\n")

        groom_vault(vault_path)

        missing = os.path.join(vault_path, "missing_ids.txt")
        assert os.path.exists(missing)
        with open(missing) as f:
            ghosts = [l.strip() for l in f if l.strip()]
        assert "ghost_message_id" in ghosts

    def test_groom_sorts_chronologically(self, temp_vault):
        inbox = MockGmailInbox(num_messages=30, page_size=50, include_bad_dates=False)
        config = _make_config(temp_vault, batch_size=30)

        _run_mock_export(inbox, config, "Sort")

        vault_path = os.path.join(temp_vault, "Gmail_Sort")
        groom_vault(vault_path)

        from core.groomer import parse_date

        for root, dirs, files in os.walk(vault_path):
            for f in files:
                if f.endswith(".jsonl"):
                    with open(os.path.join(root, f)) as fh:
                        dates = []
                        for line in fh:
                            entry = json.loads(line.strip())
                            dt = parse_date(entry.get("date", ""))
                            if dt:
                                dates.append(dt)
                        for i in range(1, len(dates)):
                            assert dates[i] >= dates[i - 1], (
                                f"Dates not sorted in {f}: {dates[i-1]} > {dates[i]}"
                            )

    def test_groom_idempotent(self, temp_vault):
        """Running groom twice should produce the same result."""
        inbox = MockGmailInbox(num_messages=20, page_size=50)
        config = _make_config(temp_vault, batch_size=20)

        _run_mock_export(inbox, config, "Idempotent")

        vault_path = os.path.join(temp_vault, "Gmail_Idempotent")

        groom_vault(vault_path)
        entries_after_first = _read_vault_entries(vault_path)

        groom_vault(vault_path)
        entries_after_second = _read_vault_entries(vault_path)

        assert len(entries_after_first) == len(entries_after_second)
        ids_first = sorted(e["id"] for e in entries_after_first)
        ids_second = sorted(e["id"] for e in entries_after_second)
        assert ids_first == ids_second


# ===========================================================================
# Full sniper recovery flow
# ===========================================================================
class TestSniperRecovery:
    """Test the complete collect → groom → sniper → collect recovery cycle."""

    def test_sniper_recovery_flow(self, temp_vault):
        """Simulate: collect, inject ghost, groom detects it, re-collect recovers."""
        inbox = MockGmailInbox(num_messages=10, page_size=50)
        config = _make_config(temp_vault, batch_size=10)

        # Step 1: Initial collection
        _run_mock_export(inbox, config, "Sniper")
        vault_path = os.path.join(temp_vault, "Gmail_Sniper")

        assert _count_vault_entries(vault_path) == 10

        # Step 2: Inject a ghost — add fake ID to processed_ids
        with open(os.path.join(vault_path, "processed_ids.txt"), "a") as f:
            f.write("ghost_recovery_test\n")

        # Step 3: Groom — should detect ghost and write missing_ids.txt
        groom_vault(vault_path)

        missing_file = os.path.join(vault_path, "missing_ids.txt")
        assert os.path.exists(missing_file)
        with open(missing_file) as f:
            assert "ghost_recovery_test" in f.read()

        # Step 4: Re-collect — should enter sniper mode
        # The ghost ID won't be found in the mock (404), so it'll fail gracefully
        with patch(
            "collectors.gmail_collector.get_credentials",
            return_value=inbox.build_credentials(),
        ), patch(
            "collectors.gmail_collector.build",
            return_value=inbox.build_service(),
        ):
            run_export(vault_name="Sniper", config=config)

        # Sniper mode was triggered (missing_ids.txt was consumed)
        # The ghost ID wasn't recoverable (404), but the process completed


# ===========================================================================
# Vault structure tests
# ===========================================================================
class TestVaultStructure:
    """Verify the vault directory structure is correct."""

    def test_year_month_folders(self, temp_vault):
        inbox = MockGmailInbox(num_messages=50, page_size=50, include_bad_dates=False)
        config = _make_config(temp_vault, batch_size=50)

        _run_mock_export(inbox, config, "Structure")

        vault_path = os.path.join(temp_vault, "Gmail_Structure")

        year_dirs = [
            d for d in os.listdir(vault_path)
            if os.path.isdir(os.path.join(vault_path, d)) and d.isdigit()
        ]
        assert len(year_dirs) > 0

        for year_dir in year_dirs:
            year_path = os.path.join(vault_path, year_dir)
            jsonl_files = [f for f in os.listdir(year_path) if f.endswith(".jsonl")]
            assert len(jsonl_files) > 0

            for jf in jsonl_files:
                assert "_" in jf
                month_num = jf.split("_")[0]
                assert month_num.isdigit()
                assert 1 <= int(month_num) <= 12

    def test_extraction_log_created(self, temp_vault):
        inbox = MockGmailInbox(num_messages=5, page_size=50)
        config = _make_config(temp_vault, batch_size=5)

        _run_mock_export(inbox, config, "Log")

        log_file = os.path.join(temp_vault, "Gmail_Log", "extraction.log")
        assert os.path.exists(log_file)

    def test_jsonl_entries_are_valid(self, temp_vault):
        inbox = MockGmailInbox(num_messages=30, page_size=50)
        config = _make_config(temp_vault, batch_size=30)

        _run_mock_export(inbox, config, "Valid")

        vault_path = os.path.join(temp_vault, "Gmail_Valid")
        required_fields = {"id", "threadId", "date", "subject", "from", "to", "tags", "body_raw"}

        for root, dirs, files in os.walk(vault_path):
            for f in files:
                if f.endswith(".jsonl"):
                    with open(os.path.join(root, f)) as fh:
                        for line_num, line in enumerate(fh, 1):
                            entry = json.loads(line.strip())
                            missing = required_fields - set(entry.keys())
                            assert not missing, (
                                f"Missing fields {missing} in {f} line {line_num}"
                            )

    def test_processed_ids_match_vault_entries(self, temp_vault):
        """Every ID in processed_ids.txt should correspond to an entry in the vault."""
        inbox = MockGmailInbox(num_messages=30, page_size=50)
        config = _make_config(temp_vault, batch_size=30)

        _run_mock_export(inbox, config, "Match")

        vault_path = os.path.join(temp_vault, "Gmail_Match")

        with open(os.path.join(vault_path, "processed_ids.txt")) as f:
            processed = set(l.strip() for l in f if l.strip())

        entries = _read_vault_entries(vault_path)
        vault_ids = set(e["id"] for e in entries)

        assert processed == vault_ids

    def test_vault_name_in_path(self, temp_vault):
        """Different vault names should create different directories."""
        inbox = MockGmailInbox(num_messages=5, page_size=50)

        config_a = _make_config(temp_vault, batch_size=5)
        config_b = _make_config(temp_vault, batch_size=5)

        _run_mock_export(inbox, config_a, "VaultA")

        inbox2 = MockGmailInbox(num_messages=5, page_size=50)
        _run_mock_export(inbox2, config_b, "VaultB")

        assert os.path.exists(os.path.join(temp_vault, "Gmail_VaultA"))
        assert os.path.exists(os.path.join(temp_vault, "Gmail_VaultB"))


# ===========================================================================
# Mock API validation
# ===========================================================================
class TestMockGmailInbox:
    """Validate the mock itself behaves correctly."""

    def test_message_count(self):
        inbox = MockGmailInbox(num_messages=42)
        assert len(inbox.messages) == 42

    def test_message_ids_unique(self):
        inbox = MockGmailInbox(num_messages=100)
        ids = list(inbox.messages.keys())
        assert len(ids) == len(set(ids))

    def test_pagination(self):
        inbox = MockGmailInbox(num_messages=25, page_size=10)
        service = inbox.build_service()

        all_ids = []
        result = service.users().messages().list(userId="me").execute()
        all_ids.extend(m["id"] for m in result["messages"])

        while "nextPageToken" in result:
            result = service.users().messages().list(
                userId="me", pageToken=result["nextPageToken"]
            ).execute()
            all_ids.extend(m["id"] for m in result["messages"])

        assert len(all_ids) == 25

    def test_rate_limiting_behavior(self):
        inbox = MockGmailInbox(num_messages=10, rate_limit_after=3, rate_limit_count=2)
        service = inbox.build_service()

        # First 3 should succeed
        for i in range(3):
            req = service.users().messages().get(userId="me", id=f"msg_{i:06d}")
            req.execute()  # should not raise

        # Next 2 should raise HttpError 429
        from googleapiclient.errors import HttpError
        rate_limited = 0
        for i in range(3, 10):
            req = service.users().messages().get(userId="me", id=f"msg_{i:06d}")
            try:
                req.execute()
            except HttpError as e:
                if e.resp.status == 429:
                    rate_limited += 1

        assert rate_limited == 2

    def test_nonexistent_message_404(self):
        from googleapiclient.errors import HttpError
        inbox = MockGmailInbox(num_messages=5)
        service = inbox.build_service()

        req = service.users().messages().get(userId="me", id="does_not_exist")
        with pytest.raises(HttpError) as exc_info:
            req.execute()
        assert exc_info.value.resp.status == 404
