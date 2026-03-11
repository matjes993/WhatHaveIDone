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
    vault = tempfile.mkdtemp(prefix="whid_test_")
    yield vault
    shutil.rmtree(vault, ignore_errors=True)


class TestMockCollectionRun:
    """Test collection with the mock Gmail API."""

    def test_collect_50_messages(self, temp_vault):
        """Collect 50 messages and verify vault structure."""
        inbox = MockGmailInbox(num_messages=50, page_size=20)

        config = {
            "vault_root": temp_vault,
            "gmail": {
                "max_workers": 2,
                "batch_size": 10,
                "page_size": 20,
                "scope": "https://www.googleapis.com/auth/gmail.readonly",
                "credentials_file": "fake.json",
                "token_file": "fake_token.json",
            },
        }

        with patch(
            "collectors.gmail_collector.get_credentials",
            return_value=inbox.build_credentials(),
        ), patch(
            "collectors.gmail_collector.build",
            return_value=inbox.build_service(),
        ):
            run_export(vault_name="MockTest", config=config)

        vault_path = os.path.join(temp_vault, "Gmail_MockTest")
        assert os.path.exists(vault_path)

        # Count entries
        total_entries = 0
        jsonl_files = 0
        for root, dirs, files in os.walk(vault_path):
            for f in files:
                if f.endswith(".jsonl"):
                    jsonl_files += 1
                    with open(os.path.join(root, f)) as fh:
                        for line in fh:
                            entry = json.loads(line.strip())
                            assert "id" in entry
                            assert "subject" in entry
                            assert "body_raw" in entry
                            total_entries += 1

        assert total_entries == 50
        assert jsonl_files > 0

        # Check processed_ids.txt
        processed = os.path.join(vault_path, "processed_ids.txt")
        assert os.path.exists(processed)
        with open(processed) as f:
            processed_ids = [l.strip() for l in f if l.strip()]
        assert len(processed_ids) == 50

    def test_collect_200_messages(self, temp_vault):
        """Larger collection to test pagination and batching."""
        inbox = MockGmailInbox(num_messages=200, page_size=50)

        config = {
            "vault_root": temp_vault,
            "gmail": {
                "max_workers": 3,
                "batch_size": 25,
                "page_size": 50,
                "scope": "https://www.googleapis.com/auth/gmail.readonly",
                "credentials_file": "fake.json",
                "token_file": "fake_token.json",
            },
        }

        with patch(
            "collectors.gmail_collector.get_credentials",
            return_value=inbox.build_credentials(),
        ), patch(
            "collectors.gmail_collector.build",
            return_value=inbox.build_service(),
        ):
            run_export(vault_name="Big", config=config)

        vault_path = os.path.join(temp_vault, "Gmail_Big")
        total = 0
        for root, dirs, files in os.walk(vault_path):
            for f in files:
                if f.endswith(".jsonl"):
                    with open(os.path.join(root, f)) as fh:
                        total += sum(1 for _ in fh)

        assert total == 200

    def test_incremental_collection(self, temp_vault):
        """Second run should skip already-processed messages."""
        inbox = MockGmailInbox(num_messages=30, page_size=50)

        config = {
            "vault_root": temp_vault,
            "gmail": {
                "max_workers": 1,
                "batch_size": 10,
                "page_size": 50,
                "scope": "https://www.googleapis.com/auth/gmail.readonly",
                "credentials_file": "fake.json",
                "token_file": "fake_token.json",
            },
        }

        mock_service = inbox.build_service()
        mock_creds = inbox.build_credentials()

        with patch(
            "collectors.gmail_collector.get_credentials",
            return_value=mock_creds,
        ), patch(
            "collectors.gmail_collector.build",
            return_value=mock_service,
        ):
            run_export(vault_name="Incr", config=config)

        # Reset fetch count
        inbox.fetch_count = 0

        # Second run — should say "up to date"
        with patch(
            "collectors.gmail_collector.get_credentials",
            return_value=mock_creds,
        ), patch(
            "collectors.gmail_collector.build",
            return_value=mock_service,
        ):
            run_export(vault_name="Incr", config=config)

        # Should not have fetched any messages on second run
        # (only listing calls, no message fetches)
        vault_path = os.path.join(temp_vault, "Gmail_Incr")
        with open(os.path.join(vault_path, "processed_ids.txt")) as f:
            ids = [l.strip() for l in f if l.strip()]
        assert len(ids) == 30  # still 30, not 60

    def test_html_emails_stripped(self, temp_vault):
        """HTML emails should have tags stripped in body_raw."""
        inbox = MockGmailInbox(num_messages=10, include_html=True)

        config = {
            "vault_root": temp_vault,
            "gmail": {
                "max_workers": 1,
                "batch_size": 10,
                "page_size": 50,
                "scope": "https://www.googleapis.com/auth/gmail.readonly",
                "credentials_file": "fake.json",
                "token_file": "fake_token.json",
            },
        }

        with patch(
            "collectors.gmail_collector.get_credentials",
            return_value=inbox.build_credentials(),
        ), patch(
            "collectors.gmail_collector.build",
            return_value=inbox.build_service(),
        ):
            run_export(vault_name="Html", config=config)

        vault_path = os.path.join(temp_vault, "Gmail_Html")
        for root, dirs, files in os.walk(vault_path):
            for f in files:
                if f.endswith(".jsonl"):
                    with open(os.path.join(root, f)) as fh:
                        for line in fh:
                            entry = json.loads(line.strip())
                            assert "<html>" not in entry["body_raw"]
                            assert "<script>" not in entry["body_raw"]

    def test_bad_dates_go_to_unknown(self, temp_vault):
        """Messages with unparseable dates should land in _unknown/."""
        inbox = MockGmailInbox(
            num_messages=20, include_bad_dates=True, include_html=False
        )

        config = {
            "vault_root": temp_vault,
            "gmail": {
                "max_workers": 1,
                "batch_size": 20,
                "page_size": 50,
                "scope": "https://www.googleapis.com/auth/gmail.readonly",
                "credentials_file": "fake.json",
                "token_file": "fake_token.json",
            },
        }

        with patch(
            "collectors.gmail_collector.get_credentials",
            return_value=inbox.build_credentials(),
        ), patch(
            "collectors.gmail_collector.build",
            return_value=inbox.build_service(),
        ):
            run_export(vault_name="Dates", config=config)

        unknown_dir = os.path.join(temp_vault, "Gmail_Dates", "_unknown")
        assert os.path.exists(unknown_dir)
        unknown_file = os.path.join(unknown_dir, "unknown_date.jsonl")
        assert os.path.exists(unknown_file)


class TestRateLimitHandling:
    """Test that rate limiting is handled gracefully."""

    def test_rate_limit_with_retry(self, temp_vault):
        """Rate-limited messages should be retried and eventually succeed."""
        inbox = MockGmailInbox(
            num_messages=20,
            rate_limit_after=10,
            rate_limit_count=5,
            page_size=50,
        )

        config = {
            "vault_root": temp_vault,
            "gmail": {
                "max_workers": 1,
                "batch_size": 10,
                "page_size": 50,
                "scope": "https://www.googleapis.com/auth/gmail.readonly",
                "credentials_file": "fake.json",
                "token_file": "fake_token.json",
            },
        }

        with patch(
            "collectors.gmail_collector.get_credentials",
            return_value=inbox.build_credentials(),
        ), patch(
            "collectors.gmail_collector.build",
            return_value=inbox.build_service(),
        ), patch(
            "collectors.gmail_collector.RETRY_BASE_DELAY", 0.1
        ):
            run_export(vault_name="RateLimit", config=config)

        vault_path = os.path.join(temp_vault, "Gmail_RateLimit")
        with open(os.path.join(vault_path, "processed_ids.txt")) as f:
            ids = [l.strip() for l in f if l.strip()]

        # Most messages should have been collected (some may fail after max retries)
        assert len(ids) >= 15  # at least 75%

    def test_adaptive_throttle_reduces_on_limit(self):
        """AdaptiveThrottle should reduce workers and batch size on rate limit."""
        throttle = AdaptiveThrottle(5, 50)
        throttle.on_rate_limit()
        assert throttle.max_workers == 2
        assert throttle.batch_size == 25

        throttle.on_rate_limit()
        assert throttle.max_workers == 1
        assert throttle.batch_size == 12

    def test_fetch_batch_separates_rate_limited(self):
        """_fetch_batch should separate rate-limited IDs from failures."""
        inbox = MockGmailInbox(
            num_messages=10,
            rate_limit_after=5,
            rate_limit_count=3,
        )
        service = inbox.build_service()
        ids = [f"msg_{i:06d}" for i in range(10)]

        entries, failed, rate_limited = _fetch_batch(service, ids)

        # Some should succeed, some rate-limited
        assert len(entries) + len(failed) + len(rate_limited) == 10
        assert len(rate_limited) > 0


class TestGroomAfterCollection:
    """Test the groom->sniper pipeline after mock collection."""

    def test_groom_deduplicates(self, temp_vault):
        """Run collect twice (simulating overlap), groom should dedup."""
        inbox = MockGmailInbox(num_messages=20, page_size=50)

        config = {
            "vault_root": temp_vault,
            "gmail": {
                "max_workers": 1,
                "batch_size": 20,
                "page_size": 50,
                "scope": "https://www.googleapis.com/auth/gmail.readonly",
                "credentials_file": "fake.json",
                "token_file": "fake_token.json",
            },
        }

        mock_service = inbox.build_service()
        mock_creds = inbox.build_credentials()

        with patch(
            "collectors.gmail_collector.get_credentials",
            return_value=mock_creds,
        ), patch(
            "collectors.gmail_collector.build",
            return_value=mock_service,
        ):
            run_export(vault_name="Dedup", config=config)

        vault_path = os.path.join(temp_vault, "Gmail_Dedup")

        # Manually duplicate some entries to simulate a bug
        for root, dirs, files in os.walk(vault_path):
            for f in files:
                if f.endswith(".jsonl"):
                    fpath = os.path.join(root, f)
                    with open(fpath) as fh:
                        lines = fh.readlines()
                    with open(fpath, "a") as fh:
                        for line in lines[:3]:
                            fh.write(line)  # duplicate first 3
                    break

        groom_vault(vault_path)

        # Count after grooming
        total = 0
        for root, dirs, files in os.walk(vault_path):
            for f in files:
                if f.endswith(".jsonl"):
                    with open(os.path.join(root, f)) as fh:
                        total += sum(1 for _ in fh)

        assert total == 20  # back to 20 after dedup

    def test_sniper_detects_ghosts(self, temp_vault):
        """Groom should detect ghost IDs and write missing_ids.txt."""
        inbox = MockGmailInbox(num_messages=10, page_size=50)

        config = {
            "vault_root": temp_vault,
            "gmail": {
                "max_workers": 1,
                "batch_size": 10,
                "page_size": 50,
                "scope": "https://www.googleapis.com/auth/gmail.readonly",
                "credentials_file": "fake.json",
                "token_file": "fake_token.json",
            },
        }

        with patch(
            "collectors.gmail_collector.get_credentials",
            return_value=inbox.build_credentials(),
        ), patch(
            "collectors.gmail_collector.build",
            return_value=inbox.build_service(),
        ):
            run_export(vault_name="Ghost", config=config)

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
        """After grooming, entries should be sorted by date."""
        inbox = MockGmailInbox(
            num_messages=30, page_size=50, include_bad_dates=False
        )

        config = {
            "vault_root": temp_vault,
            "gmail": {
                "max_workers": 1,
                "batch_size": 30,
                "page_size": 50,
                "scope": "https://www.googleapis.com/auth/gmail.readonly",
                "credentials_file": "fake.json",
                "token_file": "fake_token.json",
            },
        }

        with patch(
            "collectors.gmail_collector.get_credentials",
            return_value=inbox.build_credentials(),
        ), patch(
            "collectors.gmail_collector.build",
            return_value=inbox.build_service(),
        ):
            run_export(vault_name="Sort", config=config)

        vault_path = os.path.join(temp_vault, "Gmail_Sort")
        groom_vault(vault_path)

        # Check each JSONL file is sorted
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
                        # Dates should be in order
                        for i in range(1, len(dates)):
                            assert dates[i] >= dates[i - 1], (
                                f"Dates not sorted in {f}: {dates[i-1]} > {dates[i]}"
                            )


class TestVaultStructure:
    """Verify the vault directory structure is correct."""

    def test_year_month_folders(self, temp_vault):
        """Vault should have year folders with month JSONL files."""
        inbox = MockGmailInbox(num_messages=50, page_size=50, include_bad_dates=False)

        config = {
            "vault_root": temp_vault,
            "gmail": {
                "max_workers": 1,
                "batch_size": 50,
                "page_size": 50,
                "scope": "https://www.googleapis.com/auth/gmail.readonly",
                "credentials_file": "fake.json",
                "token_file": "fake_token.json",
            },
        }

        with patch(
            "collectors.gmail_collector.get_credentials",
            return_value=inbox.build_credentials(),
        ), patch(
            "collectors.gmail_collector.build",
            return_value=inbox.build_service(),
        ):
            run_export(vault_name="Structure", config=config)

        vault_path = os.path.join(temp_vault, "Gmail_Structure")

        # Should have year directories
        year_dirs = [
            d
            for d in os.listdir(vault_path)
            if os.path.isdir(os.path.join(vault_path, d)) and d.isdigit()
        ]
        assert len(year_dirs) > 0

        # Each year dir should have month files
        for year_dir in year_dirs:
            year_path = os.path.join(vault_path, year_dir)
            jsonl_files = [f for f in os.listdir(year_path) if f.endswith(".jsonl")]
            assert len(jsonl_files) > 0

            for jf in jsonl_files:
                # Format should be XX_MonthName.jsonl
                assert "_" in jf
                month_num = jf.split("_")[0]
                assert month_num.isdigit()
                assert 1 <= int(month_num) <= 12

    def test_extraction_log_created(self, temp_vault):
        """extraction.log should be created in vault root."""
        inbox = MockGmailInbox(num_messages=5, page_size=50)

        config = {
            "vault_root": temp_vault,
            "gmail": {
                "max_workers": 1,
                "batch_size": 5,
                "page_size": 50,
                "scope": "https://www.googleapis.com/auth/gmail.readonly",
                "credentials_file": "fake.json",
                "token_file": "fake_token.json",
            },
        }

        with patch(
            "collectors.gmail_collector.get_credentials",
            return_value=inbox.build_credentials(),
        ), patch(
            "collectors.gmail_collector.build",
            return_value=inbox.build_service(),
        ):
            run_export(vault_name="Log", config=config)

        log_file = os.path.join(temp_vault, "Gmail_Log", "extraction.log")
        assert os.path.exists(log_file)

    def test_jsonl_entries_are_valid(self, temp_vault):
        """Every line in every JSONL file should be valid JSON with required fields."""
        inbox = MockGmailInbox(num_messages=30, page_size=50)

        config = {
            "vault_root": temp_vault,
            "gmail": {
                "max_workers": 1,
                "batch_size": 30,
                "page_size": 50,
                "scope": "https://www.googleapis.com/auth/gmail.readonly",
                "credentials_file": "fake.json",
                "token_file": "fake_token.json",
            },
        }

        with patch(
            "collectors.gmail_collector.get_credentials",
            return_value=inbox.build_credentials(),
        ), patch(
            "collectors.gmail_collector.build",
            return_value=inbox.build_service(),
        ):
            run_export(vault_name="Valid", config=config)

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
