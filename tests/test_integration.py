"""
Integration tests — require real Google credentials.

Run with: pytest tests/test_integration.py -v
Requires: credentials.json and token.json in the project root.

These tests:
- Use gmail.readonly scope (cannot modify your email)
- Create a temporary vault that is cleaned up after
- Only fetch a small number of messages (max 5)
"""

import json
import os
import shutil
import tempfile

import pytest

# Skip all tests if no credentials
CREDS_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), "credentials.json")
TOKEN_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), "token.json")

pytestmark = pytest.mark.skipif(
    not os.path.exists(TOKEN_FILE),
    reason="No token.json found — run 'whid collect gmail' first to authenticate.",
)


@pytest.fixture
def temp_vault():
    vault = tempfile.mkdtemp(prefix="whid_test_")
    yield vault
    shutil.rmtree(vault, ignore_errors=True)


class TestGmailIntegration:
    def test_can_authenticate(self):
        """Test that credentials are valid and can connect to Gmail."""
        from collectors.gmail_collector import get_credentials
        scopes = ["https://www.googleapis.com/auth/gmail.readonly"]
        creds = get_credentials(CREDS_FILE, TOKEN_FILE, scopes)
        assert creds is not None
        assert creds.valid

    def test_can_list_messages(self):
        """Test that we can list messages from Gmail."""
        from collectors.gmail_collector import get_credentials
        from googleapiclient.discovery import build

        scopes = ["https://www.googleapis.com/auth/gmail.readonly"]
        creds = get_credentials(CREDS_FILE, TOKEN_FILE, scopes)
        service = build("gmail", "v1", credentials=creds)

        results = service.users().messages().list(userId="me", maxResults=5).execute()
        msgs = results.get("messages", [])
        assert len(msgs) > 0, "Gmail inbox appears empty"
        assert "id" in msgs[0]

    def test_can_fetch_single_message(self):
        """Test that we can fetch a single message's content."""
        from collectors.gmail_collector import get_credentials, _msg_to_entry
        from googleapiclient.discovery import build

        scopes = ["https://www.googleapis.com/auth/gmail.readonly"]
        creds = get_credentials(CREDS_FILE, TOKEN_FILE, scopes)
        service = build("gmail", "v1", credentials=creds)

        results = service.users().messages().list(userId="me", maxResults=1).execute()
        msg_id = results["messages"][0]["id"]

        msg = service.users().messages().get(userId="me", id=msg_id).execute()
        entry = _msg_to_entry(msg_id, msg)

        assert entry["id"] == msg_id
        assert entry["threadId"] != ""
        assert isinstance(entry["tags"], list)

    def test_small_collection_run(self, temp_vault):
        """Test a small collection run (5 messages) end-to-end."""
        from collectors.gmail_collector import run_export

        config = {
            "vault_root": temp_vault,
            "gmail": {
                "max_workers": 1,
                "batch_size": 5,
                "page_size": 5,
                "scope": "https://www.googleapis.com/auth/gmail.readonly",
                "credentials_file": CREDS_FILE,
                "token_file": TOKEN_FILE,
            },
        }

        run_export(vault_name="Test", config=config)

        vault_path = os.path.join(temp_vault, "Gmail_Test")
        assert os.path.exists(vault_path), "Vault directory was not created"

        # Check that some JSONL files were written
        jsonl_count = 0
        entry_count = 0
        for root, dirs, files in os.walk(vault_path):
            for f in files:
                if f.endswith(".jsonl"):
                    jsonl_count += 1
                    with open(os.path.join(root, f)) as fh:
                        for line in fh:
                            entry = json.loads(line.strip())
                            assert "id" in entry
                            assert "subject" in entry
                            entry_count += 1

        assert jsonl_count > 0, "No JSONL files created"
        assert entry_count > 0, "No entries written"

        # Check processed_ids.txt
        processed = os.path.join(vault_path, "processed_ids.txt")
        assert os.path.exists(processed)

    def test_groom_after_collection(self, temp_vault):
        """Test grooming a real vault after collection."""
        from collectors.gmail_collector import run_export
        from core.groomer import groom_vault

        config = {
            "vault_root": temp_vault,
            "gmail": {
                "max_workers": 1,
                "batch_size": 5,
                "page_size": 5,
                "scope": "https://www.googleapis.com/auth/gmail.readonly",
                "credentials_file": CREDS_FILE,
                "token_file": TOKEN_FILE,
            },
        }

        run_export(vault_name="Test", config=config)
        vault_path = os.path.join(temp_vault, "Gmail_Test")

        # Groom should not crash and should not produce ghosts
        groom_vault(vault_path)

        missing = os.path.join(vault_path, "missing_ids.txt")
        assert not os.path.exists(missing), "Groomer found ghosts after fresh collection"
