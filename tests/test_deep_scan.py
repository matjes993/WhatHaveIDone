"""
Tests for the Deep Computer Scanner collector.

Uses a temporary directory with synthetic files to test discovery,
classification, content extraction, overlap detection, and vault writing.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from collectors.deep_scan import (
    SKIP_DIRS,
    SKIP_EXTENSIONS,
    _classify_location,
    _detect_language,
    _extract_imports,
    _is_overlap,
    _is_secret,
    _make_id,
    _ts_to_iso,
    build_vault_entry,
    compute_partial_hash,
    deep_scan,
    discover_files,
    extract_content,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def scan_dir(tmp_path):
    """Create a realistic temp directory tree for scanning."""
    # Documents
    docs = tmp_path / "Documents"
    docs.mkdir()
    (docs / "report.pdf").write_bytes(b"%PDF-1.4 fake pdf content here")
    (docs / "notes.txt").write_text("Meeting notes from yesterday\nAction items:\n- Fix the bug\n- Deploy v2")
    (docs / "resume.docx").write_bytes(b"PK\x03\x04 fake docx")

    # Documents/Work
    work = docs / "Work"
    work.mkdir()
    (work / "quarterly_review.xlsx").write_bytes(b"PK\x03\x04 fake xlsx")

    # Desktop
    desktop = tmp_path / "Desktop"
    desktop.mkdir()
    (desktop / "todo.md").write_text("# TODO\n- Buy groceries\n- Call dentist")
    (desktop / "screenshot.png").write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 100)

    # Downloads
    downloads = tmp_path / "Downloads"
    downloads.mkdir()
    (downloads / "installer.dmg").write_bytes(b"\x00" * 50)  # Should be skipped
    (downloads / "paper.pdf").write_bytes(b"%PDF-1.5 academic paper")
    (downloads / "data.csv").write_text("name,age\nAlice,30\nBob,25")

    # Code projects
    code = tmp_path / "projects"
    code.mkdir()
    (code / "app.py").write_text("import flask\nfrom datetime import datetime\n\napp = flask.Flask(__name__)\n")
    (code / "index.js").write_text("import React from 'react';\nconst App = () => <div>Hello</div>;\nexport default App;")
    (code / "main.go").write_text("package main\n\nimport \"fmt\"\n\nfunc main() {\n\tfmt.Println(\"hello\")\n}")
    (code / "Cargo.toml").write_text("[package]\nname = \"myapp\"\nversion = \"0.1.0\"")

    # node_modules (should be skipped entirely)
    nm = code / "node_modules"
    nm.mkdir()
    (nm / "express" / "index.js").parent.mkdir(parents=True)
    (nm / "express" / "index.js").write_text("module.exports = {}")

    # Hidden dirs
    ssh = tmp_path / ".ssh"
    ssh.mkdir()
    (ssh / "id_rsa").write_text("-----BEGIN RSA PRIVATE KEY-----\nfake")
    (ssh / "known_hosts").write_text("github.com ssh-rsa AAAA...")
    (ssh / "config").write_text("Host github.com\n  User git")

    # Secrets
    (tmp_path / ".env").write_text("API_KEY=secret123")
    (tmp_path / "credentials.json").write_text('{"token": "secret"}')

    # Images
    photos = tmp_path / "Pictures"
    photos.mkdir()
    (photos / "vacation.jpg").write_bytes(b"\xff\xd8\xff\xe0" + b"\x00" * 200)
    (photos / "diagram.svg").write_text('<svg xmlns="http://www.w3.org/2000/svg"><circle r="50"/></svg>')

    # Music
    music = tmp_path / "Music"
    music.mkdir()
    (music / "song.mp3").write_bytes(b"ID3" + b"\x00" * 100)

    # Mail overlap (should be detected)
    mail = tmp_path / "Library" / "Mail"
    mail.mkdir(parents=True)
    (mail / "email.emlx").write_text("From: test@example.com")

    # Git repo
    git = code / ".git"
    git.mkdir()
    (git / "HEAD").write_text("ref: refs/heads/main")

    return tmp_path


# ---------------------------------------------------------------------------
# Helper tests
# ---------------------------------------------------------------------------


class TestHelpers:
    def test_make_id_deterministic(self):
        assert _make_id("foo/bar.txt") == _make_id("foo/bar.txt")
        assert _make_id("foo/bar.txt") != _make_id("foo/baz.txt")
        assert _make_id("foo/bar.txt").startswith("local:deep_scan:")

    def test_ts_to_iso(self):
        result = _ts_to_iso(0)
        assert "1970" in result

    def test_classify_location(self):
        assert _classify_location("Desktop/file.txt") == "desktop"
        assert _classify_location("Documents/file.txt") == "documents"
        assert _classify_location("Documents/Work/file.txt") == "work"
        assert _classify_location("Downloads/file.zip") == "downloads"
        assert _classify_location("Pictures/photo.jpg") == "photos"
        assert _classify_location("projects/app.py") == "code"
        assert _classify_location(".config/settings.json") == "system_config"

    def test_is_secret(self):
        assert _is_secret(".env") is True
        assert _is_secret("credentials.json") is True
        assert _is_secret("id_rsa") is True
        assert _is_secret(".pem") is True
        assert _is_secret("config.yaml") is True
        assert _is_secret("readme.md") is False
        assert _is_secret("app.py") is False

    def test_detect_language(self):
        assert _detect_language(".py") == "Python"
        assert _detect_language(".js") == "JavaScript"
        assert _detect_language(".go") == "Go"
        assert _detect_language(".rs") == "Rust"

    def test_extract_imports_python(self):
        code = "import os\nfrom pathlib import Path\nprint('hello')"
        imports = _extract_imports(code, ".py")
        assert len(imports) == 2
        assert "import os" in imports

    def test_extract_imports_javascript(self):
        code = "import React from 'react';\nconst x = require('express');"
        imports = _extract_imports(code, ".js")
        assert len(imports) == 2

    def test_extract_imports_go(self):
        code = 'package main\n\nimport "fmt"\n\nfunc main() {}'
        imports = _extract_imports(code, ".go")
        assert len(imports) == 1


# ---------------------------------------------------------------------------
# Overlap detection
# ---------------------------------------------------------------------------


class TestOverlapDetection:
    def test_mail_overlap(self):
        assert _is_overlap("Library/Mail/V10/msg.emlx", ".emlx") is True

    def test_photos_overlap(self):
        assert _is_overlap("Pictures/Photos Library.photoslibrary/photo.jpg", ".jpg") is True

    def test_contacts_overlap(self):
        assert _is_overlap("Library/Application Support/AddressBook/contacts.db", ".db") is True

    def test_messages_overlap(self):
        assert _is_overlap("Library/Messages/chat.db", ".db") is True

    def test_safari_overlap(self):
        assert _is_overlap("Library/Safari/History.db", ".db") is True

    def test_chrome_overlap(self):
        assert _is_overlap("Library/Application Support/Google/Chrome/Default/History", "") is True

    def test_emlx_extension_overlap(self):
        assert _is_overlap("any/path/message.emlx", ".emlx") is True

    def test_vcf_extension_overlap(self):
        assert _is_overlap("any/path/contact.vcf", ".vcf") is True

    def test_normal_file_no_overlap(self):
        assert _is_overlap("Documents/report.pdf", ".pdf") is False
        assert _is_overlap("Desktop/notes.txt", ".txt") is False
        assert _is_overlap("projects/app.py", ".py") is False


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


class TestDiscovery:
    def test_discovers_files(self, scan_dir):
        files = discover_files(str(scan_dir))
        assert len(files) > 5

    def test_skips_node_modules(self, scan_dir):
        files = discover_files(str(scan_dir))
        paths = [f["rel_path"] for f in files]
        assert not any("node_modules" in p for p in paths)

    def test_skips_git_dir(self, scan_dir):
        files = discover_files(str(scan_dir))
        paths = [f["rel_path"] for f in files]
        assert not any(".git" in p for p in paths)

    def test_skips_dmg(self, scan_dir):
        files = discover_files(str(scan_dir))
        exts = [f["extension"] for f in files]
        assert ".dmg" not in exts

    def test_classifies_file_types(self, scan_dir):
        files = discover_files(str(scan_dir))
        types = {f["file_type"] for f in files}
        assert "text" in types or "code" in types

    def test_assigns_tiers(self, scan_dir):
        files = discover_files(str(scan_dir))
        tiers = {f["tier"] for f in files}
        assert 1 in tiers  # Documents/text
        assert 2 in tiers  # Code/images

    def test_sorted_by_tier(self, scan_dir):
        files = discover_files(str(scan_dir))
        tiers = [f["tier"] for f in files]
        assert tiers == sorted(tiers)

    def test_respects_max_files(self, scan_dir):
        files = discover_files(str(scan_dir), max_files=3)
        assert len(files) == 3

    def test_includes_file_metadata(self, scan_dir):
        files = discover_files(str(scan_dir))
        f = files[0]
        assert "path" in f
        assert "filename" in f
        assert "extension" in f
        assert "size_bytes" in f
        assert "created" in f
        assert "modified" in f
        assert "location" in f
        assert "mime_type" in f


# ---------------------------------------------------------------------------
# Content extraction
# ---------------------------------------------------------------------------


class TestContentExtraction:
    def test_extract_text_file(self, scan_dir):
        files = discover_files(str(scan_dir))
        text_files = [f for f in files if f["filename"] == "notes.txt"]
        assert len(text_files) == 1
        result = extract_content(text_files[0])
        assert "content_preview" in result
        assert "Meeting notes" in result["content_preview"]
        assert result["line_count"] > 0

    def test_extract_code_file(self, scan_dir):
        files = discover_files(str(scan_dir))
        py_files = [f for f in files if f["filename"] == "app.py"]
        assert len(py_files) == 1
        result = extract_content(py_files[0])
        assert "content_preview" in result
        assert result["language"] == "Python"
        assert len(result["imports"]) > 0

    def test_skips_secret_content(self, scan_dir):
        file_info = {
            "path": str(scan_dir / ".env"),
            "filename": ".env",
            "size_bytes": 20,
            "file_type": "config",
            "extension": "",
        }
        result = extract_content(file_info)
        assert result.get("content_skipped") == "secret_file"
        assert "content_preview" not in result

    def test_skips_large_files(self):
        file_info = {
            "path": "/fake/path",
            "filename": "huge.bin",
            "size_bytes": 200_000_000,
            "file_type": "other",
            "extension": ".bin",
        }
        result = extract_content(file_info)
        assert result.get("content_skipped") == "too_large"


# ---------------------------------------------------------------------------
# Vault entry building
# ---------------------------------------------------------------------------


class TestVaultEntry:
    def test_builds_valid_entry(self, scan_dir):
        files = discover_files(str(scan_dir))
        f = files[0]
        extract_content(f)
        entry = build_vault_entry(f)
        assert "id" in entry
        assert entry["id"].startswith("local:deep_scan:")
        assert entry["sources"] == ["deep_scan"]
        assert entry["type"] == "file"
        assert "filename" in entry
        assert "path" in entry
        assert "size_bytes" in entry
        assert "file_for_embedding" in entry
        assert "updated_at" in entry

    def test_embedding_text_includes_filename(self, scan_dir):
        files = discover_files(str(scan_dir))
        f = files[0]
        entry = build_vault_entry(f)
        assert f["filename"] in entry["file_for_embedding"]

    def test_partial_hash_computed(self, scan_dir):
        files = discover_files(str(scan_dir))
        f = files[0]
        ph = compute_partial_hash(f["path"])
        assert ph is not None
        assert len(ph) == 16


# ---------------------------------------------------------------------------
# Full pipeline
# ---------------------------------------------------------------------------


class TestFullPipeline:
    def test_deep_scan_writes_to_vault(self, scan_dir, tmp_path):
        vault_root = str(tmp_path / "vault")
        stats = deep_scan(
            vault_root=vault_root,
            scan_root=str(scan_dir),
            extract_content_flag=True,
            extract_metadata_flag=False,  # Skip mdls in test
        )

        assert stats["discovered"] > 5
        assert stats["entries_written"] > 5
        assert stats["skipped_secret"] > 0  # .env + credentials.json + id_rsa

        # Verify vault file exists
        vault_path = os.path.join(vault_root, "DeepScan")
        assert os.path.exists(os.path.join(vault_path, "files.jsonl"))
        assert os.path.exists(os.path.join(vault_path, "processed_ids.txt"))

        # Read and verify entries
        entries = []
        with open(os.path.join(vault_path, "files.jsonl")) as f:
            for line in f:
                entries.append(json.loads(line))

        assert len(entries) == stats["entries_written"]
        for entry in entries:
            assert entry["id"].startswith("local:deep_scan:")
            assert entry["sources"] == ["deep_scan"]

    def test_idempotent_rescan(self, scan_dir, tmp_path):
        vault_root = str(tmp_path / "vault")

        # First scan
        stats1 = deep_scan(vault_root=vault_root, scan_root=str(scan_dir), extract_metadata_flag=False)
        written1 = stats1["entries_written"]
        assert written1 > 0

        # Second scan — all previously written should be skipped
        stats2 = deep_scan(vault_root=vault_root, scan_root=str(scan_dir), extract_metadata_flag=False)
        assert stats2["skipped_processed"] >= written1
        # New entries should be zero or very small (only if temp files changed)
        assert stats2["entries_written"] <= stats2.get("new", 0)

    def test_overlap_detection_unit(self):
        """Overlap detection catches files from other collector domains."""
        # These paths would be caught IF they pass through discovery
        assert _is_overlap("Library/Mail/V10/msg.emlx", ".emlx") is True
        assert _is_overlap("some/path/contact.vcf", ".vcf") is True
        assert _is_overlap("Documents/report.pdf", ".pdf") is False

    def test_library_skipped_by_discovery(self, scan_dir, tmp_path):
        """Library dir is skipped entirely during discovery (macOS system dir)."""
        vault_root = str(tmp_path / "vault")
        stats = deep_scan(vault_root=vault_root, scan_root=str(scan_dir), extract_metadata_flag=False)

        # Verify no Library files were discovered
        vault_path = os.path.join(vault_root, "DeepScan", "files.jsonl")
        if os.path.exists(vault_path):
            with open(vault_path) as f:
                entries = [json.loads(line) for line in f]
            paths = [e["path"] for e in entries]
            assert not any("Library" in p for p in paths)
