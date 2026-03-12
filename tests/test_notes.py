"""Tests for the Notes collector — markdown/text and media file parsers."""

import json
import os
import tempfile

import pytest

from collectors.notes import (
    _make_id,
    _extract_title,
    _extract_frontmatter_tags,
    _strip_frontmatter,
    _read_file,
    _parse_note_file,
    _parse_media_file,
    NOTE_EXTENSIONS,
    AUDIO_EXTENSIONS,
    VIDEO_EXTENSIONS,
    run_import,
)


# ═══════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════

class TestMakeId:
    def test_deterministic(self):
        assert _make_id("path/to/file.md") == _make_id("path/to/file.md")

    def test_prefix(self):
        assert _make_id("file.md").startswith("notes:")

    def test_different(self):
        assert _make_id("a.md") != _make_id("b.md")


class TestExtractTitle:
    def test_heading(self):
        content = "# My Great Note\n\nSome content here."
        assert _extract_title(content, "fallback.md") == "My Great Note"

    def test_heading_with_spaces(self):
        content = "#   Spaced Title  \n\nBody."
        assert _extract_title(content, "fallback.md") == "Spaced Title"

    def test_no_heading_uses_filename(self):
        content = "Just some text without a heading."
        assert _extract_title(content, "my-note.md") == "my note"

    def test_empty_content(self):
        assert _extract_title("", "my_note.md") == "my note"

    def test_none_content(self):
        assert _extract_title(None, "file-name.md") == "file name"

    def test_second_heading_ignored(self):
        content = "Some preamble\n# First Heading\n## Second Heading"
        assert _extract_title(content, "f.md") == "First Heading"


class TestExtractFrontmatterTags:
    def test_list_format(self):
        content = "---\ntags: [python, coding, notes]\n---\n\nBody text."
        tags = _extract_frontmatter_tags(content)
        assert tags == ["python", "coding", "notes"]

    def test_yaml_list_format(self):
        content = "---\ntags:\n  - alpha\n  - beta\n---\n\nBody."
        tags = _extract_frontmatter_tags(content)
        assert "alpha" in tags
        assert "beta" in tags

    def test_comma_string(self):
        content = "---\ntags: python, coding\n---\n\nBody."
        tags = _extract_frontmatter_tags(content)
        assert "python" in tags
        assert "coding" in tags

    def test_no_frontmatter(self):
        content = "Just plain text."
        assert _extract_frontmatter_tags(content) == []

    def test_empty_content(self):
        assert _extract_frontmatter_tags("") == []

    def test_none_content(self):
        assert _extract_frontmatter_tags(None) == []

    def test_no_tags_key(self):
        content = "---\ntitle: My Note\n---\n\nBody."
        assert _extract_frontmatter_tags(content) == []

    def test_unclosed_frontmatter(self):
        content = "---\ntags: [a, b]\nNo closing marker"
        assert _extract_frontmatter_tags(content) == []


class TestStripFrontmatter:
    def test_with_frontmatter(self):
        content = "---\ntags: [a]\n---\n\nBody text here."
        result = _strip_frontmatter(content)
        assert result == "Body text here."

    def test_without_frontmatter(self):
        content = "Just body text."
        assert _strip_frontmatter(content) == "Just body text."

    def test_empty(self):
        assert _strip_frontmatter("") == ""

    def test_none(self):
        assert _strip_frontmatter(None) is None


class TestReadFile:
    def test_utf8(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".md", delete=False, encoding="utf-8") as f:
            f.write("Hello world")
            path = f.name
        try:
            assert _read_file(path) == "Hello world"
        finally:
            os.unlink(path)

    def test_latin1(self):
        with tempfile.NamedTemporaryFile(mode="wb", suffix=".md", delete=False) as f:
            f.write("Caf\xe9".encode("latin-1"))
            path = f.name
        try:
            result = _read_file(path)
            assert result is not None
            assert "Caf" in result
        finally:
            os.unlink(path)

    def test_nonexistent(self):
        assert _read_file("/nonexistent/file.md") is None


# ═══════════════════════════════════════════════════════════════════════
# Note File Parser
# ═══════════════════════════════════════════════════════════════════════

class TestParseNoteFile:
    def test_basic_note(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            note_path = os.path.join(tmpdir, "test.md")
            with open(note_path, "w") as f:
                f.write("# Test Note\n\nThis is a test note with some content.")

            entry = _parse_note_file(note_path, tmpdir)
            assert entry is not None
            assert entry["title"] == "Test Note"
            assert "test note" in entry["body"].lower()
            assert entry["word_count"] > 0
            assert entry["id"].startswith("notes:")

    def test_with_frontmatter(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            note_path = os.path.join(tmpdir, "tagged.md")
            with open(note_path, "w") as f:
                f.write("---\ntags: [journal, daily]\n---\n\n# Daily Entry\n\nWent for a walk.")

            entry = _parse_note_file(note_path, tmpdir)
            assert entry is not None
            assert "journal" in entry["tags"]
            assert "daily" in entry["tags"]

    def test_empty_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            note_path = os.path.join(tmpdir, "empty.md")
            with open(note_path, "w") as f:
                f.write("")

            entry = _parse_note_file(note_path, tmpdir)
            assert entry is None

    def test_whitespace_only(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            note_path = os.path.join(tmpdir, "blank.md")
            with open(note_path, "w") as f:
                f.write("   \n\n   \n")

            entry = _parse_note_file(note_path, tmpdir)
            assert entry is None


# ═══════════════════════════════════════════════════════════════════════
# Media File Parser
# ═══════════════════════════════════════════════════════════════════════

class TestParseMediaFile:
    def test_audio_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            audio_path = os.path.join(tmpdir, "recording-2024.mp3")
            with open(audio_path, "wb") as f:
                f.write(b"\x00" * 1024)

            entry = _parse_media_file(audio_path, tmpdir)
            assert entry is not None
            assert entry["media_type"] == "audio-recording"
            assert "audio-recording" in entry["tags"]
            assert entry["format"] == "mp3"

    def test_video_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            video_path = os.path.join(tmpdir, "meeting.mp4")
            with open(video_path, "wb") as f:
                f.write(b"\x00" * 2048)

            entry = _parse_media_file(video_path, tmpdir)
            assert entry is not None
            assert entry["media_type"] == "video-recording"
            assert entry["format"] == "mp4"

    def test_unsupported_extension(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.csv")
            with open(path, "w") as f:
                f.write("data")

            entry = _parse_media_file(path, tmpdir)
            assert entry is None

    def test_nonexistent_file(self):
        entry = _parse_media_file("/nonexistent/file.mp3", "/nonexistent")
        assert entry is None


# ═══════════════════════════════════════════════════════════════════════
# End-to-End Import
# ═══════════════════════════════════════════════════════════════════════

class TestRunImport:
    def test_basic_import(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            notes_dir = os.path.join(tmpdir, "notes")
            vault_root = os.path.join(tmpdir, "vaults")
            os.makedirs(notes_dir)

            with open(os.path.join(notes_dir, "note1.md"), "w") as f:
                f.write("# Note One\n\nContent of note one.")
            with open(os.path.join(notes_dir, "note2.txt"), "w") as f:
                f.write("Plain text note content here.")

            run_import(notes_dir, config={"vault_root": vault_root})

            jsonl = os.path.join(vault_root, "Notes", "notes.jsonl")
            assert os.path.isfile(jsonl)
            with open(jsonl) as f:
                entries = [json.loads(line) for line in f if line.strip()]
            assert len(entries) == 2

    def test_deduplication(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            notes_dir = os.path.join(tmpdir, "notes")
            vault_root = os.path.join(tmpdir, "vaults")
            os.makedirs(notes_dir)

            with open(os.path.join(notes_dir, "note.md"), "w") as f:
                f.write("# Dedupe Test\n\nContent.")

            run_import(notes_dir, config={"vault_root": vault_root})
            run_import(notes_dir, config={"vault_root": vault_root})

            jsonl = os.path.join(vault_root, "Notes", "notes.jsonl")
            with open(jsonl) as f:
                entries = [json.loads(line) for line in f if line.strip()]
            assert len(entries) == 1

    def test_with_media_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            notes_dir = os.path.join(tmpdir, "notes")
            vault_root = os.path.join(tmpdir, "vaults")
            os.makedirs(notes_dir)

            with open(os.path.join(notes_dir, "note.md"), "w") as f:
                f.write("# A Note\n\nText content.")
            with open(os.path.join(notes_dir, "recording.mp3"), "wb") as f:
                f.write(b"\x00" * 512)

            run_import(notes_dir, config={"vault_root": vault_root})

            jsonl = os.path.join(vault_root, "Notes", "notes.jsonl")
            with open(jsonl) as f:
                entries = [json.loads(line) for line in f if line.strip()]
            assert len(entries) == 2
            types = {e.get("media_type", "text") for e in entries}
            assert "audio-recording" in types

    def test_empty_directory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            notes_dir = os.path.join(tmpdir, "notes")
            vault_root = os.path.join(tmpdir, "vaults")
            os.makedirs(notes_dir)

            run_import(notes_dir, config={"vault_root": vault_root})
            # No vault file created
            jsonl = os.path.join(vault_root, "Notes", "notes.jsonl")
            assert not os.path.isfile(jsonl)

    def test_nonexistent_directory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            vault_root = os.path.join(tmpdir, "vaults")
            # Should not raise, just prints error
            run_import("/nonexistent/dir", config={"vault_root": vault_root})
