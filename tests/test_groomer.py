"""Tests for core/groomer.py — vault grooming logic."""

import json
import os
import tempfile

import pytest

from core.groomer import groom_vault, parse_date, _sort_key, _atomic_write


# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------
class TestParseDate:
    def test_rfc2822_format(self):
        dt = parse_date("Mon, 01 Jan 2024 12:00:00 +0000")
        assert dt is not None
        assert dt.year == 2024
        assert dt.month == 1
        assert dt.day == 1

    def test_rfc2822_with_timezone_name(self):
        dt = parse_date("Mon, 01 Jan 2024 12:00:00 +0000 (UTC)")
        assert dt is not None
        assert dt.year == 2024

    def test_rfc2822_different_timezone(self):
        dt = parse_date("Fri, 15 Mar 2024 08:30:00 +0530")
        assert dt is not None
        assert dt.month == 3

    def test_iso_format(self):
        dt = parse_date("2024-06-15T10:30:00+02:00")
        assert dt is not None
        assert dt.year == 2024
        assert dt.month == 6

    def test_empty_string(self):
        assert parse_date("") is None

    def test_none(self):
        assert parse_date(None) is None

    def test_garbage(self):
        assert parse_date("not a date at all") is None

    def test_numeric_string(self):
        assert parse_date("12345") is None

    def test_whitespace_only(self):
        assert parse_date("   ") is None


# ---------------------------------------------------------------------------
# Sort key
# ---------------------------------------------------------------------------
class TestSortKey:
    def test_valid_dates_sort_correctly(self):
        entries = [
            {"date": "Wed, 15 Mar 2024 12:00:00 +0000"},
            {"date": "Mon, 01 Jan 2024 12:00:00 +0000"},
            {"date": "Fri, 28 Jun 2024 12:00:00 +0000"},
        ]
        sorted_entries = sorted(entries, key=_sort_key)
        assert sorted_entries[0]["date"].startswith("Mon, 01 Jan")
        assert sorted_entries[1]["date"].startswith("Wed, 15 Mar")
        assert sorted_entries[2]["date"].startswith("Fri, 28 Jun")

    def test_invalid_dates_sort_last(self):
        entries = [
            {"date": "INVALID"},
            {"date": "Mon, 01 Jan 2024 12:00:00 +0000"},
            {"date": ""},
        ]
        sorted_entries = sorted(entries, key=_sort_key)
        assert sorted_entries[0]["date"] == "Mon, 01 Jan 2024 12:00:00 +0000"
        # Invalid dates should be after valid ones
        assert sorted_entries[1]["date"] in ("INVALID", "")
        assert sorted_entries[2]["date"] in ("INVALID", "")

    def test_missing_date_field(self):
        entries = [
            {"id": "no_date"},
            {"date": "Mon, 01 Jan 2024 12:00:00 +0000"},
        ]
        sorted_entries = sorted(entries, key=_sort_key)
        assert sorted_entries[0].get("date") == "Mon, 01 Jan 2024 12:00:00 +0000"


# ---------------------------------------------------------------------------
# Atomic writes
# ---------------------------------------------------------------------------
class TestAtomicWrite:
    def test_writes_file(self, tmp_path):
        file_path = str(tmp_path / "test.txt")
        _atomic_write(file_path, ["line1\n", "line2\n"])

        with open(file_path) as f:
            content = f.read()
        assert content == "line1\nline2\n"

    def test_overwrites_existing(self, tmp_path):
        file_path = str(tmp_path / "test.txt")
        with open(file_path, "w") as f:
            f.write("old content\n")

        _atomic_write(file_path, ["new content\n"])

        with open(file_path) as f:
            assert f.read() == "new content\n"

    def test_no_partial_writes(self, tmp_path):
        """If write fails, original file should be unchanged."""
        file_path = str(tmp_path / "test.txt")
        with open(file_path, "w") as f:
            f.write("original\n")

        class FailingIter:
            def __init__(self):
                self.count = 0
            def __iter__(self):
                return self
            def __next__(self):
                self.count += 1
                if self.count > 1:
                    raise RuntimeError("simulated write failure")
                return "line1\n"

        with pytest.raises(RuntimeError):
            _atomic_write(file_path, FailingIter())

        with open(file_path) as f:
            assert f.read() == "original\n"

    def test_empty_lines(self, tmp_path):
        file_path = str(tmp_path / "empty.txt")
        _atomic_write(file_path, [])

        with open(file_path) as f:
            assert f.read() == ""

    def test_no_temp_file_left_on_success(self, tmp_path):
        file_path = str(tmp_path / "test.txt")
        _atomic_write(file_path, ["content\n"])

        files = os.listdir(str(tmp_path))
        assert len(files) == 1  # only the target file, no .tmp

    def test_no_temp_file_left_on_failure(self, tmp_path):
        file_path = str(tmp_path / "test.txt")
        with open(file_path, "w") as f:
            f.write("original\n")

        def bad_iter():
            yield "line\n"
            raise RuntimeError("fail")

        with pytest.raises(RuntimeError):
            _atomic_write(file_path, bad_iter())

        files = os.listdir(str(tmp_path))
        assert len(files) == 1  # only original file, no .tmp


# ---------------------------------------------------------------------------
# Groom vault
# ---------------------------------------------------------------------------
class TestGroomVault:
    def _make_entry(self, id, date="Mon, 01 Jan 2024 12:00:00 +0000", subject="Test"):
        return {"id": id, "date": date, "subject": subject}

    def test_deduplication(self, tmp_path):
        vault = str(tmp_path / "vault")
        os.makedirs(vault)

        jsonl = os.path.join(vault, "test.jsonl")
        entries = [
            self._make_entry("a"),
            self._make_entry("b"),
            self._make_entry("a"),  # duplicate
        ]
        with open(jsonl, "w") as f:
            for e in entries:
                f.write(json.dumps(e) + "\n")

        groom_vault(vault)

        with open(jsonl) as f:
            lines = [json.loads(l) for l in f if l.strip()]
        assert len(lines) == 2
        ids = [l["id"] for l in lines]
        assert "a" in ids
        assert "b" in ids

    def test_triple_duplicates(self, tmp_path):
        vault = str(tmp_path / "vault")
        os.makedirs(vault)

        jsonl = os.path.join(vault, "test.jsonl")
        entries = [self._make_entry("a")] * 5 + [self._make_entry("b")]
        with open(jsonl, "w") as f:
            for e in entries:
                f.write(json.dumps(e) + "\n")

        groom_vault(vault)

        with open(jsonl) as f:
            lines = [json.loads(l) for l in f if l.strip()]
        assert len(lines) == 2

    def test_chronological_sort(self, tmp_path):
        vault = str(tmp_path / "vault")
        os.makedirs(vault)

        jsonl = os.path.join(vault, "test.jsonl")
        entries = [
            self._make_entry("b", "Wed, 15 Mar 2024 12:00:00 +0000"),
            self._make_entry("a", "Mon, 01 Jan 2024 12:00:00 +0000"),
            self._make_entry("c", "Fri, 28 Jun 2024 12:00:00 +0000"),
        ]
        with open(jsonl, "w") as f:
            for e in entries:
                f.write(json.dumps(e) + "\n")

        groom_vault(vault)

        with open(jsonl) as f:
            lines = [json.loads(l) for l in f if l.strip()]
        assert [l["id"] for l in lines] == ["a", "b", "c"]

    def test_ghost_detection(self, tmp_path):
        """Sniper mechanism: detect IDs in processed_ids.txt that are missing from disk."""
        vault = str(tmp_path / "vault")
        os.makedirs(vault)

        # Simulate: processed_ids.txt has 3 IDs, but only 2 are on disk
        with open(os.path.join(vault, "processed_ids.txt"), "w") as f:
            f.write("a\nb\nc\n")

        jsonl = os.path.join(vault, "test.jsonl")
        entries = [
            self._make_entry("a"),
            self._make_entry("b"),
            # "c" is missing — it's a ghost
        ]
        with open(jsonl, "w") as f:
            for e in entries:
                f.write(json.dumps(e) + "\n")

        groom_vault(vault)

        missing_log = os.path.join(vault, "missing_ids.txt")
        assert os.path.exists(missing_log)
        with open(missing_log) as f:
            ghosts = [l.strip() for l in f if l.strip()]
        assert ghosts == ["c"]

    def test_multiple_ghosts(self, tmp_path):
        vault = str(tmp_path / "vault")
        os.makedirs(vault)

        with open(os.path.join(vault, "processed_ids.txt"), "w") as f:
            f.write("a\nb\nc\nd\ne\n")

        jsonl = os.path.join(vault, "test.jsonl")
        entries = [self._make_entry("a"), self._make_entry("c")]
        with open(jsonl, "w") as f:
            for e in entries:
                f.write(json.dumps(e) + "\n")

        groom_vault(vault)

        missing_log = os.path.join(vault, "missing_ids.txt")
        with open(missing_log) as f:
            ghosts = set(l.strip() for l in f if l.strip())
        assert ghosts == {"b", "d", "e"}

    def test_no_ghosts_no_missing_file(self, tmp_path):
        vault = str(tmp_path / "vault")
        os.makedirs(vault)

        with open(os.path.join(vault, "processed_ids.txt"), "w") as f:
            f.write("a\nb\n")

        jsonl = os.path.join(vault, "test.jsonl")
        entries = [self._make_entry("a"), self._make_entry("b")]
        with open(jsonl, "w") as f:
            for e in entries:
                f.write(json.dumps(e) + "\n")

        groom_vault(vault)

        assert not os.path.exists(os.path.join(vault, "missing_ids.txt"))

    def test_removes_stale_missing_file(self, tmp_path):
        """If no ghosts remain, missing_ids.txt should be removed."""
        vault = str(tmp_path / "vault")
        os.makedirs(vault)

        # Create a stale missing_ids.txt
        with open(os.path.join(vault, "missing_ids.txt"), "w") as f:
            f.write("a\n")

        with open(os.path.join(vault, "processed_ids.txt"), "w") as f:
            f.write("a\n")

        jsonl = os.path.join(vault, "test.jsonl")
        entries = [self._make_entry("a")]
        with open(jsonl, "w") as f:
            for e in entries:
                f.write(json.dumps(e) + "\n")

        groom_vault(vault)

        assert not os.path.exists(os.path.join(vault, "missing_ids.txt"))

    def test_malformed_json_skipped(self, tmp_path):
        vault = str(tmp_path / "vault")
        os.makedirs(vault)

        jsonl = os.path.join(vault, "test.jsonl")
        with open(jsonl, "w") as f:
            f.write(json.dumps(self._make_entry("a")) + "\n")
            f.write("this is not json\n")
            f.write(json.dumps(self._make_entry("b")) + "\n")

        groom_vault(vault)

        with open(jsonl) as f:
            lines = [json.loads(l) for l in f if l.strip()]
        assert len(lines) == 2

    def test_entry_without_id_skipped(self, tmp_path):
        vault = str(tmp_path / "vault")
        os.makedirs(vault)

        jsonl = os.path.join(vault, "test.jsonl")
        with open(jsonl, "w") as f:
            f.write(json.dumps(self._make_entry("a")) + "\n")
            f.write(json.dumps({"date": "Mon, 01 Jan 2024 12:00:00 +0000", "subject": "no id"}) + "\n")
            f.write(json.dumps(self._make_entry("b")) + "\n")

        groom_vault(vault)

        with open(jsonl) as f:
            lines = [json.loads(l) for l in f if l.strip()]
        assert len(lines) == 2

    def test_empty_lines_in_jsonl(self, tmp_path):
        vault = str(tmp_path / "vault")
        os.makedirs(vault)

        jsonl = os.path.join(vault, "test.jsonl")
        with open(jsonl, "w") as f:
            f.write(json.dumps(self._make_entry("a")) + "\n")
            f.write("\n")  # empty line
            f.write("   \n")  # whitespace line
            f.write(json.dumps(self._make_entry("b")) + "\n")

        groom_vault(vault)

        with open(jsonl) as f:
            lines = [json.loads(l) for l in f if l.strip()]
        assert len(lines) == 2

    def test_nonexistent_vault(self, tmp_path):
        """Should not crash on missing vault path."""
        groom_vault(str(tmp_path / "nonexistent"))

    def test_empty_vault(self, tmp_path):
        vault = str(tmp_path / "vault")
        os.makedirs(vault)
        groom_vault(vault)  # should not crash

    def test_processed_ids_updated_after_groom(self, tmp_path):
        """processed_ids.txt should reflect actual IDs on disk after grooming."""
        vault = str(tmp_path / "vault")
        os.makedirs(vault)

        jsonl = os.path.join(vault, "test.jsonl")
        entries = [self._make_entry("x"), self._make_entry("y"), self._make_entry("z")]
        with open(jsonl, "w") as f:
            for e in entries:
                f.write(json.dumps(e) + "\n")

        groom_vault(vault)

        with open(os.path.join(vault, "processed_ids.txt")) as f:
            ids = set(l.strip() for l in f if l.strip())
        assert ids == {"x", "y", "z"}

    def test_subdirectory_jsonl_files(self, tmp_path):
        """Groomer should process JSONL files in subdirectories (year folders)."""
        vault = str(tmp_path / "vault")
        year_dir = os.path.join(vault, "2024")
        os.makedirs(year_dir)

        jsonl = os.path.join(year_dir, "01_January.jsonl")
        entries = [
            self._make_entry("a", "Wed, 15 Jan 2024 12:00:00 +0000"),
            self._make_entry("b", "Mon, 01 Jan 2024 12:00:00 +0000"),
            self._make_entry("a", "Wed, 15 Jan 2024 12:00:00 +0000"),  # dup of a
        ]
        with open(jsonl, "w") as f:
            for e in entries:
                f.write(json.dumps(e) + "\n")

        groom_vault(vault)

        with open(jsonl) as f:
            lines = [json.loads(l) for l in f if l.strip()]
        assert len(lines) == 2
        assert lines[0]["id"] == "b"  # sorted first (Jan 1)
        assert lines[1]["id"] == "a"  # sorted second (Jan 15)

    def test_no_processed_ids_file(self, tmp_path):
        """Groomer should work even without processed_ids.txt."""
        vault = str(tmp_path / "vault")
        os.makedirs(vault)

        jsonl = os.path.join(vault, "test.jsonl")
        entries = [self._make_entry("a"), self._make_entry("b")]
        with open(jsonl, "w") as f:
            for e in entries:
                f.write(json.dumps(e) + "\n")

        groom_vault(vault)

        with open(jsonl) as f:
            lines = [json.loads(l) for l in f if l.strip()]
        assert len(lines) == 2

    def test_large_vault(self, tmp_path):
        """Test grooming with a larger dataset."""
        vault = str(tmp_path / "vault")
        os.makedirs(vault)

        jsonl = os.path.join(vault, "test.jsonl")
        entries = [
            self._make_entry(f"msg_{i:04d}", f"Mon, {(i % 28) + 1:02d} Jan 2024 {i % 24:02d}:00:00 +0000")
            for i in range(500)
        ]
        # Add 50 duplicates
        entries.extend(entries[:50])

        with open(jsonl, "w") as f:
            for e in entries:
                f.write(json.dumps(e) + "\n")

        groom_vault(vault)

        with open(jsonl) as f:
            lines = [json.loads(l) for l in f if l.strip()]
        assert len(lines) == 500
