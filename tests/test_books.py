"""Tests for the Books collector — Goodreads and Audible CSV parsers."""

import csv
import hashlib
import json
import os
import tempfile

import pytest

from collectors.books import (
    _normalize_columns,
    _get,
    _read_csv,
    _make_id,
    _safe_int,
    _safe_float,
    _parse_shelves,
    _parse_goodreads_row,
    _parse_audible_row,
    run_import_goodreads,
    run_import_audible,
)


# ═══════════════════════════════════════════════════════════════════════
# CSV Helpers
# ═══════════════════════════════════════════════════════════════════════

class TestNormalizeColumns:
    def test_basic(self):
        header = ["Title", "Author", "ISBN"]
        result = _normalize_columns(header)
        assert result == {"title": 0, "author": 1, "isbn": 2}

    def test_whitespace_stripping(self):
        header = ["  Title  ", " Author"]
        result = _normalize_columns(header)
        assert "title" in result
        assert "author" in result

    def test_case_insensitive(self):
        header = ["TITLE", "Author", "iSbN"]
        result = _normalize_columns(header)
        assert "title" in result
        assert "isbn" in result

    def test_empty_header(self):
        result = _normalize_columns([])
        assert result == {}


class TestGet:
    def test_basic(self):
        row = ["The Great Gatsby", "F. Scott Fitzgerald"]
        col_map = {"title": 0, "author": 1}
        assert _get(row, col_map, "title") == "The Great Gatsby"

    def test_fallback_name(self):
        row = ["", "F. Scott Fitzgerald"]
        col_map = {"author": 1}
        assert _get(row, col_map, "writer", "author") == "F. Scott Fitzgerald"

    def test_default(self):
        row = [""]
        col_map = {"title": 0}
        assert _get(row, col_map, "title", default="N/A") == "N/A"

    def test_missing_column(self):
        row = ["Something"]
        col_map = {"title": 0}
        assert _get(row, col_map, "author") == ""

    def test_index_out_of_range(self):
        row = ["Only one"]
        col_map = {"title": 0, "author": 5}
        assert _get(row, col_map, "author") == ""


class TestReadCsv:
    def test_valid_csv(self):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False, newline=""
        ) as f:
            writer = csv.writer(f)
            writer.writerow(["Title", "Author"])
            writer.writerow(["Book1", "Author1"])
            writer.writerow(["Book2", "Author2"])
            f.flush()
            path = f.name

        try:
            col_map, rows = _read_csv(path)
            assert col_map is not None
            assert "title" in col_map
            assert len(rows) == 2
        finally:
            os.unlink(path)

    def test_empty_csv(self):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False, newline=""
        ) as f:
            path = f.name

        try:
            col_map, rows = _read_csv(path)
            assert col_map is None
            assert rows == []
        finally:
            os.unlink(path)

    def test_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            _read_csv("/nonexistent/file.csv")

    def test_bom_encoding(self):
        with tempfile.NamedTemporaryFile(
            mode="wb", suffix=".csv", delete=False
        ) as f:
            f.write(b"\xef\xbb\xbfTitle,Author\nBook1,Author1\n")
            path = f.name

        try:
            col_map, rows = _read_csv(path)
            assert col_map is not None
            assert "title" in col_map
            assert len(rows) == 1
        finally:
            os.unlink(path)


# ═══════════════════════════════════════════════════════════════════════
# ID and Value Helpers
# ═══════════════════════════════════════════════════════════════════════

class TestMakeId:
    def test_deterministic(self):
        id1 = _make_id("goodreads", "123", "Author")
        id2 = _make_id("goodreads", "123", "Author")
        assert id1 == id2

    def test_prefix(self):
        result = _make_id("goodreads", "123")
        assert result.startswith("books:goodreads:")

    def test_different_inputs(self):
        id1 = _make_id("goodreads", "123")
        id2 = _make_id("goodreads", "456")
        assert id1 != id2


class TestSafeInt:
    def test_valid(self):
        assert _safe_int("42") == 42

    def test_float_string(self):
        assert _safe_int("3.7") == 3

    def test_invalid(self):
        assert _safe_int("abc") == 0

    def test_empty(self):
        assert _safe_int("") == 0

    def test_none(self):
        assert _safe_int(None) == 0

    def test_custom_default(self):
        assert _safe_int("abc", default=-1) == -1


class TestSafeFloat:
    def test_valid(self):
        assert _safe_float("3.14") == pytest.approx(3.14)

    def test_invalid(self):
        assert _safe_float("abc") == 0.0

    def test_none(self):
        assert _safe_float(None) == 0.0


class TestParseShelves:
    def test_basic(self):
        assert _parse_shelves("sci-fi, fantasy, read") == ["sci-fi", "fantasy", "read"]

    def test_empty(self):
        assert _parse_shelves("") == []

    def test_whitespace_only(self):
        assert _parse_shelves("  ,  , ") == []

    def test_single(self):
        assert _parse_shelves("read") == ["read"]


# ═══════════════════════════════════════════════════════════════════════
# Goodreads Parser
# ═══════════════════════════════════════════════════════════════════════

class TestParseGoodreadsRow:
    @pytest.fixture
    def col_map(self):
        headers = [
            "Book Id", "Title", "Author", "Author l-f", "Additional Authors",
            "ISBN", "ISBN13", "My Rating", "Average Rating", "Publisher",
            "Binding", "Number of Pages", "Original Publication Year",
            "Year Published", "Date Read", "Date Added", "Bookshelves",
            "Exclusive Shelf", "My Review",
        ]
        return _normalize_columns(headers)

    def test_full_row(self, col_map):
        row = [
            "12345", "Dune", "Frank Herbert", "Herbert, Frank", "",
            "0441172717", "9780441172719", "5", "4.25", "Ace",
            "Paperback", "412", "1965",
            "1965", "2024/01/15", "2023/12/01", "sci-fi, classics",
            "read", "Amazing book!",
        ]
        entry = _parse_goodreads_row(row, col_map)
        assert entry is not None
        assert entry["title"] == "Dune"
        assert entry["author"] == "Frank Herbert"
        assert entry["my_rating"] == 5
        assert entry["status"] == "read"
        assert entry["pages"] == 412
        assert entry["id"].startswith("books:goodreads:")
        assert "sci-fi" in entry["shelves"]

    def test_no_title(self, col_map):
        row = ["12345", "", "Author", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""]
        entry = _parse_goodreads_row(row, col_map)
        assert entry is None

    def test_currently_reading(self, col_map):
        row = [
            "99", "Some Book", "Some Author", "", "", "", "", "0", "3.5", "",
            "", "200", "2020", "2020", "", "2024/01/01", "",
            "currently-reading", "",
        ]
        entry = _parse_goodreads_row(row, col_map)
        assert entry is not None
        assert entry["status"] == "currently-reading"

    def test_isbn_cleanup(self, col_map):
        row = [
            "1", "Test", "Author", "", "", "=\"0441172717\"", "=\"9780441172719\"",
            "0", "0", "", "", "0", "0", "0", "", "", "", "", "",
        ]
        entry = _parse_goodreads_row(row, col_map)
        assert entry["isbn"] == "0441172717"
        assert entry["isbn13"] == "9780441172719"


# ═══════════════════════════════════════════════════════════════════════
# Audible Parser
# ═══════════════════════════════════════════════════════════════════════

class TestParseAudibleRow:
    @pytest.fixture
    def col_map(self):
        headers = ["Title", "Author", "Narrator", "Duration", "Date Added", "Rating"]
        return _normalize_columns(headers)

    def test_full_row(self, col_map):
        row = ["Project Hail Mary", "Andy Weir", "Ray Porter", "16 hrs 10 mins", "2024-01-15", "5"]
        entry = _parse_audible_row(row, col_map)
        assert entry is not None
        assert entry["title"] == "Project Hail Mary"
        assert entry["narrator"] == "Ray Porter"
        assert entry["format"] == "audiobook"
        assert entry["id"].startswith("books:audible:")

    def test_no_title(self, col_map):
        row = ["", "Andy Weir", "Ray Porter", "16 hrs", "", ""]
        entry = _parse_audible_row(row, col_map)
        assert entry is None

    def test_purchase_date_fallback(self):
        headers = ["Title", "Author", "Narrator", "Duration", "Date Purchased"]
        col_map = _normalize_columns(headers)
        row = ["Book", "Author", "Narrator", "5 hrs", "2024-03-01"]
        entry = _parse_audible_row(row, col_map)
        assert entry is not None
        assert entry["date_added"] == "2024-03-01"


# ═══════════════════════════════════════════════════════════════════════
# End-to-End Import Tests
# ═══════════════════════════════════════════════════════════════════════

class TestRunImportGoodreads:
    def test_basic_import(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, "goodreads_export.csv")
            vault_root = os.path.join(tmpdir, "vaults")

            with open(csv_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["Book Id", "Title", "Author", "My Rating", "Exclusive Shelf"])
                writer.writerow(["1", "Dune", "Frank Herbert", "5", "read"])
                writer.writerow(["2", "1984", "George Orwell", "4", "read"])

            run_import_goodreads(csv_path, config={"vault_root": vault_root})

            vault_path = os.path.join(vault_root, "Books")
            jsonl_path = os.path.join(vault_path, "books.jsonl")
            assert os.path.isfile(jsonl_path)

            with open(jsonl_path) as f:
                entries = [json.loads(line) for line in f if line.strip()]
            assert len(entries) == 2

    def test_deduplication(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, "goodreads.csv")
            vault_root = os.path.join(tmpdir, "vaults")

            with open(csv_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["Book Id", "Title", "Author", "My Rating", "Exclusive Shelf"])
                writer.writerow(["1", "Dune", "Frank Herbert", "5", "read"])

            run_import_goodreads(csv_path, config={"vault_root": vault_root})
            run_import_goodreads(csv_path, config={"vault_root": vault_root})

            vault_path = os.path.join(vault_root, "Books")
            jsonl_path = os.path.join(vault_path, "books.jsonl")
            with open(jsonl_path) as f:
                entries = [json.loads(line) for line in f if line.strip()]
            assert len(entries) == 1


class TestRunImportAudible:
    def test_basic_import(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, "audible.csv")
            vault_root = os.path.join(tmpdir, "vaults")

            with open(csv_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["Title", "Author", "Narrator", "Duration", "Rating"])
                writer.writerow(["Project Hail Mary", "Andy Weir", "Ray Porter", "16 hrs", "5"])

            run_import_audible(csv_path, config={"vault_root": vault_root})

            vault_path = os.path.join(vault_root, "Books")
            jsonl_path = os.path.join(vault_path, "books.jsonl")
            assert os.path.isfile(jsonl_path)

            with open(jsonl_path) as f:
                entries = [json.loads(line) for line in f if line.strip()]
            assert len(entries) == 1
            assert entries[0]["format"] == "audiobook"
