"""Tests for the Shopping collector — Amazon order history CSV parser."""

import csv
import json
import os
import tempfile

import pytest

from collectors.shopping import (
    _normalize_columns,
    _get,
    _read_csv,
    _make_id,
    _safe_float,
    _safe_int,
    _parse_date,
    _parse_amazon_row,
    run_import_amazon,
)


# ═══════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════

class TestSafeFloat:
    def test_valid(self):
        assert _safe_float("29.99") == pytest.approx(29.99)

    def test_comma_decimal(self):
        assert _safe_float("29,99") == pytest.approx(29.99)

    def test_currency_symbols(self):
        assert _safe_float("$29.99") == pytest.approx(29.99)
        assert _safe_float("€29,99") == pytest.approx(29.99)

    def test_empty(self):
        assert _safe_float("") == 0.0

    def test_none(self):
        assert _safe_float(None) == 0.0


class TestSafeInt:
    def test_valid(self):
        assert _safe_int("3") == 3

    def test_float_string(self):
        assert _safe_int("3.7") == 3

    def test_invalid(self):
        assert _safe_int("abc") == 0

    def test_none(self):
        assert _safe_int(None) == 0


class TestParseDate:
    def test_us_format(self):
        assert _parse_date("01/15/2024") == "2024-01-15"

    def test_iso_format(self):
        assert _parse_date("2024-01-15") == "2024-01-15"

    def test_long_date(self):
        assert _parse_date("January 15, 2024") == "2024-01-15"

    def test_short_date(self):
        assert _parse_date("Jan 15, 2024") == "2024-01-15"

    def test_empty(self):
        assert _parse_date("") == ""


class TestMakeId:
    def test_deterministic(self):
        assert _make_id("order1", "asin1") == _make_id("order1", "asin1")

    def test_prefix(self):
        assert _make_id("x").startswith("shopping:amazon:")


# ═══════════════════════════════════════════════════════════════════════
# Amazon Parser
# ═══════════════════════════════════════════════════════════════════════

class TestParseAmazonRow:
    @pytest.fixture
    def col_map(self):
        headers = [
            "Order ID", "Order Date", "Title", "Category", "ASIN/ISBN",
            "Quantity", "Item Total", "Currency", "Seller", "Order Status",
        ]
        return _normalize_columns(headers)

    def test_full_row(self, col_map):
        row = [
            "111-222-333", "01/15/2024", "USB-C Cable", "Electronics", "B08XYZ123",
            "2", "9.99", "EUR", "Amazon.de", "Delivered",
        ]
        entry = _parse_amazon_row(row, col_map)
        assert entry is not None
        assert entry["title"] == "USB-C Cable"
        assert entry["order_id"] == "111-222-333"
        assert entry["quantity"] == 2
        assert entry["price"] == pytest.approx(9.99)
        assert entry["year"] == 2024

    def test_no_title(self, col_map):
        row = ["111-222-333", "01/15/2024", "", "Electronics", "B08XYZ123", "1", "10", "EUR", "", ""]
        assert _parse_amazon_row(row, col_map) is None

    def test_no_order_id_still_works(self, col_map):
        row = ["", "01/15/2024", "Some Item", "", "", "1", "5.00", "EUR", "", ""]
        entry = _parse_amazon_row(row, col_map)
        assert entry is not None
        assert entry["title"] == "Some Item"

    def test_default_status(self):
        headers = ["Title", "Order Date"]
        cm = _normalize_columns(headers)
        row = ["Item", "2024-01-15"]
        entry = _parse_amazon_row(row, cm)
        assert entry["status"] == "Delivered"

    def test_default_quantity(self, col_map):
        row = ["111", "01/15/2024", "Item", "", "", "", "10", "EUR", "", ""]
        entry = _parse_amazon_row(row, col_map)
        assert entry["quantity"] == 1


# ═══════════════════════════════════════════════════════════════════════
# End-to-End Import
# ═══════════════════════════════════════════════════════════════════════

class TestRunImportAmazon:
    def test_basic_import(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, "amazon.csv")
            vault_root = os.path.join(tmpdir, "vaults")

            with open(csv_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["Order ID", "Order Date", "Title", "Item Total", "Currency"])
                writer.writerow(["ORD-1", "01/15/2024", "Widget A", "19.99", "EUR"])
                writer.writerow(["ORD-2", "01/16/2024", "Widget B", "29.99", "EUR"])

            run_import_amazon(csv_path, config={"vault_root": vault_root})

            jsonl = os.path.join(vault_root, "Shopping", "shopping.jsonl")
            assert os.path.isfile(jsonl)
            with open(jsonl) as f:
                entries = [json.loads(line) for line in f if line.strip()]
            assert len(entries) == 2

    def test_deduplication(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, "amazon.csv")
            vault_root = os.path.join(tmpdir, "vaults")

            with open(csv_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["Order ID", "Title", "Order Date"])
                writer.writerow(["ORD-1", "Widget", "2024-01-15"])

            run_import_amazon(csv_path, config={"vault_root": vault_root})
            run_import_amazon(csv_path, config={"vault_root": vault_root})

            jsonl = os.path.join(vault_root, "Shopping", "shopping.jsonl")
            with open(jsonl) as f:
                entries = [json.loads(line) for line in f if line.strip()]
            assert len(entries) == 1

    def test_empty_rows_skipped(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, "amazon.csv")
            vault_root = os.path.join(tmpdir, "vaults")

            with open(csv_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["Title", "Order Date"])
                writer.writerow(["", "2024-01-15"])  # empty title => skipped
                writer.writerow(["Good Item", "2024-01-15"])

            run_import_amazon(csv_path, config={"vault_root": vault_root})

            jsonl = os.path.join(vault_root, "Shopping", "shopping.jsonl")
            with open(jsonl) as f:
                entries = [json.loads(line) for line in f if line.strip()]
            assert len(entries) == 1
