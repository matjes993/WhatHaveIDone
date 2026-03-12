"""Tests for the Finance collector — PayPal and generic bank CSV parsers."""

import csv
import json
import os
import tempfile

import pytest

from collectors.finance import (
    _normalize_columns,
    _get,
    _read_csv,
    _make_id,
    _safe_float,
    _parse_date,
    _parse_paypal_row,
    _parse_bank_row,
    run_import_paypal,
    run_import_bank,
)


# ═══════════════════════════════════════════════════════════════════════
# Value Helpers
# ═══════════════════════════════════════════════════════════════════════

class TestSafeFloat:
    def test_valid(self):
        assert _safe_float("3.14") == pytest.approx(3.14)

    def test_comma_decimal(self):
        assert _safe_float("3,14") == pytest.approx(3.14)

    def test_currency_symbols(self):
        assert _safe_float("$29.99") == pytest.approx(29.99)
        assert _safe_float("29,99€") == pytest.approx(29.99)
        assert _safe_float("£15.00") == pytest.approx(15.00)

    def test_empty(self):
        assert _safe_float("") == 0.0

    def test_none(self):
        assert _safe_float(None) == 0.0

    def test_garbage(self):
        assert _safe_float("abc") == 0.0


class TestParseDate:
    def test_us_format(self):
        assert _parse_date("01/15/2024") == "2024-01-15"

    def test_iso_format(self):
        assert _parse_date("2024-01-15") == "2024-01-15"

    def test_german_format(self):
        assert _parse_date("15.01.2024") == "2024-01-15"

    def test_empty(self):
        assert _parse_date("") == ""

    def test_garbage_passthrough(self):
        result = _parse_date("not-a-date")
        assert result == "not-a-date"


class TestMakeId:
    def test_deterministic(self):
        assert _make_id("paypal", "txn123") == _make_id("paypal", "txn123")

    def test_prefix(self):
        assert _make_id("paypal", "x").startswith("finance:paypal:")
        assert _make_id("bank", "x").startswith("finance:bank:")

    def test_different(self):
        assert _make_id("paypal", "a") != _make_id("paypal", "b")


# ═══════════════════════════════════════════════════════════════════════
# PayPal Parser
# ═══════════════════════════════════════════════════════════════════════

class TestParsePaypalRow:
    @pytest.fixture
    def col_map(self):
        headers = [
            "Date", "Time", "Name", "Type", "Status", "Currency",
            "Gross", "Fee", "Net", "From Email Address", "To Email Address",
            "Transaction ID", "Item Title", "Balance",
        ]
        return _normalize_columns(headers)

    def test_full_row(self, col_map):
        row = [
            "01/15/2024", "10:30:00", "John Doe", "Payment", "Completed", "EUR",
            "-29.99", "-0.50", "-30.49", "", "john@example.com",
            "TXN123456", "Widget purchase", "1000.00",
        ]
        entry = _parse_paypal_row(row, col_map)
        assert entry is not None
        assert entry["date"] == "2024-01-15"
        assert entry["counterparty"] == "John Doe"
        assert entry["amount"] == pytest.approx(-29.99)
        assert entry["transaction_id"] == "TXN123456"
        assert entry["year"] == 2024
        assert entry["month"] == 1

    def test_no_date(self, col_map):
        row = ["", "10:30", "X", "Payment", "Completed", "EUR", "10", "0", "10", "", "", "", "", ""]
        assert _parse_paypal_row(row, col_map) is None

    def test_type_lowercase(self, col_map):
        row = [
            "01/15/2024", "", "Test", "Express Checkout", "Completed", "USD",
            "50", "0", "50", "", "", "TXN1", "", "100",
        ]
        entry = _parse_paypal_row(row, col_map)
        assert entry["type"] == "express checkout"


# ═══════════════════════════════════════════════════════════════════════
# Bank Parser
# ═══════════════════════════════════════════════════════════════════════

class TestParseBankRow:
    @pytest.fixture
    def col_map(self):
        headers = ["Date", "Description", "Amount", "Balance", "Currency", "Category", "Payee"]
        return _normalize_columns(headers)

    def test_full_row(self, col_map):
        row = ["2024-01-15", "Grocery Store", "-45.50", "954.50", "EUR", "Food", "REWE"]
        entry = _parse_bank_row(row, col_map, bank_name="deutschebank")
        assert entry is not None
        assert entry["date"] == "2024-01-15"
        assert entry["amount"] == pytest.approx(-45.50)
        assert entry["counterparty"] == "REWE"
        assert "bank-deutschebank" in entry["sources"]

    def test_no_date(self, col_map):
        row = ["", "Grocery", "-10", "100", "EUR", "", ""]
        assert _parse_bank_row(row, col_map) is None

    def test_no_payee_uses_description(self, col_map):
        headers = ["Date", "Description", "Amount", "Balance", "Currency"]
        cm = _normalize_columns(headers)
        row = ["2024-01-15", "ATM Withdrawal", "-50", "950", "EUR"]
        entry = _parse_bank_row(row, cm)
        assert entry["counterparty"] == "ATM Withdrawal"

    def test_german_columns(self):
        headers = ["Buchungstag", "Verwendungszweck", "Betrag", "Saldo", "Währung"]
        cm = _normalize_columns(headers)
        row = ["15.01.2024", "Miete Januar", "-800,00", "1200,00", "EUR"]
        entry = _parse_bank_row(row, cm)
        assert entry is not None
        assert entry["amount"] == pytest.approx(-800.0)


# ═══════════════════════════════════════════════════════════════════════
# End-to-End Import Tests
# ═══════════════════════════════════════════════════════════════════════

class TestRunImportPaypal:
    def test_basic_import(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, "paypal.csv")
            vault_root = os.path.join(tmpdir, "vaults")

            with open(csv_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["Date", "Name", "Gross", "Currency", "Transaction ID", "Status"])
                writer.writerow(["01/15/2024", "Shop A", "-25.00", "EUR", "TXN1", "Completed"])
                writer.writerow(["01/16/2024", "Shop B", "-30.00", "EUR", "TXN2", "Completed"])

            run_import_paypal(csv_path, config={"vault_root": vault_root})

            jsonl = os.path.join(vault_root, "Finance", "finance.jsonl")
            assert os.path.isfile(jsonl)
            with open(jsonl) as f:
                entries = [json.loads(line) for line in f if line.strip()]
            assert len(entries) == 2

    def test_deduplication(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, "paypal.csv")
            vault_root = os.path.join(tmpdir, "vaults")

            with open(csv_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["Date", "Name", "Gross", "Currency", "Transaction ID"])
                writer.writerow(["01/15/2024", "Shop", "-25.00", "EUR", "TXN1"])

            run_import_paypal(csv_path, config={"vault_root": vault_root})
            run_import_paypal(csv_path, config={"vault_root": vault_root})

            jsonl = os.path.join(vault_root, "Finance", "finance.jsonl")
            with open(jsonl) as f:
                entries = [json.loads(line) for line in f if line.strip()]
            assert len(entries) == 1


class TestRunImportBank:
    def test_basic_import(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, "bank.csv")
            vault_root = os.path.join(tmpdir, "vaults")

            with open(csv_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["Date", "Description", "Amount", "Currency"])
                writer.writerow(["2024-01-15", "Groceries", "-45.50", "EUR"])

            run_import_bank(csv_path, config={"vault_root": vault_root}, bank_name="testbank")

            jsonl = os.path.join(vault_root, "Finance", "finance.jsonl")
            assert os.path.isfile(jsonl)
            with open(jsonl) as f:
                entries = [json.loads(line) for line in f if line.strip()]
            assert len(entries) == 1
            assert "bank-testbank" in entries[0]["sources"]
