"""Tests for contact collectors — Google, LinkedIn, Facebook, Instagram.

These tests use mocks, temp files, and fixtures. No real API calls.
"""

import csv
import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from collectors.google_contacts import _contact_to_entry, run_export
from collectors.linkedin_contacts import (
    _normalize_columns,
    _get,
    _make_id as linkedin_make_id,
    _parse_connected_on,
    _row_to_entry,
    _read_csv,
    run_import as linkedin_run_import,
)
from collectors.facebook_contacts import (
    _decode_fb_name,
    _make_id as fb_make_id,
    _ts_to_iso,
    _parse_friends,
    _parse_address_book,
    _find_files,
    run_import as fb_run_import,
)
from collectors.instagram_contacts import (
    _decode_facebook_text,
    _parse_old_format,
    _parse_new_format,
    _find_export_files,
    _build_entry,
    run_import as ig_run_import,
)


# ═══════════════════════════════════════════════════════════════════════
# Google Contacts
# ═══════════════════════════════════════════════════════════════════════

class TestGoogleContactToEntry:
    """Test _contact_to_entry with various person resource shapes."""

    def test_full_data(self):
        person = {
            "resourceName": "people/c123456",
            "names": [
                {
                    "displayName": "John Doe",
                    "givenName": "John",
                    "familyName": "Doe",
                    "middleName": "M",
                    "honorificPrefix": "Dr",
                }
            ],
            "emailAddresses": [
                {"value": "john@example.com"},
                {"value": "john.doe@work.com"},
            ],
            "phoneNumbers": [
                {"value": "+1-555-0100", "type": "mobile"},
            ],
            "organizations": [
                {"name": "Acme Corp", "title": "Engineer", "department": "R&D"},
            ],
            "addresses": [
                {"formattedValue": "123 Main St, NYC", "city": "New York", "type": "home"},
            ],
            "birthdays": [
                {"date": {"year": 1990, "month": 6, "day": 15}},
            ],
            "urls": [
                {"value": "https://johndoe.com", "type": "homepage"},
            ],
            "biographies": [
                {"value": "A great engineer."},
            ],
            "relations": [
                {"person": "Jane Doe", "type": "spouse"},
            ],
            "metadata": {
                "sources": [{"updateTime": "2024-01-15T10:00:00Z", "type": "CONTACT"}],
            },
        }
        entry = _contact_to_entry(person)

        assert entry["id"] == "contacts:google:people/c123456"
        assert entry["source"] == "google"
        assert entry["source_id"] == "people/c123456"
        assert entry["name"]["display"] == "John Doe"
        assert entry["name"]["given"] == "John"
        assert entry["name"]["family"] == "Doe"
        assert entry["name"]["middle"] == "M"
        assert entry["name"]["prefix"] == "Dr"
        assert len(entry["emails"]) == 2
        assert entry["emails"][0]["value"] == "john@example.com"
        assert len(entry["phones"]) == 1
        assert entry["phones"][0]["value"] == "+1-555-0100"
        assert entry["phones"][0]["type"] == "mobile"
        assert len(entry["organizations"]) == 1
        assert entry["organizations"][0]["name"] == "Acme Corp"
        assert entry["organizations"][0]["title"] == "Engineer"
        assert entry["organizations"][0]["department"] == "R&D"
        assert len(entry["addresses"]) == 1
        assert entry["addresses"][0]["city"] == "New York"
        assert len(entry["birthdays"]) == 1
        assert entry["birthdays"][0]["year"] == 1990
        assert entry["birthdays"][0]["month"] == 6
        assert len(entry["urls"]) == 1
        assert entry["urls"][0]["value"] == "https://johndoe.com"
        assert len(entry["biographies"]) == 1
        assert "great engineer" in entry["biographies"][0]["value"]
        assert len(entry["relations"]) == 1
        assert entry["relations"][0]["person"] == "Jane Doe"
        assert entry["relations"][0]["type"] == "spouse"
        assert entry["updated_at"] == "2024-01-15T10:00:00Z"
        assert "John Doe" in entry["contact_for_embedding"]
        assert "Acme Corp" in entry["contact_for_embedding"]

    def test_minimal_data(self):
        person = {
            "resourceName": "people/c999",
            "names": [{"displayName": "Jane"}],
        }
        entry = _contact_to_entry(person)

        assert entry["id"] == "contacts:google:people/c999"
        assert entry["name"]["display"] == "Jane"
        assert entry["name"]["given"] == ""
        assert entry["name"]["family"] == ""
        assert entry["emails"] == []
        assert entry["phones"] == []
        assert entry["organizations"] == []
        assert entry["addresses"] == []
        assert entry["updated_at"] == ""

    def test_empty_person(self):
        person = {}
        entry = _contact_to_entry(person)

        assert entry["id"] == "contacts:google:"
        assert entry["name"]["display"] == ""
        assert entry["emails"] == []
        assert entry["phones"] == []

    def test_empty_names_list(self):
        person = {"resourceName": "people/c0", "names": []}
        entry = _contact_to_entry(person)
        assert entry["name"]["display"] == ""

    def test_email_with_empty_value_filtered(self):
        person = {
            "resourceName": "people/c1",
            "emailAddresses": [
                {"value": "real@test.com"},
                {"value": ""},
                {},
            ],
        }
        entry = _contact_to_entry(person)
        assert len(entry["emails"]) == 1
        assert entry["emails"][0]["value"] == "real@test.com"

    def test_phone_with_empty_value_filtered(self):
        person = {
            "resourceName": "people/c2",
            "phoneNumbers": [
                {"value": ""},
                {"value": "+1-555-0101"},
                {},
            ],
        }
        entry = _contact_to_entry(person)
        assert len(entry["phones"]) == 1
        assert entry["phones"][0]["value"] == "+1-555-0101"

    def test_multiple_organizations_all_captured(self):
        person = {
            "resourceName": "people/c3",
            "organizations": [
                {"name": "First Co", "title": "CEO"},
                {"name": "Second Co", "title": "Intern"},
            ],
        }
        entry = _contact_to_entry(person)
        assert len(entry["organizations"]) == 2
        assert entry["organizations"][0]["name"] == "First Co"
        assert entry["organizations"][1]["name"] == "Second Co"

    def test_metadata_no_sources(self):
        person = {
            "resourceName": "people/c4",
            "metadata": {"sources": []},
        }
        entry = _contact_to_entry(person)
        assert entry["updated_at"] == ""

    def test_nicknames(self):
        person = {
            "resourceName": "people/c5",
            "nicknames": [{"value": "Johnny", "type": "DEFAULT"}],
        }
        entry = _contact_to_entry(person)
        assert len(entry["nicknames"]) == 1
        assert entry["nicknames"][0]["value"] == "Johnny"

    def test_im_clients(self):
        person = {
            "resourceName": "people/c6",
            "imClients": [
                {"username": "johndoe", "protocol": "whatsapp", "type": "home"},
            ],
        }
        entry = _contact_to_entry(person)
        assert len(entry["im_clients"]) == 1
        assert entry["im_clients"][0]["username"] == "johndoe"
        assert entry["im_clients"][0]["protocol"] == "whatsapp"

    def test_external_ids(self):
        person = {
            "resourceName": "people/c7",
            "externalIds": [
                {"value": "@johndoe", "type": "account", "formattedType": "Twitter"},
            ],
        }
        entry = _contact_to_entry(person)
        assert len(entry["external_ids"]) == 1
        assert entry["external_ids"][0]["value"] == "@johndoe"

    def test_interests_and_skills(self):
        person = {
            "resourceName": "people/c8",
            "interests": [{"value": "hiking"}, {"value": "cooking"}],
            "skills": [{"value": "Python"}, {"value": "Go"}],
        }
        entry = _contact_to_entry(person)
        assert entry["interests"] == ["hiking", "cooking"]
        assert entry["skills"] == ["Python", "Go"]

    def test_memberships(self):
        person = {
            "resourceName": "people/c9",
            "memberships": [
                {"contactGroupMembership": {
                    "contactGroupId": "friends",
                    "contactGroupResourceName": "contactGroups/friends",
                }},
            ],
        }
        entry = _contact_to_entry(person)
        assert len(entry["memberships"]) == 1
        assert entry["memberships"][0]["group_id"] == "friends"

    def test_user_defined_fields(self):
        person = {
            "resourceName": "people/c10",
            "userDefined": [
                {"key": "Favorite Color", "value": "Blue"},
            ],
        }
        entry = _contact_to_entry(person)
        assert len(entry["user_defined"]) == 1
        assert entry["user_defined"][0]["key"] == "Favorite Color"

    def test_events(self):
        person = {
            "resourceName": "people/c11",
            "events": [
                {"type": "anniversary", "date": {"year": 2020, "month": 3, "day": 14}},
            ],
        }
        entry = _contact_to_entry(person)
        assert len(entry["events"]) == 1
        assert entry["events"][0]["date"] == "2020-03-14"

    def test_photos(self):
        person = {
            "resourceName": "people/c12",
            "photos": [
                {"url": "https://lh3.google.com/photo123", "default": False},
            ],
        }
        entry = _contact_to_entry(person)
        assert len(entry["photos"]) == 1
        assert "photo123" in entry["photos"][0]["url"]

    def test_contact_for_embedding(self):
        person = {
            "resourceName": "people/c13",
            "names": [{"displayName": "Alice Smith"}],
            "organizations": [{"name": "BigCorp", "title": "VP Sales"}],
            "emailAddresses": [{"value": "alice@bigcorp.com"}],
            "biographies": [{"value": "Experienced sales leader in tech."}],
        }
        entry = _contact_to_entry(person)
        embed = entry["contact_for_embedding"]
        assert "Alice Smith" in embed
        assert "VP Sales at BigCorp" in embed
        assert "alice@bigcorp.com" in embed
        assert "Experienced sales leader" in embed


class TestGoogleRunExport:
    """Test run_export with mocked API service."""

    def _build_mock_service(self, pages):
        """Build a mock People API service returning the given pages.

        pages: list of dicts, each with "connections" and optionally "nextPageToken".
        """
        service = MagicMock()
        mock_list = MagicMock()

        side_effects = []
        for page in pages:
            mock_execute = MagicMock(return_value=page)
            mock_req = MagicMock()
            mock_req.execute = mock_execute
            side_effects.append(mock_req)

        mock_list.side_effect = side_effects
        service.people.return_value.connections.return_value.list = mock_list

        # Mock contactGroups().list()
        mock_groups_execute = MagicMock(return_value={"contactGroups": []})
        mock_groups_req = MagicMock()
        mock_groups_req.execute = mock_groups_execute
        service.contactGroups.return_value.list.return_value = mock_groups_req

        return service

    @patch("collectors.google_contacts.get_credentials")
    @patch("collectors.google_contacts.build")
    def test_exports_contacts_single_page(self, mock_build, mock_creds, tmp_path):
        mock_creds.return_value = MagicMock()
        service = self._build_mock_service([
            {
                "connections": [
                    {
                        "resourceName": "people/c100",
                        "names": [{"displayName": "Alice"}],
                    },
                    {
                        "resourceName": "people/c200",
                        "names": [{"displayName": "Bob"}],
                    },
                ],
            },
        ])
        mock_build.return_value = service

        config = {"vault_root": str(tmp_path), "contacts": {}}
        run_export(config)

        vault_dir = tmp_path / "Contacts_Google"
        assert vault_dir.exists()

        jsonl = vault_dir / "contacts.jsonl"
        assert jsonl.exists()
        lines = jsonl.read_text().strip().split("\n")
        assert len(lines) == 2

        first = json.loads(lines[0])
        assert first["name"]["display"] == "Alice"
        second = json.loads(lines[1])
        assert second["name"]["display"] == "Bob"

    @patch("collectors.google_contacts.get_credentials")
    @patch("collectors.google_contacts.build")
    def test_pagination(self, mock_build, mock_creds, tmp_path):
        mock_creds.return_value = MagicMock()
        service = self._build_mock_service([
            {
                "connections": [
                    {"resourceName": "people/c1", "names": [{"displayName": "P1"}]},
                ],
                "nextPageToken": "token_page2",
            },
            {
                "connections": [
                    {"resourceName": "people/c2", "names": [{"displayName": "P2"}]},
                ],
            },
        ])
        mock_build.return_value = service

        config = {"vault_root": str(tmp_path), "contacts": {}}
        run_export(config)

        jsonl = tmp_path / "Contacts_Google" / "contacts.jsonl"
        lines = jsonl.read_text().strip().split("\n")
        assert len(lines) == 2

    @patch("collectors.google_contacts.get_credentials")
    @patch("collectors.google_contacts.build")
    def test_skips_already_processed(self, mock_build, mock_creds, tmp_path):
        mock_creds.return_value = MagicMock()
        service = self._build_mock_service([
            {
                "connections": [
                    {"resourceName": "people/c1", "names": [{"displayName": "Old"}]},
                    {"resourceName": "people/c2", "names": [{"displayName": "New"}]},
                ],
            },
        ])
        mock_build.return_value = service

        # Pre-populate processed IDs
        vault_dir = tmp_path / "Contacts_Google"
        vault_dir.mkdir(parents=True)
        (vault_dir / "processed_ids.txt").write_text("contacts:google:people/c1\n")

        config = {"vault_root": str(tmp_path), "contacts": {}}
        run_export(config)

        jsonl = vault_dir / "contacts.jsonl"
        lines = jsonl.read_text().strip().split("\n")
        assert len(lines) == 1
        assert json.loads(lines[0])["name"]["display"] == "New"


# ═══════════════════════════════════════════════════════════════════════
# LinkedIn Contacts
# ═══════════════════════════════════════════════════════════════════════

class TestLinkedInNormalizeColumns:
    def test_basic_mapping(self):
        header = ["First Name", "Last Name", "Email Address"]
        result = _normalize_columns(header)
        assert result["first name"] == 0
        assert result["last name"] == 1
        assert result["email address"] == 2

    def test_strips_whitespace(self):
        header = ["  First Name  ", " Last Name"]
        result = _normalize_columns(header)
        assert result["first name"] == 0
        assert result["last name"] == 1

    def test_case_insensitive(self):
        header = ["FIRST NAME", "last name", "Email Address"]
        result = _normalize_columns(header)
        assert "first name" in result
        assert "last name" in result
        assert "email address" in result


class TestLinkedInGet:
    def test_returns_first_match(self):
        row = ["Alice", "Smith", "alice@test.com"]
        col_map = {"first name": 0, "last name": 1, "email": 2}
        assert _get(row, col_map, "first name") == "Alice"

    def test_returns_default_when_not_found(self):
        row = ["Alice"]
        col_map = {"first name": 0}
        assert _get(row, col_map, "email", default="N/A") == "N/A"

    def test_tries_multiple_names(self):
        row = ["", "alice@test.com"]
        col_map = {"email_address": 1}
        assert _get(row, col_map, "email", "email_address") == "alice@test.com"

    def test_skips_empty_values(self):
        row = ["", "alice@test.com"]
        col_map = {"email": 0, "email_address": 1}
        result = _get(row, col_map, "email", "email_address")
        assert result == "alice@test.com"

    def test_index_out_of_range(self):
        row = ["Alice"]
        col_map = {"email": 5}
        assert _get(row, col_map, "email", default="none") == "none"


class TestLinkedInMakeId:
    def test_deterministic(self):
        id1 = linkedin_make_id("John", "Doe", "john@test.com")
        id2 = linkedin_make_id("John", "Doe", "john@test.com")
        assert id1 == id2

    def test_prefix(self):
        result = linkedin_make_id("A", "B", "c@d.com")
        assert result.startswith("contacts:linkedin:")

    def test_different_inputs_different_ids(self):
        id1 = linkedin_make_id("John", "Doe", "john@test.com")
        id2 = linkedin_make_id("Jane", "Doe", "jane@test.com")
        assert id1 != id2


class TestLinkedInParseConnectedOn:
    def test_format_dd_mmm_yyyy(self):
        assert _parse_connected_on("15 Jan 2024") == "2024-01-15"

    def test_format_iso(self):
        assert _parse_connected_on("2024-01-15") == "2024-01-15"

    def test_format_us_slash(self):
        assert _parse_connected_on("01/15/2024") == "2024-01-15"

    def test_format_mmm_dd_comma_yyyy(self):
        assert _parse_connected_on("Jan 15, 2024") == "2024-01-15"

    def test_empty_string(self):
        assert _parse_connected_on("") == ""

    def test_whitespace_only(self):
        assert _parse_connected_on("   ") == ""

    def test_unrecognized_format_returns_raw(self):
        result = _parse_connected_on("15-01-2024")
        assert result == "15-01-2024"


class TestLinkedInRowToEntry:
    def _make_col_map(self):
        return _normalize_columns([
            "First Name", "Last Name", "Email Address",
            "Company", "Position", "Connected On", "URL",
        ])

    def test_full_row(self):
        col_map = self._make_col_map()
        row = ["John", "Doe", "john@test.com", "Acme", "CTO", "15 Jan 2024", "https://linkedin.com/in/jdoe"]
        entry = _row_to_entry(row, col_map, "2024-06-01T00:00:00")

        assert entry["source"] == "linkedin"
        assert entry["name"]["display"] == "John Doe"
        assert entry["name"]["given"] == "John"
        assert entry["name"]["family"] == "Doe"
        assert entry["emails"] == ["john@test.com"]
        assert entry["organization"] == "Acme"
        assert entry["title"] == "CTO"
        assert entry["connected_on"] == "2024-01-15"
        assert entry["source_id"] == "https://linkedin.com/in/jdoe"
        assert entry["updated_at"] == "2024-06-01T00:00:00"

    def test_minimal_row(self):
        col_map = self._make_col_map()
        row = ["Jane", "", "", "", "", "", ""]
        entry = _row_to_entry(row, col_map, "2024-01-01T00:00:00")

        assert entry is not None
        assert entry["name"]["display"] == "Jane"
        assert entry["emails"] == []

    def test_empty_row_returns_none(self):
        col_map = self._make_col_map()
        row = ["", "", "", "", "", "", ""]
        assert _row_to_entry(row, col_map, "2024-01-01T00:00:00") is None


class TestLinkedInReadCsv:
    def test_utf8_csv(self, tmp_path):
        csv_file = tmp_path / "connections.csv"
        csv_file.write_text(
            "First Name,Last Name,Email Address\nAlice,Wonder,alice@test.com\n",
            encoding="utf-8",
        )
        col_map, rows = _read_csv(str(csv_file))
        assert col_map is not None
        assert len(rows) == 1
        assert rows[0][0] == "Alice"

    def test_bom_csv(self, tmp_path):
        csv_file = tmp_path / "connections.csv"
        csv_file.write_bytes(
            b"\xef\xbb\xbfFirst Name,Last Name\nBob,Builder\n"
        )
        col_map, rows = _read_csv(str(csv_file))
        assert "first name" in col_map
        assert len(rows) == 1

    def test_latin1_csv(self, tmp_path):
        csv_file = tmp_path / "connections.csv"
        csv_file.write_bytes(
            "First Name,Last Name\nRen\xe9,Fran\xe7ois\n".encode("latin-1")
        )
        col_map, rows = _read_csv(str(csv_file))
        assert col_map is not None
        assert len(rows) == 1

    def test_empty_csv(self, tmp_path):
        csv_file = tmp_path / "connections.csv"
        csv_file.write_text("", encoding="utf-8")
        col_map, rows = _read_csv(str(csv_file))
        assert col_map is None
        assert rows == []

    def test_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            _read_csv(str(tmp_path / "nope.csv"))


class TestLinkedInRunImport:
    def test_end_to_end(self, tmp_path):
        csv_file = tmp_path / "connections.csv"
        csv_file.write_text(
            "First Name,Last Name,Email Address,Company,Position,Connected On\n"
            "Alice,Wonder,alice@test.com,ACME,Dev,15 Jan 2024\n"
            "Bob,Builder,bob@test.com,BuildCo,PM,2024-02-20\n",
            encoding="utf-8",
        )
        vault_dir = tmp_path / "vaults"
        config = {"vault_root": str(vault_dir)}
        linkedin_run_import(str(csv_file), config)

        vault_path = vault_dir / "Contacts_LinkedIn"
        jsonl = vault_path / "contacts.jsonl"
        assert jsonl.exists()

        lines = jsonl.read_text().strip().split("\n")
        assert len(lines) == 2

        entries = [json.loads(line) for line in lines]
        names = {e["name"]["display"] for e in entries}
        assert "Alice Wonder" in names
        assert "Bob Builder" in names

    def test_skips_duplicates_on_reimport(self, tmp_path):
        csv_file = tmp_path / "connections.csv"
        csv_file.write_text(
            "First Name,Last Name,Email Address\n"
            "Alice,Wonder,alice@test.com\n",
            encoding="utf-8",
        )
        vault_dir = tmp_path / "vaults"
        config = {"vault_root": str(vault_dir)}

        linkedin_run_import(str(csv_file), config)
        linkedin_run_import(str(csv_file), config)

        jsonl = vault_dir / "Contacts_LinkedIn" / "contacts.jsonl"
        lines = jsonl.read_text().strip().split("\n")
        assert len(lines) == 1


# ═══════════════════════════════════════════════════════════════════════
# Facebook Contacts
# ═══════════════════════════════════════════════════════════════════════

class TestFacebookDecodeName:
    def test_ascii_passthrough(self):
        assert _decode_fb_name("John Doe") == "John Doe"

    def test_mojibake_decode(self):
        # "Ren\u00c3\u00a9" is how Facebook stores "René"
        encoded = "Ren\u00c3\u00a9"
        assert _decode_fb_name(encoded) == "René"

    def test_already_correct_unicode(self):
        # If it can't re-encode to latin-1, it returns as-is
        result = _decode_fb_name("日本語")
        assert isinstance(result, str)


class TestFacebookMakeId:
    def test_deterministic(self):
        assert fb_make_id("Alice") == fb_make_id("Alice")

    def test_prefix(self):
        assert fb_make_id("Bob").startswith("contacts:facebook:")

    def test_length(self):
        result = fb_make_id("Charlie")
        # "contacts:facebook:" + 12 hex chars
        assert len(result) == len("contacts:facebook:") + 12


class TestFacebookTsToIso:
    def test_valid_timestamp(self):
        result = _ts_to_iso(1700000000)
        assert "2023" in result
        assert "T" in result

    def test_none(self):
        assert _ts_to_iso(None) == ""

    def test_invalid(self):
        assert _ts_to_iso("not-a-number") == ""

    def test_zero_timestamp(self):
        # ts=0 is not None, so it passes the None check and produces epoch date
        result = _ts_to_iso(0)
        assert "1970" in result


class TestFacebookParseFriends:
    def test_parses_friends_v2(self, tmp_path):
        json_file = tmp_path / "friends.json"
        json_file.write_text(json.dumps({
            "friends_v2": [
                {"name": "Alice", "timestamp": 1700000000},
                {"name": "Bob", "timestamp": 1700100000},
            ]
        }))
        data = json.loads(json_file.read_text())
        entries = _parse_friends(data, str(json_file))

        assert len(entries) == 2
        assert entries[0]["name"]["display"] == "Alice"
        assert entries[0]["source"] == "facebook"
        assert entries[1]["name"]["display"] == "Bob"

    def test_skips_empty_names(self, tmp_path):
        json_file = tmp_path / "friends.json"
        json_file.write_text(json.dumps({
            "friends_v2": [
                {"name": "Alice", "timestamp": 1700000000},
                {"name": "", "timestamp": 1700100000},
                {"timestamp": 1700200000},
            ]
        }))
        data = json.loads(json_file.read_text())
        entries = _parse_friends(data, str(json_file))
        assert len(entries) == 1

    def test_empty_friends_list(self):
        entries = _parse_friends({"friends_v2": []}, "/fake/path")
        assert entries == []

    def test_missing_key(self):
        entries = _parse_friends({"other_key": []}, "/fake/path")
        assert entries == []


class TestFacebookParseAddressBook:
    def test_parses_address_book_v2(self, tmp_path):
        json_file = tmp_path / "address_book.json"
        data = {
            "address_book": {
                "address_book_v2": [
                    {
                        "name": "Carol",
                        "details": [
                            {"contact_point": "carol@test.com"},
                            {"contact_point": "+1-555-0100"},
                        ],
                        "timestamp": 1700000000,
                    },
                ]
            }
        }
        json_file.write_text(json.dumps(data))
        entries = _parse_address_book(data, str(json_file))

        assert len(entries) == 1
        assert entries[0]["emails"] == ["carol@test.com"]
        assert entries[0]["phones"] == ["+1-555-0100"]

    def test_top_level_address_book(self, tmp_path):
        json_file = tmp_path / "ab.json"
        data = {
            "address_book_v2": [
                {"name": "Dave", "details": []},
            ]
        }
        json_file.write_text(json.dumps(data))
        entries = _parse_address_book(data, str(json_file))
        assert len(entries) == 1
        assert entries[0]["name"]["display"] == "Dave"

    def test_empty_details(self, tmp_path):
        json_file = tmp_path / "ab.json"
        data = {
            "address_book_v2": [
                {"name": "Eve", "details": [{"contact_point": ""}]},
            ]
        }
        json_file.write_text(json.dumps(data))
        entries = _parse_address_book(data, str(json_file))
        assert len(entries) == 1
        assert entries[0]["emails"] == []
        assert entries[0]["phones"] == []


class TestFacebookFindFiles:
    def test_single_json_file(self, tmp_path):
        f = tmp_path / "friends.json"
        f.write_text("{}")
        result = _find_files(str(f))
        assert len(result) == 1
        assert result[0][1] == "auto"

    def test_directory_with_friends(self, tmp_path):
        friends_dir = tmp_path / "friends_and_followers"
        friends_dir.mkdir()
        (friends_dir / "friends.json").write_text("{}")
        result = _find_files(str(tmp_path))
        assert len(result) == 1
        assert result[0][1] == "friends"

    def test_directory_with_address_book(self, tmp_path):
        ab_dir = tmp_path / "about_you"
        ab_dir.mkdir()
        (ab_dir / "your_address_books.json").write_text("{}")
        result = _find_files(str(tmp_path))
        assert len(result) == 1
        assert result[0][1] == "address_book"

    def test_nonexistent_path(self, tmp_path):
        result = _find_files(str(tmp_path / "nope"))
        assert result == []

    def test_non_json_file(self, tmp_path):
        f = tmp_path / "data.txt"
        f.write_text("hello")
        result = _find_files(str(f))
        assert result == []


class TestFacebookRunImport:
    def test_end_to_end_friends(self, tmp_path):
        export_dir = tmp_path / "export"
        friends_dir = export_dir / "friends_and_followers"
        friends_dir.mkdir(parents=True)
        (friends_dir / "friends.json").write_text(json.dumps({
            "friends_v2": [
                {"name": "Alice", "timestamp": 1700000000},
                {"name": "Bob", "timestamp": 1700100000},
            ]
        }))

        vault_dir = tmp_path / "vaults"
        config = {"vault_root": str(vault_dir)}
        fb_run_import(str(export_dir), config)

        jsonl = vault_dir / "Contacts_Facebook" / "contacts.jsonl"
        assert jsonl.exists()
        lines = jsonl.read_text().strip().split("\n")
        assert len(lines) == 2

    def test_end_to_end_single_file(self, tmp_path):
        json_file = tmp_path / "friends.json"
        json_file.write_text(json.dumps({
            "friends_v2": [
                {"name": "Carol", "timestamp": 1700000000},
            ]
        }))

        vault_dir = tmp_path / "vaults"
        config = {"vault_root": str(vault_dir)}
        fb_run_import(str(json_file), config)

        jsonl = vault_dir / "Contacts_Facebook" / "contacts.jsonl"
        assert jsonl.exists()
        lines = jsonl.read_text().strip().split("\n")
        assert len(lines) == 1

    def test_deduplicates_across_files(self, tmp_path):
        export_dir = tmp_path / "export"
        friends_dir = export_dir / "friends_and_followers"
        friends_dir.mkdir(parents=True)
        (friends_dir / "friends.json").write_text(json.dumps({
            "friends_v2": [
                {"name": "Alice", "timestamp": 1700000000},
            ]
        }))
        # Also put same person in address book
        ab_dir = export_dir / "about_you"
        ab_dir.mkdir(parents=True)
        # Use "auto" by creating a single file that has both keys
        # Actually we need address book candidate path
        (ab_dir / "your_address_books.json").write_text(json.dumps({
            "address_book": {
                "address_book_v2": [
                    {"name": "Alice", "details": [{"contact_point": "alice@test.com"}]},
                ]
            }
        }))

        vault_dir = tmp_path / "vaults"
        config = {"vault_root": str(vault_dir)}
        fb_run_import(str(export_dir), config)

        jsonl = vault_dir / "Contacts_Facebook" / "contacts.jsonl"
        lines = jsonl.read_text().strip().split("\n")
        # Same name "Alice" should produce same ID, so deduplicated to 1
        assert len(lines) == 1


# ═══════════════════════════════════════════════════════════════════════
# Instagram Contacts
# ═══════════════════════════════════════════════════════════════════════

class TestInstagramDecodeText:
    def test_ascii_passthrough(self):
        assert _decode_facebook_text("hello") == "hello"

    def test_mojibake_decode(self):
        encoded = "Ren\u00c3\u00a9"
        assert _decode_facebook_text(encoded) == "René"

    def test_none_returns_empty(self):
        assert _decode_facebook_text(None) == ""

    def test_empty_string(self):
        assert _decode_facebook_text("") == ""

    def test_non_string_returns_as_is(self):
        # Non-string, non-falsy values are returned unchanged
        assert _decode_facebook_text(12345) == 12345


class TestInstagramParseOldFormat:
    def test_parses_list_format(self):
        data = [
            {
                "string_list_data": [
                    {"value": "user_one", "timestamp": 1700000000},
                ]
            },
            {
                "string_list_data": [
                    {"value": "user_two", "timestamp": 1700100000},
                ]
            },
        ]
        result = _parse_old_format(data)
        assert "user_one" in result
        assert "user_two" in result
        assert result["user_one"]["username"] == "user_one"
        assert result["user_one"]["timestamp"] == 1700000000

    def test_skips_empty_usernames(self):
        data = [
            {
                "string_list_data": [
                    {"value": "", "timestamp": 0},
                ]
            },
        ]
        result = _parse_old_format(data)
        assert len(result) == 0

    def test_non_list_returns_empty(self):
        assert _parse_old_format({"not": "a list"}) == {}

    def test_lowercases_keys(self):
        data = [
            {
                "string_list_data": [
                    {"value": "UserMixed", "timestamp": 0},
                ]
            },
        ]
        result = _parse_old_format(data)
        assert "usermixed" in result
        assert result["usermixed"]["username"] == "UserMixed"


class TestInstagramParseNewFormat:
    def test_parses_relationships_followers(self):
        data = {
            "relationships_followers": [
                {
                    "title": "follower_user",
                    "string_list_data": [
                        {"value": "follower_user", "timestamp": 1700000000},
                    ],
                },
            ]
        }
        result = _parse_new_format(data, "relationships_followers")
        assert "follower_user" in result

    def test_falls_back_to_title(self):
        data = {
            "relationships_following": [
                {
                    "title": "title_user",
                    "string_list_data": [],
                },
            ]
        }
        result = _parse_new_format(data, "relationships_following")
        assert "title_user" in result

    def test_non_dict_returns_empty(self):
        assert _parse_new_format([1, 2, 3], "key") == {}

    def test_missing_key_returns_empty(self):
        assert _parse_new_format({"other": []}, "relationships_followers") == {}


class TestInstagramFindExportFiles:
    def test_finds_in_root(self, tmp_path):
        (tmp_path / "followers.json").write_text("[]")
        (tmp_path / "following.json").write_text("[]")
        followers, following = _find_export_files(str(tmp_path))
        assert len(followers) == 1
        assert len(following) == 1

    def test_finds_in_subdirectory(self, tmp_path):
        sub = tmp_path / "followers_and_following"
        sub.mkdir()
        (sub / "followers_1.json").write_text("[]")
        (sub / "following_1.json").write_text("[]")
        followers, following = _find_export_files(str(tmp_path))
        assert len(followers) >= 1
        assert len(following) >= 1

    def test_empty_directory(self, tmp_path):
        followers, following = _find_export_files(str(tmp_path))
        assert followers == []
        assert following == []

    def test_deduplicates(self, tmp_path):
        # File in root that also matches from connections subdir
        (tmp_path / "followers.json").write_text("[]")
        followers, _ = _find_export_files(str(tmp_path))
        # Even if searched multiple patterns, should deduplicate
        assert len(followers) == 1


class TestInstagramBuildEntry:
    def test_basic_entry(self):
        entry = _build_entry("testuser", "mutual", 1700000000, "2024-01-01T00:00:00Z")
        assert entry["id"] == "contacts:instagram:testuser"
        assert entry["source"] == "instagram"
        assert entry["source_id"] == "testuser"
        assert entry["name"]["display"] == "testuser"
        assert entry["handles"]["instagram"] == "testuser"
        assert entry["relationship"] == "mutual"
        assert "2023" in entry["connected_on"]
        assert entry["updated_at"] == "2024-01-01T00:00:00Z"

    def test_zero_timestamp(self):
        entry = _build_entry("user", "follower", 0, "2024-01-01T00:00:00Z")
        assert entry["connected_on"] == ""

    def test_id_uses_lowercase(self):
        entry = _build_entry("MixedCase", "following", 0, "")
        assert entry["id"] == "contacts:instagram:mixedcase"


class TestInstagramRunImport:
    def test_old_format_followers_file(self, tmp_path):
        followers_file = tmp_path / "followers.json"
        followers_file.write_text(json.dumps([
            {
                "string_list_data": [
                    {"value": "alice", "timestamp": 1700000000},
                ]
            },
            {
                "string_list_data": [
                    {"value": "bob", "timestamp": 1700100000},
                ]
            },
        ]))

        vault_dir = tmp_path / "vaults"
        config = {"vault_root": str(vault_dir)}
        ig_run_import(str(followers_file), config)

        jsonl = vault_dir / "Contacts_Instagram" / "contacts.jsonl"
        assert jsonl.exists()
        lines = jsonl.read_text().strip().split("\n")
        assert len(lines) == 2

    def test_new_format_directory(self, tmp_path):
        export_dir = tmp_path / "export"
        sub = export_dir / "followers_and_following"
        sub.mkdir(parents=True)

        (sub / "followers_1.json").write_text(json.dumps({
            "relationships_followers": [
                {
                    "title": "alice",
                    "string_list_data": [{"value": "alice", "timestamp": 1700000000}],
                },
                {
                    "title": "charlie",
                    "string_list_data": [{"value": "charlie", "timestamp": 1700200000}],
                },
            ]
        }))
        (sub / "following.json").write_text(json.dumps({
            "relationships_following": [
                {
                    "title": "alice",
                    "string_list_data": [{"value": "alice", "timestamp": 1700000000}],
                },
                {
                    "title": "dave",
                    "string_list_data": [{"value": "dave", "timestamp": 1700300000}],
                },
            ]
        }))

        vault_dir = tmp_path / "vaults"
        config = {"vault_root": str(vault_dir)}
        ig_run_import(str(export_dir), config)

        jsonl = vault_dir / "Contacts_Instagram" / "contacts.jsonl"
        assert jsonl.exists()
        lines = jsonl.read_text().strip().split("\n")
        entries = [json.loads(line) for line in lines]

        # alice=mutual, charlie=follower, dave=following
        assert len(entries) == 3
        by_name = {e["source_id"]: e for e in entries}
        assert by_name["alice"]["relationship"] == "mutual"
        assert by_name["charlie"]["relationship"] == "follower"
        assert by_name["dave"]["relationship"] == "following"

    def test_skips_already_processed(self, tmp_path):
        followers_file = tmp_path / "followers.json"
        followers_file.write_text(json.dumps([
            {
                "string_list_data": [
                    {"value": "alice", "timestamp": 1700000000},
                ]
            },
        ]))

        vault_dir = tmp_path / "vaults"
        config = {"vault_root": str(vault_dir)}

        # Run once
        ig_run_import(str(followers_file), config)
        # Run again
        ig_run_import(str(followers_file), config)

        jsonl = vault_dir / "Contacts_Instagram" / "contacts.jsonl"
        lines = jsonl.read_text().strip().split("\n")
        assert len(lines) == 1

    def test_nonexistent_path(self, tmp_path, capsys):
        vault_dir = tmp_path / "vaults"
        config = {"vault_root": str(vault_dir)}
        ig_run_import(str(tmp_path / "nonexistent"), config)
        captured = capsys.readouterr()
        assert "not found" in captured.out.lower() or "Error" in captured.out

    def test_single_following_file(self, tmp_path):
        following_file = tmp_path / "following.json"
        following_file.write_text(json.dumps([
            {
                "string_list_data": [
                    {"value": "someone", "timestamp": 1700000000},
                ]
            },
        ]))

        vault_dir = tmp_path / "vaults"
        config = {"vault_root": str(vault_dir)}
        ig_run_import(str(following_file), config)

        jsonl = vault_dir / "Contacts_Instagram" / "contacts.jsonl"
        lines = jsonl.read_text().strip().split("\n")
        entries = [json.loads(line) for line in lines]
        assert len(entries) == 1
        # "following" in filename, so role should be "following"
        assert entries[0]["relationship"] == "following"
