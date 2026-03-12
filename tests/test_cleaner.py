"""Tests for core/cleaner.py — RAG cleaning and enrichment logic.

These tests are pure-local (no API calls).
"""

import json
import os
import tempfile

import pytest

from core.cleaner import (
    parse_contact,
    parse_contact_list,
    strip_quotes_and_signatures,
    detect_automated,
    extract_entities,
    detect_language,
    clean_entry,
    build_thread_index,
    _set_year_month_fallback,
    run_clean,
)


# ---------------------------------------------------------------------------
# Contact parsing
# ---------------------------------------------------------------------------
class TestParseContact:
    def test_name_and_email(self):
        name, email = parse_contact("John Doe <john@example.com>")
        assert name == "John Doe"
        assert email == "john@example.com"

    def test_bare_email(self):
        name, email = parse_contact("john@example.com")
        assert name == ""
        assert email == "john@example.com"

    def test_quoted_name(self):
        name, email = parse_contact('"Doe, John" <john@example.com>')
        assert name == "Doe, John"
        assert email == "john@example.com"

    def test_angle_bracket_only(self):
        name, email = parse_contact("<john@example.com>")
        assert name == ""
        assert email == "john@example.com"

    def test_empty_string(self):
        name, email = parse_contact("")
        assert name == ""
        assert email == ""

    def test_none(self):
        name, email = parse_contact(None)
        assert name == ""
        assert email == ""

    def test_email_case_normalized(self):
        _, email = parse_contact("John <John@EXAMPLE.COM>")
        assert email == "john@example.com"

    def test_name_with_special_chars(self):
        name, email = parse_contact("José García <jose@example.com>")
        assert name == "José García"
        assert email == "jose@example.com"

    def test_display_name_no_angle_brackets(self):
        name, email = parse_contact("alice@example.com (Alice)")
        assert email == "alice@example.com"


class TestParseContactList:
    def test_single_recipient(self):
        result = parse_contact_list("John <john@ex.com>")
        assert len(result) == 1
        assert result[0]["email"] == "john@ex.com"
        assert result[0]["name"] == "John"

    def test_multiple_recipients(self):
        result = parse_contact_list("John <john@ex.com>, Jane <jane@ex.com>")
        assert len(result) == 2
        assert result[0]["email"] == "john@ex.com"
        assert result[1]["email"] == "jane@ex.com"

    def test_mixed_formats(self):
        result = parse_contact_list('alice@ex.com, "Bob Smith" <bob@ex.com>')
        assert len(result) == 2
        assert result[0]["email"] == "alice@ex.com"
        assert result[1]["name"] == "Bob Smith"

    def test_empty_string(self):
        assert parse_contact_list("") == []

    def test_none(self):
        assert parse_contact_list(None) == []

    def test_invalid_entries_filtered(self):
        """Empty entries in a contact list should be filtered out."""
        result = parse_contact_list("")
        assert len(result) == 0


# ---------------------------------------------------------------------------
# Quote / signature stripping
# ---------------------------------------------------------------------------
class TestStripQuotesAndSignatures:
    def test_no_quotes(self):
        body = "Hello, this is my message.\nThanks!"
        assert strip_quotes_and_signatures(body) == body

    def test_gmail_quote(self):
        body = (
            "Sounds good, let's meet tomorrow.\n\n"
            "On Mon, Jan 1, 2024 at 12:00 PM John Doe <john@ex.com> wrote:\n"
            "> Hey, want to grab lunch?\n"
            "> Let me know."
        )
        result = strip_quotes_and_signatures(body)
        assert "Sounds good" in result
        assert "On Mon" not in result
        assert "Hey, want to grab lunch" not in result

    def test_outlook_quote(self):
        body = (
            "I agree with the proposal.\n\n"
            "From: John Doe\n"
            "Sent: Monday, January 1, 2024 12:00 PM\n"
            "To: Jane Smith\n"
            "Subject: Budget Review\n\n"
            "Please review the attached."
        )
        result = strip_quotes_and_signatures(body)
        assert "I agree" in result
        assert "Please review" not in result

    def test_apple_mail_forward(self):
        body = (
            "See below.\n\n"
            "Begin forwarded message:\n"
            "From: someone@ex.com\n"
            "Original content here."
        )
        result = strip_quotes_and_signatures(body)
        assert "See below" in result
        assert "Original content" not in result

    def test_forwarded_message_marker(self):
        body = (
            "FYI\n\n"
            "---------- Forwarded message ----------\n"
            "From: boss@company.com\n"
            "Important announcement."
        )
        result = strip_quotes_and_signatures(body)
        assert "FYI" in result
        assert "Important announcement" not in result

    def test_original_message_marker(self):
        body = (
            "Thanks for the update.\n\n"
            "--- Original Message ---\n"
            "Previous content here."
        )
        result = strip_quotes_and_signatures(body)
        assert "Thanks" in result
        assert "Previous content" not in result

    def test_signature_separator(self):
        body = (
            "Here is my response.\n"
            "--\n"
            "John Doe\n"
            "CEO, Acme Corp\n"
            "+1-555-0123"
        )
        result = strip_quotes_and_signatures(body)
        assert "Here is my response" in result
        assert "CEO" not in result
        assert "+1-555" not in result

    def test_signature_with_trailing_space(self):
        body = "Content here.\n-- \nSig line"
        result = strip_quotes_and_signatures(body)
        assert "Content here" in result
        assert "Sig line" not in result

    def test_quoted_lines_skipped(self):
        body = "My reply.\n> Previous message line 1\n> Line 2\nMore of my reply."
        result = strip_quotes_and_signatures(body)
        assert "My reply" in result
        assert "More of my reply" in result
        assert "Previous message" not in result

    def test_nested_quotes_skipped(self):
        body = "Top reply.\n>> Double nested\n>>> Triple nested\nStill my text."
        result = strip_quotes_and_signatures(body)
        assert "Top reply" in result
        assert "Still my text" in result
        assert "Double nested" not in result

    def test_empty_body(self):
        assert strip_quotes_and_signatures("") == ""

    def test_none_body(self):
        assert strip_quotes_and_signatures(None) == ""

    def test_body_is_only_quotes(self):
        body = "> line 1\n> line 2\n> line 3"
        result = strip_quotes_and_signatures(body)
        assert result == ""

    def test_gmail_on_wrote_various_format(self):
        body = "Yes!\n\nOn 2024-01-01 at 10:00, alice@example.com wrote:\n> old text"
        result = strip_quotes_and_signatures(body)
        assert "Yes!" in result
        assert "old text" not in result

    def test_outlook_from_without_sent_not_stripped(self):
        """A line starting with From: that isn't an Outlook header block should be kept."""
        body = "From: the data shows that revenue increased.\nGreat quarter!"
        result = strip_quotes_and_signatures(body)
        assert "From: the data shows" in result

    def test_real_world_thread(self):
        body = (
            "Let me check and get back to you.\n\n"
            "On Wed, Mar 6, 2024 at 2:15 PM Sarah Connor <sarah@skynet.com> wrote:\n"
            ">\n"
            "> Can you review the attached proposal?\n"
            ">\n"
            "> On Tue, Mar 5, 2024 at 9:00 AM John Connor <john@resistance.org> wrote:\n"
            ">>\n"
            ">> We need a new plan.\n"
        )
        result = strip_quotes_and_signatures(body)
        assert result == "Let me check and get back to you."


# ---------------------------------------------------------------------------
# Automation detection
# ---------------------------------------------------------------------------
class TestDetectAutomated:
    def test_noreply_sender(self):
        assert detect_automated({"from": "noreply@company.com"}) is True

    def test_no_reply_with_hyphen(self):
        assert detect_automated({"from": "no-reply@service.io"}) is True

    def test_donotreply(self):
        assert detect_automated({"from": "donotreply@bank.com"}) is True

    def test_notifications(self):
        assert detect_automated({"from": "notifications@github.com"}) is True

    def test_mailer_daemon(self):
        assert detect_automated({"from": "mailer-daemon@google.com"}) is True

    def test_list_unsubscribe_header(self):
        assert detect_automated({"from": "newsletter@shop.com", "list_unsubscribe": "<https://unsub.link>"}) is True

    def test_category_promotions_label(self):
        assert detect_automated({"from": "shop@store.com", "tags": ["CATEGORY_PROMOTIONS"]}) is True

    def test_category_updates_label(self):
        assert detect_automated({"from": "svc@app.com", "tags": ["CATEGORY_UPDATES"]}) is True

    def test_category_social_label(self):
        assert detect_automated({"from": "notif@social.com", "tags": ["CATEGORY_SOCIAL"]}) is True

    def test_normal_personal_email(self):
        assert detect_automated({"from": "friend@gmail.com", "tags": ["INBOX"]}) is False

    def test_empty_from(self):
        assert detect_automated({"from": "", "tags": []}) is False

    def test_case_insensitive(self):
        assert detect_automated({"from": "NoReply@Company.COM"}) is True


# ---------------------------------------------------------------------------
# Entity extraction
# ---------------------------------------------------------------------------
class TestExtractEntities:
    def test_urls(self):
        text = "Check https://example.com/page and http://test.org"
        entities = extract_entities(text)
        assert len(entities["urls"]) == 2
        assert "https://example.com/page" in entities["urls"]

    def test_emails(self):
        text = "Contact me at alice@example.com or bob.smith@company.co.uk"
        entities = extract_entities(text)
        assert "alice@example.com" in entities["emails_mentioned"]
        assert "bob.smith@company.co.uk" in entities["emails_mentioned"]

    def test_dollar_amounts(self):
        text = "The invoice is $1,234.56 and the deposit is $500"
        entities = extract_entities(text)
        assert len(entities["amounts"]) == 2

    def test_euro_amounts(self):
        text = "Total: €500 plus 1000 EUR"
        entities = extract_entities(text)
        assert len(entities["amounts"]) >= 1

    def test_phone_numbers(self):
        text = "Call me at +1-555-0123 or (212) 555-4567"
        entities = extract_entities(text)
        assert len(entities["phone_numbers"]) >= 1

    def test_no_entities(self):
        text = "Just a plain message with no special content."
        entities = extract_entities(text)
        assert entities["urls"] == []
        assert entities["emails_mentioned"] == []
        assert entities["amounts"] == []
        assert entities["phone_numbers"] == []

    def test_empty_text(self):
        entities = extract_entities("")
        assert all(v == [] for v in entities.values())

    def test_none_text(self):
        entities = extract_entities(None)
        assert all(v == [] for v in entities.values())


# ---------------------------------------------------------------------------
# Language detection
# ---------------------------------------------------------------------------
class TestDetectLanguage:
    def test_english(self):
        text = "This is a regular English email about the meeting tomorrow."
        assert detect_language(text) == "en"

    def test_french(self):
        text = "Bonjour, je vous écris pour confirmer la réunion de demain dans le bureau."
        assert detect_language(text) == "fr"

    def test_german(self):
        text = "Hallo, ich schreibe Ihnen wegen der Besprechung morgen. Wir haben das nicht besprochen."
        assert detect_language(text) == "de"

    def test_spanish(self):
        text = "Hola, le escribo para confirmar la reunión de mañana por la tarde en el edificio."
        assert detect_language(text) == "es"

    def test_chinese(self):
        text = "你好，我想确认一下明天的会议时间和地点。"
        assert detect_language(text) == "zh"

    def test_short_text(self):
        assert detect_language("Ok") == "unknown"

    def test_empty(self):
        assert detect_language("") == "unknown"

    def test_none(self):
        assert detect_language(None) == "unknown"

    def test_dutch(self):
        text = "Hallo, ik schrijf om de vergadering van morgen te bevestigen. Het is niet duidelijk."
        assert detect_language(text) == "nl"

    def test_japanese(self):
        text = "こんにちは。明日の会議について確認したいのですが。よろしくお願いします。"
        assert detect_language(text) == "ja"

    def test_korean(self):
        text = "안녕하세요. 내일 회의에 대해 확인하고 싶습니다. 감사합니다."
        assert detect_language(text) == "ko"

    def test_arabic(self):
        text = "مرحبا، أريد تأكيد موعد الاجتماع غدا. شكرا لكم."
        assert detect_language(text) == "ar"

    def test_russian(self):
        text = "Здравствуйте, я хочу подтвердить встречу завтра. Спасибо вам."
        assert detect_language(text) == "ru"


# ---------------------------------------------------------------------------
# Thread index
# ---------------------------------------------------------------------------
class TestBuildThreadIndex:
    def test_single_thread(self):
        entries = [
            {"id": "a", "threadId": "t1", "internalDate": "1000"},
            {"id": "b", "threadId": "t1", "internalDate": "2000"},
            {"id": "c", "threadId": "t1", "internalDate": "3000"},
        ]
        index = build_thread_index(entries)
        assert len(index) == 1
        assert index["t1"] == [("a", 1000), ("b", 2000), ("c", 3000)]

    def test_multiple_threads(self):
        entries = [
            {"id": "a", "threadId": "t1", "internalDate": "1000"},
            {"id": "b", "threadId": "t2", "internalDate": "2000"},
        ]
        index = build_thread_index(entries)
        assert len(index) == 2

    def test_sorts_chronologically(self):
        entries = [
            {"id": "c", "threadId": "t1", "internalDate": "3000"},
            {"id": "a", "threadId": "t1", "internalDate": "1000"},
            {"id": "b", "threadId": "t1", "internalDate": "2000"},
        ]
        index = build_thread_index(entries)
        assert [t[0] for t in index["t1"]] == ["a", "b", "c"]

    def test_no_thread_id(self):
        entries = [{"id": "a", "internalDate": "1000"}]
        index = build_thread_index(entries)
        assert len(index) == 0

    def test_missing_internal_date(self):
        entries = [{"id": "a", "threadId": "t1"}]
        index = build_thread_index(entries)
        assert len(index["t1"]) == 1


# ---------------------------------------------------------------------------
# Year/month fallback
# ---------------------------------------------------------------------------
class TestSetYearMonthFallback:
    def test_rfc_date(self):
        entry = {"date": "Mon, 15 Mar 2024 10:00:00 +0000"}
        _set_year_month_fallback(entry)
        assert entry["year"] == 2024
        assert entry["month"] == 3

    def test_no_date(self):
        entry = {"date": ""}
        _set_year_month_fallback(entry)
        assert entry["year"] == 0
        assert entry["month"] == 0

    def test_garbage_date(self):
        entry = {"date": "not-a-date"}
        _set_year_month_fallback(entry)
        assert entry["year"] == 0
        assert entry["month"] == 0


# ---------------------------------------------------------------------------
# Full entry cleaning
# ---------------------------------------------------------------------------
class TestCleanEntry:
    def _make_entry(self, **overrides):
        entry = {
            "id": "msg_001",
            "threadId": "thread_001",
            "internalDate": "1704067200000",  # 2024-01-01 00:00:00 UTC
            "sizeEstimate": 5000,
            "date": "Mon, 01 Jan 2024 00:00:00 +0000",
            "subject": "Test Subject",
            "from": "Alice Smith <alice@example.com>",
            "to": "Bob Jones <bob@example.com>",
            "cc": "Carol <carol@example.com>",
            "bcc": "",
            "reply_to": "",
            "message_id": "<msg001@example.com>",
            "in_reply_to": "",
            "references": "",
            "list_unsubscribe": "",
            "tags": ["INBOX"],
            "attachments": [],
            "body_raw": "Hello Bob,\nHow are you doing?\nBest regards.",
        }
        entry.update(overrides)
        return entry

    def test_basic_clean(self):
        entry = self._make_entry()
        result = clean_entry(entry)

        assert result["body_clean"] == "Hello Bob,\nHow are you doing?\nBest regards."
        assert result["from_name"] == "Alice Smith"
        assert result["from_email"] == "alice@example.com"
        assert len(result["to_list"]) == 1
        assert result["to_list"][0]["email"] == "bob@example.com"
        assert len(result["cc_list"]) == 1
        assert result["cc_list"][0]["email"] == "carol@example.com"
        assert result["year"] == 2024
        assert result["month"] == 1
        assert result["is_automated"] is False
        assert result["has_attachments"] is False
        assert result["word_count"] > 0
        assert result["lang"] == "en"

    def test_with_quotes_stripped(self):
        body = "Sure!\n\nOn Mon, Jan 1, 2024 at 12:00 PM Bob <bob@ex.com> wrote:\n> Old text"
        entry = self._make_entry(body_raw=body)
        result = clean_entry(entry)
        assert "Sure!" in result["body_clean"]
        assert "Old text" not in result["body_clean"]

    def test_with_internal_date(self):
        # 2023-06-15 12:00:00 UTC = 1686830400000 ms
        entry = self._make_entry(internalDate="1686830400000")
        result = clean_entry(entry)
        assert result["year"] == 2023
        assert result["month"] == 6

    def test_without_internal_date_falls_back(self):
        entry = self._make_entry(internalDate="", date="Mon, 15 Mar 2024 10:00:00 +0000")
        result = clean_entry(entry)
        assert result["year"] == 2024
        assert result["month"] == 3

    def test_automated_detection_noreply(self):
        entry = self._make_entry(**{"from": "noreply@service.com"})
        result = clean_entry(entry)
        assert result["is_automated"] is True

    def test_automated_detection_list_unsubscribe(self):
        entry = self._make_entry(list_unsubscribe="<https://unsub.link>")
        result = clean_entry(entry)
        assert result["is_automated"] is True

    def test_automated_detection_promo_label(self):
        entry = self._make_entry(tags=["CATEGORY_PROMOTIONS"])
        result = clean_entry(entry)
        assert result["is_automated"] is True

    def test_with_attachments(self):
        attachments = [
            {"filename": "invoice.pdf", "mimeType": "application/pdf", "size": 12345},
            {"filename": "photo.jpg", "mimeType": "image/jpeg", "size": 54321},
        ]
        entry = self._make_entry(attachments=attachments)
        result = clean_entry(entry)
        assert result["has_attachments"] is True
        assert "invoice.pdf" in result["attachment_names"]
        assert "photo.jpg" in result["attachment_names"]

    def test_thread_position(self):
        thread_index = {
            "thread_001": [("msg_000", 100), ("msg_001", 200), ("msg_002", 300)]
        }
        entry = self._make_entry()
        result = clean_entry(entry, thread_index)
        assert result["thread_position"] == 2
        assert result["thread_depth"] == 3

    def test_thread_depth_single_message(self):
        entry = self._make_entry()
        result = clean_entry(entry)
        assert result["thread_position"] == 1
        assert result["thread_depth"] == 1

    def test_entity_extraction(self):
        body = "Check https://example.com and send $500 to alice@test.com"
        entry = self._make_entry(body_raw=body)
        result = clean_entry(entry)
        assert "https://example.com" in result["entities"]["urls"]
        assert "alice@test.com" in result["entities"]["emails_mentioned"]
        assert len(result["entities"]["amounts"]) >= 1

    def test_embedding_text_format(self):
        entry = self._make_entry()
        result = clean_entry(entry)
        assert result["body_for_embedding"].startswith("From Alice Smith to Bob Jones")
        assert "re: Test Subject:" in result["body_for_embedding"]

    def test_empty_body(self):
        entry = self._make_entry(body_raw="")
        result = clean_entry(entry)
        assert result["body_clean"] == ""
        assert result["word_count"] == 0

    def test_no_cc(self):
        entry = self._make_entry(cc="")
        result = clean_entry(entry)
        assert result["cc_list"] == []

    def test_missing_optional_fields(self):
        """Entry collected before enrichment (no cc, internalDate, etc.)."""
        entry = {
            "id": "msg_old",
            "threadId": "thread_old",
            "date": "Mon, 15 Mar 2024 10:00:00 +0000",
            "subject": "Old Email",
            "from": "alice@example.com",
            "to": "bob@example.com",
            "tags": ["INBOX"],
            "body_raw": "Hello world",
        }
        result = clean_entry(entry)
        assert result["body_clean"] == "Hello world"
        assert result["from_email"] == "alice@example.com"
        assert result["cc_list"] == []
        assert result["year"] == 2024


# ---------------------------------------------------------------------------
# Integration: run_clean with temp vault
# ---------------------------------------------------------------------------
class TestRunClean:
    def _make_vault(self, tmpdir, entries):
        """Create a minimal vault structure with entries."""
        vault_path = os.path.join(tmpdir, "vaults", "Gmail_Primary")
        year_dir = os.path.join(vault_path, "2024")
        os.makedirs(year_dir, exist_ok=True)

        file_path = os.path.join(year_dir, "01_January.jsonl")
        with open(file_path, "w") as f:
            for entry in entries:
                f.write(json.dumps(entry) + "\n")

        return vault_path

    def test_basic_run(self, tmp_path):
        entries = [
            {
                "id": "msg_001",
                "threadId": "t1",
                "internalDate": "1704067200000",
                "date": "Mon, 01 Jan 2024 00:00:00 +0000",
                "subject": "Hello",
                "from": "alice@ex.com",
                "to": "bob@ex.com",
                "tags": ["INBOX"],
                "body_raw": "Hi there!",
            },
            {
                "id": "msg_002",
                "threadId": "t1",
                "internalDate": "1704070800000",
                "date": "Mon, 01 Jan 2024 01:00:00 +0000",
                "subject": "Re: Hello",
                "from": "bob@ex.com",
                "to": "alice@ex.com",
                "tags": ["INBOX"],
                "body_raw": "Hey!\n\nOn Mon, Jan 1, 2024 at 12:00 AM alice@ex.com wrote:\n> Hi there!",
            },
        ]
        vault_path = self._make_vault(str(tmp_path), entries)

        config = {"vault_root": os.path.join(str(tmp_path), "vaults")}
        run_clean(vault_name="Primary", config=config)

        # Read back the cleaned entries
        file_path = os.path.join(vault_path, "2024", "01_January.jsonl")
        with open(file_path) as f:
            cleaned = [json.loads(line) for line in f]

        assert len(cleaned) == 2

        # First message should be unchanged
        assert cleaned[0]["body_clean"] == "Hi there!"

        # Second message should have quotes stripped
        assert "Hey!" in cleaned[1]["body_clean"]
        assert "Hi there!" not in cleaned[1]["body_clean"]

        # Both should have RAG fields
        for c in cleaned:
            assert "body_for_embedding" in c
            assert "from_email" in c
            assert "to_list" in c
            assert "word_count" in c
            assert "lang" in c
            assert "entities" in c
            assert "is_automated" in c

        # Thread depth should be 2 for both
        assert cleaned[0]["thread_depth"] == 2
        assert cleaned[1]["thread_depth"] == 2
        assert cleaned[0]["thread_position"] == 1
        assert cleaned[1]["thread_position"] == 2

    def test_resume_skips_cleaned(self, tmp_path):
        """Already-cleaned entries should not be re-cleaned."""
        entries = [
            {
                "id": "msg_001",
                "threadId": "t1",
                "date": "Mon, 01 Jan 2024 00:00:00 +0000",
                "subject": "Already Done",
                "from": "alice@ex.com",
                "to": "bob@ex.com",
                "tags": ["INBOX"],
                "body_raw": "Original text",
                "body_clean": "Already cleaned text",
                "from_name": "Alice",
                "from_email": "alice@ex.com",
                "to_list": [{"name": "", "email": "bob@ex.com"}],
                "cc_list": [],
                "year": 2024,
                "month": 1,
                "thread_position": 1,
                "thread_depth": 1,
                "is_automated": False,
                "has_attachments": False,
                "attachment_names": [],
                "entities": {"urls": [], "emails_mentioned": [], "amounts": [], "phone_numbers": []},
                "word_count": 3,
                "lang": "en",
                "body_for_embedding": "test",
            },
        ]
        vault_path = self._make_vault(str(tmp_path), entries)
        config = {"vault_root": os.path.join(str(tmp_path), "vaults")}
        run_clean(vault_name="Primary", config=config)

        # Read back — should be unchanged
        file_path = os.path.join(vault_path, "2024", "01_January.jsonl")
        with open(file_path) as f:
            result = [json.loads(line) for line in f]

        assert result[0]["body_clean"] == "Already cleaned text"
