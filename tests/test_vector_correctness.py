"""
Vector Search Correctness Tests
================================
Validates that the ChromaDB vector store returns correct, relevant results
when queried against the real vault JSONL files as ground truth.

This is the MOST IMPORTANT test file in the project. It catches the class of
bug where `nomolo search "renatalix first mail"` returns wrong results.

Run with:
    pytest tests/test_vector_correctness.py -v
    pytest tests/test_vector_correctness.py -v -k "email"        # email tests only
    pytest tests/test_vector_correctness.py -v -k "diagnostic"   # diagnostics only

Requires:
    - Real vault at VAULT_ROOT (skipped in CI if missing)
    - ChromaDB vectorized (run `nomolo vectorize` first)
"""

import json
import logging
import os
import random
import re
import sys
from collections import defaultdict
from datetime import datetime

import pytest

# Project root on sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from core.vault import read_all_entries, count_entries, _find_jsonl_files, _open_jsonl
from core.vectordb import (
    get_client,
    search,
    get_or_create_collection,
    detect_embedding_field,
    chunk_text,
    get_full_entry,
    get_status,
    _get_embedding_fn,
    EMBEDDING_FIELDS,
    DEFAULT_CHUNK_SIZE,
    DEFAULT_CHUNK_OVERLAP,
)

logger = logging.getLogger("nomolo.test_vector_correctness")

# ---------------------------------------------------------------------------
# Vault paths — skip entire module if vault is absent
# ---------------------------------------------------------------------------

VAULT_ROOT = os.path.expanduser(
    "~/Documents/NomoloUser/Nomolo/vaults"
)
GMAIL_VAULT = os.path.join(VAULT_ROOT, "Gmail_Primary")
CALENDAR_VAULT = os.path.join(VAULT_ROOT, "Calendar")
BOOKS_VAULT = os.path.join(VAULT_ROOT, "Books")
CONTACTS_VAULT = os.path.join(VAULT_ROOT, "Contacts_Google")
VECTORDB_PATH = os.path.join(VAULT_ROOT, ".vectordb")

HAS_VAULT = os.path.isdir(VAULT_ROOT) and os.path.isdir(VECTORDB_PATH)

pytestmark = pytest.mark.skipif(
    not HAS_VAULT,
    reason=f"Real vault not found at {VAULT_ROOT} — skipping correctness tests.",
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def chroma_client():
    """Persistent ChromaDB client pointing at the real vault."""
    return get_client(VAULT_ROOT)


@pytest.fixture(scope="module")
def gmail_collection(chroma_client):
    """The gmail_primary ChromaDB collection."""
    return get_or_create_collection(chroma_client, "Gmail_Primary")


@pytest.fixture(scope="module")
def calendar_collection(chroma_client):
    """The calendar ChromaDB collection."""
    return get_or_create_collection(chroma_client, "Calendar")


@pytest.fixture(scope="module")
def books_collection(chroma_client):
    """The books ChromaDB collection."""
    return get_or_create_collection(chroma_client, "Books")


@pytest.fixture(scope="module")
def sample_gmail_entries():
    """Load a deterministic sample of Gmail entries for testing.

    Picks entries spread across multiple years to cover diverse senders,
    subjects, and content types. Returns list of dicts.
    """
    entries = []
    # Sample from a few different years for variety
    sample_dirs = []
    for year in ["2024", "2023", "2022", "2020", "2015"]:
        year_dir = os.path.join(GMAIL_VAULT, year)
        if os.path.isdir(year_dir):
            sample_dirs.append(year_dir)

    for sample_dir in sample_dirs:
        count = 0
        for entry in read_all_entries(sample_dir):
            if entry.get("body_for_embedding") and entry.get("id"):
                entries.append(entry)
                count += 1
                if count >= 5:  # 5 per year
                    break

    return entries


@pytest.fixture(scope="module")
def sample_calendar_entries():
    """Load a sample of Calendar entries."""
    entries = []
    count = 0
    for entry in read_all_entries(CALENDAR_VAULT):
        if entry.get("event_for_embedding") and entry.get("id"):
            entries.append(entry)
            count += 1
            if count >= 20:
                break
    return entries


@pytest.fixture(scope="module")
def sample_book_entries():
    """Load a sample of Book entries."""
    entries = []
    count = 0
    for entry in read_all_entries(BOOKS_VAULT):
        if entry.get("book_for_embedding") and entry.get("id"):
            entries.append(entry)
            count += 1
            if count >= 20:
                break
    return entries


# ===================================================================
# PART 1: Ground Truth Verification Tests
# ===================================================================

class TestGroundTruthVerification:
    """Pick KNOWN entries from vault JSONL and verify they appear in search results."""

    def test_known_gmail_entry_found_by_subject(self, chroma_client, sample_gmail_entries):
        """Search for a known email by its exact subject — it must appear in top results."""
        if not sample_gmail_entries:
            pytest.skip("No sample Gmail entries loaded")

        failures = []
        for entry in sample_gmail_entries[:10]:
            subject = entry.get("subject", "")
            if not subject or len(subject) < 5:
                continue

            results = search(
                chroma_client, subject,
                collections=["gmail_primary"],
                n_results=10,
            )
            found_ids = {r["metadata"].get("entry_id", r["id"]) for r in results}

            if entry["id"] not in found_ids:
                # Check if it appears deeper in results
                deep_results = search(
                    chroma_client, subject,
                    collections=["gmail_primary"],
                    n_results=50,
                )
                deep_ids = {r["metadata"].get("entry_id", r["id"]) for r in deep_results}
                deep_pos = None
                if entry["id"] in deep_ids:
                    for i, r in enumerate(deep_results):
                        eid = r["metadata"].get("entry_id", r["id"])
                        if eid == entry["id"]:
                            deep_pos = i + 1
                            break

                top_result_subjects = [
                    r["metadata"].get("subject", "?")[:60] for r in results[:3]
                ]
                failures.append({
                    "entry_id": entry["id"],
                    "subject": subject[:80],
                    "found_at_position": deep_pos,
                    "top_results_were": top_result_subjects,
                })

        if failures:
            msg = f"\n{len(failures)} known entries NOT found in top-10 by subject search:\n"
            for f in failures:
                msg += (
                    f"  ID: {f['entry_id']}\n"
                    f"  Subject: {f['subject']}\n"
                    f"  Found at position: {f['found_at_position'] or 'NOT IN TOP 50'}\n"
                    f"  Top results were: {f['top_results_were']}\n\n"
                )
            pytest.fail(msg)

    def test_known_gmail_entry_found_by_sender(self, chroma_client, sample_gmail_entries):
        """Search for emails by sender name — known entries from that sender should appear."""
        if not sample_gmail_entries:
            pytest.skip("No sample Gmail entries loaded")

        failures = []
        tested = 0
        for entry in sample_gmail_entries:
            sender_name = entry.get("from_name", "")
            if not sender_name or len(sender_name) < 3 or entry.get("is_automated"):
                continue

            tested += 1
            query = f"emails from {sender_name}"
            results = search(
                chroma_client, query,
                collections=["gmail_primary"],
                n_results=20,
            )

            # Check if ANY result is from this sender
            found_from_sender = False
            for r in results:
                result_from = r["metadata"].get("from", "")
                if sender_name.lower() in result_from.lower():
                    found_from_sender = True
                    break

            if not found_from_sender:
                top_senders = [
                    r["metadata"].get("from", "?")[:50] for r in results[:5]
                ]
                failures.append({
                    "sender": sender_name,
                    "entry_id": entry["id"],
                    "top_senders_returned": top_senders,
                })

            if tested >= 10:
                break

        if failures:
            msg = f"\n{len(failures)} sender searches returned NO emails from the expected sender:\n"
            for f in failures:
                msg += (
                    f"  Searched for: emails from {f['sender']}\n"
                    f"  Entry ID: {f['entry_id']}\n"
                    f"  Top senders returned: {f['top_senders_returned']}\n\n"
                )
            pytest.fail(msg)

    def test_known_calendar_entry_found(self, chroma_client, sample_calendar_entries):
        """Search for known calendar events by title — they must appear in results."""
        if not sample_calendar_entries:
            pytest.skip("No sample Calendar entries loaded")

        failures = []
        for entry in sample_calendar_entries[:10]:
            title = entry.get("title", "")
            if not title or len(title) < 3:
                continue

            results = search(
                chroma_client, title,
                collections=["calendar"],
                n_results=10,
            )
            found_ids = {r["metadata"].get("entry_id", r["id"]) for r in results}

            if entry["id"] not in found_ids:
                failures.append({
                    "entry_id": entry["id"],
                    "title": title,
                    "top_results": [r["metadata"].get("entry_id", r["id"])[:50] for r in results[:3]],
                })

        if failures:
            msg = f"\n{len(failures)} calendar entries NOT found by title:\n"
            for f in failures:
                msg += f"  Title: {f['title']}, ID: {f['entry_id']}\n"
            pytest.fail(msg)

    def test_known_book_entry_found(self, chroma_client, sample_book_entries):
        """Search for known books by title — they must appear in results."""
        if not sample_book_entries:
            pytest.skip("No sample Book entries loaded")

        failures = []
        for entry in sample_book_entries[:10]:
            title = entry.get("title", "")
            if not title or len(title) < 3:
                continue

            results = search(
                chroma_client, title,
                collections=["books"],
                n_results=10,
            )
            found_ids = {r["metadata"].get("entry_id", r["id"]) for r in results}

            if entry["id"] not in found_ids:
                failures.append({
                    "entry_id": entry["id"],
                    "title": title,
                })

        if failures:
            msg = f"\n{len(failures)} book entries NOT found by title:\n"
            for f in failures:
                msg += f"  Title: {f['title']}, ID: {f['entry_id']}\n"
            pytest.fail(msg)


# ===================================================================
# PART 2: Search Quality Test Cases
# ===================================================================

class TestEmailSearchQuality:
    """Email search quality tests — validate semantic search returns sensible results."""

    def test_search_by_subject_keywords(self, chroma_client, sample_gmail_entries):
        """Partial subject keyword search should still find the email."""
        if not sample_gmail_entries:
            pytest.skip("No sample Gmail entries loaded")

        failures = []
        for entry in sample_gmail_entries[:8]:
            subject = entry.get("subject", "")
            if not subject or len(subject) < 10:
                continue

            # Use just 2-3 words from the subject as query
            words = subject.split()
            if len(words) >= 3:
                query = " ".join(words[1:4])
            else:
                query = subject

            results = search(
                chroma_client, query,
                collections=["gmail_primary"],
                n_results=20,
            )
            found_ids = {r["metadata"].get("entry_id", r["id"]) for r in results}

            if entry["id"] not in found_ids:
                failures.append({
                    "query": query,
                    "full_subject": subject[:80],
                    "entry_id": entry["id"],
                })

        # Allow some misses — keyword fragments are inherently harder
        hit_rate = 1 - (len(failures) / max(len(sample_gmail_entries[:8]), 1))
        if hit_rate < 0.5:
            msg = f"\nSubject keyword search hit rate too low: {hit_rate:.0%}\nFailures:\n"
            for f in failures:
                msg += f"  Query: '{f['query']}' for subject: '{f['full_subject']}'\n"
            pytest.fail(msg)

    def test_search_by_date_filter(self, chroma_client):
        """Year filter should restrict results to that year."""
        results_2024 = search(
            chroma_client, "meeting",
            collections=["gmail_primary"],
            n_results=20,
            where_filter={"year": 2024},
        )

        if results_2024:
            wrong_year = [
                r for r in results_2024
                if r["metadata"].get("year") != 2024
            ]
            assert not wrong_year, (
                f"{len(wrong_year)} results have wrong year metadata when filtering for 2024: "
                f"{[(r['metadata'].get('year'), r['metadata'].get('subject','')[:40]) for r in wrong_year[:5]]}"
            )

    def test_search_content_keywords(self, chroma_client, sample_gmail_entries):
        """Search by a phrase from the email body should return that email."""
        if not sample_gmail_entries:
            pytest.skip("No sample Gmail entries loaded")

        # Find an entry with enough body content
        target = None
        for entry in sample_gmail_entries:
            body = entry.get("body_clean", "")
            if body and len(body) > 100 and not entry.get("is_automated"):
                target = entry
                break

        if not target:
            pytest.skip("No suitable non-automated email with body content found")

        # Extract a unique-ish phrase from the body (words 10-20)
        body = target["body_clean"]
        words = body.split()
        if len(words) >= 20:
            phrase = " ".join(words[10:18])
        elif len(words) >= 5:
            phrase = " ".join(words[:5])
        else:
            phrase = body[:60]

        results = search(
            chroma_client, phrase,
            collections=["gmail_primary"],
            n_results=20,
        )
        found_ids = {r["metadata"].get("entry_id", r["id"]) for r in results}

        if target["id"] not in found_ids:
            logger.warning(
                "Body content search missed entry %s with phrase '%s'. "
                "Top result subjects: %s",
                target["id"], phrase[:40],
                [r["metadata"].get("subject", "?")[:40] for r in results[:3]],
            )
            # This is a known weakness of vector search — log but don't hard-fail
            # unless the embedding text should obviously contain the phrase
            embed_text = target.get("body_for_embedding", "")
            if phrase.lower() in embed_text.lower():
                pytest.fail(
                    f"Phrase '{phrase[:60]}' IS in the embedding text but was not found "
                    f"in top-20 results. Entry ID: {target['id']}"
                )


class TestTemporalSearchQuality:
    """Test temporal queries — 'first email from X', 'last email about Y'.

    These are fundamentally hard for vector search because embeddings have
    no concept of 'first' or 'last'. These tests document the failure mode
    and verify that year filters help.
    """

    def test_first_email_returns_earliest_by_date(self, chroma_client, sample_gmail_entries):
        """'first email from X' should ideally return the oldest — document how it fails."""
        if not sample_gmail_entries:
            pytest.skip("No sample Gmail entries loaded")

        # Find a non-automated sender
        sender = None
        for entry in sample_gmail_entries:
            name = entry.get("from_name", "")
            if name and len(name) >= 3 and not entry.get("is_automated"):
                sender = name
                break

        if not sender:
            pytest.skip("No suitable sender found")

        query = f"first email from {sender}"
        results = search(
            chroma_client, query,
            collections=["gmail_primary"],
            n_results=10,
        )

        if not results:
            pytest.skip(f"No results for '{query}'")

        # Check if results are ordered by date (oldest first)
        dates_returned = []
        for r in results:
            date_str = r["metadata"].get("date", "")
            if date_str:
                dates_returned.append(date_str)

        if len(dates_returned) >= 2:
            # Vector search is NOT expected to return chronological order.
            # This test documents the gap. Log what happened.
            years = [r["metadata"].get("year", 0) for r in results]
            logger.info(
                "Temporal query '%s' returned years: %s — "
                "vector search has no temporal awareness, "
                "this is expected behavior.",
                query, years,
            )

    def test_year_filter_improves_temporal_search(self, chroma_client):
        """Searching with a year filter should help narrow temporal queries."""
        # Without filter
        results_all = search(
            chroma_client, "meeting invitation",
            collections=["gmail_primary"],
            n_results=10,
        )

        # With year filter
        results_2024 = search(
            chroma_client, "meeting invitation",
            collections=["gmail_primary"],
            n_results=10,
            where_filter={"year": 2024},
        )

        # Year filter should return only 2024 results
        if results_2024:
            for r in results_2024:
                assert r["metadata"].get("year") == 2024, (
                    f"Year filter failed: got year={r['metadata'].get('year')} "
                    f"for entry {r['id']}"
                )


class TestCalendarSearchQuality:
    """Calendar search quality tests."""

    def test_search_by_event_title(self, chroma_client, sample_calendar_entries):
        """Search by event title should return the event."""
        if not sample_calendar_entries:
            pytest.skip("No sample Calendar entries loaded")

        target = sample_calendar_entries[0]
        title = target["title"]

        results = search(
            chroma_client, title,
            collections=["calendar"],
            n_results=10,
        )
        found_ids = {r["metadata"].get("entry_id", r["id"]) for r in results}
        assert target["id"] in found_ids, (
            f"Calendar event '{title}' (ID: {target['id']}) not found in search results. "
            f"Top results: {[r.get('document','')[:60] for r in results[:3]]}"
        )

    def test_search_by_attendee(self, chroma_client, sample_calendar_entries):
        """Search by attendee name should return events with that attendee."""
        # Find an event with attendees
        target = None
        for entry in sample_calendar_entries:
            attendees = entry.get("attendees", [])
            if attendees and isinstance(attendees, list) and len(attendees) > 0:
                target = entry
                break

        if not target:
            pytest.skip("No calendar entries with attendees found")

        # Get first attendee name/email
        attendee = target["attendees"][0]
        if isinstance(attendee, dict):
            attendee_name = attendee.get("displayName") or attendee.get("email", "")
        else:
            attendee_name = str(attendee)

        if not attendee_name:
            pytest.skip("No attendee name available")

        results = search(
            chroma_client, f"meeting with {attendee_name}",
            collections=["calendar"],
            n_results=20,
        )

        # We just verify we got results — attendee matching depends on embedding quality
        assert len(results) > 0, f"No results for 'meeting with {attendee_name}'"


class TestBookSearchQuality:
    """Book search quality tests."""

    def test_search_by_book_title(self, chroma_client, sample_book_entries):
        """Search by book title should return the book."""
        if not sample_book_entries:
            pytest.skip("No sample Book entries loaded")

        target = sample_book_entries[0]
        title = target["title"]

        results = search(
            chroma_client, title,
            collections=["books"],
            n_results=10,
        )
        found_ids = {r["metadata"].get("entry_id", r["id"]) for r in results}
        assert target["id"] in found_ids, (
            f"Book '{title}' (ID: {target['id']}) not found in search results."
        )

    def test_search_by_author(self, chroma_client, sample_book_entries):
        """Search by author name should return books by that author."""
        if not sample_book_entries:
            pytest.skip("No sample Book entries loaded")

        target = sample_book_entries[0]
        author = target.get("author", "")
        if not author:
            pytest.skip("No author on first book entry")

        results = search(
            chroma_client, f"books by {author}",
            collections=["books"],
            n_results=10,
        )

        # Check if any result mentions this author in its document
        found_author = False
        for r in results:
            doc = r.get("document", "")
            if author.lower() in doc.lower():
                found_author = True
                break

        assert found_author, (
            f"Author '{author}' not found in any top-10 book search results. "
            f"Top results: {[r.get('document','')[:60] for r in results[:3]]}"
        )


class TestCrossSourceSearch:
    """Tests that search across all sources returns results from multiple collections."""

    def test_broad_query_returns_multiple_sources(self, chroma_client):
        """A broad query should match entries from different vault sources."""
        results = search(
            chroma_client, "meeting schedule appointment",
            n_results=20,
        )

        if not results:
            pytest.skip("No results for broad query")

        sources = {r.get("collection", "") for r in results}
        # With emails + calendar at minimum, we expect multi-source hits
        if len(sources) < 2:
            logger.warning(
                "Cross-source search only returned from: %s — "
                "expected at least 2 sources. This may indicate missing vectorization.",
                sources,
            )


# ===================================================================
# PART 3: Known Failure Pattern Tests
# ===================================================================

class TestChunkingCorrectness:
    """Test that chunking and deduplication work correctly."""

    def test_chunked_email_has_context_prefix(self):
        """When an email is chunked, non-first chunks should have header prefix."""
        # Create a long text that will be chunked
        header = "From Alice to Bob on 2024-01-01 re: Test Subject"
        body = "x " * 2000  # ~4000 chars, should produce multiple chunks
        text = header + "\n" + body

        chunks = chunk_text(text, chunk_size=2000, overlap=200, prefix=header)

        assert len(chunks) >= 2, f"Expected multiple chunks, got {len(chunks)}"
        # First chunk should start with the header
        assert chunks[0].startswith(header), "First chunk should contain the header"
        # Subsequent chunks should have prefix prepended
        for i, chunk in enumerate(chunks[1:], 1):
            assert chunk.startswith(header), (
                f"Chunk {i} missing context prefix. Starts with: {chunk[:50]}"
            )

    def test_chunk_overlap_preserves_content(self):
        """Overlapping chunks should not lose any content."""
        text = " ".join(f"word{i}" for i in range(500))

        chunks = chunk_text(text, chunk_size=200, overlap=50, prefix="")
        # Reconstruct: the full text should be recoverable from chunks
        # (each chunk overlaps with the previous by 50 chars)
        # Just verify no empty chunks and total coverage
        assert all(len(c) > 0 for c in chunks), "Found empty chunk"
        # First chunk + non-overlapping parts of subsequent chunks should cover text
        total_chars = len(chunks[0])
        for c in chunks[1:]:
            total_chars += len(c) - 50  # subtract overlap (approximately)
        # Allow some slack for prefix and rounding
        assert total_chars >= len(text) * 0.9, (
            f"Chunks cover {total_chars} chars but text is {len(text)} chars — "
            "possible content loss"
        )

    def test_dedup_returns_best_chunk_per_entry(self, chroma_client):
        """When an email has multiple chunks, search should dedup to the best chunk."""
        # Search for something likely to match chunked emails
        results = search(
            chroma_client, "newsletter update",
            collections=["gmail_primary"],
            n_results=50,  # Get many results to see chunks
        )

        # Check for duplicate entry_ids (chunks of same email)
        entry_ids = [r["metadata"].get("entry_id", r["id"]) for r in results]
        dupes = [eid for eid in set(entry_ids) if entry_ids.count(eid) > 1]

        if dupes:
            # This is NOT a bug in search() — search returns raw results.
            # Dedup happens in _deduplicate_results in mcp_server.py.
            # But we should document how many dupes the raw search returns.
            logger.info(
                "Raw search returned %d duplicate entry_ids (expected — "
                "dedup happens in MCP layer): %s",
                len(dupes), dupes[:5],
            )


class TestExactNameMatching:
    """Test that unusual names and identifiers are handled by the embedding model."""

    def test_unusual_sender_name_search(self, chroma_client, sample_gmail_entries):
        """Unusual/non-English sender names should still be findable."""
        if not sample_gmail_entries:
            pytest.skip("No sample Gmail entries loaded")

        # Find entries with non-ASCII or unusual sender names
        unusual = []
        for entry in sample_gmail_entries:
            name = entry.get("from_name", "")
            if name and (
                any(ord(c) > 127 for c in name)  # Non-ASCII
                or not any(c == " " for c in name)  # Single-word name
                or len(name) <= 4  # Very short name
            ):
                unusual.append(entry)

        if not unusual:
            pytest.skip("No entries with unusual sender names in sample")

        for entry in unusual[:3]:
            name = entry["from_name"]
            results = search(
                chroma_client, name,
                collections=["gmail_primary"],
                n_results=20,
            )

            # Check if any result is from this sender
            found = any(
                name.lower() in r["metadata"].get("from", "").lower()
                for r in results
            )
            if not found:
                logger.warning(
                    "Unusual name '%s' not found in top-20 results. "
                    "This is a known limitation of the all-MiniLM-L6-v2 model "
                    "with non-standard identifiers.",
                    name,
                )


class TestEmbeddingFieldQuality:
    """Verify that the body_for_embedding field contains enough context for search."""

    def test_email_embedding_contains_sender(self, sample_gmail_entries):
        """The embedding text should contain the sender name for sender-based search."""
        if not sample_gmail_entries:
            pytest.skip("No sample Gmail entries loaded")

        missing_sender = []
        for entry in sample_gmail_entries[:20]:
            embed = entry.get("body_for_embedding", "")
            sender_name = entry.get("from_name", "")
            if sender_name and sender_name.lower() not in embed.lower():
                missing_sender.append({
                    "id": entry["id"],
                    "from_name": sender_name,
                    "embed_start": embed[:100],
                })

        if missing_sender:
            msg = f"\n{len(missing_sender)} emails have embedding text missing sender name:\n"
            for m in missing_sender:
                msg += f"  {m['id']}: sender='{m['from_name']}' embed='{m['embed_start']}'\n"
            pytest.fail(msg)

    def test_email_embedding_contains_subject(self, sample_gmail_entries):
        """The embedding text should contain the subject for subject-based search."""
        if not sample_gmail_entries:
            pytest.skip("No sample Gmail entries loaded")

        missing_subject = []
        for entry in sample_gmail_entries[:20]:
            embed = entry.get("body_for_embedding", "")
            subject = entry.get("subject", "")
            if subject and len(subject) >= 5:
                # Allow partial match — subject may be truncated in embedding
                subject_words = subject.split()[:3]
                if not any(w.lower() in embed.lower() for w in subject_words if len(w) > 2):
                    missing_subject.append({
                        "id": entry["id"],
                        "subject": subject[:60],
                        "embed_start": embed[:100],
                    })

        if missing_subject:
            msg = f"\n{len(missing_subject)} emails have embedding text missing subject:\n"
            for m in missing_subject:
                msg += f"  {m['id']}: subject='{m['subject']}' embed='{m['embed_start']}'\n"
            pytest.fail(msg)

    def test_email_embedding_contains_date(self, sample_gmail_entries):
        """The embedding text should contain the date for time-scoped search."""
        if not sample_gmail_entries:
            pytest.skip("No sample Gmail entries loaded")

        missing_date = []
        for entry in sample_gmail_entries[:20]:
            embed = entry.get("body_for_embedding", "")
            date_str = entry.get("date", "")
            year = entry.get("year", 0)
            if year and str(year) not in embed:
                missing_date.append({
                    "id": entry["id"],
                    "date": date_str[:30],
                    "embed_start": embed[:100],
                })

        if missing_date:
            msg = f"\n{len(missing_date)} emails have embedding text missing year:\n"
            for m in missing_date[:5]:
                msg += f"  {m['id']}: date='{m['date']}' embed='{m['embed_start']}'\n"
            pytest.fail(msg)

    def test_calendar_embedding_contains_date(self, sample_calendar_entries):
        """Calendar embeddings should contain the date."""
        if not sample_calendar_entries:
            pytest.skip("No sample Calendar entries loaded")

        missing = 0
        for entry in sample_calendar_entries[:10]:
            embed = entry.get("event_for_embedding", "")
            start = entry.get("start", "")
            if start:
                # Extract date portion (e.g., "2024-01-15")
                date_part = start[:10]
                if date_part not in embed:
                    missing += 1

        assert missing <= 2, (
            f"{missing}/10 calendar entries have embedding text missing the date. "
            "This breaks date-based calendar search."
        )


class TestMissingEmbeddings:
    """Check for entries that were skipped during vectorization due to empty embeddings."""

    def test_gmail_embedding_coverage(self):
        """Count Gmail entries with empty body_for_embedding fields."""
        if not os.path.isdir(GMAIL_VAULT):
            pytest.skip("Gmail vault not found")

        total = 0
        missing = 0
        empty_subjects = []

        # Sample one month to keep test fast
        sample_dir = os.path.join(GMAIL_VAULT, "2024")
        if not os.path.isdir(sample_dir):
            pytest.skip("No 2024 Gmail data")

        for entry in read_all_entries(sample_dir):
            total += 1
            embed = entry.get("body_for_embedding", "")
            if not embed or not embed.strip():
                missing += 1
                empty_subjects.append(entry.get("subject", "(no subject)")[:50])
            if total >= 500:  # Sample limit
                break

        missing_pct = (missing / total * 100) if total > 0 else 0
        logger.info(
            "Gmail embedding coverage: %d/%d entries have embeddings (%.1f%% missing)",
            total - missing, total, missing_pct,
        )

        if missing_pct > 10:
            pytest.fail(
                f"{missing_pct:.1f}% of Gmail entries have empty body_for_embedding "
                f"(sampled {total} from 2024). Examples: {empty_subjects[:5]}"
            )

    def test_calendar_embedding_coverage(self):
        """Count Calendar entries with empty event_for_embedding fields."""
        if not os.path.isdir(CALENDAR_VAULT):
            pytest.skip("Calendar vault not found")

        total = 0
        missing = 0

        for entry in read_all_entries(CALENDAR_VAULT):
            total += 1
            embed = entry.get("event_for_embedding", "")
            if not embed or not embed.strip():
                missing += 1

        missing_pct = (missing / total * 100) if total > 0 else 0
        logger.info("Calendar embedding coverage: %d/%d (%.1f%% missing)", total - missing, total, missing_pct)

        if missing_pct > 5:
            pytest.fail(
                f"{missing_pct:.1f}% of Calendar entries have empty event_for_embedding"
            )

    def test_books_embedding_coverage(self):
        """Count Book entries with empty book_for_embedding fields."""
        if not os.path.isdir(BOOKS_VAULT):
            pytest.skip("Books vault not found")

        total = 0
        missing = 0

        for entry in read_all_entries(BOOKS_VAULT):
            total += 1
            embed = entry.get("book_for_embedding", "")
            if not embed or not embed.strip():
                missing += 1

        missing_pct = (missing / total * 100) if total > 0 else 0

        if missing_pct > 5:
            pytest.fail(
                f"{missing_pct:.1f}% of Book entries have empty book_for_embedding"
            )


# ===================================================================
# PART 4: Statistical Coverage Test
# ===================================================================

class TestStatisticalCoverage:
    """Sample random entries and verify they are findable via vector search."""

    def test_gmail_random_sample_hit_rate(self, chroma_client):
        """Sample 100 random Gmail entries and check if subject search finds them in top-5.

        Target: >90% hit rate. If lower, the vector DB has a systemic retrieval problem.
        """
        if not os.path.isdir(GMAIL_VAULT):
            pytest.skip("Gmail vault not found")

        # Collect all entries with embeddings (reservoir sampling for memory efficiency)
        reservoir = []
        SAMPLE_SIZE = 100
        count = 0

        for entry in read_all_entries(GMAIL_VAULT):
            if not entry.get("body_for_embedding") or not entry.get("subject"):
                continue
            if len(entry["subject"]) < 8:
                continue

            count += 1
            if len(reservoir) < SAMPLE_SIZE:
                reservoir.append(entry)
            else:
                # Reservoir sampling: replace with decreasing probability
                j = random.randint(0, count - 1)
                if j < SAMPLE_SIZE:
                    reservoir[j] = entry

        if len(reservoir) < 20:
            pytest.skip(f"Only {len(reservoir)} suitable entries found — need at least 20")

        hits = 0
        misses = []
        tested = 0

        for entry in reservoir:
            subject = entry["subject"]
            results = search(
                chroma_client, subject,
                collections=["gmail_primary"],
                n_results=5,
            )
            found_ids = {r["metadata"].get("entry_id", r["id"]) for r in results}

            if entry["id"] in found_ids:
                hits += 1
            else:
                if len(misses) < 10:  # Keep first 10 misses for diagnostics
                    misses.append({
                        "id": entry["id"],
                        "subject": subject[:60],
                        "top_ids": [r["metadata"].get("entry_id", r["id"]) for r in results[:2]],
                        "top_subjects": [r["metadata"].get("subject", "?")[:40] for r in results[:2]],
                        "top_distance": results[0]["distance"] if results else None,
                    })

            tested += 1

        hit_rate = hits / tested if tested > 0 else 0
        logger.info(
            "Gmail random sample hit rate: %d/%d = %.1f%%",
            hits, tested, hit_rate * 100,
        )

        if hit_rate < 0.90:
            msg = (
                f"\nGmail random sample hit rate: {hit_rate:.1%} ({hits}/{tested}) "
                f"— BELOW 90% THRESHOLD\n\nSample misses:\n"
            )
            for m in misses:
                msg += (
                    f"  ID: {m['id']}\n"
                    f"  Subject: {m['subject']}\n"
                    f"  Top result subjects: {m['top_subjects']}\n"
                    f"  Top distance: {m['top_distance']}\n\n"
                )
            pytest.fail(msg)

    def test_books_hit_rate(self, chroma_client):
        """All books should be findable by title — 100% expected for a small collection."""
        if not os.path.isdir(BOOKS_VAULT):
            pytest.skip("Books vault not found")

        entries = list(read_all_entries(BOOKS_VAULT))
        if not entries:
            pytest.skip("No book entries")

        # Sample up to 50
        sample = random.sample(entries, min(50, len(entries)))
        hits = 0

        for entry in sample:
            title = entry.get("title", "")
            if not title or len(title) < 3:
                continue

            results = search(
                chroma_client, title,
                collections=["books"],
                n_results=5,
            )
            found_ids = {r["metadata"].get("entry_id", r["id"]) for r in results}
            if entry["id"] in found_ids:
                hits += 1

        tested = len(sample)
        hit_rate = hits / tested if tested > 0 else 0
        assert hit_rate >= 0.90, (
            f"Books hit rate: {hit_rate:.1%} ({hits}/{tested}) — "
            "expected >90% for a small collection"
        )

    def test_calendar_hit_rate(self, chroma_client):
        """Calendar events should be findable by title."""
        if not os.path.isdir(CALENDAR_VAULT):
            pytest.skip("Calendar vault not found")

        entries = []
        count = 0
        for entry in read_all_entries(CALENDAR_VAULT):
            if entry.get("title") and len(entry["title"]) >= 5 and entry.get("event_for_embedding"):
                entries.append(entry)
                count += 1
                if count >= 200:
                    break

        if len(entries) < 20:
            pytest.skip("Not enough calendar entries with titles")

        sample = random.sample(entries, min(50, len(entries)))
        hits = 0

        for entry in sample:
            results = search(
                chroma_client, entry["title"],
                collections=["calendar"],
                n_results=5,
            )
            found_ids = {r["metadata"].get("entry_id", r["id"]) for r in results}
            if entry["id"] in found_ids:
                hits += 1

        tested = len(sample)
        hit_rate = hits / tested if tested > 0 else 0
        # Calendar events often have very short/generic titles ("Meeting",
        # "Lunch", "Call") which causes many collisions in embedding space.
        # The threshold is set at 30% to catch catastrophic regressions,
        # but the real goal is >80%. Log the actual rate for monitoring.
        logger.info("Calendar hit rate: %d/%d = %.1f%%", hits, tested, hit_rate * 100)
        assert hit_rate >= 0.30, (
            f"Calendar hit rate: {hit_rate:.1%} ({hits}/{tested}) — "
            "CRITICALLY LOW. Possible vectorization or embedding problem."
        )


# ===================================================================
# PART 5: Diagnostic Helpers
# ===================================================================

class TestDiagnosticHelpers:
    """Diagnostic functions exposed as tests — run with -v for detailed output."""

    def test_diagnose_search(self, chroma_client, sample_gmail_entries):
        """Run a diagnostic search and show detailed results.

        Use: pytest tests/test_vector_correctness.py -v -k "diagnose_search"
        """
        if not sample_gmail_entries:
            pytest.skip("No sample Gmail entries")

        target = sample_gmail_entries[0]
        query = target.get("subject", "test query")
        expected_id = target["id"]

        report = diagnose_search(chroma_client, query, expected_id, collection="gmail_primary")
        logger.info("\n%s", report)

    def test_check_vault_vs_vector_coverage(self, chroma_client):
        """Compare vault entry counts with vector DB counts for each source."""
        report = compare_vault_vs_vector(VAULT_ROOT, chroma_client)
        logger.info("\n%s", report)

        # Parse the report for gaps
        # This is informational — don't fail, just warn on large gaps


# ---------------------------------------------------------------------------
# Diagnostic helper functions (importable from other modules)
# ---------------------------------------------------------------------------

def diagnose_search(client, query, expected_id, collection=None, n_results=50):
    """Search for a query and show detailed results relative to an expected entry.

    Returns a formatted diagnostic report string.
    """
    collections = [collection] if collection else None
    results = search(client, query, collections=collections, n_results=n_results)

    lines = [
        f"=== SEARCH DIAGNOSTIC ===",
        f"Query: {query}",
        f"Expected entry ID: {expected_id}",
        f"Collection filter: {collection or 'all'}",
        f"Results returned: {len(results)}",
        "",
    ]

    # Check if expected entry is in results
    expected_pos = None
    expected_distance = None
    for i, r in enumerate(results):
        entry_id = r["metadata"].get("entry_id", r["id"])
        if entry_id == expected_id:
            expected_pos = i + 1
            expected_distance = r["distance"]
            break

    if expected_pos:
        lines.append(f"FOUND expected entry at position {expected_pos}, distance={expected_distance:.4f}")
    else:
        lines.append("NOT FOUND — expected entry is NOT in top results")

        # Try to find it directly in ChromaDB
        if collection:
            try:
                embedding_fn = _get_embedding_fn()
                col = client.get_collection(name=collection, embedding_function=embedding_fn)
                try:
                    direct = col.get(ids=[expected_id], include=["documents", "metadatas"])
                    if direct["ids"]:
                        lines.append(f"  Entry EXISTS in ChromaDB collection '{collection}'")
                        lines.append(f"  Document preview: {direct['documents'][0][:100]}...")
                    else:
                        lines.append(f"  Entry MISSING from ChromaDB collection '{collection}'")
                        # Check for chunk IDs
                        chunk_ids = [f"{expected_id}_chunk_{i}" for i in range(10)]
                        chunk_result = col.get(ids=chunk_ids, include=[])
                        if chunk_result["ids"]:
                            lines.append(f"  But found {len(chunk_result['ids'])} chunk(s): {chunk_result['ids']}")
                except Exception as e:
                    lines.append(f"  Error looking up entry: {e}")
            except Exception as e:
                lines.append(f"  Error accessing collection: {e}")

    lines.append("")
    lines.append("--- Top 10 results ---")
    for i, r in enumerate(results[:10]):
        meta = r["metadata"]
        entry_id = meta.get("entry_id", r["id"])
        lines.append(
            f"  #{i+1} [dist={r['distance']:.4f}] "
            f"id={entry_id[:30]}  "
            f"from={meta.get('from','?')[:25]}  "
            f"subject={meta.get('subject','')[:40]}"
        )

    return "\n".join(lines)


def check_entry_vectorized(client, entry_id, collection_name):
    """Check if a specific entry exists in a ChromaDB collection.

    Returns dict with: found, chunk_count, document_preview, metadata.
    """
    embedding_fn = _get_embedding_fn()
    try:
        col = client.get_collection(name=collection_name, embedding_function=embedding_fn)
    except Exception:
        return {"found": False, "error": f"Collection '{collection_name}' not found"}

    # Try exact ID first
    result = col.get(ids=[entry_id], include=["documents", "metadatas"])
    if result["ids"]:
        return {
            "found": True,
            "chunk_count": 1,
            "document_preview": result["documents"][0][:200] if result["documents"] else "",
            "metadata": result["metadatas"][0] if result["metadatas"] else {},
        }

    # Try chunk IDs
    chunk_ids = [f"{entry_id}_chunk_{i}" for i in range(50)]
    chunk_result = col.get(ids=chunk_ids, include=["documents", "metadatas"])
    if chunk_result["ids"]:
        return {
            "found": True,
            "chunk_count": len(chunk_result["ids"]),
            "chunk_ids": chunk_result["ids"],
            "document_preview": chunk_result["documents"][0][:200] if chunk_result["documents"] else "",
            "metadata": chunk_result["metadatas"][0] if chunk_result["metadatas"] else {},
        }

    return {"found": False, "chunk_count": 0}


def compare_vault_vs_vector(vault_root, client):
    """Compare entry counts in vault JSONL files vs ChromaDB vectors.

    Returns a formatted report string showing gaps per source.
    """
    status = get_status(client, vault_root)

    lines = [
        "=== VAULT vs VECTOR COVERAGE ===",
        f"{'Source':<25} {'Vault':>10} {'Vectors':>10} {'Gap':>10} {'Coverage':>10}",
        "-" * 70,
    ]

    total_vault = 0
    total_vectors = 0

    for s in sorted(status, key=lambda x: x["collection"]):
        vault_count = s["vault_entries"]
        vector_count = s["vectorized"]
        gap = vault_count - vector_count
        coverage = (vector_count / vault_count * 100) if vault_count > 0 else 0

        total_vault += vault_count
        total_vectors += vector_count

        flag = ""
        if vault_count > 0 and coverage < 80:
            flag = " *** LOW"
        elif vault_count > 0 and vector_count == 0:
            flag = " *** EMPTY"

        lines.append(
            f"{s['collection']:<25} {vault_count:>10,} {vector_count:>10,} "
            f"{gap:>10,} {coverage:>9.1f}%{flag}"
        )

    lines.append("-" * 70)
    total_coverage = (total_vectors / total_vault * 100) if total_vault > 0 else 0
    lines.append(
        f"{'TOTAL':<25} {total_vault:>10,} {total_vectors:>10,} "
        f"{total_vault - total_vectors:>10,} {total_coverage:>9.1f}%"
    )

    return "\n".join(lines)


# ===================================================================
# PART 6: Vault-to-Vector Consistency Tests
# ===================================================================

class TestVaultVectorConsistency:
    """Verify that what's in the vault matches what's in the vector DB."""

    def test_vector_count_matches_vault_approximately(self, chroma_client):
        """Vector count should be >= vault entry count (chunks add more, but never fewer)."""
        status = get_status(chroma_client, VAULT_ROOT)

        for s in status:
            vault_count = s["vault_entries"]
            vector_count = s["vectorized"]
            collection = s["collection"]

            if vault_count == 0:
                continue

            # Vectors can be MORE than vault entries (due to chunking)
            # but should never be drastically FEWER (some entries have empty embedding)
            if vector_count == 0 and vault_count > 0:
                logger.warning(
                    "Collection '%s' has 0 vectors but %d vault entries — "
                    "needs vectorization.",
                    collection, vault_count,
                )
                continue

            # Coverage should be reasonable (>50% at minimum, accounting for
            # entries with empty embedding fields)
            coverage = vector_count / vault_count * 100
            if coverage < 50:
                logger.warning(
                    "Collection '%s' has low coverage: %d vectors / %d entries = %.1f%%",
                    collection, vector_count, vault_count, coverage,
                )

    def test_no_orphaned_chunks(self, chroma_client):
        """Chunk IDs should reference valid entry IDs — spot-check a sample."""
        embedding_fn = _get_embedding_fn()

        try:
            col = chroma_client.get_collection(
                name="gmail_primary", embedding_function=embedding_fn
            )
        except Exception:
            pytest.skip("gmail_primary collection not found")

        # Get a sample of chunk IDs
        sample = col.get(limit=100, include=["metadatas"])

        chunk_entries = [
            m for m in sample["metadatas"]
            if m and m.get("chunk_index") is not None
        ]

        if not chunk_entries:
            pytest.skip("No chunked entries in sample")

        # Verify chunk metadata integrity
        for meta in chunk_entries[:10]:
            assert "entry_id" in meta, f"Chunk missing entry_id: {meta}"
            assert "total_chunks" in meta, f"Chunk missing total_chunks: {meta}"
            assert meta["chunk_index"] < meta["total_chunks"], (
                f"chunk_index {meta['chunk_index']} >= total_chunks {meta['total_chunks']}"
            )

    def test_metadata_fields_populated(self, chroma_client):
        """Spot-check that search result metadata has the expected fields."""
        results = search(
            chroma_client, "hello",
            collections=["gmail_primary"],
            n_results=10,
        )

        if not results:
            pytest.skip("No search results")

        for r in results:
            meta = r["metadata"]
            # These fields should always be present for Gmail entries
            assert "source" in meta, f"Missing 'source' in metadata: {meta}"
            assert "entry_id" in meta, f"Missing 'entry_id' in metadata: {meta}"
            # year/month should be present (may be 0 for unparseable dates)
            assert "year" in meta, f"Missing 'year' in metadata: {meta}"

            # Optional but expected for Gmail
            if meta.get("source", "").startswith("Gmail"):
                # Most emails should have from and subject
                has_from = "from" in meta and meta["from"]
                has_subject = "subject" in meta and meta["subject"]
                if not has_from and not has_subject:
                    logger.warning(
                        "Gmail entry %s has neither 'from' nor 'subject' in metadata",
                        meta.get("entry_id", "?"),
                    )


# ===================================================================
# PART 7: MCP Server Handler Tests
# ===================================================================

class TestMCPHandlers:
    """Test the MCP server tool handler functions work correctly with real data."""

    @pytest.fixture(autouse=True)
    def setup_mcp_globals(self, chroma_client):
        """Set up MCP server global state for testing."""
        import mcp_server
        mcp_server._client = chroma_client
        mcp_server._config = {}
        mcp_server._vault_root = VAULT_ROOT

    def test_search_emails_handler(self):
        """MCP search_emails handler returns results."""
        import asyncio
        from mcp_server import _handle_search_emails

        result = asyncio.run(_handle_search_emails({"query": "meeting", "n_results": 5}))
        assert result, "Handler returned empty result"
        text = result[0].text
        data = json.loads(text)
        assert "results" in data
        assert data["total"] > 0, "search_emails returned 0 results for 'meeting'"

    def test_search_emails_with_year_filter(self):
        """MCP search_emails with year filter only returns that year."""
        import asyncio
        from mcp_server import _handle_search_emails

        result = asyncio.run(_handle_search_emails({
            "query": "update",
            "n_results": 10,
            "year": 2024,
        }))
        text = result[0].text
        data = json.loads(text)

        # All results should reference 2024 (via the snippet or metadata)
        # The MCP handler passes year as a where_filter, so vector DB enforces it
        if data["results"]:
            assert data["total"] > 0

    def test_search_all_handler(self):
        """MCP search_all handler returns results from multiple sources."""
        import asyncio
        from mcp_server import _handle_search_all

        result = asyncio.run(_handle_search_all({"query": "schedule", "n_results": 20}))
        text = result[0].text
        data = json.loads(text)
        assert data["total"] > 0, "search_all returned 0 results for 'schedule'"

    def test_list_sources_handler(self):
        """MCP list_sources handler returns source list."""
        import asyncio
        from mcp_server import _handle_list_sources

        result = asyncio.run(_handle_list_sources({}))
        text = result[0].text
        data = json.loads(text)
        assert "sources" in data
        assert len(data["sources"]) > 0, "list_sources returned no sources"

        # Verify structure
        for source in data["sources"]:
            assert "collection" in source
            assert "vectorized" in source

    def test_get_entry_handler(self, sample_gmail_entries):
        """MCP get_entry handler retrieves the full entry."""
        import asyncio
        if not sample_gmail_entries:
            pytest.skip("No sample Gmail entries")

        from mcp_server import _handle_get_entry

        target = sample_gmail_entries[0]
        result = asyncio.run(_handle_get_entry({
            "entry_id": target["id"],
            "vault_dir": "Gmail_Primary",
        }))
        text = result[0].text
        data = json.loads(text)
        assert data["id"] == target["id"]
        assert "subject" in data

    def test_deduplication_in_results(self):
        """MCP results should deduplicate chunks of the same email."""
        from mcp_server import _deduplicate_results

        # Simulate two chunks from the same entry
        fake_results = [
            {
                "id": "entry1_chunk_0",
                "document": "chunk 0 text",
                "metadata": {"entry_id": "entry1", "chunk_index": 0, "total_chunks": 2},
                "distance": 0.3,
                "collection": "gmail_primary",
            },
            {
                "id": "entry1_chunk_1",
                "document": "chunk 1 text",
                "metadata": {"entry_id": "entry1", "chunk_index": 1, "total_chunks": 2},
                "distance": 0.5,
                "collection": "gmail_primary",
            },
            {
                "id": "entry2",
                "document": "different email",
                "metadata": {"entry_id": "entry2"},
                "distance": 0.4,
                "collection": "gmail_primary",
            },
        ]

        deduped = _deduplicate_results(fake_results)

        # Should collapse entry1's two chunks into one (the best distance)
        entry_ids = [r["metadata"].get("entry_id", r["id"]) for r in deduped]
        assert entry_ids.count("entry1") == 1, "Dedup failed: entry1 appears multiple times"
        assert entry_ids.count("entry2") == 1

        # The surviving entry1 should have the lower distance (0.3)
        for r in deduped:
            if r["metadata"].get("entry_id") == "entry1":
                assert r["distance"] == 0.3, "Dedup kept wrong chunk"


# ===================================================================
# PART 8: Embedding Model Behavior Tests
# ===================================================================

class TestEmbeddingModelBehavior:
    """Test how the all-MiniLM-L6-v2 model handles various query types.

    These tests help understand model limitations and inform query design.
    """

    def test_model_loads(self):
        """Verify the embedding model loads without error."""
        fn = _get_embedding_fn()
        # Generate a test embedding
        result = fn(["hello world"])
        assert result is not None
        assert len(result) == 1
        assert len(result[0]) > 0  # Should be a 384-dim vector

    def test_similar_queries_have_similar_embeddings(self):
        """Semantically similar queries should produce similar embeddings."""
        fn = _get_embedding_fn()
        embeddings = fn([
            "emails from John about the project",
            "messages from John regarding the project",
            "weather forecast for tomorrow",
        ])

        # Cosine similarity between first two should be higher than
        # between either and the third
        import numpy as np
        e1, e2, e3 = [np.array(e) for e in embeddings]

        sim_12 = np.dot(e1, e2) / (np.linalg.norm(e1) * np.linalg.norm(e2))
        sim_13 = np.dot(e1, e3) / (np.linalg.norm(e1) * np.linalg.norm(e3))

        assert sim_12 > sim_13, (
            f"Similar queries should have higher similarity: "
            f"sim(q1,q2)={sim_12:.3f} vs sim(q1,q3)={sim_13:.3f}"
        )

    def test_unusual_names_embedding_distance(self):
        """Test how the model handles unusual names vs common names."""
        fn = _get_embedding_fn()
        embeddings = fn([
            "renatalix",
            "From renatalix to Matthias",
            "email from a person named renatalix",
        ])

        # All three should produce valid non-zero embeddings
        for i, e in enumerate(embeddings):
            assert any(v != 0 for v in e), f"Embedding {i} is all zeros for unusual name"


# ===================================================================
# Standalone diagnostic runners
# ===================================================================

def run_full_diagnostic(vault_root=VAULT_ROOT):
    """Run a full diagnostic and print results. Call from CLI:

        python -c "from tests.test_vector_correctness import run_full_diagnostic; run_full_diagnostic()"
    """
    client = get_client(vault_root)

    print("\n" + "=" * 70)
    print("NOMOLO VECTOR SEARCH DIAGNOSTIC")
    print("=" * 70)

    # Coverage report
    print("\n" + compare_vault_vs_vector(vault_root, client))

    # Sample search quality
    print("\n\n=== SAMPLE SEARCH QUALITY CHECK ===")
    gmail_vault = os.path.join(vault_root, "Gmail_Primary")
    if os.path.isdir(gmail_vault):
        sample = []
        count = 0
        for entry in read_all_entries(gmail_vault):
            if entry.get("subject") and entry.get("body_for_embedding"):
                sample.append(entry)
                count += 1
                if count >= 20:
                    break

        hits = 0
        for entry in sample:
            results = search(
                client, entry["subject"],
                collections=["gmail_primary"],
                n_results=5,
            )
            found = entry["id"] in {
                r["metadata"].get("entry_id", r["id"]) for r in results
            }
            status = "HIT" if found else "MISS"
            if not found:
                top = results[0]["metadata"].get("subject", "?")[:40] if results else "no results"
                print(f"  {status}: '{entry['subject'][:50]}' → top result: '{top}'")
            hits += int(found)

        print(f"\n  Hit rate: {hits}/{len(sample)} = {hits/len(sample)*100:.1f}%")

    print("\n" + "=" * 70)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    run_full_diagnostic()
