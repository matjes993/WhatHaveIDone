"""Tests for core/vectordb.py — ChromaDB vector storage."""

import json
import os
import tempfile
import unittest

from core.vectordb import (
    chunk_text,
    detect_embedding_field,
    get_client,
    get_or_create_collection,
    get_status,
    search,
    vectorize_vault,
    get_full_entry,
)
from core.vault import flush_entries


class TestDetectEmbeddingField(unittest.TestCase):
    def test_direct_match(self):
        assert detect_embedding_field("Gmail") == "body_for_embedding"
        assert detect_embedding_field("Contacts") == "contact_for_embedding"
        assert detect_embedding_field("Notes") == "note_for_embedding"

    def test_suffix_strip(self):
        assert detect_embedding_field("Gmail_Primary") == "body_for_embedding"
        assert detect_embedding_field("Gmail_Work") == "body_for_embedding"

    def test_unknown(self):
        assert detect_embedding_field("Unknown_Vault") is None

    def test_all_known_vaults(self):
        known = [
            "Gmail", "Contacts", "Books", "YouTube", "Music", "Finance",
            "Shopping", "Notes", "Podcasts", "Health", "Browser", "Calendar", "Maps",
        ]
        for v in known:
            assert detect_embedding_field(v) is not None, f"Missing: {v}"


class TestChunkText(unittest.TestCase):
    def test_short_text(self):
        chunks = chunk_text("Hello world", chunk_size=100)
        assert len(chunks) == 1
        assert chunks[0] == "Hello world"

    def test_empty_text(self):
        assert chunk_text("") == []
        assert chunk_text(None) == []

    def test_exact_size(self):
        text = "x" * 100
        chunks = chunk_text(text, chunk_size=100)
        assert len(chunks) == 1

    def test_split_needed(self):
        text = "a" * 300
        chunks = chunk_text(text, chunk_size=100, overlap=20)
        assert len(chunks) > 1
        # Each chunk should be <= chunk_size (plus optional prefix)
        for c in chunks:
            assert len(c) <= 100

    def test_overlap(self):
        text = "a" * 250
        chunks = chunk_text(text, chunk_size=100, overlap=50)
        # With overlap, chunks should share content
        assert len(chunks) >= 3

    def test_prefix_on_subsequent_chunks(self):
        text = "a" * 300
        chunks = chunk_text(text, chunk_size=100, overlap=20, prefix="Subject: Test")
        # First chunk has no prefix modification
        assert chunks[0] == "a" * 100
        # Subsequent chunks get the prefix
        assert chunks[1].startswith("Subject: Test")


class TestVectorizeAndSearch(unittest.TestCase):
    """Integration tests using real ChromaDB (in-memory via temp dir)."""

    def test_vectorize_and_search(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            vault_root = os.path.join(tmpdir, "vaults")
            vault_path = os.path.join(vault_root, "Notes")

            # Create test entries
            entries = [
                {
                    "id": "notes:test1",
                    "title": "Meeting notes",
                    "body": "Discussed the new product launch",
                    "tags": ["work"],
                    "year": 2024,
                    "month": 3,
                    "note_for_embedding": "Meeting notes — Discussed the new product launch with the team",
                },
                {
                    "id": "notes:test2",
                    "title": "Grocery list",
                    "body": "Milk, eggs, bread",
                    "tags": ["personal"],
                    "year": 2024,
                    "month": 3,
                    "note_for_embedding": "Grocery list — Milk, eggs, bread, butter",
                },
                {
                    "id": "notes:test3",
                    "title": "Vacation planning",
                    "body": "Flight to Barcelona in June",
                    "tags": ["travel"],
                    "year": 2024,
                    "month": 5,
                    "note_for_embedding": "Vacation planning — Flight to Barcelona in June, hotel booked",
                },
            ]
            flush_entries(entries, vault_path, "notes.jsonl")

            client = get_client(vault_root)
            new, skipped, total = vectorize_vault(vault_path, "Notes", client)

            assert new == 3
            assert skipped == 0
            assert total == 3

            # Search
            results = search(client, "product launch meeting", n_results=2)
            assert len(results) > 0
            assert "product" in results[0]["document"].lower() or "meeting" in results[0]["document"].lower()

    def test_incremental_vectorize(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            vault_root = os.path.join(tmpdir, "vaults")
            vault_path = os.path.join(vault_root, "Notes")

            # First batch
            entries = [
                {"id": "notes:a1", "note_for_embedding": "First entry about Python"},
            ]
            flush_entries(entries, vault_path, "notes.jsonl")

            client = get_client(vault_root)
            new1, _, total1 = vectorize_vault(vault_path, "Notes", client)
            assert new1 == 1
            assert total1 == 1

            # Second batch (add one more)
            entries2 = [
                {"id": "notes:a2", "note_for_embedding": "Second entry about JavaScript"},
            ]
            flush_entries(entries2, vault_path, "notes.jsonl")

            new2, skipped2, total2 = vectorize_vault(vault_path, "Notes", client)
            assert new2 == 1  # Only the new one
            assert skipped2 == 1  # Old one skipped
            assert total2 == 2

    def test_force_revectorize(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            vault_root = os.path.join(tmpdir, "vaults")
            vault_path = os.path.join(vault_root, "Notes")

            entries = [
                {"id": "notes:f1", "note_for_embedding": "Force test entry"},
            ]
            flush_entries(entries, vault_path, "notes.jsonl")

            client = get_client(vault_root)
            vectorize_vault(vault_path, "Notes", client)

            # Force should re-process
            new, skipped, total = vectorize_vault(vault_path, "Notes", client, force=True)
            assert new == 1
            assert total == 1

    def test_empty_embedding_skipped(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            vault_root = os.path.join(tmpdir, "vaults")
            vault_path = os.path.join(vault_root, "Notes")

            entries = [
                {"id": "notes:e1", "note_for_embedding": ""},
                {"id": "notes:e2", "note_for_embedding": "Valid entry"},
                {"id": "notes:e3"},  # Missing field entirely
            ]
            flush_entries(entries, vault_path, "notes.jsonl")

            client = get_client(vault_root)
            new, skipped, total = vectorize_vault(vault_path, "Notes", client)
            assert new == 1
            assert skipped == 2
            assert total == 1

    def test_search_with_year_filter(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            vault_root = os.path.join(tmpdir, "vaults")
            vault_path = os.path.join(vault_root, "Notes")

            entries = [
                {"id": "notes:y1", "year": 2023, "note_for_embedding": "Old meeting about budget"},
                {"id": "notes:y2", "year": 2024, "note_for_embedding": "New meeting about budget"},
            ]
            flush_entries(entries, vault_path, "notes.jsonl")

            client = get_client(vault_root)
            vectorize_vault(vault_path, "Notes", client)

            results = search(client, "budget meeting", n_results=5, where_filter={"year": 2024})
            assert len(results) == 1
            assert "New" in results[0]["document"]


class TestGetFullEntry(unittest.TestCase):
    def test_get_existing_entry(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            vault_root = os.path.join(tmpdir, "vaults")
            vault_path = os.path.join(vault_root, "Notes")

            entries = [
                {"id": "notes:x1", "title": "Test", "note_for_embedding": "Test entry"},
            ]
            flush_entries(entries, vault_path, "notes.jsonl")

            result = get_full_entry(vault_root, "Notes", "notes:x1")
            assert result is not None
            assert result["title"] == "Test"

    def test_get_missing_entry(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            vault_root = os.path.join(tmpdir, "vaults")
            vault_path = os.path.join(vault_root, "Notes")
            os.makedirs(vault_path)

            result = get_full_entry(vault_root, "Notes", "notes:nonexistent")
            assert result is None


class TestGetStatus(unittest.TestCase):
    def test_status_with_data(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            vault_root = os.path.join(tmpdir, "vaults")
            vault_path = os.path.join(vault_root, "Notes")

            entries = [
                {"id": "notes:s1", "note_for_embedding": "Status test"},
            ]
            flush_entries(entries, vault_path, "notes.jsonl")

            client = get_client(vault_root)
            vectorize_vault(vault_path, "Notes", client)

            status = get_status(client, vault_root)
            assert len(status) == 1
            assert status[0]["vectorized"] == 1
            assert status[0]["vault_entries"] == 1

    def test_status_empty(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            vault_root = os.path.join(tmpdir, "vaults")
            os.makedirs(vault_root)

            client = get_client(vault_root)
            status = get_status(client, vault_root)
            assert len(status) == 0


if __name__ == "__main__":
    unittest.main()
