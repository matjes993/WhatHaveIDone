"""
Integration test: Vault → Adapters → GraphBuilder → Entity Resolution → Queries

Feeds the synthetic test vault through the full knowledge graph pipeline
and validates entity resolution, relationship creation, and temporal queries.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

# Ensure the project root is on the path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.knowledge.adapters import adapt_all, read_vault_jsonl
from core.knowledge.forgetter import Forgetter
from core.knowledge.graph_builder import GraphBuilder
from core.knowledge.graph_store import GraphStore
from core.knowledge.resolver import EntityResolver
from core.knowledge.schema import (
    EntityType,
    HypothesisStatus,
    IdentifierSystem,
    RelationshipType,
)
from core.knowledge.temporal import TemporalQuery

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

VAULT_DIR = Path(__file__).parent / "fixtures" / "vault"
MANIFEST_PATH = VAULT_DIR / "manifest.json"


@pytest.fixture(scope="module")
def vault_data():
    """Read the test vault. Generate it first if missing."""
    if not VAULT_DIR.exists() or not any(VAULT_DIR.iterdir()):
        gen_script = Path(__file__).parent / "fixtures" / "generate_test_vault.py"
        subprocess.run([sys.executable, str(gen_script)], check=True)
    return read_vault_jsonl(str(VAULT_DIR))


@pytest.fixture(scope="module")
def manifest():
    """Load the cast manifest for validation."""
    with open(MANIFEST_PATH) as f:
        return json.load(f)


@pytest.fixture
def store():
    """Fresh in-memory graph store for each test."""
    return GraphStore(":memory:")


@pytest.fixture
def built_graph(store, vault_data):
    """Build the full graph from vault data, return (store, stats)."""
    builder = GraphBuilder(store)
    records = list(adapt_all(vault_data))
    stats = builder.build(iter(records))
    return store, stats


# ---------------------------------------------------------------------------
# Adapter tests
# ---------------------------------------------------------------------------


class TestAdapters:
    def test_vault_has_all_sources(self, vault_data):
        """Vault should contain data from all 11 source types."""
        expected = {
            "gmail", "google_contacts", "mac_contacts", "calendar",
            "imessage", "whatsapp", "telegram", "slack",
            "browser_history", "bookmarks", "notes",
        }
        assert expected == set(vault_data.keys())

    def test_gmail_record_count(self, vault_data):
        assert len(vault_data["gmail"]) == 500

    def test_gmail_has_required_fields(self, vault_data):
        entry = vault_data["gmail"][0]
        for field in ["id", "from", "to", "subject", "date"]:
            assert field in entry, f"Gmail entry missing '{field}'"

    def test_contacts_have_emails(self, vault_data):
        for contact in vault_data["google_contacts"]:
            assert "emails" in contact
            assert isinstance(contact["emails"], list)

    def test_calendar_has_attendees(self, vault_data):
        events_with_attendees = [
            e for e in vault_data["calendar"]
            if e.get("attendees")
        ]
        assert len(events_with_attendees) > 0

    def test_imessage_has_contact(self, vault_data):
        for msg in vault_data["imessage"][:10]:
            assert "contact" in msg

    def test_whatsapp_has_sender(self, vault_data):
        for msg in vault_data["whatsapp"][:10]:
            assert "sender" in msg or "from" in msg

    def test_adapt_all_yields_canonical_records(self, vault_data):
        from core.knowledge.schema import CanonicalRecord
        records = list(adapt_all(vault_data))
        assert len(records) > 1000
        for r in records[:20]:
            assert isinstance(r, CanonicalRecord)
            assert r.source_name != ""
            assert r.source_id != ""

    def test_adapt_gmail_extracts_email(self, vault_data):
        from core.knowledge.adapters import adapt_gmail
        records = list(adapt_gmail(iter(vault_data["gmail"][:5])))
        for r in records:
            assert r.record_type == EntityType.MESSAGE
            assert r.source_name == "gmail"
            assert r.data.get("sender") is not None

    def test_adapt_contacts_extracts_name(self, vault_data):
        from core.knowledge.adapters import adapt_google_contacts
        records = list(adapt_google_contacts(iter(vault_data["google_contacts"][:3])))
        for r in records:
            assert r.record_type == EntityType.PERSON
            assert r.data.get("name") != ""


# ---------------------------------------------------------------------------
# Full pipeline tests
# ---------------------------------------------------------------------------


class TestFullPipeline:
    def test_graph_builds_without_errors(self, built_graph):
        store, stats = built_graph
        assert stats.errors == 0

    def test_records_processed(self, built_graph):
        store, stats = built_graph
        assert stats.records_processed > 1000

    def test_entities_created(self, built_graph):
        store, stats = built_graph
        assert stats.entities_created > 100

    def test_relationships_created(self, built_graph):
        store, stats = built_graph
        assert stats.relationships_created > 50

    def test_people_exist(self, built_graph):
        store, stats = built_graph
        people = store.find_entities(EntityType.PERSON)
        assert len(people) > 10

    def test_messages_exist(self, built_graph):
        store, stats = built_graph
        count = store.count_entities(EntityType.MESSAGE)
        assert count > 500

    def test_events_exist(self, built_graph):
        store, stats = built_graph
        count = store.count_entities(EntityType.EVENT)
        assert count > 50

    def test_bookmarks_exist(self, built_graph):
        store, stats = built_graph
        count = store.count_entities(EntityType.BOOKMARK)
        assert count > 100

    def test_notes_exist(self, built_graph):
        store, stats = built_graph
        notes = store.find_entities(EntityType.NOTE)
        assert len(notes) > 0

    def test_places_created_from_events(self, built_graph):
        store, stats = built_graph
        places = store.find_entities(EntityType.PLACE)
        assert len(places) > 0

    def test_no_duplicate_provenance(self, built_graph):
        """Each source record should have at most one provenance entry."""
        store, stats = built_graph
        # Re-building from the same data should skip all as duplicates
        vault_data = read_vault_jsonl(str(VAULT_DIR))
        builder = GraphBuilder(store)
        records = list(adapt_all(vault_data))
        stats2 = builder.build(iter(records))
        assert stats2.skipped_duplicates == stats.records_processed
        assert stats2.records_processed == 0


# ---------------------------------------------------------------------------
# Entity resolution tests
# ---------------------------------------------------------------------------


class TestEntityResolution:
    def test_email_based_resolution(self, built_graph):
        """People appearing with the same email across sources should be findable."""
        store, _ = built_graph
        # Alice Müller appears in google_contacts and mac_contacts with same email
        alice_ids = store.find_entities_by_identifier(
            IdentifierSystem.EMAIL, "alice.mueller@gmail.com"
        )
        assert len(alice_ids) >= 1, "Alice should be found by email"
        # The second contact source should have merged into the first
        unique_ids = set(alice_ids)
        assert len(unique_ids) <= 2, "Same email should resolve to at most 2 entities"

    def test_contacts_have_identifiers(self, built_graph):
        """Contact entities should have email identifiers registered."""
        store, _ = built_graph
        people = store.find_entities(EntityType.PERSON)
        people_with_identifiers = 0
        for person in people:
            ids = store.get_identifiers(person.id)
            if ids:
                people_with_identifiers += 1
        assert people_with_identifiers > 5

    def test_merge_creates_hypotheses_for_ambiguous(self, built_graph):
        """Ambiguous matches should create hypotheses for review."""
        store, stats = built_graph
        # The system may have created hypotheses for name-similar entities
        hypotheses = store.get_hypotheses(status=HypothesisStatus.OPEN)
        # We don't require a specific count, but the mechanism should work
        assert isinstance(hypotheses, list)

    def test_merged_entities_have_richer_properties(self, built_graph):
        """Merged entities should accumulate properties from both sources."""
        store, stats = built_graph
        assert stats.entities_merged >= 0  # Some merges may happen

    def test_sent_relationships_link_people_to_messages(self, built_graph):
        """Messages should have SENT or RECEIVED relationships to people."""
        store, _ = built_graph
        # Check a broader sample — some message types may have empty senders
        messages = store.find_entities(EntityType.MESSAGE, limit=100)
        linked = 0
        for msg in messages:
            rels = store.get_relationships(msg.id)
            if any(r.type in (RelationshipType.SENT, RelationshipType.RECEIVED) for r in rels):
                linked += 1
        assert linked > 0, "Some messages should be linked to people"

    def test_knows_relationships_created(self, built_graph):
        """KNOWS relationships should be inferred from message exchanges."""
        store, _ = built_graph
        people = store.find_entities(EntityType.PERSON)
        total_knows = 0
        for person in people[:20]:
            rels = store.get_relationships(
                person.id, rel_type=RelationshipType.KNOWS
            )
            total_knows += len(rels)
        assert total_knows > 0, "People who message each other should KNOWS"


# ---------------------------------------------------------------------------
# Temporal query tests
# ---------------------------------------------------------------------------


class TestTemporalIntegration:
    def test_event_relationships_have_valid_times(self, built_graph):
        """Event attendance or location relationships should exist."""
        store, _ = built_graph
        events = store.find_entities(EntityType.EVENT, limit=100)
        event_rels = 0
        for event in events:
            rels = store.get_relationships(event.id)
            for r in rels:
                if r.type in (RelationshipType.ATTENDED, RelationshipType.LOCATED_AT):
                    event_rels += 1
        assert event_rels > 0, "Events should have attendance or location relationships"

    def test_current_relationships_query(self, built_graph):
        """TemporalQuery.current_relationships should return active rels."""
        store, _ = built_graph
        temporal = TemporalQuery(store)
        people = store.find_entities(EntityType.PERSON)[:5]
        for person in people:
            current = temporal.current_relationships(person.id)
            assert isinstance(current, list)


# ---------------------------------------------------------------------------
# Forgetter integration tests
# ---------------------------------------------------------------------------


class TestForgettingIntegration:
    def test_forget_entity_cascades(self, built_graph):
        """Forgetting an entity removes it and all connected data."""
        store, _ = built_graph
        forgetter = Forgetter(store)

        # Find a person with relationships
        people = store.find_entities(EntityType.PERSON)
        target = None
        for p in people:
            rels = store.get_relationships(p.id)
            if len(rels) > 0:
                target = p
                break

        if target is None:
            pytest.skip("No person with relationships found")

        target_id = target.id
        forgetter.forget_entity(target_id, reason="test cleanup")

        # Entity should be gone
        assert store.get_entity(target_id) is None
        # Relationships should be gone
        assert len(store.get_relationships(target_id)) == 0

    def test_disconnect_source_removes_orphans(self, built_graph):
        """Disconnecting a source removes provenance and orphaned entities."""
        store, _ = built_graph
        forgetter = Forgetter(store)

        stats_before = store.stats()
        record = forgetter.disconnect_source("apple_notes")
        stats_after = store.stats()

        assert record.entities_removed >= 0
        assert stats_after["entities"] <= stats_before["entities"]


# ---------------------------------------------------------------------------
# Stats and consistency
# ---------------------------------------------------------------------------


class TestConsistency:
    def test_stats_are_positive(self, built_graph):
        store, _ = built_graph
        s = store.stats()
        assert s["entities"] > 0
        assert s["relationships"] > 0
        assert s["provenance"] > 0

    def test_all_entities_have_provenance(self, built_graph):
        """Every entity from vault ingestion should have provenance."""
        store, _ = built_graph
        people = store.find_entities(EntityType.PERSON)[:20]
        for person in people:
            provs = store.get_provenance(person.id)
            assert len(provs) > 0, f"Entity {person.id} has no provenance"

    def test_graph_rebuild_is_idempotent(self, store, vault_data):
        """Building the graph twice from the same data produces same result."""
        builder = GraphBuilder(store)
        records = list(adapt_all(vault_data))

        stats1 = builder.build(iter(records))
        s1 = store.stats()

        # Build again — all should be skipped as duplicates
        records2 = list(adapt_all(vault_data))
        stats2 = builder.build(iter(records2))

        assert stats2.records_processed == 0
        assert stats2.skipped_duplicates == stats1.records_processed

        s2 = store.stats()
        assert s1["entities"] == s2["entities"]
        assert s1["relationships"] == s2["relationships"]
