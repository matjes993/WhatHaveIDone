"""Tests for core.knowledge.resolver — entity resolution."""

import pytest

from core.knowledge.graph_store import GraphStore
from core.knowledge.resolver import (
    EntityResolver,
    jaro_similarity,
    jaro_winkler_similarity,
    normalize_email,
    normalize_name,
    normalize_phone,
)
from core.knowledge.schema import (
    Entity,
    EntityType,
    ExternalIdentifier,
    IdentifierSystem,
    Relationship,
    RelationshipType,
)


@pytest.fixture
def store(tmp_path):
    s = GraphStore(tmp_path / "test.db")
    yield s
    s.close()


@pytest.fixture
def resolver(store):
    return EntityResolver(store)


# ---------------------------------------------------------------------------
# String similarity functions
# ---------------------------------------------------------------------------

class TestJaroSimilarity:
    def test_identical(self):
        assert jaro_similarity("alice", "alice") == 1.0

    def test_empty(self):
        assert jaro_similarity("", "alice") == 0.0
        assert jaro_similarity("alice", "") == 0.0

    def test_similar(self):
        sim = jaro_similarity("martha", "marhta")
        assert sim > 0.9

    def test_different(self):
        sim = jaro_similarity("alice", "bob")
        assert sim < 0.5


class TestJaroWinkler:
    def test_identical(self):
        assert jaro_winkler_similarity("alice", "alice") == 1.0

    def test_common_prefix_boost(self):
        jaro = jaro_similarity("martha", "marhta")
        jw = jaro_winkler_similarity("martha", "marhta")
        assert jw >= jaro  # winkler prefix boost

    def test_name_variants(self):
        assert jaro_winkler_similarity("matthias", "matthias") == 1.0
        assert jaro_winkler_similarity("matthias", "mathias") > 0.9
        assert jaro_winkler_similarity("michael", "michel") > 0.85


class TestNormalization:
    def test_normalize_name(self):
        assert normalize_name("  John DOE  ") == "john doe"
        assert normalize_name("O'Brien") == "obrien"
        assert normalize_name("") == ""

    def test_normalize_email(self):
        assert normalize_email("  ALICE@Test.COM  ") == "alice@test.com"
        assert normalize_email("") == ""

    def test_normalize_phone(self):
        assert normalize_phone("+1 (555) 123-4567") == "+15551234567"
        assert normalize_phone("") == ""


# ---------------------------------------------------------------------------
# Tier 1: Deterministic matching
# ---------------------------------------------------------------------------

class TestTier1:
    def test_exact_email_match(self, store, resolver):
        e1 = Entity(type=EntityType.PERSON, properties={"name": "Alice", "emails": [{"value": "alice@test.com"}]})
        store.create_entity(e1)
        store.add_identifier(ExternalIdentifier(
            entity_id=e1.id, system=IdentifierSystem.EMAIL, value="alice@test.com",
        ))

        incoming = Entity(
            type=EntityType.PERSON,
            properties={"name": "A. Smith", "emails": [{"value": "alice@test.com"}]},
        )
        matches = resolver.find_matches(incoming)
        assert len(matches) == 1
        assert matches[0].entity_id == e1.id
        assert matches[0].confidence == 1.0
        assert matches[0].tier == 1

    def test_exact_phone_match(self, store, resolver):
        e1 = Entity(type=EntityType.PERSON, properties={"name": "Bob", "phones": [{"value": "+15551234567"}]})
        store.create_entity(e1)
        store.add_identifier(ExternalIdentifier(
            entity_id=e1.id, system=IdentifierSystem.PHONE, value="+15551234567",
        ))

        incoming = Entity(
            type=EntityType.PERSON,
            properties={"phones": [{"value": "+1 (555) 123-4567"}]},
        )
        matches = resolver.find_matches(incoming)
        assert len(matches) == 1
        assert matches[0].confidence == 1.0

    def test_no_match(self, store, resolver):
        e1 = Entity(type=EntityType.PERSON, properties={"name": "Alice", "emails": [{"value": "alice@test.com"}]})
        store.create_entity(e1)
        store.add_identifier(ExternalIdentifier(
            entity_id=e1.id, system=IdentifierSystem.EMAIL, value="alice@test.com",
        ))

        incoming = Entity(
            type=EntityType.PERSON,
            properties={"emails": [{"value": "bob@other.com"}]},
        )
        matches = resolver.find_matches(incoming)
        assert len(matches) == 0


# ---------------------------------------------------------------------------
# Tier 2: Probabilistic name matching
# ---------------------------------------------------------------------------

class TestTier2:
    def test_similar_name_match(self, store, resolver):
        e1 = Entity(type=EntityType.PERSON, properties={"name": "Matthias Kramer"})
        store.create_entity(e1)

        incoming = Entity(
            type=EntityType.PERSON,
            properties={"name": "Mathias Kramer"},
        )
        matches = resolver.find_matches(incoming)
        assert len(matches) >= 1
        assert matches[0].confidence >= 0.85
        assert matches[0].tier == 2

    def test_different_name_no_match(self, store, resolver):
        e1 = Entity(type=EntityType.PERSON, properties={"name": "Alice Johnson"})
        store.create_entity(e1)

        incoming = Entity(
            type=EntityType.PERSON,
            properties={"name": "Bob Williams"},
        )
        matches = resolver.find_matches(incoming)
        assert len(matches) == 0

    def test_org_boost(self, store, resolver):
        e1 = Entity(type=EntityType.PERSON, properties={
            "name": "Michael Smith",
            "organizations": [{"name": "Acme Corp"}],
        })
        store.create_entity(e1)

        incoming = Entity(type=EntityType.PERSON, properties={
            "name": "Michel Smith",
            "organizations": [{"name": "Acme Corp"}],
        })
        matches = resolver.find_matches(incoming)
        assert len(matches) >= 1
        assert any("shared_org" in r for r in matches[0].match_reasons)

    def test_blocking_filters_different_first_letter(self, store, resolver):
        e1 = Entity(type=EntityType.PERSON, properties={"name": "Alice"})
        store.create_entity(e1)

        incoming = Entity(type=EntityType.PERSON, properties={"name": "Zlice"})
        matches = resolver.find_matches(incoming)
        assert len(matches) == 0  # blocked by different first letter


# ---------------------------------------------------------------------------
# Merge operations
# ---------------------------------------------------------------------------

class TestMerge:
    def test_merge_transfers_relationships(self, store, resolver):
        e1 = Entity(type=EntityType.PERSON, properties={"name": "Alice"})
        e2 = Entity(type=EntityType.PERSON, properties={"name": "Alice S."})
        org = Entity(type=EntityType.ORGANIZATION, properties={"name": "Acme"})
        for e in [e1, e2, org]:
            store.create_entity(e)

        store.create_relationship(Relationship(
            type=RelationshipType.WORKS_AT, source_id=e2.id, target_id=org.id,
        ))

        resolver.merge_entities(e1.id, e2.id)

        assert store.get_entity(e2.id) is None  # merged entity deleted
        rels = store.get_relationships(e1.id, direction="outgoing")
        assert any(r.target_id == org.id for r in rels)

    def test_merge_transfers_identifiers(self, store, resolver):
        e1 = Entity(type=EntityType.PERSON, properties={"name": "Alice"})
        e2 = Entity(type=EntityType.PERSON, properties={"name": "Alice S."})
        store.create_entity(e1)
        store.create_entity(e2)
        store.add_identifier(ExternalIdentifier(
            entity_id=e2.id, system=IdentifierSystem.EMAIL, value="alice@work.com",
        ))

        resolver.merge_entities(e1.id, e2.id)

        idents = store.get_identifiers(e1.id)
        assert any(i.value == "alice@work.com" for i in idents)

    def test_merge_combines_properties(self, store, resolver):
        e1 = Entity(type=EntityType.PERSON, properties={
            "name": "Alice",
            "emails": [{"value": "alice@home.com"}],
        })
        e2 = Entity(type=EntityType.PERSON, properties={
            "name": "Alice Smith",
            "emails": [{"value": "alice@work.com"}],
            "phones": [{"value": "+1234"}],
        })
        store.create_entity(e1)
        store.create_entity(e2)

        resolver.merge_entities(e1.id, e2.id)

        result = store.get_entity(e1.id)
        assert result.properties["name"] == "Alice"  # keep original
        assert len(result.properties["emails"]) == 2  # merged lists
        assert result.properties["phones"] == [{"value": "+1234"}]  # new field added

    def test_merge_skips_self_loops(self, store, resolver):
        e1 = Entity(type=EntityType.PERSON, properties={"name": "Alice"})
        e2 = Entity(type=EntityType.PERSON, properties={"name": "Alice S."})
        store.create_entity(e1)
        store.create_entity(e2)
        # e2 has a relationship with e1 — after merge, would become self-loop
        store.create_relationship(Relationship(
            type=RelationshipType.KNOWS, source_id=e2.id, target_id=e1.id,
        ))

        resolver.merge_entities(e1.id, e2.id)

        rels = store.get_relationships(e1.id, current_only=False)
        for rel in rels:
            assert not (rel.source_id == e1.id and rel.target_id == e1.id)


# ---------------------------------------------------------------------------
# Threshold checks
# ---------------------------------------------------------------------------

class TestThresholds:
    def test_auto_merge(self, resolver):
        from core.knowledge.resolver import MergeCandidate
        c = MergeCandidate(entity_id="e1", confidence=0.96)
        assert resolver.should_auto_merge(c)
        assert not resolver.should_review(c)

    def test_review(self, resolver):
        from core.knowledge.resolver import MergeCandidate
        c = MergeCandidate(entity_id="e1", confidence=0.80)
        assert not resolver.should_auto_merge(c)
        assert resolver.should_review(c)

    def test_below_review(self, resolver):
        from core.knowledge.resolver import MergeCandidate
        c = MergeCandidate(entity_id="e1", confidence=0.50)
        assert not resolver.should_auto_merge(c)
        assert not resolver.should_review(c)
