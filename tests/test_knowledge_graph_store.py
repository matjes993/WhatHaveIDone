"""Tests for core.knowledge.graph_store — SQLite graph CRUD."""

import pytest
from datetime import datetime, timedelta

from core.knowledge.graph_store import GraphStore
from core.knowledge.schema import (
    Annotation,
    AnnotationStatus,
    Entity,
    EntityType,
    ExternalIdentifier,
    ForgettingRecord,
    Hypothesis,
    HypothesisStatus,
    HypothesisType,
    IdentifierSystem,
    PipelineStep,
    Provenance,
    ProvenanceByType,
    Relationship,
    RelationshipType,
    ResolutionMethod,
)


@pytest.fixture
def store(tmp_path):
    s = GraphStore(tmp_path / "test.db")
    yield s
    s.close()


# ---------------------------------------------------------------------------
# Entities
# ---------------------------------------------------------------------------

class TestEntities:
    def test_create_and_get(self, store):
        e = Entity(type=EntityType.PERSON, properties={"name": "Alice"})
        store.create_entity(e)
        result = store.get_entity(e.id)
        assert result is not None
        assert result.type == EntityType.PERSON
        assert result.properties["name"] == "Alice"

    def test_get_nonexistent(self, store):
        assert store.get_entity("nonexistent") is None

    def test_update(self, store):
        e = Entity(type=EntityType.PERSON, properties={"name": "Alice"})
        store.create_entity(e)
        e.properties["name"] = "Alice Smith"
        store.update_entity(e)
        result = store.get_entity(e.id)
        assert result.properties["name"] == "Alice Smith"

    def test_delete_cascades(self, store):
        e = Entity(type=EntityType.PERSON, properties={"name": "Alice"})
        store.create_entity(e)
        store.add_provenance(Provenance(
            target_type="entity", target_id=e.id,
            source_name="gmail", source_record_id="msg-1",
        ))
        store.add_identifier(ExternalIdentifier(
            entity_id=e.id, system=IdentifierSystem.EMAIL,
            value="alice@test.com",
        ))
        store.delete_entity(e.id)
        assert store.get_entity(e.id) is None
        assert store.get_provenance(e.id) == []
        assert store.get_identifiers(e.id) == []

    def test_find_by_type(self, store):
        store.create_entity(Entity(type=EntityType.PERSON, properties={"name": "A"}))
        store.create_entity(Entity(type=EntityType.PERSON, properties={"name": "B"}))
        store.create_entity(Entity(type=EntityType.ORGANIZATION, properties={"name": "C"}))
        people = store.find_entities(EntityType.PERSON)
        assert len(people) == 2
        orgs = store.find_entities(EntityType.ORGANIZATION)
        assert len(orgs) == 1

    def test_find_with_limit_offset(self, store):
        for i in range(5):
            store.create_entity(Entity(type=EntityType.PERSON, properties={"name": f"P{i}"}))
        page1 = store.find_entities(EntityType.PERSON, limit=2, offset=0)
        page2 = store.find_entities(EntityType.PERSON, limit=2, offset=2)
        assert len(page1) == 2
        assert len(page2) == 2
        assert page1[0].id != page2[0].id

    def test_count(self, store):
        assert store.count_entities() == 0
        store.create_entity(Entity(type=EntityType.PERSON))
        store.create_entity(Entity(type=EntityType.ORGANIZATION))
        assert store.count_entities() == 2
        assert store.count_entities(EntityType.PERSON) == 1

    def test_find_by_identifier(self, store):
        e = Entity(type=EntityType.PERSON, properties={"name": "Alice"})
        store.create_entity(e)
        store.add_identifier(ExternalIdentifier(
            entity_id=e.id, system=IdentifierSystem.EMAIL,
            value="alice@test.com",
        ))
        result = store.find_entity_by_identifier(IdentifierSystem.EMAIL, "alice@test.com")
        assert result is not None
        assert result.id == e.id

    def test_find_by_identifier_not_found(self, store):
        assert store.find_entity_by_identifier(IdentifierSystem.EMAIL, "nope@test.com") is None


# ---------------------------------------------------------------------------
# Relationships
# ---------------------------------------------------------------------------

class TestRelationships:
    def test_create_and_get(self, store):
        a = Entity(type=EntityType.PERSON)
        b = Entity(type=EntityType.ORGANIZATION)
        store.create_entity(a)
        store.create_entity(b)
        rel = Relationship(
            type=RelationshipType.WORKS_AT,
            source_id=a.id, target_id=b.id,
            properties={"role": "Engineer"},
        )
        store.create_relationship(rel)
        result = store.get_relationship(rel.id)
        assert result is not None
        assert result.type == RelationshipType.WORKS_AT
        assert result.properties["role"] == "Engineer"

    def test_get_relationships_outgoing(self, store):
        a = Entity(type=EntityType.PERSON)
        b = Entity(type=EntityType.ORGANIZATION)
        c = Entity(type=EntityType.ORGANIZATION)
        for e in [a, b, c]:
            store.create_entity(e)
        store.create_relationship(Relationship(
            type=RelationshipType.WORKS_AT, source_id=a.id, target_id=b.id,
        ))
        store.create_relationship(Relationship(
            type=RelationshipType.WORKS_AT, source_id=a.id, target_id=c.id,
        ))
        rels = store.get_relationships(a.id, direction="outgoing")
        assert len(rels) == 2

    def test_get_relationships_incoming(self, store):
        a = Entity(type=EntityType.PERSON)
        b = Entity(type=EntityType.PERSON)
        org = Entity(type=EntityType.ORGANIZATION)
        for e in [a, b, org]:
            store.create_entity(e)
        store.create_relationship(Relationship(
            type=RelationshipType.WORKS_AT, source_id=a.id, target_id=org.id,
        ))
        store.create_relationship(Relationship(
            type=RelationshipType.WORKS_AT, source_id=b.id, target_id=org.id,
        ))
        rels = store.get_relationships(org.id, direction="incoming")
        assert len(rels) == 2

    def test_current_only_filter(self, store):
        a = Entity(type=EntityType.PERSON)
        b = Entity(type=EntityType.ORGANIZATION)
        store.create_entity(a)
        store.create_entity(b)
        rel = Relationship(
            type=RelationshipType.WORKS_AT, source_id=a.id, target_id=b.id,
        )
        store.create_relationship(rel)
        store.invalidate_relationship(rel.id)
        current = store.get_relationships(a.id, current_only=True)
        all_rels = store.get_relationships(a.id, current_only=False)
        assert len(current) == 0
        assert len(all_rels) == 1

    def test_filter_by_type(self, store):
        a = Entity(type=EntityType.PERSON)
        b = Entity(type=EntityType.PERSON)
        c = Entity(type=EntityType.ORGANIZATION)
        for e in [a, b, c]:
            store.create_entity(e)
        store.create_relationship(Relationship(
            type=RelationshipType.KNOWS, source_id=a.id, target_id=b.id,
        ))
        store.create_relationship(Relationship(
            type=RelationshipType.WORKS_AT, source_id=a.id, target_id=c.id,
        ))
        knows = store.get_relationships(a.id, rel_type=RelationshipType.KNOWS)
        assert len(knows) == 1
        works = store.get_relationships(a.id, rel_type=RelationshipType.WORKS_AT)
        assert len(works) == 1

    def test_invalidate(self, store):
        a = Entity(type=EntityType.PERSON)
        b = Entity(type=EntityType.ORGANIZATION)
        store.create_entity(a)
        store.create_entity(b)
        rel = Relationship(
            type=RelationshipType.WORKS_AT, source_id=a.id, target_id=b.id,
        )
        store.create_relationship(rel)
        now = datetime.utcnow()
        store.invalidate_relationship(rel.id, valid_to=now)
        result = store.get_relationship(rel.id)
        assert result.valid_to is not None
        assert result.superseded_at is not None


# ---------------------------------------------------------------------------
# Provenance
# ---------------------------------------------------------------------------

class TestProvenance:
    def test_add_and_get(self, store):
        e = Entity(type=EntityType.PERSON)
        store.create_entity(e)
        prov = Provenance(
            target_type="entity", target_id=e.id,
            source_name="gmail", source_record_id="msg-1",
            confidence=0.95,
        )
        store.add_provenance(prov)
        results = store.get_provenance(e.id)
        assert len(results) == 1
        assert results[0].source_name == "gmail"
        assert results[0].confidence == 0.95

    def test_has_provenance(self, store):
        e = Entity(type=EntityType.PERSON)
        store.create_entity(e)
        assert not store.has_provenance(e.id)
        store.add_provenance(Provenance(
            target_type="entity", target_id=e.id,
            source_name="gmail", source_record_id="msg-1",
        ))
        assert store.has_provenance(e.id)

    def test_delete_by_source(self, store):
        e = Entity(type=EntityType.PERSON)
        store.create_entity(e)
        store.add_provenance(Provenance(
            target_type="entity", target_id=e.id,
            source_name="gmail", source_record_id="msg-1",
        ))
        store.add_provenance(Provenance(
            target_type="entity", target_id=e.id,
            source_name="contacts", source_record_id="c-1",
        ))
        count = store.delete_provenance_by_source("gmail")
        assert count == 1
        remaining = store.get_provenance(e.id)
        assert len(remaining) == 1
        assert remaining[0].source_name == "contacts"

    def test_find_by_source_record(self, store):
        e = Entity(type=EntityType.PERSON)
        store.create_entity(e)
        store.add_provenance(Provenance(
            target_type="entity", target_id=e.id,
            source_name="gmail", source_record_id="msg-42",
        ))
        results = store.find_by_source_record("gmail", "msg-42")
        assert len(results) == 1
        assert results[0].target_id == e.id


# ---------------------------------------------------------------------------
# Identifiers
# ---------------------------------------------------------------------------

class TestIdentifiers:
    def test_add_and_get(self, store):
        e = Entity(type=EntityType.PERSON)
        store.create_entity(e)
        store.add_identifier(ExternalIdentifier(
            entity_id=e.id, system=IdentifierSystem.EMAIL,
            value="alice@test.com", verified=True,
        ))
        idents = store.get_identifiers(e.id)
        assert len(idents) == 1
        assert idents[0].value == "alice@test.com"
        assert idents[0].verified is True

    def test_duplicate_ignored(self, store):
        e = Entity(type=EntityType.PERSON)
        store.create_entity(e)
        ident = ExternalIdentifier(
            entity_id=e.id, system=IdentifierSystem.EMAIL,
            value="alice@test.com",
        )
        store.add_identifier(ident)
        store.add_identifier(ident)  # duplicate
        assert len(store.get_identifiers(e.id)) == 1

    def test_find_entities_by_identifier(self, store):
        e1 = Entity(type=EntityType.PERSON)
        e2 = Entity(type=EntityType.PERSON)
        store.create_entity(e1)
        store.create_entity(e2)
        store.add_identifier(ExternalIdentifier(
            entity_id=e1.id, system=IdentifierSystem.EMAIL,
            value="shared@test.com",
        ))
        store.add_identifier(ExternalIdentifier(
            entity_id=e2.id, system=IdentifierSystem.EMAIL,
            value="shared@test.com",
        ))
        ids = store.find_entities_by_identifier(IdentifierSystem.EMAIL, "shared@test.com")
        assert len(ids) == 2


# ---------------------------------------------------------------------------
# Annotations
# ---------------------------------------------------------------------------

class TestAnnotations:
    def test_add_and_get(self, store):
        ann = Annotation(
            target_id="ent-1", field_name="normalized_name",
            value="alice smith",
            by_type=ProvenanceByType.SCROLL,
            by_id="official/name-normalizer@1.0",
            pipeline_step=PipelineStep.ENRICHMENT,
        )
        store.add_annotation(ann)
        results = store.get_annotations("ent-1")
        assert len(results) == 1
        assert results[0].value == "alice smith"
        assert results[0].by_id == "official/name-normalizer@1.0"

    def test_get_by_field(self, store):
        store.add_annotation(Annotation(
            target_id="e1", field_name="name_norm", value="alice",
            by_type=ProvenanceByType.SCROLL, by_id="s1",
            pipeline_step=PipelineStep.ENRICHMENT,
        ))
        store.add_annotation(Annotation(
            target_id="e1", field_name="phone_norm", value="+1234",
            by_type=ProvenanceByType.SCROLL, by_id="s2",
            pipeline_step=PipelineStep.ENRICHMENT,
        ))
        name_anns = store.get_annotations("e1", field_name="name_norm")
        assert len(name_anns) == 1
        assert name_anns[0].field_name == "name_norm"

    def test_effective_value(self, store):
        store.add_annotation(Annotation(
            target_id="e1", field_name="score", value=0.5,
            by_type=ProvenanceByType.SCROLL, by_id="s1",
            pipeline_step=PipelineStep.SCORING,
        ))
        store.add_annotation(Annotation(
            target_id="e1", field_name="score", value=0.9,
            by_type=ProvenanceByType.MODEL, by_id="claude-haiku",
            pipeline_step=PipelineStep.SCORING,
        ))
        # Most recent wins
        val = store.get_effective_value("e1", "score")
        assert val == 0.9

    def test_supersede(self, store):
        ann = Annotation(
            target_id="e1", field_name="tag", value="spam",
            by_type=ProvenanceByType.SCROLL, by_id="s1",
            pipeline_step=PipelineStep.DETECTION,
        )
        store.add_annotation(ann)
        store.supersede_annotation(ann.id)
        active = store.get_annotations("e1", active_only=True)
        assert len(active) == 0
        all_anns = store.get_annotations("e1", active_only=False)
        assert len(all_anns) == 1
        assert all_anns[0].status == AnnotationStatus.SUPERSEDED

    def test_remove_by_scroll(self, store):
        store.add_annotation(Annotation(
            target_id="e1", field_name="f1", value="v1",
            by_type=ProvenanceByType.SCROLL,
            by_id="community/alice/normalizer@1.0",
            pipeline_step=PipelineStep.ENRICHMENT,
        ))
        store.add_annotation(Annotation(
            target_id="e1", field_name="f2", value="v2",
            by_type=ProvenanceByType.SCROLL,
            by_id="community/alice/normalizer@1.0",
            pipeline_step=PipelineStep.ENRICHMENT,
        ))
        store.add_annotation(Annotation(
            target_id="e1", field_name="f3", value="v3",
            by_type=ProvenanceByType.SCROLL,
            by_id="official/other@2.0",
            pipeline_step=PipelineStep.ENRICHMENT,
        ))
        removed = store.remove_annotations_by_scroll("community/alice/normalizer")
        assert removed == 2
        remaining = store.get_annotations("e1", active_only=True)
        assert len(remaining) == 1
        assert remaining[0].by_id == "official/other@2.0"


# ---------------------------------------------------------------------------
# Hypotheses
# ---------------------------------------------------------------------------

class TestHypotheses:
    def test_create_and_get(self, store):
        hyp = Hypothesis(
            type=HypothesisType.IDENTITY_MERGE,
            entity_ids=["e1", "e2"],
            confidence=0.8,
            evidence={"reason": "same email domain"},
        )
        store.create_hypothesis(hyp)
        results = store.get_hypotheses(HypothesisStatus.OPEN)
        assert len(results) == 1
        assert results[0].confidence == 0.8
        assert results[0].entity_ids == ["e1", "e2"]

    def test_resolve(self, store):
        hyp = Hypothesis(
            type=HypothesisType.IDENTITY_MERGE,
            entity_ids=["e1", "e2"],
        )
        store.create_hypothesis(hyp)
        store.resolve_hypothesis(hyp.id, HypothesisStatus.CONFIRMED, ResolutionMethod.USER)
        results = store.get_hypotheses(HypothesisStatus.OPEN)
        assert len(results) == 0
        confirmed = store.get_hypotheses(HypothesisStatus.CONFIRMED)
        assert len(confirmed) == 1
        assert confirmed[0].resolution == ResolutionMethod.USER
        assert confirmed[0].resolved_at is not None


# ---------------------------------------------------------------------------
# Forgetting Log
# ---------------------------------------------------------------------------

class TestForgettingLog:
    def test_log_and_retrieve(self, store):
        record = ForgettingRecord(
            action="forget_entity",
            target_description="Forgot person: Alice",
            entities_removed=1,
            relationships_removed=5,
            reason="user request",
        )
        store.log_forgetting(record)
        log = store.get_forgetting_log()
        assert len(log) == 1
        assert log[0].action == "forget_entity"
        assert log[0].entities_removed == 1
        assert log[0].reason == "user request"


# ---------------------------------------------------------------------------
# Garbage Collection
# ---------------------------------------------------------------------------

class TestGarbageCollection:
    def test_orphan_cleanup(self, store):
        # Entity with provenance — should survive
        e1 = Entity(type=EntityType.PERSON, properties={"name": "Kept"})
        store.create_entity(e1)
        store.add_provenance(Provenance(
            target_type="entity", target_id=e1.id,
            source_name="gmail", source_record_id="msg-1",
        ))
        # Entity without provenance — orphan
        e2 = Entity(type=EntityType.PERSON, properties={"name": "Orphan"})
        store.create_entity(e2)

        orphans = store.garbage_collect_orphans()
        assert e2.id in orphans
        assert e1.id not in orphans
        assert store.get_entity(e1.id) is not None
        assert store.get_entity(e2.id) is None


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

class TestStats:
    def test_empty_stats(self, store):
        s = store.stats()
        assert s["entities"] == 0
        assert s["relationships"] == 0
        assert s["provenance"] == 0

    def test_stats_after_operations(self, store):
        e = Entity(type=EntityType.PERSON)
        store.create_entity(e)
        store.add_provenance(Provenance(
            target_type="entity", target_id=e.id,
            source_name="test", source_record_id="r1",
        ))
        s = store.stats()
        assert s["entities"] == 1
        assert s["provenance"] == 1
