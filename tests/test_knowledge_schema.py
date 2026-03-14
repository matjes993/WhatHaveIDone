"""Tests for core.knowledge.schema — data structures and canonical types."""

import pytest
from datetime import datetime

from core.knowledge.schema import (
    Annotation,
    AnnotationStatus,
    CanonicalRecord,
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
    CANONICAL_FIELDS,
)


class TestEntity:
    def test_create_with_defaults(self):
        e = Entity()
        assert e.id  # UUID auto-generated
        assert e.type == EntityType.PERSON
        assert e.properties == {}
        assert isinstance(e.created_at, datetime)

    def test_create_with_values(self):
        e = Entity(
            id="test-123",
            type=EntityType.ORGANIZATION,
            properties={"name": "Acme Corp"},
        )
        assert e.id == "test-123"
        assert e.type == EntityType.ORGANIZATION
        assert e.properties["name"] == "Acme Corp"

    def test_get_set(self):
        e = Entity()
        assert e.get("name") is None
        assert e.get("name", "default") == "default"
        e.set("name", "Alice")
        assert e.get("name") == "Alice"

    def test_set_updates_timestamp(self):
        e = Entity()
        original = e.updated_at
        e.set("name", "Bob")
        assert e.updated_at >= original


class TestRelationship:
    def test_create_with_defaults(self):
        r = Relationship()
        assert r.id
        assert r.type == RelationshipType.RELATED_TO
        assert r.valid_to is None
        assert r.superseded_at is None

    def test_bitemporal_fields(self):
        now = datetime.utcnow()
        r = Relationship(
            type=RelationshipType.WORKS_AT,
            source_id="person-1",
            target_id="org-1",
            valid_from=now,
        )
        assert r.valid_from == now
        assert r.valid_to is None  # still current


class TestCanonicalRecord:
    def test_dedup_key(self):
        r = CanonicalRecord(source_name="gmail", source_id="msg-123")
        assert r.dedup_key == "gmail:msg-123"

    def test_schema_version_default(self):
        r = CanonicalRecord()
        assert r.schema_version == 1

    def test_all_entity_types_have_canonical_fields(self):
        for etype in EntityType:
            assert etype in CANONICAL_FIELDS, f"Missing canonical fields for {etype}"

    def test_canonical_fields_are_lists(self):
        for etype, fields in CANONICAL_FIELDS.items():
            assert isinstance(fields, list)
            assert len(fields) > 0


class TestProvenance:
    def test_defaults(self):
        p = Provenance(target_id="ent-1", source_name="gmail", source_record_id="msg-1")
        assert p.confidence == 1.0
        assert p.derivation is None


class TestAnnotation:
    def test_defaults(self):
        a = Annotation(target_id="ent-1", field_name="normalized_name", value="Alice")
        assert a.status == AnnotationStatus.ACTIVE
        assert a.cost_tokens == 0
        assert a.cost_usd == 0.0
        assert a.parent_ids == []


class TestHypothesis:
    def test_defaults(self):
        h = Hypothesis(entity_ids=["e1", "e2"])
        assert h.status == HypothesisStatus.OPEN
        assert h.resolution is None
        assert h.confidence == 0.5


class TestExternalIdentifier:
    def test_isbn(self):
        i = ExternalIdentifier(
            entity_id="book-1",
            system=IdentifierSystem.ISBN,
            value="978-0-13-468599-1",
            verified=True,
        )
        assert i.system == IdentifierSystem.ISBN
        assert i.verified is True


class TestEnums:
    def test_entity_types(self):
        assert len(EntityType) == 9
        assert EntityType.PERSON.value == "person"

    def test_relationship_types(self):
        assert RelationshipType.KNOWS.value == "knows"
        assert RelationshipType.WORKS_AT.value == "works_at"

    def test_identifier_systems(self):
        assert IdentifierSystem.ISBN.value == "isbn"
        assert IdentifierSystem.WIKIDATA.value == "wikidata"

    def test_pipeline_steps(self):
        assert PipelineStep.EXTRACTION.value == "extraction"
        assert PipelineStep.COMPRESSION.value == "compression"
