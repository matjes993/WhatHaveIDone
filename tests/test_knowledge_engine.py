"""Tests for core.knowledge — KnowledgeEngine public API."""

import pytest
from datetime import datetime

from core.knowledge import (
    KnowledgeEngine,
    CanonicalRecord,
    EntityType,
    IdentifierSystem,
    PipelineStep,
    ProvenanceByType,
    RelationshipType,
    ResolutionMethod,
)


@pytest.fixture
def engine(tmp_path):
    e = KnowledgeEngine(tmp_path)
    yield e
    e.close()


class TestIngestion:
    def test_ingest_and_query(self, engine):
        records = iter([
            CanonicalRecord(
                record_type=EntityType.PERSON,
                source_name="contacts",
                source_id="c-1",
                data={"name": "Alice Smith", "emails": [{"value": "alice@test.com"}]},
            ),
            CanonicalRecord(
                record_type=EntityType.PERSON,
                source_name="contacts",
                source_id="c-2",
                data={"name": "Bob Jones"},
            ),
        ])
        stats = engine.ingest(records)
        assert stats.entities_created == 2
        assert stats.errors == 0

        people = engine.find_entities(EntityType.PERSON)
        assert len(people) == 2

    def test_count(self, engine):
        engine.ingest(iter([
            CanonicalRecord(
                record_type=EntityType.PERSON,
                source_name="test", source_id="1",
                data={"name": "Alice"},
            ),
        ]))
        assert engine.count_entities() == 1
        assert engine.count_entities(EntityType.PERSON) == 1
        assert engine.count_entities(EntityType.ORGANIZATION) == 0


class TestIdentifiers:
    def test_find_by_identifier(self, engine):
        engine.ingest(iter([
            CanonicalRecord(
                record_type=EntityType.PERSON,
                source_name="contacts", source_id="c-1",
                data={"name": "Alice", "emails": [{"value": "alice@test.com"}]},
            ),
        ]))
        result = engine.find_by_identifier(IdentifierSystem.EMAIL, "alice@test.com")
        assert result is not None
        assert result.properties["name"] == "Alice"

    def test_add_custom_identifier(self, engine):
        engine.ingest(iter([
            CanonicalRecord(
                record_type=EntityType.FILE,
                source_name="books", source_id="b-1",
                data={"name": "Clean Code"},
            ),
        ]))
        files = engine.find_entities(EntityType.FILE)
        engine.add_identifier(
            files[0].id, IdentifierSystem.ISBN, "978-0-13-468599-1", verified=True
        )
        idents = engine.get_identifiers(files[0].id)
        assert any(i.system == IdentifierSystem.ISBN for i in idents)


class TestConnections:
    def test_get_connections(self, engine):
        engine.ingest(iter([
            CanonicalRecord(
                record_type=EntityType.MESSAGE,
                source_name="gmail", source_id="msg-1",
                data={
                    "subject": "Hello",
                    "sender": {"email": "alice@test.com"},
                    "recipients": [{"email": "bob@test.com"}],
                },
            ),
        ]))
        people = engine.find_entities(EntityType.PERSON)
        alice = next(
            p for p in people
            if any(
                e.get("value") == "alice@test.com"
                for e in p.properties.get("emails", [])
                if isinstance(e, dict)
            )
        )
        conns = engine.get_connections(alice.id)
        assert len(conns) > 0


class TestAnnotations:
    def test_annotate_and_retrieve(self, engine):
        engine.ingest(iter([
            CanonicalRecord(
                record_type=EntityType.PERSON,
                source_name="test", source_id="p-1",
                data={"name": "Alice"},
            ),
        ]))
        people = engine.find_entities(EntityType.PERSON)
        entity_id = people[0].id

        engine.annotate(
            target_id=entity_id,
            field_name="importance_score",
            value=0.95,
            by_type=ProvenanceByType.SCROLL,
            by_id="official/contact-scorer@1.0",
            pipeline_step=PipelineStep.SCORING,
        )
        anns = engine.get_annotations(entity_id, "importance_score")
        assert len(anns) == 1
        assert anns[0].value == 0.95
        assert anns[0].by_id == "official/contact-scorer@1.0"

    def test_uninstall_scroll(self, engine):
        engine.ingest(iter([
            CanonicalRecord(
                record_type=EntityType.PERSON,
                source_name="test", source_id="p-1",
                data={"name": "Alice"},
            ),
        ]))
        people = engine.find_entities(EntityType.PERSON)
        eid = people[0].id

        engine.annotate(
            target_id=eid, field_name="tag", value="vip",
            by_type=ProvenanceByType.SCROLL,
            by_id="community/tagger@1.0",
            pipeline_step=PipelineStep.ENRICHMENT,
        )
        removed = engine.uninstall_scroll("community/tagger")
        assert removed == 1
        assert engine.get_annotations(eid, "tag") == []


class TestForgetting:
    def test_forget_entity(self, engine):
        engine.ingest(iter([
            CanonicalRecord(
                record_type=EntityType.PERSON,
                source_name="test", source_id="p-1",
                data={"name": "Alice"},
            ),
        ]))
        people = engine.find_entities(EntityType.PERSON)
        record = engine.forget_entity(people[0].id, reason="user request")
        assert record.entities_removed == 1
        assert engine.count_entities() == 0

    def test_disconnect_source(self, engine):
        engine.ingest(iter([
            CanonicalRecord(
                record_type=EntityType.PERSON,
                source_name="gmail", source_id="p-1",
                data={"name": "Alice"},
            ),
            CanonicalRecord(
                record_type=EntityType.PERSON,
                source_name="contacts", source_id="p-2",
                data={"name": "Bob"},
            ),
        ]))
        record = engine.disconnect_source("gmail")
        assert record.entities_removed == 1
        assert engine.count_entities() == 1

    def test_forgetting_history(self, engine):
        engine.ingest(iter([
            CanonicalRecord(
                record_type=EntityType.PERSON,
                source_name="test", source_id="p-1",
                data={"name": "Alice"},
            ),
        ]))
        people = engine.find_entities(EntityType.PERSON)
        engine.forget_entity(people[0].id)
        history = engine.forgetting_history()
        assert len(history) == 1


class TestHypotheses:
    def test_get_open_hypotheses(self, engine):
        hyps = engine.get_open_hypotheses()
        assert hyps == []


class TestStats:
    def test_stats(self, engine):
        engine.ingest(iter([
            CanonicalRecord(
                record_type=EntityType.PERSON,
                source_name="test", source_id="p-1",
                data={"name": "Alice"},
            ),
        ]))
        s = engine.stats()
        assert s["entities"] == 1
        assert s["provenance"] >= 1
