"""Tests for core.knowledge.forgetter — cascade deletion."""

import pytest

from core.knowledge.forgetter import Forgetter
from core.knowledge.graph_store import GraphStore
from core.knowledge.schema import (
    Entity,
    EntityType,
    ExternalIdentifier,
    IdentifierSystem,
    Provenance,
    Relationship,
    RelationshipType,
)


@pytest.fixture
def store(tmp_path):
    s = GraphStore(tmp_path / "test.db")
    yield s
    s.close()


@pytest.fixture
def forgetter(store):
    return Forgetter(store)


def _create_person(store, name, source_name="gmail", source_id=None):
    e = Entity(type=EntityType.PERSON, properties={"name": name})
    store.create_entity(e)
    store.add_provenance(Provenance(
        target_type="entity", target_id=e.id,
        source_name=source_name, source_record_id=source_id or f"{name}-1",
    ))
    return e


class TestForgetEntity:
    def test_forget_removes_entity(self, store, forgetter):
        e = _create_person(store, "Alice")
        forgetter.forget_entity(e.id, reason="user request")
        assert store.get_entity(e.id) is None

    def test_forget_removes_relationships(self, store, forgetter):
        alice = _create_person(store, "Alice")
        bob = _create_person(store, "Bob")
        store.create_relationship(Relationship(
            type=RelationshipType.KNOWS, source_id=alice.id, target_id=bob.id,
        ))
        forgetter.forget_entity(alice.id)
        # Bob still exists
        assert store.get_entity(bob.id) is not None
        # But relationship is gone
        rels = store.get_relationships(bob.id)
        assert len(rels) == 0

    def test_forget_logs_event(self, store, forgetter):
        e = _create_person(store, "Alice")
        record = forgetter.forget_entity(e.id, reason="privacy request")
        assert record.entities_removed == 1
        assert record.reason == "privacy request"
        log = store.get_forgetting_log()
        assert len(log) == 1
        assert "Alice" in log[0].target_description

    def test_forget_nonexistent_raises(self, store, forgetter):
        with pytest.raises(ValueError):
            forgetter.forget_entity("nonexistent-id")


class TestDisconnectSource:
    def test_disconnect_removes_source_provenance(self, store, forgetter):
        alice = _create_person(store, "Alice", source_name="gmail")
        # Also add provenance from contacts
        store.add_provenance(Provenance(
            target_type="entity", target_id=alice.id,
            source_name="contacts", source_record_id="c-1",
        ))

        forgetter.disconnect_source("gmail", reason="unlinked Google")

        # Entity survives (still has contacts provenance)
        assert store.get_entity(alice.id) is not None
        provs = store.get_provenance(alice.id)
        assert len(provs) == 1
        assert provs[0].source_name == "contacts"

    def test_disconnect_garbage_collects_orphans(self, store, forgetter):
        # Entity only from gmail — will become orphan
        alice = _create_person(store, "Alice", source_name="gmail")
        # Entity from contacts — will survive
        bob = _create_person(store, "Bob", source_name="contacts")

        record = forgetter.disconnect_source("gmail")
        assert record.entities_removed == 1
        assert store.get_entity(alice.id) is None
        assert store.get_entity(bob.id) is not None

    def test_disconnect_logs_event(self, store, forgetter):
        _create_person(store, "Alice", source_name="gmail")
        forgetter.disconnect_source("gmail", reason="privacy")
        log = store.get_forgetting_log()
        assert len(log) == 1
        assert "gmail" in log[0].target_description


class TestForgetByCriteria:
    def test_forget_by_type(self, store, forgetter):
        _create_person(store, "Alice")
        org = Entity(type=EntityType.ORGANIZATION, properties={"name": "Acme"})
        store.create_entity(org)
        store.add_provenance(Provenance(
            target_type="entity", target_id=org.id,
            source_name="test", source_record_id="org-1",
        ))

        forgetter.forget_by_criteria(entity_type="person", reason="cleanup")
        assert store.count_entities(EntityType.PERSON) == 0
        assert store.count_entities(EntityType.ORGANIZATION) == 1

    def test_forget_by_source(self, store, forgetter):
        _create_person(store, "Alice", source_name="gmail")
        _create_person(store, "Bob", source_name="contacts")

        forgetter.forget_by_criteria(source_name="gmail")
        assert store.count_entities() == 1

    def test_no_criteria_raises(self, store, forgetter):
        with pytest.raises(ValueError):
            forgetter.forget_by_criteria()


class TestForgettingHistory:
    def test_history_ordered(self, store, forgetter):
        e1 = _create_person(store, "Alice")
        e2 = _create_person(store, "Bob")
        forgetter.forget_entity(e1.id)
        forgetter.forget_entity(e2.id)
        history = forgetter.get_forgetting_history()
        assert len(history) == 2
        # Most recent first
        assert "Bob" in history[0].target_description
