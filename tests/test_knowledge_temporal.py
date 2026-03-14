"""Tests for core.knowledge.temporal — bitemporal queries."""

import pytest
from datetime import datetime, timedelta

from core.knowledge.graph_store import GraphStore
from core.knowledge.temporal import TemporalQuery
from core.knowledge.schema import (
    Entity,
    EntityType,
    Relationship,
    RelationshipType,
)


@pytest.fixture
def store(tmp_path):
    s = GraphStore(tmp_path / "test.db")
    yield s
    s.close()


@pytest.fixture
def temporal(store):
    return TemporalQuery(store)


def _setup_job_history(store):
    """Create Alice with two jobs: Acme (2020-2023) and BigCo (2023-now)."""
    alice = Entity(type=EntityType.PERSON, properties={"name": "Alice"})
    acme = Entity(type=EntityType.ORGANIZATION, properties={"name": "Acme Corp"})
    bigco = Entity(type=EntityType.ORGANIZATION, properties={"name": "BigCo"})
    for e in [alice, acme, bigco]:
        store.create_entity(e)

    # Old job: 2020 to 2023
    old_job = Relationship(
        type=RelationshipType.WORKS_AT,
        source_id=alice.id, target_id=acme.id,
        valid_from=datetime(2020, 1, 1),
        valid_to=datetime(2023, 6, 1),
    )
    store.create_relationship(old_job)

    # Current job: 2023 to now
    new_job = Relationship(
        type=RelationshipType.WORKS_AT,
        source_id=alice.id, target_id=bigco.id,
        valid_from=datetime(2023, 6, 1),
    )
    store.create_relationship(new_job)

    return alice, acme, bigco


class TestCurrentRelationships:
    def test_returns_only_current(self, store, temporal):
        alice, acme, bigco = _setup_job_history(store)
        current = temporal.current_relationships(
            alice.id, rel_type=RelationshipType.WORKS_AT
        )
        assert len(current) == 1
        assert current[0].target_id == bigco.id


class TestRelationshipsAt:
    def test_past_point(self, store, temporal):
        alice, acme, bigco = _setup_job_history(store)
        rels = temporal.relationships_at(
            alice.id, datetime(2021, 6, 1),
            rel_type=RelationshipType.WORKS_AT,
        )
        assert len(rels) == 1
        assert rels[0].target_id == acme.id

    def test_current_point(self, store, temporal):
        alice, acme, bigco = _setup_job_history(store)
        rels = temporal.relationships_at(
            alice.id, datetime(2024, 1, 1),
            rel_type=RelationshipType.WORKS_AT,
        )
        assert len(rels) == 1
        assert rels[0].target_id == bigco.id

    def test_transition_point(self, store, temporal):
        alice, acme, bigco = _setup_job_history(store)
        # At exact transition date, old job ended, new job started
        rels = temporal.relationships_at(
            alice.id, datetime(2023, 6, 1),
            rel_type=RelationshipType.WORKS_AT,
        )
        # Old job: valid_to == 2023-06-01, so NOT valid at that exact point
        # New job: valid_from == 2023-06-01, so valid
        assert len(rels) == 1
        assert rels[0].target_id == bigco.id

    def test_before_any_job(self, store, temporal):
        alice, _, _ = _setup_job_history(store)
        rels = temporal.relationships_at(
            alice.id, datetime(2019, 1, 1),
            rel_type=RelationshipType.WORKS_AT,
        )
        assert len(rels) == 0


class TestRelationshipHistory:
    def test_returns_all_sorted(self, store, temporal):
        alice, acme, bigco = _setup_job_history(store)
        history = temporal.relationship_history(
            alice.id, rel_type=RelationshipType.WORKS_AT
        )
        assert len(history) == 2
        # Sorted by recorded_at
        assert history[0].recorded_at <= history[1].recorded_at


class TestTransitionRelationship:
    def test_transition(self, store, temporal):
        alice = Entity(type=EntityType.PERSON, properties={"name": "Alice"})
        acme = Entity(type=EntityType.ORGANIZATION, properties={"name": "Acme"})
        newco = Entity(type=EntityType.ORGANIZATION, properties={"name": "NewCo"})
        for e in [alice, acme, newco]:
            store.create_entity(e)

        old_rel = Relationship(
            type=RelationshipType.WORKS_AT,
            source_id=alice.id, target_id=acme.id,
            valid_from=datetime(2020, 1, 1),
        )
        store.create_relationship(old_rel)

        new_rel = Relationship(
            type=RelationshipType.WORKS_AT,
            source_id=alice.id, target_id=newco.id,
        )
        transition_date = datetime(2024, 3, 1)
        temporal.transition_relationship(old_rel.id, new_rel, transition_date)

        # Old relationship should be closed
        old = store.get_relationship(old_rel.id)
        assert old.valid_to == transition_date
        assert old.superseded_at is not None

        # New relationship should have valid_from
        current = temporal.current_relationships(
            alice.id, rel_type=RelationshipType.WORKS_AT
        )
        assert len(current) == 1
        assert current[0].target_id == newco.id
        assert current[0].valid_from == transition_date


class TestEntityTimeline:
    def test_timeline(self, store, temporal):
        alice, acme, bigco = _setup_job_history(store)
        timeline = temporal.entity_timeline(
            alice.id, rel_type=RelationshipType.WORKS_AT
        )
        # 2 starts + 1 end = 3 events (bigco has no end)
        assert len(timeline) == 3
        assert timeline[0]["event"] == "started"
        assert timeline[0]["with"] == "Acme Corp"
        assert timeline[-1]["event"] == "started"
        assert timeline[-1]["with"] == "BigCo"

    def test_empty_timeline(self, store, temporal):
        alice = Entity(type=EntityType.PERSON, properties={"name": "Alice"})
        store.create_entity(alice)
        timeline = temporal.entity_timeline(alice.id)
        assert timeline == []
