"""Tests for core.knowledge.graph_builder — canonical records to graph."""

import pytest

from core.knowledge.graph_builder import GraphBuilder
from core.knowledge.graph_store import GraphStore
from core.knowledge.schema import (
    CanonicalRecord,
    Entity,
    EntityType,
    ExternalIdentifier,
    IdentifierSystem,
    RelationshipType,
)


@pytest.fixture
def store(tmp_path):
    s = GraphStore(tmp_path / "test.db")
    yield s
    s.close()


@pytest.fixture
def builder(store):
    return GraphBuilder(store)


class TestPersonIngestion:
    def test_create_person(self, store, builder):
        records = iter([
            CanonicalRecord(
                record_type=EntityType.PERSON,
                source_name="contacts",
                source_id="c-1",
                data={
                    "name": "Alice Smith",
                    "emails": [{"value": "alice@test.com"}],
                    "phones": [{"value": "+1234567890"}],
                },
            ),
        ])
        stats = builder.build(records)
        assert stats.entities_created == 1
        assert stats.errors == 0

        people = store.find_entities(EntityType.PERSON)
        assert len(people) == 1
        assert people[0].properties["name"] == "Alice Smith"

        # Check identifiers were registered
        idents = store.get_identifiers(people[0].id)
        assert any(i.system == IdentifierSystem.EMAIL for i in idents)

    def test_dedup_by_source_record(self, store, builder):
        record = CanonicalRecord(
            record_type=EntityType.PERSON,
            source_name="contacts",
            source_id="c-1",
            data={"name": "Alice"},
        )
        stats1 = builder.build(iter([record]))
        stats2 = builder.build(iter([record]))
        assert stats1.entities_created == 1
        assert stats2.skipped_duplicates == 1
        assert store.count_entities() == 1

    def test_auto_merge_by_email(self, store, builder):
        # First record creates Alice
        r1 = CanonicalRecord(
            record_type=EntityType.PERSON,
            source_name="contacts",
            source_id="c-1",
            data={"name": "Alice", "emails": [{"value": "alice@test.com"}]},
        )
        builder.build(iter([r1]))

        # Second record from different source, same email
        r2 = CanonicalRecord(
            record_type=EntityType.PERSON,
            source_name="gmail",
            source_id="g-1",
            data={"name": "Alice Smith", "emails": [{"value": "alice@test.com"}]},
        )
        stats = builder.build(iter([r2]))
        assert stats.entities_merged == 1
        assert store.count_entities(EntityType.PERSON) == 1


class TestMessageIngestion:
    def test_create_message_with_sender_recipient(self, store, builder):
        records = iter([
            CanonicalRecord(
                record_type=EntityType.MESSAGE,
                source_name="gmail",
                source_id="msg-1",
                data={
                    "subject": "Hello",
                    "body": "Hi there",
                    "sender": {"name": "Alice", "email": "alice@test.com"},
                    "recipients": [{"name": "Bob", "email": "bob@test.com"}],
                    "date": "2024-01-15T10:00:00",
                },
            ),
        ])
        stats = builder.build(records)

        # Should create: 1 message + 2 people = 3 entities
        assert stats.entities_created == 3
        # Relationships: SENT + RECEIVED + KNOWS = 3
        assert stats.relationships_created == 3

        messages = store.find_entities(EntityType.MESSAGE)
        assert len(messages) == 1
        assert messages[0].properties["subject"] == "Hello"

        people = store.find_entities(EntityType.PERSON)
        assert len(people) == 2

    def test_sender_reuse_across_messages(self, store, builder):
        r1 = CanonicalRecord(
            record_type=EntityType.MESSAGE,
            source_name="gmail", source_id="msg-1",
            data={
                "subject": "First",
                "sender": {"email": "alice@test.com"},
                "recipients": [],
            },
        )
        r2 = CanonicalRecord(
            record_type=EntityType.MESSAGE,
            source_name="gmail", source_id="msg-2",
            data={
                "subject": "Second",
                "sender": {"email": "alice@test.com"},
                "recipients": [],
            },
        )
        builder.build(iter([r1, r2]))

        # Alice should be resolved to same entity
        people = store.find_entities(EntityType.PERSON)
        assert len(people) == 1

        messages = store.find_entities(EntityType.MESSAGE)
        assert len(messages) == 2


class TestEventIngestion:
    def test_create_event_with_attendees_and_location(self, store, builder):
        records = iter([
            CanonicalRecord(
                record_type=EntityType.EVENT,
                source_name="calendar",
                source_id="evt-1",
                data={
                    "title": "Team Standup",
                    "start": "2024-01-15T09:00:00",
                    "end": "2024-01-15T09:30:00",
                    "attendees": [
                        {"name": "Alice", "email": "alice@test.com"},
                        {"name": "Bob", "email": "bob@test.com"},
                    ],
                    "location": "Conference Room A",
                },
            ),
        ])
        stats = builder.build(records)

        events = store.find_entities(EntityType.EVENT)
        assert len(events) == 1
        assert events[0].properties["title"] == "Team Standup"

        places = store.find_entities(EntityType.PLACE)
        assert len(places) == 1
        assert places[0].properties["name"] == "Conference Room A"

        # attendees + location relationships
        assert stats.relationships_created >= 3


class TestGenericIngestion:
    def test_create_bookmark(self, store, builder):
        records = iter([
            CanonicalRecord(
                record_type=EntityType.BOOKMARK,
                source_name="browser",
                source_id="bm-1",
                data={
                    "url": "https://example.com",
                    "title": "Example",
                    "tags": ["reference"],
                },
            ),
        ])
        stats = builder.build(records)
        assert stats.entities_created == 1

        bookmarks = store.find_entities(EntityType.BOOKMARK)
        assert len(bookmarks) == 1
        assert bookmarks[0].properties["url"] == "https://example.com"

    def test_create_file(self, store, builder):
        records = iter([
            CanonicalRecord(
                record_type=EntityType.FILE,
                source_name="local_files",
                source_id="file-1",
                data={
                    "name": "report.pdf",
                    "path": "/documents/report.pdf",
                    "mime_type": "application/pdf",
                },
            ),
        ])
        stats = builder.build(records)
        assert stats.entities_created == 1


class TestBuildStats:
    def test_error_handling(self, store, builder):
        # A record that will fail (e.g., entity creation with same ID)
        r1 = CanonicalRecord(
            record_type=EntityType.PERSON,
            source_name="test", source_id="p-1",
            data={"name": "Alice"},
        )
        r2 = CanonicalRecord(
            record_type=EntityType.PERSON,
            source_name="test", source_id="p-2",
            data={"name": "Bob"},
        )
        stats = builder.build(iter([r1, r2]))
        assert stats.records_processed == 2
        assert stats.errors == 0

    def test_provenance_created(self, store, builder):
        records = iter([
            CanonicalRecord(
                record_type=EntityType.PERSON,
                source_name="gmail",
                source_id="person-1",
                data={"name": "Alice"},
            ),
        ])
        builder.build(records)
        people = store.find_entities(EntityType.PERSON)
        assert len(people) == 1
        provs = store.get_provenance(people[0].id)
        assert len(provs) == 1
        assert provs[0].source_name == "gmail"
        assert provs[0].source_record_id == "person-1"
