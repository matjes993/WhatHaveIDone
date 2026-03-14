"""
Nomolo Knowledge Graph — Graph Builder

Source-agnostic builder that converts CanonicalRecords into graph entities,
relationships, and provenance. Runs entity resolution on each record.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterator

from core.knowledge.graph_store import GraphStore
from core.knowledge.resolver import EntityResolver, _extract_emails, _extract_phones
from core.knowledge.schema import (
    CanonicalRecord,
    Entity,
    EntityType,
    ExternalIdentifier,
    HypothesisStatus,
    IdentifierSystem,
    Provenance,
    Relationship,
    RelationshipType,
)

logger = logging.getLogger(__name__)


@dataclass
class BuildStats:
    records_processed: int = 0
    entities_created: int = 0
    entities_merged: int = 0
    relationships_created: int = 0
    hypotheses_created: int = 0
    errors: int = 0
    skipped_duplicates: int = 0


class GraphBuilder:
    """
    Converts CanonicalRecords into knowledge graph entries.
    Runs entity resolution, creates provenance, registers identifiers.
    """

    def __init__(self, store: GraphStore):
        self.store = store
        self.resolver = EntityResolver(store)

    def build(self, records: Iterator[CanonicalRecord]) -> BuildStats:
        stats = BuildStats()
        for record in records:
            try:
                self._process_record(record, stats)
            except Exception as e:
                logger.error(f"Error processing record {record.dedup_key}: {e}")
                stats.errors += 1
        return stats

    def _process_record(self, record: CanonicalRecord, stats: BuildStats) -> None:
        # Check for duplicate by source record
        existing_provs = self.store.find_by_source_record(
            record.source_name, record.source_id
        )
        if existing_provs:
            stats.skipped_duplicates += 1
            return

        stats.records_processed += 1

        if record.record_type == EntityType.MESSAGE:
            self._process_message(record, stats)
        elif record.record_type == EntityType.EVENT:
            self._process_event(record, stats)
        elif record.record_type in (EntityType.PERSON, EntityType.ORGANIZATION):
            self._process_entity_record(record, stats)
        else:
            self._process_generic(record, stats)

    def _process_entity_record(
        self, record: CanonicalRecord, stats: BuildStats
    ) -> None:
        """Process a person/org record: create or merge entity."""
        entity = Entity(
            type=record.record_type,
            properties=dict(record.data),
        )

        # Resolve against existing entities (uses in-memory extraction, no DB writes)
        matches = self.resolver.find_matches(entity)

        if matches and self.resolver.should_auto_merge(matches[0]):
            # Auto-merge into existing entity
            existing = self.store.get_entity(matches[0].entity_id)
            if existing:
                self._merge_properties(existing, record.data)
                self.store.update_entity(existing)
                self._add_provenance(existing.id, record)
                self._register_identifiers_for(existing.id, entity)
                stats.entities_merged += 1
                return

        if matches and self.resolver.should_review(matches[0]):
            # Create hypothesis for manual review
            self.store.create_entity(entity)
            self._add_provenance(entity.id, record)
            self._register_identifiers(entity)
            hyp = self.resolver.create_merge_hypothesis(entity.id, matches[0])
            self.store.create_hypothesis(hyp)
            stats.entities_created += 1
            stats.hypotheses_created += 1
            return

        # No match — create new entity
        self.store.create_entity(entity)
        self._add_provenance(entity.id, record)
        self._register_identifiers(entity)
        stats.entities_created += 1

    def _process_message(self, record: CanonicalRecord, stats: BuildStats) -> None:
        """Process a message: create message entity + sender/recipient relationships."""
        msg_entity = Entity(
            type=EntityType.MESSAGE,
            properties=dict(record.data),
        )
        self.store.create_entity(msg_entity)
        self._add_provenance(msg_entity.id, record)
        stats.entities_created += 1

        # Link sender
        sender = record.data.get("sender")
        if sender:
            sender_id = self._resolve_or_create_person(sender, record, stats)
            if sender_id:
                rel = Relationship(
                    type=RelationshipType.SENT,
                    source_id=sender_id,
                    target_id=msg_entity.id,
                    valid_from=_parse_date(record.data.get("date")),
                )
                self.store.create_relationship(rel)
                stats.relationships_created += 1

        # Link recipients
        recipients = record.data.get("recipients", [])
        if isinstance(recipients, str):
            recipients = [recipients]
        for recip in recipients:
            recip_id = self._resolve_or_create_person(recip, record, stats)
            if recip_id:
                rel = Relationship(
                    type=RelationshipType.RECEIVED,
                    source_id=recip_id,
                    target_id=msg_entity.id,
                    valid_from=_parse_date(record.data.get("date")),
                )
                self.store.create_relationship(rel)
                stats.relationships_created += 1

        # Create KNOWS relationships between sender and recipients
        if sender:
            sender_id = self._find_person_by_contact(sender)
            if sender_id:
                for recip in recipients:
                    recip_id = self._find_person_by_contact(recip)
                    if recip_id and recip_id != sender_id:
                        existing = self.store.get_relationships(
                            sender_id,
                            rel_type=RelationshipType.KNOWS,
                            current_only=True,
                        )
                        already_knows = any(
                            r.target_id == recip_id or r.source_id == recip_id
                            for r in existing
                        )
                        if not already_knows:
                            knows_rel = Relationship(
                                type=RelationshipType.KNOWS,
                                source_id=sender_id,
                                target_id=recip_id,
                            )
                            self.store.create_relationship(knows_rel)
                            stats.relationships_created += 1

    def _process_event(self, record: CanonicalRecord, stats: BuildStats) -> None:
        """Process an event: create event entity + attendee relationships."""
        event_entity = Entity(
            type=EntityType.EVENT,
            properties=dict(record.data),
        )
        self.store.create_entity(event_entity)
        self._add_provenance(event_entity.id, record)
        stats.entities_created += 1

        # Link attendees
        attendees = record.data.get("attendees", [])
        if isinstance(attendees, str):
            attendees = [attendees]
        for attendee in attendees:
            att_id = self._resolve_or_create_person(attendee, record, stats)
            if att_id:
                rel = Relationship(
                    type=RelationshipType.ATTENDED,
                    source_id=att_id,
                    target_id=event_entity.id,
                    valid_from=_parse_date(record.data.get("start")),
                    valid_to=_parse_date(record.data.get("end")),
                )
                self.store.create_relationship(rel)
                stats.relationships_created += 1

        # Link location
        location = record.data.get("location")
        if location and isinstance(location, str) and location.strip():
            place_entity = Entity(
                type=EntityType.PLACE,
                properties={"name": location},
            )
            self.store.create_entity(place_entity)
            self._add_provenance(place_entity.id, record)
            rel = Relationship(
                type=RelationshipType.LOCATED_AT,
                source_id=event_entity.id,
                target_id=place_entity.id,
            )
            self.store.create_relationship(rel)
            stats.entities_created += 1
            stats.relationships_created += 1

    def _process_generic(self, record: CanonicalRecord, stats: BuildStats) -> None:
        """Process a generic record (file, bookmark, note, etc.)."""
        entity = Entity(
            type=record.record_type,
            properties=dict(record.data),
        )
        self.store.create_entity(entity)
        self._add_provenance(entity.id, record)
        stats.entities_created += 1

    # -----------------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------------

    def _resolve_or_create_person(
        self,
        contact_info: str | dict,
        record: CanonicalRecord,
        stats: BuildStats,
    ) -> str | None:
        """Find or create a person entity from contact info (email, name, or dict)."""
        if not contact_info:
            return None

        name = ""
        email = ""

        if isinstance(contact_info, dict):
            name = contact_info.get("name", "")
            email = contact_info.get("email", "")
        elif isinstance(contact_info, str):
            if "@" in contact_info:
                email = contact_info.strip().lower()
            else:
                name = contact_info.strip()

        # Try to find by email first (strongest identifier)
        if email:
            entity_ids = self.store.find_entities_by_identifier(
                IdentifierSystem.EMAIL, email.lower()
            )
            if entity_ids:
                return entity_ids[0]

        # Try name matching against existing people
        if name:
            temp_entity = Entity(
                type=EntityType.PERSON,
                properties={"name": name},
            )
            if email:
                temp_entity.properties["emails"] = [{"value": email}]

            matches = self.resolver.find_matches(temp_entity)
            if matches and self.resolver.should_auto_merge(matches[0]):
                return matches[0].entity_id

        # Create new person
        props: dict = {}
        if name:
            props["name"] = name
        if email:
            props["emails"] = [{"value": email}]

        if not props:
            return None

        person = Entity(type=EntityType.PERSON, properties=props)
        self.store.create_entity(person)
        self._add_provenance(person.id, record)

        if email:
            self.store.add_identifier(
                ExternalIdentifier(
                    entity_id=person.id,
                    system=IdentifierSystem.EMAIL,
                    value=email.lower(),
                    verified=True,
                )
            )

        stats.entities_created += 1
        return person.id

    def _find_person_by_contact(self, contact_info: str | dict) -> str | None:
        """Find an existing person by contact info without creating."""
        if not contact_info:
            return None

        email = ""
        if isinstance(contact_info, dict):
            email = contact_info.get("email", "")
        elif isinstance(contact_info, str) and "@" in contact_info:
            email = contact_info.strip().lower()

        if email:
            entity_ids = self.store.find_entities_by_identifier(
                IdentifierSystem.EMAIL, email.lower()
            )
            if entity_ids:
                return entity_ids[0]
        return None

    def _add_provenance(self, entity_id: str, record: CanonicalRecord) -> None:
        prov = Provenance(
            target_type="entity",
            target_id=entity_id,
            source_name=record.source_name,
            source_record_id=record.source_id,
        )
        self.store.add_provenance(prov)

    def _register_identifiers(self, entity: Entity) -> None:
        self._register_identifiers_for(entity.id, entity)

    def _register_identifiers_for(self, entity_id: str, entity: Entity) -> None:
        for email in _extract_emails(entity):
            self.store.add_identifier(
                ExternalIdentifier(
                    entity_id=entity_id,
                    system=IdentifierSystem.EMAIL,
                    value=email,
                    verified=True,
                )
            )
        for phone in _extract_phones(entity):
            self.store.add_identifier(
                ExternalIdentifier(
                    entity_id=entity_id,
                    system=IdentifierSystem.PHONE,
                    value=phone,
                    verified=True,
                )
            )

    def _merge_properties(self, entity: Entity, new_props: dict) -> None:
        for key, value in new_props.items():
            if key not in entity.properties:
                entity.properties[key] = value
            elif isinstance(entity.properties[key], list) and isinstance(value, list):
                existing_strs = {str(v) for v in entity.properties[key]}
                for v in value:
                    if str(v) not in existing_strs:
                        entity.properties[key].append(v)


def _parse_date(val: str | datetime | None) -> datetime | None:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    try:
        return datetime.fromisoformat(val)
    except (ValueError, TypeError):
        return None
