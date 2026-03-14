"""
Nomolo Knowledge Graph — Temporal Query Helpers

Bitemporal query support for relationships that change over time.
Tracks both valid time (real-world truth) and system time (when we learned it).
"""

from __future__ import annotations

from datetime import datetime

from core.knowledge.graph_store import GraphStore
from core.knowledge.schema import Relationship, RelationshipType


class TemporalQuery:
    """Helpers for querying the knowledge graph across time dimensions."""

    def __init__(self, store: GraphStore):
        self.store = store

    def current_relationships(
        self,
        entity_id: str,
        rel_type: RelationshipType | None = None,
        direction: str = "both",
    ) -> list[Relationship]:
        """Get relationships that are currently valid (valid_to IS NULL, not superseded)."""
        return self.store.get_relationships(
            entity_id, rel_type=rel_type, direction=direction, current_only=True
        )

    def relationships_at(
        self,
        entity_id: str,
        point_in_time: datetime,
        rel_type: RelationshipType | None = None,
        direction: str = "both",
    ) -> list[Relationship]:
        """
        Get relationships that were valid at a specific point in time.
        A relationship is valid at time T if:
          valid_from <= T AND (valid_to IS NULL OR valid_to > T)
          AND superseded_at IS NULL (we want the current version of the record)
        """
        all_rels = self.store.get_relationships(
            entity_id, rel_type=rel_type, direction=direction, current_only=False
        )
        result = []
        for rel in all_rels:
            if rel.superseded_at is not None:
                continue
            if rel.valid_from and rel.valid_from > point_in_time:
                continue
            if rel.valid_to and rel.valid_to <= point_in_time:
                continue
            result.append(rel)
        return result

    def relationship_history(
        self,
        entity_id: str,
        rel_type: RelationshipType | None = None,
        direction: str = "both",
    ) -> list[Relationship]:
        """
        Get the full history of relationships for an entity,
        including superseded and ended ones. Sorted chronologically.
        """
        rels = self.store.get_relationships(
            entity_id, rel_type=rel_type, direction=direction, current_only=False
        )
        return sorted(rels, key=lambda r: r.recorded_at or datetime.min)

    def transition_relationship(
        self,
        old_rel_id: str,
        new_rel: Relationship,
        transition_date: datetime | None = None,
    ) -> Relationship:
        """
        Transition from an old relationship to a new one.
        Closes the old relationship and creates the new one.

        Example: person changes jobs
          old: WORKS_AT Company A (valid_from=2020)
          new: WORKS_AT Company B (valid_from=2024)
          → old gets valid_to=2024, new gets valid_from=2024
        """
        now = transition_date or datetime.utcnow()

        # Close old relationship
        self.store.invalidate_relationship(old_rel_id, valid_to=now)

        # Set valid_from on new relationship
        new_rel.valid_from = now

        # Create new relationship
        return self.store.create_relationship(new_rel)

    def entity_timeline(
        self,
        entity_id: str,
        rel_type: RelationshipType | None = None,
    ) -> list[dict]:
        """
        Build a timeline of changes for an entity.
        Returns events sorted chronologically.
        """
        history = self.relationship_history(entity_id, rel_type=rel_type)
        timeline = []

        for rel in history:
            other_id = (
                rel.target_id if rel.source_id == entity_id else rel.source_id
            )
            other = self.store.get_entity(other_id)
            other_name = other.properties.get("name", other_id) if other else other_id

            if rel.valid_from:
                timeline.append({
                    "date": rel.valid_from,
                    "event": "started",
                    "relationship": rel.type.value,
                    "with": other_name,
                    "with_id": other_id,
                    "relationship_id": rel.id,
                })
            if rel.valid_to:
                timeline.append({
                    "date": rel.valid_to,
                    "event": "ended",
                    "relationship": rel.type.value,
                    "with": other_name,
                    "with_id": other_id,
                    "relationship_id": rel.id,
                })

        timeline.sort(key=lambda e: e["date"])
        return timeline
