"""
Nomolo Knowledge Graph — Forgetter

Cascade-aware deletion with forgetting log.
Supports: forget entity, disconnect source, user-requested deletion.
"""

from __future__ import annotations

import logging
from datetime import datetime

from core.knowledge.graph_store import GraphStore
from core.knowledge.schema import ForgettingRecord

logger = logging.getLogger(__name__)


class Forgetter:
    """Handles data deletion with cascade awareness and permanent logging."""

    def __init__(self, store: GraphStore):
        self.store = store

    def forget_entity(self, entity_id: str, reason: str = "") -> ForgettingRecord:
        """
        Forget everything about a specific entity.
        Deletes the entity, all its relationships, provenance, annotations,
        and identifiers. Logs the forgetting event permanently.
        """
        entity = self.store.get_entity(entity_id)
        if not entity:
            raise ValueError(f"Entity {entity_id} not found")

        # Count what will be removed
        rels = self.store.get_relationships(entity_id, current_only=False)
        annotations = self.store.get_annotations(entity_id, active_only=False)

        description = (
            f"Forgot {entity.type.value} entity: "
            f"{entity.properties.get('name', entity.id)}"
        )

        record = ForgettingRecord(
            action="forget_entity",
            target_description=description,
            entities_removed=1,
            relationships_removed=len(rels),
            annotations_removed=len(annotations),
            reason=reason,
        )

        # Delete everything (graph_store.delete_entity handles cascade)
        self.store.delete_entity(entity_id)

        # Log permanently
        self.store.log_forgetting(record)
        logger.info(f"Forgot entity {entity_id}: {description}")

        return record

    def disconnect_source(self, source_name: str, reason: str = "") -> ForgettingRecord:
        """
        Disconnect a data source. Removes all provenance from that source,
        then garbage-collects any entities that have zero remaining provenance.
        """
        # Count provenance records
        prov_count = self.store.delete_provenance_by_source(source_name)

        # Garbage collect orphaned entities
        orphans = self.store.garbage_collect_orphans()

        # Count removed annotations and relationships for orphans
        # (they're already deleted by garbage_collect_orphans via delete_entity)

        record = ForgettingRecord(
            action="disconnect_source",
            target_description=f"Disconnected source: {source_name} "
                               f"({prov_count} provenance records removed, "
                               f"{len(orphans)} orphaned entities deleted)",
            entities_removed=len(orphans),
            relationships_removed=0,  # counted within orphan deletion
            annotations_removed=0,
            reason=reason,
        )

        self.store.log_forgetting(record)
        logger.info(
            f"Disconnected {source_name}: removed {prov_count} provenance, "
            f"{len(orphans)} orphans"
        )

        return record

    def forget_by_criteria(
        self,
        entity_type: str | None = None,
        source_name: str | None = None,
        older_than: datetime | None = None,
        reason: str = "",
    ) -> ForgettingRecord:
        """
        Forget entities matching criteria. Useful for bulk cleanup.
        At least one criterion must be provided.
        """
        if not any([entity_type, source_name, older_than]):
            raise ValueError("At least one criterion required")

        conn = self.store._get_conn()
        conditions = []
        params = []

        query = "SELECT DISTINCT e.id FROM entities e"

        if source_name:
            query += " JOIN provenance p ON e.id = p.target_id"
            conditions.append("p.source_name = ?")
            params.append(source_name)

        if entity_type:
            conditions.append("e.type = ?")
            params.append(entity_type)

        if older_than:
            conditions.append("e.updated_at < ?")
            params.append(older_than.isoformat())

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        rows = conn.execute(query, params).fetchall()
        entity_ids = [r["id"] for r in rows]

        total_rels = 0
        total_anns = 0
        for eid in entity_ids:
            rels = self.store.get_relationships(eid, current_only=False)
            anns = self.store.get_annotations(eid, active_only=False)
            total_rels += len(rels)
            total_anns += len(anns)
            self.store.delete_entity(eid)

        criteria_desc = []
        if entity_type:
            criteria_desc.append(f"type={entity_type}")
        if source_name:
            criteria_desc.append(f"source={source_name}")
        if older_than:
            criteria_desc.append(f"older_than={older_than.isoformat()}")

        record = ForgettingRecord(
            action="forget_by_criteria",
            target_description=f"Bulk forget: {', '.join(criteria_desc)} "
                               f"({len(entity_ids)} entities)",
            entities_removed=len(entity_ids),
            relationships_removed=total_rels,
            annotations_removed=total_anns,
            reason=reason,
        )

        self.store.log_forgetting(record)
        logger.info(f"Bulk forget: {len(entity_ids)} entities matching {criteria_desc}")

        return record

    def get_forgetting_history(self, limit: int = 50) -> list[ForgettingRecord]:
        """Get the forgetting log — permanent record of what was deleted."""
        return self.store.get_forgetting_log(limit=limit)
