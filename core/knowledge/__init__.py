"""
Nomolo Knowledge Graph — Public API

Single entry point for the knowledge layer. Initializes the SQLite graph
and exposes high-level methods for building, querying, and managing
the knowledge graph.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Iterator

from core.knowledge.forgetter import Forgetter
from core.knowledge.graph_builder import BuildStats, GraphBuilder
from core.knowledge.graph_store import GraphStore
from core.knowledge.resolver import EntityResolver, MergeCandidate
from core.knowledge.schema import (
    Annotation,
    CanonicalRecord,
    Entity,
    EntityType,
    ExternalIdentifier,
    ForgettingRecord,
    Hypothesis,
    HypothesisStatus,
    IdentifierSystem,
    PipelineStep,
    Provenance,
    ProvenanceByType,
    Relationship,
    RelationshipType,
    ResolutionMethod,
)
from core.knowledge.temporal import TemporalQuery

DEFAULT_DB_NAME = "knowledge.db"


class KnowledgeEngine:
    """
    Main interface to the Nomolo knowledge graph.

    Usage:
        engine = KnowledgeEngine("/path/to/vault")
        stats = engine.ingest(canonical_records)
        results = engine.find_entities(EntityType.PERSON)
        engine.forget_entity(entity_id, reason="user request")
    """

    def __init__(self, vault_root: str | Path, db_name: str = DEFAULT_DB_NAME):
        self.vault_root = Path(vault_root)
        self.db_path = self.vault_root / db_name

        self.store = GraphStore(self.db_path)
        self.builder = GraphBuilder(self.store)
        self.resolver = EntityResolver(self.store)
        self.forgetter = Forgetter(self.store)
        self.temporal = TemporalQuery(self.store)

    # -------------------------------------------------------------------
    # Ingestion
    # -------------------------------------------------------------------

    def ingest(self, records: Iterator[CanonicalRecord]) -> BuildStats:
        """Ingest canonical records into the knowledge graph."""
        return self.builder.build(records)

    # -------------------------------------------------------------------
    # Entity queries
    # -------------------------------------------------------------------

    def get_entity(self, entity_id: str) -> Entity | None:
        return self.store.get_entity(entity_id)

    def find_entities(
        self,
        entity_type: EntityType | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Entity]:
        return self.store.find_entities(entity_type, limit, offset)

    def count_entities(self, entity_type: EntityType | None = None) -> int:
        return self.store.count_entities(entity_type)

    def find_by_identifier(
        self, system: IdentifierSystem | str, value: str
    ) -> Entity | None:
        return self.store.find_entity_by_identifier(system, value)

    # -------------------------------------------------------------------
    # Relationships
    # -------------------------------------------------------------------

    def get_relationships(
        self,
        entity_id: str,
        rel_type: RelationshipType | None = None,
        direction: str = "both",
        current_only: bool = True,
    ) -> list[Relationship]:
        return self.store.get_relationships(
            entity_id, rel_type, direction, current_only
        )

    def get_connections(self, entity_id: str) -> list[dict]:
        """Get all connected entities with relationship info."""
        rels = self.store.get_relationships(entity_id, current_only=True)
        connections = []
        for rel in rels:
            other_id = (
                rel.target_id if rel.source_id == entity_id else rel.source_id
            )
            other = self.store.get_entity(other_id)
            if other:
                connections.append({
                    "entity": other,
                    "relationship": rel,
                    "direction": "outgoing" if rel.source_id == entity_id else "incoming",
                })
        return connections

    # -------------------------------------------------------------------
    # Temporal
    # -------------------------------------------------------------------

    def relationships_at(
        self, entity_id: str, point_in_time: datetime
    ) -> list[Relationship]:
        return self.temporal.relationships_at(entity_id, point_in_time)

    def entity_timeline(self, entity_id: str) -> list[dict]:
        return self.temporal.entity_timeline(entity_id)

    # -------------------------------------------------------------------
    # Entity Resolution
    # -------------------------------------------------------------------

    def find_matches(self, entity: Entity) -> list[MergeCandidate]:
        return self.resolver.find_matches(entity)

    def merge_entities(self, keep_id: str, merge_id: str) -> None:
        self.resolver.merge_entities(keep_id, merge_id)

    # -------------------------------------------------------------------
    # Hypotheses
    # -------------------------------------------------------------------

    def get_open_hypotheses(self, limit: int = 50) -> list[Hypothesis]:
        return self.store.get_hypotheses(HypothesisStatus.OPEN, limit)

    def resolve_hypothesis(
        self,
        hypothesis_id: str,
        confirmed: bool,
        method: ResolutionMethod = ResolutionMethod.USER,
    ) -> None:
        status = HypothesisStatus.CONFIRMED if confirmed else HypothesisStatus.DENIED
        self.store.resolve_hypothesis(hypothesis_id, status, method)

    # -------------------------------------------------------------------
    # Provenance
    # -------------------------------------------------------------------

    def get_provenance(self, entity_id: str) -> list[Provenance]:
        return self.store.get_provenance(entity_id)

    # -------------------------------------------------------------------
    # Annotations
    # -------------------------------------------------------------------

    def get_annotations(
        self, target_id: str, field_name: str | None = None
    ) -> list[Annotation]:
        return self.store.get_annotations(target_id, field_name)

    def annotate(
        self,
        target_id: str,
        field_name: str,
        value,
        by_type: ProvenanceByType = ProvenanceByType.SYSTEM,
        by_id: str = "system",
        pipeline_step: PipelineStep = PipelineStep.ENRICHMENT,
        cost_tokens: int = 0,
        cost_usd: float = 0.0,
        parent_ids: list[str] | None = None,
    ) -> Annotation:
        ann = Annotation(
            target_id=target_id,
            field_name=field_name,
            value=value,
            by_type=by_type,
            by_id=by_id,
            cost_tokens=cost_tokens,
            cost_usd=cost_usd,
            pipeline_step=pipeline_step,
            parent_ids=parent_ids or [],
        )
        return self.store.add_annotation(ann)

    def uninstall_scroll(self, scroll_id: str) -> int:
        """Remove all annotations from a scroll. Returns count removed."""
        return self.store.remove_annotations_by_scroll(scroll_id)

    # -------------------------------------------------------------------
    # Forgetting
    # -------------------------------------------------------------------

    def forget_entity(self, entity_id: str, reason: str = "") -> ForgettingRecord:
        return self.forgetter.forget_entity(entity_id, reason)

    def disconnect_source(
        self, source_name: str, reason: str = ""
    ) -> ForgettingRecord:
        return self.forgetter.disconnect_source(source_name, reason)

    def forgetting_history(self, limit: int = 50) -> list[ForgettingRecord]:
        return self.forgetter.get_forgetting_history(limit)

    # -------------------------------------------------------------------
    # Identifiers
    # -------------------------------------------------------------------

    def add_identifier(
        self,
        entity_id: str,
        system: IdentifierSystem,
        value: str,
        verified: bool = False,
    ) -> None:
        self.store.add_identifier(
            ExternalIdentifier(
                entity_id=entity_id, system=system, value=value, verified=verified
            )
        )

    def get_identifiers(self, entity_id: str) -> list[ExternalIdentifier]:
        return self.store.get_identifiers(entity_id)

    # -------------------------------------------------------------------
    # Stats
    # -------------------------------------------------------------------

    def stats(self) -> dict[str, int]:
        return self.store.stats()

    def close(self) -> None:
        self.store.close()
