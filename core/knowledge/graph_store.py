"""
Nomolo Knowledge Graph — SQLite Graph Store

Property graph stored in SQLite: entities, relationships, provenance,
identifiers, annotations, hypotheses, and forgetting log.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

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
)


def _serialize_dt(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.isoformat()


def _parse_dt(s: str | None) -> datetime | None:
    if s is None:
        return None
    return datetime.fromisoformat(s)


def _serialize_json(obj: Any) -> str:
    return json.dumps(obj, default=str)


def _parse_json(s: str | None) -> Any:
    if s is None:
        return None
    return json.loads(s)


class GraphStore:
    """SQLite-backed property graph with provenance and temporal support."""

    def __init__(self, db_path: str | Path):
        self._db_path = str(db_path)
        self._local = threading.local()
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn") or self._local.conn is None:
            conn = sqlite3.connect(self._db_path)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            self._local.conn = conn
        return self._local.conn

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Cursor]:
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            yield cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def _init_db(self) -> None:
        with self._tx() as cur:
            cur.executescript("""
                CREATE TABLE IF NOT EXISTS entities (
                    id TEXT PRIMARY KEY,
                    type TEXT NOT NULL,
                    properties TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS relationships (
                    id TEXT PRIMARY KEY,
                    type TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    target_id TEXT NOT NULL,
                    properties TEXT NOT NULL DEFAULT '{}',
                    valid_from TEXT,
                    valid_to TEXT,
                    recorded_at TEXT NOT NULL,
                    superseded_at TEXT,
                    FOREIGN KEY (source_id) REFERENCES entities(id),
                    FOREIGN KEY (target_id) REFERENCES entities(id)
                );

                CREATE TABLE IF NOT EXISTS provenance (
                    id TEXT PRIMARY KEY,
                    target_type TEXT NOT NULL,
                    target_id TEXT NOT NULL,
                    source_name TEXT NOT NULL,
                    source_record_id TEXT NOT NULL,
                    source_field TEXT,
                    confidence REAL NOT NULL DEFAULT 1.0,
                    derivation TEXT,
                    ingested_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS identifiers (
                    entity_id TEXT NOT NULL,
                    system TEXT NOT NULL,
                    value TEXT NOT NULL,
                    verified INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (entity_id, system, value),
                    FOREIGN KEY (entity_id) REFERENCES entities(id)
                );

                CREATE TABLE IF NOT EXISTS annotations (
                    id TEXT PRIMARY KEY,
                    target_id TEXT NOT NULL,
                    field_name TEXT NOT NULL,
                    value TEXT,
                    by_type TEXT NOT NULL,
                    by_id TEXT NOT NULL,
                    by_version TEXT,
                    cost_tokens INTEGER NOT NULL DEFAULT 0,
                    cost_usd REAL NOT NULL DEFAULT 0.0,
                    created_at TEXT NOT NULL,
                    pipeline_step TEXT NOT NULL,
                    trigger_source TEXT NOT NULL DEFAULT 'on_ingest',
                    parent_ids TEXT NOT NULL DEFAULT '[]',
                    status TEXT NOT NULL DEFAULT 'active'
                );

                CREATE TABLE IF NOT EXISTS hypotheses (
                    id TEXT PRIMARY KEY,
                    type TEXT NOT NULL,
                    entity_ids TEXT NOT NULL DEFAULT '[]',
                    confidence REAL NOT NULL DEFAULT 0.5,
                    evidence TEXT NOT NULL DEFAULT '{}',
                    status TEXT NOT NULL DEFAULT 'open',
                    resolution TEXT,
                    created_at TEXT NOT NULL,
                    resolved_at TEXT
                );

                CREATE TABLE IF NOT EXISTS forgetting_log (
                    id TEXT PRIMARY KEY,
                    action TEXT NOT NULL,
                    target_description TEXT NOT NULL,
                    entities_removed INTEGER NOT NULL DEFAULT 0,
                    relationships_removed INTEGER NOT NULL DEFAULT 0,
                    annotations_removed INTEGER NOT NULL DEFAULT 0,
                    reason TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_rel_source ON relationships(source_id);
                CREATE INDEX IF NOT EXISTS idx_rel_target ON relationships(target_id);
                CREATE INDEX IF NOT EXISTS idx_rel_type ON relationships(type);
                CREATE INDEX IF NOT EXISTS idx_prov_target ON provenance(target_id);
                CREATE INDEX IF NOT EXISTS idx_prov_source_name ON provenance(source_name);
                CREATE INDEX IF NOT EXISTS idx_prov_source_record ON provenance(source_name, source_record_id);
                CREATE INDEX IF NOT EXISTS idx_ann_target ON annotations(target_id);
                CREATE INDEX IF NOT EXISTS idx_ann_by_id ON annotations(by_id);
                CREATE INDEX IF NOT EXISTS idx_ann_status ON annotations(status);
                CREATE INDEX IF NOT EXISTS idx_hyp_status ON hypotheses(status);
                CREATE INDEX IF NOT EXISTS idx_ident_system_value ON identifiers(system, value);
                CREATE INDEX IF NOT EXISTS idx_entities_type ON entities(type);
            """)

    # -----------------------------------------------------------------------
    # Entities
    # -----------------------------------------------------------------------

    def create_entity(self, entity: Entity) -> Entity:
        with self._tx() as cur:
            cur.execute(
                "INSERT INTO entities (id, type, properties, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (
                    entity.id,
                    entity.type.value if isinstance(entity.type, EntityType) else entity.type,
                    _serialize_json(entity.properties),
                    _serialize_dt(entity.created_at),
                    _serialize_dt(entity.updated_at),
                ),
            )
        return entity

    def get_entity(self, entity_id: str) -> Entity | None:
        conn = self._get_conn()
        row = conn.execute(
            "SELECT * FROM entities WHERE id = ?", (entity_id,)
        ).fetchone()
        if row is None:
            return None
        return self._row_to_entity(row)

    def update_entity(self, entity: Entity) -> None:
        entity.updated_at = datetime.utcnow()
        with self._tx() as cur:
            cur.execute(
                "UPDATE entities SET type=?, properties=?, updated_at=? WHERE id=?",
                (
                    entity.type.value if isinstance(entity.type, EntityType) else entity.type,
                    _serialize_json(entity.properties),
                    _serialize_dt(entity.updated_at),
                    entity.id,
                ),
            )

    def delete_entity(self, entity_id: str) -> None:
        with self._tx() as cur:
            cur.execute("DELETE FROM identifiers WHERE entity_id = ?", (entity_id,))
            cur.execute(
                "DELETE FROM relationships WHERE source_id = ? OR target_id = ?",
                (entity_id, entity_id),
            )
            cur.execute("DELETE FROM provenance WHERE target_id = ?", (entity_id,))
            cur.execute("DELETE FROM annotations WHERE target_id = ?", (entity_id,))
            cur.execute("DELETE FROM entities WHERE id = ?", (entity_id,))

    def find_entities(
        self,
        entity_type: EntityType | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Entity]:
        conn = self._get_conn()
        if entity_type:
            rows = conn.execute(
                "SELECT * FROM entities WHERE type = ? ORDER BY updated_at DESC LIMIT ? OFFSET ?",
                (entity_type.value, limit, offset),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM entities ORDER BY updated_at DESC LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()
        return [self._row_to_entity(r) for r in rows]

    def count_entities(self, entity_type: EntityType | None = None) -> int:
        conn = self._get_conn()
        if entity_type:
            row = conn.execute(
                "SELECT COUNT(*) FROM entities WHERE type = ?",
                (entity_type.value,),
            ).fetchone()
        else:
            row = conn.execute("SELECT COUNT(*) FROM entities").fetchone()
        return row[0]

    def count_relationships(self) -> int:
        conn = self._get_conn()
        row = conn.execute("SELECT COUNT(*) FROM relationships").fetchone()
        return row[0]

    def find_entity_by_identifier(
        self, system: IdentifierSystem | str, value: str
    ) -> Entity | None:
        conn = self._get_conn()
        sys_val = system.value if isinstance(system, IdentifierSystem) else system
        row = conn.execute(
            "SELECT e.* FROM entities e "
            "JOIN identifiers i ON e.id = i.entity_id "
            "WHERE i.system = ? AND i.value = ? "
            "ORDER BY length(e.properties) DESC LIMIT 1",
            (sys_val, value),
        ).fetchone()
        if row is None:
            return None
        return self._row_to_entity(row)

    def _row_to_entity(self, row: sqlite3.Row) -> Entity:
        return Entity(
            id=row["id"],
            type=EntityType(row["type"]),
            properties=_parse_json(row["properties"]),
            created_at=_parse_dt(row["created_at"]),
            updated_at=_parse_dt(row["updated_at"]),
        )

    # -----------------------------------------------------------------------
    # Relationships
    # -----------------------------------------------------------------------

    def create_relationship(self, rel: Relationship) -> Relationship:
        with self._tx() as cur:
            cur.execute(
                "INSERT INTO relationships "
                "(id, type, source_id, target_id, properties, "
                "valid_from, valid_to, recorded_at, superseded_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    rel.id,
                    rel.type.value if isinstance(rel.type, RelationshipType) else rel.type,
                    rel.source_id,
                    rel.target_id,
                    _serialize_json(rel.properties),
                    _serialize_dt(rel.valid_from),
                    _serialize_dt(rel.valid_to),
                    _serialize_dt(rel.recorded_at),
                    _serialize_dt(rel.superseded_at),
                ),
            )
        return rel

    def get_relationship(self, rel_id: str) -> Relationship | None:
        conn = self._get_conn()
        row = conn.execute(
            "SELECT * FROM relationships WHERE id = ?", (rel_id,)
        ).fetchone()
        if row is None:
            return None
        return self._row_to_relationship(row)

    def get_relationships(
        self,
        entity_id: str,
        rel_type: RelationshipType | None = None,
        direction: str = "both",  # "outgoing" | "incoming" | "both"
        current_only: bool = True,
    ) -> list[Relationship]:
        conn = self._get_conn()
        conditions = []
        params: list[Any] = []

        if direction in ("outgoing", "both"):
            conditions.append("source_id = ?")
            params.append(entity_id)
        if direction in ("incoming", "both"):
            conditions.append("target_id = ?")
            params.append(entity_id)

        where = " OR ".join(conditions)
        if len(conditions) > 1:
            where = f"({where})"

        if rel_type:
            where += " AND type = ?"
            params.append(rel_type.value)

        if current_only:
            where += " AND superseded_at IS NULL AND valid_to IS NULL"

        rows = conn.execute(
            f"SELECT * FROM relationships WHERE {where} ORDER BY recorded_at DESC",
            params,
        ).fetchall()
        return [self._row_to_relationship(r) for r in rows]

    def invalidate_relationship(self, rel_id: str, valid_to: datetime | None = None) -> None:
        now = datetime.utcnow()
        with self._tx() as cur:
            cur.execute(
                "UPDATE relationships SET valid_to = ?, superseded_at = ? WHERE id = ?",
                (_serialize_dt(valid_to or now), _serialize_dt(now), rel_id),
            )

    def _row_to_relationship(self, row: sqlite3.Row) -> Relationship:
        return Relationship(
            id=row["id"],
            type=RelationshipType(row["type"]),
            source_id=row["source_id"],
            target_id=row["target_id"],
            properties=_parse_json(row["properties"]),
            valid_from=_parse_dt(row["valid_from"]),
            valid_to=_parse_dt(row["valid_to"]),
            recorded_at=_parse_dt(row["recorded_at"]),
            superseded_at=_parse_dt(row["superseded_at"]),
        )

    # -----------------------------------------------------------------------
    # Provenance
    # -----------------------------------------------------------------------

    def add_provenance(self, prov: Provenance) -> Provenance:
        with self._tx() as cur:
            cur.execute(
                "INSERT INTO provenance "
                "(id, target_type, target_id, source_name, source_record_id, "
                "source_field, confidence, derivation, ingested_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    prov.id,
                    prov.target_type,
                    prov.target_id,
                    prov.source_name,
                    prov.source_record_id,
                    prov.source_field,
                    prov.confidence,
                    prov.derivation,
                    _serialize_dt(prov.ingested_at),
                ),
            )
        return prov

    def get_provenance(self, target_id: str) -> list[Provenance]:
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM provenance WHERE target_id = ? ORDER BY ingested_at DESC",
            (target_id,),
        ).fetchall()
        return [self._row_to_provenance(r) for r in rows]

    def get_provenance_by_source(self, source_name: str) -> list[Provenance]:
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM provenance WHERE source_name = ?", (source_name,)
        ).fetchall()
        return [self._row_to_provenance(r) for r in rows]

    def delete_provenance_by_source(self, source_name: str) -> int:
        with self._tx() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM provenance WHERE source_name = ?",
                (source_name,),
            )
            count = cur.fetchone()[0]
            cur.execute(
                "DELETE FROM provenance WHERE source_name = ?", (source_name,)
            )
        return count

    def has_provenance(self, target_id: str) -> bool:
        conn = self._get_conn()
        row = conn.execute(
            "SELECT COUNT(*) FROM provenance WHERE target_id = ?", (target_id,)
        ).fetchone()
        return row[0] > 0

    def find_by_source_record(
        self, source_name: str, source_record_id: str
    ) -> list[Provenance]:
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM provenance WHERE source_name = ? AND source_record_id = ?",
            (source_name, source_record_id),
        ).fetchall()
        return [self._row_to_provenance(r) for r in rows]

    def _row_to_provenance(self, row: sqlite3.Row) -> Provenance:
        return Provenance(
            id=row["id"],
            target_type=row["target_type"],
            target_id=row["target_id"],
            source_name=row["source_name"],
            source_record_id=row["source_record_id"],
            source_field=row["source_field"],
            confidence=row["confidence"],
            derivation=row["derivation"],
            ingested_at=_parse_dt(row["ingested_at"]),
        )

    # -----------------------------------------------------------------------
    # External Identifiers
    # -----------------------------------------------------------------------

    def add_identifier(self, ident: ExternalIdentifier) -> None:
        with self._tx() as cur:
            cur.execute(
                "INSERT OR IGNORE INTO identifiers (entity_id, system, value, verified) "
                "VALUES (?, ?, ?, ?)",
                (
                    ident.entity_id,
                    ident.system.value if isinstance(ident.system, IdentifierSystem) else ident.system,
                    ident.value,
                    1 if ident.verified else 0,
                ),
            )

    def get_identifiers(self, entity_id: str) -> list[ExternalIdentifier]:
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM identifiers WHERE entity_id = ?", (entity_id,)
        ).fetchall()
        return [
            ExternalIdentifier(
                entity_id=r["entity_id"],
                system=IdentifierSystem(r["system"]) if r["system"] in IdentifierSystem._value2member_map_ else IdentifierSystem.CUSTOM,
                value=r["value"],
                verified=bool(r["verified"]),
            )
            for r in rows
        ]

    def find_entities_by_identifier(
        self, system: IdentifierSystem | str, value: str
    ) -> list[str]:
        conn = self._get_conn()
        sys_val = system.value if isinstance(system, IdentifierSystem) else system
        rows = conn.execute(
            "SELECT entity_id FROM identifiers WHERE system = ? AND value = ?",
            (sys_val, value),
        ).fetchall()
        return [r["entity_id"] for r in rows]

    # -----------------------------------------------------------------------
    # Annotations
    # -----------------------------------------------------------------------

    def add_annotation(self, ann: Annotation) -> Annotation:
        with self._tx() as cur:
            cur.execute(
                "INSERT INTO annotations "
                "(id, target_id, field_name, value, by_type, by_id, by_version, "
                "cost_tokens, cost_usd, created_at, pipeline_step, trigger_source, "
                "parent_ids, status) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    ann.id,
                    ann.target_id,
                    ann.field_name,
                    _serialize_json(ann.value),
                    ann.by_type.value if isinstance(ann.by_type, ProvenanceByType) else ann.by_type,
                    ann.by_id,
                    ann.by_version,
                    ann.cost_tokens,
                    ann.cost_usd,
                    _serialize_dt(ann.created_at),
                    ann.pipeline_step.value if isinstance(ann.pipeline_step, PipelineStep) else ann.pipeline_step,
                    ann.trigger,
                    _serialize_json(ann.parent_ids),
                    ann.status.value if isinstance(ann.status, AnnotationStatus) else ann.status,
                ),
            )
        return ann

    def get_annotations(
        self,
        target_id: str,
        field_name: str | None = None,
        active_only: bool = True,
    ) -> list[Annotation]:
        conn = self._get_conn()
        query = "SELECT * FROM annotations WHERE target_id = ?"
        params: list[Any] = [target_id]
        if field_name:
            query += " AND field_name = ?"
            params.append(field_name)
        if active_only:
            query += " AND status = 'active'"
        query += " ORDER BY created_at DESC"
        rows = conn.execute(query, params).fetchall()
        return [self._row_to_annotation(r) for r in rows]

    def get_effective_value(self, target_id: str, field_name: str) -> Any | None:
        annotations = self.get_annotations(target_id, field_name, active_only=True)
        if not annotations:
            return None
        return annotations[0].value  # most recent active annotation

    def supersede_annotation(self, annotation_id: str) -> None:
        with self._tx() as cur:
            cur.execute(
                "UPDATE annotations SET status = ? WHERE id = ?",
                (AnnotationStatus.SUPERSEDED.value, annotation_id),
            )

    def remove_annotations_by_scroll(self, scroll_id: str) -> int:
        with self._tx() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM annotations WHERE by_id LIKE ? AND status = 'active'",
                (f"{scroll_id}%",),
            )
            count = cur.fetchone()[0]
            cur.execute(
                "UPDATE annotations SET status = ? WHERE by_id LIKE ? AND status = 'active'",
                (AnnotationStatus.REMOVED.value, f"{scroll_id}%"),
            )
        return count

    def _row_to_annotation(self, row: sqlite3.Row) -> Annotation:
        return Annotation(
            id=row["id"],
            target_id=row["target_id"],
            field_name=row["field_name"],
            value=_parse_json(row["value"]),
            by_type=ProvenanceByType(row["by_type"]),
            by_id=row["by_id"],
            by_version=row["by_version"],
            cost_tokens=row["cost_tokens"],
            cost_usd=row["cost_usd"],
            created_at=_parse_dt(row["created_at"]),
            pipeline_step=PipelineStep(row["pipeline_step"]),
            trigger=row["trigger_source"],
            parent_ids=_parse_json(row["parent_ids"]),
            status=AnnotationStatus(row["status"]),
        )

    # -----------------------------------------------------------------------
    # Hypotheses
    # -----------------------------------------------------------------------

    def create_hypothesis(self, hyp: Hypothesis) -> Hypothesis:
        with self._tx() as cur:
            cur.execute(
                "INSERT INTO hypotheses "
                "(id, type, entity_ids, confidence, evidence, status, "
                "resolution, created_at, resolved_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    hyp.id,
                    hyp.type.value if isinstance(hyp.type, HypothesisType) else hyp.type,
                    _serialize_json(hyp.entity_ids),
                    hyp.confidence,
                    _serialize_json(hyp.evidence),
                    hyp.status.value if isinstance(hyp.status, HypothesisStatus) else hyp.status,
                    hyp.resolution.value if hyp.resolution else None,
                    _serialize_dt(hyp.created_at),
                    _serialize_dt(hyp.resolved_at),
                ),
            )
        return hyp

    def get_hypotheses(
        self, status: HypothesisStatus | None = None, limit: int = 50
    ) -> list[Hypothesis]:
        conn = self._get_conn()
        if status:
            rows = conn.execute(
                "SELECT * FROM hypotheses WHERE status = ? "
                "ORDER BY confidence DESC LIMIT ?",
                (status.value, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM hypotheses ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [self._row_to_hypothesis(r) for r in rows]

    def resolve_hypothesis(
        self,
        hypothesis_id: str,
        status: HypothesisStatus,
        resolution: ResolutionMethod,
    ) -> None:
        with self._tx() as cur:
            cur.execute(
                "UPDATE hypotheses SET status = ?, resolution = ?, resolved_at = ? "
                "WHERE id = ?",
                (
                    status.value,
                    resolution.value,
                    _serialize_dt(datetime.utcnow()),
                    hypothesis_id,
                ),
            )

    def _row_to_hypothesis(self, row: sqlite3.Row) -> Hypothesis:
        resolution = None
        if row["resolution"]:
            resolution = ResolutionMethod(row["resolution"])
        return Hypothesis(
            id=row["id"],
            type=HypothesisType(row["type"]),
            entity_ids=_parse_json(row["entity_ids"]),
            confidence=row["confidence"],
            evidence=_parse_json(row["evidence"]),
            status=HypothesisStatus(row["status"]),
            resolution=resolution,
            created_at=_parse_dt(row["created_at"]),
            resolved_at=_parse_dt(row["resolved_at"]),
        )

    # -----------------------------------------------------------------------
    # Forgetting Log
    # -----------------------------------------------------------------------

    def log_forgetting(self, record: ForgettingRecord) -> None:
        with self._tx() as cur:
            cur.execute(
                "INSERT INTO forgetting_log "
                "(id, action, target_description, entities_removed, "
                "relationships_removed, annotations_removed, reason, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    record.id,
                    record.action,
                    record.target_description,
                    record.entities_removed,
                    record.relationships_removed,
                    record.annotations_removed,
                    record.reason,
                    _serialize_dt(record.created_at),
                ),
            )

    def get_forgetting_log(self, limit: int = 50) -> list[ForgettingRecord]:
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM forgetting_log ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [
            ForgettingRecord(
                id=r["id"],
                action=r["action"],
                target_description=r["target_description"],
                entities_removed=r["entities_removed"],
                relationships_removed=r["relationships_removed"],
                annotations_removed=r["annotations_removed"],
                reason=r["reason"],
                created_at=_parse_dt(r["created_at"]),
            )
            for r in rows
        ]

    # -----------------------------------------------------------------------
    # Garbage Collection
    # -----------------------------------------------------------------------

    def garbage_collect_orphans(self) -> list[str]:
        """Remove entities that have zero provenance records."""
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT e.id FROM entities e "
            "LEFT JOIN provenance p ON e.id = p.target_id AND p.target_type = 'entity' "
            "WHERE p.id IS NULL"
        ).fetchall()
        orphan_ids = [r["id"] for r in rows]
        for eid in orphan_ids:
            self.delete_entity(eid)
        return orphan_ids

    # -----------------------------------------------------------------------
    # Stats
    # -----------------------------------------------------------------------

    def stats(self) -> dict[str, int]:
        conn = self._get_conn()
        return {
            "entities": conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0],
            "relationships": conn.execute("SELECT COUNT(*) FROM relationships").fetchone()[0],
            "provenance": conn.execute("SELECT COUNT(*) FROM provenance").fetchone()[0],
            "identifiers": conn.execute("SELECT COUNT(*) FROM identifiers").fetchone()[0],
            "annotations": conn.execute(
                "SELECT COUNT(*) FROM annotations WHERE status = 'active'"
            ).fetchone()[0],
            "hypotheses_open": conn.execute(
                "SELECT COUNT(*) FROM hypotheses WHERE status = 'open'"
            ).fetchone()[0],
            "forgetting_events": conn.execute(
                "SELECT COUNT(*) FROM forgetting_log"
            ).fetchone()[0],
        }

    def close(self) -> None:
        if hasattr(self._local, "conn") and self._local.conn:
            self._local.conn.close()
            self._local.conn = None
