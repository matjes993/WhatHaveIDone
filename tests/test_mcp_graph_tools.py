"""
Tests for knowledge graph MCP server tools.

Tests the graph handler functions by extracting them from mcp_server.py
and running them against a real KnowledgeEngine with test vault data.
Avoids importing mcp_server.py directly (which requires chromadb).
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest
import asyncio

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.knowledge import KnowledgeEngine
from core.knowledge.adapters import adapt_all, read_vault_jsonl
from core.knowledge.schema import (
    EntityType,
    HypothesisStatus,
    IdentifierSystem,
    RelationshipType,
    ResolutionMethod,
)

VAULT_DIR = Path(__file__).parent / "fixtures" / "vault"


# ---------------------------------------------------------------------------
# Minimal TextContent mock (avoids importing mcp.types)
# ---------------------------------------------------------------------------

@dataclass
class TextContent:
    type: str
    text: str


# ---------------------------------------------------------------------------
# Extract handler functions from mcp_server.py without importing it
# (avoids chromadb/mcp dependency chain)
# ---------------------------------------------------------------------------

_knowledge: KnowledgeEngine | None = None


def _entity_summary(entity) -> dict:
    summary = {"id": entity.id, "type": entity.type.value}
    props = entity.properties
    for field in ["name", "title", "subject", "url", "body"]:
        if field in props and props[field]:
            val = props[field]
            if field == "body" and isinstance(val, str) and len(val) > 200:
                val = val[:200] + "..."
            summary[field] = val
    return summary


async def _handle_graph_stats(args):
    if not _knowledge:
        return [TextContent(type="text", text='{"error": "Knowledge graph not initialized."}')]
    stats = _knowledge.stats()
    breakdown = {}
    for et in EntityType:
        count = _knowledge.count_entities(et)
        if count > 0:
            breakdown[et.value] = count
    output = {
        "totals": stats,
        "entity_breakdown": breakdown,
        "hypotheses_open": len(_knowledge.get_open_hypotheses(limit=1000)),
    }
    return [TextContent(type="text", text=json.dumps(output, ensure_ascii=False))]


async def _handle_find_people(args):
    if not _knowledge:
        return [TextContent(type="text", text='{"error": "Knowledge graph not initialized."}')]
    email = args.get("email", "")
    phone = args.get("phone", "")
    name = args.get("name", "")
    limit = min(args.get("limit", 20), 100)
    results = []
    if email:
        entity = _knowledge.find_by_identifier(IdentifierSystem.EMAIL, email.lower())
        if entity:
            results.append(entity)
    if phone and not results:
        entity = _knowledge.find_by_identifier(IdentifierSystem.PHONE, phone)
        if entity:
            results.append(entity)
    if name or (not email and not phone):
        people = _knowledge.find_entities(EntityType.PERSON, limit=500)
        name_lower = name.lower() if name else ""
        for person in people:
            if len(results) >= limit:
                break
            if person in results:
                continue
            person_name = person.properties.get("name", "")
            if not name or (person_name and name_lower in person_name.lower()):
                results.append(person)
    output = []
    for entity in results[:limit]:
        entry = _entity_summary(entity)
        identifiers = _knowledge.get_identifiers(entity.id)
        if identifiers:
            entry["identifiers"] = [{"system": i.system.value, "value": i.value} for i in identifiers]
        provs = _knowledge.get_provenance(entity.id)
        if provs:
            entry["sources"] = list({p.source_name for p in provs})
        output.append(entry)
    return [TextContent(type="text", text=json.dumps({"people": output, "total": len(output)}, ensure_ascii=False, default=str))]


async def _handle_get_graph_entity(args):
    if not _knowledge:
        return [TextContent(type="text", text='{"error": "Knowledge graph not initialized."}')]
    entity_id = args.get("entity_id", "")
    if not entity_id:
        return [TextContent(type="text", text='{"error": "entity_id is required."}')]
    entity = _knowledge.get_entity(entity_id)
    if not entity:
        return [TextContent(type="text", text=f'{{"error": "Entity not found: {entity_id}"}}')]
    output = {"id": entity.id, "type": entity.type.value, "properties": entity.properties, "created_at": entity.created_at.isoformat() if entity.created_at else None}
    identifiers = _knowledge.get_identifiers(entity.id)
    if identifiers:
        output["identifiers"] = [{"system": i.system.value, "value": i.value, "verified": i.verified} for i in identifiers]
    provs = _knowledge.get_provenance(entity.id)
    if provs:
        output["provenance"] = [{"source": p.source_name, "source_record_id": p.source_record_id, "confidence": p.confidence, "ingested_at": p.ingested_at.isoformat() if p.ingested_at else None} for p in provs]
    rels = _knowledge.get_relationships(entity.id, current_only=True)
    if rels:
        rel_counts = {}
        for r in rels:
            rel_counts[r.type.value] = rel_counts.get(r.type.value, 0) + 1
        output["relationship_summary"] = rel_counts
        output["total_connections"] = len(rels)
    return [TextContent(type="text", text=json.dumps(output, ensure_ascii=False, default=str))]


async def _handle_get_connections(args):
    if not _knowledge:
        return [TextContent(type="text", text='{"error": "Knowledge graph not initialized."}')]
    entity_id = args.get("entity_id", "")
    if not entity_id:
        return [TextContent(type="text", text='{"error": "entity_id is required."}')]
    entity = _knowledge.get_entity(entity_id)
    if not entity:
        return [TextContent(type="text", text=f'{{"error": "Entity not found: {entity_id}"}}')]
    rel_type_str = args.get("relationship_type")
    include_history = args.get("include_history", False)
    rel_type = None
    if rel_type_str:
        try:
            rel_type = RelationshipType(rel_type_str)
        except ValueError:
            return [TextContent(type="text", text=f'{{"error": "Unknown relationship type: {rel_type_str}"}}')]
    rels = _knowledge.get_relationships(entity_id, rel_type=rel_type, current_only=not include_history)
    connections = []
    for rel in rels:
        other_id = rel.target_id if rel.source_id == entity_id else rel.source_id
        other = _knowledge.get_entity(other_id)
        conn = {"relationship": rel.type.value, "direction": "outgoing" if rel.source_id == entity_id else "incoming", "entity": _entity_summary(other) if other else {"id": other_id, "type": "unknown"}}
        if rel.valid_from:
            conn["since"] = rel.valid_from.isoformat()
        if rel.valid_to:
            conn["until"] = rel.valid_to.isoformat()
        connections.append(conn)
    output = {"entity": _entity_summary(entity), "connections": connections, "total": len(connections)}
    return [TextContent(type="text", text=json.dumps(output, ensure_ascii=False, default=str))]


async def _handle_entity_timeline(args):
    if not _knowledge:
        return [TextContent(type="text", text='{"error": "Knowledge graph not initialized."}')]
    entity_id = args.get("entity_id", "")
    if not entity_id:
        return [TextContent(type="text", text='{"error": "entity_id is required."}')]
    entity = _knowledge.get_entity(entity_id)
    if not entity:
        return [TextContent(type="text", text=f'{{"error": "Entity not found: {entity_id}"}}')]
    timeline = _knowledge.entity_timeline(entity_id)
    output = {"entity": _entity_summary(entity), "timeline": [{"date": e["date"].isoformat() if hasattr(e["date"], "isoformat") else str(e["date"]), "event": e["event"], "relationship": e["relationship"], "with": e["with"], "with_id": e["with_id"]} for e in timeline], "total_events": len(timeline)}
    return [TextContent(type="text", text=json.dumps(output, ensure_ascii=False, default=str))]


async def _handle_open_hypotheses(args):
    if not _knowledge:
        return [TextContent(type="text", text='{"error": "Knowledge graph not initialized."}')]
    limit = min(args.get("limit", 20), 100)
    hypotheses = _knowledge.get_open_hypotheses(limit=limit)
    output_list = []
    for hyp in hypotheses:
        entry = {"id": hyp.id, "type": hyp.type.value, "confidence": hyp.confidence, "status": hyp.status.value, "created_at": hyp.created_at.isoformat() if hyp.created_at else None}
        entities = []
        for eid in hyp.entity_ids:
            e = _knowledge.get_entity(eid)
            if e:
                entities.append(_entity_summary(e))
        entry["entities"] = entities
        if hyp.evidence:
            entry["evidence"] = hyp.evidence
        output_list.append(entry)
    return [TextContent(type="text", text=json.dumps({"hypotheses": output_list, "total": len(output_list)}, ensure_ascii=False, default=str))]


async def _handle_resolve_hypothesis(args):
    if not _knowledge:
        return [TextContent(type="text", text='{"error": "Knowledge graph not initialized."}')]
    hypothesis_id = args.get("hypothesis_id", "")
    confirmed = args.get("confirmed")
    if not hypothesis_id:
        return [TextContent(type="text", text='{"error": "hypothesis_id is required."}')]
    if confirmed is None:
        return [TextContent(type="text", text='{"error": "confirmed (true/false) is required."}')]
    try:
        _knowledge.resolve_hypothesis(hypothesis_id, confirmed=confirmed, method=ResolutionMethod.LLM)
        action = "confirmed" if confirmed else "denied"
        return [TextContent(type="text", text=json.dumps({"status": "resolved", "action": action, "hypothesis_id": hypothesis_id}))]
    except Exception as e:
        return [TextContent(type="text", text=json.dumps({"error": str(e)}))]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def engine():
    """Build a knowledge engine with test vault data."""
    import subprocess
    if not VAULT_DIR.exists() or not any(VAULT_DIR.iterdir()):
        gen_script = Path(__file__).parent / "fixtures" / "generate_test_vault.py"
        subprocess.run([sys.executable, str(gen_script)], check=True)

    eng = KnowledgeEngine(str(VAULT_DIR), db_name="test_mcp.db")
    vault_data = read_vault_jsonl(str(VAULT_DIR))
    records = list(adapt_all(vault_data))
    eng.ingest(iter(records))
    yield eng
    eng.close()
    db_path = VAULT_DIR / "test_mcp.db"
    if db_path.exists():
        db_path.unlink()


@pytest.fixture(autouse=True)
def set_knowledge(engine):
    """Set the global _knowledge for handler functions."""
    global _knowledge
    _knowledge = engine


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestGraphStats:
    def test_returns_stats(self):
        result = asyncio.run(_handle_graph_stats({}))
        data = json.loads(result[0].text)
        assert "totals" in data
        assert "entity_breakdown" in data
        assert data["totals"]["entities"] > 1000
        assert data["entity_breakdown"]["person"] > 10
        assert data["entity_breakdown"]["message"] > 100

    def test_includes_hypothesis_count(self):
        result = asyncio.run(_handle_graph_stats({}))
        data = json.loads(result[0].text)
        assert "hypotheses_open" in data


class TestFindPeople:
    def test_find_all_people(self):
        result = asyncio.run(_handle_find_people({}))
        data = json.loads(result[0].text)
        assert "people" in data
        assert data["total"] > 10

    def test_find_by_name(self):
        result = asyncio.run(_handle_find_people({"name": "Alice"}))
        data = json.loads(result[0].text)
        assert data["total"] >= 1
        assert any("Alice" in p.get("name", "") for p in data["people"])

    def test_find_by_email(self):
        result = asyncio.run(_handle_find_people({"email": "alice.mueller@gmail.com"}))
        data = json.loads(result[0].text)
        assert data["total"] >= 1

    def test_includes_sources(self):
        result = asyncio.run(_handle_find_people({"name": "Alice"}))
        data = json.loads(result[0].text)
        if data["total"] > 0:
            person = data["people"][0]
            assert "sources" in person

    def test_no_match_returns_empty(self):
        result = asyncio.run(_handle_find_people({"name": "Zzzznonexistent"}))
        data = json.loads(result[0].text)
        assert data["total"] == 0

    def test_limit_respected(self):
        result = asyncio.run(_handle_find_people({"limit": 3}))
        data = json.loads(result[0].text)
        assert data["total"] <= 3


class TestGetGraphEntity:
    def test_get_existing_entity(self, engine):
        people = engine.find_entities(EntityType.PERSON, limit=1)
        result = asyncio.run(_handle_get_graph_entity({"entity_id": people[0].id}))
        data = json.loads(result[0].text)
        assert data["id"] == people[0].id
        assert data["type"] == "person"
        assert "properties" in data
        assert "provenance" in data

    def test_includes_relationship_summary(self, engine):
        people = engine.find_entities(EntityType.PERSON, limit=20)
        target = None
        for p in people:
            if engine.get_relationships(p.id):
                target = p
                break
        if not target:
            pytest.skip("No person with relationships")
        result = asyncio.run(_handle_get_graph_entity({"entity_id": target.id}))
        data = json.loads(result[0].text)
        assert "relationship_summary" in data
        assert data["total_connections"] > 0

    def test_missing_entity_returns_error(self):
        result = asyncio.run(_handle_get_graph_entity({"entity_id": "nonexistent-uuid"}))
        data = json.loads(result[0].text)
        assert "error" in data

    def test_missing_id_returns_error(self):
        result = asyncio.run(_handle_get_graph_entity({}))
        data = json.loads(result[0].text)
        assert "error" in data


class TestGetConnections:
    def test_get_connections(self, engine):
        people = engine.find_entities(EntityType.PERSON, limit=20)
        target = None
        for p in people:
            if engine.get_relationships(p.id):
                target = p
                break
        if not target:
            pytest.skip("No person with connections")
        result = asyncio.run(_handle_get_connections({"entity_id": target.id}))
        data = json.loads(result[0].text)
        assert "connections" in data
        assert data["total"] > 0

    def test_connection_has_required_fields(self, engine):
        people = engine.find_entities(EntityType.PERSON, limit=20)
        target = None
        for p in people:
            if engine.get_relationships(p.id):
                target = p
                break
        if not target:
            pytest.skip("No person with connections")
        result = asyncio.run(_handle_get_connections({"entity_id": target.id}))
        data = json.loads(result[0].text)
        conn = data["connections"][0]
        assert "relationship" in conn
        assert "direction" in conn
        assert "entity" in conn

    def test_filter_by_type(self, engine):
        people = engine.find_entities(EntityType.PERSON, limit=20)
        target = None
        for p in people:
            rels = engine.get_relationships(p.id)
            if any(r.type.value == "sent" for r in rels):
                target = p
                break
        if not target:
            pytest.skip("No person with SENT rels")
        result = asyncio.run(_handle_get_connections({"entity_id": target.id, "relationship_type": "sent"}))
        data = json.loads(result[0].text)
        for conn in data["connections"]:
            assert conn["relationship"] == "sent"

    def test_invalid_entity_returns_error(self):
        result = asyncio.run(_handle_get_connections({"entity_id": "bad-id"}))
        data = json.loads(result[0].text)
        assert "error" in data


class TestEntityTimeline:
    def test_returns_events(self, engine):
        people = engine.find_entities(EntityType.PERSON, limit=20)
        target = None
        for p in people:
            if engine.entity_timeline(p.id):
                target = p
                break
        if not target:
            pytest.skip("No person with timeline")
        result = asyncio.run(_handle_entity_timeline({"entity_id": target.id}))
        data = json.loads(result[0].text)
        assert "timeline" in data
        assert data["total_events"] > 0

    def test_event_has_fields(self, engine):
        people = engine.find_entities(EntityType.PERSON, limit=20)
        target = None
        for p in people:
            if engine.entity_timeline(p.id):
                target = p
                break
        if not target:
            pytest.skip("No person with timeline")
        result = asyncio.run(_handle_entity_timeline({"entity_id": target.id}))
        data = json.loads(result[0].text)
        event = data["timeline"][0]
        assert "date" in event
        assert "event" in event
        assert "relationship" in event


class TestOpenHypotheses:
    def test_returns_hypotheses(self):
        result = asyncio.run(_handle_open_hypotheses({}))
        data = json.loads(result[0].text)
        assert "hypotheses" in data
        assert "total" in data

    def test_hypothesis_has_entity_details(self):
        result = asyncio.run(_handle_open_hypotheses({}))
        data = json.loads(result[0].text)
        if data["total"] > 0:
            hyp = data["hypotheses"][0]
            assert "id" in hyp
            assert "type" in hyp
            assert "confidence" in hyp
            assert "entities" in hyp

    def test_limit_respected(self):
        result = asyncio.run(_handle_open_hypotheses({"limit": 1}))
        data = json.loads(result[0].text)
        assert data["total"] <= 1


class TestResolveHypothesis:
    def test_resolve_confirm(self, engine):
        hyps = engine.get_open_hypotheses(limit=1)
        if not hyps:
            pytest.skip("No open hypotheses")
        result = asyncio.run(_handle_resolve_hypothesis({"hypothesis_id": hyps[0].id, "confirmed": True}))
        data = json.loads(result[0].text)
        assert data["status"] == "resolved"
        assert data["action"] == "confirmed"

    def test_resolve_deny(self, engine):
        hyps = engine.get_open_hypotheses(limit=1)
        if not hyps:
            pytest.skip("No open hypotheses")
        result = asyncio.run(_handle_resolve_hypothesis({"hypothesis_id": hyps[0].id, "confirmed": False}))
        data = json.loads(result[0].text)
        assert data["status"] == "resolved"
        assert data["action"] == "denied"

    def test_missing_id_returns_error(self):
        result = asyncio.run(_handle_resolve_hypothesis({"confirmed": True}))
        data = json.loads(result[0].text)
        assert "error" in data
