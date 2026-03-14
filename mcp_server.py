"""
NOMOLO MCP Server
Exposes semantic search over personal vault data via Model Context Protocol.
Runs in stdio mode for Claude Desktop integration.

Usage:
  python mcp_server.py

Claude Desktop config (claude_desktop_config.json):
  {
    "mcpServers": {
      "nomolo": {
        "command": "python3",
        "args": ["/path/to/Nomolo/mcp_server.py"]
      }
    }
  }
"""

import json
import os
import sys
import logging

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.vectordb import get_client, get_full_entry, get_status, _get_embedding_fn
from core.search_engine import hybrid_search, get_fts_entry_count, index_all, _get_fts_db_path
from core.knowledge import KnowledgeEngine
from core.knowledge.schema import (
    EntityType,
    HypothesisStatus,
    IdentifierSystem,
    RelationshipType,
    ResolutionMethod,
)

logger = logging.getLogger("nomolo.mcp")

# ---------------------------------------------------------------------------
# Config loading (mirrors nomolo.py logic)
# ---------------------------------------------------------------------------

def _load_config():
    """Load config.yaml from the project root."""
    project_root = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(project_root, "config.yaml")

    if not os.path.exists(config_path):
        return {}

    try:
        import yaml
        with open(config_path, "r") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


def _get_vault_root(config):
    """Resolve the vault root directory."""
    project_root = os.path.dirname(os.path.abspath(__file__))
    vault_root = config.get("vault_root", os.path.join(project_root, "vaults"))
    vault_root = os.path.expanduser(vault_root)
    if not os.path.isabs(vault_root):
        vault_root = os.path.join(project_root, vault_root)
    return vault_root


# ---------------------------------------------------------------------------
# MCP Server
# ---------------------------------------------------------------------------

app = Server("nomolo")

# Global state (initialized in main)
_client = None
_config = None
_vault_root = None
_knowledge: KnowledgeEngine | None = None


TOOLS = [
    Tool(
        name="search_emails",
        description="Search the Captain's scroll vault (email archive) using semantic search. Returns the most relevant scrolls matching yer query.",
        inputSchema={
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "What to search for in the scrolls (natural language)",
                },
                "n_results": {
                    "type": "integer",
                    "description": "Number of scrolls to return (default: 10, max: 50)",
                    "default": 10,
                },
                "year": {
                    "type": "integer",
                    "description": "Filter by year (optional)",
                },
            },
            "required": ["query"],
        },
    ),
    Tool(
        name="search_contacts",
        description="Search the Captain's soul bonds (contacts) using semantic search. Find any crewmate or acquaintance.",
        inputSchema={
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "What to search for in soul bonds (name, company, email, etc.)",
                },
                "n_results": {
                    "type": "integer",
                    "description": "Number of results to return (default: 10)",
                    "default": 10,
                },
            },
            "required": ["query"],
        },
    ),
    Tool(
        name="search_all",
        description="Search across ALL the Captain's plunder — scrolls, soul bonds, manuscripts, time crystals, footprints, echoes, visions, tomes, life force, coins, marketplace receipts, whispers, waypoints. The ultimate treasure hunt. Uses hybrid search (keyword + semantic + metadata boosting).",
        inputSchema={
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "What treasure to search for (natural language)",
                },
                "n_results": {
                    "type": "integer",
                    "description": "Number of results to return (default: 10, max: 50)",
                    "default": 10,
                },
                "source_filter": {
                    "type": "string",
                    "description": "Comma-separated vault names to limit the search (e.g. 'gmail_primary,contacts'). Leave empty to search all waters.",
                },
                "sort_by": {
                    "type": "string",
                    "enum": ["relevance", "date_asc", "date_desc"],
                    "description": "Sort the plunder: 'relevance' (default), 'date_asc' (oldest first), 'date_desc' (newest first)",
                    "default": "relevance",
                },
            },
            "required": ["query"],
        },
    ),
    Tool(
        name="get_entry",
        description="Retrieve the full, detailed loot for a specific entry by its ID. Use this after a search to inspect the complete treasure.",
        inputSchema={
            "type": "object",
            "properties": {
                "entry_id": {
                    "type": "string",
                    "description": "The entry ID from a search result",
                },
                "vault_dir": {
                    "type": "string",
                    "description": "The vault directory name (from the 'source' field in search metadata)",
                },
            },
            "required": ["entry_id", "vault_dir"],
        },
    ),
    Tool(
        name="list_sources",
        description="Survey all the Captain's plundered islands and count the loot in each vault. Use this to understand what treasures have been collected.",
        inputSchema={
            "type": "object",
            "properties": {},
        },
    ),
    # -------------------------------------------------------------------
    # Knowledge Graph tools
    # -------------------------------------------------------------------
    Tool(
        name="graph_stats",
        description="Survey the Captain's knowledge graph — how many souls (people), bonds (relationships), places, events, messages, and hypotheses are charted. The big picture of everything connected.",
        inputSchema={
            "type": "object",
            "properties": {},
        },
    ),
    Tool(
        name="find_people",
        description="Search for known souls (people) in the knowledge graph. Find by name, email, phone, or browse all. Returns identity details, connected emails/phones, and source provenance.",
        inputSchema={
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "description": "Name to search for (partial match supported)",
                },
                "email": {
                    "type": "string",
                    "description": "Email address to look up (exact match)",
                },
                "phone": {
                    "type": "string",
                    "description": "Phone number to look up (exact match)",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max results (default: 20)",
                    "default": 20,
                },
            },
        },
    ),
    Tool(
        name="get_graph_entity",
        description="Get full details of any entity in the knowledge graph — person, organization, place, event, message, bookmark, note. Includes properties, relationships, provenance trail, and external identifiers.",
        inputSchema={
            "type": "object",
            "properties": {
                "entity_id": {
                    "type": "string",
                    "description": "The entity UUID from a previous search or connection result",
                },
            },
            "required": ["entity_id"],
        },
    ),
    Tool(
        name="get_connections",
        description="Reveal the web of connections for any entity. For a person: who they know, what events they attended, messages sent/received. For an event: who attended, where it was. Returns relationship type, direction, and connected entity summaries.",
        inputSchema={
            "type": "object",
            "properties": {
                "entity_id": {
                    "type": "string",
                    "description": "The entity UUID to explore connections for",
                },
                "relationship_type": {
                    "type": "string",
                    "enum": ["knows", "works_at", "sent", "received", "attended", "located_at", "mentions", "member_of", "created", "related_to"],
                    "description": "Filter to a specific relationship type (optional)",
                },
                "include_history": {
                    "type": "boolean",
                    "description": "Include ended/superseded relationships (default: false)",
                    "default": False,
                },
            },
            "required": ["entity_id"],
        },
    ),
    Tool(
        name="entity_timeline",
        description="View the life story of an entity across time — when relationships started and ended, job changes, location moves, event attendance. A chronological biography built from all data sources.",
        inputSchema={
            "type": "object",
            "properties": {
                "entity_id": {
                    "type": "string",
                    "description": "The entity UUID to build a timeline for",
                },
            },
            "required": ["entity_id"],
        },
    ),
    Tool(
        name="open_hypotheses",
        description="Show unresolved mysteries in the knowledge graph — suspected identity merges, missing links, data gaps. These are the Captain's open investigations. You can help resolve them.",
        inputSchema={
            "type": "object",
            "properties": {
                "limit": {
                    "type": "integer",
                    "description": "Max hypotheses to return (default: 20)",
                    "default": 20,
                },
            },
        },
    ),
    Tool(
        name="resolve_hypothesis",
        description="Resolve an open hypothesis — confirm or deny a suspected identity merge, missing link, or data gap. Provide reasoning for the decision.",
        inputSchema={
            "type": "object",
            "properties": {
                "hypothesis_id": {
                    "type": "string",
                    "description": "The hypothesis UUID to resolve",
                },
                "confirmed": {
                    "type": "boolean",
                    "description": "True to confirm the hypothesis, False to deny it",
                },
            },
            "required": ["hypothesis_id", "confirmed"],
        },
    ),
]


@app.list_tools()
async def list_tools():
    return TOOLS


@app.call_tool()
async def call_tool(name: str, arguments: dict):
    if name == "search_emails":
        return await _handle_search_emails(arguments)
    elif name == "search_contacts":
        return await _handle_search_contacts(arguments)
    elif name == "search_all":
        return await _handle_search_all(arguments)
    elif name == "get_entry":
        return await _handle_get_entry(arguments)
    elif name == "list_sources":
        return await _handle_list_sources(arguments)
    elif name == "graph_stats":
        return await _handle_graph_stats(arguments)
    elif name == "find_people":
        return await _handle_find_people(arguments)
    elif name == "get_graph_entity":
        return await _handle_get_graph_entity(arguments)
    elif name == "get_connections":
        return await _handle_get_connections(arguments)
    elif name == "entity_timeline":
        return await _handle_entity_timeline(arguments)
    elif name == "open_hypotheses":
        return await _handle_open_hypotheses(arguments)
    elif name == "resolve_hypothesis":
        return await _handle_resolve_hypothesis(arguments)
    else:
        return [TextContent(type="text", text=f"Arrr! Unknown order: {name}. That's not in the Captain's handbook.")]


# ---------------------------------------------------------------------------
# Tool handlers
# ---------------------------------------------------------------------------

async def _handle_search_emails(args):
    query = args.get("query", "")
    n_results = min(args.get("n_results", 10), 50)
    year = args.get("year")

    if not query:
        return [TextContent(type="text", text="Arrr! Ye need to tell me what treasure to search for, Captain.")]

    # Find Gmail collections
    all_cols = _client.list_collections()
    gmail_cols = [c.name for c in all_cols if c.name.startswith("gmail")]

    if not gmail_cols:
        return [TextContent(type="text", text="No scrolls in the vault! Chart the treasure maps first with 'nomolo vectorize'.")]

    results = hybrid_search(
        query, _vault_root, _client, config=_config,
        n_results=n_results, collections=gmail_cols, year_filter=year,
    )

    return [TextContent(type="text", text=_format_hybrid_results_json(results, query))]


async def _handle_search_contacts(args):
    query = args.get("query", "")
    n_results = min(args.get("n_results", 10), 50)

    if not query:
        return [TextContent(type="text", text="Arrr! Ye need to tell me what treasure to search for, Captain.")]

    all_cols = _client.list_collections()
    contact_cols = [c.name for c in all_cols if "contact" in c.name]

    if not contact_cols:
        return [TextContent(type="text", text="No soul bonds in the vault! Chart the treasure maps first with 'nomolo vectorize'.")]

    results = hybrid_search(
        query, _vault_root, _client, config=_config,
        n_results=n_results, collections=contact_cols,
    )

    return [TextContent(type="text", text=_format_hybrid_results_json(results, query))]


async def _handle_search_all(args):
    query = args.get("query", "")
    n_results = min(args.get("n_results", 10), 50)
    source_filter = args.get("source_filter", "")
    sort_by = args.get("sort_by", "relevance")

    if not query:
        return [TextContent(type="text", text="Arrr! Ye need to tell me what treasure to search for, Captain.")]

    collections = None
    if source_filter:
        filter_names = [s.strip() for s in source_filter.split(",") if s.strip()]
        all_cols = _client.list_collections()
        collections = [c.name for c in all_cols if c.name in filter_names]

    results = hybrid_search(
        query, _vault_root, _client, config=_config,
        n_results=n_results, collections=collections,
        sort_by=sort_by,
    )

    return [TextContent(type="text", text=_format_hybrid_results_json(results, query))]


async def _handle_get_entry(args):
    entry_id = args.get("entry_id", "")
    vault_dir = args.get("vault_dir", "")

    if not entry_id or not vault_dir:
        return [TextContent(type="text", text="Arrr! Ye need to provide both the entry_id and the vault_dir to find that treasure.")]

    entry = get_full_entry(_vault_root, vault_dir, entry_id)

    if entry is None:
        return [TextContent(type="text", text=f"That treasure seems to have sunk, Captain! Entry not found: {entry_id} in {vault_dir}")]

    # Format the full entry nicely
    formatted = json.dumps(entry, indent=2, ensure_ascii=False, default=str)
    return [TextContent(type="text", text=formatted)]


async def _handle_list_sources(args):
    status = get_status(_client, _vault_root)

    if not status:
        return [TextContent(type="text", text=json.dumps({"sources": []}, ensure_ascii=False))]

    sources = []
    for s in sorted(status, key=lambda x: x["collection"]):
        sources.append({
            "collection": s["collection"],
            "vectorized": s["vectorized"],
            "vault_entries": s["vault_entries"],
        })

    return [TextContent(type="text", text=json.dumps({"sources": sources}, ensure_ascii=False))]


# ---------------------------------------------------------------------------
# Result formatting
# ---------------------------------------------------------------------------

def _deduplicate_results(results):
    """Group results by entry_id and keep only the best (lowest distance) chunk per entry."""
    if not results:
        return results

    best_by_id = {}
    for r in results:
        meta = r.get("metadata", {})
        entry_id = meta.get("entry_id", r.get("id", ""))
        if entry_id not in best_by_id or r["distance"] < best_by_id[entry_id]["distance"]:
            best_by_id[entry_id] = r

    # Return sorted by distance (best first)
    return sorted(best_by_id.values(), key=lambda r: r["distance"])


def _format_hybrid_results_json(results, query):
    """Format hybrid search results as a structured JSON string."""
    if not results:
        return json.dumps({"results": [], "total": 0, "query": query}, ensure_ascii=False)

    formatted = []
    metadata_fields = ["subject", "from", "to", "date", "title", "tags"]

    for r in results:
        meta = r.get("metadata", {})
        entry = {
            "entry_id": r.get("entry_id", meta.get("entry_id", "")),
            "source": r.get("source", meta.get("source", "unknown")),
            "collection": r.get("collection", ""),
            "relevance": round(r.get("combined_score", 0.0), 4),
        }

        # Only include metadata fields that exist and are non-empty
        for field in metadata_fields:
            value = meta.get(field)
            if value:
                entry[field] = value

        # Include snippet
        snippet = r.get("snippet", "")
        if snippet:
            entry["snippet"] = snippet[:800]

        formatted.append(entry)

    output = {
        "results": formatted,
        "total": len(formatted),
        "query": query,
    }
    return json.dumps(output, ensure_ascii=False)


def _format_results_json(results, query):
    """Format search results as a structured JSON string."""
    deduped = _deduplicate_results(results)

    if not deduped:
        return json.dumps({"results": [], "total": 0, "query": query}, ensure_ascii=False)

    formatted = []
    metadata_fields = ["subject", "from", "to", "date", "title", "tags"]

    for r in deduped:
        meta = r.get("metadata", {})
        entry = {
            "entry_id": meta.get("entry_id", r.get("id", "")),
            "source": meta.get("source", r.get("collection", "unknown")),
            "collection": r.get("collection", ""),
            "relevance": round(max(0.0, min(1.0, 1 - r["distance"])), 4),
        }

        # Only include metadata fields that exist and are non-empty
        for field in metadata_fields:
            value = meta.get(field)
            if value:
                entry[field] = value

        # Include snippet (first 800 chars of document)
        doc = r.get("document", "")
        if doc:
            entry["snippet"] = doc[:800]

        formatted.append(entry)

    output = {
        "results": formatted,
        "total": len(formatted),
        "query": query,
    }
    return json.dumps(output, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Knowledge Graph handlers
# ---------------------------------------------------------------------------

def _entity_summary(entity) -> dict:
    """Create a compact summary of an entity for JSON output."""
    summary = {
        "id": entity.id,
        "type": entity.type.value,
    }
    props = entity.properties
    # Include key identifying fields
    for field in ["name", "title", "subject", "url", "body"]:
        if field in props and props[field]:
            val = props[field]
            if field == "body" and isinstance(val, str) and len(val) > 200:
                val = val[:200] + "..."
            summary[field] = val
    return summary


async def _handle_graph_stats(args):
    if not _knowledge:
        return [TextContent(type="text", text='{"error": "Knowledge graph not initialized. Run ingestion first."}')]

    stats = _knowledge.stats()
    # Add entity type breakdown
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

    # Exact lookup by email
    if email:
        entity = _knowledge.find_by_identifier(IdentifierSystem.EMAIL, email.lower())
        if entity:
            results.append(entity)

    # Exact lookup by phone
    if phone and not results:
        entity = _knowledge.find_by_identifier(IdentifierSystem.PHONE, phone)
        if entity:
            results.append(entity)

    # Name search: browse all people and filter
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

    # Enrich with identifiers
    output = []
    for entity in results[:limit]:
        entry = _entity_summary(entity)
        identifiers = _knowledge.get_identifiers(entity.id)
        if identifiers:
            entry["identifiers"] = [
                {"system": ident.system.value, "value": ident.value}
                for ident in identifiers
            ]
        provs = _knowledge.get_provenance(entity.id)
        if provs:
            entry["sources"] = list({p.source_name for p in provs})
        output.append(entry)

    return [TextContent(type="text", text=json.dumps(
        {"people": output, "total": len(output)}, ensure_ascii=False, default=str
    ))]


async def _handle_get_graph_entity(args):
    if not _knowledge:
        return [TextContent(type="text", text='{"error": "Knowledge graph not initialized."}')]

    entity_id = args.get("entity_id", "")
    if not entity_id:
        return [TextContent(type="text", text='{"error": "entity_id is required."}')]

    entity = _knowledge.get_entity(entity_id)
    if not entity:
        return [TextContent(type="text", text=f'{{"error": "Entity not found: {entity_id}"}}')]

    output = {
        "id": entity.id,
        "type": entity.type.value,
        "properties": entity.properties,
        "created_at": entity.created_at.isoformat() if entity.created_at else None,
    }

    # Identifiers
    identifiers = _knowledge.get_identifiers(entity.id)
    if identifiers:
        output["identifiers"] = [
            {"system": i.system.value, "value": i.value, "verified": i.verified}
            for i in identifiers
        ]

    # Provenance
    provs = _knowledge.get_provenance(entity.id)
    if provs:
        output["provenance"] = [
            {
                "source": p.source_name,
                "source_record_id": p.source_record_id,
                "confidence": p.confidence,
                "ingested_at": p.ingested_at.isoformat() if p.ingested_at else None,
            }
            for p in provs
        ]

    # Relationship summary (counts by type)
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

    rels = _knowledge.get_relationships(
        entity_id,
        rel_type=rel_type,
        current_only=not include_history,
    )

    connections = []
    for rel in rels:
        other_id = rel.target_id if rel.source_id == entity_id else rel.source_id
        other = _knowledge.get_entity(other_id)

        conn = {
            "relationship": rel.type.value,
            "direction": "outgoing" if rel.source_id == entity_id else "incoming",
            "entity": _entity_summary(other) if other else {"id": other_id, "type": "unknown"},
        }
        if rel.valid_from:
            conn["since"] = rel.valid_from.isoformat()
        if rel.valid_to:
            conn["until"] = rel.valid_to.isoformat()
        if rel.superseded_at:
            conn["superseded"] = True
        connections.append(conn)

    output = {
        "entity": _entity_summary(entity),
        "connections": connections,
        "total": len(connections),
    }
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

    output = {
        "entity": _entity_summary(entity),
        "timeline": [
            {
                "date": event["date"].isoformat() if hasattr(event["date"], "isoformat") else str(event["date"]),
                "event": event["event"],
                "relationship": event["relationship"],
                "with": event["with"],
                "with_id": event["with_id"],
            }
            for event in timeline
        ],
        "total_events": len(timeline),
    }
    return [TextContent(type="text", text=json.dumps(output, ensure_ascii=False, default=str))]


async def _handle_open_hypotheses(args):
    if not _knowledge:
        return [TextContent(type="text", text='{"error": "Knowledge graph not initialized."}')]

    limit = min(args.get("limit", 20), 100)
    hypotheses = _knowledge.get_open_hypotheses(limit=limit)

    output_list = []
    for hyp in hypotheses:
        entry = {
            "id": hyp.id,
            "type": hyp.type.value,
            "confidence": hyp.confidence,
            "status": hyp.status.value,
            "created_at": hyp.created_at.isoformat() if hyp.created_at else None,
        }
        # Enrich with entity details
        entities = []
        for eid in hyp.entity_ids:
            e = _knowledge.get_entity(eid)
            if e:
                entities.append(_entity_summary(e))
        entry["entities"] = entities

        if hyp.evidence:
            entry["evidence"] = hyp.evidence

        output_list.append(entry)

    return [TextContent(type="text", text=json.dumps(
        {"hypotheses": output_list, "total": len(output_list)},
        ensure_ascii=False, default=str,
    ))]


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
        _knowledge.resolve_hypothesis(
            hypothesis_id,
            confirmed=confirmed,
            method=ResolutionMethod.LLM,
        )
        action = "confirmed" if confirmed else "denied"
        return [TextContent(type="text", text=json.dumps(
            {"status": "resolved", "action": action, "hypothesis_id": hypothesis_id}
        ))]
    except Exception as e:
        return [TextContent(type="text", text=json.dumps({"error": str(e)}))]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    global _client, _config, _vault_root, _knowledge

    # Suppress noisy logs in MCP mode
    logging.basicConfig(level=logging.WARNING, stream=sys.stderr)

    _config = _load_config()
    _vault_root = _get_vault_root(_config)
    _client = get_client(_vault_root)

    # Ensure FTS database directory exists (hybrid search will use it)
    fts_path = _get_fts_db_path(_vault_root, _config)
    os.makedirs(os.path.dirname(fts_path), exist_ok=True)

    # Initialize knowledge graph (if DB exists or vault has data)
    try:
        _knowledge = KnowledgeEngine(_vault_root)
        logger.info("Knowledge graph initialized at %s", _knowledge.db_path)
    except Exception as e:
        logger.warning("Knowledge graph not available: %s", e)
        _knowledge = None

    async with stdio_server() as (read, write):
        await app.run(read, write, app.create_initialization_options())


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
