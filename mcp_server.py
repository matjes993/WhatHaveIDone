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


TOOLS = [
    Tool(
        name="search_emails",
        description="Search through your email archive using semantic search. Returns relevant emails matching your query.",
        inputSchema={
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "What to search for in emails (natural language)",
                },
                "n_results": {
                    "type": "integer",
                    "description": "Number of results to return (default: 10, max: 50)",
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
        description="Search through your contacts using semantic search.",
        inputSchema={
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "What to search for in contacts (name, company, email, etc.)",
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
        description="Search across ALL your personal data — emails, contacts, notes, calendar, browsing history, music, YouTube, books, health, finance, shopping, podcasts, maps. Use this for broad searches. Uses hybrid search (keyword + semantic + metadata boosting).",
        inputSchema={
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "What to search for (natural language)",
                },
                "n_results": {
                    "type": "integer",
                    "description": "Number of results to return (default: 10, max: 50)",
                    "default": 10,
                },
                "source_filter": {
                    "type": "string",
                    "description": "Comma-separated source names to limit search (e.g. 'gmail_primary,contacts'). Leave empty to search all.",
                },
                "sort_by": {
                    "type": "string",
                    "enum": ["relevance", "date_asc", "date_desc"],
                    "description": "Sort order: 'relevance' (default), 'date_asc' (oldest first), 'date_desc' (newest first)",
                    "default": "relevance",
                },
            },
            "required": ["query"],
        },
    ),
    Tool(
        name="get_entry",
        description="Retrieve the full, detailed record for a specific entry by its ID. Use this after a search to get complete details.",
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
        description="List all available data sources and how many entries are vectorized in each. Use this to understand what personal data is available.",
        inputSchema={
            "type": "object",
            "properties": {},
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
    else:
        return [TextContent(type="text", text=f"Unknown tool: {name}")]


# ---------------------------------------------------------------------------
# Tool handlers
# ---------------------------------------------------------------------------

async def _handle_search_emails(args):
    query = args.get("query", "")
    n_results = min(args.get("n_results", 10), 50)
    year = args.get("year")

    if not query:
        return [TextContent(type="text", text="Please provide a search query.")]

    # Find Gmail collections
    all_cols = _client.list_collections()
    gmail_cols = [c.name for c in all_cols if c.name.startswith("gmail")]

    if not gmail_cols:
        return [TextContent(type="text", text="No email data found. Run 'nomolo vectorize' first.")]

    results = hybrid_search(
        query, _vault_root, _client, config=_config,
        n_results=n_results, collections=gmail_cols, year_filter=year,
    )

    return [TextContent(type="text", text=_format_hybrid_results_json(results, query))]


async def _handle_search_contacts(args):
    query = args.get("query", "")
    n_results = min(args.get("n_results", 10), 50)

    if not query:
        return [TextContent(type="text", text="Please provide a search query.")]

    all_cols = _client.list_collections()
    contact_cols = [c.name for c in all_cols if "contact" in c.name]

    if not contact_cols:
        return [TextContent(type="text", text="No contacts data found. Run 'nomolo vectorize' first.")]

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
        return [TextContent(type="text", text="Please provide a search query.")]

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
        return [TextContent(type="text", text="Please provide both entry_id and vault_dir.")]

    entry = get_full_entry(_vault_root, vault_dir, entry_id)

    if entry is None:
        return [TextContent(type="text", text=f"Entry not found: {entry_id} in {vault_dir}")]

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
# Main
# ---------------------------------------------------------------------------

async def main():
    global _client, _config, _vault_root

    # Suppress noisy logs in MCP mode
    logging.basicConfig(level=logging.WARNING, stream=sys.stderr)

    _config = _load_config()
    _vault_root = _get_vault_root(_config)
    _client = get_client(_vault_root)

    # Ensure FTS database directory exists (hybrid search will use it)
    fts_path = _get_fts_db_path(_vault_root, _config)
    os.makedirs(os.path.dirname(fts_path), exist_ok=True)

    async with stdio_server() as (read, write):
        await app.run(read, write, app.create_initialization_options())


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
