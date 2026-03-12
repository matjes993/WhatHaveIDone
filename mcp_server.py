"""
WHID MCP Server
Exposes semantic search over personal vault data via Model Context Protocol.
Runs in stdio mode for Claude Desktop integration.

Usage:
  python mcp_server.py

Claude Desktop config (claude_desktop_config.json):
  {
    "mcpServers": {
      "whid": {
        "command": "python3",
        "args": ["/path/to/WhatHaveIDone/mcp_server.py"]
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

from core.vectordb import get_client, search, get_full_entry, get_status, _get_embedding_fn

logger = logging.getLogger("whid.mcp")

# ---------------------------------------------------------------------------
# Config loading (mirrors whid.py logic)
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

app = Server("whid")

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
        description="Search across ALL your personal data — emails, contacts, notes, calendar, browsing history, music, YouTube, books, health, finance, shopping, podcasts, maps. Use this for broad searches.",
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
        return [TextContent(type="text", text="No email data found. Run 'whid vectorize' first.")]

    where_filter = None
    if year:
        where_filter = {"year": year}

    results = search(_client, query, collections=gmail_cols, n_results=n_results,
                     where_filter=where_filter, config=_config)

    return [TextContent(type="text", text=_format_results(results, "email"))]


async def _handle_search_contacts(args):
    query = args.get("query", "")
    n_results = min(args.get("n_results", 10), 50)

    if not query:
        return [TextContent(type="text", text="Please provide a search query.")]

    all_cols = _client.list_collections()
    contact_cols = [c.name for c in all_cols if "contact" in c.name]

    if not contact_cols:
        return [TextContent(type="text", text="No contacts data found. Run 'whid vectorize' first.")]

    results = search(_client, query, collections=contact_cols, n_results=n_results, config=_config)

    return [TextContent(type="text", text=_format_results(results, "contact"))]


async def _handle_search_all(args):
    query = args.get("query", "")
    n_results = min(args.get("n_results", 10), 50)
    source_filter = args.get("source_filter", "")

    if not query:
        return [TextContent(type="text", text="Please provide a search query.")]

    collections = None
    if source_filter:
        filter_names = [s.strip() for s in source_filter.split(",") if s.strip()]
        all_cols = _client.list_collections()
        collections = [c.name for c in all_cols if c.name in filter_names]

    results = search(_client, query, collections=collections, n_results=n_results, config=_config)

    return [TextContent(type="text", text=_format_results(results, "all"))]


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
        return [TextContent(type="text", text="No data sources found. Run 'whid vectorize' first.")]

    lines = ["Available data sources:\n"]
    for s in sorted(status, key=lambda x: x["collection"]):
        lines.append(
            f"  {s['collection']}: {s['vectorized']:,} vectorized / {s['vault_entries']:,} in vault"
        )

    return [TextContent(type="text", text="\n".join(lines))]


# ---------------------------------------------------------------------------
# Result formatting
# ---------------------------------------------------------------------------

def _format_results(results, result_type):
    """Format search results into readable text."""
    if not results:
        return "No results found."

    lines = [f"Found {len(results)} result(s):\n"]

    for i, r in enumerate(results, 1):
        meta = r.get("metadata", {})
        lines.append(f"--- Result {i} ---")
        lines.append(f"Source: {meta.get('source', r.get('collection', 'unknown'))}")

        if meta.get("subject"):
            lines.append(f"Subject: {meta['subject']}")
        if meta.get("from"):
            lines.append(f"From: {meta['from']}")
        if meta.get("to"):
            lines.append(f"To: {meta['to']}")
        if meta.get("date"):
            lines.append(f"Date: {meta['date']}")
        if meta.get("title"):
            lines.append(f"Title: {meta['title']}")
        if meta.get("tags"):
            lines.append(f"Tags: {meta['tags']}")

        lines.append(f"ID: {meta.get('entry_id', r['id'])}")
        lines.append(f"Relevance: {1 - r['distance']:.2%}")
        lines.append("")

        # Include the document text (truncated for readability)
        doc = r.get("document", "")
        if len(doc) > 1000:
            doc = doc[:1000] + "..."
        lines.append(doc)
        lines.append("")

    return "\n".join(lines)


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

    async with stdio_server() as (read, write):
        await app.run(read, write, app.create_initialization_options())


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
