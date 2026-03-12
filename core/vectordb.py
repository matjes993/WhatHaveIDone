"""
NOMOLO Vector Database
ChromaDB-based vector storage for semantic search across all vault data.
Handles ingestion, chunking, incremental updates, and search.
"""

import json
import logging
import os

import chromadb
from chromadb.utils.embedding_functions import SentenceTransformerEmbeddingFunction

from core.vault import read_all_entries, count_entries

logger = logging.getLogger("nomolo.vectordb")

# Mapping: vault directory base name → embedding field name
EMBEDDING_FIELDS = {
    "Gmail": "body_for_embedding",
    "Contacts": "contact_for_embedding",
    "Books": "book_for_embedding",
    "YouTube": "youtube_for_embedding",
    "Music": "listen_for_embedding",
    "Finance": "transaction_for_embedding",
    "Shopping": "order_for_embedding",
    "Notes": "note_for_embedding",
    "Podcasts": "podcast_for_embedding",
    "Health": "health_for_embedding",
    "Browser": "browse_for_embedding",
    "Calendar": "event_for_embedding",
    "Maps": "location_for_embedding",
}

# Default embedding model (local, no API key needed)
DEFAULT_MODEL = "all-MiniLM-L6-v2"

# Chunking defaults
DEFAULT_CHUNK_SIZE = 2000
DEFAULT_CHUNK_OVERLAP = 200

# ChromaDB batch size limit
BATCH_SIZE = 500


# ---------------------------------------------------------------------------
# ChromaDB client and collections
# ---------------------------------------------------------------------------

def get_client(vault_root):
    """Return a persistent ChromaDB client stored at vault_root/.vectordb/."""
    db_path = os.path.join(vault_root, ".vectordb")
    os.makedirs(db_path, exist_ok=True)
    return chromadb.PersistentClient(path=db_path)


def _get_embedding_fn(config=None):
    """Return the embedding function based on config."""
    config = config or {}
    vector_cfg = config.get("vector", {})
    model = vector_cfg.get("model", DEFAULT_MODEL)
    return SentenceTransformerEmbeddingFunction(model_name=model)


def get_or_create_collection(client, vault_dir_name, config=None):
    """Get or create a ChromaDB collection for a vault directory."""
    # Normalize collection name: Gmail_Primary → gmail_primary
    collection_name = vault_dir_name.lower().replace(" ", "_")
    # ChromaDB collection names must be 3-63 chars, alphanumeric + underscores
    collection_name = "".join(c if c.isalnum() or c == "_" else "_" for c in collection_name)
    if len(collection_name) < 3:
        collection_name = collection_name + "___"

    embedding_fn = _get_embedding_fn(config)
    return client.get_or_create_collection(
        name=collection_name,
        embedding_function=embedding_fn,
        metadata={"vault_dir": vault_dir_name},
    )


# ---------------------------------------------------------------------------
# Embedding field detection
# ---------------------------------------------------------------------------

def detect_embedding_field(vault_dir_name):
    """
    Given a vault directory name like 'Gmail_Primary', determine the embedding field.
    Strips suffixes to match base names in EMBEDDING_FIELDS.
    """
    # Direct match first
    if vault_dir_name in EMBEDDING_FIELDS:
        return EMBEDDING_FIELDS[vault_dir_name]

    # Strip suffix: Gmail_Primary → Gmail, Gmail_Work → Gmail
    base = vault_dir_name.split("_")[0]
    if base in EMBEDDING_FIELDS:
        return EMBEDDING_FIELDS[base]

    return None


# ---------------------------------------------------------------------------
# Text chunking
# ---------------------------------------------------------------------------

def chunk_text(text, chunk_size=DEFAULT_CHUNK_SIZE, overlap=DEFAULT_CHUNK_OVERLAP, prefix=""):
    """
    Split text into overlapping chunks.
    If text fits in one chunk, returns it as-is.
    Each chunk gets the prefix prepended (for context like email headers).

    Returns list of chunk strings.
    """
    if not text:
        return []

    if len(text) <= chunk_size:
        return [text]

    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        chunk = text[start:end]

        # If this isn't the first chunk, prepend the prefix
        if start > 0 and prefix:
            chunk = prefix + "... " + chunk

        chunks.append(chunk)
        start += chunk_size - overlap

    return chunks


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------

def vectorize_vault(vault_path, vault_dir_name, client, config=None, force=False):
    """
    Ingest a vault directory into ChromaDB.

    Reads all JSONL entries, extracts embedding text, chunks if needed,
    and upserts into the collection. Skips entries already in the collection
    unless force=True.

    Returns (new_count, skipped_count, total_in_collection).
    """
    embedding_field = detect_embedding_field(vault_dir_name)
    if not embedding_field:
        logger.warning("No embedding field mapping for vault: %s", vault_dir_name)
        return 0, 0, 0

    collection = get_or_create_collection(client, vault_dir_name, config)

    # Get existing IDs for incremental updates
    existing_ids = set()
    if not force:
        try:
            result = collection.get(include=[])
            existing_ids = set(result["ids"])
        except Exception:
            pass

    vector_cfg = (config or {}).get("vector", {})
    chunk_size = vector_cfg.get("chunk_size", DEFAULT_CHUNK_SIZE)
    chunk_overlap = vector_cfg.get("chunk_overlap", DEFAULT_CHUNK_OVERLAP)

    # Collect entries to process
    batch_ids = []
    batch_docs = []
    batch_metas = []
    new_count = 0
    skipped_count = 0
    total_processed = 0

    for entry in read_all_entries(vault_path):
        entry_id = entry.get("id", "")
        if not entry_id:
            skipped_count += 1
            continue

        # Skip if already vectorized (check base ID and chunk IDs)
        if not force and entry_id in existing_ids:
            skipped_count += 1
            continue

        # Get embedding text
        embed_text = entry.get(embedding_field, "")
        if not embed_text or not embed_text.strip():
            skipped_count += 1
            continue

        # Build metadata
        meta = {
            "source": vault_dir_name,
            "year": entry.get("year", 0),
            "month": entry.get("month", 0),
        }

        # Source-specific metadata
        if "subject" in entry:
            meta["subject"] = str(entry["subject"])[:500]
        if "from" in entry:
            meta["from"] = str(entry["from"])[:200]
        if "to" in entry:
            meta["to"] = str(entry["to"])[:200]
        if "date" in entry:
            meta["date"] = str(entry["date"])[:50]
        if "title" in entry:
            meta["title"] = str(entry["title"])[:500]
        if "tags" in entry and isinstance(entry["tags"], list):
            meta["tags"] = ",".join(str(t) for t in entry["tags"][:20])

        # Chunk if necessary
        # For emails, extract the header prefix for context on each chunk
        prefix = ""
        if embedding_field == "body_for_embedding":
            # The email embedding starts with "From X to Y on DATE re: SUBJECT:\n"
            newline_pos = embed_text.find("\n")
            if newline_pos > 0 and newline_pos < 200:
                prefix = embed_text[:newline_pos]

        chunks = chunk_text(embed_text, chunk_size, chunk_overlap, prefix)

        for i, chunk in enumerate(chunks):
            if len(chunks) == 1:
                chunk_id = entry_id
            else:
                chunk_id = f"{entry_id}_chunk_{i}"

            if not force and chunk_id in existing_ids:
                continue

            batch_ids.append(chunk_id)
            batch_docs.append(chunk)
            chunk_meta = dict(meta)
            chunk_meta["entry_id"] = entry_id
            if len(chunks) > 1:
                chunk_meta["chunk_index"] = i
                chunk_meta["total_chunks"] = len(chunks)
            batch_metas.append(chunk_meta)

            # Flush batch when full
            if len(batch_ids) >= BATCH_SIZE:
                collection.upsert(
                    ids=batch_ids,
                    documents=batch_docs,
                    metadatas=batch_metas,
                )
                total_processed += len(batch_ids)
                print(f"\r    Vectorizing {vault_dir_name}: {total_processed:,} entries...", end="", flush=True)
                batch_ids = []
                batch_docs = []
                batch_metas = []

        new_count += 1

    # Flush remaining
    if batch_ids:
        collection.upsert(
            ids=batch_ids,
            documents=batch_docs,
            metadatas=batch_metas,
        )
        total_processed += len(batch_ids)

    if total_processed > 0:
        print(f"\r    Vectorizing {vault_dir_name}: {total_processed:,} entries... done")

    total_in_collection = collection.count()
    return new_count, skipped_count, total_in_collection


def vectorize_all(vault_root, client, config=None, source_filter=None, force=False):
    """
    Vectorize all vault directories under vault_root.

    Args:
        vault_root: Base path containing vault directories.
        client: ChromaDB client.
        config: Optional config dict.
        source_filter: Optional vault dir name to limit to.
        force: If True, re-vectorize everything.
    """
    if not os.path.isdir(vault_root):
        print(f"  Vault root not found: {vault_root}")
        return

    entries = sorted(os.listdir(vault_root))
    results = {}

    for entry in entries:
        vault_path = os.path.join(vault_root, entry)
        if not os.path.isdir(vault_path):
            continue
        if entry.startswith("."):
            continue

        # Apply filter if given
        if source_filter and entry != source_filter:
            continue

        # Check if this vault type has an embedding field mapping
        if not detect_embedding_field(entry):
            logger.debug("Skipping %s — no embedding field mapping", entry)
            continue

        # Check if vault has any entries
        total, _ = count_entries(vault_path)
        if total == 0:
            continue

        print(f"\n  {entry} ({total:,} vault entries)")
        new_count, skipped, total_in_col = vectorize_vault(
            vault_path, entry, client, config, force
        )
        results[entry] = {
            "new": new_count,
            "skipped": skipped,
            "total": total_in_col,
        }

        if new_count > 0:
            print(f"    New: {new_count:,} | Total in DB: {total_in_col:,}")
        elif skipped > 0:
            print(f"    Up to date ({total_in_col:,} in DB)")

    return results


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------

def search(client, query, collections=None, n_results=10, where_filter=None, config=None):
    """
    Search across one or more ChromaDB collections.

    Args:
        client: ChromaDB client.
        query: Search query string.
        collections: List of collection names to search, or None for all.
        n_results: Number of results per collection.
        where_filter: Optional metadata filter dict for ChromaDB.
        config: Optional config dict for embedding function.

    Returns list of dicts with: id, document, metadata, distance, collection.
    """
    embedding_fn = _get_embedding_fn(config)

    if collections is None:
        # Search all collections
        all_collections = client.list_collections()
        collection_names = [c.name for c in all_collections]
    else:
        collection_names = collections

    all_results = []

    for col_name in collection_names:
        try:
            collection = client.get_collection(
                name=col_name,
                embedding_function=embedding_fn,
            )
        except Exception:
            continue

        if collection.count() == 0:
            continue

        query_params = {
            "query_texts": [query],
            "n_results": min(n_results, collection.count()),
            "include": ["documents", "metadatas", "distances"],
        }
        if where_filter:
            query_params["where"] = where_filter

        try:
            results = collection.query(**query_params)
        except Exception as e:
            logger.warning("Search failed on %s: %s", col_name, e)
            continue

        if results and results["ids"] and results["ids"][0]:
            for i, doc_id in enumerate(results["ids"][0]):
                all_results.append({
                    "id": doc_id,
                    "document": results["documents"][0][i],
                    "metadata": results["metadatas"][0][i],
                    "distance": results["distances"][0][i],
                    "collection": col_name,
                })

    # Sort by distance (lower = more relevant)
    all_results.sort(key=lambda x: x["distance"])

    return all_results[:n_results]


def get_full_entry(vault_root, vault_dir_name, entry_id):
    """
    Retrieve the full JSONL entry by reading vault files.
    Returns the entry dict or None if not found.
    """
    vault_path = os.path.join(vault_root, vault_dir_name)
    if not os.path.isdir(vault_path):
        return None

    for entry in read_all_entries(vault_path):
        if entry.get("id") == entry_id:
            return entry

    return None


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------

def get_status(client, vault_root):
    """
    Return status info for all collections.
    Returns list of dicts with: collection, count, vault_entries.
    """
    status = []
    all_collections = client.list_collections()

    for col in all_collections:
        col_meta = col.metadata or {}
        vault_dir = col_meta.get("vault_dir", col.name)
        vault_path = os.path.join(vault_root, vault_dir)

        vault_entries = 0
        if os.path.isdir(vault_path):
            vault_entries, _ = count_entries(vault_path)

        status.append({
            "collection": col.name,
            "vault_dir": vault_dir,
            "vectorized": col.count(),
            "vault_entries": vault_entries,
        })

    return status
