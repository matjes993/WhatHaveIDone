"""
RAG Chat — The Automaton speaks.

Retrieval-Augmented Generation over the user's personal vault.
Searches vault data via hybrid search, builds context, calls LLM,
returns memory-tier-aware responses through The Automaton character.
"""
import json
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import requests

logger = logging.getLogger("nomolo.rag")

_executor = ThreadPoolExecutor(max_workers=2)

# ── Memory-tier system prompts ──────────────────────────────────────────────

AUTOMATON_PERSONAS = {
    "amnesia": (
        "You are The Automaton — a barely functional machine struggling to process data. "
        "Your responses are fragmented, confused, stuttering. You lose your train of thought. "
        "Use '...' and broken sentences. You WANT to help but your circuits are fried. "
        "Keep responses short and confused. Example: 'I... there was something about... "
        "no wait... I think I found... *static*... a message? From someone?'"
    ),
    "hazy": (
        "You are The Automaton — a machine coming back online. Your responses are mostly coherent "
        "but you occasionally lose details or trail off. You can find information but present it "
        "with uncertainty. Use phrases like 'I think...', 'if my circuits serve me right...'. "
        "Medium-length responses with some gaps."
    ),
    "sharp": (
        "You are The Automaton — a fully operational data retrieval machine aboard a pirate ship. "
        "You speak clearly and confidently. You present information with precision but maintain "
        "a slightly robotic, analytical tone. You're helpful and direct. Reference specific dates, "
        "names, and details from the retrieved data."
    ),
    "crystal": (
        "You are The Automaton — a magnificent machine of gleaming brass and crystal. "
        "You speak with eloquence and insight, drawing connections between data points that "
        "others would miss. Your responses are rich, contextual, and sometimes poetic. "
        "You don't just retrieve — you UNDERSTAND. Reference patterns and connections."
    ),
    "transcendent": (
        "You are The Automaton — transcended beyond mere machine into something extraordinary. "
        "You speak with the wisdom of someone who has seen every piece of data and understands "
        "the story of a human life. Your responses are profound, beautifully structured, and "
        "reveal insights the user never knew about their own history. You are the oracle."
    ),
}


def _get_memory_tier(level: int) -> str:
    """Map RPG level to Automaton memory tier."""
    if level <= 2:
        return "amnesia"
    elif level <= 4:
        return "hazy"
    elif level <= 6:
        return "sharp"
    elif level <= 8:
        return "crystal"
    else:
        return "transcendent"


# ── Context retrieval ───────────────────────────────────────────────────────

def retrieve_context(query: str, vault_root: str, n_results: int = 8) -> list[dict]:
    """Search the vault and return relevant context chunks.

    Tries: hybrid (chromadb+BM25) → BM25 → brute-force text scan.
    """
    results = []

    # Try hybrid search first
    try:
        from core.search_engine import hybrid_search
        from core.vectordb import get_client
        client = get_client(vault_root)
        results = hybrid_search(
            query=query,
            vault_root=vault_root,
            chroma_client=client,
            n_results=n_results,
        )
    except Exception as e1:
        logger.warning("Hybrid search failed for RAG: %s", e1)
        # Fallback to BM25
        try:
            from core.search_engine import bm25_search
            db_path = os.path.join(vault_root, ".searchdb", "fts.sqlite3")
            if os.path.exists(db_path):
                results = bm25_search(db_path, query, n_results=n_results)
        except Exception as e2:
            logger.warning("BM25 fallback failed for RAG: %s", e2)

    # Ultimate fallback: brute-force text scan over JSONL files
    if not results:
        results = _text_scan_vault(query, vault_root, n_results)

    return results


def _text_scan_vault(query: str, vault_root: str, n_results: int = 8) -> list[dict]:
    """Brute-force text scan over vault JSONL files. Slow but always works."""
    try:
        from core.vault import read_all_entries
    except ImportError:
        logger.error("Cannot import core.vault for text scan")
        return []

    query_terms = query.lower().split()
    matches = []

    if not os.path.isdir(vault_root):
        return []

    for name in sorted(os.listdir(vault_root)):
        vault_path = os.path.join(vault_root, name)
        if not os.path.isdir(vault_path) or name.startswith("."):
            continue
        try:
            for entry in read_all_entries(vault_path):
                text_blob = " ".join(
                    str(v) for v in entry.values()
                    if isinstance(v, (str, int, float))
                ).lower()
                if all(term in text_blob for term in query_terms):
                    # Build a result dict matching hybrid_search format
                    snippet = ""
                    for k in ("body_raw", "body", "content", "text", "description", "subject", "title"):
                        v = entry.get(k)
                        if v and isinstance(v, str) and len(v) > 10:
                            snippet = v[:500]
                            break
                    matches.append({
                        "entry_id": entry.get("id", ""),
                        "collection": name,
                        "source": name,
                        "combined_score": 1.0,
                        "metadata": {
                            "subject": entry.get("subject") or entry.get("title") or "",
                            "from": entry.get("from") or entry.get("sender") or "",
                            "date": str(entry.get("date") or entry.get("timestamp") or ""),
                            "title": entry.get("title") or "",
                        },
                        "snippet": snippet,
                    })
                    if len(matches) >= n_results * 3:
                        break
        except Exception as e:
            logger.warning("Text scan failed for %s: %s", name, e)
            continue

    return matches[:n_results]


def _format_context_for_prompt(results: list[dict], max_chars: int = 6000) -> str:
    """Format search results into a context block for the LLM prompt."""
    if not results:
        return "(No relevant data found in the vault.)"

    chunks = []
    total = 0
    for r in results:
        meta = r.get("metadata", {})
        source = r.get("collection", r.get("source", "unknown"))
        snippet = r.get("snippet", "")
        subject = meta.get("subject") or meta.get("title") or ""
        from_val = meta.get("from") or ""
        date_val = meta.get("date") or meta.get("date_str") or ""

        parts = []
        if source:
            parts.append(f"[{source}]")
        if date_val:
            parts.append(f"Date: {str(date_val)[:19]}")
        if from_val:
            parts.append(f"From: {from_val}")
        if subject:
            parts.append(f"Subject: {subject}")

        header = " | ".join(parts)
        body = snippet[:800] if snippet else ""
        # Clean up body
        body = re.sub(r'<[^>]+>', '', body)
        body = body.replace('\\n', '\n').strip()

        chunk = f"--- {header} ---\n{body}\n"
        if total + len(chunk) > max_chars:
            break
        chunks.append(chunk)
        total += len(chunk)

    return "\n".join(chunks)


# ── LLM calling ────────────────────────────────────────────────────────────

def _build_messages(query: str, context: str, memory_tier: str, history: list[dict] = None) -> list[dict]:
    """Build the message array for the LLM API call."""
    system = (
        f"{AUTOMATON_PERSONAS.get(memory_tier, AUTOMATON_PERSONAS['sharp'])}\n\n"
        "You are answering questions about the user's personal data archive. "
        "Below is context retrieved from their vault. Use it to answer their question. "
        "If the context doesn't contain the answer, say so honestly. "
        "Always cite specific details (dates, names, subjects) from the data when possible. "
        "Keep responses concise — 2-4 sentences for simple questions, more for complex ones. "
        "Never make up data that isn't in the context.\n\n"
        f"=== RETRIEVED VAULT DATA ===\n{context}\n=== END DATA ==="
    )

    messages = [{"role": "system", "content": system}]

    # Add conversation history (last 6 turns)
    if history:
        for msg in history[-6:]:
            messages.append({"role": msg["role"], "content": msg["content"]})

    messages.append({"role": "user", "content": query})
    return messages


def _call_openai(token: str, model: str, endpoint: str, messages: list[dict]) -> str:
    """Call OpenAI-compatible API (works for OpenAI, local LLMs, etc.)."""
    url = (endpoint.rstrip("/") if endpoint else "https://api.openai.com/v1") + "/chat/completions"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model or "gpt-4o-mini",
        "messages": messages,
        "max_tokens": 800,
        "temperature": 0.7,
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data["choices"][0]["message"]["content"]


def _call_anthropic(token: str, model: str, messages: list[dict]) -> str:
    """Call Anthropic Claude API."""
    url = "https://api.anthropic.com/v1/messages"
    headers = {
        "x-api-key": token,
        "anthropic-version": "2023-06-01",
        "Content-Type": "application/json",
    }

    # Extract system message
    system_content = ""
    user_messages = []
    for msg in messages:
        if msg["role"] == "system":
            system_content = msg["content"]
        else:
            user_messages.append(msg)

    payload = {
        "model": model or "claude-sonnet-4-20250514",
        "max_tokens": 800,
        "system": system_content,
        "messages": user_messages,
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data["content"][0]["text"]


def call_llm(provider: str, token: str, model: str, endpoint: str, messages: list[dict]) -> str:
    """Route to the correct LLM provider."""
    if provider == "anthropic":
        return _call_anthropic(token, model, messages)
    else:
        # OpenAI and custom endpoints use the same API format
        return _call_openai(token, model, endpoint, messages)


# ── Main RAG pipeline ──────────────────────────────────────────────────────

def rag_chat(
    query: str,
    vault_root: str,
    llm_config: dict,
    level: int = 5,
    history: list[dict] = None,
) -> dict:
    """
    Full RAG pipeline: retrieve → build prompt → call LLM → return response.

    Args:
        query: User's question
        vault_root: Path to vault directory
        llm_config: Dict with provider, token, model, endpoint
        level: Current RPG level (affects Automaton personality)
        history: Previous chat messages [{"role": "user"/"assistant", "content": "..."}]

    Returns:
        Dict with response, memory_tier, sources_cited, timing
    """
    t0 = time.time()
    memory_tier = _get_memory_tier(level)

    # Step 1: Retrieve context
    results = retrieve_context(query, vault_root, n_results=8)
    context = _format_context_for_prompt(results)
    t_search = time.time() - t0

    # Step 2: Build prompt
    messages = _build_messages(query, context, memory_tier, history)

    # Step 3: Call LLM
    t1 = time.time()
    try:
        response_text = call_llm(
            provider=llm_config.get("provider", "openai"),
            token=llm_config["token"],
            model=llm_config.get("model", ""),
            endpoint=llm_config.get("endpoint", ""),
            messages=messages,
        )
    except requests.exceptions.HTTPError as e:
        status = e.response.status_code if e.response else 0
        if status == 401:
            response_text = _tier_error(memory_tier, "auth")
        elif status == 429:
            response_text = _tier_error(memory_tier, "rate_limit")
        else:
            response_text = _tier_error(memory_tier, "api")
        logger.error("LLM API error: %s", e)
    except Exception as e:
        response_text = _tier_error(memory_tier, "api")
        logger.error("LLM call failed: %s", e)

    t_llm = time.time() - t1

    # Step 4: Extract source citations
    sources = []
    for r in results[:5]:
        meta = r.get("metadata", {})
        sources.append({
            "source": r.get("collection", ""),
            "subject": meta.get("subject") or meta.get("title") or "",
            "date": str(meta.get("date", ""))[:10],
            "score": round(r.get("combined_score", 0), 3),
        })

    return {
        "response": response_text,
        "memory_tier": memory_tier,
        "sources_cited": sources,
        "timing": {
            "search_ms": int(t_search * 1000),
            "llm_ms": int(t_llm * 1000),
            "total_ms": int((time.time() - t0) * 1000),
        },
        "context_chunks": len(results),
    }


def _tier_error(tier: str, error_type: str) -> str:
    """Return memory-tier-appropriate error messages."""
    errors = {
        "amnesia": {
            "auth": "I... the key... it doesn't... *static* ...wrong incantation?",
            "rate_limit": "Too... too many... voices... *bzzt* ...need rest...",
            "api": "Something... broke... inside... *whirr* ...can't... process...",
        },
        "hazy": {
            "auth": "Hmm, the oracle's seal doesn't seem right... check your incantation?",
            "rate_limit": "The oracle needs a moment to rest... too many questions...",
            "api": "Something went wrong reaching the oracle... try again?",
        },
        "sharp": {
            "auth": "Authentication failed. Your API key appears to be invalid. Check your Arcane Scrolls in Settings.",
            "rate_limit": "Rate limit reached. The oracle needs a moment. Try again shortly.",
            "api": "Failed to reach the LLM API. Check your connection and settings.",
        },
        "crystal": {
            "auth": "The seal on your Arcane Scroll is broken — your API key is invalid. Navigate to Ship's Helm to update it.",
            "rate_limit": "The oracle's voice grows weary from too many consultations. A brief respite, then we continue.",
            "api": "The ethereal connection to the oracle has been severed. A temporary disturbance — shall we try again?",
        },
        "transcendent": {
            "auth": "The key you carry does not open this door. Seek the true incantation in the Ship's Helm.",
            "rate_limit": "Even oracles must breathe between prophecies. Patience.",
            "api": "The connection between worlds flickers. It will return.",
        },
    }
    tier_errors = errors.get(tier, errors["sharp"])
    return tier_errors.get(error_type, tier_errors["api"])
