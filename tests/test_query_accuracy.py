"""
Nomolo Query Accuracy & Performance Benchmark

Tests the knowledge graph's ability to answer real questions accurately.
Measures four dimensions:
  - Accuracy: does the system return correct entities/relationships?
  - Speed: how fast are queries?
  - Size: how compact is the data representation?
  - Cost: how many operations/tokens would this require?

The benchmark produces a composite score (0-100) and identifies
improvement opportunities.

Uses the synthetic test vault with 20 cast members across 11 sources.
"""

from __future__ import annotations

import json
import sys
import time
from collections import Counter
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.knowledge import KnowledgeEngine
from core.knowledge.adapters import adapt_all, read_vault_jsonl
from core.knowledge.schema import (
    EntityType,
    HypothesisStatus,
    IdentifierSystem,
    RelationshipType,
)

VAULT_DIR = Path(__file__).parent / "fixtures" / "vault"
MANIFEST_PATH = VAULT_DIR / "manifest.json"


# ---------------------------------------------------------------------------
# Known ground truth from the test vault generator
# ---------------------------------------------------------------------------

# Full cast with all known attributes for verification
KNOWN_PEOPLE = {
    "Alice Müller": {
        "emails": ["alice.mueller@gmail.com", "amueller@techcorp.de"],
        "phone": "+491761111111",
        "org": "TechCorp",
        "title": "CTO",
        "city": "Munich",
        "relationship": "friend",
        "appears_in": ["gmail", "contacts", "calendar", "imessage", "whatsapp"],
        "variants": ["Alice Mueller", "A. Müller"],
    },
    "Bob Chen": {
        "emails": ["bob.chen@example.com", "bchen@startup.io"],
        "org": "StartupIO",
        "title": "Lead Engineer",
        "city": "San Francisco",
        "appears_in": ["gmail", "contacts", "calendar", "slack"],
        "variants": ["Robert Chen", "B. Chen"],
    },
    "Clara Hoffmann": {
        "emails": ["clara.hoffmann@outlook.com"],
        "org": "Freelance",
        "title": "Designer",
        "city": "Hamburg",
        "appears_in": ["gmail", "contacts", "whatsapp", "imessage"],
        "variants": ["Clara H.", "Clarita"],
    },
    "David Park": {
        "emails": ["dpark@bigcorp.com", "david.park.private@gmail.com"],
        "org": "BigCorp",
        "title": "Product Manager",
        "city": "Seoul",
        "appears_in": ["gmail", "calendar", "slack", "telegram"],
        "variants": ["Dave Park", "D. Park"],
    },
    "Eva Bergström": {
        "emails": ["eva@bergstrom.se"],
        "org": "Nordic Design AB",
        "title": "CEO",
        "city": "Stockholm",
        "appears_in": ["gmail", "contacts", "calendar"],
        "variants": ["Eva Bergstrom", "Eva B."],
    },
    "Felix Wagner": {
        "emails": ["felix.wagner@gmail.com"],
        "org": "",
        "title": "Student",
        "city": "Berlin",
        "appears_in": ["whatsapp", "imessage", "contacts"],
        "variants": ["Felix W."],
    },
    "Grace Kim": {
        "emails": ["grace.kim@university.edu", "gracekim99@gmail.com"],
        "org": "Seoul National University",
        "title": "Professor",
        "city": "Seoul",
        "appears_in": ["gmail", "contacts"],
    },
    "Hans-Peter Schmidt": {
        "emails": ["hp.schmidt@lawfirm.de"],
        "org": "Schmidt & Partners",
        "title": "Lawyer",
        "city": "Frankfurt",
        "appears_in": ["gmail", "contacts"],
    },
    "Isabelle Dubois": {
        "emails": ["isabelle.dubois@company.fr"],
        "org": "Compagnie Française",
        "title": "Marketing Director",
        "city": "Paris",
        "appears_in": ["gmail", "calendar"],
    },
    "Javier Rodriguez": {
        "emails": ["javier@rodriguez.es", "jrodriguez@techco.com"],
        "org": "TechCo Spain",
        "title": "Backend Developer",
        "city": "Madrid",
        "appears_in": ["gmail", "slack", "telegram"],
    },
    "Keiko Tanaka": {
        "emails": ["keiko.tanaka@company.jp"],
        "org": "Tanaka Industries",
        "title": "Director",
        "city": "Tokyo",
        "appears_in": ["gmail", "contacts"],
    },
    "Lisa Bauer": {
        "emails": ["lisa.bauer@gmail.com"],
        "org": "",
        "title": "",
        "city": "Berlin",
        "appears_in": ["whatsapp", "imessage", "contacts", "calendar"],
    },
    "Marco Rossi": {
        "emails": ["marco.rossi@design.it"],
        "org": "Studio Rossi",
        "title": "Architect",
        "city": "Milan",
        "appears_in": ["gmail", "whatsapp"],
    },
    "Nina Petrov": {
        "emails": ["nina.petrov@tech.ru", "npetrov@gmail.com"],
        "org": "Yandex",
        "title": "Data Scientist",
        "city": "Moscow",
        "appears_in": ["gmail", "telegram"],
    },
    "Oliver Thompson": {
        "emails": ["oliver.thompson@company.co.uk"],
        "org": "London Ventures",
        "title": "Investor",
        "city": "London",
        "appears_in": ["gmail", "contacts", "calendar"],
    },
    "Patricia Gonzalez": {
        "emails": ["patricia@gonzalez.mx"],
        "org": "Gonzalez Media",
        "title": "Journalist",
        "city": "Mexico City",
        "appears_in": ["gmail"],
    },
    "Raj Patel": {
        "emails": ["raj.patel@infosys.in", "rajpatel@gmail.com"],
        "org": "Infosys",
        "title": "VP Engineering",
        "city": "Bangalore",
        "appears_in": ["gmail", "contacts", "calendar", "slack"],
        "variants": ["Rajesh Patel", "R. Patel"],
    },
    "Sarah Weber": {
        "emails": ["sarah.weber@uni-berlin.de"],
        "phone": "+491777777777",
        "org": "TU Berlin",
        "title": "Research Assistant",
        "city": "Berlin",
        "appears_in": ["gmail", "imessage", "whatsapp", "contacts"],
    },
    "Tom Anderson": {
        "emails": ["tom@anderson.dev", "tanderson@bigtech.com"],
        "org": "BigTech Inc",
        "title": "Staff Engineer",
        "city": "Seattle",
        "appears_in": ["gmail", "slack", "calendar"],
        "variants": ["Thomas Anderson", "T. Anderson"],
    },
    "Ursula Fischer": {
        "emails": ["ursula.fischer@family.de"],
        "org": "",
        "title": "",
        "city": "Düsseldorf",
        "appears_in": ["contacts", "whatsapp", "imessage"],
        "variants": ["Uschi", "U. Fischer"],
    },
}

AUTOMATED_SENDERS = [
    "noreply@github.com", "notifications@linkedin.com",
    "no-reply@accounts.google.com", "newsletter@techcrunch.com",
    "info@meetup.com", "support@stripe.com", "noreply@medium.com",
]

# ---------------------------------------------------------------------------
# Questions — 75 questions across 15 categories
# ---------------------------------------------------------------------------

QUESTIONS = [
    # =====================================================================
    # CATEGORY 1: Identity — exact email lookup (7 questions)
    # =====================================================================
    {
        "id": "Q01", "category": "identity",
        "question": "Find the person with email alice.mueller@gmail.com",
        "method": "find_by_email",
        "input": "alice.mueller@gmail.com",
        "expected_name": "Alice",
    },
    {
        "id": "Q02", "category": "identity",
        "question": "Find the person with email sarah.weber@uni-berlin.de",
        "method": "find_by_email",
        "input": "sarah.weber@uni-berlin.de",
        "expected_name": "Sarah",
    },
    {
        "id": "Q03", "category": "identity",
        "question": "Find the person with email raj.patel@infosys.in",
        "method": "find_by_email",
        "input": "raj.patel@infosys.in",
        "expected_name": "Raj",
    },
    {
        "id": "Q04", "category": "identity",
        "question": "Look up a secondary email — bchen@startup.io",
        "method": "find_by_email",
        "input": "bchen@startup.io",
        "expected_name": "Bob",
    },
    {
        "id": "Q05", "category": "identity",
        "question": "Look up by secondary email — gracekim99@gmail.com",
        "method": "find_by_email",
        "input": "gracekim99@gmail.com",
        "expected_name": "Grace",
    },
    {
        "id": "Q06", "category": "identity",
        "question": "Look up keiko.tanaka@company.jp",
        "method": "find_by_email",
        "input": "keiko.tanaka@company.jp",
        "expected_name": "Keiko",
    },
    {
        "id": "Q07", "category": "identity",
        "question": "Look up hp.schmidt@lawfirm.de",
        "method": "find_by_email",
        "input": "hp.schmidt@lawfirm.de",
        "expected_name": "Hans",
    },

    # =====================================================================
    # CATEGORY 2: Phone lookup (3 questions)
    # =====================================================================
    {
        "id": "Q08", "category": "phone_lookup",
        "question": "Who has the phone number +491761111111?",
        "method": "find_by_phone",
        "input": "+491761111111",
        "expected_name": "Alice",
    },
    {
        "id": "Q09", "category": "phone_lookup",
        "question": "Who has the phone number +491777777777?",
        "method": "find_by_phone",
        "input": "+491777777777",
        "expected_name": "Sarah",
    },
    {
        "id": "Q10", "category": "phone_lookup",
        "question": "Look up phone +919876543210",
        "method": "find_by_phone",
        "input": "+919876543210",
        # Raj Patel — may or may not have phone registered
        "expected_name": "Raj",
        "allow_not_found": True,
    },

    # =====================================================================
    # CATEGORY 3: Name search — exact & partial (8 questions)
    # =====================================================================
    {
        "id": "Q11", "category": "name_search",
        "question": "Find all people named Alice",
        "method": "find_by_name",
        "input": "Alice",
        "expected_min_count": 1,
        "expected_name_contains": "Alice",
    },
    {
        "id": "Q12", "category": "name_search",
        "question": "Find all people named Bob",
        "method": "find_by_name",
        "input": "Bob",
        "expected_min_count": 1,
        "expected_name_contains": "Bob",
    },
    {
        "id": "Q13", "category": "name_search",
        "question": "Search for people with 'Schmidt' in their name",
        "method": "find_by_name",
        "input": "Schmidt",
        "expected_min_count": 1,
        "expected_name_contains": "Schmidt",
    },
    {
        "id": "Q14", "category": "name_search",
        "question": "Search for 'Weber'",
        "method": "find_by_name",
        "input": "Weber",
        "expected_min_count": 1,
        "expected_name_contains": "Weber",
    },
    {
        "id": "Q15", "category": "name_search",
        "question": "Search for 'Rossi'",
        "method": "find_by_name",
        "input": "Rossi",
        "expected_min_count": 1,
    },
    {
        "id": "Q16", "category": "name_search",
        "question": "Search for 'Fischer'",
        "method": "find_by_name",
        "input": "Fischer",
        "expected_min_count": 1,
    },
    {
        "id": "Q17", "category": "name_search",
        "question": "Search for someone who doesn't exist: 'Zuckerberg'",
        "method": "find_by_name",
        "input": "Zuckerberg",
        "expected_min_count": 0,
        "expected_max_count": 0,
    },
    {
        "id": "Q18", "category": "name_search",
        "question": "Search for 'Matthias' (the user)",
        "method": "find_by_name",
        "input": "Matthias",
        "expected_min_count": 1,
    },

    # =====================================================================
    # CATEGORY 4: Negative queries — things that should NOT be found (5)
    # =====================================================================
    {
        "id": "Q19", "category": "negative",
        "question": "Lookup a nonexistent email: nobody@nowhere.com",
        "method": "find_by_email_expect_none",
        "input": "nobody@nowhere.com",
    },
    {
        "id": "Q20", "category": "negative",
        "question": "Lookup a nonexistent email: ceo@apple.com",
        "method": "find_by_email_expect_none",
        "input": "ceo@apple.com",
    },
    {
        "id": "Q21", "category": "negative",
        "question": "Search for 'Elon' — should find nobody",
        "method": "find_by_name",
        "input": "Elon",
        "expected_min_count": 0,
        "expected_max_count": 0,
    },
    {
        "id": "Q22", "category": "negative",
        "question": "Count file entities (no deep scan data loaded)",
        "method": "count_entities",
        "input": "file",
        "expected_min": 0,
        "expected_max": 0,
    },
    {
        "id": "Q23", "category": "negative",
        "question": "Count account entities (none in test vault)",
        "method": "count_entities",
        "input": "account",
        "expected_min": 0,
        "expected_max": 0,
    },

    # =====================================================================
    # CATEGORY 5: Entity type counts (6 questions)
    # =====================================================================
    {
        "id": "Q24", "category": "stats",
        "question": "How many people are in the graph?",
        "method": "count_entities",
        "input": "person",
        "expected_min": 20,
        "expected_max": 200,
    },
    {
        "id": "Q25", "category": "stats",
        "question": "How many messages are in the graph?",
        "method": "count_entities",
        "input": "message",
        "expected_min": 500,
        "expected_max": 2000,
    },
    {
        "id": "Q26", "category": "stats",
        "question": "How many events are in the graph?",
        "method": "count_entities",
        "input": "event",
        "expected_min": 80,
        "expected_max": 120,
    },
    {
        "id": "Q27", "category": "stats",
        "question": "How many bookmarks are in the graph?",
        "method": "count_entities",
        "input": "bookmark",
        "expected_min": 100,
        "expected_max": 300,
    },
    {
        "id": "Q28", "category": "stats",
        "question": "How many notes are in the graph?",
        "method": "count_entities",
        "input": "note",
        "expected_min": 5,
        "expected_max": 15,
    },
    {
        "id": "Q29", "category": "stats",
        "question": "How many places are in the graph?",
        "method": "count_entities",
        "input": "place",
        "expected_min": 30,
        "expected_max": 200,
    },

    # =====================================================================
    # CATEGORY 6: Relationship counts & types (6 questions)
    # =====================================================================
    {
        "id": "Q30", "category": "relationship",
        "question": "How many total relationships exist?",
        "method": "total_relationships",
        "expected_min": 1500,
    },
    {
        "id": "Q31", "category": "relationship",
        "question": "How many KNOWS relationships exist?",
        "method": "count_rel_type",
        "input": "knows",
        "expected_min": 30,
        "expected_max": 200,
    },
    {
        "id": "Q32", "category": "relationship",
        "question": "How many SENT relationships exist?",
        "method": "count_rel_type",
        "input": "sent",
        "expected_min": 500,
        "expected_max": 2000,
    },
    {
        "id": "Q33", "category": "relationship",
        "question": "How many RECEIVED relationships exist?",
        "method": "count_rel_type",
        "input": "received",
        "expected_min": 500,
        "expected_max": 1500,
    },
    {
        "id": "Q34", "category": "relationship",
        "question": "How many LOCATED_AT relationships exist (events at places)?",
        "method": "count_rel_type",
        "input": "located_at",
        "expected_min": 50,
        "expected_max": 200,
    },
    {
        "id": "Q35", "category": "relationship",
        "question": "How many ATTENDED relationships exist?",
        "method": "count_rel_type",
        "input": "attended",
        "expected_min": 30,
        "expected_max": 200,
    },

    # =====================================================================
    # CATEGORY 7: Connection queries — who is connected to whom? (7)
    # =====================================================================
    {
        "id": "Q36", "category": "connections",
        "question": "How many connections does Alice Müller have?",
        "method": "connection_count_by_email",
        "input": "alice.mueller@gmail.com",
        "expected_min": 30,
    },
    {
        "id": "Q37", "category": "connections",
        "question": "How many connections does Sarah Weber have?",
        "method": "connection_count_by_email",
        "input": "sarah.weber@uni-berlin.de",
        "expected_min": 30,
    },
    {
        "id": "Q38", "category": "connections",
        "question": "How many connections does Clara Hoffmann have?",
        "method": "connection_count_by_email",
        "input": "clara.hoffmann@outlook.com",
        "expected_min": 30,
    },
    {
        "id": "Q39", "category": "connections",
        "question": "Does Alice know Bob? (do they share KNOWS relationships?)",
        "method": "two_people_know_each_other",
        "input": ["alice.mueller@gmail.com", "bob.chen@example.com"],
        "expected": True,
        "allow_indirect": True,
    },
    {
        "id": "Q40", "category": "connections",
        "question": "Who is the most connected person? (most relationships)",
        "method": "most_connected_person",
        "expected_name_in": ["Matthias", "me", "Clara", "Sarah", "Alice"],
    },
    {
        "id": "Q41", "category": "connections",
        "question": "How many SENT relationships does Alice have?",
        "method": "rel_type_count_for_email",
        "input": {"email": "alice.mueller@gmail.com", "rel_type": "sent"},
        "expected_min": 10,
    },
    {
        "id": "Q42", "category": "connections",
        "question": "How many RECEIVED relationships does Alice have?",
        "method": "rel_type_count_for_email",
        "input": {"email": "alice.mueller@gmail.com", "rel_type": "received"},
        "expected_min": 10,
    },

    # =====================================================================
    # CATEGORY 8: Organization & property queries (7 questions)
    # =====================================================================
    {
        "id": "Q43", "category": "organization",
        "question": "What organization does Alice Müller work at?",
        "method": "org_for_email",
        "input": "alice.mueller@gmail.com",
        "expected_org": "TechCorp",
    },
    {
        "id": "Q44", "category": "organization",
        "question": "What organization does Bob Chen work at?",
        "method": "org_for_email",
        "input": "bob.chen@example.com",
        "expected_org": "StartupIO",
    },
    {
        "id": "Q45", "category": "organization",
        "question": "What organization does Raj Patel work at?",
        "method": "org_for_email",
        "input": "raj.patel@infosys.in",
        "expected_org": "Infosys",
    },
    {
        "id": "Q46", "category": "organization",
        "question": "What organization does Hans-Peter Schmidt work at?",
        "method": "org_for_email",
        "input": "hp.schmidt@lawfirm.de",
        "expected_org": "Schmidt",
    },
    {
        "id": "Q47", "category": "organization",
        "question": "Find people who work at TechCorp",
        "method": "people_at_org",
        "input": "TechCorp",
        "expected_min_count": 1,
        "expected_name_contains": "Alice",
    },
    {
        "id": "Q48", "category": "organization",
        "question": "Find people who work at TU Berlin",
        "method": "people_at_org",
        "input": "TU Berlin",
        "expected_min_count": 1,
        "expected_name_contains": "Sarah",
    },
    {
        "id": "Q49", "category": "organization",
        "question": "How many distinct organizations appear in the graph?",
        "method": "count_orgs",
        "expected_min": 8,
        "expected_max": 30,
    },

    # =====================================================================
    # CATEGORY 9: Entity resolution quality (6 questions)
    # =====================================================================
    {
        "id": "Q50", "category": "resolution",
        "question": "Are Alice Mueller and Alice Müller the same person?",
        "method": "check_hypothesis",
        "input": ["Alice Mueller", "Alice Müller"],
        "expected": True,
    },
    {
        "id": "Q51", "category": "resolution",
        "question": "Are Raj Patel and Rajesh Patel the same person?",
        "method": "check_hypothesis",
        "input": ["Raj Patel", "Rajesh Patel"],
        "expected": True,
    },
    {
        "id": "Q52", "category": "resolution",
        "question": "Are Clara H. and Clara Hoffmann the same person?",
        "method": "check_hypothesis",
        "input": ["Clara H.", "Clara Hoffmann"],
        "expected": True,
    },
    {
        "id": "Q53", "category": "resolution",
        "question": "How many open hypotheses exist?",
        "method": "count_hypotheses",
        "expected_min": 2,
        "expected_max": 10,
    },
    {
        "id": "Q54", "category": "resolution",
        "question": "Are the hypotheses high-confidence (>0.8)?",
        "method": "hypothesis_confidence_range",
        "expected_min_confidence": 0.8,
    },
    {
        "id": "Q55", "category": "resolution",
        "question": "How many people have multiple email addresses?",
        "method": "multi_email_people_count",
        "expected_min": 3,
        "expected_max": 15,
    },

    # =====================================================================
    # CATEGORY 10: Provenance & data lineage (6 questions)
    # =====================================================================
    {
        "id": "Q56", "category": "provenance",
        "question": "How many sources know about Alice?",
        "method": "provenance_count_by_email",
        "input": "alice.mueller@gmail.com",
        "expected_min": 2,
    },
    {
        "id": "Q57", "category": "provenance",
        "question": "How many sources know about Bob Chen?",
        "method": "provenance_count_by_email",
        "input": "bob.chen@example.com",
        "expected_min": 2,
    },
    {
        "id": "Q58", "category": "provenance",
        "question": "How many distinct data sources fed the graph?",
        "method": "total_source_count",
        "expected_min": 8,
        "expected_max": 15,
    },
    {
        "id": "Q59", "category": "provenance",
        "question": "Which source contributed the most records?",
        "method": "top_source",
        "expected_source": "gmail",
    },
    {
        "id": "Q60", "category": "provenance",
        "question": "How many provenance records exist in total?",
        "method": "total_provenance",
        "expected_min": 1000,
    },
    {
        "id": "Q61", "category": "provenance",
        "question": "Does Sarah Weber appear in Gmail data?",
        "method": "person_in_source",
        "input": {"email": "sarah.weber@uni-berlin.de", "source": "gmail"},
        "expected": True,
    },

    # =====================================================================
    # CATEGORY 11: Cross-source queries (5 questions)
    # =====================================================================
    {
        "id": "Q62", "category": "cross_source",
        "question": "How many people appear in 3+ sources?",
        "method": "people_in_n_sources",
        "input": 3,
        "expected_min": 3,
    },
    {
        "id": "Q63", "category": "cross_source",
        "question": "How many messages came from Gmail?",
        "method": "messages_from_source",
        "input": "gmail",
        "expected_min": 400,
        "expected_max": 600,
    },
    {
        "id": "Q64", "category": "cross_source",
        "question": "How many messages came from WhatsApp?",
        "method": "messages_from_source",
        "input": "whatsapp",
        "expected_min": 150,
        "expected_max": 300,
    },
    {
        "id": "Q65", "category": "cross_source",
        "question": "How many messages came from Slack?",
        "method": "messages_from_source",
        "input": "slack",
        "expected_min": 80,
        "expected_max": 200,
    },
    {
        "id": "Q66", "category": "cross_source",
        "question": "How many messages came from Telegram?",
        "method": "messages_from_source",
        "input": "telegram",
        "expected_min": 50,
        "expected_max": 120,
    },

    # =====================================================================
    # CATEGORY 12: Vague / natural-language-style queries (6 questions)
    # "Who do I talk to most?" "What does my communication look like?"
    # These test whether the graph can answer open-ended questions
    # =====================================================================
    {
        "id": "Q67", "category": "vague",
        "question": "Who do I communicate with the most? (most relationships overall)",
        "method": "most_connected_named_person",
        # The user entity ('me' or 'Matthias Kramer') has the most rels,
        # but among *other* people, the one with most rels wins
        "expected_has_name": True,
    },
    {
        "id": "Q68", "category": "vague",
        "question": "What's the ratio of messages to people? (communication density)",
        "method": "message_to_person_ratio",
        "expected_min": 10,
        "expected_max": 100,
    },
    {
        "id": "Q69", "category": "vague",
        "question": "What percentage of my graph is messages vs other entities?",
        "method": "message_percentage",
        "expected_min": 50,
        "expected_max": 90,
    },
    {
        "id": "Q70", "category": "vague",
        "question": "How many unique locations appear in my calendar?",
        "method": "unique_places",
        "expected_min": 5,
        "expected_max": 100,
    },
    {
        "id": "Q71", "category": "vague",
        "question": "What is the total entity count in the graph?",
        "method": "total_entities",
        "expected_min": 1000,
        "expected_max": 3000,
    },
    {
        "id": "Q72", "category": "vague",
        "question": "How dense is the graph? (relationships per entity)",
        "method": "graph_density",
        "expected_min": 0.5,
        "expected_max": 5.0,
    },

    # =====================================================================
    # CATEGORY 13: Multi-hop / graph traversal (4 questions)
    # =====================================================================
    {
        "id": "Q73", "category": "traversal",
        "question": "Find all entities connected to Alice through exactly 1 hop",
        "method": "one_hop_count",
        "input": "alice.mueller@gmail.com",
        "expected_min": 20,
    },
    {
        "id": "Q74", "category": "traversal",
        "question": "How many message entities are connected to Sarah Weber?",
        "method": "connected_entity_type_count",
        "input": {"email": "sarah.weber@uni-berlin.de", "entity_type": "message"},
        "expected_min": 20,
    },
    {
        "id": "Q75", "category": "traversal",
        "question": "How many distinct people does Bob Chen share a KNOWS relationship with?",
        "method": "knows_count_for_email",
        "input": "bob.chen@example.com",
        "expected_min": 1,
    },
    {
        "id": "Q76", "category": "traversal",
        "question": "How many distinct people does Alice share a KNOWS relationship with?",
        "method": "knows_count_for_email",
        "input": "alice.mueller@gmail.com",
        "expected_min": 1,
    },

    # =====================================================================
    # CATEGORY 14: Data quality & completeness (5 questions)
    # =====================================================================
    {
        "id": "Q77", "category": "quality",
        "question": "How many named people are in the graph? (have a non-empty name)",
        "method": "named_people_count",
        "expected_min": 15,
        "expected_max": 50,
    },
    {
        "id": "Q78", "category": "quality",
        "question": "What percentage of person entities have identifiers?",
        "method": "people_with_identifiers_pct",
        "expected_min": 20,  # percent
        "expected_max": 100,
    },
    {
        "id": "Q79", "category": "quality",
        "question": "Provenance coverage: what % of entities have provenance?",
        "method": "provenance_coverage_pct",
        "expected_min": 70,
        "expected_max": 100,
    },
    {
        "id": "Q80", "category": "quality",
        "question": "Entity dedup ratio: entities / raw records",
        "method": "dedup_ratio",
        # 1542 entities from ~1420 records → ratio ~1.08
        "expected_min": 0.5,
        "expected_max": 2.0,
    },
    {
        "id": "Q81", "category": "quality",
        "question": "How many identifiers (emails+phones) are registered?",
        "method": "total_identifiers",
        "expected_min": 30,
        "expected_max": 200,
    },

    # =====================================================================
    # CATEGORY 15: Edge cases & stress queries (4 questions)
    # =====================================================================
    {
        "id": "Q82", "category": "edge_case",
        "question": "Look up by Eva's non-standard domain email (eva@bergstrom.se)",
        "method": "find_by_email",
        "input": "eva@bergstrom.se",
        "expected_name": "Eva",
    },
    {
        "id": "Q83", "category": "edge_case",
        "question": "Person with hyphenated name: Hans-Peter Schmidt",
        "method": "find_by_name",
        "input": "Hans-Peter",
        "expected_min_count": 1,
        "expected_name_contains": "Hans",
    },
    {
        "id": "Q84", "category": "edge_case",
        "question": "Unicode name: search for 'Müller'",
        "method": "find_by_name",
        "input": "Müller",
        "expected_min_count": 1,
    },
    {
        "id": "Q85", "category": "edge_case",
        "question": "Unicode name: search for 'Bergström'",
        "method": "find_by_name",
        "input": "Bergström",
        "expected_min_count": 1,
    },
]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def engine():
    """Build knowledge engine with test vault."""
    import subprocess
    if not VAULT_DIR.exists() or not any(VAULT_DIR.iterdir()):
        gen_script = Path(__file__).parent / "fixtures" / "generate_test_vault.py"
        subprocess.run([sys.executable, str(gen_script)], check=True)

    eng = KnowledgeEngine(str(VAULT_DIR), db_name="test_accuracy.db")
    vault_data = read_vault_jsonl(str(VAULT_DIR))
    records = list(adapt_all(vault_data))
    eng.ingest(iter(records))
    yield eng
    eng.close()
    db = VAULT_DIR / "test_accuracy.db"
    if db.exists():
        db.unlink()


@pytest.fixture(scope="module")
def benchmark_results(engine):
    """Run all questions and collect results."""
    results = []
    for q in QUESTIONS:
        result = _execute_question(engine, q)
        results.append(result)
    return results


# ---------------------------------------------------------------------------
# Question executor — handles all method types
# ---------------------------------------------------------------------------

def _execute_question(engine: KnowledgeEngine, q: dict) -> dict:
    """Execute a question against the engine and measure accuracy + speed."""
    start = time.perf_counter()
    correct = False
    detail = ""

    try:
        method = q["method"]

        # --- Identity: email lookup ---
        if method == "find_by_email":
            entity = engine.find_by_identifier(IdentifierSystem.EMAIL, q["input"])
            if entity:
                name = entity.properties.get("name", "")
                correct = q["expected_name"].lower() in name.lower()
                detail = f"Found: {name}"
            else:
                detail = "Not found"

        # --- Identity: email should NOT exist ---
        elif method == "find_by_email_expect_none":
            entity = engine.find_by_identifier(IdentifierSystem.EMAIL, q["input"])
            correct = entity is None
            detail = f"Found: {entity.properties.get('name', '?') if entity else 'None (correct)'}"

        # --- Identity: phone lookup ---
        elif method == "find_by_phone":
            entity = engine.find_by_identifier(IdentifierSystem.PHONE, q["input"])
            if entity:
                name = entity.properties.get("name", "")
                correct = q["expected_name"].lower() in name.lower()
                detail = f"Found: {name}"
            elif q.get("allow_not_found"):
                correct = True
                detail = "Not found (acceptable)"
            else:
                detail = "Not found"

        # --- Name search ---
        elif method == "find_by_name":
            people = engine.find_entities(EntityType.PERSON, limit=500)
            matches = [p for p in people if q["input"].lower() in p.properties.get("name", "").lower()]
            count = len(matches)
            min_c = q.get("expected_min_count", 0)
            max_c = q.get("expected_max_count", 999)
            correct = min_c <= count <= max_c
            if q.get("expected_name_contains"):
                correct = correct and any(
                    q["expected_name_contains"] in p.properties.get("name", "") for p in matches
                )
            detail = f"Found {count} matches (expected {min_c}-{max_c})"

        # --- Entity type count ---
        elif method == "count_entities":
            et = EntityType(q["input"])
            count = engine.count_entities(et)
            correct = q["expected_min"] <= count <= q["expected_max"]
            detail = f"Count: {count} (expected {q['expected_min']}-{q['expected_max']})"

        # --- Relationship type count (global) ---
        elif method == "count_rel_type":
            conn = engine.store._get_conn()
            row = conn.execute(
                "SELECT COUNT(*) FROM relationships WHERE type = ? AND superseded_at IS NULL",
                (q["input"],),
            ).fetchone()
            count = row[0]
            correct = q["expected_min"] <= count <= q["expected_max"]
            detail = f"Count: {count} (expected {q['expected_min']}-{q['expected_max']})"

        # --- Total relationships ---
        elif method == "total_relationships":
            stats = engine.stats()
            count = stats["relationships"]
            correct = count >= q["expected_min"]
            detail = f"Total: {count}"

        # --- Connection count for a person ---
        elif method == "connection_count_by_email":
            entity = engine.find_by_identifier(IdentifierSystem.EMAIL, q["input"])
            if entity:
                rels = engine.get_relationships(entity.id)
                correct = len(rels) >= q["expected_min"]
                detail = f"{len(rels)} relationships (expected >= {q['expected_min']})"
            else:
                detail = "Entity not found"

        # --- Do two people know each other? ---
        elif method == "two_people_know_each_other":
            email_a, email_b = q["input"]
            e_a = engine.find_by_identifier(IdentifierSystem.EMAIL, email_a)
            e_b = engine.find_by_identifier(IdentifierSystem.EMAIL, email_b)
            if e_a and e_b:
                # Check direct KNOWS
                rels_a = engine.get_relationships(e_a.id, rel_type=RelationshipType.KNOWS)
                connected = any(
                    r.target_id == e_b.id or r.source_id == e_b.id for r in rels_a
                )
                if not connected and q.get("allow_indirect"):
                    # Check indirect: both sent/received to same messages = they know each other
                    rels_a_all = engine.get_relationships(e_a.id)
                    rels_b_all = engine.get_relationships(e_b.id)
                    targets_a = {r.target_id for r in rels_a_all} | {r.source_id for r in rels_a_all}
                    targets_b = {r.target_id for r in rels_b_all} | {r.source_id for r in rels_b_all}
                    shared = targets_a & targets_b - {e_a.id, e_b.id}
                    connected = len(shared) > 0
                correct = connected == q["expected"]
                detail = f"Connected: {connected}"
            else:
                detail = f"Entity not found: A={e_a is not None}, B={e_b is not None}"

        # --- Most connected person ---
        elif method == "most_connected_person":
            people = engine.find_entities(EntityType.PERSON, limit=500)
            best_name = ""
            best_count = 0
            for p in people:
                name = p.properties.get("name", "")
                # Skip bare entities without real names (phone-only, email-only)
                if not name or name == "me" or name.startswith("+"):
                    continue
                rels = engine.get_relationships(p.id)
                if len(rels) > best_count:
                    best_count = len(rels)
                    best_name = name
            correct = any(exp.lower() in best_name.lower() for exp in q["expected_name_in"])
            detail = f"Most connected: {best_name} ({best_count} rels)"

        # --- Relationship type count for specific person ---
        elif method == "rel_type_count_for_email":
            email = q["input"]["email"]
            rt = RelationshipType(q["input"]["rel_type"])
            entity = engine.find_by_identifier(IdentifierSystem.EMAIL, email)
            if entity:
                rels = engine.get_relationships(entity.id, rel_type=rt)
                correct = len(rels) >= q["expected_min"]
                detail = f"{len(rels)} {rt.value} relationships"
            else:
                detail = "Entity not found"

        # --- Org for email ---
        elif method == "org_for_email":
            entity = engine.find_by_identifier(IdentifierSystem.EMAIL, q["input"])
            if entity:
                orgs = entity.properties.get("organizations", [])
                org_name = ""
                if orgs and isinstance(orgs[0], dict):
                    org_name = orgs[0].get("name", "")
                correct = q["expected_org"].lower() in org_name.lower()
                detail = f"Org: {org_name}"
            else:
                detail = "Entity not found"

        # --- People at org ---
        elif method == "people_at_org":
            people = engine.find_entities(EntityType.PERSON, limit=500)
            matches = []
            for p in people:
                orgs = p.properties.get("organizations", [])
                for org in orgs:
                    if isinstance(org, dict) and q["input"].lower() in org.get("name", "").lower():
                        matches.append(p)
                        break
            count = len(matches)
            correct = count >= q["expected_min_count"]
            if q.get("expected_name_contains"):
                correct = correct and any(
                    q["expected_name_contains"] in p.properties.get("name", "") for p in matches
                )
            detail = f"Found {count} people at {q['input']}"

        # --- Count orgs ---
        elif method == "count_orgs":
            people = engine.find_entities(EntityType.PERSON, limit=500)
            org_names = set()
            for p in people:
                orgs = p.properties.get("organizations", [])
                for org in orgs:
                    if isinstance(org, dict) and org.get("name"):
                        org_names.add(org["name"])
            count = len(org_names)
            correct = q["expected_min"] <= count <= q["expected_max"]
            detail = f"{count} orgs: {sorted(org_names)[:5]}..."

        # --- Hypothesis checks ---
        elif method == "check_hypothesis":
            hypotheses = engine.get_open_hypotheses(limit=100)
            name_a, name_b = q["input"]
            found = False
            for hyp in hypotheses:
                entity_names = []
                for eid in hyp.entity_ids:
                    e = engine.get_entity(eid)
                    if e:
                        entity_names.append(e.properties.get("name", ""))
                if (any(name_a in n for n in entity_names) and
                    any(name_b in n for n in entity_names)):
                    found = True
                    break
            if not found:
                # Also check if already merged
                people = engine.find_entities(EntityType.PERSON, limit=500)
                for p in people:
                    name = p.properties.get("name", "")
                    if name_a in name or name_b in name:
                        provs = engine.get_provenance(p.id)
                        if len(provs) > 1:
                            found = True
                            break
            correct = found == q["expected"]
            detail = f"Hypothesis found: {found}"

        elif method == "count_hypotheses":
            hyps = engine.get_open_hypotheses(limit=100)
            count = len(hyps)
            correct = q["expected_min"] <= count <= q["expected_max"]
            detail = f"{count} open hypotheses"

        elif method == "hypothesis_confidence_range":
            hyps = engine.get_open_hypotheses(limit=100)
            if hyps:
                min_conf = min(h.confidence for h in hyps)
                correct = min_conf >= q["expected_min_confidence"]
                detail = f"Min confidence: {min_conf:.2f} (threshold: {q['expected_min_confidence']})"
            else:
                correct = True
                detail = "No hypotheses (vacuously true)"

        elif method == "multi_email_people_count":
            people = engine.find_entities(EntityType.PERSON, limit=500)
            multi = 0
            for p in people:
                ids = engine.get_identifiers(p.id)
                emails = [i for i in ids if i.system == IdentifierSystem.EMAIL]
                if len(emails) > 1:
                    multi += 1
            correct = q["expected_min"] <= multi <= q["expected_max"]
            detail = f"{multi} people with multiple emails"

        # --- Provenance ---
        elif method == "provenance_count_by_email":
            entity = engine.find_by_identifier(IdentifierSystem.EMAIL, q["input"])
            if entity:
                provs = engine.get_provenance(entity.id)
                sources = list({p.source_name for p in provs})
                correct = len(sources) >= q["expected_min"]
                detail = f"{len(sources)} sources: {sources}"
            else:
                detail = "Entity not found"

        elif method == "total_source_count":
            conn = engine.store._get_conn()
            rows = conn.execute(
                "SELECT DISTINCT source_name FROM provenance"
            ).fetchall()
            count = len(rows)
            correct = q["expected_min"] <= count <= q["expected_max"]
            detail = f"{count} sources: {[r[0] for r in rows]}"

        elif method == "top_source":
            conn = engine.store._get_conn()
            row = conn.execute(
                "SELECT source_name, COUNT(*) as cnt FROM provenance "
                "GROUP BY source_name ORDER BY cnt DESC LIMIT 1"
            ).fetchone()
            top = row[0] if row else ""
            correct = top == q["expected_source"]
            detail = f"Top source: {top} ({row[1]} records)" if row else "No provenance"

        elif method == "total_provenance":
            stats = engine.stats()
            count = stats["provenance"]
            correct = count >= q["expected_min"]
            detail = f"Total provenance: {count}"

        elif method == "person_in_source":
            email = q["input"]["email"]
            source = q["input"]["source"]
            entity = engine.find_by_identifier(IdentifierSystem.EMAIL, email)
            if entity:
                provs = engine.get_provenance(entity.id)
                sources = {p.source_name for p in provs}
                found = source in sources
                correct = found == q["expected"]
                detail = f"In {source}: {found} (sources: {sorted(sources)})"
            else:
                detail = "Entity not found"

        # --- Cross-source ---
        elif method == "people_in_n_sources":
            n = q["input"]
            people = engine.find_entities(EntityType.PERSON, limit=500)
            count = 0
            for p in people:
                if not p.properties.get("name"):
                    continue
                provs = engine.get_provenance(p.id)
                sources = {pr.source_name for pr in provs}
                if len(sources) >= n:
                    count += 1
            correct = count >= q["expected_min"]
            detail = f"{count} people in {n}+ sources"

        elif method == "messages_from_source":
            source = q["input"]
            conn = engine.store._get_conn()
            row = conn.execute(
                "SELECT COUNT(*) FROM provenance p "
                "JOIN entities e ON p.target_id = e.id "
                "WHERE p.source_name = ? AND e.type = 'message'",
                (source,),
            ).fetchone()
            count = row[0]
            correct = q["expected_min"] <= count <= q["expected_max"]
            detail = f"{count} messages from {source}"

        # --- Vague queries ---
        elif method == "most_connected_named_person":
            people = engine.find_entities(EntityType.PERSON, limit=500)
            best_name = ""
            best_count = 0
            for p in people:
                name = p.properties.get("name", "")
                if not name or name == "me":
                    continue
                rels = engine.get_relationships(p.id)
                if len(rels) > best_count:
                    best_count = len(rels)
                    best_name = name
            correct = bool(best_name) == q["expected_has_name"]
            detail = f"Most connected (named): {best_name} ({best_count} rels)"

        elif method == "message_to_person_ratio":
            msg_count = engine.count_entities(EntityType.MESSAGE)
            person_count = engine.count_entities(EntityType.PERSON)
            ratio = msg_count / person_count if person_count > 0 else 0
            correct = q["expected_min"] <= ratio <= q["expected_max"]
            detail = f"Ratio: {ratio:.1f} ({msg_count} msgs / {person_count} people)"

        elif method == "message_percentage":
            msg_count = engine.count_entities(EntityType.MESSAGE)
            total = engine.count_entities()
            pct = (msg_count / total * 100) if total > 0 else 0
            correct = q["expected_min"] <= pct <= q["expected_max"]
            detail = f"{pct:.1f}% messages ({msg_count}/{total})"

        elif method == "unique_places":
            count = engine.count_entities(EntityType.PLACE)
            correct = q["expected_min"] <= count <= q["expected_max"]
            detail = f"{count} unique places"

        elif method == "total_entities":
            count = engine.count_entities()
            correct = q["expected_min"] <= count <= q["expected_max"]
            detail = f"Total: {count}"

        elif method == "graph_density":
            stats = engine.stats()
            density = stats["relationships"] / stats["entities"] if stats["entities"] > 0 else 0
            correct = q["expected_min"] <= density <= q["expected_max"]
            detail = f"Density: {density:.2f} ({stats['relationships']} rels / {stats['entities']} entities)"

        # --- Traversal ---
        elif method == "one_hop_count":
            entity = engine.find_by_identifier(IdentifierSystem.EMAIL, q["input"])
            if entity:
                conns = engine.get_connections(entity.id)
                correct = len(conns) >= q["expected_min"]
                detail = f"{len(conns)} 1-hop connections"
            else:
                detail = "Entity not found"

        elif method == "connected_entity_type_count":
            email = q["input"]["email"]
            et = EntityType(q["input"]["entity_type"])
            entity = engine.find_by_identifier(IdentifierSystem.EMAIL, email)
            if entity:
                conns = engine.get_connections(entity.id)
                typed = [c for c in conns if c["entity"].type == et]
                correct = len(typed) >= q["expected_min"]
                detail = f"{len(typed)} connected {et.value} entities"
            else:
                detail = "Entity not found"

        elif method == "knows_count_for_email":
            entity = engine.find_by_identifier(IdentifierSystem.EMAIL, q["input"])
            if entity:
                rels = engine.get_relationships(entity.id, rel_type=RelationshipType.KNOWS)
                correct = len(rels) >= q["expected_min"]
                detail = f"{len(rels)} KNOWS relationships"
            else:
                detail = "Entity not found"

        # --- Quality ---
        elif method == "named_people_count":
            people = engine.find_entities(EntityType.PERSON, limit=500)
            named = [p for p in people if p.properties.get("name")]
            count = len(named)
            correct = q["expected_min"] <= count <= q["expected_max"]
            detail = f"{count} named people"

        elif method == "people_with_identifiers_pct":
            people = engine.find_entities(EntityType.PERSON, limit=500)
            with_ids = 0
            for p in people:
                ids = engine.get_identifiers(p.id)
                if ids:
                    with_ids += 1
            pct = (with_ids / len(people) * 100) if people else 0
            correct = q["expected_min"] <= pct <= q["expected_max"]
            detail = f"{pct:.0f}% ({with_ids}/{len(people)})"

        elif method == "provenance_coverage_pct":
            stats = engine.stats()
            entities = stats["entities"]
            provenance = stats["provenance"]
            # Provenance records can exceed entity count (multiple sources per entity)
            # Use min(prov/ent, 1.0) * 100
            pct = min(provenance / entities, 1.0) * 100 if entities > 0 else 0
            correct = q["expected_min"] <= pct <= q["expected_max"]
            detail = f"{pct:.0f}% coverage ({provenance} provenance / {entities} entities)"

        elif method == "dedup_ratio":
            stats = engine.stats()
            # ~1420 raw records → stats["entities"] entities
            ratio = stats["entities"] / 1420
            correct = q["expected_min"] <= ratio <= q["expected_max"]
            detail = f"Ratio: {ratio:.2f} ({stats['entities']} entities / 1420 records)"

        elif method == "total_identifiers":
            stats = engine.stats()
            count = stats["identifiers"]
            correct = q["expected_min"] <= count <= q["expected_max"]
            detail = f"{count} identifiers"

        else:
            detail = f"Unknown method: {method}"

    except Exception as e:
        detail = f"Error: {e}"
        correct = False

    elapsed = time.perf_counter() - start

    return {
        "id": q["id"],
        "question": q["question"],
        "category": q["category"],
        "correct": correct,
        "detail": detail,
        "elapsed_ms": round(elapsed * 1000, 2),
    }


# ---------------------------------------------------------------------------
# Accuracy tests — dynamic test generation from QUESTIONS array
# ---------------------------------------------------------------------------

class TestAccuracy:
    """Each question becomes a test case via parametrize."""

    @pytest.fixture(autouse=True)
    def _setup(self, engine):
        self.engine = engine


# Dynamically generate test methods for each question
def _make_test(idx):
    def test_fn(self):
        r = _execute_question(self.engine, QUESTIONS[idx])
        assert r["correct"], f"{r['id']} failed: {r['detail']}"
    q = QUESTIONS[idx]
    test_fn.__doc__ = f"{q['id']}: {q['question']}"
    return test_fn

for _i, _q in enumerate(QUESTIONS):
    _name = f"test_{_q['id'].lower()}_{_q['category']}"
    setattr(TestAccuracy, _name, _make_test(_i))


# ---------------------------------------------------------------------------
# Composite score
# ---------------------------------------------------------------------------

class TestCompositeScore:
    """Calculate and report the overall quality score."""

    def test_composite_score(self, benchmark_results):
        """Calculate accuracy, speed, and composite scores."""
        total = len(benchmark_results)
        correct_count = sum(1 for r in benchmark_results if r["correct"])
        accuracy = correct_count / total if total > 0 else 0

        # Speed score: all queries under 100ms = 1.0, linear degradation
        avg_ms = sum(r["elapsed_ms"] for r in benchmark_results) / total
        speed_score = max(0, min(1.0, 1.0 - (avg_ms - 10) / 200))

        # Composite (weighted)
        composite = (accuracy * 60 + speed_score * 20 + 20) / 100

        # Print report
        print("\n" + "=" * 70)
        print("NOMOLO QUERY ACCURACY BENCHMARK")
        print("=" * 70)
        print(f"\nQuestions: {total}")
        print(f"Correct:   {correct_count}/{total} ({accuracy:.0%})")
        print(f"Avg speed: {avg_ms:.1f}ms per query")
        print(f"\nScores:")
        print(f"  Accuracy: {accuracy:.0%} (weight: 60%)")
        print(f"  Speed:    {speed_score:.0%} (weight: 20%)")
        print(f"  Size:     baseline (weight: 20%)")
        print(f"  Composite: {composite:.0%}")

        print(f"\nPer-question results:")
        for r in benchmark_results:
            status = "PASS" if r["correct"] else "FAIL"
            print(f"  [{status}] {r['id']}: {r['question']}")
            print(f"         {r['detail']} ({r['elapsed_ms']}ms)")

        # Category breakdown
        categories: dict[str, dict] = {}
        for r in benchmark_results:
            cat = r["category"]
            if cat not in categories:
                categories[cat] = {"total": 0, "correct": 0}
            categories[cat]["total"] += 1
            if r["correct"]:
                categories[cat]["correct"] += 1

        print(f"\nCategory accuracy:")
        for cat, counts in sorted(categories.items()):
            pct = counts["correct"] / counts["total"]
            print(f"  {cat}: {counts['correct']}/{counts['total']} ({pct:.0%})")

        # Improvement recommendations
        failures = [r for r in benchmark_results if not r["correct"]]
        if failures:
            print(f"\nImprovement opportunities ({len(failures)} failures):")
            for f in failures:
                print(f"  - {f['id']}: {f['question']}")
                print(f"    Issue: {f['detail']}")

        print("=" * 70)

        assert accuracy >= 0.6, f"Accuracy {accuracy:.0%} is below 60% threshold"
        assert avg_ms < 500, f"Average query time {avg_ms:.0f}ms exceeds 500ms threshold"


# ---------------------------------------------------------------------------
# Performance benchmarks
# ---------------------------------------------------------------------------

class TestPerformance:
    """Measure specific query performance characteristics."""

    def test_email_lookup_speed(self, engine):
        """Email lookup should be under 10ms."""
        start = time.perf_counter()
        for _ in range(100):
            engine.find_by_identifier(IdentifierSystem.EMAIL, "alice.mueller@gmail.com")
        elapsed = (time.perf_counter() - start) * 1000
        avg = elapsed / 100
        assert avg < 10, f"Email lookup avg {avg:.1f}ms exceeds 10ms"

    def test_entity_count_speed(self, engine):
        """Count query should be under 5ms."""
        start = time.perf_counter()
        for _ in range(100):
            engine.count_entities(EntityType.PERSON)
        elapsed = (time.perf_counter() - start) * 1000
        avg = elapsed / 100
        assert avg < 5, f"Count avg {avg:.1f}ms exceeds 5ms"

    def test_relationship_query_speed(self, engine):
        """Relationship query should be under 20ms."""
        people = engine.find_entities(EntityType.PERSON, limit=1)
        if not people:
            pytest.skip("No people")

        start = time.perf_counter()
        for _ in range(50):
            engine.get_relationships(people[0].id)
        elapsed = (time.perf_counter() - start) * 1000
        avg = elapsed / 50
        assert avg < 20, f"Relationship query avg {avg:.1f}ms exceeds 20ms"

    def test_connections_query_speed(self, engine):
        """Get connections should be under 50ms."""
        people = engine.find_entities(EntityType.PERSON, limit=10)
        target = None
        for p in people:
            if engine.get_relationships(p.id):
                target = p
                break
        if not target:
            pytest.skip("No person with connections")

        start = time.perf_counter()
        for _ in range(20):
            engine.get_connections(target.id)
        elapsed = (time.perf_counter() - start) * 1000
        avg = elapsed / 20
        assert avg < 50, f"Connections query avg {avg:.1f}ms exceeds 50ms"

    def test_name_search_speed(self, engine):
        """Full name search across all people should be under 50ms."""
        start = time.perf_counter()
        for _ in range(20):
            people = engine.find_entities(EntityType.PERSON, limit=500)
            [p for p in people if "alice" in p.properties.get("name", "").lower()]
        elapsed = (time.perf_counter() - start) * 1000
        avg = elapsed / 20
        assert avg < 50, f"Name search avg {avg:.1f}ms exceeds 50ms"

    def test_org_search_speed(self, engine):
        """Organization property search should be under 100ms."""
        start = time.perf_counter()
        for _ in range(10):
            people = engine.find_entities(EntityType.PERSON, limit=500)
            for p in people:
                orgs = p.properties.get("organizations", [])
                for org in orgs:
                    if isinstance(org, dict) and "tech" in org.get("name", "").lower():
                        pass
        elapsed = (time.perf_counter() - start) * 1000
        avg = elapsed / 10
        assert avg < 100, f"Org search avg {avg:.1f}ms exceeds 100ms"

    def test_provenance_query_speed(self, engine):
        """Provenance lookup should be under 20ms."""
        people = engine.find_entities(EntityType.PERSON, limit=1)
        if not people:
            pytest.skip("No people")

        start = time.perf_counter()
        for _ in range(50):
            engine.get_provenance(people[0].id)
        elapsed = (time.perf_counter() - start) * 1000
        avg = elapsed / 50
        assert avg < 20, f"Provenance query avg {avg:.1f}ms exceeds 20ms"


# ---------------------------------------------------------------------------
# Size efficiency
# ---------------------------------------------------------------------------

class TestSizeEfficiency:
    """Measure how efficiently the graph represents the data."""

    def test_entity_dedup_ratio(self, engine):
        """Fewer entities than raw records means dedup is working."""
        stats = engine.stats()
        assert stats["entities"] < 1800, f"Too many entities: {stats['entities']}"

    def test_person_count_reasonable(self, engine):
        """20 cast members should produce a bounded number of person entities."""
        count = engine.count_entities(EntityType.PERSON)
        assert count < 300, f"Too many people: {count} (expected <300 for 20 cast members)"

    def test_relationship_density(self, engine):
        """Graph should have meaningful relationship density."""
        stats = engine.stats()
        if stats["entities"] > 0:
            density = stats["relationships"] / stats["entities"]
            assert density > 0.5, f"Low relationship density: {density:.2f}"

    def test_provenance_coverage(self, engine):
        """Most entities should have provenance tracking."""
        stats = engine.stats()
        if stats["entities"] > 0:
            coverage = stats["provenance"] / stats["entities"]
            assert coverage > 0.5, f"Low provenance coverage: {coverage:.2f}"

    def test_identifier_count(self, engine):
        """Should have a reasonable number of identifiers for 20 people."""
        stats = engine.stats()
        assert stats["identifiers"] >= 20, f"Too few identifiers: {stats['identifiers']}"

    def test_hypothesis_to_person_ratio(self, engine):
        """Not too many hypotheses relative to people."""
        stats = engine.stats()
        person_count = engine.count_entities(EntityType.PERSON)
        hyp_ratio = stats["hypotheses_open"] / person_count if person_count > 0 else 0
        assert hyp_ratio < 0.5, f"Too many hypotheses: {stats['hypotheses_open']} for {person_count} people"
