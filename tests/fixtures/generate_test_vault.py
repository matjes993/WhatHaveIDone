"""
Nomolo Test Vault Generator

Generates a realistic, coherent test vault with synthetic data across
all supported sources. The same people appear across Gmail, Contacts,
Calendar, iMessage, WhatsApp, etc. — with intentional overlaps,
name variants, and identity ambiguities for entity resolution testing.

Usage:
    python3 tests/fixtures/generate_test_vault.py [output_dir]

Default output: tests/fixtures/vault/
"""

from __future__ import annotations

import hashlib
import json
import os
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

# ---------------------------------------------------------------------------
# The Cast: a coherent world of people
# ---------------------------------------------------------------------------

# The "user" (Matthias-like persona)
USER = {
    "name": "Matthias Kramer",
    "first": "Matthias",
    "last": "Kramer",
    "emails": ["matjes993@gmail.com", "mail@matthias-kramer.com"],
    "phone": "+491721234567",
    "org": "Nomolo",
    "title": "Founder",
    "city": "Berlin",
    "nicknames": ["Matjes", "matjes993"],
}

# 30 contacts who appear across multiple sources with realistic variations
CAST = [
    {
        "name": "Alice Müller",
        "variants": ["Alice Mueller", "A. Müller", "alice"],
        "emails": ["alice.mueller@gmail.com", "amueller@techcorp.de"],
        "phone": "+491761111111",
        "org": "TechCorp",
        "title": "CTO",
        "city": "Munich",
        "relationship": "friend",
        "appears_in": ["gmail", "contacts", "calendar", "imessage", "whatsapp"],
    },
    {
        "name": "Bob Chen",
        "variants": ["Robert Chen", "B. Chen"],
        "emails": ["bob.chen@example.com", "bchen@startup.io"],
        "phone": "+14155552222",
        "org": "StartupIO",
        "title": "Lead Engineer",
        "city": "San Francisco",
        "relationship": "colleague",
        "appears_in": ["gmail", "contacts", "calendar", "slack"],
    },
    {
        "name": "Clara Hoffmann",
        "variants": ["Clara H.", "Clarita"],
        "emails": ["clara.hoffmann@outlook.com"],
        "phone": "+491733333333",
        "org": "Freelance",
        "title": "Designer",
        "city": "Hamburg",
        "relationship": "friend",
        "appears_in": ["gmail", "contacts", "whatsapp", "imessage"],
    },
    {
        "name": "David Park",
        "variants": ["Dave Park", "D. Park"],
        "emails": ["dpark@bigcorp.com", "david.park.private@gmail.com"],
        "phone": "+821012345678",
        "org": "BigCorp",
        "title": "Product Manager",
        "city": "Seoul",
        "relationship": "colleague",
        "appears_in": ["gmail", "calendar", "slack", "telegram"],
    },
    {
        "name": "Eva Bergström",
        "variants": ["Eva Bergstrom", "Eva B."],
        "emails": ["eva@bergstrom.se"],
        "phone": "+46701234567",
        "org": "Nordic Design AB",
        "title": "CEO",
        "city": "Stockholm",
        "relationship": "business",
        "appears_in": ["gmail", "contacts", "calendar"],
    },
    {
        "name": "Felix Wagner",
        "variants": ["Felix W."],
        "emails": ["felix.wagner@gmail.com"],
        "phone": "+491744444444",
        "org": "",
        "title": "Student",
        "city": "Berlin",
        "relationship": "friend",
        "appears_in": ["whatsapp", "imessage", "contacts"],
    },
    {
        "name": "Grace Kim",
        "variants": ["G. Kim", "Grace K."],
        "emails": ["grace.kim@university.edu", "gracekim99@gmail.com"],
        "phone": "+821087654321",
        "org": "Seoul National University",
        "title": "Professor",
        "city": "Seoul",
        "relationship": "mentor",
        "appears_in": ["gmail", "contacts"],
    },
    {
        "name": "Hans-Peter Schmidt",
        "variants": ["HP Schmidt", "Hans Schmidt", "H.P. Schmidt"],
        "emails": ["hp.schmidt@lawfirm.de"],
        "phone": "+491755555555",
        "org": "Schmidt & Partners",
        "title": "Lawyer",
        "city": "Frankfurt",
        "relationship": "professional",
        "appears_in": ["gmail", "contacts"],
    },
    {
        "name": "Isabelle Dubois",
        "variants": ["Isa", "Isabelle D."],
        "emails": ["isabelle.dubois@company.fr"],
        "phone": "+33612345678",
        "org": "Compagnie Française",
        "title": "Marketing Director",
        "city": "Paris",
        "relationship": "business",
        "appears_in": ["gmail", "calendar"],
    },
    {
        "name": "Javier Rodriguez",
        "variants": ["Javi", "J. Rodriguez"],
        "emails": ["javier@rodriguez.es", "jrodriguez@techco.com"],
        "phone": "+34612345678",
        "org": "TechCo Spain",
        "title": "Backend Developer",
        "city": "Madrid",
        "relationship": "colleague",
        "appears_in": ["gmail", "slack", "telegram"],
    },
    {
        "name": "Keiko Tanaka",
        "variants": ["K. Tanaka"],
        "emails": ["keiko.tanaka@company.jp"],
        "phone": "+81901234567",
        "org": "Tanaka Industries",
        "title": "Director",
        "city": "Tokyo",
        "relationship": "business",
        "appears_in": ["gmail", "contacts"],
    },
    {
        "name": "Lisa Bauer",
        "variants": ["Lisa B.", "L. Bauer"],
        "emails": ["lisa.bauer@gmail.com"],
        "phone": "+491766666666",
        "org": "",
        "title": "",
        "city": "Berlin",
        "relationship": "friend",
        "appears_in": ["whatsapp", "imessage", "contacts", "calendar"],
    },
    {
        "name": "Marco Rossi",
        "variants": ["Marco R."],
        "emails": ["marco.rossi@design.it"],
        "phone": "+393331234567",
        "org": "Studio Rossi",
        "title": "Architect",
        "city": "Milan",
        "relationship": "friend",
        "appears_in": ["gmail", "whatsapp"],
    },
    {
        "name": "Nina Petrov",
        "variants": ["Nina P."],
        "emails": ["nina.petrov@tech.ru", "npetrov@gmail.com"],
        "phone": "+79161234567",
        "org": "Yandex",
        "title": "Data Scientist",
        "city": "Moscow",
        "relationship": "colleague",
        "appears_in": ["gmail", "telegram"],
    },
    {
        "name": "Oliver Thompson",
        "variants": ["Ollie", "O. Thompson"],
        "emails": ["oliver.thompson@company.co.uk"],
        "phone": "+447901234567",
        "org": "London Ventures",
        "title": "Investor",
        "city": "London",
        "relationship": "business",
        "appears_in": ["gmail", "contacts", "calendar"],
    },
    {
        "name": "Patricia Gonzalez",
        "variants": ["Pati", "Patricia G."],
        "emails": ["patricia@gonzalez.mx"],
        "phone": "+5215512345678",
        "org": "Gonzalez Media",
        "title": "Journalist",
        "city": "Mexico City",
        "relationship": "acquaintance",
        "appears_in": ["gmail"],
    },
    {
        "name": "Raj Patel",
        "variants": ["Rajesh Patel", "R. Patel"],
        "emails": ["raj.patel@infosys.in", "rajpatel@gmail.com"],
        "phone": "+919876543210",
        "org": "Infosys",
        "title": "VP Engineering",
        "city": "Bangalore",
        "relationship": "professional",
        "appears_in": ["gmail", "contacts", "calendar", "slack"],
    },
    {
        "name": "Sarah Weber",
        "variants": ["Sarah W."],
        "emails": ["sarah.weber@uni-berlin.de"],
        "phone": "+491777777777",
        "org": "TU Berlin",
        "title": "Research Assistant",
        "city": "Berlin",
        "relationship": "friend",
        "appears_in": ["gmail", "imessage", "whatsapp", "contacts"],
    },
    {
        "name": "Tom Anderson",
        "variants": ["Thomas Anderson", "T. Anderson"],
        "emails": ["tom@anderson.dev", "tanderson@bigtech.com"],
        "phone": "+12025551234",
        "org": "BigTech Inc",
        "title": "Staff Engineer",
        "city": "Seattle",
        "relationship": "colleague",
        "appears_in": ["gmail", "slack", "calendar"],
    },
    {
        "name": "Ursula Fischer",
        "variants": ["Uschi", "U. Fischer"],
        "emails": ["ursula.fischer@family.de"],
        "phone": "+491788888888",
        "org": "",
        "title": "",
        "city": "Düsseldorf",
        "relationship": "family",
        "appears_in": ["contacts", "whatsapp", "imessage"],
    },
]

# Automated senders (newsletters, notifications)
AUTOMATED_SENDERS = [
    ("noreply@github.com", "[GitHub] New pull request"),
    ("notifications@linkedin.com", "You have new connections"),
    ("no-reply@accounts.google.com", "Security alert"),
    ("newsletter@techcrunch.com", "TechCrunch Daily"),
    ("info@meetup.com", "New events near you"),
    ("support@stripe.com", "Your invoice from Stripe"),
    ("noreply@medium.com", "Daily digest"),
]

# Calendar event templates
EVENT_TEMPLATES = [
    {"title": "Team Standup", "duration_min": 30, "recurring": True},
    {"title": "1:1 with {name}", "duration_min": 30, "recurring": True},
    {"title": "Product Review", "duration_min": 60, "recurring": False},
    {"title": "Lunch with {name}", "duration_min": 90, "recurring": False},
    {"title": "Berlin Tech Meetup", "duration_min": 120, "recurring": False},
    {"title": "Flight to {city}", "duration_min": 180, "recurring": False},
    {"title": "Workshop: {topic}", "duration_min": 180, "recurring": False},
    {"title": "Coffee with {name}", "duration_min": 60, "recurring": False},
    {"title": "Board Meeting", "duration_min": 120, "recurring": True},
    {"title": "Dentist Appointment", "duration_min": 60, "recurring": False},
]

# Email subject templates
EMAIL_SUBJECTS = [
    "Re: {topic}",
    "Quick question about {topic}",
    "Follow up: {topic}",
    "{topic} — next steps",
    "Invitation: {event}",
    "FYI: {topic}",
    "Can we chat about {topic}?",
    "Feedback on {topic}",
    "Update: {topic}",
    "Proposal for {topic}",
]

TOPICS = [
    "the Berlin project", "Q1 roadmap", "new design mockups",
    "hiring plan", "investor meeting", "conference talk",
    "API integration", "data migration", "launch timeline",
    "partnership proposal", "budget review", "team offsite",
    "customer feedback", "product strategy", "tech stack decision",
]

WORKSHOP_TOPICS = [
    "AI in Production", "Knowledge Graphs", "Data Privacy",
    "Startup Funding", "Product Design", "Growth Hacking",
]

LOCATIONS = [
    "Zoom", "Google Meet", "Office — Room A", "Office — Room B",
    "WeWork Potsdamer Platz", "Café Einstein", "Restaurant Nobelhart",
    "Factory Berlin", "TU Berlin Campus", "",
]

# Slack channels
SLACK_CHANNELS = [
    "general", "engineering", "product", "random", "design",
    "standup", "deployments", "feedback",
]

# WhatsApp/iMessage conversation topics
CHAT_MESSAGES = [
    "Hey, are you free this weekend?",
    "Just saw the news about {topic}, what do you think?",
    "Running 10 min late, sorry!",
    "Can you send me that link again?",
    "Happy birthday! 🎉",
    "Did you see the email from {name}?",
    "Dinner at 7?",
    "Thanks for the recommendation!",
    "How's the new apartment?",
    "Let's catch up soon",
    "Check out this article: https://example.com/{slug}",
    "Haha that's hilarious 😂",
    "Meeting moved to Thursday",
    "Flight just landed ✈️",
    "Good morning!",
    "Can you pick up milk on the way?",
    "The presentation went really well",
    "Miss you! When are you coming to {city}?",
    "Just finished reading {book}, highly recommend it",
    "Need help with something, can you call me?",
]

BOOKS = [
    "Sapiens", "Clean Code", "The Lean Startup", "Thinking Fast and Slow",
    "Atomic Habits", "Deep Work", "Zero to One",
]

# Browser history domains
BROWSER_DOMAINS = [
    ("github.com", "GitHub", 342),
    ("stackoverflow.com", "Stack Overflow", 287),
    ("news.ycombinator.com", "Hacker News", 156),
    ("docs.python.org", "Python Documentation", 89),
    ("claude.ai", "Claude", 67),
    ("twitter.com", "Twitter", 234),
    ("youtube.com", "YouTube", 445),
    ("gmail.com", "Gmail", 1203),
    ("calendar.google.com", "Google Calendar", 456),
    ("figma.com", "Figma", 78),
    ("notion.so", "Notion", 234),
    ("linear.app", "Linear", 156),
    ("vercel.com", "Vercel", 45),
    ("reddit.com", "Reddit", 189),
    ("medium.com", "Medium", 67),
    ("spotify.com", "Spotify", 312),
    ("amazon.de", "Amazon", 178),
    ("maps.google.com", "Google Maps", 89),
    ("translate.google.com", "Google Translate", 34),
    ("bahn.de", "Deutsche Bahn", 23),
]

# Bookmark folders
BOOKMARKS = [
    ("To Read", "https://arxiv.org/abs/2301.12345", "Attention Is All You Need Revisited"),
    ("To Read", "https://blog.pragmaticengineer.com/data-eng", "The Data Engineering Landscape"),
    ("Dev Tools", "https://docs.astral.sh/ruff/", "Ruff — Python Linter"),
    ("Dev Tools", "https://htmx.org", "htmx — high power tools for HTML"),
    ("Dev Tools", "https://sqlitebrowser.org", "DB Browser for SQLite"),
    ("Recipes", "https://www.chefkoch.de/rezepte/1234/kartoffelsalat.html", "Omas Kartoffelsalat"),
    ("Recipes", "https://www.seriouseats.com/crispy-smashed-potatoes", "Crispy Smashed Potatoes"),
    ("Travel", "https://www.japan-guide.com/e/e2164.html", "Tokyo Travel Guide"),
    ("Travel", "https://wikitravel.org/en/Lisbon", "Lisbon Travel Guide"),
    ("Work", "https://stripe.com/docs/api", "Stripe API Reference"),
    ("Work", "https://platform.openai.com/docs", "OpenAI API Docs"),
    ("Inspiration", "https://dribbble.com/shots/popular", "Dribbble — Popular Shots"),
    ("Inspiration", "https://www.awwwards.com", "Awwwards — Website Design Awards"),
]

NOTES = [
    ("Meeting Notes — Product Review", "Discussed Q1 roadmap. Alice presented new mockups. Decision: postpone feature X to Q2. Action items: Bob to finalize API spec by Friday."),
    ("Book Notes — Sapiens", "Key insight: cognitive revolution enabled humans to cooperate in large numbers through shared myths. Religion, money, nations are all shared fictions."),
    ("Startup Ideas", "1. Personal data vault with gamification\n2. AI-powered recipe generator from fridge photos\n3. Language learning through real conversations with AI"),
    ("Grocery List", "- Milk\n- Bread\n- Avocados\n- Chicken breast\n- Rice\n- Tomatoes\n- Olive oil"),
    ("German Vocabulary", "Wissenschaft = science\nGemütlichkeit = coziness\nSchadenfreude = joy from others' misfortune\nWanderlust = desire to travel"),
    ("Apartment Hunting", "Requirements: 2 rooms min, balcony, near U-Bahn, max 1200€ warm. Check: Immoscout, WG-Gesucht, eBay Kleinanzeigen."),
    ("Workshop Ideas for Berlin Tech Meetup", "- Knowledge graphs for personal data\n- Building local-first apps\n- Privacy-preserving AI\n- The future of personal computing"),
    ("Travel Packing List", "Passport, charger, adapter (UK plug), headphones, book, jacket, umbrella"),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _md5_id(prefix: str, *parts: str) -> str:
    h = hashlib.md5(":".join(parts).encode()).hexdigest()[:12]
    return f"{prefix}:{h}"


def _sha256_id(prefix: str, *parts: str) -> str:
    h = hashlib.sha256(":".join(parts).encode()).hexdigest()[:12]
    return f"{prefix}:{h}"


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def _rfc2822(dt: datetime) -> str:
    return dt.strftime("%a, %d %b %Y %H:%M:%S +0000")


def _random_dt(start_year: int = 2022, end_year: int = 2025) -> datetime:
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


def _write_jsonl(path: Path, entries: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        for entry in entries:
            f.write(json.dumps(entry, default=str) + "\n")


def _write_ids(path: Path, ids: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        for id_ in ids:
            f.write(id_ + "\n")


# ---------------------------------------------------------------------------
# Gmail Generator
# ---------------------------------------------------------------------------

def generate_gmail(vault: Path) -> list[str]:
    entries_by_month: dict[str, list[dict]] = {}
    all_ids = []
    thread_counter = 0

    # Real conversations with cast members
    gmail_cast = [c for c in CAST if "gmail" in c["appears_in"]]

    for i in range(500):
        dt = _random_dt()
        month_key = f"{dt.year}/{dt.month:02d}_{dt.strftime('%B')}"

        if i < 450:
            # Human emails
            if random.random() < 0.6:
                # Incoming
                person = random.choice(gmail_cast)
                from_email = random.choice(person["emails"])
                from_name = random.choice([person["name"]] + person["variants"][:1])
                from_field = f"{from_name} <{from_email}>"
                to_field = random.choice(USER["emails"])
                recipients = [{"name": USER["name"], "email": to_field}]
            else:
                # Outgoing
                person = random.choice(gmail_cast)
                from_email = USER["emails"][0]
                from_field = f"{USER['name']} <{from_email}>"
                to_email = random.choice(person["emails"])
                to_field = f"{person['name']} <{to_email}>"
                recipients = [{"name": person["name"], "email": to_email}]

            topic = random.choice(TOPICS)
            subject_tpl = random.choice(EMAIL_SUBJECTS)
            subject = subject_tpl.format(
                topic=topic,
                event=random.choice(EVENT_TEMPLATES)["title"].format(
                    name=person["name"].split()[0], city=person["city"],
                    topic=random.choice(WORKSHOP_TOPICS),
                ),
            )

            # Generate realistic email body
            greeting = random.choice(["Hi", "Hey", "Hello", f"Hi {USER['first']}" if random.random() < 0.5 else f"Hey {person['name'].split()[0]}"])
            body_lines = [
                f"{greeting},",
                "",
                fake.paragraph(nb_sentences=random.randint(2, 5)),
            ]
            if random.random() < 0.3:
                body_lines.append("")
                body_lines.append(fake.paragraph(nb_sentences=random.randint(1, 3)))

            # Signature
            if random.random() < 0.4:
                sig_person = person if random.random() < 0.6 else {"name": USER["name"], "org": USER["org"], "title": USER["title"]}
                body_lines.extend([
                    "",
                    "Best,",
                    sig_person["name"],
                    f"{sig_person['title']} | {sig_person['org']}" if sig_person.get("org") else "",
                ])

            body = "\n".join(body_lines)

            thread_counter += 1
            thread_id = f"thread_{thread_counter:05d}"

            # Entities
            entities = {"urls": [], "emails_mentioned": [], "amounts": [], "phone_numbers": []}
            if random.random() < 0.2:
                entities["urls"].append(f"https://example.com/{fake.slug()}")
            if random.random() < 0.1:
                entities["amounts"].append(f"${random.randint(50, 5000)}.00")

            msg_id = f"msg_{i:06d}"
            entry = {
                "id": msg_id,
                "threadId": thread_id,
                "internalDate": str(int(dt.timestamp() * 1000)),
                "sizeEstimate": random.randint(2000, 50000),
                "date": _rfc2822(dt),
                "subject": subject,
                "from": from_field,
                "to": to_field,
                "cc": "",
                "bcc": "",
                "reply_to": "",
                "message_id": f"<{msg_id}@mail.gmail.com>",
                "in_reply_to": "",
                "references": "",
                "list_unsubscribe": "",
                "tags": random.choice([
                    ["INBOX"], ["INBOX", "IMPORTANT"], ["SENT"],
                    ["INBOX", "STARRED"], ["INBOX", "CATEGORY_PERSONAL"],
                ]),
                "attachments": [],
                "body_raw": body,
                # Cleaner-added fields
                "body_clean": body.split("\nBest,")[0].strip() if "Best," in body else body,
                "body_for_embedding": f"From {from_field} to {to_field} on {_rfc2822(dt)} re: {subject}: {body[:500]}",
                "from_name": from_name if "from_name" in dir() else "",
                "from_email": from_email.lower(),
                "to_list": recipients,
                "cc_list": [],
                "year": dt.year,
                "month": dt.month,
                "thread_depth": random.randint(1, 5),
                "thread_position": 1,
                "is_automated": False,
                "has_attachments": False,
                "attachment_names": [],
                "entities": entities,
                "word_count": len(body.split()),
                "lang": "en",
            }
        else:
            # Automated emails
            sender_email, subject = random.choice(AUTOMATED_SENDERS)
            from_field = sender_email
            msg_id = f"msg_{i:06d}"
            body = fake.paragraph(nb_sentences=3)
            entry = {
                "id": msg_id,
                "threadId": f"thread_{thread_counter + i:05d}",
                "internalDate": str(int(dt.timestamp() * 1000)),
                "sizeEstimate": random.randint(5000, 100000),
                "date": _rfc2822(dt),
                "subject": subject,
                "from": sender_email,
                "to": USER["emails"][0],
                "cc": "",
                "bcc": "",
                "reply_to": "",
                "message_id": f"<{msg_id}@automated.example.com>",
                "in_reply_to": "",
                "references": "",
                "list_unsubscribe": f"<mailto:unsub@{sender_email.split('@')[1]}>",
                "tags": ["INBOX", "CATEGORY_UPDATES"],
                "attachments": [],
                "body_raw": body,
                "body_clean": body,
                "body_for_embedding": f"From {sender_email} on {_rfc2822(dt)} re: {subject}: {body}",
                "from_name": "",
                "from_email": sender_email.lower(),
                "to_list": [{"name": USER["name"], "email": USER["emails"][0]}],
                "cc_list": [],
                "year": dt.year,
                "month": dt.month,
                "thread_depth": 1,
                "thread_position": 1,
                "is_automated": True,
                "has_attachments": False,
                "attachment_names": [],
                "entities": {"urls": [], "emails_mentioned": [], "amounts": [], "phone_numbers": []},
                "word_count": len(body.split()),
                "lang": "en",
            }

        entries_by_month.setdefault(month_key, []).append(entry)
        all_ids.append(msg_id)

    # Write files organized by month
    gmail_dir = vault / "Gmail_Primary"
    for month_key, entries in entries_by_month.items():
        _write_jsonl(gmail_dir / f"{month_key}.jsonl", entries)
    _write_ids(gmail_dir / "processed_ids.txt", all_ids)
    return all_ids


# ---------------------------------------------------------------------------
# Google Contacts Generator
# ---------------------------------------------------------------------------

def generate_contacts(vault: Path) -> list[str]:
    entries = []
    all_ids = []

    contacts_cast = [c for c in CAST if "contacts" in c["appears_in"]]

    for person in contacts_cast:
        contact_id = f"contacts:google:people/c{hashlib.md5(person['name'].encode()).hexdigest()[:16]}"

        name_parts = person["name"].split()
        given = name_parts[0]
        family = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""

        orgs = []
        if person["org"]:
            orgs.append({
                "name": person["org"],
                "title": person["title"],
                "department": "",
                "type": "work",
                "job_description": "",
                "start_date": "",
                "end_date": "",
                "current": True,
                "primary": True,
            })

        entry = {
            "id": contact_id,
            "sources": ["google"],
            "source_id": contact_id.replace("contacts:google:", ""),
            "name": {
                "display": person["name"],
                "given": given,
                "family": family,
                "middle": "",
                "prefix": "",
                "suffix": "",
                "phonetic_given": "",
                "phonetic_family": "",
            },
            "nicknames": [{"value": v, "type": "DEFAULT"} for v in person.get("variants", [])[:1]],
            "emails": [
                {"value": e.lower(), "type": "home" if i == 0 else "work", "display_name": "", "primary": i == 0}
                for i, e in enumerate(person["emails"])
            ],
            "phones": [
                {"value": person["phone"], "type": "mobile", "canonical": person["phone"].replace(" ", ""), "primary": True}
            ] if person.get("phone") else [],
            "addresses": [
                {
                    "type": "home",
                    "formatted": f"{fake.street_address()}, {person['city']}",
                    "street": fake.street_address(),
                    "city": person["city"],
                    "region": "",
                    "postal_code": fake.postcode(),
                    "country": "",
                    "country_code": "",
                    "primary": True,
                }
            ] if random.random() < 0.4 else [],
            "organizations": orgs,
            "occupations": [],
            "skills": [],
            "interests": [],
            "urls": [],
            "external_ids": [],
            "im_clients": [],
            "relations": [{"person": USER["name"], "type": person["relationship"], "formatted_type": person["relationship"]}]
                if random.random() < 0.2 else [],
            "birthdays": [{"year": random.randint(1975, 1998), "month": random.randint(1, 12), "day": random.randint(1, 28), "text": ""}]
                if random.random() < 0.5 else [],
            "events": [],
            "biographies": [{"value": fake.sentence(), "content_type": "TEXT_PLAIN"}]
                if random.random() < 0.2 else [],
            "photos": [{"url": f"https://lh3.googleusercontent.com/a/{hashlib.md5(person['name'].encode()).hexdigest()[:20]}", "default": True}]
                if random.random() < 0.7 else [],
            "genders": [],
            "age_ranges": [],
            "locales": [],
            "locations": [],
            "memberships": [],
            "user_defined": [],
            "client_data": [],
            "misc_keywords": [],
            "sip_addresses": [],
            "calendar_urls": [],
            "updated_at": _iso(_random_dt(2023, 2025)),
            "contact_for_embedding": f"{person['name']}, {person['title']} at {person['org']}, {person['city']}. Email: {person['emails'][0]}",
        }

        entries.append(entry)
        all_ids.append(contact_id)

    contacts_dir = vault / "Contacts"
    _write_jsonl(contacts_dir / "contacts.jsonl", entries)
    _write_ids(contacts_dir / "processed_ids.txt", all_ids)
    return all_ids


# ---------------------------------------------------------------------------
# Calendar Generator
# ---------------------------------------------------------------------------

def generate_calendar(vault: Path) -> list[str]:
    entries = []
    all_ids = []

    calendar_cast = [c for c in CAST if "calendar" in c["appears_in"]]

    for i in range(100):
        dt = _random_dt(2023, 2025)
        template = random.choice(EVENT_TEMPLATES)
        person = random.choice(calendar_cast) if "{name}" in template["title"] or "{city}" in template["title"] else None

        title = template["title"].format(
            name=person["name"].split()[0] if person else "",
            city=person["city"] if person else random.choice(["London", "Paris", "Tokyo"]),
            topic=random.choice(WORKSHOP_TOPICS),
        )

        start = dt.replace(hour=random.choice([9, 10, 11, 13, 14, 15, 16]), minute=random.choice([0, 30]), second=0)
        end = start + timedelta(minutes=template["duration_min"])

        attendees = []
        if person:
            attendees.append({
                "email": random.choice(person["emails"]),
                "name": person["name"],
                "status": random.choice(["accepted", "tentative", "needsAction"]),
            })
        if random.random() < 0.3 and len(calendar_cast) > 1:
            extra = random.choice([c for c in calendar_cast if c != person])
            attendees.append({
                "email": random.choice(extra["emails"]),
                "name": extra["name"],
                "status": "accepted",
            })

        event_id = f"calendar:google:{hashlib.sha256(f'{title}:{_iso(start)}'.encode()).hexdigest()[:12]}"

        entry = {
            "id": event_id,
            "sources": ["google-calendar"],
            "title": title,
            "description": fake.sentence() if random.random() < 0.3 else "",
            "location": random.choice(LOCATIONS),
            "start": _iso(start),
            "end": _iso(end),
            "all_day": False,
            "year": start.year,
            "month": start.month,
            "attendees": attendees,
            "organizer": USER["emails"][0],
            "status": "confirmed",
            "recurring": template["recurring"],
            "url": f"https://calendar.google.com/event?id={event_id}",
            "updated_at": _iso(dt),
            "event_for_embedding": (
                f"Event {title} — on {_iso(start)} at {random.choice(LOCATIONS)} — "
                f"Attendees: {', '.join(a['name'] for a in attendees)}"
            ),
        }

        entries.append(entry)
        all_ids.append(event_id)

    cal_dir = vault / "Calendar"
    _write_jsonl(cal_dir / "calendar.jsonl", entries)
    _write_ids(cal_dir / "processed_ids.txt", all_ids)
    return all_ids


# ---------------------------------------------------------------------------
# iMessage Generator
# ---------------------------------------------------------------------------

def generate_imessage(vault: Path) -> list[str]:
    entries = []
    all_ids = []

    imessage_cast = [c for c in CAST if "imessage" in c["appears_in"]]

    for i in range(200):
        person = random.choice(imessage_cast)
        dt = _random_dt()
        is_from_me = random.random() < 0.45

        msg_template = random.choice(CHAT_MESSAGES)
        text = msg_template.format(
            topic=random.choice(TOPICS),
            name=random.choice(imessage_cast)["name"].split()[0],
            city=person["city"],
            slug=fake.slug(),
            book=random.choice(BOOKS),
        )

        contact = random.choice([person["phone"], person["name"], person["emails"][0]])

        msg_id = _md5_id("local:imessage", str(i))

        entry = {
            "id": msg_id,
            "sources": ["mac_imessage"],
            "type": "message",
            "text": text,
            "contact": contact,
            "is_from_me": is_from_me,
            "service": random.choice(["iMessage", "iMessage", "iMessage", "SMS"]),
            "date": _iso(dt),
            "updated_at": _iso(dt),
            "message_for_embedding": f"{'Sent to' if is_from_me else 'From'} {contact}: {text}",
        }

        entries.append(entry)
        all_ids.append(msg_id)

    msg_dir = vault / "Messages"
    _write_jsonl(msg_dir / "imessage.jsonl", entries)
    _write_ids(msg_dir / "processed_ids.txt", all_ids)
    return all_ids


# ---------------------------------------------------------------------------
# WhatsApp Generator
# ---------------------------------------------------------------------------

def generate_whatsapp(vault: Path) -> list[str]:
    all_ids = []

    whatsapp_cast = [c for c in CAST if "whatsapp" in c["appears_in"]]

    for person in whatsapp_cast:
        entries = []
        # 20-40 messages per chat
        n_messages = random.randint(20, 40)
        base_dt = _random_dt(2023, 2025)

        for j in range(n_messages):
            dt = base_dt + timedelta(minutes=random.randint(1, 60) * j)
            is_user = random.random() < 0.45
            sender = USER["name"] if is_user else person["name"]

            msg_template = random.choice(CHAT_MESSAGES)
            text = msg_template.format(
                topic=random.choice(TOPICS),
                name=random.choice(whatsapp_cast)["name"].split()[0],
                city=person["city"],
                slug=fake.slug(),
                book=random.choice(BOOKS),
            )

            parts = f"{person['name']}:{_iso(dt)}:{sender}:{text[:50]}"
            msg_id = _sha256_id("whatsapp", parts)

            entry = {
                "id": msg_id,
                "date": _iso(dt),
                "sender": sender,
                "body": text,
                "body_for_embedding": f"{sender}: {text}",
                "chat": person["name"],
                "type": "message",
            }

            entries.append(entry)
            all_ids.append(msg_id)

        chat_name = person["name"].replace(" ", "_")
        wa_dir = vault / "WhatsApp"
        _write_jsonl(wa_dir / f"{person['name']}.jsonl", entries)

    _write_ids(vault / "WhatsApp" / "processed_ids.txt", all_ids)
    return all_ids


# ---------------------------------------------------------------------------
# Telegram Generator
# ---------------------------------------------------------------------------

def generate_telegram(vault: Path) -> list[str]:
    entries = []
    all_ids = []

    telegram_cast = [c for c in CAST if "telegram" in c["appears_in"]]

    for i in range(80):
        person = random.choice(telegram_cast)
        dt = _random_dt()
        is_user = random.random() < 0.45
        sender = USER["name"] if is_user else person["name"]

        msg_template = random.choice(CHAT_MESSAGES)
        text = msg_template.format(
            topic=random.choice(TOPICS),
            name=random.choice(telegram_cast)["name"].split()[0],
            city=person["city"],
            slug=fake.slug(),
            book=random.choice(BOOKS),
        )

        msg_id = _sha256_id("telegram", f"{person['name']}:{i}")

        entry = {
            "id": msg_id,
            "date": _iso(dt),
            "sender": sender,
            "body": text,
            "body_for_embedding": f"{sender} in {person['name']}: {text}",
            "chat": person["name"],
            "chat_type": "personal_chat",
            "type": "message",
        }

        entries.append(entry)
        all_ids.append(msg_id)

    tg_dir = vault / "Telegram"
    _write_jsonl(tg_dir / "messages.jsonl", entries)
    _write_ids(tg_dir / "processed_ids.txt", all_ids)
    return all_ids


# ---------------------------------------------------------------------------
# Slack Generator
# ---------------------------------------------------------------------------

def generate_slack(vault: Path) -> list[str]:
    entries = []
    all_ids = []

    slack_cast = [c for c in CAST if "slack" in c["appears_in"]]

    for i in range(120):
        person = random.choice(slack_cast)
        dt = _random_dt()
        channel = random.choice(SLACK_CHANNELS)
        is_user = random.random() < 0.4
        sender = USER["name"] if is_user else person["name"]

        # More work-related messages for Slack
        text = random.choice([
            f"Pushed the fix for {random.choice(TOPICS)} to main",
            f"@{random.choice(slack_cast)['name'].split()[0].lower()} can you review the PR?",
            "Standup: yesterday I worked on the API. Today: tests. No blockers.",
            f"Deploy to staging done :rocket:",
            f"FYI: {random.choice(TOPICS)} meeting moved to 3pm",
            f"Found a bug in the {random.choice(['auth', 'search', 'API', 'UI'])} module",
            fake.sentence(),
            f"LGTM :+1:",
            f"Interesting article: https://example.com/{fake.slug()}",
            f"Who's joining for lunch?",
        ])

        msg_id = _sha256_id("slack", f"{channel}:{dt.timestamp()}")

        entry = {
            "id": msg_id,
            "date": _iso(dt),
            "sender": sender,
            "channel": channel,
            "body": text,
            "body_for_embedding": f"{sender} in #{channel}: {text}",
            "type": "message",
        }

        if random.random() < 0.1:
            entry["reactions"] = [{"name": random.choice(["thumbsup", "rocket", "eyes", "heart"]), "count": random.randint(1, 5)}]

        entries.append(entry)
        all_ids.append(msg_id)

    slack_dir = vault / "Slack"
    _write_jsonl(slack_dir / "messages.jsonl", entries)
    _write_ids(slack_dir / "processed_ids.txt", all_ids)
    return all_ids


# ---------------------------------------------------------------------------
# Browser History Generator
# ---------------------------------------------------------------------------

def generate_browser_history(vault: Path) -> list[str]:
    entries = []
    all_ids = []

    for domain, title, visit_count in BROWSER_DOMAINS:
        for i in range(random.randint(3, 15)):
            dt = _random_dt()
            path = f"/{fake.slug()}" if random.random() < 0.5 else ""
            url = f"https://{domain}{path}"
            page_title = f"{title} — {fake.sentence(nb_words=4)}" if path else title

            entry_id = _md5_id("local:safari", f"{url}:{i}")

            entry = {
                "id": entry_id,
                "sources": ["safari"],
                "type": "browse",
                "url": url,
                "title": page_title,
                "domain": domain,
                "visit_count": visit_count + random.randint(0, 50),
                "last_visit": _iso(dt),
                "updated_at": _iso(dt),
                "browse_for_embedding": f"Visited {page_title} at {url}",
            }

            entries.append(entry)
            all_ids.append(entry_id)

    browser_dir = vault / "Browser"
    _write_jsonl(browser_dir / "safari_history.jsonl", entries)
    _write_ids(browser_dir / "processed_ids.txt", all_ids)
    return all_ids


# ---------------------------------------------------------------------------
# Bookmarks Generator
# ---------------------------------------------------------------------------

def generate_bookmarks(vault: Path) -> list[str]:
    entries = []
    all_ids = []

    for folder, url, name in BOOKMARKS:
        dt = _random_dt(2021, 2025)
        domain = url.split("/")[2]
        entry_id = _md5_id("local:chrome_bm", url)

        entry = {
            "id": entry_id,
            "sources": ["chrome_bookmarks"],
            "type": "bookmark",
            "name": name,
            "url": url,
            "domain": domain,
            "folder": folder,
            "date_added": _iso(dt),
            "updated_at": _iso(dt),
            "bookmark_for_embedding": f"Bookmark: {name} — {url} in folder {folder}",
        }

        entries.append(entry)
        all_ids.append(entry_id)

    bm_dir = vault / "Bookmarks"
    _write_jsonl(bm_dir / "chrome_bookmarks.jsonl", entries)
    _write_ids(bm_dir / "processed_ids.txt", all_ids)
    return all_ids


# ---------------------------------------------------------------------------
# Notes Generator
# ---------------------------------------------------------------------------

def generate_notes(vault: Path) -> list[str]:
    entries = []
    all_ids = []

    for i, (title, body) in enumerate(NOTES):
        dt = _random_dt()
        note_id = _md5_id("local:notes", str(i))

        entry = {
            "id": note_id,
            "sources": ["mac_notes"],
            "type": "note",
            "title": title,
            "snippet": body[:500],
            "folder": random.choice(["", "Personal", "Work", "Ideas"]),
            "created_at": _iso(dt - timedelta(days=random.randint(0, 30))),
            "modified_at": _iso(dt),
            "updated_at": _iso(dt),
            "note_for_embedding": f"{title}: {body}",
        }

        entries.append(entry)
        all_ids.append(note_id)

    notes_dir = vault / "Notes"
    _write_jsonl(notes_dir / "notes.jsonl", entries)
    _write_ids(notes_dir / "processed_ids.txt", all_ids)
    return all_ids


# ---------------------------------------------------------------------------
# Local Contacts (Mac) Generator
# ---------------------------------------------------------------------------

def generate_local_contacts(vault: Path) -> list[str]:
    """Generate Mac-native contacts (some overlap with Google Contacts)."""
    entries = []
    all_ids = []

    # Include some cast members with slightly different data (simulates Mac vs Google sync)
    local_contacts_cast = [c for c in CAST if "contacts" in c["appears_in"]]

    for person in local_contacts_cast:
        name_parts = person["name"].split()
        first = name_parts[0]
        last = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""

        # Use a name variant sometimes (simulates different contact cards)
        display_name = random.choice([person["name"]] + person["variants"][:1]) if random.random() < 0.3 else person["name"]

        dt = _random_dt(2020, 2025)
        contact_id = _md5_id("local:contacts", f"{first}:{last}")

        entry = {
            "id": contact_id,
            "sources": ["mac_contacts"],
            "type": "contact",
            "name": display_name,
            "first_name": first,
            "last_name": last,
            "organization": person["org"],
            "job_title": person["title"],
            "birthday": f"{random.randint(1975, 1998)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}" if random.random() < 0.4 else None,
            "created_at": _iso(dt - timedelta(days=random.randint(100, 1000))),
            "modified_at": _iso(dt),
            "updated_at": _iso(dt),
            "contact_for_embedding": f"{display_name} at {person['org']}, {person['title']}" if person["org"] else display_name,
        }

        entries.append(entry)
        all_ids.append(contact_id)

    # Write to same Contacts dir (different file than Google contacts)
    contacts_dir = vault / "Contacts"
    _write_jsonl(contacts_dir / "mac_contacts.jsonl", entries)
    # Append IDs to existing processed_ids
    ids_file = contacts_dir / "processed_ids.txt"
    with open(ids_file, "a") as f:
        for id_ in all_ids:
            f.write(id_ + "\n")
    return all_ids


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def generate_test_vault(output_dir: str | Path | None = None) -> Path:
    """Generate a complete test vault with coherent synthetic data."""
    if output_dir is None:
        output_dir = Path(__file__).parent / "vault"
    else:
        output_dir = Path(output_dir)

    # Clean existing
    if output_dir.exists():
        import shutil
        shutil.rmtree(output_dir)

    output_dir.mkdir(parents=True, exist_ok=True)

    print("Generating test vault...")

    gmail_ids = generate_gmail(output_dir)
    print(f"  Gmail: {len(gmail_ids)} emails")

    contact_ids = generate_contacts(output_dir)
    print(f"  Google Contacts: {len(contact_ids)} contacts")

    local_contact_ids = generate_local_contacts(output_dir)
    print(f"  Mac Contacts: {len(local_contact_ids)} contacts")

    cal_ids = generate_calendar(output_dir)
    print(f"  Calendar: {len(cal_ids)} events")

    imsg_ids = generate_imessage(output_dir)
    print(f"  iMessage: {len(imsg_ids)} messages")

    wa_ids = generate_whatsapp(output_dir)
    print(f"  WhatsApp: {len(wa_ids)} messages")

    tg_ids = generate_telegram(output_dir)
    print(f"  Telegram: {len(tg_ids)} messages")

    slack_ids = generate_slack(output_dir)
    print(f"  Slack: {len(slack_ids)} messages")

    browser_ids = generate_browser_history(output_dir)
    print(f"  Browser History: {len(browser_ids)} entries")

    bm_ids = generate_bookmarks(output_dir)
    print(f"  Bookmarks: {len(bm_ids)} bookmarks")

    note_ids = generate_notes(output_dir)
    print(f"  Notes: {len(note_ids)} notes")

    total = (len(gmail_ids) + len(contact_ids) + len(local_contact_ids) +
             len(cal_ids) + len(imsg_ids) + len(wa_ids) + len(tg_ids) +
             len(slack_ids) + len(browser_ids) + len(bm_ids) + len(note_ids))

    print(f"\nTotal: {total} records across 11 sources")
    print(f"Cast: {len(CAST)} people with cross-source identities")
    print(f"Output: {output_dir}")

    # Write manifest
    manifest = {
        "generated_at": _iso(datetime.utcnow()),
        "generator_version": "1.0.0",
        "seed": 42,
        "total_records": total,
        "sources": {
            "gmail": len(gmail_ids),
            "google_contacts": len(contact_ids),
            "mac_contacts": len(local_contact_ids),
            "calendar": len(cal_ids),
            "imessage": len(imsg_ids),
            "whatsapp": len(wa_ids),
            "telegram": len(tg_ids),
            "slack": len(slack_ids),
            "browser_history": len(browser_ids),
            "bookmarks": len(bm_ids),
            "notes": len(note_ids),
        },
        "cast_size": len(CAST),
        "user": USER["name"],
        "entity_resolution_challenges": [
            "Alice Müller appears as 'Alice Mueller' in Mac contacts and 'A. Müller' in some emails",
            "Bob Chen uses 'Robert Chen' in formal contexts and 'B. Chen' in signatures",
            "Hans-Peter Schmidt appears as 'HP Schmidt', 'Hans Schmidt', and 'H.P. Schmidt'",
            "David Park uses two different email addresses across Gmail and Slack",
            "Raj Patel also appears as 'Rajesh Patel' in some contacts",
            "Tom Anderson uses 'Thomas Anderson' in formal settings",
            "Same people appear in both Google Contacts and Mac Contacts with slight differences",
            "Ursula Fischer uses nickname 'Uschi' in WhatsApp",
        ],
    }
    with open(output_dir / "manifest.json", "w") as f:
        json.dump(manifest, f, indent=2)

    return output_dir


if __name__ == "__main__":
    output = sys.argv[1] if len(sys.argv) > 1 else None
    generate_test_vault(output)
