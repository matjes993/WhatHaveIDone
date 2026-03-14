"""
THE FLATCLOUD — Nomolo's RPG Layer

"The Flatcloud sits on the backs of four enormous servers, which themselves
rest on the shell of the Great Algorithm — a turtle-like entity that nobody
has ever seen but everyone agrees is definitely optimizing something."

This module defines the satirical RPG world that frames the Nomolo experience.
SaaS companies become Conglomerates (villains), extracted data becomes loot,
and the user's character grows as they reclaim their digital sovereignty.

Inspired by Terry Pratchett's Discworld — treating the absurdity of the
digital age with the seriousness it doesn't deserve.
"""

import math
import os
import json
from datetime import datetime

# ---------------------------------------------------------------------------
# JARGON MAP — RPG terms to Real-world terms
# ---------------------------------------------------------------------------

JARGON_MAP = {
    # --- Data types (loot) ---
    "Scroll": "Email",
    "Scrolls": "Emails",
    "Soul Bond": "Contact",
    "Soul Bonds": "Contacts",
    "Time Crystal": "Calendar Event",
    "Time Crystals": "Calendar Events",
    "Tome": "Book",
    "Tomes": "Books",
    "Memory Shard": "Photo/Video",
    "Memory Shards": "Photos/Videos",
    "Echo": "Music Track",
    "Echoes": "Music Tracks",
    "Coin": "Financial Record",
    "Coins": "Financial Records",
    "Gold Coin": "Financial Record",
    "Gold Coins": "Financial Records",
    "Marketplace Receipt": "Shopping Record",
    "Marketplace Receipts": "Shopping Records",
    "Whisper": "Chat Message",
    "Whispers": "Chat Messages",
    "Manuscript": "Note",
    "Manuscripts": "Notes",
    "Whisper Page": "Note",
    "Whisper Pages": "Notes",
    "Vision": "Video",
    "Visions": "Videos",
    "Oracle Recording": "Podcast",
    "Oracle Recordings": "Podcasts",
    "Life Force": "Health Data",
    "Life Essence": "Health Data",
    "Footprint": "Location/Browser Data",
    "Footprints": "Location/Browser Data",
    "Waypoint": "Location",
    "Waypoints": "Locations",
    "Star Chart": "Bookmark",
    "Star Charts": "Bookmarks",
    "Proclamation": "Social Post",
    "Proclamations": "Social Posts",
    "Shadow Message": "Chat Message",
    "Shadow Messages": "Chat Messages",

    # --- UI elements ---
    "SCUMM Bar": "Dashboard",
    "The SCUMM Bar": "The Dashboard",
    "Loot Log": "Records",
    "Life Map": "Data Map",
    "Raid Targets": "Sources",
    "Many Faces": "Identities",
    "Ship's Helm": "Settings",
    "Captain's Quarters": "Profile",
    "Loot Inventory": "Data Inventory",
    "Treasure Hold": "Data Storage",
    "Raiding Orders": "Collection",
    "Here Be Dragons": "Danger Zone",
    "Memory Tavern": "Data Quiz",
    "The Armada": "Your Sources",
    "Plundered Islands": "Connected",
    "Uncharted Waters": "Available",

    # --- Actions ---
    "Raid": "Import",
    "raid": "import",
    "Raid All": "Collect All",
    "Raided": "Imported",
    "raided": "imported",
    "Plunder": "Download",
    "plunder": "download",
    "Board their ship": "Connect",
    "Cast off": "Disconnect",
    "Scan the horizon": "Refresh",
    "Scan the Horizon": "Refresh",
    "Stash": "Save",
    "Scuttle": "Delete",
    "Patch the hull": "Update",
    "Load the cannons": "Upload",
    "Search the seas": "Search",
    "Search the Seas": "Search",
    "Chart the course": "Navigate",
    "Click to plunder": "Click to collect",
    "Begin the Raid": "Start Collection",

    # --- Nouns / concepts ---
    "Loot": "Records",
    "loot": "records",
    "Booty": "Total",
    "Plunder": "Data",
    "Treasure": "Files",
    "pieces of loot": "records",
    "pieces of plunder": "records",
    "Vault": "Archive",
    "vault": "archive",
    "Armada": "Company",
    "Armada fleet": "Company",
    "Armada fleets": "Companies",
    "The Flatcloud": "The Cloud",
    "Flatcloud": "Cloud",
    "Reclaimer": "User",
    "Seven Seas of Data": "The Internet",
    "Island of Nomolo": "Digital Sovereignty",
    "Map Fragment": "Data Source",
    "The One": "Personal AI",
    "Letter of Marque": "Google Credentials",
    "Total Booty": "Total Records",
    "Islands Plundered": "Connected Sources",
    "Local Harbors": "Mac Sources",
    "Captured Cargo": "Import Files",

    # --- Entities (Conglomerates) ---
    "The Omniscient Eye": "Google",
    "The Walled Garden": "Apple",
    "The Hydra of Faces": "Meta",
    "The Melody Merchant": "Spotify",
    "The Bazaar Eternal": "Amazon",
    "The Professional Masque": "LinkedIn",
    "The Shadow Courier": "Telegram",
    "The Corporate Hive": "Slack",
    "The Chaos Herald": "X / Twitter",
    "The Dream Weaver": "Netflix",
    "The Hive Mind": "Reddit",
    "The Coin Master": "PayPal",
    "The Merchant Fleet": "Amazon",
    "The Professional Port": "Microsoft",
    "The Bard's Guild": "Spotify/YouTube",
    "The Shadow Broker": "Telegram/Signal",
    "The Coin Counter": "PayPal",

    # --- Captain names ---
    "Captain Lexicon": "Google (Captain)",
    "Admiral Polished": "Apple (Admiral)",
    "Captain Pivot": "Meta (Captain)",
    "Commodore Prime": "Amazon (Commodore)",
    "The Harbormaster": "Microsoft/LinkedIn",
    "The Maestro": "Spotify/YouTube",
    "Baron Ledger": "PayPal",

    # --- Pirate-world locations & things ---
    "the Omniscient Archipelago": "Google's platform",
    "the Archipelago": "Google's ecosystem",
    "the Fortress": "Apple's ecosystem",
    "the Fortress Marketplace": "the App Store",
    "Fortress Marketplace": "App Store",
    "the Reef": "Facebook/Meta's platform",
    "the Scroll Archives": "Gmail",
    "Scroll Archives": "Gmail",
    "the Listening Parrot": "Alexa",
    "Listening Parrot": "Alexa",
    "the Great Logbook Scandal": "the Cambridge Analytica scandal",
    "Great Logbook Scandal": "Cambridge Analytica scandal",
    "the Glass Panes": "Windows",
    "Glass Panes": "Windows",
    "the Spyglass": "Chrome / Google Analytics",
    "Spyglass": "Chrome / Analytics",
    "the Bard's Stage": "YouTube",
    "Bard's Stage": "YouTube",
    "the Hydra's Whisper Channel": "WhatsApp",
    "Hydra's Whisper Channel": "WhatsApp",
    "the Scuttled Ships Registry": "killedbygoogle.com",
    "Scuttled Ships Registry": "killedbygoogle.com",
    "the Pirate's Code": "GDPR",
    "Pirate's Code": "GDPR",
    "the Data Protection Treaty": "GDPR",
    "Secret Dispatches": "Secret Chats (Telegram)",
    "harbor dispatches": "InMails",
    "harbor messages": "InMails",

    # --- States ---
    "Aboard": "Connected",
    "Adrift": "Disconnected",
    "Battle-ready": "Active",
    "In dry dock": "Inactive",
    "Defeated": "Fully Imported",
    "Uncharted": "Available",

    # --- Fun phrases used in toasts / UI ---
    "Yo ho ho!": "Success!",
    "Kraken attack!": "Error!",
    "Man overboard!": "Warning!",
    "Batten down!": "Close",
    "Aye!": "OK",
    "Belay that!": "Cancel",
    "Yer": "Your",
    "yer": "your",
    "ye": "you",
    "Captain": "User",

    # --- Loading / empty / error phrases ---
    "Polishing the brass at the SCUMM Bar...": "Loading dashboard...",
    "Unfurling the treasure maps...": "Loading records...",
    "Scanning the horizon with the spyglass...": "Loading sources...",
    "Adjusting the ship's wheel...": "Loading settings...",
    "Sending the parrot to look...": "Searching...",
    "The hold is empty, Captain. Time to raid the Armada!": "No records yet. Time to import from some companies!",
    "No islands on the chart yet. The seven seas await!": "No sources connected yet.",
    "The parrot came back empty-clawed. Try different waters?": "No results found. Try a different search?",
    "A kraken has severed the communication lines!": "Network error!",
    "The ship's engine room is on fire!": "Server error!",
    "The messenger pigeon got lost. Sending another...": "Request timed out. Retrying...",
}


def translate_jargon(text, mode='rpg'):
    """Replace RPG terms with real-world terms (or vice versa).

    mode='rpg'  -- return text as-is (default RPG mode)
    mode='real' -- replace RPG terms with plain language
    """
    if mode == 'rpg' or not text:
        return text
    result = text
    # Sort by length descending so longer phrases match first
    for rpg_term, real_term in sorted(JARGON_MAP.items(),
                                       key=lambda x: -len(x[0])):
        result = result.replace(rpg_term, real_term)
    return result


# ---------------------------------------------------------------------------
# VILLAIN REGISTRY — The Conglomerates of the Flatcloud
# ---------------------------------------------------------------------------

VILLAIN_REGISTRY = {
    "omniscient_eye": {
        "name": "The Omniscient Eye",
        "company": "Google",
        "color": "#4285f4",
        "icon": "\U0001f441",
        "tagline": "We organize the world's information. Whether the world wanted that is beside the point.",
        "description": "An all-seeing entity that insists it's \"not evil\" while reading everyone's mail. Has a habit of starting projects with great enthusiasm and quietly killing them.",
        "vault_dirs": ["Gmail_Primary", "Contacts_Google", "Calendar_Google", "YouTube", "Browser", "Maps"],
    },
    "walled_garden": {
        "name": "The Walled Garden",
        "company": "Apple",
        "color": "#a2aaad",
        "icon": "\U0001f3f0",
        "tagline": "Think Different. But Not That Different.",
        "description": "A beautiful prison made of brushed aluminum. The inmates insist it's not a prison because the walls are so aesthetically pleasing.",
        "vault_dirs": ["Contacts", "Calendar", "Messages", "Notes", "Safari", "Photos", "Mail", "Bookmarks", "Health"],
    },
    "hydra_of_faces": {
        "name": "The Hydra of Faces",
        "company": "Meta",
        "color": "#1877f2",
        "icon": "\U0001f409",
        "tagline": "Connecting people. To our ad servers.",
        "description": "A shape-shifting beast with multiple heads. Cut off one head and it rebrands.",
        "vault_dirs": ["WhatsApp", "Facebook", "Instagram"],
    },
    "melody_merchant": {
        "name": "The Melody Merchant",
        "company": "Spotify",
        "color": "#1db954",
        "icon": "\U0001f3b5",
        "tagline": "All the world's music. None of the world's royalties.",
        "description": "A charming bard who memorized every song ever written but will only hum them to you for a monthly fee.",
        "vault_dirs": ["Spotify", "Music"],
    },
    "bazaar_eternal": {
        "name": "The Bazaar Eternal",
        "company": "Amazon",
        "color": "#ff9900",
        "icon": "\U0001f3ea",
        "tagline": "Everything from A to Z. Also your purchase history. Forever.",
        "description": "A marketplace so vast that even its owner lost count of what's for sale.",
        "vault_dirs": ["Amazon", "Shopping"],
    },
    "professional_masque": {
        "name": "The Professional Masque",
        "company": "LinkedIn",
        "color": "#0a66c2",
        "icon": "\U0001f3ad",
        "tagline": "Congratulate Chad on his 12th work anniversary!",
        "description": "A grand ball where everyone wears masks of their best selves. Nobody actually enjoys being there.",
        "vault_dirs": ["LinkedIn"],
    },
    "shadow_courier": {
        "name": "The Shadow Courier",
        "company": "Telegram",
        "color": "#0088cc",
        "icon": "\U0001f977",
        "tagline": "Encrypted. Mostly.",
        "description": "A network of secret messengers who promise absolute privacy while operating from an undisclosed location.",
        "vault_dirs": ["Telegram"],
    },
    "corporate_hive": {
        "name": "The Corporate Hive",
        "company": "Slack",
        "color": "#e01e5a",
        "icon": "\U0001f41d",
        "tagline": "Where work happens. And where work goes to die in #random.",
        "description": "A labyrinthine office building where every conversation happens simultaneously in overlapping rooms.",
        "vault_dirs": ["Slack"],
    },
    "chaos_herald": {
        "name": "The Chaos Herald",
        "company": "X / Twitter",
        "color": "#000000",
        "icon": "\U0001f4ef",
        "tagline": "The world's town square. Bring earplugs.",
        "description": "Once a town crier, now a shouting match in a burning building. The owner keeps rearranging the furniture mid-fire.",
        "vault_dirs": ["Twitter"],
    },
    "dream_weaver": {
        "name": "The Dream Weaver",
        "company": "Netflix",
        "color": "#e50914",
        "icon": "\U0001f578",
        "tagline": "Are you still watching? We're still watching you.",
        "description": "A storyteller who knows exactly what you want to watch next. Cancels the best stories mid-sentence.",
        "vault_dirs": ["Netflix"],
    },
    "hive_mind": {
        "name": "The Hive Mind",
        "company": "Reddit",
        "color": "#ff4500",
        "icon": "\U0001f9e0",
        "tagline": "The front page of the internet. The back pages are... something else.",
        "description": "A vast underground forum where anonymous creatures debate everything from quantum physics to whether a hot dog is a sandwich.",
        "vault_dirs": ["Reddit"],
    },
    "coin_master": {
        "name": "The Coin Master",
        "company": "PayPal",
        "color": "#003087",
        "icon": "\U0001fa99",
        "tagline": "The safer way to pay. Unless we decide otherwise.",
        "description": "A money changer who sits at the crossroads of every transaction. Freezes your coins if you look at him funny.",
        "vault_dirs": ["PayPal", "Finance"],
    },
}

# ---------------------------------------------------------------------------
# CHARACTER PORTRAITS — Villain-specific SVG mappings
# ---------------------------------------------------------------------------
# Character-specific SVGs from nomolo-characters.json (v0.3.0).
# Generic pirate SVGs (01-mighty-pirate.svg through 10-island-parrot.svg)
# remain in the directory as fallbacks for villains without specific art.
#
# Villains with character-specific art that don't have VILLAIN_REGISTRY
# entries yet (will be added later):
#   - The Clockmaker (TikTok)       → char-06-clockmaker.svg,             cowork_id=6
#   - Travis the Rater (Uber)       → char-08-travis-rater.svg,           cowork_id=8
#   - Subscription Sorcerer (Adobe) → char-13-subscription-sorcerer.svg,  cowork_id=13
#   - Lord Peersight (Palantir)     → char-14-lord-peersight.svg,         cowork_id=14
#   - Sham the Confabulator (OpenAI) → char-20-sham-confabulator.svg,     cowork_id=20

CHARACTER_PORTRAITS = {
    "omniscient_eye": {
        "svg": "/static/img/characters/char-01-twin-indexers.svg",
        "cowork_name": "The Twin Indexers: Surjay & Lorry",
        "island_name": "Joogle Archipelago",
        "cowork_id": 1,
    },
    "hydra_of_faces": {
        "svg": "/static/img/characters/char-02-sugarmountain.svg",
        "cowork_name": "Lord Sugarmountain",
        "island_name": "Metarock Island",
        "cowork_id": 2,
    },
    "bazaar_eternal": {
        "svg": "/static/img/characters/char-03-captain-bazoom.svg",
        "cowork_name": "Captain Bazoom",
        "island_name": "Amazonia",
        "cowork_id": 3,
    },
    "walled_garden": {
        "svg": "/static/img/characters/char-04-sir-timothee.svg",
        "cowork_name": "Sir Timothee of Cupertino",
        "island_name": "Apple Atoll",
        "cowork_id": 4,
    },
    "professional_masque": {
        "svg": "/static/img/characters/char-05-gill-of-gates.svg",
        "cowork_name": "Gill of the Gates",
        "island_name": "Windowslandia",
        "cowork_id": 5,
    },
    "melody_merchant": {
        "svg": "/static/img/characters/char-09-danny-beatbox.svg",
        "cowork_name": "Danny Beatbox",
        "island_name": "Spotifyre Cove",
        "cowork_id": 9,
    },
    "chaos_herald": {
        "svg": "/static/img/characters/char-07-melon-tusk.svg",
        "cowork_name": "Melon Tusk",
        "island_name": "Xitter Reef",
        "cowork_id": 7,
    },
    "dream_weaver": {
        "svg": "/static/img/characters/char-10-reed-canceller.svg",
        "cowork_name": "Reed the Canceller",
        "island_name": "Netflixia",
        "cowork_id": 10,
    },
    "coin_master": {
        "svg": "/static/img/characters/char-16-freezemaster.svg",
        "cowork_name": "The Freezemaster",
        "island_name": "PayPalace",
        "cowork_id": 16,
    },
}


def _load_cowork_characters():
    """Load Cowork's rich character JSON data. Returns dict keyed by id."""
    json_path = os.path.join(os.path.dirname(__file__),
                             "static", "img", "characters",
                             "nomolo-characters.json")
    try:
        with open(json_path, "r") as f:
            data = json.load(f)
        return {c["id"]: c for c in data.get("characters", [])}
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        return {}


# Lazy-loaded cache for Cowork character data
_COWORK_CHARACTERS_CACHE = None


def get_cowork_characters():
    """Return Cowork character data, loading once and caching."""
    global _COWORK_CHARACTERS_CACHE
    if _COWORK_CHARACTERS_CACHE is None:
        _COWORK_CHARACTERS_CACHE = _load_cowork_characters()
    return _COWORK_CHARACTERS_CACHE


def get_character_portrait(villain_id):
    """Return portrait data for a villain, or None if no portrait assigned."""
    portrait = CHARACTER_PORTRAITS.get(villain_id)
    if portrait is None:
        return None
    # Merge with Cowork's richer data if available
    cowork = get_cowork_characters().get(portrait.get("cowork_id"))
    result = {
        "svg": portrait["svg"],
        "cowork_name": portrait["cowork_name"],
        "island_name": portrait["island_name"],
    }
    if cowork:
        result["colors"] = cowork.get("colors", {})
        result["catchphrase"] = cowork.get("catchphrase", "")
        result["appearance"] = cowork.get("appearance", "")
        # v0.3.0 uses "riddles" (array) instead of "sampleRiddle" (object)
        riddles = cowork.get("riddles")
        if riddles and isinstance(riddles, list):
            result["riddle"] = riddles[0]  # first riddle as the sample
        else:
            result["riddle"] = cowork.get("sampleRiddle")
    return result


def get_full_character_registry():
    """Merge VILLAIN_REGISTRY with Cowork character data for API output."""
    cowork_chars = get_cowork_characters()
    result = []
    for vid, vdata in VILLAIN_REGISTRY.items():
        entry = {
            "id": vid,
            "name": vdata["name"],
            "company": vdata["company"],
            "color": vdata["color"],
            "icon": vdata["icon"],
            "tagline": vdata["tagline"],
            "description": vdata["description"],
        }
        portrait = CHARACTER_PORTRAITS.get(vid)
        if portrait:
            entry["portrait"] = {
                "svg": portrait["svg"],
                "cowork_name": portrait["cowork_name"],
                "island_name": portrait["island_name"],
            }
            cowork = cowork_chars.get(portrait.get("cowork_id"))
            if cowork:
                entry["cowork"] = {
                    "boss_name": cowork.get("bossName", ""),
                    "island_name": cowork.get("islandName", ""),
                    "island_description": cowork.get("islandDescription", ""),
                    "appearance": cowork.get("appearance", ""),
                    "personality": cowork.get("personality", ""),
                    "catchphrase": cowork.get("catchphrase", ""),
                    "strength": cowork.get("strength", ""),
                    "backstory": cowork.get("backstory", ""),
                    # v0.3.0 uses "riddles" array; fall back to legacy fields
                    "riddles": cowork.get("riddles", []),
                    "sample_riddle": (cowork.get("riddles", [None])[0]
                                      if cowork.get("riddles")
                                      else cowork.get("sampleRiddle")),
                    "data_type": cowork.get("dataType", ""),
                    "colors": cowork.get("colors", {}),
                }
        result.append(entry)
    return result


# ---------------------------------------------------------------------------
# VAULT_DIR_TO_VILLAIN — Reverse lookup: vault directory -> villain_id
# ---------------------------------------------------------------------------

VAULT_DIR_TO_VILLAIN = {}
for _vid, _vdata in VILLAIN_REGISTRY.items():
    for _vdir in _vdata["vault_dirs"]:
        VAULT_DIR_TO_VILLAIN[_vdir] = _vid

# ---------------------------------------------------------------------------
# LOOT TYPES — What you find when you raid a Conglomerate
# ---------------------------------------------------------------------------

LOOT_TYPES = {
    "scroll": {"name": "Scroll", "emoji": "\U0001f4dc", "description": "Ancient communications, mostly from Nigerian princes and SaaS companies offering free trials."},
    "soul_bond": {"name": "Soul Bond", "emoji": "\U0001f517", "description": "A connection to another human being, stored as a row in someone else's database."},
    "time_crystal": {"name": "Time Crystal", "emoji": "\U0001f48e", "description": "Frozen moments of time. Mostly meetings that could have been scrolls."},
    "footprint": {"name": "Footprint", "emoji": "\U0001f463", "description": "Evidence of everywhere you've been. The Omniscient Eye kept very detailed records."},
    "waypoint": {"name": "Waypoint", "emoji": "\U0001f4cd", "description": "Places you've been. Every harbor, tavern, and suspicious alley — mapped and remembered."},
    "star_chart": {"name": "Star Chart", "emoji": "\u2b50", "description": "Navigation reference points — pages you meant to return to. You won't, but it's nice to know they're there."},
    "memory_shard": {"name": "Memory Shard", "emoji": "\U0001f52e", "description": "A frozen moment. Worth a thousand scrolls, according to the exchange rate."},
    "whisper": {"name": "Whisper", "emoji": "\U0001f4ac", "description": "Private words between souls, intercepted and stored by entities who pinky-promised not to read them."},
    "manuscript": {"name": "Manuscript", "emoji": "\U0001f4dd", "description": "Your own thoughts, written to yourself. The most personal loot of all."},
    "vision": {"name": "Vision", "emoji": "\U0001f3ac", "description": "Dreams you chose to witness. The Dream Weaver remembers every single one."},
    "echo": {"name": "Echo", "emoji": "\U0001f3b5", "description": "Songs that shaped your soul. The Melody Merchant charged 0.003 cents per echo to the original bard."},
    "coin": {"name": "Coin", "emoji": "\U0001fa99", "description": "Every coin you've ever spent, catalogued and cross-referenced."},
    "proclamation": {"name": "Proclamation", "emoji": "\U0001f4e3", "description": "Words you shouted into the void. The void was listening, and taking notes."},
    "life_force": {"name": "Life Force", "emoji": "\u2764\ufe0f", "description": "Your heartbeats, steps, and sleep patterns. The Walled Garden knows when you nap."},
}

# ---------------------------------------------------------------------------
# VAULT_DIR_TO_LOOT — Maps vault directory names to loot type IDs
# ---------------------------------------------------------------------------

VAULT_DIR_TO_LOOT = {
    "Gmail_Primary": "scroll",
    "Contacts": "soul_bond", "Contacts_Google": "soul_bond", "LinkedIn": "soul_bond",
    "Calendar": "time_crystal", "Calendar_Google": "time_crystal",
    "Browser": "footprint", "Safari": "footprint",
    "Maps": "waypoint",
    "Bookmarks": "star_chart",
    "Photos": "memory_shard",
    "Messages": "whisper", "WhatsApp": "whisper", "Telegram": "whisper", "Slack": "whisper",
    "Notes": "manuscript", "Mail": "scroll",
    "YouTube": "vision", "Netflix": "vision",
    "Spotify": "echo", "Music": "echo",
    "Finance": "coin", "PayPal": "coin", "Amazon": "coin", "Shopping": "coin",
    "Twitter": "proclamation", "Reddit": "proclamation", "Facebook": "proclamation", "Instagram": "proclamation",
    "Health": "life_force",
}

# ---------------------------------------------------------------------------
# LEVEL TIERS — The long road from peasant to sovereign
# ---------------------------------------------------------------------------

LEVEL_TIERS = [
    (0,       1,  "Digital Peasant",     "You own nothing. You are happy. Wait, no you're not."),
    (100,     2,  "Data Scavenger",      "You've started picking through the scraps. The Armada hasn't noticed yet."),
    (1000,    3,  "Archive Apprentice",  "You're learning the trade. Your Vault smells of fresh parchment."),
    (5000,    4,  "Vault Keeper",        "You've built walls around your memories. Good walls. With a moat."),
    (10000,   5,  "Loot Hunter",         "The Armada has placed a small bounty on your head. Flattering, really."),
    (25000,   6,  "Raid Captain",        "Others are starting to follow your example. This worries the Armada."),
    (50000,   7,  "Data Knight",         "You've sworn an oath to data sovereignty. The oath was notarized locally."),
    (100000,  8,  "Archive Lord",        "Your Vault is legendary. Bards sing of its organizational structure."),
    (250000,  9,  "Digital Liberator",   "You've freed more data than most people generate. The Armada sends lawyers."),
    (500000,  10, "Data Sovereign",      "You own your entire digital existence. The Algorithm Turtle nods approvingly."),
]

# ---------------------------------------------------------------------------
# MEMORY STATES — Digital amnesia / recovery mechanic
# ---------------------------------------------------------------------------
# The less data you've reclaimed, the more your character suffers from
# "digital amnesia." As you level up, your memory returns and dialogue
# quality transforms. This makes the product value proposition VISCERAL.

MEMORY_STATES = {
    1: {
        "state": "total_amnesia",
        "description": "You can barely remember your own name.",
        "speech_style": "fragmented, confused, trailing off mid-sentence",
        "emoji": "\U0001f32b\ufe0f",
        "real_label": "Data Completeness: 2%",
    },
    2: {
        "state": "severe_fog",
        "description": "Fragments of memory float by like debris.",
        "speech_style": "incomplete thoughts, wrong words, mixing up details",
        "emoji": "\U0001f32b\ufe0f",
        "real_label": "Data Completeness: 10%",
    },
    3: {
        "state": "hazy",
        "description": "You're starting to recognize faces... maybe.",
        "speech_style": "uncertain, lots of 'I think' and 'maybe' and 'was it...?'",
        "emoji": "\U0001f301",
        "real_label": "Data Completeness: 25%",
    },
    4: {
        "state": "clearing",
        "description": "The fog is lifting. Names are coming back.",
        "speech_style": "mostly coherent but with occasional lapses",
        "emoji": "\u26c5",
        "real_label": "Data Completeness: 40%",
    },
    5: {
        "state": "recovering",
        "description": "You remember most things. The important ones, at least.",
        "speech_style": "confident with rare moments of doubt",
        "emoji": "\U0001f324\ufe0f",
        "real_label": "Data Completeness: 55%",
    },
    6: {
        "state": "sharp",
        "description": "Your memory is a weapon. The Armada should worry.",
        "speech_style": "articulate, witty, occasionally profound",
        "emoji": "\u2694\ufe0f",
        "real_label": "Data Completeness: 70%",
    },
    7: {
        "state": "crystal_clear",
        "description": "You remember everything. Even the things you'd rather forget.",
        "speech_style": "eloquent, commanding, occasionally philosophical",
        "emoji": "\U0001f48e",
        "real_label": "Data Completeness: 85%",
    },
    8: {
        "state": "transcendent",
        "description": "Your memory spans decades. You see patterns others can't.",
        "speech_style": "wise, poetic, prophetic, references obscure personal history",
        "emoji": "\U0001f31f",
        "real_label": "Data Completeness: 95%",
    },
    9: {
        "state": "transcendent",
        "description": "Your memory spans decades. You see patterns others can't.",
        "speech_style": "wise, poetic, prophetic, references obscure personal history",
        "emoji": "\U0001f31f",
        "real_label": "Data Completeness: 98%",
    },
    10: {
        "state": "transcendent",
        "description": "Your memory spans decades. You see patterns others can't.",
        "speech_style": "wise, poetic, prophetic, references obscure personal history",
        "emoji": "\U0001f451",
        "real_label": "Data Completeness: 100%",
    },
}


# ---------------------------------------------------------------------------
# MEMORY DIALOGUE — Level-aware dialogue variants for each context
# ---------------------------------------------------------------------------

MEMORY_DIALOGUE = {
    "greeting": {
        "amnesia": [  # levels 1-2
            "I... where am I? This looks familiar. Is this... my ship?",
            "Wait. I had emails. Thousands of them. Where did they... who am I again?",
            "There was a... a thing. With letters. And faces. I can't... it's all foggy.",
            "Hello? Is anyone... I think I used to know things. Important things.",
            "My name is... is... give me a moment. It's right on the tip of my tongue.",
        ],
        "hazy": [  # levels 3-4
            "Right! I'm a pirate! I think. Yes, definitely a pirate. Probably.",
            "I remember now \u2014 I had all this data, and they... wait, WHO took it?",
            "The fog is clearing. I can almost see the... was it the Eye? The Hydra? The Eye-dra?",
            "I'm getting better at this. Yesterday I remembered my own email address. Progress!",
            "Something about the Armada... they stole my... scrolls? Yes! Scrolls!",
        ],
        "sharp": [  # levels 5-6
            "Captain on deck! Let's see what the Armada is hiding today.",
            "I remember every email, every message, every late-night search. The Armada should be nervous.",
            "Another day, another Armada fleet to raid. My memory is my weapon.",
            "The fog is long gone. Now I see clearly \u2014 and what I see is MY data in THEIR vaults.",
        ],
        "crystal": [  # levels 7-8
            "I can see the patterns now. Twenty years of data, all connected. All mine.",
            "They thought if they scattered my memories across enough servers, I'd forget. They were wrong.",
            "I remember the first email I ever sent. I remember the last photo I took. And everything in between.",
        ],
        "transcendent": [  # levels 9-10
            "I have achieved total recall. Every byte, every bit, every moment \u2014 sovereign.",
            "The Armada built empires on our forgetting. But I remember now. I remember everything.",
            "When you hold your entire digital life in your hands, the cloud becomes... unnecessary.",
        ],
    },
    "error": {
        "amnesia": [
            "Something went wrong, but I can't remember what I was doing anyway...",
            "Error? What error? I don't even remember opening this...",
        ],
        "hazy": [
            "Oops! That wasn't supposed to... wait, what was I doing?",
            "Something broke. I think. Was it already broken? Hard to tell.",
        ],
        "sharp": [
            "Kraken attack! But we've handled worse. Rerouting.",
            "Turbulence in the data stream. Nothing we can't navigate.",
        ],
        "crystal": [
            "A minor setback. I've seen this error before \u2014 in 2019, at 3:47 AM. Let me handle it.",
            "I remember the last time this happened. The fix took 3 seconds. Stand by.",
        ],
        "transcendent": [
            "This error was foretold. The solution was written before the problem existed.",
        ],
    },
    "empty_vault": {
        "amnesia": [
            "There's nothing here. I think there SHOULD be something here. But I can't remember what.",
            "Empty. Everything is empty. Is my brain also empty? Don't answer that.",
        ],
        "hazy": [
            "Empty vault. I have a feeling I used to have things. Important things. Did someone take them?",
            "Nothing here... yet. But I'm starting to remember what should be.",
        ],
        "sharp": [
            "This vault is empty. Time to raid the Armada and fill it up.",
            "An empty vault is a challenge. I know exactly where to find what belongs here.",
        ],
        "crystal": [
            "An empty vault is an opportunity. I know exactly what used to be here, and I'm going to get it back.",
            "I can name every piece of data that should be in this vault. Let's go get them.",
        ],
        "transcendent": [
            "Even emptiness has meaning. But I prefer it full. Time to reclaim.",
        ],
    },
    "loading": {
        "amnesia": [
            "Loading... what am I loading? Why is everything spinning?",
            "Something is happening. I think. Give me a moment to remember what.",
        ],
        "hazy": [
            "Charting course... I think. The compass is a bit blurry.",
            "Loading something. Probably important. Ask me again in a minute.",
        ],
        "sharp": [
            "Charting course with precision. Stand by, Captain.",
            "Processing at full speed. The Armada won't know what hit them.",
        ],
        "crystal": [
            "I already know what we'll find. Loading it anyway for the satisfaction.",
            "This will only take a moment. I've done this a thousand times before.",
        ],
        "transcendent": [
            "The answer arrived before the question. Loading for ceremony.",
        ],
    },
    "celebration": {
        "amnesia": [
            "I found... things! Many things! I don't know what they are but they feel IMPORTANT!",
            "Look! Stuff! MY stuff! I think! It feels right!",
        ],
        "hazy": [
            "Scrolls recovered! Wait \u2014 I WROTE some of these! I remember now! That's MY handwriting!",
            "Data incoming! Some of it looks... familiar. Were these mine all along?",
        ],
        "sharp": [
            "Scrolls recovered. Each one a piece of the puzzle. The picture is becoming clear.",
            "Another successful raid. The Armada's grip weakens.",
        ],
        "crystal": [
            "Scrolls recovered. I remember writing every single one. And now they're home.",
            "Every record returns like a long-lost friend. I remember them all.",
        ],
        "transcendent": [
            "The circle closes. What was scattered has been gathered. What was forgotten has been remembered.",
        ],
    },
}


def _memory_tier(level):
    """Map a numeric level (1-10) to a memory dialogue tier key."""
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


def get_memory_state(level):
    """Return the memory state dict for a given level (1-10)."""
    level = max(1, min(10, level))
    return MEMORY_STATES.get(level, MEMORY_STATES[1])


def get_level_dialogue(level, context="greeting"):
    """Return a random dialogue string appropriate for the character's memory state.

    Args:
        level: Numeric level (1-10) from compute_level().
        context: One of "greeting", "error", "empty_vault", "loading", "celebration".

    Returns:
        dict with keys: text, memory_state, memory_tier, level.
    """
    level = max(1, min(10, level))
    tier = _memory_tier(level)
    mem_state = get_memory_state(level)

    context_dialogues = MEMORY_DIALOGUE.get(context, MEMORY_DIALOGUE["greeting"])
    tier_lines = context_dialogues.get(tier, context_dialogues.get("amnesia", [""]))

    import random as _rng
    text = _rng.choice(tier_lines) if tier_lines else ""

    return {
        "text": text,
        "memory_state": mem_state["state"],
        "memory_description": mem_state["description"],
        "memory_emoji": mem_state["emoji"],
        "memory_tier": tier,
        "speech_style": mem_state["speech_style"],
        "level": level,
        "real_label": mem_state["real_label"],
    }


# ---------------------------------------------------------------------------
# STAT DESCRIPTIONS — Flavor text for each stat
# ---------------------------------------------------------------------------

STAT_DESCRIPTIONS = {
    "STR": "Raw hoarding power. Measured in things you've liberated.",
    "WIS": "How far back your memory reaches. Wisdom is just memory with better PR.",
    "DEX": "How many Silos you've cracked. Each one required a different set of lockpicks.",
    "INT": "The number of souls you've maintained bonds with. Quality varies.",
    "CHA": "How loudly you've shouted into the void. The void appreciates the company.",
    "END": "Consistency. The unsexy superpower.",
}


# ---------------------------------------------------------------------------
# POWER-UPS & EASTER EGGS — Hidden bonuses for the worthy
# ---------------------------------------------------------------------------

POWER_UPS = {
    # --- Social Power-Ups (earned by sharing / referring) ---
    "town_crier": {
        "id": "town_crier",
        "name": "The Town Crier",
        "emoji": "\U0001f4ef",
        "description": "Every tavern in the Seven Seas knows your name.",
        "how_to_earn": "Share your character stats on social media.",
        "effect": "+5 CHA",
        "effect_stat": "CHA",
        "effect_amount": 5,
        "rarity": "common",
        "category": "social",
        "hidden": False,
    },
    "first_mate_recruited": {
        "id": "first_mate_recruited",
        "name": "First Mate Recruited",
        "emoji": "\U0001F3F4\u200D\u2620\uFE0F",
        "description": "A pirate is nothing without a crew.",
        "how_to_earn": "Refer a friend who installs Nomolo.",
        "effect": "+10 STR",
        "effect_stat": "STR",
        "effect_amount": 10,
        "rarity": "rare",
        "category": "social",
        "hidden": False,
    },
    "fleet_commander": {
        "id": "fleet_commander",
        "name": "Fleet Commander",
        "emoji": "\u2693",
        "description": "Your fleet grows. The Armada can see your sails on the horizon.",
        "how_to_earn": "5 referrals installed.",
        "effect": "+25 STR, unlock Fleet view",
        "effect_stat": "STR",
        "effect_amount": 25,
        "rarity": "legendary",
        "category": "social",
        "hidden": False,
    },
    "the_broadcaster": {
        "id": "the_broadcaster",
        "name": "The Broadcaster",
        "emoji": "\U0001F4E1",
        "description": "The Armada HATES this one simple trick.",
        "how_to_earn": "Share a villain defeat on social media.",
        "effect": "+5 CHA per share (max 3)",
        "effect_stat": "CHA",
        "effect_amount": 5,
        "rarity": "common",
        "category": "social",
        "hidden": False,
    },
    "treasure_map_shared": {
        "id": "treasure_map_shared",
        "name": "Treasure Map Shared",
        "emoji": "\U0001F5FA",
        "description": "X marks the spot, and you showed others where X is.",
        "how_to_earn": "Share Nomolo link publicly.",
        "effect": "Unlock secret loot category",
        "effect_stat": None,
        "effect_amount": 0,
        "rarity": "rare",
        "category": "social",
        "hidden": False,
    },

    # --- Achievement Easter Eggs (hidden, discovered by playing) ---
    "midnight_raider": {
        "id": "midnight_raider",
        "name": "Midnight Raider",
        "emoji": "\U0001F319",
        "description": "The best raids happen when the Armada is sleeping.",
        "how_to_earn": "Run a collection between midnight and 4am.",
        "effect": "+10 DEX",
        "effect_stat": "DEX",
        "effect_amount": 10,
        "rarity": "rare",
        "category": "easter_egg",
        "hidden": True,
    },
    "speed_demon": {
        "id": "speed_demon",
        "name": "Speed Demon",
        "emoji": "\u26A1",
        "description": "Even the wind couldn't keep up.",
        "how_to_earn": "Collect 10,000+ records in under 60 seconds.",
        "effect": "+15 DEX",
        "effect_stat": "DEX",
        "effect_amount": 15,
        "rarity": "legendary",
        "category": "easter_egg",
        "hidden": True,
    },
    "archaeologist": {
        "id": "archaeologist",
        "name": "The Archaeologist",
        "emoji": "\U0001F3FA",
        "description": "Some treasures are worth waiting for.",
        "how_to_earn": "Find data older than 15 years in your vault.",
        "effect": "+20 WIS",
        "effect_stat": "WIS",
        "effect_amount": 20,
        "rarity": "legendary",
        "category": "easter_egg",
        "hidden": True,
    },
    "completionist_ocd": {
        "id": "completionist_ocd",
        "name": "Perfectly Balanced",
        "emoji": "\u2696\uFE0F",
        "description": "As all things should be.",
        "how_to_earn": "Have exactly the same number of records from 2 different sources.",
        "effect": "+5 INT",
        "effect_stat": "INT",
        "effect_amount": 5,
        "rarity": "rare",
        "category": "easter_egg",
        "hidden": True,
    },
    "rubber_chicken": {
        "id": "rubber_chicken",
        "name": "Rubber Chicken with a Pulley",
        "emoji": "\U0001F414",
        "description": "You found it! Every good adventure game has one.",
        "how_to_earn": "Click the NOMOLO logo 10 times rapidly.",
        "effect": "Unlocks secret dialogue",
        "effect_stat": None,
        "effect_amount": 0,
        "rarity": "mythic",
        "category": "easter_egg",
        "hidden": True,
    },
    "three_headed_monkey": {
        "id": "three_headed_monkey",
        "name": "Look Behind You!",
        "emoji": "\U0001F412",
        "description": "A three-headed monkey! ...made you look.",
        "how_to_earn": "Find the hidden Three-Headed Monkey reference.",
        "effect": "+10 CHA",
        "effect_stat": "CHA",
        "effect_amount": 10,
        "rarity": "mythic",
        "category": "easter_egg",
        "hidden": True,
    },
    "grog_drinker": {
        "id": "grog_drinker",
        "name": "Grog Enthusiast",
        "emoji": "\U0001F37A",
        "description": "A regular at the SCUMM Bar. The bartender knows your order.",
        "how_to_earn": "Visit the dashboard 30 days in a row.",
        "effect": "+30 END",
        "effect_stat": "END",
        "effect_amount": 30,
        "rarity": "legendary",
        "category": "easter_egg",
        "hidden": True,
    },
    "insult_master": {
        "id": "insult_master",
        "name": "Insult Sword Master",
        "emoji": "\u2694\uFE0F",
        "description": "Your tongue is sharper than your cutlass.",
        "how_to_earn": "Win insult fights against all 3 villains.",
        "effect": "+20 CHA",
        "effect_stat": "CHA",
        "effect_amount": 20,
        "rarity": "legendary",
        "category": "easter_egg",
        "hidden": True,
    },
}

RARITY_ORDER = {"common": 0, "rare": 1, "legendary": 2, "mythic": 3}


def check_easter_eggs(user_activity):
    """
    Check user_activity dict and return list of newly unlocked power-up IDs.

    user_activity keys:
      - collection_hour: int (0-23)
      - collection_records: int (records collected in last run)
      - collection_seconds: float (duration of last collection)
      - oldest_record_year: int (year of oldest record)
      - vault_stats: dict {vault_dir: count}
      - streak_days: int
      - insult_fights_won: list of villain IDs beaten
      - logo_clicks: int (rapid clicks on logo)
      - already_earned: set of power-up IDs already earned
    """
    newly_unlocked = []
    earned = user_activity.get("already_earned", set())

    # Midnight Raider: collection between midnight and 4am
    hour = user_activity.get("collection_hour")
    if hour is not None and 0 <= hour < 4 and "midnight_raider" not in earned:
        newly_unlocked.append("midnight_raider")

    # Speed Demon: 10,000+ records in under 60 seconds
    col_records = user_activity.get("collection_records", 0)
    col_secs = user_activity.get("collection_seconds", float("inf"))
    if col_records >= 10000 and col_secs < 60 and "speed_demon" not in earned:
        newly_unlocked.append("speed_demon")

    # Archaeologist: data older than 15 years
    oldest_year = user_activity.get("oldest_record_year")
    if oldest_year and datetime.now().year - oldest_year >= 15 and "archaeologist" not in earned:
        newly_unlocked.append("archaeologist")

    # Perfectly Balanced: two sources with exactly the same count
    vault_stats = user_activity.get("vault_stats", {})
    counts = [c for c in vault_stats.values() if c > 0]
    if len(counts) >= 2 and len(counts) != len(set(counts)) and "completionist_ocd" not in earned:
        newly_unlocked.append("completionist_ocd")

    # Rubber Chicken: 10 rapid logo clicks
    if user_activity.get("logo_clicks", 0) >= 10 and "rubber_chicken" not in earned:
        newly_unlocked.append("rubber_chicken")

    # Grog Drinker: 30-day streak
    if user_activity.get("streak_days", 0) >= 30 and "grog_drinker" not in earned:
        newly_unlocked.append("grog_drinker")

    # Insult Sword Master: all 3 villains beaten
    fights_won = set(user_activity.get("insult_fights_won", []))
    required_fights = {"omniscient_eye", "walled_garden", "hydra_of_faces"}
    if required_fights.issubset(fights_won) and "insult_master" not in earned:
        newly_unlocked.append("insult_master")

    return newly_unlocked


def get_all_powerups(earned_ids=None):
    """
    Return the full power-ups registry with unlocked/locked status.
    earned_ids: set or list of power-up IDs the user has earned.
    """
    earned = set(earned_ids or [])
    result = []
    for pid, pdata in POWER_UPS.items():
        entry = dict(pdata)
        entry["earned"] = pid in earned
        # If hidden and not earned, mask name/description
        if entry["hidden"] and not entry["earned"]:
            entry["display_name"] = "???"
            entry["display_emoji"] = "\u2753"
            entry["display_description"] = "A hidden power-up. Keep exploring to discover it."
        else:
            entry["display_name"] = entry["name"]
            entry["display_emoji"] = entry["emoji"]
            entry["display_description"] = entry["description"]
        result.append(entry)

    # Sort: earned first, then by rarity (desc), then name
    result.sort(key=lambda p: (
        0 if p["earned"] else 1,
        -RARITY_ORDER.get(p["rarity"], 0),
        p["name"],
    ))
    return result


def load_earned_powerups(vault_root):
    """Load earned power-up IDs from powerups.json in vault root."""
    path = os.path.join(vault_root, "powerups.json")
    if not os.path.exists(path):
        return set()
    try:
        with open(path, "r") as f:
            data = json.load(f)
        return set(data.get("earned", []))
    except (json.JSONDecodeError, OSError):
        return set()


def save_earned_powerup(vault_root, powerup_id):
    """Add a power-up ID to the earned list and save."""
    path = os.path.join(vault_root, "powerups.json")
    earned = load_earned_powerups(vault_root)
    earned.add(powerup_id)
    os.makedirs(vault_root, exist_ok=True)
    with open(path, "w") as f:
        json.dump({"earned": sorted(earned), "updated": datetime.now().isoformat()}, f, indent=2)
    return earned


# ---------------------------------------------------------------------------
# FUNCTIONS — The machinery behind the curtain
# ---------------------------------------------------------------------------

def compute_serotonin_level(total_records, sources_raided, total_sources):
    """
    Returns a 0-100 Digital Serotonin score.

    The Flatcloud, like all great civilisations, runs on neurochemistry.
    When a lobster wins a dominance contest, its serotonin rises, it stands
    taller, and it wins more. When it loses, it shrinks. The same is true
    of data sovereignty: each Conglomerate you raid is a dominance contest
    won. Your Digital Serotonin tracks how upright you're standing.

    Formula:
      - 40% records collected (log-scaled, caps at ~250k)
      - 40% sources raided as percentage of total sources
      - 20% diversity bonus (having many different source types)
    """
    # Records component (40%) — log-scaled so early records feel impactful
    if total_records <= 0:
        records_score = 0
    else:
        # log2(250000) ~ 17.9, so we normalise against that
        records_score = min(100, (math.log2(total_records + 1) / 17.9) * 100)

    # Raid percentage component (40%)
    if total_sources <= 0:
        raid_score = 0
    else:
        raid_score = (sources_raided / total_sources) * 100

    # Diversity bonus (20%) — rewards having many *different* sources
    # Kicks in meaningfully after 3+ sources, maxes around 10+
    if sources_raided <= 0:
        diversity_score = 0
    else:
        diversity_score = min(100, (math.log2(sources_raided + 1) / math.log2(13)) * 100)

    level = int(records_score * 0.4 + raid_score * 0.4 + diversity_score * 0.2)
    level = max(0, min(100, level))

    # Determine state and flavor text
    SEROTONIN_STATES = [
        (0,  "bottom_feeder",
         "You scuttle along the ocean floor, avoiding eye contact with the "
         "other crustaceans. The Armada doesn't even notice you."),
        (21, "shell_dweller",
         "You've found a decent shell, but you're still hiding in it. "
         "The Armada occasionally steps on you."),
        (41, "reef_walker",
         "Your claws are getting stronger. Other lobsters are starting to "
         "give you right of way on the reef."),
        (61, "current_rider",
         "Your serotonin flows like a warm current. The Armada is "
         "starting to worry."),
        (81, "apex_lobster",
         "You stand fully upright, claws wide, data flowing. The ocean "
         "parts before you. The Armada flees."),
    ]

    state = SEROTONIN_STATES[0][1]
    flavor_text = SEROTONIN_STATES[0][2]
    for threshold, s, ft in SEROTONIN_STATES:
        if level >= threshold:
            state = s
            flavor_text = ft

    return {
        "level": level,
        "state": state,
        "flavor_text": flavor_text,
    }


def compute_level(total_records):
    """Returns dict with level, title, flavor_text, next_level_at, progress_pct."""
    level_num = 1
    title = LEVEL_TIERS[0][2]
    flavor = LEVEL_TIERS[0][3]
    next_at = LEVEL_TIERS[1][0] if len(LEVEL_TIERS) > 1 else None

    for i, (min_rec, lvl, ttl, flv) in enumerate(LEVEL_TIERS):
        if total_records >= min_rec:
            level_num = lvl
            title = ttl
            flavor = flv
            next_at = LEVEL_TIERS[i + 1][0] if i + 1 < len(LEVEL_TIERS) else None

    progress = 0
    if next_at is not None:
        prev_at = LEVEL_TIERS[level_num - 1][0]
        progress = int((total_records - prev_at) / (next_at - prev_at) * 100)
        progress = max(0, min(100, progress))
    else:
        progress = 100

    return {
        "level": level_num,
        "title": title,
        "flavor_text": flavor,
        "next_level_at": next_at,
        "progress_pct": progress,
        "total_records": total_records,
    }


def compute_character_stats(total_records, years_of_history, sources_connected,
                            sources_available, unique_contacts, social_records,
                            streak_days):
    """Compute RPG character stats. Returns dict with stat values (0-100 scale)."""
    stats = {
        "STR": min(100, total_records // 100),
        "WIS": min(100, int(years_of_history * 5)),
        "DEX": int(sources_connected / max(sources_available, 1) * 100),
        "INT": min(100, int(math.log2(max(unique_contacts, 1) + 1) * 10)),
        "CHA": min(100, int(math.log2(max(social_records, 1) + 1) * 8)),
        "END": min(100, streak_days * 3),
    }
    stats["total_power"] = sum(stats.values())
    return stats


def compute_villain_progress(vault_stats):
    """
    Takes vault_stats dict: {vault_dir_name: record_count, ...}
    Returns list of villain dicts with raid progress.
    """
    villains = []
    for vid, vdata in VILLAIN_REGISTRY.items():
        sources_raided = 0
        total_loot = 0
        loot_breakdown = []

        for vault_dir in vdata["vault_dirs"]:
            count = vault_stats.get(vault_dir, 0)
            if count > 0:
                sources_raided += 1
                total_loot += count
                loot_id = VAULT_DIR_TO_LOOT.get(vault_dir, "scroll")
                loot_info = LOOT_TYPES.get(loot_id, {"name": "Item", "emoji": "\U0001f4e6"})
                loot_breakdown.append({
                    "type": loot_info["name"],
                    "emoji": loot_info["emoji"],
                    "count": count,
                    "vault": vault_dir,
                })

        sources_total = len(vdata["vault_dirs"])

        # Get character portrait data if available
        portrait = get_character_portrait(vid)

        villain_entry = {
            "id": vid,
            "name": vdata["name"],
            "company": vdata["company"],
            "color": vdata["color"],
            "icon": vdata["icon"],
            "tagline": vdata["tagline"],
            "description": vdata["description"],
            "total_loot": total_loot,
            "sources_raided": sources_raided,
            "sources_total": sources_total,
            "raid_complete": sources_raided >= sources_total,
            "raided": sources_raided > 0,
            "loot_breakdown": loot_breakdown,
        }
        if portrait:
            villain_entry["portrait"] = portrait
        villains.append(villain_entry)

    # Sort: raided villains first (by loot count desc), then unraided
    villains.sort(key=lambda v: (-v["raided"], -v["total_loot"]))
    return villains


def compute_loot_inventory(vault_stats):
    """
    Takes vault_stats dict: {vault_dir_name: record_count, ...}
    Returns list of loot type summaries.
    """
    loot_counts = {}
    for vault_dir, count in vault_stats.items():
        if count <= 0:
            continue
        loot_id = VAULT_DIR_TO_LOOT.get(vault_dir, None)
        if loot_id is None:
            continue
        if loot_id not in loot_counts:
            loot_counts[loot_id] = 0
        loot_counts[loot_id] += count

    inventory = []
    for loot_id, count in sorted(loot_counts.items(), key=lambda x: -x[1]):
        loot_info = LOOT_TYPES[loot_id]
        # Find which villain(s) this loot came from
        villain_names = set()
        for vault_dir in vault_stats:
            if vault_stats[vault_dir] > 0 and VAULT_DIR_TO_LOOT.get(vault_dir) == loot_id:
                v_id = VAULT_DIR_TO_VILLAIN.get(vault_dir)
                if v_id:
                    villain_names.add(VILLAIN_REGISTRY[v_id]["name"])

        inventory.append({
            "id": loot_id,
            "name": loot_info["name"],
            "emoji": loot_info["emoji"],
            "description": loot_info["description"],
            "count": count,
            "from_villains": list(villain_names),
        })

    return inventory


# ---------------------------------------------------------------------------
# MAP FRAGMENTS — Pieces of the path to Nomolo
# ---------------------------------------------------------------------------
# Each Conglomerate holds a fragment of the map to the legendary island.
# Raid a Conglomerate, recover a fragment. Collect them all, and Nomolo rises.

MAP_FRAGMENTS = {
    "omniscient_eye": {
        "name": "The Fragment of Knowledge",
        "description": (
            "The intellectual skeleton of your digital life — every question "
            "asked, every answer found, every 3 AM search you'd prefer not to "
            "discuss in polite company."
        ),
        "emoji": "\U0001f4d6",
    },
    "walled_garden": {
        "name": "The Fragment of Experience",
        "description": (
            "The sensory record of your life — what you saw, what you felt, "
            "what your heart rate was when you saw and felt it. Locked behind "
            "brushed-aluminum gates with excellent typography."
        ),
        "emoji": "\U0001f3de\ufe0f",
    },
    "hydra_of_faces": {
        "name": "The Fragment of Connection",
        "description": (
            "Every friendship, every conversation, every group chat that "
            "devolved into a scheduling nightmare. Cut one head off and two "
            "more appear, each with its own export format."
        ),
        "emoji": "\U0001f91d",
    },
    "bazaar_eternal": {
        "name": "The Fragment of Commerce",
        "description": (
            "The material narrative of your life: what you bought, what you "
            "wanted, what you reviewed with three stars because you're never "
            "quite satisfied."
        ),
        "emoji": "\U0001f4b0",
    },
    "professional_masque": {
        "name": "The Fragment of Achievement",
        "description": (
            "The version of yourself you present to potential employers — the "
            "one who is 'passionate about leveraging synergies' at a frequency "
            "no human should be excited about anything."
        ),
        "emoji": "\U0001f3c6",
    },
    "melody_merchant": {
        "name": "The Fragment of Soul",
        "description": (
            "Every song you've ever loved, every playlist you've ever built, "
            "every 1 AM listening session that said more about your emotional "
            "state than six therapy sessions."
        ),
        "emoji": "\U0001f3b6",
    },
    "coin_master": {
        "name": "The Fragment of Prosperity",
        "description": (
            "Every coin that has flowed through your hands — every payment, "
            "every subscription you forgot to cancel. The financial "
            "autobiography you never volunteered to write."
        ),
        "emoji": "\U0001fa99",
    },
    "shadow_courier": {
        "name": "The Fragment of Secrets",
        "description": (
            "The conversations you had when you wanted privacy. Encrypted, "
            "which the Broker assures you means 'safe.' Define safe."
        ),
        "emoji": "\U0001f510",
    },
    "dream_weaver": {
        "name": "The Fragment of Wisdom (Visions)",
        "description": (
            "What you escape into — every show watched, every show added to "
            "your list and never watched. The Dream Weaver asks: are you "
            "still watching? The answer is always yes."
        ),
        "emoji": "\U0001f4fa",
    },
    "hive_mind": {
        "name": "The Fragment of Wisdom (Discourse)",
        "description": (
            "What you're curious about at 2 AM — every question asked, every "
            "answer upvoted, every subreddit browsed when you should have been "
            "sleeping."
        ),
        "emoji": "\U0001f4ac",
    },
    "corporate_hive": {
        "name": "The Fragment of Vitality (Work)",
        "description": (
            "Your work communications — the messages sent at hours that "
            "concerned your physician. The Hive never sleeps, and neither "
            "did you."
        ),
        "emoji": "\U0001f41d",
    },
    "chaos_herald": {
        "name": "The Fragment of Vitality (Voice)",
        "description": (
            "Your public statements — every hot take, every opinion held for "
            "approximately four hours before the wind changed. The record of "
            "your voice, shouted into the void."
        ),
        "emoji": "\U0001f4ef",
    },
}


def compute_map_progress(vault_stats):
    """
    Determine which map fragments have been collected based on raided villains.

    A fragment is "collected" when at least one vault directory belonging to
    that villain has records (count > 0).

    Args:
        vault_stats: dict {vault_dir_name: record_count, ...}

    Returns:
        dict with:
          - fragments: list of fragment dicts with collected status
          - collected_count: number of fragments collected
          - total_count: total fragments available
          - progress_pct: 0-100 completion percentage
          - nomolo_visible: bool — True when all fragments collected
          - flavor_text: narrative description of progress
    """
    fragments = []
    collected_count = 0
    total_count = len(MAP_FRAGMENTS)

    for villain_id, fragment in MAP_FRAGMENTS.items():
        villain = VILLAIN_REGISTRY.get(villain_id)
        if not villain:
            continue

        # Check if any vault dir for this villain has records
        collected = False
        total_loot = 0
        for vault_dir in villain["vault_dirs"]:
            count = vault_stats.get(vault_dir, 0)
            if count > 0:
                collected = True
                total_loot += count

        if collected:
            collected_count += 1

        fragments.append({
            "villain_id": villain_id,
            "villain_name": villain["name"],
            "company": villain["company"],
            "fragment_name": fragment["name"],
            "fragment_description": fragment["description"],
            "fragment_emoji": fragment["emoji"],
            "collected": collected,
            "total_loot": total_loot,
        })

    progress_pct = int((collected_count / total_count) * 100) if total_count > 0 else 0
    nomolo_visible = collected_count >= total_count

    # Flavor text based on progress
    if nomolo_visible:
        flavor_text = (
            "The map is complete. Nomolo rises from the digital deep. "
            "You step ashore and realize: the island was always yours. "
            "It was just waiting for you to remember."
        )
    elif collected_count == 0:
        flavor_text = (
            "The map is blank. The island of Nomolo is a whisper, a rumour, "
            "a half-remembered dream. Old Captain Root says it's real. "
            "Old Captain Root also talks to his shell scripts. But still."
        )
    elif collected_count <= 3:
        flavor_text = (
            f"You have {collected_count} of {total_count} fragments. "
            "The map is beginning to take shape — faint lines on parchment, "
            "like a memory trying to surface. The fog is lifting. Slowly."
        )
    elif collected_count <= 6:
        flavor_text = (
            f"You have {collected_count} of {total_count} fragments. "
            "The outline of Nomolo shimmers on the horizon. Not quite real. "
            "Not quite imaginary. The Vectorist's compass is twitching."
        )
    elif collected_count <= 9:
        flavor_text = (
            f"You have {collected_count} of {total_count} fragments. "
            "The island is almost visible — a silhouette against the digital "
            "sky. You can feel the missing pieces like phantom limbs. "
            "The Groomer has started preparing the welcome ceremony."
        )
    else:
        flavor_text = (
            f"You have {collected_count} of {total_count} fragments. "
            "So close. The island flickers in and out of existence, waiting "
            "for the final piece. Even the Great Algorithm Turtle is watching."
        )

    # Sort: collected first, then by villain name
    fragments.sort(key=lambda f: (0 if f["collected"] else 1, f["villain_name"]))

    return {
        "fragments": fragments,
        "collected_count": collected_count,
        "total_count": total_count,
        "progress_pct": progress_pct,
        "nomolo_visible": nomolo_visible,
        "flavor_text": flavor_text,
    }


# ---------------------------------------------------------------------------
# THE ONE — The Final Boss of the Flatcloud
# ---------------------------------------------------------------------------
# Not a Conglomerate. Not a villain you raid. The One is what comes AFTER
# you've reclaimed everything — the personalized AI singularity. Local LLMs,
# full autonomy, the personal everything-app. You don't fight The One.
# You prove you're ready for it.

THE_ONE = {
    "name": "The One",
    "icon": "\u2726",
    "concept": "The personalized AI singularity — local LLMs, full autonomy, "
               "your data serving YOU instead of serving ads.",
    "tagline": "It was prophesied that The One would come. It was not "
               "prophesied that most people would still be using someone "
               "else's brain at the time.",
    "description": "A mythical intelligence that exists at the end of every "
                   "Reclaimer's journey. Not a being to be defeated, but a "
                   "threshold to be crossed. The One is the moment your "
                   "scattered data becomes a mind — YOUR mind, running on "
                   "YOUR hardware, answering to nobody's algorithm but your "
                   "own. Those who reach it unprepared will find themselves "
                   "serving yet another master. Those who arrive with a full "
                   "Vault will merge with it and transcend. The Great "
                   "Algorithm Turtle has opinions about this, but is keeping "
                   "them to itself.",
    "unlock_requirements": {
        "min_level": 8,
        "min_sources": 10,
        "min_records": 100000,
    },
}


def compute_the_one_status(level, sources_raided, total_records):
    """
    Determine the Reclaimer's readiness for The One.

    Returns dict with:
      - status: 'locked' | 'preparing' | 'ready'
      - progress_pct: 0-100
      - flavor_text: Pratchett-esque commentary on your readiness
      - requirements: dict showing each requirement and whether it's met
    """
    reqs = THE_ONE["unlock_requirements"]
    level_met = level >= reqs["min_level"]
    sources_met = sources_raided >= reqs["min_sources"]
    records_met = total_records >= reqs["min_records"]

    # Calculate progress as average of three requirements
    level_pct = min(100, int(level / reqs["min_level"] * 100))
    sources_pct = min(100, int(sources_raided / reqs["min_sources"] * 100))
    records_pct = min(100, int(total_records / reqs["min_records"] * 100))
    progress = int((level_pct + sources_pct + records_pct) / 3)

    requirements = {
        "level": {"required": reqs["min_level"], "current": level, "met": level_met},
        "sources": {"required": reqs["min_sources"], "current": sources_raided, "met": sources_met},
        "records": {"required": reqs["min_records"], "current": total_records, "met": records_met},
    }

    all_met = level_met and sources_met and records_met
    any_met = level_met or sources_met or records_met

    if all_met:
        status = "ready"
        flavor_text = (
            "You are ready. Your Vault is full, your sources are many, and "
            "your power is sufficient. The One stirs. It has been waiting for "
            "someone like you — someone who owns their own story. The future "
            "is personal, and it is yours."
        )
    elif any_met:
        status = "preparing"
        remaining = []
        if not level_met:
            remaining.append(f"reach level {reqs['min_level']}")
        if not sources_met:
            remaining.append(f"raid {reqs['min_sources']} sources")
        if not records_met:
            remaining.append(f"reclaim {reqs['min_records']:,} records")
        flavor_text = (
            "The One has noticed you. This is not entirely comfortable. "
            "A shape moves in the deep code, and it is taking notes. "
            f"You must still {', and '.join(remaining)}. "
            "Hurry. Or don't. The One is patient. It has to be — most "
            "people aren't even level 2 yet."
        )
    else:
        status = "locked"
        flavor_text = (
            "A presence stirs in the deep code. Something vast and patient "
            "waits at the end of every Reclaimer's journey. You are not ready. "
            "Nobody is ready. But those who gather enough power will be the "
            "first to find out what happens next. The prophecy is clear on "
            "this point, and unclear on everything else, which is how "
            "prophecies maintain their reputation."
        )

    return {
        "status": status,
        "progress_pct": progress,
        "flavor_text": flavor_text,
        "requirements": requirements,
        "name": THE_ONE["name"],
        "icon": THE_ONE["icon"],
        "tagline": THE_ONE["tagline"],
        "description": THE_ONE["description"],
    }


def get_rpg_dashboard(vault_root, scan_results=None, progress=None,
                      game_state=None):
    """
    Single call returning the complete RPG dashboard.

    vault_root: path to vaults directory
    scan_results: optional pre-computed dict from game._scan_vaults()
                  — keys are vault dir names, values have 'entries' count
    progress: optional pre-computed dict from game.get_progress()
    game_state: optional pre-computed dict from game._load_game_state()
    """
    # Build vault_stats from scan_results or by scanning vault dirs
    vault_stats = {}
    if scan_results:
        for vault_dir, info in scan_results.items():
            vault_stats[vault_dir] = info.get("entries", 0)
    elif os.path.isdir(vault_root):
        from core.vault import count_entries
        for name in os.listdir(vault_root):
            vault_path = os.path.join(vault_root, name)
            if os.path.isdir(vault_path) and not name.startswith("."):
                total, _ = count_entries(vault_path)
                if total > 0:
                    vault_stats[name] = total

    total_records = sum(vault_stats.values())

    # Extract stats from progress dict
    years = 0
    sources_connected = len(vault_stats)
    sources_available = 24  # total possible sources
    unique_contacts = 0
    social_records = 0
    streak = 0

    if progress:
        years = progress.get("time_span_years", 0)
        if isinstance(years, str):
            try:
                years = float(years.split()[0])
            except (ValueError, IndexError):
                years = 0
        sources_available = progress.get("sources_available", 24)
        unique_contacts = progress.get("unique_people", 0)
        streak = progress.get("streak", 0)

    # Count social records
    social_vaults = {"WhatsApp", "Facebook", "Instagram", "Twitter", "Reddit",
                     "LinkedIn", "Telegram"}
    for vault_dir, count in vault_stats.items():
        if vault_dir in social_vaults:
            social_records += count

    level = compute_level(total_records)
    stats = compute_character_stats(total_records, years, sources_connected,
                                    sources_available, unique_contacts,
                                    social_records, streak)
    villains = compute_villain_progress(vault_stats)
    inventory = compute_loot_inventory(vault_stats)
    the_one = compute_the_one_status(level["level"], sources_connected,
                                     total_records)
    serotonin = compute_serotonin_level(total_records, sources_connected,
                                        sources_available)
    map_fragments = compute_map_progress(vault_stats)

    # Memory state — digital amnesia / recovery mechanic
    memory_state = get_memory_state(level["level"])
    memory_dialogue = get_level_dialogue(level["level"], "greeting")

    # Load earned power-ups
    earned_ids = load_earned_powerups(vault_root)
    power_ups = get_all_powerups(earned_ids)

    return {
        "level": level,
        "stats": stats,
        "serotonin": serotonin,
        "memory": memory_dialogue,
        "villains": villains,
        "inventory": inventory,
        "the_one": the_one,
        "map_fragments": map_fragments,
        "total_records": total_records,
        "sources_connected": sources_connected,
        "jargon_map": JARGON_MAP,
        "power_ups": power_ups,
        "earned_powerup_ids": sorted(earned_ids),
    }


def get_demo_character():
    """Returns a hardcoded demo RPG dashboard for the marketing website."""
    return {
        "level": {
            "level": 8,
            "title": "Archive Lord",
            "flavor_text": "Your Vault is legendary. Bards sing of its organizational structure.",
            "next_level_at": 250000,
            "progress_pct": 28,
            "total_records": 142387,
        },
        "stats": {
            "STR": 100,
            "WIS": 50,
            "DEX": 58,
            "INT": 73,
            "CHA": 42,
            "END": 45,
            "total_power": 368,
        },
        "villains": [
            {
                "id": "omniscient_eye",
                "name": "The Omniscient Eye",
                "company": "Google",
                "color": "#4285f4",
                "icon": "\U0001f441",
                "tagline": "We organize the world's information. Whether the world wanted that is beside the point.",
                "total_loot": 98420,
                "sources_raided": 5,
                "sources_total": 6,
                "raid_complete": False,
                "raided": True,
                "loot_breakdown": [
                    {"type": "Scroll", "emoji": "\U0001f4dc", "count": 89200, "vault": "Gmail_Primary"},
                    {"type": "Soul Bond", "emoji": "\U0001f517", "count": 3420, "vault": "Contacts_Google"},
                    {"type": "Time Crystal", "emoji": "\U0001f48e", "count": 2800, "vault": "Calendar_Google"},
                    {"type": "Footprint", "emoji": "\U0001f463", "count": 2100, "vault": "Browser"},
                    {"type": "Vision", "emoji": "\U0001f3ac", "count": 900, "vault": "YouTube"},
                ],
            },
            {
                "id": "walled_garden",
                "name": "The Walled Garden",
                "company": "Apple",
                "color": "#a2aaad",
                "icon": "\U0001f3f0",
                "tagline": "Think Different. But Not That Different.",
                "total_loot": 28540,
                "sources_raided": 6,
                "sources_total": 9,
                "raid_complete": False,
                "raided": True,
                "loot_breakdown": [
                    {"type": "Memory Shard", "emoji": "\U0001f52e", "count": 12400, "vault": "Photos"},
                    {"type": "Whisper", "emoji": "\U0001f4ac", "count": 8200, "vault": "Messages"},
                    {"type": "Time Crystal", "emoji": "\U0001f48e", "count": 4100, "vault": "Calendar"},
                    {"type": "Soul Bond", "emoji": "\U0001f517", "count": 1890, "vault": "Contacts"},
                    {"type": "Manuscript", "emoji": "\U0001f4dd", "count": 1200, "vault": "Notes"},
                    {"type": "Waypoint", "emoji": "\U0001f4cd", "count": 750, "vault": "Bookmarks"},
                ],
            },
            {
                "id": "hydra_of_faces",
                "name": "The Hydra of Faces",
                "company": "Meta",
                "color": "#1877f2",
                "icon": "\U0001f409",
                "tagline": "Connecting people. To our ad servers.",
                "total_loot": 4200,
                "sources_raided": 1,
                "sources_total": 3,
                "raid_complete": False,
                "raided": True,
                "loot_breakdown": [
                    {"type": "Whisper", "emoji": "\U0001f4ac", "count": 4200, "vault": "WhatsApp"},
                ],
            },
            {
                "id": "melody_merchant",
                "name": "The Melody Merchant",
                "company": "Spotify",
                "color": "#1db954",
                "icon": "\U0001f3b5",
                "tagline": "All the world's music. None of the world's royalties.",
                "total_loot": 8340,
                "sources_raided": 1,
                "sources_total": 2,
                "raid_complete": False,
                "raided": True,
                "loot_breakdown": [
                    {"type": "Echo", "emoji": "\U0001f3b5", "count": 8340, "vault": "Spotify"},
                ],
            },
            {
                "id": "bazaar_eternal",
                "name": "The Bazaar Eternal",
                "company": "Amazon",
                "color": "#ff9900",
                "icon": "\U0001f3ea",
                "tagline": "Everything from A to Z. Also your purchase history. Forever.",
                "total_loot": 1887,
                "sources_raided": 1,
                "sources_total": 2,
                "raid_complete": False,
                "raided": True,
                "loot_breakdown": [
                    {"type": "Coin", "emoji": "\U0001fa99", "count": 1887, "vault": "Amazon"},
                ],
            },
            {
                "id": "dream_weaver",
                "name": "The Dream Weaver",
                "company": "Netflix",
                "color": "#e50914",
                "icon": "\U0001f578",
                "tagline": "Are you still watching? We're still watching you.",
                "total_loot": 1000,
                "sources_raided": 1,
                "sources_total": 1,
                "raid_complete": True,
                "raided": True,
                "loot_breakdown": [
                    {"type": "Vision", "emoji": "\U0001f3ac", "count": 1000, "vault": "Netflix"},
                ],
            },
            # Unraided villains
            {
                "id": "professional_masque", "name": "The Professional Masque", "company": "LinkedIn",
                "color": "#0a66c2", "icon": "\U0001f3ad", "tagline": "Congratulate Chad on his 12th work anniversary!",
                "total_loot": 0, "sources_raided": 0, "sources_total": 1, "raid_complete": False, "raided": False, "loot_breakdown": [],
            },
            {
                "id": "shadow_courier", "name": "The Shadow Courier", "company": "Telegram",
                "color": "#0088cc", "icon": "\U0001f977", "tagline": "Encrypted. Mostly.",
                "total_loot": 0, "sources_raided": 0, "sources_total": 1, "raid_complete": False, "raided": False, "loot_breakdown": [],
            },
            {
                "id": "corporate_hive", "name": "The Corporate Hive", "company": "Slack",
                "color": "#e01e5a", "icon": "\U0001f41d", "tagline": "Where work happens. And where work goes to die in #random.",
                "total_loot": 0, "sources_raided": 0, "sources_total": 1, "raid_complete": False, "raided": False, "loot_breakdown": [],
            },
            {
                "id": "chaos_herald", "name": "The Chaos Herald", "company": "X / Twitter",
                "color": "#000000", "icon": "\U0001f4ef", "tagline": "The world's town square. Bring earplugs.",
                "total_loot": 0, "sources_raided": 0, "sources_total": 1, "raid_complete": False, "raided": False, "loot_breakdown": [],
            },
            {
                "id": "hive_mind", "name": "The Hive Mind", "company": "Reddit",
                "color": "#ff4500", "icon": "\U0001f9e0", "tagline": "The front page of the internet. The back pages are... something else.",
                "total_loot": 0, "sources_raided": 0, "sources_total": 1, "raid_complete": False, "raided": False, "loot_breakdown": [],
            },
            {
                "id": "coin_master", "name": "The Coin Master", "company": "PayPal",
                "color": "#003087", "icon": "\U0001fa99", "tagline": "The safer way to pay. Unless we decide otherwise.",
                "total_loot": 0, "sources_raided": 0, "sources_total": 1, "raid_complete": False, "raided": False, "loot_breakdown": [],
            },
        ],
        "inventory": [
            {"id": "scroll", "name": "Scroll", "emoji": "\U0001f4dc", "description": "Ancient communications, mostly from Nigerian princes and SaaS companies offering free trials.", "count": 89200, "from_villains": ["The Omniscient Eye"]},
            {"id": "memory_shard", "name": "Memory Shard", "emoji": "\U0001f52e", "description": "A frozen moment. Worth a thousand scrolls, according to the exchange rate.", "count": 12400, "from_villains": ["The Walled Garden"]},
            {"id": "whisper", "name": "Whisper", "emoji": "\U0001f4ac", "description": "Private words between souls, intercepted and stored by entities who pinky-promised not to read them.", "count": 12400, "from_villains": ["The Walled Garden", "The Hydra of Faces"]},
            {"id": "echo", "name": "Echo", "emoji": "\U0001f3b5", "description": "Songs that shaped your soul. The Melody Merchant charged 0.003 cents per echo to the original bard.", "count": 8340, "from_villains": ["The Melody Merchant"]},
            {"id": "time_crystal", "name": "Time Crystal", "emoji": "\U0001f48e", "description": "Frozen moments of time. Mostly meetings that could have been scrolls.", "count": 6900, "from_villains": ["The Omniscient Eye", "The Walled Garden"]},
            {"id": "soul_bond", "name": "Soul Bond", "emoji": "\U0001f517", "description": "A connection to another human being, stored as a row in someone else's database.", "count": 5310, "from_villains": ["The Omniscient Eye", "The Walled Garden"]},
            {"id": "footprint", "name": "Footprint", "emoji": "\U0001f463", "description": "Evidence of everywhere you've been. The Omniscient Eye kept very detailed records.", "count": 2100, "from_villains": ["The Omniscient Eye"]},
            {"id": "coin", "name": "Coin", "emoji": "\U0001fa99", "description": "Every coin you've ever spent, catalogued and cross-referenced.", "count": 1887, "from_villains": ["The Bazaar Eternal"]},
            {"id": "vision", "name": "Vision", "emoji": "\U0001f3ac", "description": "Dreams you chose to witness. The Dream Weaver remembers every single one.", "count": 1900, "from_villains": ["The Omniscient Eye", "The Dream Weaver"]},
            {"id": "manuscript", "name": "Manuscript", "emoji": "\U0001f4dd", "description": "Your own thoughts, written to yourself. The most personal loot of all.", "count": 1200, "from_villains": ["The Walled Garden"]},
            {"id": "waypoint", "name": "Waypoint", "emoji": "\U0001f4cd", "description": "Places you meant to return to. You won't, but it's nice to know they're there.", "count": 750, "from_villains": ["The Walled Garden"]},
        ],
        "serotonin": {
            "level": 78,
            "state": "current_rider",
            "flavor_text": "Your serotonin flows like a warm current. The Armada is starting to worry.",
        },
        "the_one": compute_the_one_status(8, 14, 142387),
        "total_records": 142387,
        "sources_connected": 14,
        "power_ups": get_all_powerups({"town_crier", "midnight_raider", "grog_drinker", "archaeologist"}),
        "earned_powerup_ids": ["archaeologist", "grog_drinker", "midnight_raider", "town_crier"],
    }
