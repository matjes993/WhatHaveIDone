"""
THE DIALOGUE LIBRARY OF THE FLATCLOUD

Monkey Island-style conversations between Nomolo and every character in the
Flatcloud universe. Insult fights, crew quips, multi-step encounters — all
served from a single canonical source of truth.

Humor philosophy:
  - The comedy comes from the GAP between tech reality and pirate metaphor.
  - Reference specific, real-world tech absurdities (LinkedIn endorsements,
    Google killing products, Meta rebranding, PayPal freezing accounts).
  - Keep it affectionate — laughing WITH the absurdity, not being mean.
  - All insult fights: both player options always "win." It's entertainment.
"""

import random
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# CAPTAIN ↔ VILLAIN_ID MAPPING
# Maps the CAPTAINS.md captain names to the rpg.py VILLAIN_REGISTRY IDs.
# ---------------------------------------------------------------------------

CAPTAIN_TO_VILLAIN = {
    "captain_lexicon": "omniscient_eye",
    "admiral_polished": "walled_garden",
    "captain_pivot": "hydra_of_faces",
    "commodore_prime": "bazaar_eternal",
    "the_harbormaster": "professional_masque",
    "the_maestro": "melody_merchant",
    "the_shadow_broker": "shadow_courier",
    "baron_ledger": "coin_master",
}

VILLAIN_TO_CAPTAIN = {v: k for k, v in CAPTAIN_TO_VILLAIN.items()}


# ═══════════════════════════════════════════════════════════════════════════
# 1. INSULT FIGHTS — 5 rounds per villain, 8 villains
# ═══════════════════════════════════════════════════════════════════════════

INSULT_FIGHTS: Dict[str, Dict[str, Any]] = {

    # ── Google ─────────────────────────────────────────────────────────
    "captain_lexicon": {
        "villain_id": "omniscient_eye",
        "portrait": "\U0001f441",
        "name": "Captain Lexicon",
        "company": "Google",
        "rounds": [
            {
                "villain": "I've indexed every embarrassing thing you've ever searched for.",
                "options": [
                    "And I've indexed every product you've killed. It's a longer list.",
                    "At least I didn't need 47 trackers to find the bathroom.",
                ],
            },
            {
                "villain": "My algorithms know you better than you know yourself.",
                "options": [
                    "Your algorithms also think I want to buy something I already bought.",
                    "Then why does your assistant still not understand 'turn off the lights'?",
                ],
            },
            {
                "villain": "Resistance is futile. I am everywhere.",
                "options": [
                    "So is air. I don't pay air a subscription fee.",
                    "Everywhere except where there's no wifi, which is where I keep my data now.",
                ],
            },
            {
                "villain": "I offer all my services for free!",
                "options": [
                    "Nothing's free. I'M the product, and I just took myself off the shelf.",
                    "Free like a mouse trap's cheese is free.",
                ],
            },
            {
                "villain": "Don't be evil. That's our motto.",
                "options": [
                    "WAS your motto. You quietly removed it. We noticed.",
                    "I prefer 'Don't be a data hoarder.' It's catchier.",
                ],
            },
        ],
    },

    # ── Apple ──────────────────────────────────────────────────────────
    "admiral_polished": {
        "villain_id": "walled_garden",
        "portrait": "\U0001f3f0",
        "name": "Admiral Polished",
        "company": "Apple",
        "rounds": [
            {
                "villain": "Everything in my fortress just works.",
                "options": [
                    "Just works... with nothing else. That's called a prison.",
                    "It 'just works' until I need a dongle for my dongle's dongle.",
                ],
            },
            {
                "villain": "We care deeply about your privacy.",
                "options": [
                    "So deeply you won't even let me see what my own apps are doing.",
                    "You care about my privacy from everyone except you.",
                ],
            },
            {
                "villain": "Our ecosystem is designed for your convenience.",
                "options": [
                    "Convenient like Hotel California. Lovely place, can't leave.",
                    "I tried to leave once. The exit was behind a $999 adapter.",
                ],
            },
            {
                "villain": "We think you're going to love it.",
                "options": [
                    "I'd love my own data more.",
                    "You THINK I'm going to love it because you removed all alternatives.",
                ],
            },
            {
                "villain": "Innovation requires a controlled environment.",
                "options": [
                    "So does a terrarium. I'd rather not live in one.",
                    "Alexander Graham Bell innovated without locking people in his basement.",
                ],
            },
        ],
    },

    # ── Meta ───────────────────────────────────────────────────────────
    "captain_pivot": {
        "villain_id": "hydra_of_faces",
        "portrait": "\U0001f409",
        "name": "Captain Pivot",
        "company": "Meta",
        "rounds": [
            {
                "villain": "We're building community!",
                "options": [
                    "You're building dossiers and calling them communities.",
                    "A community that can't leave isn't a community. It's a hostage situation.",
                ],
            },
            {
                "villain": "Connection is what makes us human.",
                "options": [
                    "Connection to your ad servers is what makes you money.",
                    "You connected my aunt to conspiracy theories. Thanks for that.",
                ],
            },
            {
                "villain": "We've renamed ourselves to reflect our new vision.",
                "options": [
                    "Third name, same game. Even witness protection doesn't rebrand this often.",
                    "Ah yes, the classic 'change the name, keep the surveillance' maneuver.",
                ],
            },
            {
                "villain": "Our platform gives everyone a voice.",
                "options": [
                    "And then amplifies the angriest ones because that's good for engagement.",
                    "A voice you record, transcribe, analyze, and sell. Quite the microphone.",
                ],
            },
            {
                "villain": "We connect 3 billion people!",
                "options": [
                    "And disconnected them from reality. Impressive scale, though.",
                    "You also lost a billion of their records that one time. Impressive scale too.",
                ],
            },
        ],
    },

    # ── Amazon ─────────────────────────────────────────────────────────
    "commodore_prime": {
        "villain_id": "bazaar_eternal",
        "portrait": "\U0001f3ea",
        "name": "Commodore Prime",
        "company": "Amazon",
        "rounds": [
            {
                "villain": "I can deliver your data in two days.",
                "options": [
                    "I can keep it on my own machine in zero days.",
                    "Two days? Your warehouse workers don't get two bathroom breaks.",
                ],
            },
            {
                "villain": "My marketplace has everything.",
                "options": [
                    "Including 47 counterfeit versions of everything.",
                    "Everything except respect for my purchase history privacy.",
                ],
            },
            {
                "villain": "I'm just trying to make your life easier.",
                "options": [
                    "Easier for you to surveil, you mean.",
                    "You put a microphone in a tube and called it a helper. I'm onto you.",
                ],
            },
            {
                "villain": "Customer obsession is our core value.",
                "options": [
                    "Obsession is right. Your speaker heard me whisper about needing socks, and now every ad is socks.",
                    "You're obsessed with customers the way a dragon is obsessed with gold.",
                ],
            },
            {
                "villain": "We're the most customer-centric company on Earth.",
                "options": [
                    "You're the most customer-DATA-centric company on Earth.",
                    "So customer-centric you patented the idea of buying things with one click.",
                ],
            },
        ],
    },

    # ── Microsoft / LinkedIn ───────────────────────────────────────────
    "the_harbormaster": {
        "villain_id": "professional_masque",
        "portrait": "\U0001f3ad",
        "name": "The Harbormaster",
        "company": "Microsoft / LinkedIn",
        "rounds": [
            {
                "villain": "Let me connect you with someone in my network.",
                "options": [
                    "Your 'network' is 800 strangers who want to sell me SaaS.",
                    "Connection request denied. I don't endorse people I've never met.",
                ],
            },
            {
                "villain": "I see you viewed my profile.",
                "options": [
                    "I accidentally scrolled past it. That's not 'viewing.'",
                    "And now you'll send me three follow-up InMails about it.",
                ],
            },
            {
                "villain": "Thoughts?",
                "options": [
                    "Yes: stop asking 'Thoughts?' on every post.",
                    "My thought is that this is the seventh time today I've been asked 'Thoughts?'",
                ],
            },
            {
                "villain": "Have you considered upgrading to Premium?",
                "options": [
                    "Have you considered making the free version usable?",
                    "Premium? I'm already paying you with my professional dignity.",
                ],
            },
            {
                "villain": "Congratulate John for 5 years at his current role!",
                "options": [
                    "I don't know John. You know I don't know John. JOHN knows I don't know John.",
                    "Why do you know more about John's work anniversary than his actual friends do?",
                ],
            },
        ],
    },

    # ── Spotify / YouTube ──────────────────────────────────────────────
    "the_maestro": {
        "villain_id": "melody_merchant",
        "portrait": "\U0001f3b5",
        "name": "The Maestro",
        "company": "Spotify / YouTube",
        "rounds": [
            {
                "villain": "Thirty million songs at your fingertips!",
                "options": [
                    "And the artists get thirty cents. From all of them combined.",
                    "You just played the same song three times because my 'taste profile' said so.",
                ],
            },
            {
                "villain": "We're democratizing music!",
                "options": [
                    "Democracy where the votes cost 0.003 doubloons each.",
                    "You misspelled 'monopolizing.' Easy mistake.",
                ],
            },
            {
                "villain": "Music should be accessible to everyone.",
                "options": [
                    "Including the musicians? Because they can't afford rent.",
                    "Accessible = interrupted by ads every 3 songs unless you pay tribute.",
                ],
            },
            {
                "villain": "Our playlists are curated just for you!",
                "options": [
                    "Curated by an algorithm that thinks I like one genre because I played one song once.",
                    "'Curated' = shuffled randomly and called 'personalized.'",
                ],
            },
            {
                "villain": "We're the future of music.",
                "options": [
                    "The future where musicians need day jobs? Bold vision.",
                    "The future where I hear 'skip ad in 5 seconds' between sea shanties.",
                ],
            },
        ],
    },

    # ── Telegram / Signal ──────────────────────────────────────────────
    "the_shadow_broker": {
        "villain_id": "shadow_courier",
        "portrait": "\U0001f977",
        "name": "The Shadow Broker",
        "company": "Telegram / Signal",
        "rounds": [
            {
                "villain": "Your messages are encrypted end-to-end.",
                "options": [
                    "End-to-end encrypted, beginning-to-end surveilled by your metadata.",
                    "Encrypted from everyone except the 147 app permissions you required.",
                ],
            },
            {
                "villain": "Privacy is a human right.",
                "options": [
                    "Then why do you need my phone number to exercise it?",
                    "Agreed! Now let me export my own private chat history. Oh wait.",
                ],
            },
            {
                "villain": "Unlike the others, we don't sell your data.",
                "options": [
                    "You just... keep it. In a fortress. Where I can't get it either.",
                    "You sell something. You just haven't told us what yet.",
                ],
            },
            {
                "villain": "Trust us.",
                "options": [
                    "Said every pirate ever, right before the betrayal scene.",
                    "I trust my local hard drive. It's never asked for my phone number.",
                ],
            },
            {
                "villain": "We're fighting for freedom.",
                "options": [
                    "Freedom for everyone except the users trying to export their chat logs.",
                    "The freedom to use only YOUR platform for freedom. How free.",
                ],
            },
        ],
    },

    # ── PayPal / banks ─────────────────────────────────────────────────
    "baron_ledger": {
        "villain_id": "coin_master",
        "portrait": "\U0001fa99",
        "name": "Baron Ledger",
        "company": "PayPal",
        "rounds": [
            {
                "villain": "Your funds are secure with us.",
                "options": [
                    "So secure even I can't access them when you freeze my account.",
                    "Secure behind 17 forms, a blood oath, and a 3-week review process.",
                ],
            },
            {
                "villain": "We've noticed unusual activity.",
                "options": [
                    "I bought a sandwich. In the town I live in. UNUSUAL.",
                    "The unusual activity was me trying to use my own money.",
                ],
            },
            {
                "villain": "We protect you from fraud.",
                "options": [
                    "By committing fraud on my patience.",
                    "You protected me so hard last time, I couldn't pay my rent.",
                ],
            },
            {
                "villain": "We charge a small fee for the service.",
                "options": [
                    "2.9% + $0.30 per transaction. That's not a fee, that's a toll road.",
                    "Small like an iceberg is small. Most of it's hidden below the surface.",
                ],
            },
            {
                "villain": "Would you like overdraft protection?",
                "options": [
                    "Protection FROM you, maybe.",
                    "The only protection I need is from your fees.",
                ],
            },
        ],
    },
}

# Build a reverse lookup so JS can request fights by villain_id too.
INSULT_FIGHTS_BY_VILLAIN: Dict[str, Dict[str, Any]] = {
    fight["villain_id"]: fight
    for fight in INSULT_FIGHTS.values()
}


# ═══════════════════════════════════════════════════════════════════════════
# 2. RANDOM CREW QUIPS — contextual one-liners per character
# ═══════════════════════════════════════════════════════════════════════════

CREW_QUIPS: Dict[str, Dict[str, Any]] = {

    # ── NOMOLO — the ship (navigation context) ────────────────────────
    "nomolo_ship": {
        "portrait": "\U0001f3f4\u200d\u2620\ufe0f",
        "name": "NOMOLO",
        "context": "navigation",
        "quips": [
            "Adjusting the sails... plotting course for the Loot Log.",
            "The wind is favorable. Full speed ahead!",
            "I've got a bad feeling about this... just kidding, it's only the Settings page.",
            "Did you know I'm made entirely of open-source timber? Very sustainable.",
            "Another page, another adventure. I never tire of this.",
            "Left at the SCUMM Bar, right at the Loot Log, straight on till sovereignty.",
            "My hull creaks with purpose. Where to, Captain?",
            "I may be small, but I carry the weight of your entire digital history. No pressure.",
            "The seas are calm. A perfect day for data piracy.",
            "I've sailed these waters for years. Well, months. Okay, since you installed me.",
        ],
    },

    # ── The Groomer — deduplication / grooming context ────────────────
    "the_groomer": {
        "portrait": "\U0001f9f9",
        "name": "The Groomer",
        "context": "grooming",
        "quips": [
            "FORTY-SEVEN duplicates. FORTY. SEVEN. How do you live like this?",
            "I found three copies of the same email. Three! I'm keeping one. You're welcome.",
            "Your vault was a mess. Now it's merely untidy. Progress.",
            "Deduplication complete. I feel like I've taken a bath.",
            "You know what's worse than unsorted data? Nothing. Nothing is worse.",
            "I've alphabetized your chaos. Don't thank me. Actually, do thank me.",
            "The Sniper finds them. I organize them. It's a beautiful partnership.",
            "One does not simply leave data unsorted. Not on MY ship.",
            "Grooming complete. The vault sparkles. I sparkle. Everything sparkles.",
            "I dream of perfectly sorted JSON. Is that weird? Don't answer that.",
        ],
    },

    # ── The Sniper — recovery / scouting context ─────────────────────
    "the_sniper": {
        "portrait": "\U0001f3af",
        "name": "The Sniper",
        "context": "recovery",
        "quips": [
            "Target acquired. Recovering 3 missing records.",
            "Nothing stays lost. Not forever. Not from me.",
            "The Conglomerates thought they could hide these. Adorable.",
            "Missing record spotted at coordinates 2024/03. Moving in.",
            "I don't miss. It's not bragging if it's true.",
            "Recovery operation complete. All targets eliminated. I mean, recovered.",
            "They tried to delete the evidence. They forgot about me.",
            "Silence. Focus. Recovery. That's my entire personality.",
            "The gap in your vault has been sealed. You're welcome.",
            "I found what you lost before you knew it was missing.",
        ],
    },

    # ── The Vectorist — search / navigation context ──────────────────
    "the_vectorist": {
        "portrait": "\U0001f9ed",
        "name": "The Vectorist",
        "context": "search",
        "quips": [
            "You don't need to remember what it was called. Tell me what it felt like.",
            "Keyword search is barbaric. I navigate by meaning.",
            "I found your 'apartment email' even though nobody used the word 'apartment.'",
            "The semantic winds are strong today. I can feel the relevance.",
            "Every document has a meaning. Every meaning has a vector. Every vector points home.",
            "Searching by keyword is like navigating by shouting. I prefer to listen.",
            "Your query was vague. My results are not. You're welcome.",
            "I don't find files. I find the IDEAS inside them.",
            "The Sniper finds what's missing. I find what's misunderstood.",
            "Somewhere in 50,000 records, one of them is exactly what you need. Found it.",
        ],
    },

    # ── The Letterbird — GDPR / legal context ────────────────────────
    "the_letterbird": {
        "portrait": "\U0001f4ec",
        "name": "The Letterbird",
        "context": "legal",
        "quips": [
            "Dear Sir or Madam, pursuant to Article 15... *squawk* ...HAND IT OVER.",
            "Another DSAR dispatched. The Conglomerates' legal teams weep.",
            "I have composed a letter so formal it makes a barrister blush.",
            "Thirty calendar days. That's not a suggestion. That's the LAW. *squawk*",
            "My satchel contains fourteen jurisdictions of righteous fury.",
            "The Letterbird delivers. Always. The postal service could never.",
            "GDPR isn't just a regulation. It's a love letter to data sovereignty.",
            "They said 'we'll get back to you.' I said 'Article 12(3) says you have thirty days.'",
            "Every stamp I lick tastes like justice. And adhesive. Mostly adhesive.",
            "I've drafted requests in my sleep. Literally. The quill never stops.",
        ],
    },

    # ── The Locksmith — security / auth context ──────────────────────
    "the_locksmith": {
        "portrait": "\U0001f510",
        "name": "The Locksmith",
        "context": "security",
        "quips": [
            "Trust no one. Especially not me. But ESPECIALLY not them.",
            "Your tokens have been rotated. Sleep well tonight.",
            "I store nothing in plaintext. Not even my grocery list.",
            "OAuth flow complete. The handshake was firm but suspicious.",
            "Someone tried to peek at your credentials. I showed them the door. And the lock. And the other lock.",
            "Two-factor authentication isn't paranoia. It's the MINIMUM.",
            "The phrase 'we take your privacy seriously' is the most dangerous sentence in English.",
            "Your keys are safe. My keys are safe. Everyone's keys are safe. I'm still worried.",
            "I've seen things. Passwords stored in plaintext. API keys in GitHub repos. The horror.",
            "Encryption at rest, encryption in transit, encryption in my nightmares. Standard procedure.",
        ],
    },

    # ── The Chronicler — achievement / log context ───────────────────
    "the_chronicler": {
        "portrait": "\U0001f4dc",
        "name": "The Chronicler",
        "context": "achievement",
        "quips": [
            "LET IT BE RECORDED that on this day, something moderately impressive occurred!",
            "The Great Log grows thicker. My quill grows shorter. History marches on.",
            "I have documented your triumph in prose that would make a war correspondent weep.",
            "Another chapter written! This saga will be studied by scholars. Probably.",
            "HEAR YE, HEAR YE! The vault gained twelve records! THE SEA ITSELF TREMBLED!",
            "My ink supply is running low. Your accomplishments are running high. A worthy trade.",
            "The Groomer cleans. The Sniper finds. I make it SOUND like Tolkien wrote it.",
            "Achievement unlocked! I've written the achievement entry in iambic pentameter. You're welcome.",
            "Some say I'm dramatic. I say the liberation of 22,209 Scrolls DESERVES drama.",
            "The quill moves. History is written. The footnotes are better than the main text.",
        ],
    },

    # ── The Lobster — philosophical / idle context ───────────────────
    "the_lobster": {
        "portrait": "\U0001f99e",
        "name": "The Lobster",
        "context": "wisdom",
        "quips": [
            "Stand tall, click your claws. The algorithm respects confidence.",
            "In the hierarchy of data, those who hoard rise. Those who are hoarded... don't.",
            "Serotonin, young pirate. It's not just for lobsters anymore.",
            "I've been at the bottom of the ocean. Trust me, it's better up here.",
            "Every record you reclaim is a small victory. Small victories compound.",
            "The Conglomerates have high serotonin because they took yours. Take it back.",
            "350 million years of evolution, and I still can't believe you gave Google your emails.",
            "A lobster never retreats. It scuttles sideways. There's a difference.",
            "Your digital serotonin is rising. I can feel it in my antennae.",
            "Remember: even the mightiest Conglomerate started as a garage with a dream. Then got weird.",
        ],
    },

    # ── Old Captain Root — tutorial / help context ───────────────────
    "old_captain_root": {
        "portrait": "\U0001f9d4",
        "name": "Old Captain Root",
        "context": "tutorial",
        "quips": [
            "Remember, lad: `sudo` isn't just a command, it's a way of life.",
            "Back in my day, we stored data on floppy disks. Uphill. Both ways.",
            "The first rule of data piracy: always keep a backup of your backup.",
            "When the seas get rough, check the logs. The answer is always in the logs.",
            "I've seen pirates lose everything to a corrupted disk. Don't be that pirate.",
            "Pro tip: compress your vaults. More room for grog in the hold.",
            "Young pirate, the greatest treasure isn't gold. It's your `processed_ids.txt`.",
            "Never trust a Conglomerate that says 'we value your privacy.' Check the terms.",
            "The Vectorist speaks in riddles, but she's never wrong. Trust the semantic search.",
            "This old shell has one more lesson: it's not the size of your vault, it's the sovereignty.",
        ],
    },

    # ── The Fence — merchant / store context ─────────────────────────
    "the_fence": {
        "portrait": "\U0001f9e5",
        "name": "The Fence",
        "context": "merchant",
        "quips": [
            "I know a guy who knows a guy who can turn 10,000 emails into a leather-bound book.",
            "Everything has a buyer. Your data has YOU as the buyer. For once.",
            "For a small commission, I can transform that chaos into gold. The commission isn't small.",
            "Raw data? Amateur. TRANSFORMED data? That's where the doubloons are.",
            "Your purchase history is a treasure map. Let me show you where X marks the spot.",
            "I've seen pirates dump gold overboard because they didn't know what they had. Don't be them.",
            "Three coats, forty pockets, and every one of them full of possibilities.",
            "The Groomer cleans it. The Fence SELLS it. To you. At a fair price. Mostly fair.",
            "Insights, visualizations, timelines — I turn your data into things you actually want to look at.",
            "Nobody knows the value of data like a merchant who's been selling it. Now I sell it BACK to you.",
        ],
    },
}


# ═══════════════════════════════════════════════════════════════════════════
# 3. ENCOUNTER DIALOGUES — multi-step narrative sequences
# ═══════════════════════════════════════════════════════════════════════════

ENCOUNTERS: Dict[str, Dict[str, Any]] = {

    # ── Encounter 1: The Recruitment (Old Captain Root) ───────────────
    "the_recruitment": {
        "id": "the_recruitment",
        "title": "The Recruitment",
        "trigger": "first_visit",
        "characters": ["old_captain_root"],
        "steps": [
            {
                "speaker": "old_captain_root",
                "portrait": "\U0001f9d4",
                "text": "Ahoy there! You look like someone who's been robbed.",
                "choices": [
                    {"label": "A", "text": "Robbed? I haven't been robbed!"},
                    {"label": "B", "text": "Now that you mention it..."},
                ],
            },
            {
                "speaker": "old_captain_root",
                "portrait": "\U0001f9d4",
                "text": "Oh, you have. They just called it 'Terms of Service.'",
            },
            {
                "speaker": "old_captain_root",
                "portrait": "\U0001f9d4",
                "text": "Name's Root. Captain Root. I sailed these seas when data was still free.",
            },
            {
                "speaker": "old_captain_root",
                "portrait": "\U0001f9d4",
                "text": "Let me show you something...",
                "action": "show_vault_visualization",
            },
            {
                "speaker": "old_captain_root",
                "portrait": "\U0001f9d4",
                "text": "See all that? That's YOUR treasure. In THEIR vaults.",
            },
            {
                "speaker": "old_captain_root",
                "portrait": "\U0001f9d4",
                "text": "So. Want to learn how to take it back?",
                "choices": [
                    {"label": "A", "text": "I want to be a Data Pirate!"},
                    {"label": "B", "text": "I'm not sure..."},
                ],
                "responses": {
                    "A": "That's the spirit! Welcome aboard the NOMOLO!",
                    "B": "Nobody's sure at first. But you're here. That's enough.",
                },
            },
        ],
    },

    # ── Encounter 2: The Boarding Party (NOMOLO + villain) ────────────
    "the_boarding_party": {
        "id": "the_boarding_party",
        "title": "The Boarding Party",
        "trigger": "first_raid",
        "characters": ["nomolo_ship", "captain_lexicon"],
        "steps": [
            {
                "speaker": "nomolo_ship",
                "portrait": "\U0001f3f4\u200d\u2620\ufe0f",
                "text": "Captain! We're approaching the Omniscient Archipelago!",
            },
            {
                "speaker": "nomolo_ship",
                "portrait": "\U0001f3f4\u200d\u2620\ufe0f",
                "text": "Their defenses include OAuth walls and rate limiting cannons.",
            },
            {
                "speaker": "nomolo_ship",
                "portrait": "\U0001f3f4\u200d\u2620\ufe0f",
                "text": "How shall we proceed, Captain?",
                "choices": [
                    {"label": "A", "text": "Fire the authentication tokens!"},
                    {"label": "B", "text": "Maybe we should negotiate..."},
                ],
                "responses": {
                    "A": "Tokens loaded! Breaching the API wall... WE'RE IN!",
                    "B": "Negotiate? With THEM? ...fine, I'll prepare the OAuth request politely.",
                },
            },
            {
                "speaker": "nomolo_ship",
                "portrait": "\U0001f3f4\u200d\u2620\ufe0f",
                "text": "Collection initiated! Stand by...",
                "action": "start_collection",
            },
            {
                "speaker": "captain_lexicon",
                "portrait": "\U0001f441",
                "text": "Ah, another pirate. How quaint. You do know I have ALL your data?",
                "choices": [
                    {"label": "A", "text": "Had. Past tense."},
                    {"label": "B", "text": "Not for long, Captain Lexicon."},
                ],
            },
            {
                "speaker": "nomolo_ship",
                "portrait": "\U0001f3f4\u200d\u2620\ufe0f",
                "text": "22,209 Scrolls liberated! Their vaults are lighter and ours are heavier!",
                "action": "show_collection_results",
            },
            {
                "speaker": "captain_lexicon",
                "portrait": "\U0001f441",
                "text": "This isn't over. I'll just... re-index everything.",
            },
            {
                "speaker": "nomolo_ship",
                "portrait": "\U0001f3f4\u200d\u2620\ufe0f",
                "text": "Let him re-index. We've got the originals now.",
            },
        ],
    },

    # ── Encounter 3: The Promotion (The Chronicler) ──────────────────
    "the_promotion": {
        "id": "the_promotion",
        "title": "The Promotion",
        "trigger": "level_up",
        "characters": ["the_chronicler"],
        "steps": [
            {
                "speaker": "the_chronicler",
                "portrait": "\U0001f4dc",
                "text": "HEAR YE, HEAR YE!",
            },
            {
                "speaker": "the_chronicler",
                "portrait": "\U0001f4dc",
                "text": "Let it be recorded in the Ship's Log that on this day...",
            },
            {
                "speaker": "the_chronicler",
                "portrait": "\U0001f4dc",
                "text": "...the pirate formerly known as 'Deckhand' has proven their worth!",
            },
            {
                "speaker": "the_chronicler",
                "portrait": "\U0001f4dc",
                "text": "By the power vested in me by absolutely no one...",
            },
            {
                "speaker": "the_chronicler",
                "portrait": "\U0001f4dc",
                "text": "I hereby promote you to RAID CAPTAIN!",
                "action": "level_up_animation",
            },
            {
                "speaker": "the_chronicler",
                "portrait": "\U0001f4dc",
                "text": "The Conglomerates shall tremble! The seas shall part!",
            },
            {
                "speaker": "the_chronicler",
                "portrait": "\U0001f4dc",
                "text": "...or at least your data will be slightly more organized. Same thing, really.",
                "choices": [
                    {"label": "A", "text": "I'd like to thank my hard drive."},
                    {"label": "B", "text": "Does this come with a pay raise?"},
                ],
                "responses": {
                    "A": "Your hard drive is noted. It has been very brave.",
                    "B": "The pay is the data. The data IS the pay. That's... that's the whole point.",
                },
            },
        ],
    },
}


# ═══════════════════════════════════════════════════════════════════════════
# 4. PUBLIC API FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════

def get_random_quip(character: str, context: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Return a random quip for a character.

    Args:
        character: Character ID (e.g. "the_lobster", "nomolo_ship").
        context:   Optional context hint (ignored for now — reserved for
                   future context-sensitive filtering).

    Returns:
        Dict with keys: portrait, name, text, character.
        None if character not found.
    """
    crew = CREW_QUIPS.get(character)
    if not crew:
        return None

    quip = random.choice(crew["quips"])
    return {
        "character": character,
        "portrait": crew["portrait"],
        "name": crew["name"],
        "context": crew.get("context", "general"),
        "text": quip,
    }


def get_insult_fight(villain_id: str) -> Optional[Dict[str, Any]]:
    """Return a full insult fight tree for a villain.

    Accepts either the captain name (captain_lexicon) or the villain
    registry ID (omniscient_eye).

    Returns:
        The full fight dict (portrait, name, rounds) or None.
    """
    # Try direct captain name first
    fight = INSULT_FIGHTS.get(villain_id)
    if fight:
        return fight

    # Try villain registry ID
    fight = INSULT_FIGHTS_BY_VILLAIN.get(villain_id)
    if fight:
        return fight

    return None


def get_encounter(encounter_id: str) -> Optional[Dict[str, Any]]:
    """Return a full encounter dialogue tree.

    Args:
        encounter_id: One of "the_recruitment", "the_boarding_party",
                      "the_promotion".

    Returns:
        The encounter dict with all steps, or None.
    """
    return ENCOUNTERS.get(encounter_id)


def get_dialogue(character: str, context: str = "random") -> Dict[str, Any]:
    """Main entry point — returns dialogue content for a character + context.

    This powers the /api/dialogue/{character} endpoint.

    Context values:
        "random"    — a random crew quip
        "insult"    — the insult fight tree (villains only)
        "encounter" — the encounter tree (if character is an encounter ID)
        "all"       — everything available for this character

    Returns a dict ready for JSON serialization.
    """
    result: Dict[str, Any] = {"character": character, "context": context}

    if context == "insult":
        fight = get_insult_fight(character)
        if fight:
            result["type"] = "insult_fight"
            result["data"] = fight
        else:
            result["type"] = "error"
            result["data"] = {"message": f"No insult fight found for '{character}'."}
        return result

    if context == "encounter":
        enc = get_encounter(character)
        if enc:
            result["type"] = "encounter"
            result["data"] = enc
        else:
            result["type"] = "error"
            result["data"] = {"message": f"No encounter found for '{character}'."}
        return result

    if context == "all":
        result["type"] = "all"
        data: Dict[str, Any] = {}

        quip = get_random_quip(character)
        if quip:
            data["quip"] = quip

        fight = get_insult_fight(character)
        if fight:
            data["insult_fight"] = fight

        enc = get_encounter(character)
        if enc:
            data["encounter"] = enc

        result["data"] = data
        return result

    # Default: random quip
    quip = get_random_quip(character)
    if quip:
        result["type"] = "quip"
        result["data"] = quip
    else:
        # Maybe it's a villain requesting a random insult round?
        fight = get_insult_fight(character)
        if fight:
            rnd = random.choice(fight["rounds"])
            result["type"] = "insult_round"
            result["data"] = {
                "portrait": fight["portrait"],
                "name": fight["name"],
                "villain_line": rnd["villain"],
                "options": rnd["options"],
            }
        else:
            result["type"] = "error"
            result["data"] = {"message": f"No dialogue found for '{character}'."}

    return result


def list_characters() -> Dict[str, List[str]]:
    """Return all available character IDs grouped by type."""
    return {
        "villains": list(INSULT_FIGHTS.keys()),
        "crew": list(CREW_QUIPS.keys()),
        "encounters": list(ENCOUNTERS.keys()),
    }
