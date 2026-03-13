"""
THE DIALOGUE LIBRARY OF THE FLATCLOUD

Monkey Island-style conversations between Nomolo and every character in the
Flatcloud universe. Insult fights, crew quips, multi-step encounters,
villain riddles — all served from a single canonical source of truth.

Humor philosophy:
  - The comedy comes from the GAP between tech reality and pirate metaphor.
  - Reference specific, real-world tech absurdities (The Professional Port's endorsements,
    The Omniscient Eye killing products, The Hydra rebranding, The Coin Counter freezing accounts).
  - Keep it affectionate — laughing WITH the absurdity, not being mean.
  - All insult fights: both player options always "win." It's entertainment.
  - Riddles teach real facts — fun and educational, never preachy.
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

    # ── The Omniscient Eye ─────────────────────────────────────────────
    "captain_lexicon": {
        "villain_id": "omniscient_eye",
        "portrait": "\U0001f441",
        "name": "Captain Lexicon",
        "company": "The Omniscient Eye",
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

    # ── The Walled Garden ──────────────────────────────────────────────
    "admiral_polished": {
        "villain_id": "walled_garden",
        "portrait": "\U0001f3f0",
        "name": "Admiral Polished",
        "company": "The Walled Garden",
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

    # ── The Hydra of Faces ──────────────────────────────────────────────
    "captain_pivot": {
        "villain_id": "hydra_of_faces",
        "portrait": "\U0001f409",
        "name": "Captain Pivot",
        "company": "The Hydra of Faces",
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
                "villain": "Our harbor gives everyone a voice.",
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

    # ── The Merchant Fleet ─────────────────────────────────────────────
    "commodore_prime": {
        "villain_id": "bazaar_eternal",
        "portrait": "\U0001f3ea",
        "name": "Commodore Prime",
        "company": "The Merchant Fleet",
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

    # ── The Professional Port ──────────────
    "the_harbormaster": {
        "villain_id": "professional_masque",
        "portrait": "\U0001f3ad",
        "name": "The Harbormaster",
        "company": "The Professional Port",
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
                    "And now you'll send me three follow-up harbor dispatches about it.",
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

    # ── The Bard's Guild ──────────────────────────────────────────────
    "the_maestro": {
        "villain_id": "melody_merchant",
        "portrait": "\U0001f3b5",
        "name": "The Maestro",
        "company": "The Bard's Guild",
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

    # ── The Shadow Courier ──────────────────────────────────────────────
    "the_shadow_broker": {
        "villain_id": "shadow_courier",
        "portrait": "\U0001f977",
        "name": "The Shadow Broker",
        "company": "The Shadow Courier",
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
                    "The freedom to use only YOUR harbor for freedom. How free.",
                ],
            },
        ],
    },

    # ── The Coin Counter ──────────────────────────────────────────────
    "baron_ledger": {
        "villain_id": "coin_master",
        "portrait": "\U0001fa99",
        "name": "Baron Ledger",
        "company": "The Coin Counter",
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
# 1b. VILLAIN RIDDLES — educational data-sovereignty facts in pirate metaphor
#
# Each villain has 5-6 riddles. Every riddle teaches a real fact about data
# privacy, platform economics, or digital rights — but expressed entirely
# through the Flatcloud universe. Zero real company names. The metaphor
# IS the education.
# ═══════════════════════════════════════════════════════════════════════════

VILLAIN_RIDDLES: Dict[str, Dict[str, Any]] = {

    # ── The Omniscient Eye ──────────────────────────────────────────────
    "omniscient_eye": {
        "villain_name": "Captain Lexicon",
        "portrait": "\U0001f441",
        "company": "The Omniscient Eye",
        "intro": "Captain Lexicon adjusts his telescope and smirks. 'Think you know the seas, pirate? Answer me this...'",
        "riddles": [
            {
                "question": "How many pieces of treasure does the Omniscient Eye plunder from each sailor, every single day, without them even noticing?",
                "options": ["About 100", "About 1,000", "About 10,000", "About 100,000"],
                "correct": 2,
                "explanation": "The Omniscient Eye collects roughly 10,000 data points per sailor per day — through searches, location tracking, the Bard's Stage, and more. That's a LOT of scrolls, Captain.",
                "villain_right": "Impressive! You know your enemy well. That won't save your data, though.",
                "villain_wrong": "Ha! You don't even know how much we've taken. This will be easy.",
            },
            {
                "question": "How many ships has the Omniscient Eye scuttled over the years — vessels it built, launched, and then sank without warning?",
                "options": ["About 50", "About 100", "Over 250", "Over 500"],
                "correct": 2,
                "explanation": "The Omniscient Eye has scuttled over 250 of its own vessels — check the Scuttled Ships Registry for the full graveyard. Reader, Inbox, Hangouts, Stadia... the list goes on and on.",
                "villain_right": "You've been reading the obituaries, I see. Each one was a... strategic realignment.",
                "villain_wrong": "You underestimate our capacity for creative destruction! We've buried entire fleets!",
            },
            {
                "question": "What does the Pirate's Code — the great treaty that protects sailors' treasure — actually stand for in the old tongue?",
                "options": [
                    "Great Digital Privacy Rules",
                    "General Data Protection Regulation",
                    "Global Digital Privacy Rights",
                    "Government Data Processing Requirements",
                ],
                "correct": 1,
                "explanation": "The General Data Protection Regulation is the EU law that gives ye the right to access, delete, and port yer data. It's the Pirate's Code of the digital age — the Data Protection Treaty itself.",
                "villain_right": "So you know the law. Knowing it and enforcing it are different things, pirate.",
                "villain_wrong": "You don't even know the name of the treaty protecting you? How delightful for us.",
            },
            {
                "question": "How many of the world's scrolls pass through the Scroll Archives of the Omniscient Eye?",
                "options": ["About 10%", "About 20%", "About 30%", "About 50%"],
                "correct": 2,
                "explanation": "The Scroll Archives handle roughly 30% of all scrolls worldwide — nearly 2 billion sailors use them. That's a LOT of scrolls in one vault.",
                "villain_right": "You've done your research. But knowing the size of my treasury doesn't shrink it.",
                "villain_wrong": "It's even more than you thought! Every scroll passes through my hands.",
            },
            {
                "question": "What year did the Omniscient Eye quietly remove 'Don't Be Evil' from its code of conduct?",
                "options": ["2014", "2016", "2018", "They never removed it"],
                "correct": 2,
                "explanation": "In 2018, the Omniscient Eye quietly removed 'Don't Be Evil' as the opening of its code of conduct. It's now buried deep in the document. Subtle, Captain. Very subtle.",
                "villain_right": "You noticed that, did you? It was a... typographical reorganization.",
                "villain_wrong": "You thought we'd keep that promise forever? How charmingly naive.",
            },
            {
                "question": "What percentage of the top ports and harbors have the Omniscient Eye's Spyglass installed, tracking every sailor who passes through?",
                "options": ["About 40%", "About 55%", "About 70%", "About 85%"],
                "correct": 3,
                "explanation": "The Omniscient Eye's Spyglass is installed on roughly 85% of the top harbors. That means the Eye sees where you go across almost the entire sea. The ultimate surveillance network.",
                "villain_right": "You understand the reach of my fleet. Most pirates don't even notice the trackers.",
                "villain_wrong": "Even more than you guessed! My lighthouses are on nearly every shore.",
            },
        ],
    },

    # ── The Walled Garden ───────────────────────────────────────────────
    "walled_garden": {
        "villain_name": "Admiral Polished",
        "portrait": "\U0001f3f0",
        "company": "The Walled Garden",
        "intro": "Admiral Polished polishes an already gleaming apple. 'You think you understand my fortress? Prove it.'",
        "riddles": [
            {
                "question": "The Walled Garden has removed wares from the Fortress Marketplace for privacy violations. But it's also removed wares that:",
                "options": [
                    "Competed with the Fortress's own wares",
                    "Were crafted in unapproved languages",
                    "Had ugly figureheads",
                    "Were too popular",
                ],
                "correct": 0,
                "explanation": "The Walled Garden has a history of removing or rejecting wares that compete with its own services — screen time trackers, scroll readers, and more. The Fortress has very selective gates.",
                "villain_right": "You see through the polish. But understanding the garden doesn't get you out of it.",
                "villain_wrong": "How innocent. The Fortress Marketplace is curated for YOUR benefit. Always. Only. Exclusively. Yours.",
            },
            {
                "question": "What percentage of Fortress Marketplace revenue does the Walled Garden take from craftsmen?",
                "options": ["10%", "15%", "20%", "30%"],
                "correct": 3,
                "explanation": "The Walled Garden takes a 30% cut of all Fortress Marketplace purchases and in-app payments — the infamous 'Fortress Tax.' Small craftsmen (under $1M) get a reduced 15% rate.",
                "villain_right": "You know the toll. It's not a tax, it's a... service fee. For the privilege of my ecosystem.",
                "villain_wrong": "It's thirty doubloons per hundred! And they call ME the pirate. Wait, I am the pirate.",
            },
            {
                "question": "What is 'right to repair'?",
                "options": [
                    "The Walled Garden's warranty extension program",
                    "The right to fix your own vessels with third-party parts",
                    "A subscription service for vessel maintenance",
                    "A legal term for hull updates",
                ],
                "correct": 1,
                "explanation": "Right to repair is the movement for yer legal right to fix yer own devices. The Walled Garden has historically fought against it, using proprietary screws, serialized parts, and voided warranties.",
                "villain_right": "You understand the chains. But knowing about them and breaking them are different things.",
                "villain_wrong": "You think I'd let just ANYONE open my beautiful devices? With their... fingers?",
            },
            {
                "question": "How far back do the Fortress's whisper archives go on the average device?",
                "options": [
                    "Last 6 months",
                    "Last 2 years",
                    "Last 5 years",
                    "All of it, since day one",
                ],
                "correct": 3,
                "explanation": "The Fortress stores yer entire conversation history by default — every text, memory shard, and link since ye first swore allegiance. Years of scrolls, in one vault.",
                "villain_right": "You've checked your own storage. Smart. Most people don't even look.",
                "villain_wrong": "We keep EVERYTHING, darling. Every message, every emoji, every 'haha.' All of it.",
            },
            {
                "question": "What happens to yer treasure if yer Fortress key gets locked or disabled?",
                "options": [
                    "You can still access it via the Fortress gate",
                    "The Walled Garden sends you a backup crate",
                    "You lose access to everything — memory shards, whispers, purchases",
                    "Nothing changes, it's all on your device",
                ],
                "correct": 2,
                "explanation": "If yer Fortress key is locked or disabled, ye lose access to all cloud-stored memory shards, whispers, app purchases, echoes — everything tied to that key. One key to rule them all, and if it's lost...",
                "villain_right": "You understand the architecture. The golden cage is real. But it's SO comfortable inside.",
                "villain_wrong": "You thought there was a safety net? The fortress protects YOU from the outside. Not the other way around.",
            },
            {
                "question": "In what format does the Walled Garden let ye export yer treasure?",
                "options": [
                    "Only parchment scrolls — unstructured and useless",
                    "A proprietary Fortress format",
                    "JSON and CSV — actually quite good, but buried in the deepest vault",
                    "You can't export your treasure at all",
                ],
                "correct": 2,
                "explanation": "Credit where due: the Walled Garden's data export is actually decent — JSON and CSV files. But it's buried deep in the Fortress privacy vault and takes days to process. The treasure is there, but the map is hidden.",
                "villain_right": "You've found the secret passage. I'm almost impressed. Almost.",
                "villain_wrong": "We DO let you export! We just... don't make it easy to find. It's a feature.",
            },
        ],
    },

    # ── The Hydra of Faces ──────────────────────────────────────────────
    "hydra_of_faces": {
        "villain_name": "Captain Pivot",
        "portrait": "\U0001f409",
        "company": "The Hydra of Faces",
        "intro": "Captain Pivot adjusts his mask for the third time today. 'Let's see if you can keep up with my pivots...'",
        "riddles": [
            {
                "question": "How many times has the Hydra of Faces shed its skin and taken a new name?",
                "options": ["Once", "Twice", "Three times", "It's always had the same name"],
                "correct": 1,
                "explanation": "The Hydra has renamed twice: first from 'thefacebook' to its more famous name (2005), then again in 2021 after a massive whistleblower scandal. Coincidence? Never.",
                "villain_right": "You've been paying attention. Most people don't even notice when we change the sails.",
                "villain_wrong": "We've pivoted more times than you think! Or fewer. Depends on the narrative.",
            },
            {
                "question": "How many sailor records were exposed in the Great Logbook Scandal — the Hydra's most infamous data breach?",
                "options": ["About 5 million", "About 30 million", "About 87 million", "About 200 million"],
                "correct": 2,
                "explanation": "Approximately 87 million sailor records from the Reef were improperly shared during the Great Logbook Scandal for political ad targeting. The biggest data heist that wasn't technically called a heist.",
                "villain_right": "You remember that. We were hoping everyone would forget. They mostly did.",
                "villain_wrong": "It was 87 million. MILLION. And they still didn't leave. Engagement is a powerful thing.",
            },
            {
                "question": "What percentage of the Hydra of Faces' revenue comes from advertising?",
                "options": ["About 60%", "About 75%", "About 85%", "About 97%"],
                "correct": 3,
                "explanation": "Roughly 97% of the Hydra's revenue comes from advertising. The 'free' social reef is actually the world's most sophisticated ad delivery system.",
                "villain_right": "You see through the community theater to the actual business. How uncomfortable.",
                "villain_wrong": "Almost ALL of it is ads, pirate. We're not a social reef. We're an ad harbor with social features.",
            },
            {
                "question": "How many categories of treasure does the Hydra of Faces track about each sailor on the Reef?",
                "options": ["About 500", "About 5,000", "About 20,000", "Over 52,000"],
                "correct": 3,
                "explanation": "The Hydra tracks over 52,000 data categories per sailor — from yer clicks and hovers to yer shopping habits and political leanings. They know ye better than yer therapist.",
                "villain_right": "You've read the fine print. Nobody reads the fine print. I'm... unsettled.",
                "villain_wrong": "FIFTY-TWO THOUSAND categories. And those are just the ones we admit to.",
            },
            {
                "question": "What year was the Hydra's infamous 'poke' — a mysterious gesture that nobody has ever understood — introduced on the Reef?",
                "options": ["2004", "2006", "2008", "2010"],
                "correct": 0,
                "explanation": "The 'poke' was introduced in 2004, at the Reef's launch. And yes, it's STILL there. Nobody knows what it means. Nobody has ever known. It endures beyond all reason.",
                "villain_right": "A true historian! The poke outlived entire Armada fleets and basic human dignity.",
                "villain_wrong": "2004! The poke is older than most of our sailors. It's our cockroach feature — unkillable.",
            },
            {
                "question": "If you plunder and download yer own data from the Hydra's Reef, how heavy is the typical haul?",
                "options": ["A few megabytes", "About 100 MB", "Multiple gigabytes", "Over 50 GB"],
                "correct": 2,
                "explanation": "The average data download from the Reef is multiple gigabytes — years of memory shards, whispers, likes, ad clicks, and location history. Most sailors are shocked by how heavy the anchor is.",
                "villain_right": "You've weighed the anchor. It IS heavy, isn't it? All those memories... and metadata.",
                "villain_wrong": "It's GIGABYTES, pirate. Years of your life, compressed into a crate you'll probably never open.",
            },
        ],
    },

    # ── The Merchant Fleet ─────────────────────────────────────────────
    "bazaar_eternal": {
        "villain_name": "Commodore Prime",
        "portrait": "\U0001f3ea",
        "company": "The Merchant Fleet",
        "intro": "Commodore Prime checks his delivery manifest. 'I know everything you've ever wanted. Let's see if you know anything about me.'",
        "riddles": [
            {
                "question": "How many voice recordings does the Merchant Fleet's Listening Parrot store per sailor?",
                "options": [
                    "Only the last 100",
                    "Only commands, not conversations",
                    "All of them, unless you manually delete",
                    "None — it's all processed locally",
                ],
                "correct": 2,
                "explanation": "The Merchant Fleet stores ALL Listening Parrot voice recordings by default — every wake word, every misfire, every background conversation it picked up. Ye have to manually delete them in the app.",
                "villain_right": "You've checked your voice history. Smart. Most people don't even know it exists.",
                "villain_wrong": "ALL of them, pirate. Every whisper, every accidental activation. My ears never close.",
            },
            {
                "question": "What does the Merchant Fleet do with yer browsing history even when ye don't buy anything?",
                "options": [
                    "Deletes it after 24 hours",
                    "Uses it for recommendations and ad targeting",
                    "Shares it only with the seller",
                    "Nothing — browsing is anonymous",
                ],
                "correct": 1,
                "explanation": "The Merchant Fleet uses yer browsing history for product recommendations, ad targeting across the seas, and building yer consumer profile — even for items ye never purchased.",
                "villain_right": "You understand the marketplace. Every glance at a product is a data point in my ledger.",
                "villain_wrong": "Just LOOKING at something tells me what you want. And what to show you next. And next. And next.",
            },
            {
                "question": "How many treasure crates does the Merchant Fleet deliver per day in the US alone?",
                "options": ["About 100,000", "About 500,000", "About 1.6 million", "About 5 million"],
                "correct": 2,
                "explanation": "The Merchant Fleet delivers approximately 1.6 million packages per day in the US alone. That's a lot of treasure chests making their way to doorsteps.",
                "villain_right": "You know my fleet's capacity. Impressive logistics, isn't it? Now imagine the data from each one.",
                "villain_wrong": "1.6 MILLION per day! And each one generates purchase data, delivery data, and return data. It never stops.",
            },
            {
                "question": "What is the Merchant Fleet's infamous '1-click patent' about?",
                "options": [
                    "A one-click ship builder",
                    "They literally patented buying something with one click",
                    "A one-click returns process",
                    "An instant delivery guarantee",
                ],
                "correct": 1,
                "explanation": "The Merchant Fleet literally patented the concept of buying something with a single click in 1999. They enforced it against competitors for years. They patented a BUTTON PRESS.",
                "villain_right": "You know about our most audacious claim. One click, patented. Innovation? Or piracy? Both.",
                "villain_wrong": "We PATENTED clicking a button! One click! The most pirate thing an Armada fleet has ever done.",
            },
            {
                "question": "How much data does the Merchant Fleet's Listening Parrot send home per day?",
                "options": [
                    "Almost nothing — it processes locally",
                    "A few kilobytes of command logs",
                    "Hundreds of megabytes of audio and metadata",
                    "It's always listening and streaming constantly",
                ],
                "correct": 2,
                "explanation": "The Listening Parrot sends hundreds of megabytes of data to the Merchant Fleet daily — voice recordings, usage patterns, smart home data, and ambient noise analysis. It's always listening for its wake word, which means it's always listening.",
                "villain_right": "You've monitored the monitors. The Parrot hears all, reports all. It's quite efficient.",
                "villain_wrong": "It sends MUCH more than you thought. The convenience comes at a cost — your audio diary.",
            },
            {
                "question": "How long does the Merchant Fleet keep yer purchase history?",
                "options": [
                    "2 years",
                    "5 years",
                    "10 years",
                    "Forever — there's no auto-delete",
                ],
                "correct": 3,
                "explanation": "The Merchant Fleet keeps yer purchase history forever. There is no auto-delete feature. Yer first order from 2003? Still there. Every impulse buy, every 3 AM purchase. Eternal.",
                "villain_right": "You've scrolled to the very bottom. It IS infinite, isn't it? Every purchase, immortalized.",
                "villain_wrong": "FOREVER, pirate. I never forget a transaction. Not one. Your first order is still in my books.",
            },
        ],
    },

    # ── The Professional Port ──────────────
    "professional_masque": {
        "villain_name": "The Harbormaster",
        "portrait": "\U0001f3ad",
        "company": "The Professional Port",
        "intro": "The Harbormaster straightens his tie and opens his ledger. 'I have yer professional record right here. Let's see what ye know about mine.'",
        "riddles": [
            {
                "question": "How many data points does The Professional Port collect per sailor's profile?",
                "options": ["About a dozen", "About 50", "About 200", "Hundreds, including behavioral patterns"],
                "correct": 3,
                "explanation": "The Professional Port collects hundreds of data points per profile — not just what you enter, but who viewed you, time spent reading posts, scroll depth, message response times, and more.",
                "villain_right": "Ye know the depth of the harbor charts. Most sailors think it's just their crew papers.",
                "villain_wrong": "HUNDREDS, pirate. I know how long ye hovered over that voyage posting. To the millisecond.",
            },
            {
                "question": "What is the Glass Panes Telemetry — the Harbormaster's hidden surveillance rigging?",
                "options": [
                    "A diagnostic tool you run manually",
                    "The Professional Port collecting usage data from your PC, enabled by default",
                    "A feature for enterprise fleet commanders only",
                    "A Glass Panes gaming port",
                ],
                "correct": 1,
                "explanation": "Glass Panes Telemetry is the Harbormaster's system for collecting usage data from yer vessel — enabled by default on every installation. It tracks app usage, hardware data, browsing patterns, and more.",
                "villain_right": "You've found the hidden rigging. Most sailors never check their Telemetry settings.",
                "villain_wrong": "It's ON by default, pirate. Every Glass Panes vessel is sending dispatches back to port.",
            },
            {
                "question": "How many unsolicited harbor messages does the average Professional Port sailor receive from recruiters per year?",
                "options": ["About 5", "About 20", "About 50", "Too many to count"],
                "correct": 3,
                "explanation": "The average Professional Port sailor with a decent profile gets bombarded with recruiter scrolls — some report dozens per month. The 'exciting voyage' that's always 'perfect for yer background.'",
                "villain_right": "Ye've felt the barrage. Each one be a data-driven cannonball aimed at yer professional vanity.",
                "villain_wrong": "TOO MANY. Even I've lost count. But each one tells me more about what makes sailors click.",
            },
            {
                "question": "What did the Harbormaster do with private voice conversations in 2019?",
                "options": [
                    "Encrypted them end-to-end",
                    "Used human clerks to listen to 'em without anyone knowin'",
                    "Deleted them all for privacy",
                    "Made them all public by accident",
                ],
                "correct": 1,
                "explanation": "In 2019, it was revealed that the Harbormaster used human contractors to listen to private call recordings — without sailors' knowledge. The contractors heard personal conversations, voice calls, and more.",
                "villain_right": "You remember the scandal. We prefer to call it a 'quality assurance program.'",
                "villain_wrong": "HUMAN CLERKS listened to private calls. For 'quality.' Without tellin' anyone. Oops.",
            },
            {
                "question": "How much would your Professional Port data be worth if sold on the open market?",
                "options": ["About $5", "About $10-20", "About $50-100", "About $500+"],
                "correct": 2,
                "explanation": "A detailed Professional Port profile is estimated to be worth $50-100 on data markets — your job history, skills, connections, and behavioral patterns. Your professional identity has a price tag.",
                "villain_right": "Ye know yer own value. Most sailors give it away for free in exchange for voyage alerts.",
                "villain_wrong": "Fifty to a hundred doubloons PER DOSSIER. And ye post it all voluntarily. Beautiful.",
            },
            {
                "question": "What happens to your Professional Port profile if ye perish at sea?",
                "options": [
                    "It's struck from the records after 6 months",
                    "Next of kin is contacted to manage it",
                    "It stays up, and still gets 'voyage anniversary' congratulations",
                    "It's converted to an 'In Memoriam' flag",
                ],
                "correct": 2,
                "explanation": "Dead Professional Port profiles stay active — they can still get 'work anniversary' notifications, endorsement requests, and recruiter scrolls. Even death doesn't stop the algorithm.",
                "villain_right": "Ye've seen the ghost ships. The algorithm doesn't check for a pulse before sendin' congratulations.",
                "villain_wrong": "It stays UP. 'Congratulate Barnacle Bill for 10 years at his post!' Bill's been at the bottom of the sea for three. The algorithm sails on.",
            },
        ],
    },

    # ── The Bard's Guild ──────────────────────────────────────────────
    "melody_merchant": {
        "villain_name": "The Maestro",
        "portrait": "\U0001f3b5",
        "company": "The Bard's Guild",
        "intro": "The Maestro taps his baton against the railing. 'Let's see if ye can stay on beat with these questions...'",
        "riddles": [
            {
                "question": "How much does The Bard's Guild pay its minstrels per shanty played?",
                "options": ["A whole penny", "Three-tenths of a penny", "A silver piece", "Half a doubloon"],
                "correct": 1,
                "explanation": "The Bard's Guild pays minstrels roughly three-tenths of a copper piece per shanty played. A single shanty needs about 250,000 plays before the bard who wrote it earns enough for a month's grog money. The treasure flows to the Maestro's palace, not the musicians' pockets. He calls it 'democratizing music.' The bards call it something unprintable.",
                "villain_right": "You know the ugly math. Three-tenths of a penny per play. But hey, exposure!",
                "villain_wrong": "Even LESS than you thought! Three-tenths of a penny. A sea shanty needs a quarter million plays to buy grog.",
            },
            {
                "question": "How many hours of performances are uploaded to The Bard's Guild's stage every minute?",
                "options": ["About 50 hours", "About 100 hours", "About 300 hours", "About 500 hours"],
                "correct": 3,
                "explanation": "Over 500 hours of performances are uploaded to the Bard's Guild's grand stage every single minute. That's more than 30,000 hours per hour. You could never experience it all — you'd need over 82 years to watch just ONE day's uploads. The stage is infinite, and every performance generates data for the Maestro.",
                "villain_right": "You've measured the ocean. It IS infinite, practically. And every second generates data.",
                "villain_wrong": "FIVE HUNDRED hours per minute! My stage is infinite and always growing.",
            },
            {
                "question": "What percentage of The Bard's Guild's entire music catalog has NEVER been played — not even once?",
                "options": ["About 2%", "About 5%", "About 10%", "About 20%"],
                "correct": 3,
                "explanation": "Roughly 20% of the Bard's Guild catalog — millions of echoes — has never been played by anyone, ever. Ghost ships on a silent sea. Someone recorded them, uploaded them, and nobody pressed play.",
                "villain_right": "You know about the silent fleet. Millions of shanties, zero plays. The loneliest data in me waters.",
                "villain_wrong": "TWENTY PERCENT! Millions of shanties nobody has heard. I have more ghosts than a haunted lighthouse.",
            },
            {
                "question": "What can yer Bard's Guild listening history reveal about ye?",
                "options": [
                    "Just yer taste in shanties",
                    "Yer mood patterns, sleep schedule, and political leanings",
                    "Only what genres ye fancy",
                    "Nothin' — it be anonymized",
                ],
                "correct": 1,
                "explanation": "Yer Bard's Guild listening history can reveal yer mood patterns (sad songs at night?), sleep schedule (when music stops), exercise habits, and even political leanings from podcast choices. Your playlist IS your diary.",
                "villain_right": "You understand the melody beneath the melody. Every play is a data point about your soul.",
                "villain_wrong": "It reveals EVERYTHING. When you're sad, when you're awake, when you're angry. Your playlist is a mood ring.",
            },
            {
                "question": "How long would it take to watch ALL of the Bard's Stage content?",
                "options": ["About 10 years", "About 30 years", "Over 82 years", "Over 500 years"],
                "correct": 2,
                "explanation": "Watching all of the Bard's Stage would take over 82 years of non-stop, 24/7 viewing. And by the time you finished, there'd be centuries more. The infinite stage never stops growing.",
                "villain_right": "You've calculated the impossible voyage. Even the most dedicated pirate couldn't sail these waters.",
                "villain_wrong": "Over EIGHTY-TWO YEARS! And that's just today's content. Tomorrow there'll be more.",
            },
        ],
    },

    # ── The Shadow Courier ──────────────────────────────────────────────
    "shadow_courier": {
        "villain_name": "The Shadow Broker",
        "portrait": "\U0001f977",
        "company": "The Shadow Courier",
        "intro": "The Shadow Broker steps from the darkness, face obscured. 'Ye think ye understand encryption? Let's test that theory.'",
        "riddles": [
            {
                "question": "When the Shadow Broker says your messages are sealed in bottles that only the sender and receiver can open, what sorcery is this called?",
                "options": [
                    "The bottles are sealed at the Broker's fortress — the Broker holds the key",
                    "Only the sender and receiver hold the keys — not even the Shadow Broker can peek inside",
                    "The bottles are sealed but the Broker keeps a spare key — just in case",
                    "The bottles aren't truly sealed — port authorities can open them with a warrant",
                ],
                "correct": 1,
                "explanation": "True end-to-end encryption means only the sender and receiver hold the cipher keys. Not the Shadow Broker, not the port authorities, not even the cleverest Armada captain. The message is a sealed bottle that only two people can open. It's genuine sorcery — one of the few where the promise matches the mathematics.",
                "villain_right": "You understand the lock. But understanding the lock doesn't tell you who else might have a key.",
                "villain_wrong": "Only sender and receiver! That's the whole point. Or at least, that's what we TELL you.",
            },
            {
                "question": "Are the Shadow Courier's messages encrypted by default?",
                "options": [
                    "Aye, all messages be fully sealed",
                    "Nay — only 'Secret Dispatches' are truly sealed end-to-end",
                    "Aye, but only on mobile vessels",
                    "No sealin' at all — it be open-source",
                ],
                "correct": 1,
                "explanation": "Regular Shadow Courier messages are NOT end-to-end encrypted — only 'Secret Dispatches' are. Normal messages are encrypted in transit but the Shadow Courier holds the keys. Most users don't know this.",
                "villain_right": "You've read the fine print on my scrolls. Most pirates assume all messages are sealed. They're not.",
                "villain_wrong": "Only SECRET DISPATCHES are truly sealed! Regular messages? I can read every one. Surprise!",
            },
            {
                "question": "What can encrypted courier services still see, even when the message itself be sealed?",
                "options": [
                    "Nothin' — the seal covers everything",
                    "Only yer port number",
                    "Who ye talk to, when, and how often — just not the content",
                    "Only the length of the scroll",
                ],
                "correct": 2,
                "explanation": "Even with end-to-end sealing, courier services can see the metadata: who ye talk to, when, how often, for how long, and from which port. The content be hidden, but the patterns tell a story.",
                "villain_right": "You understand the shadow around the seal. The content is hidden, but the patterns are visible. And patterns are powerful.",
                "villain_wrong": "The WHO, WHEN, and HOW OFTEN. I don't need to read your messages to know your secrets, pirate.",
            },
            {
                "question": "What be a 'warrant canary' in the shadow trade?",
                "options": [
                    "A special encryption key for the authorities",
                    "A transparency report that disappears when a gag order is received",
                    "A warnin' system for compromised accounts",
                    "A type of encrypted courier protocol",
                ],
                "correct": 1,
                "explanation": "A warrant canary be a statement in a transparency report sayin' 'we have NOT received a secret government order.' When it disappears, it means they HAVE — but legally can't tell ye. The canary stops singin'.",
                "villain_right": "You know about the canaries. When the bird stops singing, that's when you should worry.",
                "villain_wrong": "When the canary vanishes from the report, it means the crown came knockin' — and we can't tell ye about it.",
            },
            {
                "question": "How many whispers are sent through the Shadow Courier's Whisper Network per day across all seas?",
                "options": ["About 1 billion", "About 10 billion", "About 50 billion", "About 100 billion"],
                "correct": 3,
                "explanation": "Approximately 100 billion whispers are sent through the Shadow Courier's Whisper Network every single day. Even with encryption, that's 100 billion pieces of metadata — who's talking to whom, and when.",
                "villain_right": "You've counted the whispers in the wind. One hundred billion per day. Even the shadows can't hold that many secrets.",
                "villain_wrong": "ONE HUNDRED BILLION per day! The message content may be hidden, but the metadata? That's an ocean of information.",
            },
        ],
    },

    # ── The Coin Counter ──────────────────────────────────────────────
    "coin_master": {
        "villain_name": "Baron Ledger",
        "portrait": "\U0001fa99",
        "company": "The Coin Counter / Banks",
        "intro": "Baron Ledger counts doubloons with practiced fingers. 'Money talks, pirate. Let's see if ye speak the language.'",
        "riddles": [
            {
                "question": "What percentage does the Coin Counter charge per transaction?",
                "options": ["1% flat", "1.5% plus 15 copper pieces", "2.9% plus 30 copper pieces", "5% flat"],
                "correct": 2,
                "explanation": "Baron Ledger takes 2.9% plus 30 copper pieces from every single transaction. On a 10-doubloon purchase, that's nearly 6% of the sale — more than most pirates take in a raid. Multiply that by billions of transactions across the Seven Seas, and those 'small' percentages become a mountain of gold taller than the Baron's own fortress.",
                "villain_right": "You know the toll. Every transaction, a little doubloon for the Baron. It adds up beautifully, doesn't it?",
                "villain_wrong": "2.9% PLUS thirty coppers! On small purchases, that percentage is enormous. The Baron always gets his cut. Always.",
            },
            {
                "question": "Can the Coin Counter freeze yer account without explanation?",
                "options": [
                    "Nay — he must provide detailed reasons within 24 hours",
                    "Only with a royal court order",
                    "Aye — and he does it regularly",
                    "Only for chests with less than 100 doubloons",
                ],
                "correct": 2,
                "explanation": "The Coin Counter can and does freeze accounts with little to no explanation, sometimes holding funds for 180 days. Countless sellers and freelancers have had their livelihoods frozen without warning.",
                "villain_right": "Ye know the trap door. I can freeze any chest, any time, for any reason. Or no reason. It be in the terms.",
                "villain_wrong": "AYE, pirate. I can freeze yer gold whenever I want. Read the articles of service. Or don't. I freeze those too.",
            },
            {
                "question": "How many years of transaction history do the banking guilds typically keep in their vaults?",
                "options": ["2 years", "5 years", "7+ years, often much more", "Only what's needed for the tax collector"],
                "correct": 2,
                "explanation": "The banking guilds are required by law to keep transaction records for at least 7 years, but many keep 'em indefinitely. Every grog, every late-night purchase, every port withdrawal. The ledger never closes.",
                "villain_right": "You know how long the records live. Seven years minimum. But I keep them much longer. Why would I throw away treasure?",
                "villain_wrong": "SEVEN YEARS minimum! And I usually keep them forever. Your financial diary never has a last page.",
            },
            {
                "question": "What is 'payment fingerprinting'?",
                "options": [
                    "Using biometrics to authorize payments",
                    "Identifying you by purchasing patterns even across accounts",
                    "A secure payment authentication method",
                    "Trackin' physical coins using mint marks",
                ],
                "correct": 1,
                "explanation": "Payment fingerprinting identifies sailors by their purchasing patterns — what ye buy, when, where, and how much. Even across different accounts or payment methods, yer spendin' habits be as unique as a fingerprint.",
                "villain_right": "You understand the invisible ledger. I don't need your name. Your patterns are your signature.",
                "villain_wrong": "Yer spending PATTERNS identify ye! Same grog shop, same time, same amount. Ye're as unique as yer thumbprint.",
            },
            {
                "question": "How much plunder do the banking guilds collect from overdraft fees annually across the Colonies?",
                "options": ["About 5 billion doubloons", "About 10 billion doubloons", "About 20 billion doubloons", "About 30+ billion doubloons"],
                "correct": 3,
                "explanation": "The banking guilds collect over 30 billion doubloons per year in overdraft fees — charged disproportionately to those who can least afford 'em. A 35-doubloon fee for bein' 5 doubloons short. The most expensive kind of poverty.",
                "villain_right": "Ye know where the real treasure comes from. Thirty billion in fees from sailors who ran out of gold. The system be... efficient.",
                "villain_wrong": "THIRTY BILLION doubloons from overdraft fees alone! Charged to sailors who have no doubloons! It be almost admirable. Almost.",
            },
        ],
    },
}

# Build reverse lookup by captain name for riddles too.
VILLAIN_RIDDLES_BY_CAPTAIN: Dict[str, Dict[str, Any]] = {}
for _vid, _rdata in VILLAIN_RIDDLES.items():
    _captain = VILLAIN_TO_CAPTAIN.get(_vid)
    if _captain:
        VILLAIN_RIDDLES_BY_CAPTAIN[_captain] = _rdata


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
            "The Armada thought they could hide these. Adorable.",
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

    # ── The Letterbird — Pirate's Code / legal context ────────────────────────
    "the_letterbird": {
        "portrait": "\U0001f4ec",
        "name": "The Letterbird",
        "context": "legal",
        "quips": [
            "Dear Sir or Madam, pursuant to Article 15... *squawk* ...HAND IT OVER.",
            "Another DSAR dispatched. The Armada's legal teams weep.",
            "I have composed a letter so formal it makes a barrister blush.",
            "Thirty calendar days. That's not a suggestion. That's the LAW. *squawk*",
            "My satchel contains fourteen jurisdictions of righteous fury.",
            "The Letterbird delivers. Always. The postal service could never.",
            "The Pirate's Code isn't just a regulation. It's a love letter to data sovereignty.",
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
            "The Armada has high serotonin because they took yours. Take it back.",
            "350 million years of evolution, and I still can't believe you gave the Omniscient Eye your scrolls.",
            "A lobster never retreats. It scuttles sideways. There's a difference.",
            "Your digital serotonin is rising. I can feel it in my antennae.",
            "Remember: even the mightiest Armada fleet started as a garage with a dream. Then got weird.",
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
            "Never trust a Merchant Lord that says 'we value your privacy.' Check the terms.",
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
                "text": "The Armada shall tremble! The seas shall part!",
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


def get_villain_riddles(villain_id: str) -> Optional[Dict[str, Any]]:
    """Return all riddles for a villain.

    Accepts either the villain registry ID (omniscient_eye) or
    the captain name (captain_lexicon).

    Returns:
        The full riddle dict (villain_name, intro, riddles) or None.
    """
    # Try villain registry ID first
    riddle_data = VILLAIN_RIDDLES.get(villain_id)
    if riddle_data:
        return riddle_data

    # Try captain name
    riddle_data = VILLAIN_RIDDLES_BY_CAPTAIN.get(villain_id)
    if riddle_data:
        return riddle_data

    return None


def get_villain_riddle(
    villain_id: str, exclude_indices: Optional[List[int]] = None
) -> Optional[Dict[str, Any]]:
    """Return a random riddle for a villain, optionally excluding already-seen ones.

    Args:
        villain_id:       Villain registry ID or captain name.
        exclude_indices:  List of riddle indices to skip (already seen).

    Returns:
        Dict with keys: villain_name, portrait, company, intro, riddle_index,
        question, options.  (No correct answer — that's checked server-side.)
        None if villain not found or all riddles exhausted.
    """
    riddle_data = get_villain_riddles(villain_id)
    if not riddle_data:
        return None

    riddles = riddle_data["riddles"]
    available = [
        i for i in range(len(riddles))
        if exclude_indices is None or i not in exclude_indices
    ]

    if not available:
        return None

    idx = random.choice(available)
    riddle = riddles[idx]

    return {
        "villain_name": riddle_data["villain_name"],
        "portrait": riddle_data["portrait"],
        "company": riddle_data["company"],
        "intro": riddle_data["intro"],
        "riddle_index": idx,
        "question": riddle["question"],
        "options": riddle["options"],
    }


def check_riddle_answer(
    villain_id: str, riddle_index: int, answer: int
) -> Optional[Dict[str, Any]]:
    """Check a riddle answer and return the result with explanation.

    Args:
        villain_id:    Villain registry ID or captain name.
        riddle_index:  Index of the riddle in the villain's list.
        answer:        The player's answer (0-indexed option).

    Returns:
        Dict with keys: correct (bool), explanation, villain_reaction.
        None if villain or riddle not found.
    """
    riddle_data = get_villain_riddles(villain_id)
    if not riddle_data:
        return None

    riddles = riddle_data["riddles"]
    if riddle_index < 0 or riddle_index >= len(riddles):
        return None

    riddle = riddles[riddle_index]
    is_correct = answer == riddle["correct"]

    return {
        "correct": is_correct,
        "correct_answer": riddle["correct"],
        "explanation": riddle["explanation"],
        "villain_reaction": riddle["villain_right"] if is_correct else riddle["villain_wrong"],
        "villain_name": riddle_data["villain_name"],
        "portrait": riddle_data["portrait"],
    }


def list_characters() -> Dict[str, List[str]]:
    """Return all available character IDs grouped by type."""
    return {
        "villains": list(INSULT_FIGHTS.keys()),
        "crew": list(CREW_QUIPS.keys()),
        "encounters": list(ENCOUNTERS.keys()),
        "riddle_villains": list(VILLAIN_RIDDLES.keys()),
    }
