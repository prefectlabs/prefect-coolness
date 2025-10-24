"""
Dungeon Campaign Flow - A D&D-Themed Demo Flow

This flow demonstrates:
1. Creating and updating artifacts with Marvin-generated campaign content
2. Custom "Cancelled" state (Skipped type) for abandoned quests
3. Complex task tagging with overlapping and distinct D&D-themed tags
4. Nested subflow architecture simulating a multi-party dungeon campaign
"""

import os
import random
import time
from datetime import datetime

import marvin
from prefect import flow, task
from prefect.artifacts import MarkdownArtifact, TableArtifact
from prefect.assets import Asset, AssetProperties, materialize
from prefect.client.schemas.objects import State
from prefect.states import Cancelled, Completed
from pydantic_ai.models.anthropic import AnthropicModel

# Define Assets for the dungeon campaign
exploration_records = Asset(
    key="local://dungeon-campaign/exploration-records",
    properties=AssetProperties(
        name="Exploration Records",
        description="Records of all dungeon room explorations by adventuring parties",
        owners=["dungeon-master"],
    ),
)

combat_records = Asset(
    key="local://dungeon-campaign/combat-records",
    properties=AssetProperties(
        name="Combat Records",
        description="Records of all combat encounters and battle outcomes",
        owners=["dungeon-master"],
    ),
)

treasure_records = Asset(
    key="local://dungeon-campaign/treasure-records",
    properties=AssetProperties(
        name="Treasure Records",
        description="Records of all treasure looted from dungeons",
        owners=["dungeon-master"],
    ),
)

quest_summaries = Asset(
    key="local://dungeon-campaign/quest-summaries",
    properties=AssetProperties(
        name="Quest Summaries",
        description="Summary records for individual party quests",
        owners=["dungeon-master"],
    ),
)

expedition_summaries = Asset(
    key="local://dungeon-campaign/expedition-summaries",
    properties=AssetProperties(
        name="Expedition Summaries",
        description="Summary records for multi-room party expeditions",
        owners=["dungeon-master"],
    ),
)

experience_awards = Asset(
    key="local://dungeon-campaign/experience-awards",
    properties=AssetProperties(
        name="Experience Awards",
        description="XP awards and level progression for all parties",
        owners=["dungeon-master"],
    ),
)


def get_anthropic_model():
    """Get Anthropic model if API key is available."""
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if api_key:
        return AnthropicModel(model_name="claude-3-5-sonnet-latest")
    else:
        return None


anthropic_model = get_anthropic_model()

# Marvin agent for D&D content generation
dungeon_master = marvin.Agent(
    name="Dungeon Master",
    instructions="You are an experienced Dungeon Master for D&D campaigns. Generate creative, engaging narrative content about dungeon adventures, character encounters, treasure discoveries, and campaign outcomes. Keep responses flavorful but concise.",
    model=anthropic_model if anthropic_model else None,
)


@materialize(exploration_records, tags=["exploration", "dungeon", "core"])
def explore_dungeon_room(room_name: str, party_name: str) -> dict:
    """Explore a dungeon room - core exploration mechanics."""
    print(f"🗺️  {party_name} exploring {room_name}...")
    time.sleep(1)

    exploration = {
        "room": room_name,
        "party": party_name,
        "monsters_encountered": random.randint(0, 5),
        "treasure_found": random.randint(10, 100),
        "timestamp": datetime.now().isoformat(),
    }
    print(
        f"⚔️  Encountered {exploration['monsters_encountered']} monsters, found {exploration['treasure_found']} gold"
    )

    # Create exploration artifact
    exploration_artifact = MarkdownArtifact(
        key=f"exploration-{party_name.lower().replace(' ', '-')}-{room_name.lower().replace(' ', '-')[:20]}",
        description=f"Exploration record for {party_name} in {room_name}",
        markdown=f"""## 🗺️ Exploration Report
**Party:** {party_name}
**Location:** {room_name}
**Monsters Encountered:** {exploration["monsters_encountered"]}
**Treasure Found:** {exploration["treasure_found"]} gold
**Time:** {datetime.fromisoformat(exploration["timestamp"]).strftime("%H:%M:%S")}
""",
    )
    exploration_artifact.create()

    return exploration


@task(tags=["exploration", "combat", "core"], asset_deps=[exploration_records])
def roll_initiative(exploration: dict):
    """Roll initiative for combat - may be abandoned."""
    print(f"🎲 Rolling initiative in {exploration['room']}...")
    time.sleep(1)

    # 25% chance of abandoning combat (fleeing or avoiding)
    if random.random() < 0.25:
        print(f"🏃 {exploration['party']} abandoned combat in {exploration['room']}!")
        return Cancelled(
            name="Abandoned",
            message=f"Combat abandoned - party fled from {exploration['room']}",
        )

    exploration["initiative_roll"] = random.randint(1, 20)
    exploration["combat_engaged"] = True
    print(f"⚔️  Initiative: {exploration['initiative_roll']}")
    return exploration


@materialize(
    combat_records,
    tags=["combat", "dice", "challenge"],
    asset_deps=[exploration_records],
)
def engage_combat(exploration: dict) -> dict:
    """Engage in combat encounters."""
    print(f"⚔️  {exploration['party']} engaging in combat...")
    time.sleep(1)

    exploration["damage_dealt"] = random.randint(10, 50)
    exploration["damage_taken"] = random.randint(5, 30)
    exploration["enemies_defeated"] = exploration["monsters_encountered"]
    print(
        f"💥 Dealt {exploration['damage_dealt']} damage, took {exploration['damage_taken']} damage"
    )

    # Create combat artifact
    combat_artifact = MarkdownArtifact(
        key=f"combat-{exploration['party'].lower().replace(' ', '-')}-{exploration['room'].lower().replace(' ', '-')[:20]}",
        description=f"Combat record for {exploration['party']} in {exploration['room']}",
        markdown=f"""## ⚔️ Combat Report
**Party:** {exploration["party"]}
**Location:** {exploration["room"]}
**Enemies Defeated:** {exploration["enemies_defeated"]}
**Damage Dealt:** {exploration["damage_dealt"]} HP
**Damage Taken:** {exploration["damage_taken"]} HP
**Status:** Victory
""",
    )
    combat_artifact.create()

    return exploration


@materialize(
    treasure_records,
    tags=["exploration", "loot", "reward"],
    asset_deps=[combat_records],
)
def loot_treasure(exploration: dict) -> dict:
    """Loot treasure from defeated enemies and chests."""
    print(f"💰 Looting {exploration['room']}...")
    time.sleep(1)

    exploration["total_loot"] = exploration["treasure_found"] + (
        exploration.get("enemies_defeated", 0) * 10
    )
    exploration["magic_items"] = random.randint(0, 3)
    exploration["completed"] = True
    print(
        f"✨ Total loot: {exploration['total_loot']} gold, {exploration['magic_items']} magic items"
    )

    # Create loot artifact
    loot_artifact = MarkdownArtifact(
        key=f"loot-{exploration['party'].lower().replace(' ', '-')}-{exploration['room'].lower().replace(' ', '-')[:20]}",
        description=f"Loot record for {exploration['party']} in {exploration['room']}",
        markdown=f"""## 💰 Treasure Haul
**Party:** {exploration["party"]}
**Location:** {exploration["room"]}
**Base Treasure:** {exploration["treasure_found"]} gold
**Enemy Loot:** {exploration.get("enemies_defeated", 0) * 10} gold
**Total Gold:** {exploration["total_loot"]} gold
**Magic Items:** {exploration["magic_items"]}
**Quest Status:** ✅ Complete
""",
    )
    loot_artifact.create()

    return exploration


@task(
    tags=["narrative", "campaign", "storytelling"],
    asset_deps=[expedition_summaries, quest_summaries],
)
def narrate_campaign_events(all_explorations: list[dict]) -> str:
    """Generate narrative summary of campaign events using Marvin."""
    print("📜 Dungeon Master narrating campaign events...")

    successful_rooms = [e for e in all_explorations if e.get("completed")]
    abandoned_rooms = [e for e in all_explorations if not e.get("combat_engaged")]
    total_gold = sum(e.get("total_loot", 0) for e in successful_rooms)
    total_enemies = sum(e.get("enemies_defeated", 0) for e in successful_rooms)

    if anthropic_model:
        try:
            with marvin.Thread(id="campaign-narrative") as thread:
                prompt = f"""Generate an exciting D&D campaign summary narrative.
                The party cleared {len(successful_rooms)} rooms, defeated {total_enemies} enemies,
                collected {total_gold} gold, but fled from {len(abandoned_rooms)} dangerous encounters.
                Make it dramatic and memorable in 3-4 short paragraphs."""
                narrative = dungeon_master.run(prompt)
            print("✅ DM narrative generated")
            return narrative
        except Exception as e:
            print(f"⚠️  DM unavailable, using fallback narrative: {e}")

    # Fallback narrative
    return f"""
## Campaign Chronicle

### The Dungeon Delve
Your brave adventurers ventured deep into the ancient dungeons, facing untold dangers
and mysterious challenges. Through {len(successful_rooms)} perilous chambers, they
battled valiantly against {total_enemies} fearsome creatures.

### Trials and Tribulations
Not every encounter went as planned. {len(abandoned_rooms)} times the party wisely chose
discretion over valor, retreating from overwhelming odds to fight another day. Such is
the wisdom of seasoned adventurers.

### Rewards and Glory
Their courage was richly rewarded with {total_gold} gold pieces worth of treasure and
magical artifacts. Tales of their deeds will echo through taverns for years to come.

*The campaign continues...*
"""


@task(
    tags=["narrative", "artifacts", "campaign"],
    asset_deps=[expedition_summaries, quest_summaries, treasure_records],
)
def create_campaign_chronicle(all_explorations: list[dict], narrative: str) -> str:
    """Create a campaign chronicle artifact."""
    print("📖 Creating Campaign Chronicle artifact...")

    # Build party statistics table
    table_data: dict[str, list[str]] = {
        "Party": [],
        "Room": [],
        "Monsters": [],
        "Gold Looted": [],
        "Status": [],
    }

    for exploration in all_explorations:
        table_data["Party"].append(exploration["party"])
        table_data["Room"].append(exploration["room"])
        table_data["Monsters"].append(str(exploration.get("enemies_defeated", 0)))

        if exploration.get("completed"):
            table_data["Gold Looted"].append(f"{exploration['total_loot']}g")
            table_data["Status"].append("✅ Victorious")
        elif exploration.get("combat_engaged"):
            table_data["Gold Looted"].append("0g")
            table_data["Status"].append("🔄 In Progress")
        else:
            table_data["Gold Looted"].append("0g")
            table_data["Status"].append("🏃 Fled")

    # Calculate final statistics
    successful = len([e for e in all_explorations if e.get("completed")])
    abandoned = len([e for e in all_explorations if not e.get("combat_engaged")])
    total_gold = sum(
        e.get("total_loot", 0) for e in all_explorations if e.get("completed")
    )
    total_enemies = sum(
        e.get("enemies_defeated", 0) for e in all_explorations if e.get("completed")
    )

    # Create comprehensive markdown artifact
    chronicle_markdown = f"""# 📖 Campaign Chronicle
## *The Dungeon of Eternal Shadows*
*Chronicle Updated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}*

## ⚔️ Campaign Status
✅ *Campaign Session Complete*

**Final Tally:**
- Rooms Cleared: {successful} of {len(all_explorations)}
- Enemies Vanquished: {total_enemies}
- Combats Abandoned: {abandoned}
- Gold Acquired: {total_gold} pieces
- Success Rate: {(successful / len(all_explorations) * 100):.1f}%

## 📜 Dungeon Master's Tale

{narrative}
"""

    chronicle = MarkdownArtifact(
        key="dungeon-campaign-chronicle",
        description="Dungeon Campaign Chronicle and Party Statistics",
        markdown=chronicle_markdown,
    )
    chronicle.create()

    # Create party statistics table artifact
    party_stats = TableArtifact(
        key="dungeon-campaign-party-stats",
        description="Party statistics for dungeon campaign",
        table=table_data,
    )
    party_stats.create()

    print("✅ Campaign Chronicle complete")
    return f"chronicle-{len(all_explorations)}-rooms"


@materialize(
    quest_summaries,
    tags=["subflow", "quest", "summary"],
    asset_deps=[exploration_records, treasure_records],
)
def create_party_quest_summary(quest_result: dict) -> dict:
    """Create a summary artifact for a party's quest through one room."""
    print(
        f"📊 Creating quest summary for {quest_result['party']} in {quest_result['room']}..."
    )

    # Determine quest status
    if quest_result.get("completed"):
        status = "✅ Victorious"
        status_emoji = "🎉"
    elif quest_result.get("combat_engaged"):
        status = "🔄 In Progress"
        status_emoji = "⚔️"
    else:
        status = "🏃 Fled"
        status_emoji = "💨"

    # Build the markdown content
    markdown_content = f"""## {status_emoji} Party Quest Report
### {quest_result["party"]} - {quest_result["room"]}

**Monsters Encountered:** {quest_result.get("monsters_encountered", 0)}
**Enemies Defeated:** {quest_result.get("enemies_defeated", 0)}
**Gold Looted:** {quest_result.get('total_loot', 0)}g
**Magic Items:** {quest_result.get("magic_items", 0)}
**Status:** {status}
"""

    if quest_result.get("combat_engaged"):
        markdown_content += f"""
### Combat Details
- **Damage Dealt:** {quest_result.get("damage_dealt", 0)} HP
- **Damage Taken:** {quest_result.get("damage_taken", 0)} HP
- **Initiative Roll:** {quest_result.get("initiative_roll", "N/A")}
"""

    # Create party quest artifact
    quest_artifact = MarkdownArtifact(
        key=f"quest-summary-{quest_result['party'].lower().replace(' ', '-')}-{quest_result['room'].lower().replace(' ', '-')[:20]}",
        description=f"Quest summary for {quest_result['party']} in {quest_result['room']}",
        markdown=markdown_content,
    )
    quest_artifact.create()

    return quest_result


@flow(name="Party Quest", log_prints=True)
def party_quest_subflow(party_name: str, room_name: str) -> dict:
    """Subflow representing a single party's quest through one room."""
    print(f"\n⚔️  {party_name} begins quest in {room_name}...")

    # Exploration phase
    exploration = explore_dungeon_room(room_name, party_name)

    # Initiative phase (may be abandoned)
    combat_ready = roll_initiative(exploration)

    # Check if combat was abandoned
    if isinstance(combat_ready, State) and combat_ready.is_cancelled():
        print(f"🏃 {party_name} fled from {room_name}!")
        quest_result = create_party_quest_summary(exploration)
        return quest_result

    # Combat phase
    combat_result = engage_combat(combat_ready)

    # Looting phase
    final_result = loot_treasure(combat_result)

    # Create quest summary
    quest_result = create_party_quest_summary(final_result)

    print(f"✅ {party_name} completed quest in {room_name}!")
    return quest_result


@materialize(
    expedition_summaries,
    tags=["subflow", "expedition", "summary"],
    asset_deps=[quest_summaries],
)
def create_expedition_summary(
    party_name: str, expedition_results: list[dict]
) -> list[dict]:
    """Create a summary artifact for a party's expedition through multiple rooms."""
    print(f"📊 Creating expedition summary for {party_name}...")

    # Calculate expedition statistics
    completed_quests = [r for r in expedition_results if r.get("completed")]
    fled_quests = [r for r in expedition_results if not r.get("combat_engaged")]
    total_gold = sum(r.get("total_loot", 0) for r in completed_quests)
    total_enemies = sum(r.get("enemies_defeated", 0) for r in completed_quests)
    total_magic_items = sum(r.get("magic_items", 0) for r in completed_quests)

    # Create expedition markdown artifact
    expedition_markdown = f"""## 🗡️ Expedition Report: {party_name}
**Total Rooms Explored:** {len(expedition_results)}
**Quests Completed:** {len(completed_quests)}
**Combats Fled:** {len(fled_quests)}
**Success Rate:** {(len(completed_quests) / len(expedition_results) * 100):.1f}%

### 💰 Total Rewards
- **Gold Collected:** {total_gold} pieces
- **Enemies Defeated:** {total_enemies}
- **Magic Items Found:** {total_magic_items}
"""

    expedition_artifact = MarkdownArtifact(
        key=f"expedition-summary-{party_name.lower().replace(' ', '-')}",
        description=f"Expedition summary for {party_name}",
        markdown=expedition_markdown,
    )
    expedition_artifact.create()

    # Room-by-room breakdown table
    room_table = {
        "Room": [r["room"] for r in expedition_results],
        "Monsters": [str(r.get("enemies_defeated", 0)) for r in expedition_results],
        "Gold": [f"{r.get('total_loot', 0)}g" for r in expedition_results],
        "Magic Items": [str(r.get("magic_items", 0)) for r in expedition_results],
        "Status": [
            "✅ Complete"
            if r.get("completed")
            else "🏃 Fled"
            if not r.get("combat_engaged")
            else "🔄 In Progress"
            for r in expedition_results
        ],
    }
    room_breakdown = TableArtifact(
        key=f"expedition-rooms-{party_name.lower().replace(' ', '-')}",
        description=f"Room-by-room breakdown for {party_name}",
        table=room_table,
    )
    room_breakdown.create()

    return expedition_results


@flow(name="Dungeon Level Expedition", log_prints=True)
def dungeon_level_expedition(party_name: str, rooms: list[str]) -> list[dict]:
    """Nested subflow coordinating a party's expedition through multiple rooms."""
    print(f"\n🗡️  {party_name} embarking on expedition through {len(rooms)} rooms...")

    results = []
    for room in rooms:
        result = party_quest_subflow(party_name, room)
        results.append(result)
        time.sleep(0.5)  # Brief rest between rooms

    # Create expedition summary
    expedition_results = create_expedition_summary(party_name, results)

    completed = len([r for r in expedition_results if r.get("completed")])
    print(
        f"✅ {party_name} expedition complete - {completed}/{len(rooms)} rooms cleared"
    )
    return expedition_results


@task(tags=["campaign", "notification", "tavern"])
def spread_tavern_tales(chronicle_key: str):
    """Spread tales of adventure through the taverns - may be cancelled."""
    print("🍺 Spreading tales through local taverns...")
    time.sleep(1)

    # 20% chance of cancellation (too drunk to tell coherent stories)
    if random.random() < 0.2:
        print("🍻 Tales abandoned - party celebrating too hard!")
        return Cancelled(
            name="Skipped", message="Tavern tales cancelled - excessive celebration"
        )

    print("✅ Glorious tales spread throughout the realm!")
    return Completed(message="Legends born in taverns across the land")


@materialize(
    experience_awards,
    tags=["reward", "experience", "progression"],
    asset_deps=[treasure_records, quest_summaries],
)
def award_experience_points(all_results: list[dict]) -> dict:
    """Calculate and award experience points to all parties."""
    print("⭐ Calculating experience points...")
    time.sleep(1)

    total_xp = 0
    party_xp = {}
    for result in all_results:
        if result.get("completed"):
            monster_xp = result.get("enemies_defeated", 0) * 50
            room_xp = 100
            quest_xp = monster_xp + room_xp
            total_xp += quest_xp

            party = result["party"]
            party_xp[party] = party_xp.get(party, 0) + quest_xp

    xp_stats = {
        "total_xp": total_xp,
        "levels_gained": total_xp // 1000,
        "parties_rewarded": len(
            set(r["party"] for r in all_results if r.get("completed"))
        ),
        "party_breakdown": party_xp,
    }

    print(f"✨ Awarded {total_xp} XP, {xp_stats['levels_gained']} levels gained!")

    # Create XP award markdown artifact
    xp_markdown = f"""## ⭐ Experience Awards
**Total XP Awarded:** {total_xp}
**Total Levels Gained:** {xp_stats["levels_gained"]}
**Parties Rewarded:** {xp_stats["parties_rewarded"]}
"""

    xp_artifact = MarkdownArtifact(
        key="campaign-experience-awards",
        description="Experience point awards for all parties",
        markdown=xp_markdown,
    )
    xp_artifact.create()

    # Create XP table artifact
    xp_table_data = {
        "Party": list(party_xp.keys()),
        "XP Earned": [str(xp) for xp in party_xp.values()],
        "Levels Gained": [str(xp // 1000) for xp in party_xp.values()],
    }

    xp_table = TableArtifact(
        key="campaign-experience-table",
        description="Detailed XP breakdown by party",
        table=xp_table_data,
    )
    xp_table.create()

    return xp_stats


@flow(name="Grand Campaign", log_prints=True)
def dungeon_campaign_flow(num_parties: int = 3, rooms_per_party: int = 4):
    """
    Grand D&D Campaign Flow showcasing artifacts, custom states, tags, and nested subflows.

    Architecture:
    - Main flow: dungeon_campaign_flow (Grand Campaign)
      - Nested subflow 1: dungeon_level_expedition (per party)
        - Nested subflow 2: party_quest_subflow (per room)
          - Tasks: explore_dungeon_room, roll_initiative, engage_combat, loot_treasure
      - Tasks: narrate_campaign_events, create_campaign_chronicle,
               spread_tavern_tales, award_experience_points

    Args:
        num_parties: Number of adventuring parties (default: 3)
        rooms_per_party: Number of rooms per party expedition (default: 4)
    """
    print("=" * 80)
    print("🐉 GRAND DUNGEON CAMPAIGN BEGINNING")
    print("=" * 80)
    print(f"⚔️  Assembling {num_parties} parties for dungeon expedition")
    print(f"🗺️  Each party will explore {rooms_per_party} perilous rooms")
    print()

    # Generate party names
    party_names = [
        "The Crimson Blades",
        "Shadow Walkers",
        "Steel Serpents",
        "Arcane Brotherhood",
        "Dragon Slayers",
        "Moon Guardians",
    ][:num_parties]

    # Generate dungeon rooms
    room_templates = [
        "Chamber of Echoing Doom",
        "Hall of Shattered Mirrors",
        "Crypt of the Fallen King",
        "Library of Forbidden Knowledge",
        "Throne Room of Shadows",
        "Treasury of Lost Souls",
        "Armory of Ancient Warriors",
        "Chapel of Dark Rituals",
        "Laboratory of Mad Alchemist",
        "Vault of Eternal Ice",
    ]

    print("🎭 ADVENTURING PARTIES:")
    for party in party_names:
        print(f"   ⚔️  {party}")
    print()

    # Each party explores their own set of rooms
    all_results = []
    for i, party_name in enumerate(party_names):
        rooms = random.sample(room_templates, rooms_per_party)
        print(f"🗡️  {party_name} will explore:")
        for room in rooms:
            print(f"      📍 {room}")
        print()

        party_results = dungeon_level_expedition(party_name, rooms)
        all_results.extend(party_results)
        time.sleep(0.5)

    print("\n" + "=" * 80)
    print("📖 CHRONICLING THE CAMPAIGN")
    print("=" * 80)

    # Generate campaign narrative
    narrative = narrate_campaign_events(all_results)

    # Create campaign chronicle artifact
    chronicle_key = create_campaign_chronicle(all_results, narrative)

    # Award XP
    xp_stats = award_experience_points(all_results)

    # Spread tavern tales (may be cancelled)
    tales_result = spread_tavern_tales(chronicle_key)

    # Calculate final statistics
    successful = len([r for r in all_results if r.get("completed")])
    abandoned = len([r for r in all_results if not r.get("combat_engaged")])
    total_gold = sum(r.get("total_loot", 0) for r in all_results if r.get("completed"))
    total_enemies = sum(
        r.get("enemies_defeated", 0) for r in all_results if r.get("completed")
    )
    magic_items = sum(
        r.get("magic_items", 0) for r in all_results if r.get("completed")
    )

    print("\n" + "=" * 80)
    print("🎯 CAMPAIGN SUMMARY")
    print("=" * 80)
    print(f"⚔️  Adventuring Parties: {num_parties}")
    print(f"🗺️  Total Rooms Explored: {len(all_results)}")
    print(f"✅ Rooms Cleared: {successful}")
    print(f"🏃 Rooms Abandoned: {abandoned}")
    print(f"💀 Enemies Defeated: {total_enemies}")
    print(f"💰 Gold Acquired: {total_gold} pieces")
    print(f"✨ Magic Items: {magic_items}")
    print(f"⭐ Experience Points: {xp_stats['total_xp']} XP")
    print(f"📈 Levels Gained: {xp_stats['levels_gained']}")
    print(f"📊 Success Rate: {(successful / len(all_results) * 100):.1f}%")
    print(f"📖 Chronicle: {chronicle_key}")

    if isinstance(tales_result, State) and tales_result.is_cancelled():
        print("🍻 Tavern Tales: Cancelled (celebrating too hard!)")
    else:
        print("✅ Tavern Tales: Spread across the realm")

    print("=" * 80)
    print()

    return {
        "parties": num_parties,
        "total_rooms": len(all_results),
        "successful": successful,
        "abandoned": abandoned,
        "total_gold": total_gold,
        "total_enemies": total_enemies,
        "magic_items": magic_items,
        "xp_awarded": xp_stats["total_xp"],
        "levels_gained": xp_stats["levels_gained"],
        "chronicle": chronicle_key,
    }


if __name__ == "__main__":
    # Begin the grand campaign!
    result = dungeon_campaign_flow(num_parties=3, rooms_per_party=4)
    print("\n🎉 The campaign has concluded!")
    print(f"📊 Final results: {result}")
