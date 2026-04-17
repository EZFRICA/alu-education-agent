from typing import Optional
from datetime import datetime
from apu.mmu.controller import save_dll
from logger import get_logger

logger = get_logger(__name__)

# Heuristic signals for block auto-detection — triggered by keyword frequency in recent messages
BLOCK_DETECTION_SIGNALS = {
    "visa_documents": {
        "label": "Visa & Documents",
        "type": "fondamental",
        "trigger_keywords": ["visa", "passport", "document", "embassy",
                             "consulate", "insurance", "permit", "identity"],
        "min_mentions": 3,
        "proposal_message": (
            "I notice we keep talking about travel documents. "
            "Would you like me to create a dedicated 'Visa & Documents' block "
            "to centralize all this information?"
        ),
        "initial_content": "Block reserved for official documents and visas.",
    },
    "budget_tracker": {
        "label": "Budget Tracker",
        "type": "projet",
        "trigger_keywords": ["budget", "euros", "price", "cost", "expense",
                             "rate", "pay", "book", "expensive", "cheap"],
        "min_mentions": 4,
        "proposal_message": (
            "You're managing a lot of numbers and costs. "
            "Would you like a 'Budget Tracker' block to track "
            "your expenses and forecasts?"
        ),
        "initial_content": "Expense tracking and budget overview.",
    },
    "travel_companions": {
        "label": "Travel Companions",
        "type": "projet",
        "trigger_keywords": ["with", "companion", "friend", "family",
                             "partner", "colleague", "group", "children"],
        "min_mentions": 2,
        "proposal_message": (
            "It looks like you're traveling with others. "
            "Would you like a 'Travel Companions' block to remember "
            "their preferences and constraints?"
        ),
        "initial_content": "Information about other travelers in the group.",
    },
    "food_wishlist": {
        "label": "Food Wishlist",
        "type": "temp",
        "trigger_keywords": ["restaurant", "eat", "food", "chef",
                             "specialty", "market", "café", "bar", "cuisine", "vegetarian"],
        "min_mentions": 3,
        "proposal_message": (
            "You keep mentioning food spots. "
            "Would you like a 'Food Wishlist' block to keep track of them?"
        ),
        "initial_content": "Curated list of food addresses to visit.",
    },
    "activities_wishlist": {
        "label": "Activities Wishlist",
        "type": "temp",
        "trigger_keywords": ["museum", "visit", "activity", "show",
                             "concert", "hiking", "beach", "monument", "see"],
        "min_mentions": 3,
        "proposal_message": (
            "You've mentioned several activities you want to do. "
            "Would you like an 'Activities Wishlist' block to centralize them?"
        ),
        "initial_content": "Activities and tourist spots to visit.",
    },
}


def detect_new_block_opportunity(
    conversation_history: list,  # list of dicts {"role": ..., "content": ...} or LangChain messages
    dll: dict,
) -> Optional[dict]:
    """
    Analyze the last N messages to detect a structuring topic.
    Returns the proposed block configuration dict, or None if no signal is found.

    Supports both raw dict history ({"role": "user", "content": "..."})
    and LangChain message objects (.type / .content attributes).
    """
    if not conversation_history:
        return None

    if dll["dynamic_block_count"] >= dll["dynamic_block_max"]:
        logger.debug("Block detection skipped: dynamic block limit reached (%d/%d).",
                     dll["dynamic_block_count"], dll["dynamic_block_max"])
        return None

    existing_block_ids = set(dll["nodes"].keys())
    recent_messages = conversation_history[-20:]

    # Unify format: support both dicts and LangChain message objects
    if hasattr(recent_messages[0], "content"):
        # LangChain message objects
        full_text = " ".join(
            str(msg.content).lower()
            for msg in recent_messages
            if msg.type == "human"
        )
    else:
        # Raw dict format
        full_text = " ".join(
            str(msg.get("content", "")).lower()
            for msg in recent_messages
            if msg.get("role") == "user"
        )

    for block_id, signal_config in BLOCK_DETECTION_SIGNALS.items():
        if block_id in existing_block_ids:
            continue  # Block already exists

        mention_count = sum(
            full_text.count(keyword)
            for keyword in signal_config["trigger_keywords"]
        )

        if mention_count >= signal_config["min_mentions"]:
            logger.debug(
                "Block opportunity detected: '%s' (mentions=%d, threshold=%d).",
                block_id, mention_count, signal_config["min_mentions"]
            )
            return {
                "proposed_id": block_id,
                "label": signal_config["label"],
                "type": signal_config["type"],
                "keywords": signal_config["trigger_keywords"][:5],
                "proposal_message": signal_config["proposal_message"],
                "initial_content": signal_config["initial_content"],
            }

    return None
