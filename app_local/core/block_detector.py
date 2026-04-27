from typing import List, Dict, Optional
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from logger import get_logger

logger = get_logger(__name__)

# Trigger thresholds for proposing a new block
MIN_TURNS_FOR_DETECTION = 4      # Minimum number of conversation turns
TOPIC_REPETITION_THRESHOLD = 2   # Number of times a topic should be mentioned

def detect_new_block_opportunity(history: List[Dict], dll: Dict) -> Optional[Dict]:
    """
    Self-contained local implementation of the block detector.
    Analyzes recent history to detect if a new knowledge block
    should be created (e.g., note-taking on a new chapter).
    
    Returns a configuration dict if an opportunity is detected, else None.
    """
    # 1. Check minimum conversation threshold
    if len(history) < MIN_TURNS_FOR_DETECTION:
        return None

    # 2. Check that dynamic block limit isn't reached
    dynamic_count = dll.get("dynamic_block_count", 0)
    dynamic_max = dll.get("dynamic_block_max", 5)
    if dynamic_count >= dynamic_max:
        logger.debug(f"Dynamic block limit reached ({dynamic_count}/{dynamic_max}).")
        return None

    # 3. Heuristic analysis of recent student messages
    recent_user_messages = [
        m["content"] for m in history[-6:]
        if m.get("role") == "user" and isinstance(m.get("content"), str)
    ]

    if not recent_user_messages:
        return None

    # Look for patterns signaling a new topic to memorize
    learning_triggers = [
        "i don't understand", "explain", "what is", "how", 
        "why", "i'm struggling", "difficult", "new chapter",
        "lesson", "exercise", "je ne comprends pas", "explique"
    ]

    trigger_count = sum(
        1 for msg in recent_user_messages
        for trigger in learning_triggers
        if trigger.lower() in msg.lower()
    )

    if trigger_count >= TOPIC_REPETITION_THRESHOLD:
        # Propose a "course" type block to memorize the current topic
        proposed_id = f"dynamic_block_{dynamic_count + 1}"
        logger.info(f"Block opportunity detected (triggers={trigger_count}): {proposed_id}")
        return {
            "proposed_id": proposed_id,
            "label": "Topic currently being learned",
            "block_type": "course",
            "reason": f"Student showed {trigger_count} active learning signals."
        }

    return None
