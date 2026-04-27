"""
DLL Manager — LanceDB-backed version (Local Edition).
Implements the Bidirectional Metadata Jump (BMJ) algorithm for memory routing.
Uses LanceDB for vector search instead of Weaviate.
DLL structure (prev/next pointers) stored in metadata_links.json.
"""

import fcntl
import json
import os
import uuid
from datetime import datetime
import asyncio
from typing import Optional, Dict, List

# Local imports
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from app_local.config import settings
from app_local.storage import lance_driver
from logger import get_logger

logger = get_logger(__name__)

# ── Dynamic Isolation Locks ──────────────────────────────────────────────────
_dll_locks: Dict[str, asyncio.Lock] = {}

def get_dll_lock(agent_id: str) -> asyncio.Lock:
    """Get or create an asyncio.Lock for a specific agent."""
    if agent_id not in _dll_locks:
        _dll_locks[agent_id] = asyncio.Lock()
    return _dll_locks[agent_id]

# ── Adaptive certainty thresholds by block type (Education context) ───────────
CERTAINTY_THRESHOLDS = {
    "fondamental": 0.70,  # student_profile / learning_preferences — always relevant
    "cours":       0.75,  # active_course — neutral threshold, subject-driven
    "temp":        0.80,  # current_session — high threshold, most recent context
}

# Minimum certainty for a block to be included in the working context
MIN_RELEVANCE_CERTAINTY = 0.70


async def init_dll() -> dict:
    """
    Initialize the Living DLL with 4 fixed core blocks.
    Vectors are managed by LanceDB (local).
    """
    logger.debug("Initializing DLL — setting up fixed blocks...")

    dll = {
        "agent_id": f"agent-{uuid.uuid4()}",
        "head_id": "current_session",
        "tail_id": "student_profile",
        "dynamic_block_count": 0,
        "dynamic_block_max": settings.MAX_DYNAMIC_BLOCKS,
        "created_at": datetime.now().isoformat(),
        "last_modified": datetime.now().isoformat(),
        "course_selection": {
            "class": settings.EDU_DEFAULT_CLASS,
            "subject": settings.EDU_DEFAULT_SUBJECT
        },
        "nodes": {
            "current_session": {
                "id": "current_session",
                "label": "Current Session",
                "type": "temp",
                "is_fixed": True,
                "created_by": "system",
                "keywords": ["session", "current", "question", "explain", "today", "now", "help"],
                "active": True,
                "access_count": 0,
                "last_accessed": datetime.now().isoformat(),
                "last_modified": datetime.now().isoformat(),
                "prev": None,
                "next": "active_course"
            },
            "active_course": {
                "id": "active_course",
                "label": "Active Course",
                "type": "cours",
                "is_fixed": True,
                "created_by": "system",
                "keywords": ["course", "subject", "chapter", "lesson", "exercise", "exam", "homework", "assignment", "topic"],
                "active": True,
                "access_count": 0,
                "last_accessed": datetime.now().isoformat(),
                "last_modified": datetime.now().isoformat(),
                "prev": "current_session",
                "next": "learning_preferences"
            },
            "learning_preferences": {
                "id": "learning_preferences",
                "label": "Learning Preferences",
                "type": "fondamental",
                "is_fixed": True,
                "created_by": "system",
                "keywords": ["learning", "style", "difficulty", "strength", "weakness", "method", "preference", "practice"],
                "active": True,
                "access_count": 0,
                "last_accessed": datetime.now().isoformat(),
                "last_modified": datetime.now().isoformat(),
                "prev": "active_course",
                "next": "student_profile"
            },
            "student_profile": {
                "id": "student_profile",
                "label": "Student Profile",
                "type": "fondamental",
                "is_fixed": True,
                "created_by": "system",
                "keywords": ["name", "age", "level", "grade", "school", "language", "goal", "background"],
                "active": True,
                "access_count": 0,
                "last_accessed": datetime.now().isoformat(),
                "last_modified": datetime.now().isoformat(),
                "prev": "learning_preferences",
                "next": None
            }
        }
    }

    save_dll(dll)
    return dll


async def force_reinit_dll() -> dict:
    """
    Forcefully resets the DLL to its original 4-block state.
    Deletes all dynamic paging history.
    """
    logger.warning("FORCED REINIT: Wiping dynamic memory blocks...")
    return await init_dll()


async def switch_course(class_level: str, subject: str) -> dict:
    """
    Updates the active course context in the DLL without reinitializing memory.
    Updates course_selection and refreshes the active_course node label.
    """
    dll = await load_dll()
    dll["course_selection"] = {"class": class_level, "subject": subject}

    # Update the label of the active_course node to reflect the new context
    if "active_course" in dll["nodes"]:
        dll["nodes"]["active_course"]["label"] = f"{class_level.upper()} — {subject.title()}"
        dll["nodes"]["active_course"]["last_modified"] = datetime.now().isoformat()

    save_dll(dll)
    logger.info("Course switched to: %s/%s", class_level, subject)
    return dll


async def load_dll(agent_id: str | None = None) -> dict:
    """
    Load the DLL state from disk. Initializes a fresh DLL if no file exists.
    """
    path = settings.METADATA_LINKS_PATH
    if not os.path.exists(path):
        return await init_dll()

    with open(path, "r", encoding="utf-8") as f:
        dll = json.load(f)

    # Ensure course selection exists
    if not isinstance(dll.get("course_selection"), dict):
        dll["course_selection"] = {
            "class": settings.EDU_DEFAULT_CLASS,
            "subject": settings.EDU_DEFAULT_SUBJECT
        }
        save_dll(dll)

    return dll


def save_dll(dll: dict) -> None:
    """
    Persist the DLL state to disk (JSON) — atomic write with exclusive file lock.
    """
    dll["last_modified"] = datetime.now().isoformat()
    path = settings.METADATA_LINKS_PATH
    os.makedirs(os.path.dirname(path), exist_ok=True)

    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            json.dump(dll, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)
    os.replace(tmp_path, path)


def get_head_threshold(dll: dict) -> float:
    """Return the adaptive certainty threshold based on the HEAD node type."""
    head_node = dll["nodes"][dll["head_id"]]
    return CERTAINTY_THRESHOLDS.get(head_node["type"], 0.55)


async def search_memory(query_vector: List[float], class_level: str, subject: str) -> List[Dict]:
    """
    Bidirectional Metadata Jump (BMJ) — powered by LanceDB vector search.
    Replaces the Weaviate near_text with a local embedding search.
    """
    logger.debug("DLL Search | class='%s' subject='%s'", class_level, subject)

    results = await lance_driver.search_block_index(
        query_vector,
        limit=12,
        class_level=class_level,
        subject=subject
    )

    filtered = []
    for res in results:
        b_type = res.get("block_type", "cours")
        threshold = CERTAINTY_THRESHOLDS.get(b_type, MIN_RELEVANCE_CERTAINTY)
        if res["certainty"] >= threshold:
            filtered.append(res)

    # BMJ Algorithm: Move the most relevant DLL memory node to HEAD.
    # Only applies to DLL nodes (student_profile, learning_preferences, etc.),
    # NOT to course content blocks returned from LanceDB (chapitre_X, etc.).
    if filtered:
        dll = await load_dll()
        dll_node_ids = set(dll.get("nodes", {}).keys())
        for block in filtered:
            block_id = block.get("block_id", "")
            if block_id in dll_node_ids:
                dll = move_to_front(block_id, dll)
                save_dll(dll)
                logger.debug("BMJ | Moved to HEAD: %s", block_id)
                break  # Only promote the first matching DLL node

    return filtered


def toggle_block(block_id: str, state: bool, dll: dict) -> dict:
    """Enable or disable a block."""
    if block_id in dll["nodes"]:
        dll["nodes"][block_id]["active"] = state
    return dll


from app_local.mmu import cache_l1

async def update_node_content(block_id: str, content: str, dll: dict) -> dict:
    """
    Updates a node's content in both the DLL (metadata) and LanceDB (user_memory).
    Follows the ALU invalidation pattern:
        1. Invalidate L1 (prevent stale reads during write window)
        2. Update LanceDB (L3 persistence)
        3. Re-populate L1 (write-back)
        4. Update DLL JSON (L2 index)
    """
    if block_id not in dll["nodes"]:
        return dll

    node = dll["nodes"][block_id]

    # 1. Invalidate L1 before write
    cache_l1.invalidate(block_id)

    # 2. Persist to LanceDB (L3) — user_memory table
    from langchain_google_genai import GoogleGenerativeAIEmbeddings
    embeddings_model = GoogleGenerativeAIEmbeddings(model="models/gemini-embedding-2")
    vector = await embeddings_model.aembed_query(content)

    await lance_driver.upsert_local_block(
        block_id=block_id,
        content=content,
        block_type=node["type"],
        class_level=dll.get("course_selection", {}).get("class", "general"),
        subject=dll.get("course_selection", {}).get("subject", "general"),
        vector=vector
    )

    # 3. Write-back to L1 with correct TTL
    cache_l1.set(block_id, content, block_type=node["type"])

    # 4. Update DLL JSON metadata
    node["content"] = content
    node["last_modified"] = datetime.now().isoformat()
    save_dll(dll)

    return dll


def move_to_front(block_id: str, dll: dict) -> dict:
    """Move the selected node to HEAD position (BMJ algorithm)."""
    if dll["head_id"] == block_id:
        return dll
    nodes = dll["nodes"]
    target = nodes[block_id]
    prev_id, next_id = target["prev"], target["next"]
    if prev_id: nodes[prev_id]["next"] = next_id
    if next_id: nodes[next_id]["prev"] = prev_id
    if dll["tail_id"] == block_id: dll["tail_id"] = prev_id
    old_head = dll["head_id"]
    nodes[old_head]["prev"] = block_id
    target["prev"], target["next"] = None, old_head
    dll["head_id"] = block_id
    return dll


def get_all_nodes(dll: dict) -> list:
    """Return all nodes in HEAD → TAIL order (DLL traversal)."""
    nodes, current = [], dll["head_id"]
    visited = set()
    while current and current not in visited:
        visited.add(current)
        nodes.append(dll["nodes"][current])
        current = dll["nodes"][current]["next"]
    return nodes


def _head_to_tail_order(dll: dict) -> list:
    """Traverse DLL from HEAD to TAIL, returning ordered node IDs."""
    order, current = [], dll["head_id"]
    visited = set()
    while current and current not in visited:
        visited.add(current)
        order.append(current)
        current = dll["nodes"][current]["next"]
    return order


def _tail_to_head_order(dll: dict) -> list:
    """Traverse DLL from TAIL to HEAD, returning ordered node IDs."""
    order, current = [], dll["tail_id"]
    visited = set()
    while current and current not in visited:
        visited.add(current)
        order.append(current)
        current = dll["nodes"][current]["prev"]
    return order
