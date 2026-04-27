from datetime import datetime
from typing import Optional
import asyncio

from app_local.core.scheduler import scheduler
from app_local.mmu.controller import update_node_content, save_dll, get_dll_lock
from app_local.storage import lance_driver
from logger import get_logger

logger = get_logger(__name__)

def insert_node_by_type(block_type: str, new_node: dict, dll: dict) -> dict:
    """
    Inserts a new node based on its semantic priority:
        temp        → HEAD (recent context)
        projet      → Middle (active planning)
        fondamental → Before TAIL (permanent knowledge)
    """
    nodes = dll["nodes"]

    if block_type == "temp":
        old_head = dll["head_id"]
        new_node["next"] = old_head
        new_node["prev"] = None
        if old_head:
            nodes[old_head]["prev"] = new_node["id"]
        dll["head_id"] = new_node["id"]

    elif block_type == "fondamental":
        old_tail = dll["tail_id"]
        if old_tail:
            prev_to_tail = nodes[old_tail]["prev"]
            new_node["next"] = old_tail
            new_node["prev"] = prev_to_tail
            nodes[old_tail]["prev"] = new_node["id"]
            if prev_to_tail:
                nodes[prev_to_tail]["next"] = new_node["id"]
            else:
                dll["head_id"] = new_node["id"]
        else:
            dll["head_id"] = dll["tail_id"] = new_node["id"]

    else:  # projet — insert after HEAD
        head = dll["head_id"]
        if head:
            next_to_head = nodes[head]["next"]
            new_node["prev"] = head
            new_node["next"] = next_to_head
            nodes[head]["next"] = new_node["id"]
            if next_to_head:
                nodes[next_to_head]["prev"] = new_node["id"]
            else:
                dll["tail_id"] = new_node["id"]
        else:
            dll["head_id"] = dll["tail_id"] = new_node["id"]

    nodes[new_node["id"]] = new_node
    return dll

async def delete_block_stitching(block_id: str, dll: dict) -> dict:
    """
    Deletes a block from the DLL and local LanceDB.
    """
    agent_id = dll.get("agent_id")
    async with get_dll_lock(agent_id):
        nodes = dll["nodes"]
        if block_id not in nodes:
            raise ValueError(f"Block '{block_id}' does not exist.")

        target = nodes[block_id]
        if target.get("is_fixed", False):
            raise ValueError(f"Block '{block_id}' is fixed and cannot be deleted.")

        # 1. Local deletion in LanceDB
        await lance_driver.delete_local_block(block_id)

        # 2. Update DLL chain
        prev_id, next_id = target["prev"], target["next"]
        if prev_id:
            nodes[prev_id]["next"] = next_id
        if next_id:
            nodes[next_id]["prev"] = prev_id

        if dll["head_id"] == block_id:
            dll["head_id"] = next_id
        if dll["tail_id"] == block_id:
            dll["tail_id"] = prev_id

        del nodes[block_id]
        dll["dynamic_block_count"] = max(0, dll["dynamic_block_count"] - 1)
        
        save_dll(dll)
        logger.info(f"Block '{block_id}' deleted locally.")
    
    return dll

async def page_out_block(block_id: str, dll: dict) -> dict:
    """
    Deactivates a block (swap to disk). It stays in LanceDB but leaves the active DLL.
    """
    nodes = dll["nodes"]
    if block_id not in nodes:
        return dll
    
    target = nodes[block_id]
    if target.get("is_fixed", False):
        return dll

    prev_id, next_id = target["prev"], target["next"]
    if prev_id:
        nodes[prev_id]["next"] = next_id
    if next_id:
        nodes[next_id]["prev"] = prev_id

    if dll["head_id"] == block_id:
        dll["head_id"] = next_id
    if dll["tail_id"] == block_id:
        dll["tail_id"] = prev_id

    del nodes[block_id]
    dll["dynamic_block_count"] = max(0, dll["dynamic_block_count"] - 1)
    
    save_dll(dll)
    logger.info(f"Block '{block_id}' PAGED OUT (Moved to local storage).")
    return dll

async def create_dynamic_block(
    block_id: str,
    label: str,
    block_type: str,
    initial_content: str,
    keywords: list[str],
    created_by: str,
    dll: dict,
    vector: Optional[list[float]] = None
) -> dict:
    """
    Creates a dynamic block in the DLL and LanceDB.
    """
    if dll["dynamic_block_count"] >= dll["dynamic_block_max"]:
        # Semantic MMU: Page Out oldest block
        dynamic_nodes = [n for n in dll["nodes"].values() if not n.get("is_fixed")]
        if dynamic_nodes:
            lru_node = min(dynamic_nodes, key=lambda x: x.get("last_accessed", "1970-01-01T00:00:00"))
            await page_out_block(lru_node["id"], dll)

    if block_id in dll["nodes"]:
        raise ValueError(f"Block '{block_id}' already exists.")

    agent_id = dll.get("agent_id")

    # 1. Save in local LanceDB
    await lance_driver.upsert_local_block(
        block_id=block_id,
        content=initial_content,
        block_type=block_type,
        class_level="local",
        subject="local",
        vector=vector or ([0.0] * 768)
    )

    # 2. Update Local DLL State
    new_node = {
        "id": block_id,
        "label": label,
        "type": block_type,
        "is_fixed": False,
        "created_by": created_by,
        "keywords": keywords,
        "active": True,
        "access_count": 0,
        "last_accessed": None,
        "last_modified": datetime.now().isoformat(),
        "prev": None,
        "next": None,
    }

    dll = insert_node_by_type(block_type, new_node, dll)
    dll["dynamic_block_count"] += 1
    save_dll(dll)
    
    return dll

async def update_block_content(
    block_id: str,
    new_content: str,
    new_keywords: list[str],
    dll: dict,
    vector: Optional[list[float]] = None
) -> dict:
    """
    Updates a block's content locally.
    """
    nodes = dll["nodes"]
    if block_id not in nodes:
        raise ValueError(f"Block '{block_id}' not found.")

    node = nodes[block_id]
    agent_id = dll.get("agent_id")

    async with get_dll_lock(agent_id):
        # Use centralized update_node_content to persist to both DLL and LanceDB
        dll = await update_node_content(block_id, new_content, dll)
    
    return dll

async def auto_execute_block_proposal(proposal: dict) -> bool:
    """
    Automatically executes a block proposal detected by the agent.
    """
    from app_local.mmu.controller import load_dll
    try:
        dll_latest = await load_dll()
        await create_dynamic_block(
            block_id=proposal.get("proposed_id"),
            label=proposal.get("label"),
            block_type=proposal.get("type"),
            initial_content=proposal.get("initial_content"),
            keywords=proposal.get("keywords", []),
            created_by="Akili",
            dll=dll_latest
        )
        return True
    except Exception as e:
        logger.error(f"Auto-execute failed: {e}")
        return False
