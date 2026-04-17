from datetime import datetime
from typing import Optional
from apu.core.scheduler import scheduler
from apu.mmu.controller import update_node_keywords, save_dll, get_dll_lock
from apu.core.pipeline import get_core_block_content
from apu.storage.weaviate_driver import (
    get_weaviate_client_async,
    upsert_block_index,
    delete_block_index,
    upsert_block_content,
    delete_block_vectors,
)
from logger import get_logger

logger = get_logger(__name__)


def insert_node_by_type(block_type: str, new_node: dict, dll: dict) -> dict:
    """
    Insert a new node at the position matching its semantic type:
        temp        → HEAD (most recent context)
        projet      → after HEAD (mid-list, active planning)
        fondamental → before TAIL (permanent knowledge, always reachable)
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
    Remove a dynamic block from the DLL and re-stitch its neighbors.
    Also removes it from Weaviate BlockIndex and TravelDynamic.
    Wrapped in isolation lock to prevent race conditions.
    """
    agent_id = dll.get("agent_id")
    if not agent_id:
        raise ValueError("Agent ID is not defined in the DLL.")

    async with get_dll_lock(agent_id):
        nodes = dll["nodes"]
        if block_id not in nodes:
            raise ValueError(f"Block '{block_id}' does not exist.")

        target = nodes[block_id]
        if target.get("is_fixed", False):
            raise ValueError(f"Block '{block_id}' is a fixed block and cannot be deleted.")

        # 1. Clean up Letta & Weaviate FIRST
        try:
            # Note: delete_block vectors from Weaviate
            async with get_weaviate_client_async() as client:
                await delete_block_index(client, block_id, agent_id)
                await delete_block_vectors(client, block_id, agent_id)
            
            # Note: Logical delete from Letta
            from apu.storage.letta_driver import delete_block
            await delete_block(agent_id, block_id)
            
        except Exception as e:
            # If external cleanup fails, we raise an error and do NOT touch the local DLL
            # This ensures we don't end up with vectors in Weaviate but no link in JSON
            logger.error("External cleanup failed for '%s': %s. Aborting DLL removal.", block_id, e)
            raise RuntimeError(f"Sync failed during block deletion. DLL remains intact to avoid ghost vectors. Error: {e}")

        # 2. Update Local DLL State (Only if external cleanup succeeded)
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
        logger.debug("Block '%s' fully removed from DLL and External DBs. count: %d", block_id, dll["dynamic_block_count"])
    
    return dll


async def page_out_block(block_id: str, dll: dict) -> dict:
    """
    Semantic MMU: Remove a block from the active DLL (Swap to disk).
    Data remains in Letta (L4) and Weaviate (L3).
    Only the pointer in metadata_links.json is removed.
    """
    nodes = dll["nodes"]
    if block_id not in nodes:
        return dll
    
    target = nodes[block_id]
    if target.get("is_fixed", False):
        return dll # Never page out fixed blocks

    # Update local DLL State (Unlink)
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
    logger.info("Block '%s' PAGED OUT from active memory (Swapped to L3/L4).", block_id)
    return dll


async def create_dynamic_block(
    block_id: str,
    label: str,
    block_type: str,
    initial_content: str,
    keywords: list[str],
    created_by: str,
    dll: dict,
    letta_client,
    wcd_client,
) -> dict:
    """
    Create a new dynamic block using ACID-like principles.
    Enforces strict synchronization: Letta -> Weaviate -> Local JSON.
    If Letta or Weaviate fails, the operation aborts and local state remains untouched.
    """
    if dll["dynamic_block_count"] >= dll["dynamic_block_max"]:
        # Semantic MMU: Page Out oldest block to make room
        dynamic_nodes = [n for n in dll["nodes"].values() if not n.get("is_fixed")]
        if dynamic_nodes:
            # Sort by last_accessed (None counts as epoch 0)
            lru_node = min(dynamic_nodes, key=lambda x: x.get("last_accessed", "1970-01-01T00:00:00"))
            logger.info("Semantic MMU: Paging Out '%s' (LRU) to make room for new block.", lru_node["id"])
            await page_out_block(lru_node["id"], dll)
        else:
            raise ValueError("No dynamic blocks available to page out.")

    if block_id in dll["nodes"]:
        raise ValueError(f"Block '{block_id}' already exists.")

    agent_id = dll.get("agent_id")
    if not agent_id:
        raise ValueError("Agent ID is not defined in the DLL.")

    # 1. Sync to Letta Cloud (DEFERRED TO SCHEDULER)
    async with get_dll_lock(agent_id):
        if letta_client:
            await scheduler.push(
                task_type="SYNC_LETTA",
                payload={
                    "agent_id": agent_id,
                    "block_id": block_id,
                    "content": initial_content
                },
                priority=2 # Background
            )
            logger.debug("Cascade: Letta creation task PUSHED to scheduler for '%s'.", block_id)
        # 2. Sync to Weaviate Cloud (Search Index & Content Backup)
        if wcd_client:
            try:
                async with wcd_client.get_weaviate_client_async() as client:
                    # 2A: Index Keywords
                    await upsert_block_index(client, block_id, keywords, block_type, agent_id)
                    # 2B: Ingest initial Content (predictable UUID for L3/L4 retrieval)
                    await wcd_client.upsert_block_content(client, "TravelDynamic", block_id, block_type, initial_content, agent_id)
                    logger.debug("Weaviate content & index sync: block '%s' upserted.", block_id)
            except Exception as e:
                # Note: Weaviate failed. Strictly speaking, we should delete from Letta here to rollback completely.
                logger.error("Weaviate sync failed for '%s'. Assuming Letta succeeded but Weaviate failed. Error: %s", block_id, e)
                try:
                    if letta_client:
                        await letta_client.delete_block(agent_id, block_id)
                        logger.warning("Rollback: Letta block '%s' deleted after Weaviate failure.", block_id)
                except Exception as rollback_err:
                    logger.error(
                        "Rollback FAILED for block '%s': %s. "
                        "Letta and Weaviate may be out of sync — manual cleanup required.",
                        block_id, rollback_err,
                    )
                raise RuntimeError(f"Failed to create block in Weaviate. Rollback triggered. Error: {e}")

        # 3. Update Local DLL State (Only if external DBs succeed)
        new_node = {
            "id": block_id,
            "label": label,
            "letta_block_label": block_id,
            "weaviate_collection": "TravelDynamic",
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
        logger.debug("Dynamic block '%s' fully created and persisted to JSON (type=%s).", block_id, block_type)
    
    return dll


async def update_block_content(
    block_id: str,
    new_content: str,
    new_keywords: list[str],
    dll: dict,
    letta_client,
    wcd_client,
    old_content: Optional[str] = None,
) -> dict:
    """
    Cascade Update (ACID-like) — synchronizes across all 3 stores:
        1. Letta Cloud Core Memory (Source of Truth)
        2. Weaviate BlockIndex (re-index keywords) & Weaviate content
        3. Local JSON (Last, only if 1 and 2 succeed)
    
    Includes compensatory rollback for Letta if Weaviate fails.
    """
    nodes = dll["nodes"]
    if block_id not in nodes:
        raise ValueError(f"Block '{block_id}' not found in DLL.")

    node = nodes[block_id]
    agent_id = dll.get("agent_id")
    if not agent_id:
        raise ValueError("Agent ID is not defined in the DLL.")

    async with get_dll_lock(agent_id):
        # 0. Capture old content for potential rollback (skip if already provided by caller)
        if old_content is None:
            old_content = await get_core_block_content(agent_id, block_id)
        
        # 1. Update Letta Core Memory (DEFERRED TO SCHEDULER)
        # We don't await this, we push it to the background sync worker.
        await scheduler.push(
            task_type="SYNC_LETTA",
            payload={
                "agent_id": agent_id,
                "block_id": block_id,
                "content": new_content
            },
            priority=2 # Background
        )
        logger.debug("Cascade: Letta sync task PUSHED to scheduler for '%s'.", block_id)

        # 2. Update Weaviate (Content and Keywords)
        if wcd_client:
            try:
                async with wcd_client.get_weaviate_client_async() as client:
                    # 2A: Re-ingest content (deterministic UUID overwrite)
                    collection = "TravelFixed" if node.get("is_fixed") else "TravelDynamic"
                    await wcd_client.upsert_block_content(client, collection, block_id, node["type"], new_content, agent_id)
                    logger.debug("Cascade: Weaviate block '%s' upserted into '%s'.", block_id, collection)
            except Exception as e:
                logger.error("Cascade: Weaviate sync failed for '%s'. Rollback triggered. Error: %s", block_id, e)
                # Rollback Letta to previous content
                try:
                    if letta_client and old_content is not None:
                        await letta_client.update_block(agent_id, block_id, old_content)
                        logger.warning("Rollback: Letta block '%s' restored to previous content.", block_id)
                except Exception as rollback_err:
                    logger.error("Rollback FAILED for '%s': %s. Manual sync required.", block_id, rollback_err)
                
                raise RuntimeError(f"Failed to update block in Weaviate. Rollback attempted. Error: {e}")

        # 2.5. L1 cache — invalidate stale entry, re-populate with new content
        #      Placed after Weaviate success, before JSON write:
        #      ensures next BMJ read hits L1 with fresh content immediately.
        from apu.mmu import cache_l1 as block_cache
        block_cache.invalidate(block_id)
        block_cache.set(block_id, new_content, node.get("type"))
        block_cache.record_write_back(block_id)
        logger.debug("L1 cache refreshed for '%s'.", block_id)

        # 3. Update Local DLL State (Only if external APIs succeeded)
        dll = await update_node_keywords(block_id, new_keywords, dll)
        # Ensure node exists in our reference after update
        node = dll["nodes"][block_id]
        node["last_modified"] = datetime.now().isoformat()
        save_dll(dll)
        logger.debug("Cascade: block '%s' metadata saved to JSON.", block_id)
    
    return dll
