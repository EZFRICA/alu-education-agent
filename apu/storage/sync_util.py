import os
import sys
import asyncio

# Add project root to path for module imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from apu.mmu.controller import load_dll, get_all_nodes
from apu.storage.weaviate_driver import get_weaviate_client_async, get_block_content
from apu.storage.letta_driver import update_block, append_block
from logger import get_logger

logger = get_logger(__name__)

async def _push_node_to_letta(client, node: dict, agent_id: str) -> None:
    """
    Read content from Weaviate and Push it to Letta Cloud.
    """
    b_id = node["id"]
    coll = "TravelFixed" if node.get("is_fixed") else "TravelDynamic"
    
    try:
        # Step 1: Read content from Weaviate (Source of truth)
        content = await get_block_content(client, coll, b_id, agent_id)
        
        if not content:
            logger.debug("Skipping '%s' — no content found in Weaviate.", b_id)
            return
            
        # Step 2: Push content to Letta Cloud
        try:
            # Assume it exists and try to update
            await update_block(agent_id, b_id, content)
            logger.info("Pushed '%s' to Letta Cloud (Updated).", b_id)
        except Exception as update_err:
            # If update fails (e.g. 404), fallback to create & attach
            logger.debug("Update failed for '%s', attempting creation...", b_id)
            await append_block(agent_id, b_id, content)
            logger.info("Pushed '%s' to Letta Cloud (Created).", b_id)
            
    except Exception as e:
        logger.error("Push failed for block '%s': %s", b_id, e)


async def sync_all():
    """
    Push all local DLL memory (Weaviate) up to Letta Cloud.
    Runs concurrently.
    """
    dll = await load_dll()
    agent_id = dll.get("agent_id")

    if not agent_id:
        logger.error("No agent_id found in DLL metadata.")
        return

    async with get_weaviate_client_async() as client:
        nodes = get_all_nodes(dll)
        logger.info("Starting upward sync to Letta for %d blocks...", len(nodes))

        # Dispatch all sync tasks concurrently
        tasks = [_push_node_to_letta(client, node, agent_id) for node in nodes]
        await asyncio.gather(*tasks)

        logger.info("Sync complete — Local Weaviate State pushed to Letta Cloud.")


if __name__ == "__main__":
    asyncio.run(sync_all())
