from datetime import datetime
from memory.dll_manager import update_node_keywords, save_dll
from memory.weaviate_cloud_client import (
    get_weaviate_client,
    upsert_block_index,
    delete_block_index,
    ingest_block,
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


def delete_block_stitching(block_id: str, dll: dict) -> dict:
    """
    Remove a dynamic block from the DLL and re-stitch its neighbors.
    Also removes it from Weaviate BlockIndex and TravelDynamic.
    """
    nodes = dll["nodes"]

    if block_id not in nodes:
        raise ValueError(f"Block '{block_id}' does not exist.")

    target = nodes[block_id]

    if target.get("is_fixed", False):
        raise ValueError(f"Block '{block_id}' is a fixed block and cannot be deleted.")

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

    # Clean up Weaviate
    try:
        client = get_weaviate_client()
        try:
            delete_block_index(client, block_id)
            delete_block_vectors(client, block_id)
        finally:
            client.close()
    except Exception as e:
        logger.warning("Weaviate cleanup failed for '%s': %s", block_id, e)

    logger.debug("Block '%s' removed from DLL. Dynamic count: %d", block_id, dll["dynamic_block_count"])
    return dll


def create_dynamic_block(
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
    Create a new dynamic block:
        1. Insert into DLL at correct position
        2. Index keywords in Weaviate BlockIndex
        3. Sync content to Letta + Weaviate TravelDynamic
    """
    if dll["dynamic_block_count"] >= dll["dynamic_block_max"]:
        raise ValueError(
            f"Dynamic block limit reached ({dll['dynamic_block_max']} blocks maximum)."
        )

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
    logger.debug("Dynamic block '%s' created (type=%s).", block_id, block_type)

    # Index keywords in Weaviate BlockIndex
    try:
        client = get_weaviate_client()
        try:
            upsert_block_index(client, block_id, keywords, block_type)
            logger.debug("BlockIndex: '%s' indexed.", block_id)
        finally:
            client.close()
    except Exception as e:
        logger.warning("BlockIndex indexing failed for '%s': %s", block_id, e)

    # Sync to Letta Core Memory
    try:
        if letta_client:
            agent_id = dll.get("agent_id")
            if agent_id:
                letta_client.append_block(agent_id, block_id, initial_content, block_type)
                logger.debug("Letta sync: block '%s' appended.", block_id)
    except Exception as e:
        logger.warning("Letta sync skipped for '%s': %s", block_id, e)

    # Sync to Weaviate TravelDynamic
    try:
        if wcd_client:
            client = wcd_client.get_weaviate_client()
            try:
                wcd_client.ingest_block(client, "TravelDynamic", block_id, block_type, initial_content)
                logger.debug("Weaviate content sync: block '%s' ingested.", block_id)
            finally:
                client.close()
    except Exception as e:
        logger.warning("Weaviate content sync skipped for '%s': %s", block_id, e)

    return dll


def update_block_content(
    block_id: str,
    new_content: str,
    new_keywords: list[str],
    dll: dict,
    letta_client,
    wcd_client,
) -> dict:
    """
    Cascade Update — synchronizes across all 3 stores:
        1. Letta Cloud Core Memory
        2. Weaviate BlockIndex (re-index keywords)
        3. Weaviate content collection (re-ingest)
    """
    nodes = dll["nodes"]
    if block_id not in nodes:
        raise ValueError(f"Block '{block_id}' not found in DLL.")

    node = nodes[block_id]

    # 1. Update Letta Core Memory
    try:
        agent_id = dll.get("agent_id")
        if agent_id and letta_client:
            letta_client.update_block(agent_id, block_id, new_content)
            logger.debug("Cascade: Letta block '%s' updated.", block_id)
    except Exception as e:
        logger.error("Cascade: Letta update failed for '%s': %s", block_id, e)

    # 2. Re-index keywords in Weaviate BlockIndex
    dll = update_node_keywords(block_id, new_keywords, dll)

    # 3. Weaviate content: delete old, re-ingest
    try:
        if wcd_client:
            client = wcd_client.get_weaviate_client()
            try:
                collection = "TravelFixed" if node.get("is_fixed") else "TravelDynamic"
                wcd_client.delete_block_vectors(client, block_id)
                wcd_client.ingest_block(client, collection, block_id, node["type"], new_content)
                logger.debug("Cascade: Weaviate block '%s' re-ingested.", block_id)
            finally:
                client.close()
    except Exception as e:
        logger.error("Cascade: Weaviate sync failed for '%s': %s", block_id, e)

    node["last_modified"] = datetime.now().isoformat()
    save_dll(dll)
    return dll
