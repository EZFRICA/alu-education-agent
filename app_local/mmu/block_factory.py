from datetime import datetime
from typing import Optional

from app_local.config import settings
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

def _refuse_zero_vector(block_id: str):
    raise ValueError(
        f"create_dynamic_block('{block_id}') was given no vector. Writing a "
        f"zero vector would put an unsearchable row into a semantic index; "
        f"embed the content first (see auto_execute_block_proposal)."
    )


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
        # Semantic MMU: page out the least recently accessed block.
        #
        # `.get(key, default)` returned the STORED None rather than the default,
        # so min() compared None < None and raised TypeError -- the cap raised
        # instead of evicting, and the working set was never bounded at all.
        # A node that has never been accessed sorts oldest, by creation time.
        dynamic_nodes = [n for n in dll["nodes"].values() if not n.get("is_fixed")]
        if dynamic_nodes:
            def _lru_key(node):
                return (
                    node.get("last_accessed")
                    or node.get("last_modified")
                    or "1970-01-01T00:00:00"
                )
            lru_node = min(dynamic_nodes, key=_lru_key)
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
        # A zero vector is equidistant from every query, so the block would be
        # written and never retrievable. Callers must supply a real embedding.
        vector=vector if vector else _refuse_zero_vector(block_id),
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
    Execute a block proposal, or refuse it.

    Returns True only if the block was really created and is really searchable.
    Callers must honour the return value — the dashboard used to announce
    "Entry created!" regardless.
    """
    from app_local.core.block_proposal import validate
    from app_local.mmu.controller import load_dll

    problem = validate(proposal)
    if problem:
        logger.error("Refusing block proposal: %s | %r", problem, proposal)
        return False

    try:
        # A real embedding of the real content. Previously no vector was ever
        # computed and create_dynamic_block fell back to a zero vector, which is
        # equidistant from every query: the block was written and could never be
        # retrieved. Refusing beats writing a permanently invisible block.
        from llm_provider import get_embedder
        vector = await get_embedder().aembed_query(proposal["initial_content"])
    except Exception as e:
        logger.error("Refusing block proposal: could not embed content (%s)", e)
        return False

    try:
        dll_latest = await load_dll()
        await create_dynamic_block(
            block_id=proposal["proposed_id"],
            label=proposal["label"],
            block_type=proposal["type"],
            initial_content=proposal["initial_content"],
            keywords=proposal.get("keywords", []),
            created_by="Akili",
            dll=dll_latest,
            vector=vector,
        )
        return True
    except Exception as e:
        logger.error(f"Auto-execute failed: {e}")
        return False
