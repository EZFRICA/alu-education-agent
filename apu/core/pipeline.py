"""
context_compiler.py — Async-first working context builder.

Fixes applied:
- [Fix 3] Filter [DELETED_BLOCK] zombie blocks before injecting into Gemini prompt.
- [Fix 4] Parallelize all Letta HTTP reads natively with asyncio.gather().
          Previously: Sequential blocking calls, then asyncio.to_thread wrappers.
          Now: Native non-blocking async calls via AsyncLetta.
"""

import asyncio
from typing import Optional
from logger import get_logger

logger = get_logger(__name__)


async def get_core_block_content(agent_id: str, label: str) -> Optional[str]:
    """
    Fetch the content of a core memory block from Letta Cloud API (Async).
    Hierarchy: L1 Cache -> L3 Weaviate -> L4 Letta Cloud.
    """
    from apu.mmu import cache_l1 as block_cache
    from apu.storage import weaviate_driver as wcd_client
    from apu.mmu.controller import load_dll

    # ── 1. L1 Hit check ───────────────────────────────────────────────────────
    cached = await block_cache.get(label)
    if cached is not None:
        return cached

    # ── 2. L3 Weaviate Check ──────────────────────────────────────────────────
    try:
        # Determine collection from DLL metadata
        dll = await load_dll(agent_id)
        node = dll["nodes"].get(label)
        if node:
            collection = "TravelFixed" if node.get("is_fixed") else "TravelDynamic"
            async with wcd_client.get_weaviate_client_async() as client:
                wcd_content = await wcd_client.get_block_content(client, collection, label, agent_id)
                if wcd_content:
                    block_cache.set(label, wcd_content, node.get("type"))
                    logger.debug("L3 HIT (Direct) — '%s' fetched from Weaviate.", label)
                    return wcd_content
    except Exception as e:
        logger.debug("L3 Direct Fetch failed for '%s': %s", label, e)

    # ── 3. L4 Letta Fallback ──────────────────────────────────────────────────
    from apu.storage.letta_driver import get_letta_client_async
    letta = get_letta_client_async()
    try:
        block = await letta.agents.blocks.retrieve(label, agent_id=agent_id)
        if block.value is not None:
            # Warm up L1/L3 for next time
            val = block.value
            block_cache.set(label, val)
            return val
    except Exception as e:
        if "404" in str(e):
            return None
        logger.warning("Could not read block '%s' from Letta API (Async): %s", label, e)
    return ""


async def _fetch_block_async(agent_id: str, block: dict) -> tuple[dict, str]:
    """
    Fetch a single block's content (native async).

    Hierarchy Logic (APU-First):
        1. L1 Cache    — (~0ns)   In-process hit.
        2. L3 Weaviate — (~10ms)  Direct ID fetch (predictable UUID).
        3. L4 Letta    — (~200ms) Fallback if L3 is missing/cold.

    Returns (block_meta, content) tuple.
    """
    from apu.mmu import cache_l1 as block_cache
    from apu.storage import weaviate_driver as wcd_client

    block_id = block["id"]
    block_type = block.get("type")
    is_fixed = block.get("is_fixed", False)
    collection = "TravelFixed" if is_fixed else "TravelDynamic"

    # ── 1. L1 Check ───────────────────────────────────────────────────────────
    cached = await block_cache.get(block_id)
    if cached is not None:
        return block, cached

    # ── 2. L3 Weaviate Check (Direct ID Fetch) ────────────────────────────────
    try:
        async with wcd_client.get_weaviate_client_async() as client:
            wcd_content = await wcd_client.get_block_content(client, collection, block_id, agent_id)
            if wcd_content:
                # Re-populate L1 for next turn
                block_cache.set(block_id, wcd_content, block_type)
                logger.debug("L3 HIT — '%s' fetched from Weaviate.", block_id)
                return block, wcd_content
    except Exception as e:
        logger.warning("L3 Fetch Error for '%s': %s", block_id, e)

    # ── 3. L4 Letta Fallback ──────────────────────────────────────────────────
    content = await get_core_block_content(agent_id, block_id) or ""
    
    if content:
        # Write-back to L3 (Warm up) and L1
        async with wcd_client.get_weaviate_client_async() as client:
            await wcd_client.upsert_block_content(client, collection, block_id, block_type, content, agent_id)
        block_cache.set(block_id, content, block_type)
        logger.debug("L4 FALLBACK — '%s' fetched from Letta Cloud (Warming up L3).", block_id)

    return block, content


async def compile_working_context(
    agent_id: str, relevant_blocks: list[dict], query: str = ""
) -> str:
    """
    Assemble the final Working Context string to inject into the Gemini prompt.

    Optimizations vs previous version:
    - All Letta HTTP calls run in parallel (asyncio.gather + to_thread).
    - Zombie blocks (value starts with '[DELETED') are filtered out silently.
    """
    if not relevant_blocks:
        return ""

    # Parallel fetch — all blocks fetched concurrently from Letta Cloud
    fetch_tasks = [_fetch_block_async(agent_id, b) for b in relevant_blocks]
    results: list[tuple[dict, str]] = await asyncio.gather(*fetch_tasks)

    context_parts = []
    injected = 0

    for block, content in results:
        # Fix 3: Skip zombie / soft-deleted blocks
        if content.strip().startswith("[DELETED"):
            logger.debug("Block '%s' skipped (soft-deleted).", block["id"])
            continue

        context_parts.append(f"--- BLOCK: {block['label'].upper()} ({block['type']}) ---")
        context_parts.append(content)
        context_parts.append("")  # blank line separator
        injected += 1

    logger.debug(
        "Working context compiled: %d/%d blocks injected.",
        injected, len(relevant_blocks),
    )
    return "\n".join(context_parts)
