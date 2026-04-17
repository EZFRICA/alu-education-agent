"""
block_cache.py — L1 in-process content cache for DLL blocks.

Sits between the BMJ router (which selects block_ids) and Letta Cloud
(which stores block contents). A cache hit eliminates a ~100ms Letta HTTP call.

Architecture position:
    BMJ → [block_ids] → L1 BlockCache → hit:  content direct (~0ns)
                                       → miss: Letta HTTP → write-back to L1

Invalidation triggers (3):
    1. write-back loop      — auto-repopulated after each agent response
    2. update_block_content — explicit invalidate + re-populate after ACID update
    3. Force Override       — operator disables/modifies block from dashboard

Design decisions:
    - In-process dict (not Redis): ~0 ns latency vs ~5 ms Redis, no extra dependency.
      Redis is L2 (DLL). This is L1.
    - TTL by block type: temp=60s, projet=180s, fondamental=600s.
      Fondamental blocks are stable; temp blocks change each session.
    - Single global asyncio.Lock for reads (TTL check + eviction).
      Writes (set/invalidate) are dict mutations protected by the GIL — no lock needed.
    - Module-level singleton: no object to plumb through every function call.
"""

import asyncio
import time
from typing import Optional

from logger import get_logger

logger = get_logger(__name__)


# ── TTL by block type ─────────────────────────────────────────────────────────
_TTL_BY_TYPE: dict[str, int] = {
    "temp":        300,  # Session block — long turns
    "projet":     600,   # Itinerary — stable
    "fondamental": 1800, # Profile/prefs — very stable
}
_TTL_DEFAULT = 300  # Fallback for unknown types


# ── Internal state (module-level singleton) ───────────────────────────────────
# _cache: block_id → (content, expiry_monotonic_ts)
_cache: dict[str, tuple[str, float]] = {}

# _metrics: block_id → stats dict
_metrics: dict[str, dict] = {}

# Single global lock — 4–8 blocks max in practice, contention is negligible
_lock = asyncio.Lock()


# ── Internal helpers ──────────────────────────────────────────────────────────

def _ensure_metrics(block_id: str) -> None:
    """Initialize metrics entry for a block if not already present."""
    if block_id not in _metrics:
        _metrics[block_id] = {
            "l1_hits": 0,
            "l1_misses": 0,
            "write_backs": 0,
            "last_hit_at": None,
            "last_miss_at": None,
            "last_write_back_at": None,
        }


def _get_ttl(block_type: Optional[str]) -> int:
    """Return TTL in seconds for a given block type."""
    return _TTL_BY_TYPE.get(block_type or "", _TTL_DEFAULT)


# ── Public API ────────────────────────────────────────────────────────────────

async def get(block_id: str) -> Optional[str]:
    """
    Attempt to retrieve block content from L1 cache.

    Returns:
        str   — content on hit (TTL not expired)
        None  — on miss or TTL expiry (caller should fetch from Letta)
    """
    async with _lock:
        _ensure_metrics(block_id)
        entry = _cache.get(block_id)
        now = time.monotonic()

        if entry is not None:
            content, expiry = entry
            if now < expiry:
                # ✅ Cache hit
                _metrics[block_id]["l1_hits"] += 1
                _metrics[block_id]["last_hit_at"] = time.time()
                logger.debug("L1 HIT    — '%s'", block_id)
                return content
            else:
                # ⏱ TTL expired — silent eviction
                del _cache[block_id]
                logger.debug("L1 EVICT  — '%s' (TTL expired)", block_id)

        # ❌ Cache miss
        _metrics[block_id]["l1_misses"] += 1
        _metrics[block_id]["last_miss_at"] = time.time()
        logger.debug("L1 MISS   — '%s'", block_id)
        return None


def set(block_id: str, content: str, block_type: Optional[str] = None) -> None:
    """
    Store block content in L1 cache with type-aware TTL.

    Called after a Letta fetch (miss write-back) or after a successful
    update_block_content (re-populate with new content).
    """
    ttl = _get_ttl(block_type)
    _cache[block_id] = (content, time.monotonic() + ttl)
    _ensure_metrics(block_id)
    logger.debug("L1 SET    — '%s' (type=%s, TTL=%ds)", block_id, block_type or "?", ttl)


def invalidate(block_id: str) -> None:
    """
    Remove a block from L1 cache immediately.

    Called at the start of update_block_content to prevent stale reads
    during the Letta → Weaviate update window.
    """
    removed = _cache.pop(block_id, None)
    if removed is not None:
        logger.debug("L1 INVALIDATE — '%s'", block_id)


def record_write_back(block_id: str) -> None:
    """
    Increment write-back counter for metrics tracking.
    Call after a successful update_block_content + L1 re-populate.
    """
    _ensure_metrics(block_id)
    _metrics[block_id]["write_backs"] += 1
    _metrics[block_id]["last_write_back_at"] = time.time()
    logger.debug("L1 WRITE-BACK — '%s' recorded", block_id)


# ── Metrics ───────────────────────────────────────────────────────────────────

def get_metrics() -> dict[str, dict]:
    """
    Return a per-block snapshot of L1 cache metrics.

    Enriched with computed fields:
        hit_rate        — l1_hits / total_requests
        total_requests  — l1_hits + l1_misses
        in_cache        — whether block is currently cached (TTL not expired)

    Hot block pattern   : high hit_rate + high write_backs → re-populate immediately
    Cold block pattern  : low hit_rate + low write_backs   → TTL eviction handles cleanup
    """
    now = time.monotonic()
    snapshot: dict[str, dict] = {}
    for block_id, m in _metrics.items():
        total = m["l1_hits"] + m["l1_misses"]
        entry = _cache.get(block_id)
        in_cache = entry is not None and now < entry[1]
        snapshot[block_id] = {
            **m,
            "hit_rate": round(m["l1_hits"] / total, 3) if total > 0 else 0.0,
            "total_requests": total,
            "in_cache": in_cache,
        }
    return snapshot


def get_summary() -> dict:
    """
    Return aggregate L1 stats across all blocks.
    Designed for the Streamlit dashboard header widget.
    """
    now = time.monotonic()
    all_hits = sum(m["l1_hits"] for m in _metrics.values())
    all_misses = sum(m["l1_misses"] for m in _metrics.values())
    total = all_hits + all_misses
    return {
        "total_hits": all_hits,
        "total_misses": all_misses,
        "total_requests": total,
        "global_hit_rate": round(all_hits / total, 3) if total > 0 else 0.0,
        "cached_blocks": sum(
            1 for _, (_, exp) in _cache.items() if now < exp
        ),
    }
