"""
cache_l1.py — L1 in-process content cache for DLL blocks.
Pure singleton module.
"""

import time
from typing import Optional
from logger import get_logger

logger = get_logger(__name__)

# ── TTL by block type ─────────────────────────────────────────────────────────
_TTL_BY_TYPE: dict[str, int] = {
    "temp":        300,
    "cours":       600,
    "fondamental": 1800,
}
_TTL_DEFAULT = 300

# ── Internal state (Global Singleton) ─────────────────────────────────────────
# These variables stay in memory as long as the Streamlit process is alive.
if "_cache" not in globals():
    _cache: dict[str, tuple[str, float]] = {}
if "_metrics" not in globals():
    _metrics: dict[str, dict] = {}

def _ensure_metrics(block_id: str) -> None:
    if block_id not in _metrics:
        _metrics[block_id] = {
            "l1_hits": 0,
            "l1_misses": 0,
            "write_backs": 0,
            "last_hit_at": None,
            "last_miss_at": None,
            "last_write_back_at": None,
        }

def get(block_id: str) -> Optional[str]:
    _ensure_metrics(block_id)
    entry = _cache.get(block_id)
    now = time.monotonic()

    if entry is not None:
        content, expiry = entry
        if now < expiry:
            _metrics[block_id]["l1_hits"] += 1
            _metrics[block_id]["last_hit_at"] = time.time()
            logger.debug("L1 HIT    — '%s'", block_id)
            return content
        else:
            del _cache[block_id]
            logger.debug("L1 EVICT  — '%s' (TTL expired)", block_id)

    _metrics[block_id]["l1_misses"] += 1
    _metrics[block_id]["last_miss_at"] = time.time()
    logger.debug("L1 MISS   — '%s'", block_id)
    return None

def set(block_id: str, content: str, block_type: Optional[str] = None) -> None:
    ttl = _TTL_BY_TYPE.get(block_type or "", _TTL_DEFAULT)
    _cache[block_id] = (content, time.monotonic() + ttl)
    _ensure_metrics(block_id)
    _metrics[block_id]["write_backs"] += 1
    _metrics[block_id]["last_write_back_at"] = time.time()
    logger.debug("L1 SET    — '%s' (type=%s, TTL=%ds)", block_id, block_type or "?", ttl)

def invalidate(block_id: str) -> None:
    _cache.pop(block_id, None)

def get_all_cached() -> dict[str, str]:
    now = time.monotonic()
    return {k: v[0] for k, v in _cache.items() if now < v[1]}

def get_metrics() -> dict[str, dict]:
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
    now = time.monotonic()
    all_hits   = sum(m["l1_hits"]   for m in _metrics.values())
    all_misses = sum(m["l1_misses"] for m in _metrics.values())
    total = all_hits + all_misses
    return {
        "total_hits":      all_hits,
        "total_misses":    all_misses,
        "total_requests":  total,
        "global_hit_rate": round(all_hits / total, 3) if total > 0 else 0.0,
        "cached_blocks":   sum(1 for _, (_, exp) in _cache.items() if now < exp),
    }

def flush_all():
    """Wipes all cache and metrics from RAM."""
    global _cache, _metrics
    _cache.clear()
    _metrics.clear()
    logger.info("L1 RAM Cache & Metrics completely flushed.")
