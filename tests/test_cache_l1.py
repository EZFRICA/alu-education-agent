"""
L1 in-process cache: TTL, invalidation, and growth.

Target: app_local/mmu/cache_l1.py
"""

import pytest

from app_local.mmu import cache_l1


# ── TTL ──────────────────────────────────────────────────────────────────────

def test_ttl_by_type_values():
    assert cache_l1._TTL_BY_TYPE == {"temp": 300, "cours": 600, "fondamental": 1800}
    assert cache_l1._TTL_DEFAULT == 300


@pytest.mark.parametrize("block_type,ttl", [
    ("temp", 300), ("cours", 600), ("fondamental", 1800),
    ("course", 300), ("manual_chapter", 300), ("projet", 300), (None, 300),
])
def test_unknown_types_silently_get_the_default_ttl(block_type, ttl, monkeypatch):
    t = [1000.0]
    monkeypatch.setattr(cache_l1.time, "monotonic", lambda: t[0])
    cache_l1.set("b", "content", block_type=block_type)
    assert cache_l1._cache["b"][1] == pytest.approx(1000.0 + ttl)


def test_entry_expires_exactly_at_its_ttl(monkeypatch):
    t = [1000.0]
    monkeypatch.setattr(cache_l1.time, "monotonic", lambda: t[0])

    cache_l1.set("temp_block", "hello", block_type="temp")
    assert cache_l1.get("temp_block") == "hello"

    t[0] = 1000.0 + 299.9
    assert cache_l1.get("temp_block") == "hello"

    t[0] = 1000.0 + 300.0          # `now < expiry` is False at exactly TTL
    assert cache_l1.get("temp_block") is None

    assert "temp_block" not in cache_l1._cache, "expired entry is dropped on read"


def test_expiry_is_only_evaluated_on_read(monkeypatch):
    """
    There is no background sweep. An expired entry occupies RAM until someone
    calls get() for that exact id.
    """
    t = [1000.0]
    monkeypatch.setattr(cache_l1.time, "monotonic", lambda: t[0])

    cache_l1.set("stale", "x", block_type="temp")
    t[0] = 99999.0

    assert "stale" in cache_l1._cache, "still resident after expiry"
    assert cache_l1.get_all_cached() == {}, "but filtered out of the listing"
    assert "stale" in cache_l1._cache, "and get_all_cached does not evict it"

    cache_l1.get("stale")
    assert "stale" not in cache_l1._cache, "only get() evicts"


# ── invalidate ───────────────────────────────────────────────────────────────

def test_invalidate_then_get_is_a_miss():
    cache_l1.set("b", "content", block_type="fondamental")
    assert cache_l1.get("b") == "content"
    cache_l1.invalidate("b")
    assert cache_l1.get("b") is None


def test_invalidate_is_safe_on_an_absent_key():
    cache_l1.invalidate("never_existed")


def test_invalidate_keeps_the_metrics_row():
    """
    invalidate() pops from _cache but never from _metrics (cache_l1.py:67-68).
    This is what makes _metrics grow independently of the cache.
    """
    cache_l1.set("b", "c", block_type="temp")
    cache_l1.get("b")
    cache_l1.invalidate("b")
    assert "b" not in cache_l1._cache
    assert "b" in cache_l1._metrics
    assert cache_l1._metrics["b"]["l1_hits"] == 1


def test_a_miss_on_an_unknown_id_still_creates_a_metrics_row():
    """_ensure_metrics runs before the lookup (cache_l1.py:39)."""
    assert cache_l1._metrics == {}
    cache_l1.get("id_that_never_existed")
    assert "id_that_never_existed" in cache_l1._metrics


# ── growth ───────────────────────────────────────────────────────────────────

def test_the_cache_holds_the_most_recently_used_entries():
    """
    Was `test_cache_has_no_entry_cap`, which asserted 2000 writes -> 2000 live
    entries. Unbounded RAM growth was the defect, not the contract.
    """
    for i in range(2000):
        cache_l1.set(f"block_{i}", "x" * 100, block_type="fondamental")

    assert len(cache_l1._cache) == cache_l1.MAX_ENTRIES
    assert cache_l1.get_summary()["cached_blocks"] == cache_l1.MAX_ENTRIES
    # the survivors are the newest, the earliest are gone
    assert cache_l1.get("block_1999") == "x" * 100
    assert cache_l1.get("block_0") is None


def test_metrics_are_bounded_too():
    """
    Was `test_metrics_dict_grows_with_every_distinct_id_ever_queried`: _metrics
    grew on every distinct id ever queried, including pure misses, and was
    pruned only by flush_all(). Tracking must not outgrow what it tracks.
    """
    for i in range(1000):
        cache_l1.get(f"absent_{i}")          # pure misses, nothing cached
    assert len(cache_l1._cache) == 0
    assert len(cache_l1._metrics) <= cache_l1.MAX_ENTRIES * 2


def test_sweep_reclaims_entries_that_are_never_read_again(monkeypatch):
    """
    Expiry only ever ran inside get() for the id being read, so a block written
    once and never read again stayed in RAM until flush_all().
    """
    t = [1000.0]
    monkeypatch.setattr(cache_l1.time, "monotonic", lambda: t[0])
    for i in range(10):
        cache_l1.set(f"b{i}", "c", block_type="temp")     # TTL 300
    assert len(cache_l1._cache) == 10

    t[0] = 5000.0
    assert cache_l1.sweep() == 10
    assert len(cache_l1._cache) == 0


def test_flush_all_clears_both_structures():
    cache_l1.set("a", "x", block_type="temp")
    cache_l1.get("b")
    cache_l1.flush_all()
    assert cache_l1._cache == {}
    assert cache_l1._metrics == {}


def test_cache_is_bounded():
    for i in range(5000):
        cache_l1.set(f"block_{i}", "x" * 1000, block_type="fondamental")
    assert len(cache_l1._cache) <= 256


# ── hit-rate accounting ──────────────────────────────────────────────────────

def test_hit_rate_accounting():
    cache_l1.get("b")                                  # miss
    cache_l1.set("b", "c", block_type="temp")
    cache_l1.get("b")                                  # hit
    cache_l1.get("b")                                  # hit
    m = cache_l1.get_metrics()["b"]
    assert m["l1_hits"] == 2
    assert m["l1_misses"] == 1
    assert m["hit_rate"] == pytest.approx(0.667, abs=0.001)
    assert m["in_cache"] is True


def test_expired_read_counts_as_a_miss_and_flips_in_cache(monkeypatch):
    t = [1000.0]
    monkeypatch.setattr(cache_l1.time, "monotonic", lambda: t[0])
    cache_l1.set("b", "c", block_type="temp")
    t[0] = 2000.0
    assert cache_l1.get("b") is None
    m = cache_l1.get_metrics()["b"]
    assert m["l1_misses"] == 1
    assert m["in_cache"] is False
