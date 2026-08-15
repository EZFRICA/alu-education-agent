"""
MAX_DYNAMIC_BLOCKS enforcement and eviction policy.

Target: app_local/mmu/block_factory.py:127-182 (create_dynamic_block)
        app_local/config/settings.py:20 (MAX_DYNAMIC_BLOCKS = 5)
"""

import pytest

from app_local.config import settings
from app_local.mmu import block_factory, controller
from conftest import V_A, check_dll_invariants


async def _fresh(akili_paths):
    return await controller.init_dll()


async def test_max_dynamic_blocks_is_five():
    assert settings.MAX_DYNAMIC_BLOCKS == 5


async def test_init_dll_carries_the_cap_into_the_state(akili_paths):
    dll = await controller.init_dll()
    assert dll["dynamic_block_max"] == settings.MAX_DYNAMIC_BLOCKS
    assert dll["dynamic_block_count"] == 0


async def test_creating_blocks_up_to_the_cap_stays_healthy(akili_paths):
    dll = await controller.init_dll()
    for i in range(settings.MAX_DYNAMIC_BLOCKS):
        dll = await block_factory.create_dynamic_block(
            block_id=f"dyn_{i}",
            label=f"Dyn {i}",
            block_type="temp",
            initial_content=f"content {i}",
            keywords=["k"],
            created_by="test",
            dll=dll,
            vector=list(V_A),
        )
    assert dll["dynamic_block_count"] == 5
    assert check_dll_invariants(dll) == []
    # 4 fixed + 5 dynamic
    assert len(dll["nodes"]) == 9


async def test_duplicate_block_id_is_rejected(akili_paths):
    dll = await controller.init_dll()
    dll = await block_factory.create_dynamic_block(
        "dup", "Dup", "temp", "c", [], "test", dll, vector=list(V_A)
    )
    with pytest.raises(ValueError, match="already exists"):
        await block_factory.create_dynamic_block(
            "dup", "Dup", "temp", "c", [], "test", dll, vector=list(V_A)
        )


async def test_eviction_of_a_single_dynamic_block_at_the_cap(akili_paths):
    """
    With exactly one dynamic node present, min() over a 1-element list never
    compares, so the None last_accessed never blows up. This is the only
    configuration in which eviction currently works.
    """
    dll = await controller.init_dll()
    dll["dynamic_block_max"] = 1
    dll = await block_factory.create_dynamic_block(
        "first", "First", "temp", "c1", [], "test", dll, vector=list(V_A)
    )
    assert dll["dynamic_block_count"] == 1

    dll = await block_factory.create_dynamic_block(
        "second", "Second", "temp", "c2", [], "test", dll, vector=list(V_A)
    )
    assert "first" not in dll["nodes"], "the older block should have been paged out"
    assert "second" in dll["nodes"]
    assert dll["dynamic_block_count"] == 1
    assert check_dll_invariants(dll) == []


async def test_working_set_stays_bounded_past_the_cap(akili_paths):
    dll = await controller.init_dll()
    for i in range(settings.MAX_DYNAMIC_BLOCKS + 3):
        dll = await block_factory.create_dynamic_block(
            block_id=f"dyn_{i}",
            label=f"Dyn {i}",
            block_type="temp",
            initial_content=f"content {i}",
            keywords=["k"],
            created_by="test",
            dll=dll,
            vector=list(V_A),
        )
    assert dll["dynamic_block_count"] <= settings.MAX_DYNAMIC_BLOCKS
    assert check_dll_invariants(dll) == []


async def test_eviction_picks_the_least_recently_accessed_block(
    akili_paths, stub_embeddings
):
    dll = await controller.init_dll()
    dll["dynamic_block_max"] = 2
    for i in ("old", "new"):
        dll = await block_factory.create_dynamic_block(
            i, i, "temp", f"c-{i}", [], "test", dll, vector=list(V_A)
        )

    # "Access" the old block through every read path the app actually has.
    from app_local.mmu import cache_l1
    cache_l1.set("old", "c-old", block_type="temp")
    cache_l1.get("old")
    dll = await controller.update_node_content("old", "touched", dll)

    assert dll["nodes"]["old"]["last_accessed"] is not None, (
        "reading a block never records an access time"
    )

    dll = await block_factory.create_dynamic_block(
        "third", "third", "temp", "c3", [], "test", dll, vector=list(V_A)
    )
    assert "new" not in dll["nodes"], "the untouched block should be the victim"
    assert "old" in dll["nodes"]
