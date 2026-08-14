"""
DLL structural invariants across insert / move_to_front / page_out.

Target: app_local/mmu/controller.py (move_to_front, traversals)
        app_local/mmu/block_factory.py (insert_node_by_type, page_out_block)
"""

import pytest

from app_local.mmu import controller
from app_local.mmu import block_factory
from conftest import make_chain, make_node, check_dll_invariants


# ── the healthy baseline ─────────────────────────────────────────────────────

def test_freshly_built_chain_is_healthy():
    dll = make_chain(["a", "b", "c", "d"])
    assert check_dll_invariants(dll) == []


async def test_init_dll_produces_a_healthy_four_block_chain(akili_paths):
    dll = await controller.init_dll()
    assert check_dll_invariants(dll) == []
    assert controller._head_to_tail_order(dll) == [
        "current_session", "active_course", "learning_preferences", "student_profile",
    ]
    assert controller._tail_to_head_order(dll) == [
        "student_profile", "learning_preferences", "active_course", "current_session",
    ]


# ── move_to_front ────────────────────────────────────────────────────────────

@pytest.mark.parametrize("target", ["a", "b", "c", "d"])
def test_move_to_front_preserves_invariants(target):
    dll = make_chain(["a", "b", "c", "d"])
    dll = controller.move_to_front(target, dll)
    assert check_dll_invariants(dll) == [], f"moving {target!r} broke the chain"
    assert dll["head_id"] == target
    assert set(controller._head_to_tail_order(dll)) == {"a", "b", "c", "d"}


def test_move_to_front_of_tail_repoints_tail():
    dll = make_chain(["a", "b", "c"])
    dll = controller.move_to_front("c", dll)
    assert dll["head_id"] == "c"
    assert dll["tail_id"] == "b"
    assert controller._head_to_tail_order(dll) == ["c", "a", "b"]
    assert controller._tail_to_head_order(dll) == ["b", "a", "c"]


def test_move_to_front_repeated_is_stable():
    dll = make_chain(["a", "b", "c", "d"])
    for target in ["c", "a", "d", "c", "b", "b"]:
        dll = controller.move_to_front(target, dll)
        assert check_dll_invariants(dll) == [], f"after moving {target!r}"


# ── insert_node_by_type ──────────────────────────────────────────────────────

@pytest.mark.parametrize("block_type", ["temp", "fondamental", "projet", "course"])
def test_insert_node_by_type_preserves_invariants(block_type):
    dll = make_chain(["a", "b", "c"])
    node = make_node("new", node_type=block_type)
    dll = block_factory.insert_node_by_type(block_type, node, dll)
    assert check_dll_invariants(dll) == [], f"inserting a {block_type!r} block"
    assert set(controller._head_to_tail_order(dll)) == {"a", "b", "c", "new"}


def test_insert_temp_lands_at_head():
    dll = make_chain(["a", "b", "c"])
    dll = block_factory.insert_node_by_type("temp", make_node("new", "temp"), dll)
    assert dll["head_id"] == "new"
    assert controller._head_to_tail_order(dll) == ["new", "a", "b", "c"]


def test_insert_fondamental_lands_just_before_tail_never_at_tail():
    """A dynamic 'fondamental' block can never become TAIL by insertion."""
    dll = make_chain(["a", "b", "c"])
    dll = block_factory.insert_node_by_type(
        "fondamental", make_node("new", "fondamental"), dll
    )
    assert controller._head_to_tail_order(dll) == ["a", "b", "new", "c"]
    assert dll["tail_id"] == "c"


def test_insert_unknown_type_falls_through_to_the_projet_branch():
    """
    'course' is what block_detector.py:62 proposes. There is no branch for it,
    so it silently takes the else-branch (insert after HEAD).
    """
    dll = make_chain(["a", "b", "c"])
    dll = block_factory.insert_node_by_type("course", make_node("new", "course"), dll)
    assert controller._head_to_tail_order(dll) == ["a", "new", "b", "c"]


# ── page_out_block ───────────────────────────────────────────────────────────

async def test_page_out_middle_node(akili_paths):
    dll = make_chain(["a", "b", "c", "d"])
    dll = await block_factory.page_out_block("b", dll)
    assert check_dll_invariants(dll) == []
    assert controller._head_to_tail_order(dll) == ["a", "c", "d"]
    assert controller._tail_to_head_order(dll) == ["d", "c", "a"]


async def test_page_out_the_current_head(akili_paths):
    dll = make_chain(["a", "b", "c"])
    assert dll["head_id"] == "a"
    dll = await block_factory.page_out_block("a", dll)
    assert check_dll_invariants(dll) == []
    assert dll["head_id"] == "b"
    assert dll["nodes"]["b"]["prev"] is None
    assert controller._head_to_tail_order(dll) == ["b", "c"]
    assert controller._tail_to_head_order(dll) == ["c", "b"]


async def test_page_out_the_current_tail(akili_paths):
    dll = make_chain(["a", "b", "c"])
    assert dll["tail_id"] == "c"
    dll = await block_factory.page_out_block("c", dll)
    assert check_dll_invariants(dll) == []
    assert dll["tail_id"] == "b"
    assert dll["nodes"]["b"]["next"] is None
    assert controller._head_to_tail_order(dll) == ["a", "b"]
    assert controller._tail_to_head_order(dll) == ["b", "a"]


async def test_page_out_is_refused_for_fixed_blocks(akili_paths):
    dll = make_chain(["a", "b", "c"], fixed={"b"})
    before = controller._head_to_tail_order(dll)
    dll = await block_factory.page_out_block("b", dll)
    assert controller._head_to_tail_order(dll) == before


async def test_page_out_unknown_id_is_a_noop(akili_paths):
    dll = make_chain(["a", "b"])
    dll = await block_factory.page_out_block("nope", dll)
    assert check_dll_invariants(dll) == []


# ── mixed sequence ───────────────────────────────────────────────────────────

async def test_mixed_insert_move_pageout_sequence_keeps_the_chain_healthy(akili_paths):
    dll = make_chain(["a", "b", "c"])
    dll = block_factory.insert_node_by_type("temp", make_node("t1", "temp"), dll)
    dll = controller.move_to_front("c", dll)
    dll = block_factory.insert_node_by_type(
        "fondamental", make_node("f1", "fondamental"), dll
    )
    dll = await block_factory.page_out_block("t1", dll)
    dll = controller.move_to_front("f1", dll)
    dll = await block_factory.page_out_block(dll["head_id"], dll)
    dll = await block_factory.page_out_block(dll["tail_id"], dll)
    assert check_dll_invariants(dll) == []


# ── known defects, pinned as xfail ───────────────────────────────────────────

async def test_page_out_last_remaining_node_leaves_a_consistent_empty_dll(akili_paths):
    """Checked as a suspected defect; it is not one. Pinned so it stays correct."""
    dll = make_chain(["only"])
    dll = await block_factory.page_out_block("only", dll)
    assert dll["nodes"] == {}
    assert dll["head_id"] is None
    assert dll["tail_id"] is None
    assert check_dll_invariants(dll) == []


def test_move_to_front_of_unknown_id_is_a_noop():
    dll = make_chain(["a", "b"])
    out = controller.move_to_front("ghost", dll)
    assert out["head_id"] == "a"
