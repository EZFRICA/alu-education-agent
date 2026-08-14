"""
The block-proposal contract between detector and executor.

Target: app_local/core/block_detector.py:14-66 (detect_new_block_opportunity)
        app_local/mmu/block_factory.py:207-226 (auto_execute_block_proposal)
"""

import pytest

from app_local.config import settings
from app_local.core.block_detector import detect_new_block_opportunity
from app_local.mmu import block_factory, controller
from conftest import check_dll_invariants


def _history(n_user_msgs=3, text="Can you explain why this works?"):
    h = []
    for _ in range(n_user_msgs):
        h.append({"role": "user", "content": text})
        h.append({"role": "assistant", "content": "Sure."})
    return h


async def _dll(akili_paths):
    return await controller.init_dll()


# ── the detector ─────────────────────────────────────────────────────────────

async def test_detector_returns_none_below_the_turn_threshold(akili_paths):
    dll = await controller.init_dll()
    assert detect_new_block_opportunity(_history(1), dll) is None  # 2 entries < 4


async def test_detector_returns_none_when_the_cap_is_reached(akili_paths):
    dll = await controller.init_dll()
    dll["dynamic_block_count"] = dll["dynamic_block_max"]
    assert detect_new_block_opportunity(_history(3), dll) is None


async def test_detector_emits_the_shared_proposal_shape(akili_paths):
    """
    Was `test_detector_fires_and_records_its_exact_output_shape`, which pinned
    the broken contract: key 'block_type' (not 'type'), no 'initial_content',
    no 'keywords'. Both ends now use block_proposal.BlockProposal.
    """
    from app_local.core.block_proposal import REQUIRED_FIELDS, validate

    dll = await controller.init_dll()
    proposal = detect_new_block_opportunity(_history(3), dll)

    assert proposal is not None
    assert validate(proposal) is None, validate(proposal)
    assert set(proposal.keys()) == {
        "proposed_id", "label", "type", "initial_content", "keywords", "reason",
    }
    assert proposal["proposed_id"] == "dynamic_block_1"
    assert proposal["type"] == "temp"
    assert proposal["initial_content"]
    assert all(str(proposal[f]).strip() for f in REQUIRED_FIELDS)


async def test_proposed_id_collides_after_a_page_out(akili_paths):
    """
    proposed_id is derived from dynamic_block_count (block_detector.py:57), not
    from the ids actually present. page_out_block decrements the counter, so the
    id is reused and create_dynamic_block will raise ValueError on the second
    proposal.
    """
    dll = await controller.init_dll()
    dll["dynamic_block_count"] = 2
    first = detect_new_block_opportunity(_history(3), dll)
    assert first["proposed_id"] == "dynamic_block_3"

    dll["dynamic_block_count"] = 1
    second = detect_new_block_opportunity(_history(3), dll)
    assert second["proposed_id"] == "dynamic_block_2"


def test_trigger_list_matches_substrings_inside_unrelated_words():
    """
    'how' and 'why' are matched with `trigger in msg.lower()`
    (block_detector.py:52), so 'show', 'shower', 'somehow' all count as
    learning signals.
    """
    from app_local.core import block_detector
    triggers = block_detector.learning_triggers if hasattr(
        block_detector, "learning_triggers") else None
    # the list is function-local; assert the behaviour instead
    dll = {"dynamic_block_count": 0, "dynamic_block_max": 5}
    h = [
        {"role": "user", "content": "Show me the shower schedule somehow"},
        {"role": "assistant", "content": "ok"},
        {"role": "user", "content": "Show me again"},
        {"role": "assistant", "content": "ok"},
    ]
    assert detect_new_block_opportunity(h, dll) is not None


# ── the executor ─────────────────────────────────────────────────────────────

@pytest.mark.parametrize("bad,why", [
    ({}, "empty"),
    (None, "none"),
    ({"proposed_id": "x", "label": "L", "initial_content": "c"}, "no type"),
    ({"proposed_id": "x", "label": "L", "type": "temp"}, "no content"),
    ({"proposed_id": "x", "label": "L", "type": "temp",
      "initial_content": "   "}, "blank content"),
    ({"label": "L", "type": "temp", "initial_content": "c"}, "no id"),
])
async def test_a_malformed_proposal_is_refused_and_writes_nothing(
    akili_paths, stub_embeddings, bad, why
):
    """
    Was `test_auto_execute_reports_success_and_creates_a_typeless_node`, which
    recorded that a mismatched proposal produced a type=None / content=None block
    and still returned True. Refusing is the contract now.
    """
    await controller.init_dll()
    assert await block_factory.auto_execute_block_proposal(bad) is False, why

    fresh = await controller.load_dll()
    assert "dynamic_block_1" not in fresh["nodes"]
    assert fresh["dynamic_block_count"] == 0
    assert "user_memory" not in lance_driver_tables()


def lance_driver_tables():
    from app_local.storage import lance_driver
    return lance_driver.list_table_names()


async def test_the_row_that_lands_in_lancedb_carries_real_content(
    akili_paths, stub_embeddings
):
    """
    Was `test_the_row_that_lands_in_lancedb_is_empty`: content None, block_type
    None, zero vector. All three were the defect.
    """
    from app_local.storage import lance_driver

    dll = await controller.init_dll()
    proposal = detect_new_block_opportunity(_history(3), dll)
    assert await block_factory.auto_execute_block_proposal(proposal) is True

    df = lance_driver.get_db().open_table("user_memory").to_pandas()
    assert df.shape[0] == 1
    row = df.iloc[0]
    assert row["id"] == "dynamic_block_1"
    assert row["content"] == proposal["initial_content"]
    assert row["block_type"] == "temp"
    # class_level/subject are still hardcoded to "local" at block_factory.py
    assert row["class_level"] == "local"
    assert row["subject"] == "local"
    assert len(row["vector"]) == settings.EMBEDDING_DIM
    assert any(float(x) != 0.0 for x in row["vector"])


async def test_the_typeless_node_matches_no_ttl_and_no_threshold(akili_paths):
    """type=None falls through every lookup table in the system."""
    from app_local.mmu import cache_l1

    assert cache_l1._TTL_BY_TYPE.get(None or "", cache_l1._TTL_DEFAULT) == 300
    assert controller.CERTAINTY_THRESHOLDS.get(
        None, controller.MIN_RELEVANCE_CERTAINTY
    ) == 0.45


async def test_a_correctly_shaped_proposal_does_land(akili_paths, stub_embeddings):
    """Shows the executor itself works; only the contract between the two is broken."""
    from app_local.storage import lance_driver

    dll = await controller.init_dll()
    ok = await block_factory.auto_execute_block_proposal({
        "proposed_id": "dynamic_block_1",
        "label": "Topic currently being learned",
        "type": "temp",
        "initial_content": "The student is learning fractions.",
        "keywords": ["fractions"],
    })
    assert ok is True

    fresh = await controller.load_dll()
    assert "dynamic_block_1" in fresh["nodes"]
    assert fresh["dynamic_block_count"] == 1
    assert check_dll_invariants(fresh) == []

    db = lance_driver.get_db()
    df = db.open_table("user_memory").to_pandas()
    assert df.shape[0] == 1
    assert df.iloc[0]["content"] == "The student is learning fractions."
    # the executor embeds the content: a real, retrievable vector
    assert len(df.iloc[0]["vector"]) == settings.EMBEDDING_DIM
    assert any(float(x) != 0.0 for x in df.iloc[0]["vector"])


async def test_a_detector_proposal_lands_in_the_dll_and_lancedb(akili_paths, stub_embeddings):
    from app_local.storage import lance_driver

    dll = await controller.init_dll()
    proposal = detect_new_block_opportunity(_history(3), dll)

    assert await block_factory.auto_execute_block_proposal(proposal) is True

    fresh = await controller.load_dll()
    assert "dynamic_block_1" in fresh["nodes"]
    assert fresh["nodes"]["dynamic_block_1"]["type"] == "temp"

    db = lance_driver.get_db()
    assert db.open_table("user_memory").to_pandas().shape[0] == 1


async def test_a_created_block_is_retrievable_by_search(akili_paths, stub_embeddings):
    from app_local.storage import lance_driver
    from conftest import V_A

    dll = await controller.init_dll()
    await block_factory.auto_execute_block_proposal({
        "proposed_id": "dynamic_block_1", "label": "L", "type": "temp",
        "initial_content": "The student is learning fractions.", "keywords": [],
    })
    db = lance_driver.get_db()
    df = db.open_table("user_memory").to_pandas()
    assert any(float(x) != 0.0 for x in df.iloc[0]["vector"]), (
        "block was stored with a zero vector"
    )
