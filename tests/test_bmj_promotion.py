"""
Does the BMJ move-to-front actually survive a turn?

Target: app_local/mmu/controller.py:203-238 (search_memory / BMJ)
        app_local/runtime/agent.py:100 vs :171 (the stale `dll` handle)

These tests bypass lance_driver.search_block_index with a stub, because on
lancedb 0.30.2 it returns [] unconditionally (see test_lance_driver.py) and BMJ
would never fire at all. The point here is that BMJ is broken for a SECOND,
independent reason: fixing the storage layer will not make it work.
"""

import json

import pytest
from langchain_core.messages import HumanMessage

from app_local.mmu import controller
from app_local.storage import lance_driver


class _Msg:
    def __init__(self, content):
        self.content = content


class _LLM:
    def __init__(self, payload):
        self.payload = payload

    async def ainvoke(self, messages):
        return _Msg(self.payload)


@pytest.fixture
def hit_on_student_profile(monkeypatch):
    """Pretend the vector search found the student_profile DLL node."""
    async def fake_search(query_vector, limit=12, class_level=None, subject=None):
        return [{
            "block_id": "student_profile",
            "chapter_id": "student_profile",
            "block_type": "fondamental",
            "certainty": 0.99,
            "content": "Marc plays basketball",
            "source_table": "user_memory",
        }]

    monkeypatch.setattr(lance_driver, "search_block_index", fake_search)


async def test_search_memory_alone_does_promote_to_head(
    akili_paths, no_network, hit_on_student_profile
):
    """The BMJ step itself works when it is reached in isolation."""
    await controller.init_dll()
    assert (await controller.load_dll())["head_id"] == "current_session"

    await controller.search_memory([0.1] * 8, "6eme", "math")

    assert (await controller.load_dll())["head_id"] == "student_profile"


async def test_the_promoted_order_is_persisted_by_the_turn(
    akili_paths, no_network, hit_on_student_profile, stub_embeddings, monkeypatch
):
    """
    Was `test_the_promotion_is_reverted_before_the_turn_ends`, which recorded the
    S5-C defect: HEAD went back to current_session and the original order was
    restored byte for byte. That revert is the defect, not the contract.

    Kept as a separate case from test_the_promotion_survives_the_turn because it
    asserts the whole prev/next chain, not just head_id -- the write-back used to
    revert every pointer, not only the head.
    """
    import app_local.runtime.agent as agent

    monkeypatch.setattr(agent, "_llm", _LLM("an answer"))
    monkeypatch.setattr(agent, "_extractor_llm", _LLM(json.dumps({
        "student_profile": "",
        "learning_preferences": "",
        "current_session": "The student is studying fractions.",
    })))

    await controller.init_dll()
    before = await controller.load_dll()
    assert before["head_id"] == "current_session"

    await agent.planner_node({
        "messages": [HumanMessage(content="hi")],
        "agent_id": "agent-test", "class_level": "6eme", "subject": "math",
        "memory_only_mode": False, "needs_new_block": "False",
        "proposed_block_config": {},
    })

    after = await controller.load_dll()
    assert after["head_id"] == "student_profile", "promotion did not survive"
    assert controller._head_to_tail_order(after) == [
        "student_profile", "current_session", "active_course", "learning_preferences",
    ]
    # and the chain is still coherent in both directions after the write-back
    assert controller._tail_to_head_order(after) == [
        "learning_preferences", "active_course", "current_session", "student_profile",
    ]
    assert after["nodes"][after["head_id"]]["prev"] is None
    assert after["nodes"][after["tail_id"]]["next"] is None


async def test_search_memory_promotes_in_the_callers_handle(
    akili_paths, no_network, hit_on_student_profile
):
    """
    The ownership contract behind the S5-C fix: when a caller passes its own DLL,
    search_memory mutates that object rather than a private copy, so the caller
    cannot later persist a pre-promotion view of the chain.
    """
    await controller.init_dll()
    mine = await controller.load_dll()
    assert mine["head_id"] == "current_session"

    await controller.search_memory([0.1] * 8, "6eme", "math", dll=mine)

    assert mine["head_id"] == "student_profile"          # caller's object moved
    assert mine["nodes"]["student_profile"]["prev"] is None
    assert (await controller.load_dll())["head_id"] == "student_profile"

    # persisting the caller's handle afterwards must not undo it
    controller.save_dll(mine)
    assert (await controller.load_dll())["head_id"] == "student_profile"


async def test_search_memory_without_a_handle_still_loads_its_own(
    akili_paths, no_network, hit_on_student_profile
):
    """The standalone contract is unchanged — dll is optional."""
    await controller.init_dll()
    await controller.search_memory([0.1] * 8, "6eme", "math")
    assert (await controller.load_dll())["head_id"] == "student_profile"


async def test_the_promotion_survives_the_turn(
    akili_paths, no_network, hit_on_student_profile, stub_embeddings, monkeypatch
):
    import app_local.runtime.agent as agent

    monkeypatch.setattr(agent, "_llm", _LLM("an answer"))
    monkeypatch.setattr(agent, "_extractor_llm", _LLM(json.dumps({
        "student_profile": "",
        "learning_preferences": "",
        "current_session": "The student is studying fractions.",
    })))

    await controller.init_dll()
    await agent.planner_node({
        "messages": [HumanMessage(content="hi")],
        "agent_id": "agent-test", "class_level": "6eme", "subject": "math",
        "memory_only_mode": False, "needs_new_block": "False",
        "proposed_block_config": {},
    })

    assert (await controller.load_dll())["head_id"] == "student_profile"


async def test_bmj_promotes_at_most_one_node_per_search(akili_paths, no_network,
                                                        monkeypatch):
    """`break` at controller.py:236 -- only the first matching DLL node moves."""
    async def two_hits(query_vector, limit=12, class_level=None, subject=None):
        return [
            {"block_id": "learning_preferences", "chapter_id": "x",
             "block_type": "fondamental", "certainty": 0.99, "content": "a",
             "source_table": "user_memory"},
            {"block_id": "student_profile", "chapter_id": "y",
             "block_type": "fondamental", "certainty": 0.98, "content": "b",
             "source_table": "user_memory"},
        ]

    monkeypatch.setattr(lance_driver, "search_block_index", two_hits)
    await controller.init_dll()
    await controller.search_memory([0.1] * 8, "6eme", "math")

    dll = await controller.load_dll()
    assert dll["head_id"] == "learning_preferences"
    assert dll["nodes"]["student_profile"]["prev"] is not None


async def test_course_blocks_below_threshold_are_filtered_out(akili_paths, no_network,
                                                              monkeypatch):
    """
    Thresholds recalibrated to the embedding model (see settings): the old
    0.70/0.75/0.80 rejected even an exact-match chapter, measured at 0.670.
    'manual_chapter' -- the type every shipped course row actually carries -- is
    now an explicit key rather than falling through to the default.
    """
    async def mixed(query_vector, limit=12, class_level=None, subject=None):
        return [
            {"block_id": "ch1", "chapter_id": "ch1", "block_type": "manual_chapter",
             "certainty": 0.46, "content": "kept", "source_table": "edu_registry"},
            {"block_id": "ch2", "chapter_id": "ch2", "block_type": "manual_chapter",
             "certainty": 0.44, "content": "dropped", "source_table": "edu_registry"},
            {"block_id": "ch3", "chapter_id": "ch3", "block_type": "temp",
             "certainty": 0.49, "content": "dropped, temp needs 0.50",
             "source_table": "edu_registry"},
        ]

    monkeypatch.setattr(lance_driver, "search_block_index", mixed)
    await controller.init_dll()
    out = await controller.search_memory([0.1] * 8, "6eme", "math")

    assert [r["block_id"] for r in out] == ["ch1"]
