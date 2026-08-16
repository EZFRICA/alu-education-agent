"""
How far the read path gets with no network and no API key.

Target: app_local/runtime/agent.py:84-184 (planner_node)
        app_local/mmu/controller.py:250-289 (update_node_content)
"""

import json

import pytest
from langchain_core.messages import HumanMessage

from app_local.mmu import controller
from conftest import V_A


def _state(query="I don't understand fractions"):
    return {
        "messages": [HumanMessage(content=query)],
        "agent_id": "agent-test",
        "class_level": "6eme",
        "subject": "math",
        "memory_only_mode": False,
        "needs_new_block": "False",
        "proposed_block_config": {},
    }


class _Msg:
    def __init__(self, content):
        self.content = content


class _RecordingLLM:
    def __init__(self, payload):
        self.payload, self.calls = payload, 0

    async def ainvoke(self, messages):
        self.calls += 1
        self.seen = messages
        return _Msg(self.payload)


# ── the load-bearing test: where does it die offline ─────────────────────────

async def test_the_read_path_reaches_retrieval_offline(
    akili_paths, no_network, real_local_embedder, monkeypatch
):
    """
    Was `test_read_path_dies_on_the_query_embedding_before_any_retrieval`, which
    recorded C6: with sockets blocked, planner_node died on the Google embedding
    call as its first awaited statement, never reaching LanceDB, the DLL or L1.

    The retrieval half of the turn is now genuinely offline. This uses the REAL
    ONNX embedder (no stub), so it proves the model runs with no network rather
    than proving a fixture works.

    Deliberately narrower than "a turn completes offline": in this deployment
    inference is Gemini, so generation still needs the network. That boundary is
    asserted by test_generation_is_the_only_remaining_network_dependency.
    """
    import app_local.runtime.agent as agent

    reached = {"search": False}
    real_search = controller.search_memory

    async def spy_search(*a, **k):
        reached["search"] = True
        return await real_search(*a, **k)

    monkeypatch.setattr(controller, "search_memory", spy_search)
    llm = _RecordingLLM("answer")
    monkeypatch.setattr(agent, "_llm", llm)
    monkeypatch.setattr(agent, "_extractor_llm", _RecordingLLM("{}"))

    await agent.planner_node(_state())

    assert reached["search"] is True, "retrieval was reached with sockets blocked"
    assert llm.calls == 1, "the turn got all the way to generation"


async def test_generation_is_the_only_remaining_network_dependency(
    akili_paths, no_network, real_local_embedder, monkeypatch
):
    """
    The honest boundary for this deployment: embeddings and retrieval are local,
    inference is Gemini. With sockets blocked and the LLM NOT stubbed, the turn
    fails -- and it fails at generation, after retrieval, not before it.

    This replaces the old xfail `test_a_turn_completes_offline_using_local_memory_only`,
    which asserted a fully offline turn. That is not a goal here: a degraded
    memory-only answer would be a separate feature, and inference is remote by
    design in this configuration.
    """
    import app_local.runtime.agent as agent

    reached = {"search": False}
    real_search = controller.search_memory

    async def spy_search(*a, **k):
        reached["search"] = True
        return await real_search(*a, **k)

    monkeypatch.setattr(controller, "search_memory", spy_search)

    class _NetworkLLM:
        calls = 0

        async def ainvoke(self, messages):
            type(self).calls += 1
            raise OSError("network is unreachable")

    monkeypatch.setattr(agent, "_llm", _NetworkLLM())
    monkeypatch.setattr(agent, "_extractor_llm", _RecordingLLM("{}"))

    with pytest.raises(OSError):
        await agent.planner_node(_state())

    assert reached["search"] is True, "retrieval ran before the failure"
    assert _NetworkLLM.calls == 1, "the failure came from generation, not embedding"


async def test_everything_after_the_embedding_works_offline(
    akili_paths, no_network, stub_embeddings, monkeypatch
):
    """
    With only the embedding stubbed, the whole rest of the turn completes with
    sockets blocked -- proving the embedding is the sole network dependency that
    is structurally unavoidable on the read path (the two LLM calls are the
    other two, stubbed here).
    """
    import app_local.runtime.agent as agent

    main = _RecordingLLM("Great question. What do you already know about halves?")
    extractor = _RecordingLLM(json.dumps({
        "student_profile": "",
        "learning_preferences": "",
        "current_session": "The student is studying fractions in 6eme math.",
    }))
    monkeypatch.setattr(agent, "_llm", main)
    monkeypatch.setattr(agent, "_extractor_llm", extractor)

    out = await agent.planner_node(_state())

    assert main.calls == 1
    assert extractor.calls == 1
    assert out["messages"][0].content.startswith("Great question")
    assert out["needs_new_block"] == "False"

    dll = await controller.load_dll()
    assert dll["nodes"]["current_session"]["content"] == (
        "The student is studying fractions in 6eme math."
    )


async def test_course_context_reaches_the_prompt(
    akili_paths, no_network, stub_embeddings, monkeypatch
):
    """
    Was `test_course_context_is_empty_even_with_a_populated_registry`, which
    pinned the user-visible consequence of S5-A: an empty COURSE CONTEXT block
    despite edu_registry holding an exact-match row. Now asserts the retrieved
    chapter actually lands in the system prompt.
    """
    import app_local.runtime.agent as agent
    from app_local.storage import lance_driver

    db = lance_driver.get_db()
    db.create_table("edu_registry", data=[{
        "id": "ch_fractions", "chapter": "ch_fractions",
        "content": "A fraction represents a part of a whole.",
        "block_type": "manual_chapter", "class_level": "6eme", "subject": "math",
        "vector": list(V_A), "updated_at": "2026-01-01T00:00:00",
    }])

    main = _RecordingLLM("ok")
    monkeypatch.setattr(agent, "_llm", main)
    monkeypatch.setattr(agent, "_extractor_llm", _RecordingLLM("{}"))

    await agent.planner_node(_state())

    system_text = main.seen[0].content
    assert "COURSE CONTEXT (Search Results):" in system_text
    after = system_text.split("COURSE CONTEXT (Search Results):", 1)[1]
    course_context = after.split("STUDENT MEMORY", 1)[0]
    assert "A fraction represents a part of a whole." in course_context
    assert "--- ch_fractions ---" in course_context


async def test_the_system_prompt_is_sent_as_a_systemmessage(
    akili_paths, no_network, stub_embeddings, monkeypatch
):
    """agent.py:167 wraps the whole system prompt in a HumanMessage."""
    import app_local.runtime.agent as agent
    from langchain_core.messages import SystemMessage

    main = _RecordingLLM("ok")
    monkeypatch.setattr(agent, "_llm", main)
    monkeypatch.setattr(agent, "_extractor_llm", _RecordingLLM("{}"))

    await agent.planner_node(_state())

    # Was `..._as_a_humanmessage`, which pinned C17: the instructions were sent
    # as a student turn. Changing this alters how the model weighs the persona
    # and course context — flagged for manual evaluation, not proven better here.
    assert isinstance(main.seen[0], SystemMessage)
    assert not isinstance(main.seen[0], HumanMessage)
    assert "You are Akili" in main.seen[0].content


async def test_the_embedding_no_longer_depends_on_the_llm_provider(
    akili_paths, no_network, real_local_embedder, monkeypatch
):
    """
    Was `test_llm_provider_ollama_does_not_remove_the_embedding_network_call`,
    which pinned C6: LLM_PROVIDER changed get_main_llm/get_extractor_llm and
    nothing else, because the query embedding was hardcoded to Google.

    Inverted: embeddings are now selected by EMBEDDING_PROVIDER, independently of
    LLM_PROVIDER, so the query vector is produced offline whatever the inference
    backend is. Asserted for each LLM_PROVIDER value the app accepts.
    """
    import app_local.runtime.agent as agent

    for provider in ("ollama", "gemma", "gemini", "openrouter"):
        monkeypatch.setenv("LLM_PROVIDER", provider)
        main = _RecordingLLM("ok")
        monkeypatch.setattr(agent, "_llm", main)
        monkeypatch.setattr(agent, "_extractor_llm", _RecordingLLM("{}"))

        out = await agent.planner_node(_state())

        assert main.calls == 1, f"turn failed with LLM_PROVIDER={provider}"
        assert out["messages"][0].content == "ok"


# ── the conversation window (dashboard setting) ──────────────────────────────

def _log(n):
    """The UI's own transcript: n exchanges, oldest first."""
    out = []
    for i in range(1, n + 1):
        out.append({"role": "user", "content": f"q{i}"})
        out.append({"role": "assistant", "content": f"a{i}"})
    return out


def test_zero_exchanges_is_memory_only():
    """
    The old behaviour, now a deliberate setting rather than an accident: the
    tutor sees the L1/L2 blocks but no transcript.
    """
    from app_local.runtime.agent import build_message_window

    window = build_message_window(_log(3), "now", 0)
    assert [m.content for m in window] == ["now"]


def test_the_window_keeps_the_most_recent_exchanges():
    from app_local.runtime.agent import build_message_window

    window = build_message_window(_log(5), "now", 2)
    assert [m.content for m in window] == ["q4", "a4", "q5", "a5", "now"]


def test_roles_survive_the_round_trip():
    """
    The UI stores plain dicts; the model needs typed messages. Getting this
    wrong makes the assistant's own turns look like the student's.
    """
    from langchain_core.messages import AIMessage
    from app_local.runtime.agent import build_message_window

    window = build_message_window(_log(1), "now", 1)
    assert isinstance(window[0], HumanMessage)
    assert isinstance(window[1], AIMessage)
    assert isinstance(window[2], HumanMessage)


def test_asking_for_more_history_than_exists_is_safe():
    from app_local.runtime.agent import build_message_window

    window = build_message_window(_log(2), "now", 10)
    assert [m.content for m in window] == ["q1", "a1", "q2", "a2", "now"]
    assert build_message_window([], "first question", 5)[0].content == "first question"


async def test_the_window_reaches_the_model(
    akili_paths, no_network, stub_embeddings, monkeypatch
):
    """
    End to end: what build_message_window returns is what the model is sent,
    after the system prompt. This is the defect the dashboard had — history
    existed in the UI and never left it.
    """
    import app_local.runtime.agent as agent
    from app_local.runtime.agent import build_message_window

    main = _RecordingLLM("ok")
    monkeypatch.setattr(agent, "_llm", main)
    monkeypatch.setattr(agent, "_extractor_llm", _RecordingLLM("{}"))

    state = _state()
    state["messages"] = build_message_window(_log(2), "and now?", 2)
    await agent.planner_node(state)

    sent = [m.content for m in main.seen[1:]]        # [0] is the system prompt
    assert sent == ["q1", "a1", "q2", "a2", "and now?"]
