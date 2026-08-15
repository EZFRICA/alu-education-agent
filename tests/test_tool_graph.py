"""
The TEU wired into the graph (S5-F).

Target: app_local/runtime/agent.py — _route_after_planner, create_agent_graph

Uses a scripted LLM that emits tool calls, so the loop is exercised without a
network and without depending on whether the configured model supports
function calling.
"""

import json

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from app_local.mmu import controller


def _state(query="Combien font 3/4 + 2/5 ?"):
    return {
        "messages": [HumanMessage(content=query)],
        "agent_id": "agent-test", "class_level": "6eme", "subject": "math",
        "memory_only_mode": False, "needs_new_block": "False",
        "proposed_block_config": {},
    }


class _ScriptedLLM:
    """Returns each queued response in turn, recording what it was sent."""

    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = 0
        self.seen = []

    async def ainvoke(self, messages):
        self.seen.append(messages)
        self.calls += 1
        return self.responses[min(self.calls - 1, len(self.responses) - 1)]

    def bind_tools(self, tools):
        self.bound = tools
        return self


def _tool_call(name, args, call_id="c1"):
    return AIMessage(content="", tool_calls=[
        {"name": name, "args": args, "id": call_id, "type": "tool_call"}
    ])


# ── routing ──────────────────────────────────────────────────────────────────

def test_a_response_without_tool_calls_ends_the_turn():
    from app_local.runtime import agent
    state = {"messages": [AIMessage(content="Voici la réponse.")]}
    assert agent._route_after_planner(state) == "__end__"


def test_a_response_with_tool_calls_routes_to_the_teu():
    from app_local.runtime import agent
    state = {"messages": [_tool_call("calculate", {"expression": "1+1"})],
             "tool_iterations": 1}
    assert agent._route_after_planner(state) == "Tools"


def test_the_loop_is_capped():
    """
    A model that keeps re-requesting a tool must not loop forever on hardware
    a student cannot debug.
    """
    from app_local.runtime import agent
    state = {"messages": [_tool_call("calculate", {"expression": "1+1"})],
             "tool_iterations": agent.MAX_TOOL_ITERATIONS + 1}
    assert agent._route_after_planner(state) == "__end__"


def test_the_graph_has_a_tool_node_and_loops_back():
    from app_local.runtime import agent
    graph = agent.create_agent_graph()
    nodes = set(graph.get_graph().nodes)
    assert "Tools" in nodes and "Planner" in nodes
    edges = {(e.source, e.target) for e in graph.get_graph().edges}
    assert ("Tools", "Planner") in edges


# ── the loop end to end ──────────────────────────────────────────────────────

async def test_a_tool_call_runs_the_tool_and_feeds_the_result_back(
    akili_paths, no_network, stub_embeddings, monkeypatch
):
    from app_local.runtime import agent

    scripted = _ScriptedLLM(
        _tool_call("calculate", {"expression": "(3/4 + 2/5) * 20"}),
        AIMessage(content="Cela fait 23."),
    )
    monkeypatch.setattr(agent, "_llm", scripted)
    monkeypatch.setattr(agent, "_extractor_llm", _ScriptedLLM(
        AIMessage(content=json.dumps({"current_session": "Fractions en 6eme."}))
    ))

    await controller.init_dll()
    result = await agent.create_agent_graph().ainvoke(_state())

    # the tool actually ran and its result reached the model
    tool_messages = [m for m in result["messages"] if isinstance(m, ToolMessage)]
    assert len(tool_messages) == 1
    assert tool_messages[0].content == "23"

    assert result["messages"][-1].content == "Cela fait 23."
    assert scripted.calls == 2, "the planner re-entered after the tool"


async def test_memory_is_written_once_not_once_per_tool_call(
    akili_paths, no_network, stub_embeddings, monkeypatch
):
    """
    The write-back costs an extraction LLM call. Firing it per tool round trip
    would multiply the cost of a turn and archive a half-finished exchange.
    """
    from app_local.runtime import agent

    monkeypatch.setattr(agent, "_llm", _ScriptedLLM(
        _tool_call("calculate", {"expression": "2+2"}, "a"),
        _tool_call("calculate", {"expression": "3+3"}, "b"),
        AIMessage(content="Terminé."),
    ))
    extractor = _ScriptedLLM(
        AIMessage(content=json.dumps({"current_session": "Additions."}))
    )
    monkeypatch.setattr(agent, "_extractor_llm", extractor)

    await controller.init_dll()
    await agent.create_agent_graph().ainvoke(_state())

    assert extractor.calls == 1, (
        f"extraction ran {extractor.calls} times across 2 tool round trips"
    )


async def test_retrieval_is_not_rerun_on_each_tool_round_trip(
    akili_paths, no_network, stub_embeddings, monkeypatch
):
    """The system prompt and DLL handle travel in the state, so re-entry is cheap."""
    from app_local.runtime import agent

    searches = {"n": 0}
    real_search = controller.search_memory

    async def counting_search(*a, **k):
        searches["n"] += 1
        return await real_search(*a, **k)

    monkeypatch.setattr(controller, "search_memory", counting_search)
    monkeypatch.setattr(agent, "_llm", _ScriptedLLM(
        _tool_call("calculate", {"expression": "2+2"}, "a"),
        _tool_call("calculate", {"expression": "3+3"}, "b"),
        AIMessage(content="Terminé."),
    ))
    monkeypatch.setattr(agent, "_extractor_llm", _ScriptedLLM(
        AIMessage(content="{}")
    ))

    await controller.init_dll()
    await agent.create_agent_graph().ainvoke(_state())

    assert searches["n"] == 1, f"retrieval ran {searches['n']} times"
    assert len(stub_embeddings) == 1, "the query was embedded more than once"


async def test_the_student_question_survives_tool_round_trips(
    akili_paths, no_network, stub_embeddings, monkeypatch
):
    """
    After a tool call, messages[-1] is a ToolMessage. The extraction prompt must
    still receive the student's actual question.
    """
    from app_local.runtime import agent

    monkeypatch.setattr(agent, "_llm", _ScriptedLLM(
        _tool_call("calculate", {"expression": "2+2"}),
        AIMessage(content="Quatre."),
    ))
    extractor = _ScriptedLLM(AIMessage(content="{}"))
    monkeypatch.setattr(agent, "_extractor_llm", extractor)

    await controller.init_dll()
    await agent.create_agent_graph().ainvoke(_state("Combien font 2+2 ?"))

    prompt = extractor.seen[0][0].content
    assert "Combien font 2+2 ?" in prompt


# ── binding degrades rather than crashing ────────────────────────────────────

def test_a_model_that_cannot_bind_tools_still_starts(monkeypatch):
    """
    Function calling is a per-model capability. A backend that lacks it must
    yield a tutor without tools, not a process that refuses to start.
    """
    from app_local.runtime import agent

    class _NoToolSupport:
        def bind_tools(self, tools):
            raise NotImplementedError("this model does not support tools")

    llm = _NoToolSupport()
    assert agent._bind_tools(llm) is llm


def test_teu_can_be_disabled_by_configuration(monkeypatch):
    from app_local.config import settings
    from app_local.runtime import agent

    monkeypatch.setattr(settings, "TEU_ENABLED", False)

    class _Tracking:
        bound = False

        def bind_tools(self, tools):
            self.bound = True
            return self

    llm = _Tracking()
    assert agent._bind_tools(llm) is llm
    assert llm.bound is False
