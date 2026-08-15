"""
Extractor JSON parsing against realistic malformed model output.

Target: app_local/runtime/agent.py:68-80 (_update_student_memory)

The parsing logic is not factored out, so these tests drive the real
_update_student_memory with a stub extractor LLM and observe which updates land
in the DLL and which are swallowed by the bare `except` at agent.py:79-80.
"""

import json

import pytest

from app_local.mmu import controller

GOOD = {
    "student_profile": "The student is named Marc and plays basketball.",
    "learning_preferences": "",
    "current_session": "The student is studying fractions in 6eme math.",
}


class _Msg:
    def __init__(self, content):
        self.content = content


class _StubExtractor:
    def __init__(self, payload):
        self.payload = payload

    async def ainvoke(self, messages):
        return _Msg(self.payload)


async def _extract(payload, akili_paths, stub_embeddings, monkeypatch, caplog):
    """Run the real extraction path with `payload` as the model's raw output."""
    import app_local.runtime.agent as agent
    monkeypatch.setattr(agent, "_extractor_llm", _StubExtractor(payload))

    dll = await controller.init_dll()
    with caplog.at_level("WARNING"):
        problems = await agent._update_student_memory("a question", "an answer", dll)

    fresh = await controller.load_dll()
    landed = {
        nid: node.get("content")
        for nid, node in fresh["nodes"].items()
        if node.get("content")
    }
    return landed, problems


# ── shapes that parse ────────────────────────────────────────────────────────

async def test_bare_json_object_parses(akili_paths, stub_embeddings, monkeypatch, caplog):
    landed, errors = await _extract(json.dumps(GOOD), akili_paths, stub_embeddings,
                                    monkeypatch, caplog)
    assert landed == {
        "student_profile": GOOD["student_profile"],
        "current_session": GOOD["current_session"],
    }
    assert errors == []


async def test_fenced_json_parses(akili_paths, stub_embeddings, monkeypatch, caplog):
    """agent.py:73 strips ```json and ``` before json.loads."""
    payload = "```json\n" + json.dumps(GOOD) + "\n```"
    landed, errors = await _extract(payload, akili_paths, stub_embeddings,
                                    monkeypatch, caplog)
    assert "student_profile" in landed
    assert errors == []


async def test_bare_fence_without_a_language_tag_parses(
    akili_paths, stub_embeddings, monkeypatch, caplog
):
    payload = "```\n" + json.dumps(GOOD) + "\n```"
    landed, errors = await _extract(payload, akili_paths, stub_embeddings,
                                    monkeypatch, caplog)
    assert "student_profile" in landed
    assert errors == []


async def test_short_values_are_dropped_by_the_length_filter(
    akili_paths, stub_embeddings, monkeypatch, caplog
):
    """agent.py:77 requires len(new_info.strip()) > 5."""
    payload = json.dumps({"student_profile": "Marc", "current_session": "Fractions!"})
    landed, errors = await _extract(payload, akili_paths, stub_embeddings,
                                    monkeypatch, caplog)
    assert "student_profile" not in landed        # "Marc" is 4 chars
    assert landed["current_session"] == "Fractions!"
    assert errors == []


# ── shapes that are swallowed ────────────────────────────────────────────────

async def test_trailing_prose_after_the_object_is_tolerated(
    akili_paths, stub_embeddings, monkeypatch, caplog
):
    """
    Was `..._is_swallowed_silently`: a very common model behaviour that wrote
    nothing and left one log line. The object is now extracted from the prose.
    """
    payload = json.dumps(GOOD) + "\n\nI hope that helps with the extraction!"
    landed, problems = await _extract(payload, akili_paths, stub_embeddings,
                                      monkeypatch, caplog)
    assert landed["current_session"] == GOOD["current_session"]
    assert problems == []


async def test_leading_prose_before_the_object_is_tolerated(
    akili_paths, stub_embeddings, monkeypatch, caplog
):
    payload = "Here is the JSON you asked for:\n" + json.dumps(GOOD)
    landed, problems = await _extract(payload, akili_paths, stub_embeddings,
                                      monkeypatch, caplog)
    assert landed["current_session"] == GOOD["current_session"]
    assert problems == []


async def test_a_truncated_object_keeps_its_complete_keys(
    akili_paths, stub_embeddings, monkeypatch, caplog
):
    """
    Hit whenever the extractor runs out of output tokens (num_predict=1024).
    Was `..._is_swallowed_silently`: the whole turn was discarded. The complete
    prefix is now recovered rather than lost.
    """
    payload = '{"student_profile": "The student is named Marc and plays bask'
    landed, problems = await _extract(payload, akili_paths, stub_embeddings,
                                      monkeypatch, caplog)
    assert "Marc" in landed["student_profile"]


async def test_a_fence_inside_prose_is_tolerated(
    akili_paths, stub_embeddings, monkeypatch, caplog
):
    payload = "Sure! ```json\n" + json.dumps(GOOD) + "\n``` Let me know if that works."
    landed, problems = await _extract(payload, akili_paths, stub_embeddings,
                                      monkeypatch, caplog)
    assert landed["current_session"] == GOOD["current_session"]
    assert problems == []


async def test_single_quoted_pseudo_json_is_reported_not_swallowed(
    akili_paths, stub_embeddings, monkeypatch, caplog
):
    payload = "{'student_profile': 'The student is named Marc', 'current_session': 'x'}"
    landed, problems = await _extract(payload, akili_paths, stub_embeddings,
                                      monkeypatch, caplog)
    assert landed == {}
    assert any("no JSON object" in p for p in problems), problems


async def test_a_json_array_is_reported_not_swallowed(
    akili_paths, stub_embeddings, monkeypatch, caplog
):
    """
    json.loads succeeds but the result is not a mapping. The parser keeps
    looking for an object rather than crashing on .items().
    """
    payload = json.dumps([GOOD])
    landed, problems = await _extract(payload, akili_paths, stub_embeddings,
                                      monkeypatch, caplog)
    assert landed["current_session"] == GOOD["current_session"]


async def test_an_unknown_key_is_silently_discarded(
    akili_paths, stub_embeddings, monkeypatch, caplog
):
    """
    update_node_content returns early for an id not in dll['nodes']
    (controller.py:259-260) with no log line at all.
    """
    payload = json.dumps({"favourite_colour": "The student likes blue a lot."})
    landed, errors = await _extract(payload, akili_paths, stub_embeddings,
                                    monkeypatch, caplog)
    assert landed == {}
    assert errors == [], "not even an error is logged for an unknown block id"


async def test_a_non_string_value_is_reported_and_the_rest_survives(
    akili_paths, stub_embeddings, monkeypatch, caplog
):
    """
    Was `..._raises_and_is_swallowed`: new_info.strip() on a dict raised and the
    bare except discarded the whole turn. The bad key is now reported and the
    others still land. ('x' is under the 5-char floor, so nothing lands here.)
    """
    payload = json.dumps({"student_profile": {"name": "Marc"}, "current_session": "x"})
    landed, problems = await _extract(payload, akili_paths, stub_embeddings,
                                      monkeypatch, caplog)
    assert landed == {}
    assert any("student_profile" in p and "dict" in p for p in problems), problems


async def test_one_bad_key_does_not_discard_the_good_ones(
    akili_paths, stub_embeddings, monkeypatch, caplog
):
    """
    The loop at agent.py:76-78 is inside the try. A failure on the first key
    abandons the rest -- extraction is all-or-nothing per turn, not per key.
    """
    payload = json.dumps({
        "student_profile": 12345,                                   # not a str
        "current_session": "The student is studying fractions.",    # valid
    })
    landed, problems = await _extract(payload, akili_paths, stub_embeddings,
                                      monkeypatch, caplog)
    assert landed["current_session"] == "The student is studying fractions."
    assert "student_profile" not in landed
    assert any("student_profile" in p for p in problems), problems


async def test_realistic_malformed_output_still_updates_memory(
    akili_paths, stub_embeddings, monkeypatch, caplog
):
    payload = "Here you go:\n```json\n" + json.dumps(GOOD) + "\n```\nHope that helps!"
    landed, problems = await _extract(payload, akili_paths, stub_embeddings,
                                      monkeypatch, caplog)
    assert landed.get("current_session") == GOOD["current_session"]


def test_extractor_llm_is_deterministic_on_every_branch(monkeypatch):
    import inspect
    import llm_provider
    src = inspect.getsource(llm_provider.get_extractor_llm)
    temps = [
        line.strip() for line in src.splitlines() if "temperature=" in line
    ]
    assert all("temperature=0.0" in t or "temperature=0," in t for t in temps), temps
