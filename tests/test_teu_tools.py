"""
Tool Execution Unit — every tool must run on-device and never build an LLM.

Target: app_local/teu/tools.py
"""

import pytest

from app_local.teu import tools
from conftest import V_A


# ── calculate: correctness ───────────────────────────────────────────────────

@pytest.mark.parametrize("expression,expected", [
    ("2 + 3 * 4", "14"),
    ("(3/4 + 2/5) * 20", "23"),
    ("7 // 2", "3"),
    ("7 % 3", "1"),
    ("2 ** 10", "1024"),
    ("-5 + 3", "-2"),
    ("10 / 4", "2.5"),
])
def test_calculate_is_exact(expression, expected):
    assert tools.calculate.invoke({"expression": expression}) == expected


def test_calculate_reports_division_by_zero_without_raising():
    out = tools.calculate.invoke({"expression": "1/0"})
    assert "Division by zero" in out


# ── calculate: it must not be an eval() ──────────────────────────────────────

@pytest.mark.parametrize("hostile", [
    "__import__('os').system('echo pwned')",
    "open('/etc/passwd').read()",
    "().__class__.__bases__[0].__subclasses__()",
    "print('x')",
    "1 if True else 2",
    "[i for i in range(10)]",
])
def test_calculate_refuses_anything_that_is_not_arithmetic(hostile):
    """
    Student text reaches this tool via the model. eval() here would be arbitrary
    code execution on the device.
    """
    out = tools.calculate.invoke({"expression": hostile})
    assert out.startswith("Could not evaluate"), out


def test_calculate_refuses_a_huge_exponent():
    """9**9**9 would hang a Raspberry Pi."""
    out = tools.calculate.invoke({"expression": "9 ** 9 ** 9"})
    assert "Could not evaluate" in out


# ── the local-only guarantee ─────────────────────────────────────────────────

def test_no_tool_constructs_an_llm_or_a_hardcoded_model():
    """
    google_search built its own google.genai client against a hardcoded model
    id, so GEMMA_MODEL had no effect on it. All inference goes through
    llm_provider now.
    """
    import ast
    import pathlib
    from test_import_time import REPO_ROOT

    source = pathlib.Path(REPO_ROOT, "app_local/teu/tools.py").read_text()
    tree = ast.parse(source)

    # Imports, including function-local ones. Prose in comments explaining what
    # was removed is not a violation; importing an SDK is.
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(a.name for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)

    forbidden = [m for m in imported
                 if m.split(".")[0] in {"google", "langchain_google_genai",
                                        "langchain_ollama", "langchain_openai"}]
    assert forbidden == [], f"TEU imports an LLM SDK directly: {forbidden}"

    # No hardcoded chat-model identifier in any string literal.
    literals = [n.value for n in ast.walk(tree)
                if isinstance(n, ast.Constant) and isinstance(n.value, str)]
    hardcoded = [s for s in literals
                 if "gemma-4" in s or "gemini-" in s or "gpt-" in s]
    assert hardcoded == [], f"TEU hardcodes a model id: {hardcoded}"


def test_every_registered_tool_is_exposed():
    names = {t.name for t in tools.get_tools()}
    assert names == {"load_course_chapter", "search_course_content", "calculate"}


# ── the retrieval tools, offline ─────────────────────────────────────────────

async def test_load_course_chapter_returns_local_content(akili_paths):
    from app_local.storage import lance_driver

    await lance_driver.upsert_local_block(
        block_id="chapter_2_decimals", content="A decimal number has a comma.",
        block_type="cours", class_level="6eme", subject="math", vector=list(V_A),
    )
    out = await tools.load_course_chapter.ainvoke(
        {"chapter_id": "chapter_2_decimals"}
    )
    assert out == "A decimal number has a comma."


async def test_load_course_chapter_says_so_when_absent(akili_paths):
    out = await tools.load_course_chapter.ainvoke({"chapter_id": "nope"})
    assert "not available locally" in out


async def test_search_course_content_finds_material(
    akili_paths, no_network, stub_embeddings
):
    from app_local.storage import lance_driver

    await lance_driver.upsert_local_block(
        block_id="ch_fractions", content="A fraction is part of a whole.",
        block_type="cours", class_level="6eme", subject="math", vector=list(V_A),
    )
    out = await tools.search_course_content.ainvoke({"query": "fractions"})
    assert "ch_fractions" in out
    assert "A fraction is part of a whole." in out


async def test_search_course_content_reports_an_empty_device(
    akili_paths, no_network, stub_embeddings
):
    out = await tools.search_course_content.ainvoke({"query": "anything"})
    assert "No course material" in out


async def test_search_course_content_surfaces_an_embedding_mismatch(
    akili_paths, no_network, stub_embeddings, monkeypatch
):
    """A mismatch must reach the tutor as a readable message, not a traceback."""
    from app_local.config import settings
    from app_local.storage import lance_driver

    db = lance_driver.get_db()
    db.create_table("user_memory", data=[{
        "id": "a", "chapter": "a", "content": "x", "block_type": "cours",
        "class_level": "6eme", "subject": "math", "vector": [0.1] * 16,
        "updated_at": "2026-01-01T00:00:00",
    }])
    monkeypatch.setattr(settings, "EMBEDDING_DIM", 8)

    out = await tools.search_course_content.ainvoke({"query": "anything"})
    assert "Course search is unavailable" in out
    assert "different vector spaces" in out
