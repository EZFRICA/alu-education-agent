"""
Tool Execution Unit — tools the tutor can call.

Every tool here runs ON THE DEVICE. That is deliberate: a tool that needs the
network turns a student question into a failure the moment connectivity drops,
which is the condition this project is built for.

No tool constructs an LLM client. Inference belongs to llm_provider, and there
is exactly one model in play. The previous `google_search` tool built its own
google.genai client against a hardcoded model id, so changing GEMMA_MODEL had no
effect on it — the same defect as C6 for embeddings.
"""

import ast
import operator
import os
from typing import List

from langchain_core.tools import tool

# Imports locaux
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from app_local.storage import lance_driver
from logger import get_logger

logger = get_logger(__name__)


@tool
async def load_course_chapter(chapter_id: str) -> str:
    """
    Load the full text of one course chapter from the local database.
    Use this when you need the exact wording of a lesson rather than a summary.
    Takes the chapter identifier, e.g. 'chapter_2_decimal_numbers'.
    """
    logger.info("TEU: loading chapter '%s'", chapter_id)
    content = await lance_driver.get_block_content(chapter_id)
    if not content:
        return (
            f"Chapter '{chapter_id}' is not available locally. "
            f"Use search_course_content to find what this device actually has."
        )
    return content


@tool
async def search_course_content(query: str, limit: int = 3) -> str:
    """
    Search the downloaded curriculum for passages relevant to a question.
    Use this to ground an explanation in the student's own course material
    instead of answering from general knowledge. Runs entirely on the device.
    """
    from llm_provider import get_embedder

    logger.info("TEU: searching course content for '%s'", query)
    query_vector = await get_embedder().aembed_query(query)

    try:
        results = await lance_driver.search_block_index(query_vector, limit=limit)
    except lance_driver.EmbeddingMismatch as e:
        return f"Course search is unavailable: {e}"

    if not results:
        return "No course material on this device matches that question."

    parts = []
    for r in results[:limit]:
        parts.append(
            f"--- {r['chapter_id']} (certainty {r['certainty']:.2f}) ---\n"
            f"{str(r['content'])[:800]}"
        )
    return "\n\n".join(parts)


# ── safe arithmetic ──────────────────────────────────────────────────────────
# A tutor that does mental arithmetic in the model's head gets it wrong
# occasionally and confidently. Evaluating the expression is cheap and exact.
# ast-based, NOT eval(): student input reaches this, and eval() on a string a
# model composed from student text is arbitrary code execution.

_OPS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
    ast.USub: operator.neg,
    ast.UAdd: operator.pos,
}

_MAX_EXPONENT = 64  # keeps 9**9**9 from freezing the device


def _eval_node(node):
    if isinstance(node, ast.Constant):
        if isinstance(node.value, bool) or not isinstance(node.value, (int, float)):
            raise ValueError("only numbers are allowed")
        return node.value
    if isinstance(node, ast.BinOp):
        op = _OPS.get(type(node.op))
        if op is None:
            raise ValueError("unsupported operator")
        left, right = _eval_node(node.left), _eval_node(node.right)
        if type(node.op) is ast.Pow and abs(right) > _MAX_EXPONENT:
            raise ValueError(f"exponent above {_MAX_EXPONENT} is not allowed")
        return op(left, right)
    if isinstance(node, ast.UnaryOp):
        op = _OPS.get(type(node.op))
        if op is None:
            raise ValueError("unsupported operator")
        return op(_eval_node(node.operand))
    raise ValueError("only arithmetic on numbers is allowed")


@tool
def calculate(expression: str) -> str:
    """
    Evaluate an arithmetic expression exactly: + - * / // % ** and parentheses.
    Use this for any calculation before stating a numeric result, rather than
    working it out mentally. Example: '(3/4 + 2/5) * 20'.
    """
    logger.info("TEU: calculate '%s'", expression)
    try:
        tree = ast.parse(expression, mode="eval")
        result = _eval_node(tree.body)
    except ZeroDivisionError:
        return "Division by zero — that expression has no value."
    except (ValueError, SyntaxError, TypeError) as e:
        return f"Could not evaluate '{expression}': {e}"

    if isinstance(result, float) and result.is_integer():
        result = int(result)
    return str(result)


def get_tools() -> List:
    """Every tool available to the tutor. All local, none needs the network."""
    return [load_course_chapter, search_course_content, calculate]
