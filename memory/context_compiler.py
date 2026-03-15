from .letta_cloud_client import letta
from logger import get_logger

logger = get_logger(__name__)


def get_core_block_content(agent_id: str, label: str) -> str:
    """Fetch the content of a core memory block from Letta Cloud API."""
    try:
        block = letta.agents.blocks.retrieve(label, agent_id=agent_id)
        return block.value or ""
    except Exception as e:
        logger.warning("Could not read block '%s' from Letta API: %s", label, e)
    return ""


def compile_working_context(agent_id: str, relevant_blocks: list[dict], query: str = "") -> str:
    """
    Assemble the final Working Context string to inject into the Gemini prompt.
    Only the active blocks selected by the DLL are included.
    """
    context_parts = []

    for block in relevant_blocks:
        label = block["id"]
        content = get_core_block_content(agent_id, label)
        context_parts.append(f"--- BLOCK: {block['label'].upper()} ({block['type']}) ---")
        context_parts.append(content)
        context_parts.append("")  # blank line separator

    return "\n".join(context_parts)
