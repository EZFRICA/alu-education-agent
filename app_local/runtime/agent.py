import os
import json
from typing import Annotated, TypedDict, List
from langgraph.graph import StateGraph, END
from langgraph.graph.message import add_messages
from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage

# Path alignment
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import llm_provider
from llm_provider import get_main_llm, get_extractor_llm
from logger import get_logger
from app_local.config import settings
from app_local.mmu import controller
from app_local.core.block_detector import detect_new_block_opportunity
from app_local.core.extraction import parse_extraction

logger = get_logger(__name__)

# Providers — built lazily. Constructing them at import made the module
# unimportable without an API key, and since llm_provider now raises on an
# unknown LLM_PROVIDER, a typo in .env stopped the app from starting instead of
# failing at the first question.
_llm = None
_extractor_llm = None


def _bind_tools(llm):
    """
    Give the tutor the TEU tools, if the configured model supports it.

    Not every backend does — function calling is a per-model capability, and a
    small local model chosen to fit a Raspberry Pi may not have it. Failing to
    bind must degrade to a tutor without tools, not stop the app from starting;
    set TEU_ENABLED=false to skip this entirely.
    """
    if not settings.TEU_ENABLED:
        logger.info("TEU disabled by configuration — no tools bound.")
        return llm
    try:
        from app_local.teu.tools import get_tools
        return llm.bind_tools(get_tools())
    except Exception as e:
        logger.warning(
            "Could not bind TEU tools to the configured model (%s). "
            "Continuing without tools.", e
        )
        return llm


def _get_llm():
    global _llm
    if _llm is None:
        _llm = _bind_tools(get_main_llm())
    return _llm


def _get_extractor_llm():
    global _extractor_llm
    if _extractor_llm is None:
        _extractor_llm = get_extractor_llm()
    return _extractor_llm

# --- State Graph Definition ---
class AgentState(TypedDict):
    # add_messages so tool round trips ACCUMULATE. Without a reducer each node
    # return replaced the list, which is fine for a single node and silently
    # loses the tool call/result pair the moment there are two.
    messages: Annotated[List[BaseMessage], add_messages]
    agent_id: str
    class_level: str
    subject: str
    memory_only_mode: bool
    needs_new_block: str
    proposed_block_config: dict
    # Carried across tool round trips so re-entry does not re-embed the query,
    # re-run retrieval, or re-run BMJ. `dll` stays the turn's single handle
    # (see search_memory) rather than being reloaded into a second copy.
    system_prompt: str
    dll: dict
    tool_iterations: int
    memory_problems: List[str]


# A tutor needs a couple of lookups, not an agent loop. Bounded because this
# runs unattended on hardware a student cannot debug.
MAX_TOOL_ITERATIONS = 4

# --- Memory Write-Back (Local Edition) ---
async def _update_student_memory(
    user_query: str,
    agent_response: str,
    dll: dict
) -> List[str]:
    """
    Extract new information and save it to local LanceDB.

    Returns the list of problems encountered, empty when everything landed. The
    caller surfaces them: a student whose memory silently stopped updating has
    no way to know, and this used to be a single log line.
    """
    # Normalize response (Gemini may return a list of blocks)
    def _to_str(c):
        if isinstance(c, str): return c
        if isinstance(c, list): return " ".join(b.get("text", "") for b in c if isinstance(b, dict))
        return str(c)
    agent_response_str = _to_str(agent_response)
    
    extraction_prompt = f"""You are a memory extraction system for Akili education agent.
Extract information from this exchange to update the student memory blocks.

STUDENT: "{user_query}"
AKILI: "{agent_response_str[:400]}"

Return ONLY a valid JSON with the following keys (empty string if nothing to update):
- student_profile: Any personal info (name, age, grade, school, goals...)
- learning_preferences: Learning style, difficulty level, preferences...
- current_session: What the student is currently studying (topic, subject, chapter). Always fill this based on the conversation.

Rules:
- current_session MUST always be updated with the current topic.
- Use plain text sentences, not keywords.
- If no personal info is shared, leave student_profile and learning_preferences empty.

Example: {{"student_profile": "", "learning_preferences": "", "current_session": "The student is asking about the manorial system in the Middle Ages (5th Grade History)."}}
"""
    problems: List[str] = []
    try:
        response = await _get_extractor_llm().ainvoke([HumanMessage(content=extraction_prompt)])
    except Exception as e:
        logger.error(f"Error during memory extraction: {e}")
        return [f"the memory extractor could not be reached ({e})"]

    updates, problems = parse_extraction(response.content)

    for block_id, new_info in updates.items():
        # Per block, so one failing write cannot discard the others.
        try:
            await controller.update_node_content(block_id, new_info, dll)
        except Exception as e:
            logger.error("Failed to store memory block '%s': %s", block_id, e)
            problems.append(f"{block_id}: could not be saved ({e})")

    if not updates:
        problems.append("nothing was extracted from this exchange")
    for p in problems:
        logger.warning("Memory extraction: %s", p)
    return problems

# --- Nodes ---

async def planner_node(state: AgentState):
    """
    Main node that:
    1. Vectorizes the query
    2. Searches course data (LanceDB)
    3. Searches student memory (DLL)
    4. Generates a pedagogical response
    """
    # The student's question is the first human turn: on re-entry after a tool
    # call, messages[-1] is a ToolMessage.
    user_query = next(
        (m.content for m in reversed(state["messages"])
         if isinstance(m, HumanMessage)),
        state["messages"][-1].content,
    )

    # On re-entry after a tool round trip the context is already built. Redoing
    # it would re-embed, re-run retrieval and re-run the BMJ promotion once per
    # tool call.
    if state.get("system_prompt") and state.get("dll"):
        return await _generate(state, user_query, state["system_prompt"],
                               state["dll"])

    # 1. Query Vectorization — local ONNX by default, no network round trip.
    # Resolved through the module, not a name bound at import: the embedder is
    # process-cached and swappable, and binding the factory at import time
    # freezes whatever it happened to be.
    query_vector = await llm_provider.get_embedder().aembed_query(user_query)

    # 2. DLL Routing & Context Compilation
    dll = await controller.load_dll()

    # Local semantic search in LanceDB
    # `dll` is this turn's single DLL handle: search_memory applies the BMJ
    # promotion to it in place, and the memory write-back below persists that
    # same object. Passing it is what stops the write-back reverting the routing.
    relevant_blocks = await controller.search_memory(
        query_vector,
        state["class_level"],
        state["subject"],
        dll=dll
    )
    
    context_text = "\n".join([f"--- {b['chapter_id']} ---\n{b['content']}" for b in relevant_blocks])
    
    # 3. Memory Context (Hybrid L1/L2 access)
    from app_local.mmu import cache_l1
    memory_context = ""
    for node_id in ["student_profile", "learning_preferences", "current_session"]:
        # Priority 1: L1 Cache (Hot RAM)
        content = cache_l1.get(node_id)
        if content:
            controller.record_access(node_id, dll)
        
        # Priority 2: DLL Metadata (L2)
        if not content:
            node = dll["nodes"].get(node_id, {})
            content = node.get("content")
            
            # Write-back to L1 if found in L2
            if content:
                cache_l1.set(node_id, content, block_type=node.get("type"))
                controller.record_access(node_id, dll)
        
        # Fallback to keywords for routing context
        if not content:
            node = dll["nodes"].get(node_id, {})
            content = ", ".join(node.get("keywords", []))

        if content:
            label = dll["nodes"].get(node_id, {}).get("label", node_id)
            memory_context += f"- {label}: {content}\n"

    # 4. Dynamic Pedagogical Prompt
    prompts_path = os.path.join(os.path.dirname(settings.LANCE_DB_PATH), "prompts.json")
    base_instructions = "You are Akili, an expert academic tutor."
    class_guidelines = ""
    
    if os.path.exists(prompts_path):
        try:
            with open(prompts_path, "r") as f:
                prompts_data = json.load(f)
                # 1. Load general tutor persona
                base_instructions = prompts_data.get("system_tutor", base_instructions)
                # 2. Load class-specific guidelines (e.g., 6eme)
                class_guidelines = prompts_data.get(state["class_level"], "")
        except Exception as e:
            logger.warning(f"Failed to load dynamic prompts: {e}")

    system_prompt = f"""{base_instructions}

{class_guidelines}

CURRENT MISSION: Help the student master {state['subject']} ({state['class_level']}).

COURSE CONTEXT (Search Results):
{context_text}

STUDENT MEMORY (L1/L2):
{memory_context}

Respond as a helpful tutor. Keep it concise but warm. Use the Socratic method when possible.
"""
    
    return await _generate(state, user_query, system_prompt, dll)


async def _generate(state: AgentState, user_query: str, system_prompt: str,
                    dll: dict) -> dict:
    """
    Invoke the tutor LLM and either hand a tool request back to the graph or
    finish the turn. Shared by the first pass and by every re-entry after a
    tool round trip, so the write-back logic exists once.
    """
    # SystemMessage, not HumanMessage: the instructions are not a student turn.
    # NOTE: this changes how the model weighs the persona and course context.
    # It needs manual evaluation of answer quality — no test can show it is
    # better, only that the right message type is sent.
    messages = [SystemMessage(content=system_prompt)] + state["messages"]
    response = await _get_llm().ainvoke(messages)

    # Tool calls: hand back to the graph, which routes to the TEU and re-enters
    # here with the results. Memory write-back is deliberately NOT done on this
    # path -- it would fire once per tool round trip, costing an extra
    # extraction LLM call each time and archiving a half-finished turn.
    if _tool_calls_of(response):
        return {
            "messages": [response],
            "system_prompt": system_prompt,
            "dll": dll,
            "tool_iterations": state.get("tool_iterations", 0) + 1,
        }

    # Final answer — write memory back once, on the completed turn.
    memory_problems = await _update_student_memory(user_query, response.content, dll)

    history = [{"role": "user" if isinstance(m, HumanMessage) else "assistant", "content": m.content} for m in state["messages"]]
    history.append({"role": "assistant", "content": response.content})

    proposal = detect_new_block_opportunity(history, dll)
    needs_new = "True" if proposal else "False"

    return {
        "messages": [response],
        "needs_new_block": needs_new,
        "proposed_block_config": proposal or {},
        # Surfaced by the UI. A parse failure used to be one log line, so a
        # student whose memory stopped updating had no way to know.
        "memory_problems": memory_problems,
    }


def _tool_calls_of(message) -> list:
    """Tool calls on a response, tolerating providers that omit the attribute."""
    return list(getattr(message, "tool_calls", None) or [])


def _route_after_planner(state: AgentState) -> str:
    """
    Send the turn to the TEU when the model asked for a tool, otherwise finish.

    The iteration cap is not a formality: a model that keeps re-requesting a
    tool would loop until the process is killed, and this runs unattended on
    hardware a student cannot debug.
    """
    last = state["messages"][-1] if state["messages"] else None
    if last is not None and _tool_calls_of(last):
        if state.get("tool_iterations", 0) <= MAX_TOOL_ITERATIONS:
            return "Tools"
        logger.warning(
            "TEU: iteration cap (%d) reached — answering without further tools.",
            MAX_TOOL_ITERATIONS,
        )
    return END


# --- Graph Assembly ---

def create_agent_graph():
    """
    Planner ──tool_calls?──> Tools ──> Planner ──> END

    The Planner re-enters after each tool round trip; it rebuilds nothing,
    because the system prompt and the DLL handle travel in the state.
    """
    from langgraph.prebuilt import ToolNode
    from app_local.teu.tools import get_tools

    workflow = StateGraph(AgentState)

    workflow.add_node("Planner", planner_node)
    workflow.add_node("Tools", ToolNode(get_tools()))

    workflow.set_entry_point("Planner")
    workflow.add_conditional_edges(
        "Planner", _route_after_planner, {"Tools": "Tools", END: END}
    )
    workflow.add_edge("Tools", "Planner")

    return workflow.compile()
