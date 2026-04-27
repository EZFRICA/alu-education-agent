import os
import json
import asyncio
from typing import TypedDict, List, Dict, Annotated
from langgraph.graph import StateGraph, END
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage

# Path alignment
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from llm_provider import get_main_llm, get_extractor_llm
from logger import get_logger
from app_local.config import settings
from app_local.mmu import controller
from app_local.core.block_detector import detect_new_block_opportunity

logger = get_logger(__name__)

# Providers
_llm = get_main_llm()
_extractor_llm = get_extractor_llm()

# --- State Graph Definition ---
class AgentState(TypedDict):
    messages: List[BaseMessage]
    agent_id: str
    class_level: str
    subject: str
    memory_only_mode: bool
    needs_new_block: str
    proposed_block_config: dict

# --- Memory Write-Back (Local Edition) ---
async def _update_student_memory(
    user_query: str,
    agent_response: str,
    dll: dict
) -> None:
    """
    Extracts new information and saves it to local LanceDB.
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
    try:
        response = await _extractor_llm.ainvoke([HumanMessage(content=extraction_prompt)])
        raw_content = response.content
        if isinstance(raw_content, list):
            raw_content = " ".join(b.get("text", "") for b in raw_content if isinstance(b, dict))
        raw = raw_content.strip().replace("```json", "").replace("```", "").strip()
        updates = json.loads(raw)

        for block_id, new_info in updates.items():
            if new_info and len(new_info.strip()) > 5:
                await controller.update_node_content(block_id, new_info, dll)
    except Exception as e:
        logger.error(f"Error during memory extraction: {e}")

# --- Nodes ---

async def planner_node(state: AgentState):
    """
    Main node that:
    1. Vectorizes the query
    2. Searches course data (LanceDB)
    3. Searches student memory (DLL)
    4. Generates a pedagogical response
    """
    user_query = state["messages"][-1].content
    
    # 1. Query Vectorization (via Gemini)
    from langchain_google_genai import GoogleGenerativeAIEmbeddings
    embeddings_model = GoogleGenerativeAIEmbeddings(model="models/gemini-embedding-2")
    query_vector = await embeddings_model.aembed_query(user_query)

    # 2. DLL Routing & Context Compilation
    dll = await controller.load_dll()
    
    # Local semantic search in LanceDB
    relevant_blocks = await controller.search_memory(
        query_vector, 
        state["class_level"], 
        state["subject"]
    )
    
    context_text = "\n".join([f"--- {b['chapter_id']} ---\n{b['content']}" for b in relevant_blocks])
    
    # 3. Memory Context (Hybrid L1/L2 access)
    from app_local.mmu import cache_l1
    memory_context = ""
    for node_id in ["student_profile", "learning_preferences", "current_session"]:
        # Priority 1: L1 Cache (Hot RAM)
        content = cache_l1.get(node_id)
        
        # Priority 2: DLL Metadata (L2)
        if not content:
            node = dll["nodes"].get(node_id, {})
            content = node.get("content")
            
            # Write-back to L1 if found in L2
            if content:
                cache_l1.set(node_id, content, block_type=node.get("type"))
        
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
    
    messages = [HumanMessage(content=system_prompt)] + state["messages"]
    response = await _llm.ainvoke(messages)
    
    # 5. Await Memory Update (must complete before returning to ensure L1 write-back)
    await _update_student_memory(user_query, response.content, dll)
    
    # 6. Block Opportunity Detection
    history = [{"role": "user" if isinstance(m, HumanMessage) else "assistant", "content": m.content} for m in state["messages"]]
    history.append({"role": "assistant", "content": response.content})
    
    proposal = detect_new_block_opportunity(history, dll)
    needs_new = "True" if proposal else "False"

    return {
        "messages": [response],
        "needs_new_block": needs_new,
        "proposed_block_config": proposal or {}
    }

# --- Graph Assembly ---

def create_agent_graph():
    workflow = StateGraph(AgentState)
    
    workflow.add_node("Planner", planner_node)
    workflow.set_entry_point("Planner")
    workflow.add_edge("Planner", END)
    
    return workflow.compile()
