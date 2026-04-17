import os
import sys
import asyncio

# Ensure Python can find the 'agent_os' and 'apu' modules from the root
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agent_os.graph import create_dll_agent_graph
from langchain_core.messages import HumanMessage
from logger import get_logger
from apu.mmu.controller import load_dll

logger = get_logger(__name__)

async def run_cli():
    print("\n--- Agent OS CLI Driver ---")
    graph = create_dll_agent_graph()
    
    # Fetch real agent_id from DLL
    dll = await load_dll()
    agent_id = dll.get("agent_id")
    if not agent_id:
        print("Error: No agent_id found. Please run Option 4 (Create Letta Agent) first.")
        return

    state = {
        "messages": [],
        "agent_id": agent_id,
        "search_enabled": True,
        "memory_only_mode": False,
        "strict_manual_mode": False,
        "needs_new_block": "False",
        "proposed_block_config": {}
    }

    while True:
        user_input = input("\nUser: ").strip()
        if user_input.lower() in ["exit", "quit"]:
            break
            
        state["messages"].append(HumanMessage(content=user_input))
        
        print("\nAgent thinking...")
        async for output in graph.astream(state):
            for node_name, node_state in output.items():
                if node_name == "Planner":
                    last_msg = node_state["messages"][-1]
                    print(f"\nAgent: {last_msg.content}")

if __name__ == "__main__":
    try:
        asyncio.run(run_cli())
    except KeyboardInterrupt:
        print("\nExiting.")
