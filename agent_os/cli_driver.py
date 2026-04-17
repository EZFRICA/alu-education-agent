import asyncio
from agent_os.graph import create_dll_agent_graph
from langchain_core.messages import HumanMessage
from logger import get_logger

logger = get_logger(__name__)

async def run_cli():
    print("\n--- Agent OS CLI Driver ---")
    graph = create_dll_agent_graph()
    
    agent_id = "user_abc123" # Fallback
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
