import sys
import os

# Add root path for memory imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from memory.dll_manager import search_memory, load_dll, save_dll, move_to_front, toggle_block
from memory.context_compiler import compile_working_context
from memory.letta_cloud_client import send_message

def handle_manage_memory(command: str, dll: dict) -> dict:
    parts = command.strip().split()
    if len(parts) == 1:
        # just /manage-memory
        print("\n--- DLL MEMORY STATE ---")
        nodes = dll["nodes"]
        current = dll["head_id"]
        while current:
            n = nodes[current]
            status = "ACTIVE" if n["active"] else "INACTIVE"
            print(f"[{current}] -> Type: {n['type']} | Status: {status} | Access: {n['access_count']}")
            current = n["next"]
        print("------------------------------")
        print("Commands: /manage-memory enable <id> | disable <id> | reset")
        return dll
        
    elif len(parts) == 3 and parts[1] == "enable":
        dll = toggle_block(parts[2], True, dll)
        save_dll(dll)
    elif len(parts) == 3 and parts[1] == "disable":
        dll = toggle_block(parts[2], False, dll)
        save_dll(dll)
    elif len(parts) == 2 and parts[1] == "reset":
        print("Reset not implemented. Delete metadata_links.json to re-init.")
        
    return dll


def main():
    print("======================================================")
    print("   TRAVEL PLANNER AGENT (DLL + LETTA CLOUD)  ")
    print("======================================================")
    print("Type '/manage-memory' to inspect blocks")
    print("Type 'quit' or 'exit' to stop")
    print("------------------------------------------------------\n")

    dll = load_dll()
    agent_id = dll.get("agent_id")
    
    if not agent_id:
        print("[!] No Letta agent_id defined in metadata_links.json.")
        print("[!] Please run `python memory/letta_cloud_client.py --create-agent` first")
        print("[!] Then add the generated ID in the 'agent_id' field of metadata_links.json.")
        return

    while True:
        try:
            user_input = input("\nYou > ")
            if not user_input.strip():
                continue
                
            if user_input.lower() in ["quit", "exit"]:
                break
                
            if user_input.startswith("/manage-memory"):
                dll = handle_manage_memory(user_input, dll)
                continue
                
            # 1. DLL Memory Pipeline
            print("\n[Processing DLL...]")
            relevant_blocks = search_memory(user_input, dll)
            
            # 2. Context Compilation
            working_context = compile_working_context(agent_id, relevant_blocks, user_input)
            
            # 3. LLM Request via Letta
            print("[Generating via Letta/Gemini...]")
            response = send_message(agent_id, working_context, user_input)
            
            # Print response
            print("\nAgent  > " + response)
            
            # 4. Post-processing: Move-To-Front on the most relevant block
            if relevant_blocks:
                primary_block_id = relevant_blocks[0]['id']
                dll = move_to_front(primary_block_id, dll)
                save_dll(dll)
                
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"\n[Error] {e}")

if __name__ == "__main__":
    main()
