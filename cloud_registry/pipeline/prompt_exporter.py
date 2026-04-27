import os
import json
import sys

def export_prompts():
    """Reads all txt files in courses/prompts and packs them into registry/prompts.json."""
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    prompts_dir = os.path.join(base_dir, "courses", "prompts")
    registry_dir = os.path.join(base_dir, "registry")
    
    if not os.path.exists(prompts_dir):
        print(f"Error: Prompts directory {prompts_dir} not found.")
        return

    prompts_data = {}
    
    for filename in os.listdir(prompts_dir):
        if filename.endswith(".txt"):
            prompt_key = filename.replace(".txt", "")
            filepath = os.path.join(prompts_dir, filename)
            
            with open(filepath, "r", encoding="utf-8") as f:
                prompts_data[prompt_key] = f.read()
            
    if prompts_data:
        os.makedirs(registry_dir, exist_ok=True)
        output_path = os.path.join(registry_dir, "prompts.json")
        
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(prompts_data, f, indent=2)
            
        print(f"Success! Exported {len(prompts_data)} prompts to {output_path}")
    else:
        print("No prompts found to export.")

if __name__ == "__main__":
    export_prompts()
