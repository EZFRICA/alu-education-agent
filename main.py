import sys
import subprocess

def main():
    print("======================================================")
    print("   UX-Driven Agent Memory 🧠✈️  ")
    print("======================================================")
    print("1. Launch Streamlit Dashboard (UI)")
    print("2. Launch CLI Travel Agent")
    print("3. Initialize Weaviate Schemas")
    print("4. Create Letta Agent")
    print("5. Sync Memory to Weaviate (Full Sync)")
    print("------------------------------------------------------")
    
    choice = input("Select an option [1-5]: ").strip()
    
    if choice == "1":
        print("\n[Launching Dashboard...] (Press Ctrl+C to stop)")
        subprocess.run(["uv", "run", "streamlit", "run", "dashboard/app.py"])
    elif choice == "2":
        subprocess.run([sys.executable, "agent_os/cli_driver.py"])
    elif choice == "3":
        subprocess.run([sys.executable, "apu/storage/schema.py"])
    elif choice == "4":
        subprocess.run([sys.executable, "apu/storage/letta_driver.py", "--create-agent"])
    elif choice == "5":
        subprocess.run([sys.executable, "apu/storage/sync_util.py"])
    else:
        print("Invalid choice. Exiting.")

if __name__ == "__main__":
    main()
