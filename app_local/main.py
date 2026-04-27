import os
import subprocess
import sys
from pathlib import Path

def main():
    """
    Main entry point for Akili Education Agent.
    Launches the Streamlit Dashboard directly.
    """
    # Find the dashboard script path
    root_dir = Path(__file__).resolve().parent
    dashboard_path = root_dir / "ui" / "dashboard.py"

    if not dashboard_path.exists():
        print(f"Error: Dashboard not found at {dashboard_path}")
        sys.exit(1)

    print("\n" + "="*50)
    print("🚀 LAUNCHING AKILI EDUCATION AGENT DASHBOARD")
    print("="*50 + "\n")

    # Run streamlit as a subprocess
    try:
        subprocess.run(["streamlit", "run", str(dashboard_path)], check=True)
    except KeyboardInterrupt:
        print("\nStopping Akili...")
    except Exception as e:
        print(f"Error launching dashboard: {e}")

if __name__ == "__main__":
    main()
