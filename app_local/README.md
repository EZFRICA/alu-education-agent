# 📱 Akili Student App (Local-First)

This is the client-side application for the student. It operates with a **Local-First** philosophy: all student data and course materials are stored on the device for maximum privacy and zero latency, while syncing with the Cloud Registry for updates.

---

## 🏗️ Architecture

The app is built as a **Semantic APU (Agent Processor Unit)**:

*   **`mmu/` (Memory Management Unit)**: Handles the DLL (Dynamic Learning Layer). It manages the **L1 Cache (RAM)** and **L2 Storage** to provide the agent with the perfect context for every turn.
*   **`runtime/` (Agent OS)**: The core reasoning engine built with **LangGraph**. It implements the Socratic method and manages the interaction loop.
*   **`storage/` (L3 Archive)**: Driven by **LanceDB**, it stores the vectorized course content and archival memories.
*   **`sync/` (Sync Manager)**: Automatically fetches the `manifest.json` from the Cloud Registry and downloads Parquet files or updated Prompts.
*   **`ui/` (Dashboard)**: A real-time observability monitor built with Streamlit to visualize the APU's internal state.

---

## 🛠️ Getting Started

### Launch the Application
```bash
uv run python app_local/main.py
```

### Key Features
1.  **AI Tutoring**: Chat with Akili about any subject in the curriculum.
2.  **Memory Monitoring**: Watch L1 hits and misses as you talk.
3.  **Registry Sync**: Use the "Check for Updates" button to download new lessons from the cloud.
4.  **Memory Reset**: A "Nuclear Option" to wipe L1 and L2 for a fresh start.

---

## ⚙️ Configuration

Settings are centralized in `app_local/config/settings.py`:
*   `MANIFEST_URL`: Your GCS bucket manifest link.
*   `LANCE_DB_PATH`: Where the vector database lives.
*   `EDU_DEFAULT_CLASS`: Default class level on startup (e.g., "6eme").

---
*Empowering students through agentic, personalized learning.*
