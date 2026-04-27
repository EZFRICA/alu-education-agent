# ☁️ Akili Cloud Registry

This module manages the curriculum, pedagogical prompts, and distribution manifest. It acts as the "Source of Truth" for all Akili tutoring instances.

---

## 🏗️ Structure

*   **`config/curriculum.yaml`**: The master list of classes and subjects to be processed.
*   **`courses/`**: Source markdown files organized by grade and subject, plus the dynamic system prompts.
*   **`pipeline/`**:
    *   `batch_pipeline.py`: The main entry point. Processes all MD files, generates embeddings, packages them into Parquet, exports prompts, and uploads everything to GCS.
*   **`registry/`**: The distribution folder (local mirror of GCS). Contains `manifest.json`, course parquets, and `prompts_v1.json`.

---

## 🛠️ Usage

### 1. Configure the Curriculum
Edit `config/curriculum.yaml` to define your subjects (e.g., `math`, `history`, `english`).

### 2. Run the Full Pipeline
Processes everything in one go and uploads to your GCS bucket:
```bash
uv run python cloud_registry/pipeline/batch_pipeline.py --upload
```

### 3. Incremental Updates
To regenerate only the manifest (if you manually added a file to GCS):
```bash
uv run python cloud_registry/pipeline/manifest_generator.py
```

---

## ⚙️ Requirements
*   **Gemini API Key**: For generating knowledge embeddings.
*   **GCS Permissions**: To host the manifest and parquet files for the students.

---
*Ensuring a sovereign, scalable, and standardized academic knowledge base.*
