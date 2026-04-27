import os
import json
import httpx
import pandas as pd
import hashlib
from typing import Dict, List, Optional, Tuple
import asyncio

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from app_local.config import settings
from app_local.storage.lance_driver import get_db

# ── Local manifest path ───────────────────────────────────────────────────────
LOCAL_MANIFEST_PATH = os.path.join(
    os.path.dirname(settings.LANCE_DB_PATH), "local_manifest.json"
)


async def _get_storage_client():
    """Initializes an authenticated GCS client."""
    try:
        from google.cloud import storage
        from google.oauth2 import service_account
        import google.auth
    except ImportError:
        print("  [ERROR] Google Cloud libraries not installed.")
        return None

    gac_path = settings.GOOGLE_APPLICATION_CREDENTIALS
    try:
        if gac_path and os.path.exists(gac_path):
            credentials = service_account.Credentials.from_service_account_file(gac_path)
            return storage.Client(credentials=credentials)
        else:
            credentials, project = google.auth.default()
            return storage.Client(credentials=credentials, project=project)
    except Exception as e:
        print(f"[Sync] Auth failed: {e}")
        return None


async def _fetch_remote_json(blob_name: str) -> Optional[dict]:
    """Downloads and parses a remote JSON file from GCS."""
    client = await _get_storage_client()
    if not client:
        return None
    
    try:
        bucket_name = settings.MANIFEST_URL.split("/")[3] # Extrait akili-registry de l'URL
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        content = blob.download_as_text()
        return json.loads(content)
    except Exception as e:
        print(f"[Sync] Error fetching {blob_name}: {e}")
        return None


async def get_remote_catalog() -> dict:
    """
    Fetches the remote manifest and returns the course catalog.
    """
    manifest = await _fetch_remote_json("manifest.json")
    if not manifest:
        return {}
    return manifest.get("catalog", {})


def _load_local_manifest() -> dict:
    """Loads the local manifest from disk."""
    if os.path.exists(LOCAL_MANIFEST_PATH):
        with open(LOCAL_MANIFEST_PATH, "r") as f:
            return json.load(f)
    return {"files": {}, "last_sync": None}


def _save_local_manifest(manifest: dict):
    """Saves the local manifest to disk."""
    os.makedirs(os.path.dirname(LOCAL_MANIFEST_PATH), exist_ok=True)
    with open(LOCAL_MANIFEST_PATH, "w") as f:
        json.dump(manifest, f, indent=2)


def is_course_available_locally(class_level: str, subject: str) -> bool:
    """
    Checks if a course already exists in local LanceDB.
    Uses the local manifest as the source of truth.
    """
    local_manifest = _load_local_manifest()
    course_id = f"{class_level}_{subject}"
    return course_id in local_manifest.get("files", {})


def get_file_hash(filepath: str) -> str:
    """Calculates the SHA256 hash of a local file."""
    sha256_hash = hashlib.sha256()
    with open(filepath, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return f"sha256:{sha256_hash.hexdigest()}"


async def download_course(class_level: str, subject: str) -> Tuple[bool, str]:
    """
    Downloads a specific course from the registry and imports it into LanceDB.
    """
    print(f"[Sync] Downloading course: {class_level}/{subject}...")

    # 1. Fetch remote manifest
    manifest = await _fetch_remote_json("manifest.json")
    if not manifest:
        return False, "Unable to reach the registry. Check your credentials."

    # 2. Find the course file in the manifest
    course_id = f"{class_level}_{subject}"
    file_info = None
    for f in manifest.get("files", []):
        if f.get("id") == course_id or (
            f.get("class") == class_level and f.get("subject") == subject
        ):
            file_info = f
            break

    if not file_info:
        return False, f"Course '{class_level}/{subject}' not found in the registry."

    # 3. Download the parquet file via GCS Client
    os.makedirs(settings.CACHE_DIR, exist_ok=True)
    temp_parquet = os.path.join(settings.CACHE_DIR, file_info["filename"])

    try:
        client = await _get_storage_client()
        bucket_name = settings.MANIFEST_URL.split("/")[3]
        bucket = client.bucket(bucket_name)
        # The parquet files are at the root of the bucket, not in /courses
        blob_name = file_info['filename']
        blob = bucket.blob(blob_name)
        blob.download_to_filename(temp_parquet)
    except Exception as e:
        return False, f"GCS Download failed: {e}"

    # 4. Import into LanceDB
    try:
        df = pd.read_parquet(temp_parquet)
        db = get_db()

        # Robust check for table existence
        all_tables = db.list_tables()
        if "edu_registry" not in all_tables:
            try:
                db.create_table("edu_registry", data=df)
            except Exception:
                # If it was created by another thread just in time, just open it
                table = db.open_table("edu_registry")
                table.delete(f"class_level = '{class_level}' AND subject = '{subject}'")
                table.add(df)
        else:
            table = db.open_table("edu_registry")
            # Replace old entries for this class/subject
            table.delete(f"class_level = '{class_level}' AND subject = '{subject}'")
            table.add(df)

        # Cleanup temp file
        os.remove(temp_parquet)
    except Exception as e:
        return False, f"Import into LanceDB failed: {e}"

    # 5. Update local manifest
    local_manifest = _load_local_manifest()
    local_manifest["files"][course_id] = {
        "class": class_level,
        "subject": subject,
        "hash": file_info.get("hash", ""),
        "downloaded_at": __import__("datetime").datetime.now().isoformat()
    }
    _save_local_manifest(local_manifest)

    return True, f"Course '{class_level} — {subject}' successfully downloaded and imported."


async def download_prompts() -> Tuple[bool, str]:
    """Downloads and saves prompts from the registry."""
    manifest = await _fetch_json(settings.MANIFEST_URL)
    if not manifest or "prompts" not in manifest:
        return False, "No prompts section found in registry manifest."

    prompts_info = manifest["prompts"]
    prompts_data = await _fetch_json(prompts_info["url"])
    if not prompts_data:
        return False, "Could not download prompts."

    prompts_path = os.path.join(
        os.path.dirname(settings.LANCE_DB_PATH), "prompts.json"
    )
    with open(prompts_path, "w", encoding="utf-8") as f:
        json.dump(prompts_data, f, indent=2, ensure_ascii=False)

    return True, "Prompts updated successfully."


async def sync_with_registry():
    """
    Full synchronization: checks all locally downloaded courses against remote
    manifest and updates any that have a newer hash.
    """
    print("Starting synchronization with Akili registry...")

    manifest = await _fetch_json(settings.MANIFEST_URL)
    if not manifest:
        print("Error: Unable to reach the registry (check your connection).")
        return

    # --- Part 2: Sync Prompts ---
    if "prompts" in manifest:
        print("  Checking for prompt updates...")
        prompts_info = manifest["prompts"]
        
        # Simple check: do we have a local prompts file?
        prompts_path = os.path.join(os.path.dirname(settings.LANCE_DB_PATH), "prompts.json")
        needs_update = True
        
        if os.path.exists(prompts_path):
            local_hash = get_file_hash(prompts_path)
            if local_hash == prompts_info["hash"]:
                needs_update = False
        
        if needs_update:
            print("  Updating system prompts...")
            # We reuse the logic from _fetch_remote_json (using blob_name)
            # The prompt file in GCS is at: prompts/prompts_v1.json
            prompts_data = await _fetch_remote_json("prompts/prompts_v1.json")
            if prompts_data:
                with open(prompts_path, "w", encoding="utf-8") as f:
                    json.dump(prompts_data, f, indent=2, ensure_ascii=False)
                updated = True
                print("  ✅ Prompts updated.")
            else:
                print("  ❌ Failed to update prompts.")

    if updated:
        print("Synchronization completed successfully.")
    else:
        print("Everything is up to date. No action required.")


if __name__ == "__main__":
    asyncio.run(sync_with_registry())
