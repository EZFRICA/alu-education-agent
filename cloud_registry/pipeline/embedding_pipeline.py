import os
import sys
import pandas as pd
import asyncio
from typing import List

# Root directory setup
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from cloud_registry.storage_client import get_weaviate_client_async
from logger import get_logger
from weaviate.util import generate_uuid5
from weaviate.classes.query import Filter
from langchain_google_genai import GoogleGenerativeAIEmbeddings

REGISTRY_COLLECTION = "RegistryIndex"

# Embedding model (matches the one used by the local agent)
_embedder = GoogleGenerativeAIEmbeddings(model="models/gemini-embedding-2")

async def index_to_registry(client, class_level: str, subject: str, base_dir: str):
    """Reads MD files, generates Gemini embeddings, and pushes to RegistryIndex."""
    subject_path = os.path.join(base_dir, class_level, subject)
    
    if not os.path.exists(subject_path):
        print(f"Error: Directory not found {subject_path}")
        return

    collection = client.collections.get(REGISTRY_COLLECTION)
    print(f"Ingesting into {REGISTRY_COLLECTION} (Generalist mode with Gemini embeddings)...")

    for root, _, files in os.walk(subject_path):
        for file in files:
            if file.endswith(".md"):
                chapter_id = file.replace(".md", "")
                file_path = os.path.join(root, file)
                
                with open(file_path, "r", encoding="utf-8") as f:
                    content = f.read()

                keywords_text = f"Level: {class_level} | Subject: {subject} | Chapter: {chapter_id.replace('_', ' ')}"
                obj_uuid = generate_uuid5(f"registry_{class_level}_{subject}_{chapter_id}")

                # Explicit Gemini embedding generation
                embed_text = f"{keywords_text}\n\n{content[:2000]}"
                embedding = await _embedder.aembed_query(embed_text)

                properties = {
                    "content": content,
                    "keywords_text": keywords_text,
                    "block_id": chapter_id,
                    "chapter_id": chapter_id,
                    "class_level": class_level,
                    "subject": subject,
                    "block_type": "manual_chapter"
                }
                
                # Using replace to overwrite if object exists
                await collection.data.replace(
                    uuid=obj_uuid,
                    properties=properties,
                    vector=embedding
                )
                print(f"  [OK] {chapter_id} indexed (dim={len(embedding)}).")

async def export_weaviate_to_parquet(class_level: str, subject: str):
    """Exports data from Weaviate to Parquet for local distribution."""
    # Path to courses in cloud-registry folder
    base_courses_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "courses")

    async with get_weaviate_client_async() as client:
        # 1. Ingestion (MD -> Weaviate)
        print(f"\n--- CLOUD PRODUCTION: {class_level}/{subject} ---")
        await index_to_registry(client, class_level, subject, base_courses_dir)

        # 2. Extraction (Weaviate -> Parquet)
        print(f"\nExtracting vectorized data...")
        collection = client.collections.get(REGISTRY_COLLECTION)
        
        response = await collection.query.fetch_objects(
            filters=Filter.by_property("subject").equal(subject) & Filter.by_property("class_level").equal(class_level),
            include_vector=True,
            limit=1000
        )
        
        all_blocks = []
        for i, obj in enumerate(response.objects):
            # Debug: show vector structure on first object
            if i == 0:
                print(f"  [DEBUG] Available keys in obj.vector: {list(obj.vector.keys()) if obj.vector else 'None'}")
            
            # Robust extraction: fallback to first available key
            vector = None
            if obj.vector:
                vector = obj.vector.get("default") or next(iter(obj.vector.values()), None)
            
            all_blocks.append({
                "id": obj.properties.get("block_id"),
                "chapter": obj.properties.get("chapter_id"),
                "class_level": obj.properties.get("class_level"),
                "subject": obj.properties.get("subject"),
                "block_type": obj.properties.get("block_type"),
                "content": obj.properties.get("content"),
                "vector": vector,
                "keywords": obj.properties.get("keywords_text")
            })
        
        # Verification
        has_vectors = sum(1 for b in all_blocks if b['vector'] is not None)
        print(f"  Blocks with vectors: {has_vectors}/{len(all_blocks)}")

    if all_blocks:
        # 3. Save to Parquet
        df = pd.DataFrame(all_blocks)
        output_filename = f"{class_level}_{subject}_v1.parquet"
        
        # Save in cloud-registry/registry/ for distribution
        registry_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "registry")
        os.makedirs(registry_dir, exist_ok=True)
        
        output_path = os.path.join(registry_dir, output_filename)
        df.to_parquet(output_path, index=False)
        print(f"\n✅ SUCCESS: {output_path} generated ({len(df)} blocks).")
    else:
        print(f"\n❌ ERROR: No blocks found in Weaviate for {class_level}/{subject}.")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python embedding_pipeline.py <class_level> <subject>")
    else:
        asyncio.run(export_weaviate_to_parquet(sys.argv[1], sys.argv[2]))
