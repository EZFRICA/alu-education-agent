import os
import lancedb
import pandas as pd
from typing import List, Optional, Dict
from datetime import datetime
from weaviate.util import generate_uuid5 # Keeping UUID utility for consistency

# Add root folder for config access
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from app_local.config import settings

import threading

_db = None
_db_lock = threading.Lock()

def get_db():
    """Initializes or retrieves the connection to the local LanceDB (Thread-safe)."""
    global _db
    with _db_lock:
        if _db is None:
            db_path = settings.LANCE_DB_PATH
            os.makedirs(db_path, exist_ok=True)
            _db = lancedb.connect(db_path)
        return _db

def reset_local_db():
    """Wipes the local database entirely. Use with caution."""
    global _db
    import shutil
    with _db_lock:
        _db = None # Drop connection
        db_path = settings.LANCE_DB_PATH
        if os.path.exists(db_path):
            shutil.rmtree(db_path)
            print(f"LanceDB at {db_path} has been wiped.")

async def search_block_index(query_vector: List[float], limit: int = 12, 
                         class_level: str = None, subject: str = None) -> List[Dict]:
    """
    Unified semantic search in LanceDB across multiple tables (Courses + Memory).
    """
    db = get_db()
    all_results = []
    
    # Tables to search
    tables_to_search = ["edu_registry", "user_memory"]
    
    for table_name in tables_to_search:
        if table_name not in db.list_tables():
            continue
            
        table = db.open_table(table_name)
        
        # Build filter (optional for user_memory, strict for edu_registry)
        filter_query = ""
        if table_name == "edu_registry" and class_level and subject:
            filter_query = f"class_level = '{class_level}' AND subject = '{subject}'"
        
        # Search
        query = table.search(query_vector, vector_column_name="vector").limit(limit)
        if filter_query:
            query = query.where(filter_query)
            
        df = query.to_pandas()
        
        for _, row in df.iterrows():
            dist = row.get("_distance", 0)
            certainty = 1 - (dist / 2)
            
            all_results.append({
                "block_id": row["id"],
                "chapter_id": row.get("chapter", row.get("id")), # Fallback to ID for memory
                "block_type": row.get("block_type", "memory" if table_name == "user_memory" else "cours"),
                "certainty": certainty,
                "content": row.get("content", ""),
                "source_table": table_name
            })
            
    # Sort all merged results by certainty
    all_results.sort(key=lambda x: x["certainty"], reverse=True)
    return all_results[:limit]

async def get_block_content(block_id: str) -> Optional[str]:
    """Retrieves the content of a DLL memory node from 'user_memory' table."""
    db = get_db()
    if "user_memory" not in db.list_tables():
        return None
        
    table = db.open_table("user_memory")
    result = table.search().where(f"id = '{block_id}'").limit(1).to_pandas()
    
    if not result.empty:
        return result.iloc[0]["content"]
    return None

async def upsert_local_block(block_id: str, content: str, block_type: str, 
                           class_level: str, subject: str, vector: List[float]):
    """
    Allows the student to add their own blocks (notes, session) 
    into a separate local table 'user_memory'.
    """
    db = get_db()
    data = [{
        "id": block_id,
        "content": content,
        "block_type": block_type,
        "class_level": class_level,
        "subject": subject,
        "vector": vector,
        "updated_at": datetime.now().isoformat()
    }]
    
    if "user_memory" not in db.list_tables():
        db.create_table("user_memory", data=data)
    else:
        table = db.open_table("user_memory")
        # Simplified Upsert: we add (we could delete before for the same ID)
        table.add(data)
