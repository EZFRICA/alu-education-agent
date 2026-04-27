import os
import sys
import asyncio

# Root directory setup
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from weaviate.classes.config import Property, DataType, Tokenization, Configure
from cloud_registry.storage_client import get_weaviate_client
from logger import get_logger

logger = get_logger(__name__)

def init_registry_schema():
    """
    Initializes the RegistryIndex collection — Global reference for all users.
    Ensures multi-tenancy is DISABLED.
    """
    client = get_weaviate_client()
    try:
        if client.collections.exists("RegistryIndex"):
            print("Cleaning up old RegistryIndex collection...")
            client.collections.delete("RegistryIndex")

        client.collections.create(
            name="RegistryIndex",
            description="Global reference course registry (Single Tenant / General)",
            multi_tenancy_config=Configure.multi_tenancy(enabled=False), # Force disabled
            properties=[
                Property(name="content", data_type=DataType.TEXT, description="Chapter content"),
                Property(name="keywords_text", data_type=DataType.TEXT, description="Text for vectorization"),
                Property(name="block_id", data_type=DataType.TEXT),
                Property(name="chapter_id", data_type=DataType.TEXT),
                Property(name="class_level", data_type=DataType.TEXT),
                Property(name="subject", data_type=DataType.TEXT),
                Property(name="block_type", data_type=DataType.TEXT),
            ],
            vector_config=Configure.Vectors.text2vec_google_gemini(
                name="keywords_text",
                model="gemini-embedding-2-preview",
            ),
        )
        print("Success: RegistryIndex (Generalist) collection created.")
    except Exception as e:
        print(f"Error during schema creation: {e}")
    finally:
        client.close()

if __name__ == "__main__":
    init_registry_schema()
