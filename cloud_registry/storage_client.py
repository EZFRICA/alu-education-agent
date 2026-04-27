"""
Cloud Registry Storage Client — Handles connection to the central Weaviate instance.
Settings are imported from the centralized config module.
"""

from weaviate import WeaviateClient, connect_to_weaviate_cloud
from weaviate.classes.init import Auth
from cloud_registry.config import settings
from logger import get_logger

logger = get_logger(__name__)

def get_weaviate_client() -> WeaviateClient:
    """Connect to the Weaviate Cloud cluster (Sync)."""
    if not settings.WCD_CLUSTER_URL or not settings.WCD_API_KEY:
        raise ValueError("Missing WCD_CLUSTER_URL or WCD_API_KEY in cloud_registry/config/settings.py")
    
    return connect_to_weaviate_cloud(
        cluster_url=settings.WCD_CLUSTER_URL,
        auth_credentials=Auth.api_key(settings.WCD_API_KEY),
        headers={"X-Goog-Api-Key": settings.GEMINI_API_KEY},
        skip_init_checks=True
    )

def get_weaviate_client_async():
    """Connect to the Weaviate Cloud cluster (Async context manager)."""
    import weaviate
    if not settings.WCD_CLUSTER_URL or not settings.WCD_API_KEY:
        raise ValueError("Missing WCD_CLUSTER_URL or WCD_API_KEY in cloud_registry/config/settings.py")

    return weaviate.use_async_with_weaviate_cloud(
        cluster_url=settings.WCD_CLUSTER_URL,
        auth_credentials=Auth.api_key(settings.WCD_API_KEY),
        headers={"X-Goog-Api-Key": settings.GEMINI_API_KEY},
        skip_init_checks=True
    )
