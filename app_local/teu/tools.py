import os
from typing import Optional
from langchain_core.tools import tool

# Imports locaux
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from app_local.storage import lance_driver
from logger import get_logger

logger = get_logger(__name__)

@tool
async def load_course_chapter(chapter_id: str) -> str:
    """
    Charge le contenu complet d'un chapitre de cours depuis la base locale LanceDB.
    Utilisez cet outil pour obtenir des détails précis sur un sujet.
    """
    logger.info(f"Outil: Chargement du chapitre '{chapter_id}'")
    content = await lance_driver.get_block_content(chapter_id)
    if not content:
        return f"Erreur : Le chapitre '{chapter_id}' est introuvable localement."
    return content

@tool
async def google_search(query: str) -> str:
    """
    Search Google for real-time educational information if local knowledge is insufficient.
    Requires an active internet connection and Gemini API key.
    """
    logger.info(f"TEU: Dispatching grounded search for query: '{query}'")
    
    from google.genai import Client as GeminiClient
    from google.genai.types import Tool, GenerateContentConfig, GoogleSearch
    from llm_provider import GEMINI_API_KEY

    try:
        # Offload the blocking SDK call to a thread
        def _sync_search():
            client = GeminiClient(api_key=GEMINI_API_KEY)
            response = client.models.generate_content(
                model="gemini-flash-lite-latest",
                contents=query,
                config=GenerateContentConfig(
                    tools=[Tool(google_search=GoogleSearch())],
                    response_modalities=["TEXT"],
                ),
            )
            return response.text or "No results found."

        result = await asyncio.to_thread(_sync_search)
        logger.info("TEU: Search completed.")
        return result
    except Exception as e:
        logger.error(f"TEU: Search failed: {e}")
        return f"Error performing search: {e}"
