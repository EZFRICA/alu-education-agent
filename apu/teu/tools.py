"""
APU Tools — Managed by the TEU (Tool Execution Unit).
Contains all external capabilities with built-in caching and monitoring.
"""

import os
from langchain_core.tools import tool
from apu.teu.controller import teu
from config import GEMINI_API_KEY # Or your Search API Key
from logger import get_logger

logger = get_logger(__name__)

# Note: In a real scenario, you would use a search API like Serper, Tavily or Google Custom Search.
# Here we implement a wrapper that the TEU will manage.

async def _raw_search_logic(query: str) -> str:
    """
    The actual implementation of the search.
    This is what the TEU will call and cache.
    """
    # Placeholder: Replace with real API call (e.g. requests.get("https://api.serper.dev/..."))
    logger.info("Performing REAL network search for: %s", query)
    
    # Simulating API latency
    import asyncio
    await asyncio.sleep(1.5)
    
    return f"Search result for '{query}': [Real-time data from TEU about flights and prices in Porto...]"

@tool
async def google_search(query: str) -> str:
    """Search Google for real-time travel information like flight prices, weather, or local events."""
    # We delegate the execution to the TEU Controller
    return await teu.execute_tool(
        tool_name="google_search",
        tool_func=_raw_search_logic,
        query=query
    )
