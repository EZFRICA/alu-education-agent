"""
llm_provider.py — LLM Abstraction Layer
========================================
Routes between:
  - CLOUD  : Gemini via Google API (default — provider principal)
  - LOCAL  : Gemma 4 via Ollama  (fallback si ressources insuffisantes)

Usage:
    from llm_provider import get_main_llm, get_extractor_llm
"""

import os
from langchain_ollama import ChatOllama
from langchain_google_genai import ChatGoogleGenerativeAI
from logger import get_logger

# Fallback values for configuration
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "gemini")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "gemma:2b")
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemma-4-26b-a4b-it")

logger = get_logger(__name__)

# A valid Google AI Studio key is always 39+ chars and starts with "AIza"
_GEMINI_KEY_VALID = bool(
    GEMINI_API_KEY
    and len(GEMINI_API_KEY) > 20
    and GEMINI_API_KEY.startswith("AIza")
)

if not _GEMINI_KEY_VALID and GEMINI_API_KEY:
    logger.warning(
        "GEMINI_API_KEY is set but looks invalid — falling back to Ollama for extraction. "
        "Set a valid key (starts with 'AIza') to enable Gemini-powered memory extraction."
    )


def get_main_llm():
    """
    Returns the main conversational LLM.
    - CLOUD  → ChatGoogleGenerativeAI (Gemini flash-lite, default)
    - LOCAL  → ChatOllama (Gemma 4, fallback)
    """
    if LLM_PROVIDER == "ollama":
        logger.info("LLM Provider: Ollama — model=%s", OLLAMA_MODEL)
        return ChatOllama(
            model=OLLAMA_MODEL,
            base_url=OLLAMA_BASE_URL,
            temperature=0.7,
            num_predict=2048,
        )
    else:
        logger.info("LLM Provider: Gemini — model=%s", GEMINI_MODEL)
        return ChatGoogleGenerativeAI(
            model=GEMINI_MODEL,
            temperature=0.7,
            google_api_key=GEMINI_API_KEY,
        )


def get_extractor_llm():
    """
    Returns the deterministic memory-extraction LLM (temperature=0).
    Uses Gemini when a valid API key is available (faster, better at JSON).
    Falls back to Ollama with JSON mode enabled when no valid Gemini key exists.
    """
    if _GEMINI_KEY_VALID:
        logger.debug("Extractor LLM: Gemini — model=%s (deterministic)", GEMINI_MODEL)
        return ChatGoogleGenerativeAI(
            model=GEMINI_MODEL,
            temperature=0.7,
            google_api_key=GEMINI_API_KEY,
        )
    else:
        logger.info("Extractor LLM: Ollama (local fallback) — model=%s", OLLAMA_MODEL)
        return ChatOllama(
            model=OLLAMA_MODEL,
            base_url=OLLAMA_BASE_URL,
            temperature=0.7,
            num_predict=1024,
            format="json",
        )
