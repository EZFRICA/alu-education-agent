"""
llm_provider.py — LLM Abstraction Layer
========================================
Inference is the only pluggable component in Akili, and the only one that may
need the network. Embeddings and retrieval are always local. Swapping the
backend below is a configuration change and nothing else — which is what makes
a 100% offline deployment reachable without touching any code.

LLM_PROVIDER:
  - gemma      : gemma-4-26b-a4b-it via Google AI Studio (default)
  - gemini     : gemini-flash-lite-latest via Google AI Studio
  - openrouter : any OpenRouter-hosted model
  - ollama     : an Ollama host — LOCAL when OLLAMA_BASE_URL is localhost,
                 remote otherwise. `ollama` does not by itself mean offline.

Usage:
    from llm_provider import get_main_llm, get_extractor_llm, get_embedder
"""

import asyncio
import os
import threading
from typing import List

from dotenv import load_dotenv
from langchain_ollama import ChatOllama
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_openai import ChatOpenAI
from logger import get_logger

# Must run BEFORE the os.getenv calls below. Without it, which provider you get
# depended on import order: a caller that imported app_local.config.settings
# first (which calls load_dotenv) saw .env, while a script importing
# llm_provider directly silently fell back to the hardcoded defaults. The same
# .env produced two different models depending on the entry point.
load_dotenv()

# Fallback values for configuration
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "gemma")
KNOWN_LLM_PROVIDERS = ("gemma", "gemini", "openrouter", "ollama")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "gemma:2b")
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
GEMMA_MODEL = os.getenv("GEMMA_MODEL", "gemma-4-26b-a4b-it")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-flash-lite-latest")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "google/gemma-4-26b-a4b-it:free")
OPENROUTER_BASE_URL = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")

logger = get_logger(__name__)

# DO NOT try to validate this by shape.
#
# Google AI Studio issues keys in at least two formats: the classic "AIza…"
# (~39 chars) and "AQ.…" (~53 chars). Both were verified working against
# generativelanguage.googleapis.com. A short-lived OAuth access token also looks
# like "AQ.…" and is NOT usable here — so format cannot distinguish a valid key
# from an expired token, and any prefix/length rule produces false verdicts in
# one direction or the other. Both were tried here and both were wrong.
#
# The API is the only authority. Check presence, and let the 401 speak.
_GEMINI_KEY_VALID = bool(GEMINI_API_KEY)

if not _GEMINI_KEY_VALID and LLM_PROVIDER in ("gemma", "gemini"):
    logger.warning(
        "LLM_PROVIDER=%s needs GEMINI_API_KEY, which is not set. Requests will "
        "fail with 401. Get a key at https://aistudio.google.com/app/apikey — "
        "note that an OAuth access token is not a key and expires within the "
        "hour, even though it looks similar.",
        LLM_PROVIDER,
    )


def _unknown_provider(provider: str) -> ValueError:
    """
    An unrecognised provider used to fall through to Gemma silently, so a typo
    in a .env ('gemmma') sent every student message to a backend nobody chose,
    with no message anywhere. On an unattended classroom machine that is
    undiagnosable — same reasoning as list_table_names in lance_driver.
    """
    return ValueError(
        f"Unknown LLM_PROVIDER={provider!r}. "
        f"Expected one of: {', '.join(KNOWN_LLM_PROVIDERS)}."
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
    elif LLM_PROVIDER == "openrouter":
        logger.info("LLM Provider: OpenRouter — model=%s", OPENROUTER_MODEL)
        return ChatOpenAI(
            model_name=OPENROUTER_MODEL,
            openai_api_base=OPENROUTER_BASE_URL,
            openai_api_key=OPENROUTER_API_KEY,
            temperature=0.7,
        )
    elif LLM_PROVIDER == "gemini":
        logger.info("LLM Provider: Gemini — model=%s", GEMINI_MODEL)
        return ChatGoogleGenerativeAI(
            model=GEMINI_MODEL,
            temperature=0.7,
            google_api_key=GEMINI_API_KEY,
        )
    elif LLM_PROVIDER == "gemma":
        logger.info("LLM Provider: Gemma — model=%s", GEMMA_MODEL)
        return ChatGoogleGenerativeAI(
            model=GEMMA_MODEL,
            temperature=0.7,
            google_api_key=GEMINI_API_KEY,
        )
    else:
        raise _unknown_provider(LLM_PROVIDER)


def get_extractor_llm():
    """
    Returns the deterministic memory-extraction LLM (zero temperature).
    Uses Gemini when a valid API key is available (faster, better at JSON).
    Falls back to Ollama with JSON mode enabled when no valid Gemini key exists.
    """
    if LLM_PROVIDER == "gemini":
        logger.debug("Extractor LLM: Gemini — model=%s (deterministic)", GEMINI_MODEL)
        return ChatGoogleGenerativeAI(
            model=GEMINI_MODEL,
            temperature=0.0,
            google_api_key=GEMINI_API_KEY,
            # The output is parsed as strict JSON. The OpenRouter and Ollama
            # branches already ask for JSON; this one did not, and it is the
            # branch most likely to answer with prose around the object.
            response_mime_type="application/json",
        )
    elif LLM_PROVIDER == "openrouter":
        logger.debug("Extractor LLM: OpenRouter — model=%s (deterministic)", OPENROUTER_MODEL)
        return ChatOpenAI(
            model_name=OPENROUTER_MODEL,
            openai_api_base=OPENROUTER_BASE_URL,
            openai_api_key=OPENROUTER_API_KEY,
            temperature=0.0,
            model_kwargs={"response_format": {"type": "json_object"}},
        )
    elif LLM_PROVIDER == "ollama":
        logger.info("Extractor LLM: Ollama (local fallback) — model=%s", OLLAMA_MODEL)
        return ChatOllama(
            model=OLLAMA_MODEL,
            base_url=OLLAMA_BASE_URL,
            temperature=0.0,
            num_predict=1024,
            format="json",
        )
    elif LLM_PROVIDER == "gemma":
        logger.info("Extractor LLM: Gemma — model=%s", GEMMA_MODEL)
        return ChatGoogleGenerativeAI(
            model=GEMMA_MODEL,
            temperature=0.0,
            google_api_key=GEMINI_API_KEY,
            response_mime_type="application/json",
        )
    else:
        raise _unknown_provider(LLM_PROVIDER)


# ═════════════════════════════════════════════════════════════════════════════
# Embeddings
# ═════════════════════════════════════════════════════════════════════════════
# Before this existed, GoogleGenerativeAIEmbeddings was constructed inline at
# agent.py:95 and controller.py:268 (REVIEW_FINDINGS.md C6), so the read path
# made a network round trip to embed the query no matter what LLM_PROVIDER said,
# and controller.py rebuilt the client on every single write-back.

_embedder = None
_embedder_lock = threading.Lock()


class LocalOnnxEmbedder:
    """
    ONNX embedder via fastembed. No torch, no sentence-transformers — both are
    disqualifying on the target hardware.

    Resolves the model from a local cache directory and never downloads. On a
    classroom machine with no connectivity a download attempt would hang at the
    first student question; failing immediately with an actionable message is
    the only useful behaviour.
    """

    def __init__(self, model_name: str, cache_dir: str, expected_dim: int,
                 allow_download: bool = False):
        self.model_name = model_name
        self.cache_dir = cache_dir
        self.expected_dim = expected_dim

        try:
            from fastembed import TextEmbedding
        except ImportError as e:  # pragma: no cover - install-time failure
            raise RuntimeError(
                "fastembed is not installed. Run `uv sync`. "
                "Do not install sentence-transformers or torch as a substitute — "
                "they are disqualifying on the target hardware."
            ) from e

        if allow_download:
            # Only the cloud pipeline and scripts/fetch_embedding_model.py take
            # this path: they run on a connected machine by definition.
            os.makedirs(cache_dir, exist_ok=True)
            self._model = TextEmbedding(model_name=model_name, cache_dir=cache_dir)
        else:
            if not os.path.isdir(cache_dir):
                raise RuntimeError(_missing_model_message(model_name, cache_dir))
            try:
                self._model = TextEmbedding(
                    model_name=model_name,
                    cache_dir=cache_dir,
                    local_files_only=True,
                )
            except Exception as e:
                raise RuntimeError(
                    _missing_model_message(model_name, cache_dir)
                ) from e

        logger.info(
            "Embedder: local ONNX — model=%s dim=%d cache=%s",
            model_name, expected_dim, cache_dir,
        )

    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        return [list(map(float, v)) for v in self._model.embed(list(texts))]

    def embed_query(self, text: str) -> List[float]:
        return self.embed_documents([text])[0]

    async def aembed_query(self, text: str) -> List[float]:
        # fastembed is synchronous CPU work; keep it off the event loop.
        return await asyncio.to_thread(self.embed_query, text)

    async def aembed_documents(self, texts: List[str]) -> List[List[float]]:
        return await asyncio.to_thread(self.embed_documents, texts)


def _missing_model_message(model_name: str, cache_dir: str) -> str:
    return (
        f"Embedding model '{model_name}' is not present in {cache_dir}.\n"
        f"The local embedder never downloads at runtime, because on a machine "
        f"with no connectivity that hangs instead of failing.\n"
        f"Fix: on a CONNECTED machine run\n"
        f"    uv run python scripts/fetch_embedding_model.py\n"
        f"then copy '{cache_dir}' to this device.\n"
        f"Alternatively set EMBEDDING_PROVIDER=google to use the remote embedder "
        f"(requires a network connection and an API key on every query)."
    )


def get_embedder():
    """
    Return the process-wide embeddings client.

    Cached: the local backend loads ~220MB of ONNX weights, which must happen
    once per process, not once per write-back.
    """
    global _embedder
    if _embedder is not None:
        return _embedder

    with _embedder_lock:
        if _embedder is None:
            _embedder = build_embedder()
    return _embedder


def build_embedder(allow_download: bool = False):
    """
    Uncached embedder factory.

    `allow_download=True` is for the cloud pipeline and the fetch script, which
    run on a connected machine. The client must never use it: a download attempt
    on a disconnected classroom device hangs instead of failing.
    """
    from app_local.config import settings

    provider = (settings.EMBEDDING_PROVIDER or "local").lower()

    if provider == "google":
        from langchain_google_genai import GoogleGenerativeAIEmbeddings
        logger.info("Embedder: Google (remote) — model=%s", settings.EMBEDDING_MODEL)
        return GoogleGenerativeAIEmbeddings(
            model=settings.EMBEDDING_MODEL,
            google_api_key=GEMINI_API_KEY,
        )

    if provider != "local":
        raise ValueError(
            f"Unknown EMBEDDING_PROVIDER={provider!r}. Expected 'local' or 'google'."
        )

    return LocalOnnxEmbedder(
        model_name=settings.EMBEDDING_MODEL,
        cache_dir=settings.EMBEDDING_CACHE_DIR,
        expected_dim=settings.EMBEDDING_DIM,
        allow_download=allow_download,
    )


def reset_embedder() -> None:
    """Drop the cached embedder. For tests and for config changes at runtime."""
    global _embedder
    with _embedder_lock:
        _embedder = None
