import os
from dotenv import load_dotenv

load_dotenv()

# API Keys
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# Registry Sync
MANIFEST_URL = "https://storage.googleapis.com/akili-registry/manifest.json"

# Local Storage
LANCE_DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "storage", "akili_db")
CACHE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "storage", "cache")

# DLL Memory
METADATA_LINKS_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "memory", "metadata_links.json")

# ── Embeddings ────────────────────────────────────────────────────────────────
# Re-exported from the repo-root embedding_config, which the cloud pipeline
# reads too. Do not redeclare these here: the client and the pipeline having
# separate definitions is exactly what let the registry be published in one
# vector space while the client queried in another.
import sys as _sys
_sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from embedding_config import (  # noqa: E402
    EMBEDDING_PROVIDER,
    EMBEDDING_MODEL,
    EMBEDDING_DIM,
    EMBEDDING_CACHE_DIR,
)

# Sidecar recording which embedder wrote the local store, next to local_manifest.json.
EMBEDDING_STAMP_PATH = os.getenv(
    "EMBEDDING_STAMP_PATH",
    os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "storage", "embedding_stamp.json",
    ),
)

# Tool Execution Unit — all tools are local (see app_local/teu/tools.py).
# Set false when the configured model does not support function calling; the
# tutor then answers without tools instead of erroring.
TEU_ENABLED = os.getenv("TEU_ENABLED", "true").lower() not in ("0", "false", "no")

# ── Retrieval thresholds ──────────────────────────────────────────────────────
# Minimum cosine similarity for a block to enter the working context.
#
# THESE ARE CALIBRATED TO THE EMBEDDING MODEL and must be re-measured if it
# changes. Measured with paraphrase-multilingual-MiniLM-L12-v2 on real course
# content, query "Explique-moi les fractions":
#
#   chapter_1_simple_fractions      0.670   <- the right chapter
#   chapter_2_decimal_numbers       0.507   <- same subject, related
#   chapter_3_angles_and_geometry   0.284   <- unrelated
#
# The previous values (0.70/0.75/0.80) rejected ALL of them, including the exact
# match, so no course content ever reached the prompt even after retrieval was
# repaired. Ranking was never the problem; the cut-off was.
#
# Ordering is preserved from the original design: fondamental (always relevant)
# < cours < temp (most recent context, most selective).
CERTAINTY_THRESHOLDS = {
    "fondamental":    0.40,   # student_profile / learning_preferences
    "cours":          0.45,   # active_course
    "manual_chapter": 0.45,   # what every shipped course row actually carries
    "temp":           0.50,   # current_session
}
MIN_RELEVANCE_CERTAINTY = 0.45

# DLL Configuration
MAX_DYNAMIC_BLOCKS = 5
EDU_DEFAULT_CLASS  = "6eme"
EDU_DEFAULT_SUBJECT = "math"

# GCS Credentials for Sync
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
