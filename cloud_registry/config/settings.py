import os
from dotenv import load_dotenv

load_dotenv()

# Gemini Configuration
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# ── Embeddings ────────────────────────────────────────────────────────────────
# Imported from the repo-root embedding_config, the SAME module the client reads.
# This used to be the hardcoded literal "models/gemini-embedding-2", independent
# of the client's setting — so the pipeline kept publishing 3072-dimension
# vectors while the client queried at 384, and nothing detected it.
# Never redeclare the model here.
import sys as _sys
_sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from embedding_config import (  # noqa: E402
    EMBEDDING_MODEL,
    EMBEDDING_DIM,
    EMBEDDING_CACHE_DIR,
    stamp as embedding_stamp,
)

# Weaviate Configuration
WCD_CLUSTER_URL = os.getenv("WCD_CLUSTER_URL")
WCD_API_KEY = os.getenv("WCD_API_KEY")
REGISTRY_COLLECTION = "RegistryIndex"

# Paths
COURSES_DIR = "courses"
OUTPUT_DIR = "../registry"

# GCS Configuration
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
