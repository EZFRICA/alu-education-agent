"""
embedding_config.py — single source of truth for the embedding model.
=====================================================================

Query vectors and document vectors MUST come from the same model. Before this
module existed the constraint was unenforceable by construction: the client read
`app_local/config/settings.py` while the cloud pipeline read a hardcoded literal
in `cloud_registry/config/settings.py`. Setting EMBEDDING_PROVIDER=local changed
the client and left the pipeline publishing 3072-dimension Gemini vectors, with
nothing anywhere reporting the mismatch.

Both sides now import from here. `app_local.config.settings` and
`cloud_registry.config.settings` re-export these names, so existing call sites
keep working while there is only one place to change them.

The model is a DEPLOYMENT PARAMETER, not an architectural constant. The default
targets constrained hardware: 384 dimensions, multilingual, ONNX via fastembed,
no torch. Changing it requires regenerating the cloud registry AND re-running
scripts/migrate_embeddings.py on every device. See README.
"""

import math
import os
from typing import List, Sequence

from dotenv import load_dotenv

load_dotenv()

_REPO_ROOT = os.path.dirname(os.path.abspath(__file__))

# local | google
EMBEDDING_PROVIDER = os.getenv("EMBEDDING_PROVIDER", "local")

EMBEDDING_MODEL = os.getenv(
    "EMBEDDING_MODEL", "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
)

EMBEDDING_DIM = int(os.getenv("EMBEDDING_DIM", "384"))

# Where the ONNX model files live on disk. Bundled with the deployment; the
# client never downloads at runtime (see llm_provider.LocalOnnxEmbedder).
EMBEDDING_CACHE_DIR = os.getenv(
    "EMBEDDING_CACHE_DIR", os.path.join(_REPO_ROOT, "models")
)


def normalize_vector(vector: Sequence[float]) -> List[float]:
    """
    Scale a vector to unit L2 length.

    Part of the embedding contract, not a storage detail, which is why it lives
    here: the certainty scale is `1 - distance/2`, and with LanceDB's squared-L2
    metric that equals cosine similarity only for unit vectors. Both sides must
    normalise or the scale is meaningless — the cloud pipeline normalises before
    writing a parquet, and the client normalises queries and local writes.

    A zero vector has no direction and is returned unchanged rather than raising.
    """
    norm = math.sqrt(sum(float(x) * float(x) for x in vector))
    if norm == 0.0:
        return [float(x) for x in vector]
    return [float(x) / norm for x in vector]


def stamp() -> dict:
    """
    The identity of the configured embedder, as recorded in the registry
    manifest and in the local sidecar. Comparing these is what turns a silent
    vector-space mismatch into a readable refusal.
    """
    return {"model": EMBEDDING_MODEL, "dim": EMBEDDING_DIM}
