"""
Shared fixtures for the Akili characterization suite.

Invariants this suite maintains:
  * No API key is required and no INET socket is ever opened.
  * Every test gets its own LanceDB directory and its own metadata_links.json.
    The developer's app_local/storage/ and app_local/memory/ are never touched.
  * Vectors are small, hand-written and deterministic. Nothing is generated and
    no embedding API is called.
"""

import os
import socket
import sys

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

# Make sure nothing picks up a real key from the developer's .env.
#
# These are set to an obviously-fake non-empty string rather than "". Empty is
# what we would prefer, but `import app_local.runtime.agent` calls get_main_llm()
# at module scope (agent.py:21) and ChatGoogleGenerativeAI raises
# pydantic ValidationError "API key required for Gemini Developer API" on an
# empty key -- so an empty key makes the module unimportable and the whole suite
# uncollectable. See tests/test_import_time.py, which pins that behaviour on
# purpose in a subprocess. Sockets are blocked, so this value never leaves the
# process.
_FAKE_KEY = "test-key-not-real-do-not-use"
os.environ["GEMINI_API_KEY"] = _FAKE_KEY
os.environ["GOOGLE_API_KEY"] = _FAKE_KEY
os.environ["OPENROUTER_API_KEY"] = _FAKE_KEY


# ── hand-built deterministic vectors ─────────────────────────────────────────
DIM = 8


def vec(*first_values):
    """An 8-dim vector whose leading components are given, rest zero."""
    v = [0.0] * DIM
    for i, x in enumerate(first_values):
        v[i] = float(x)
    return v


V_A = vec(1.0)                      # unit, axis 0
V_B = vec(0.0, 1.0)                 # unit, axis 1 — orthogonal to V_A
V_A_SCALED = vec(3.0)               # same direction as V_A, 3x magnitude
V_A_OPPOSITE = vec(-1.0)            # antiparallel to V_A


# ── network lockdown ─────────────────────────────────────────────────────────
class NetworkBlocked(Exception):
    """Raised when code under test tries to open an INET socket."""


_real_socket = socket.socket
_real_create_connection = socket.create_connection
_real_getaddrinfo = socket.getaddrinfo


@pytest.fixture
def no_network(monkeypatch):
    """
    Block outbound network.

    Note: this blocks AF_INET/AF_INET6 only, deliberately. `import lancedb`
    builds a background asyncio loop at module scope, which needs
    socket.socketpair() -> AF_UNIX. A blanket socket.socket ban makes the whole
    package unimportable, so the ban is scoped to internet families.
    """
    def guarded(family=socket.AF_INET, *a, **k):
        if family in (socket.AF_INET, socket.AF_INET6):
            raise NetworkBlocked(f"socket.socket(family={family!r})")
        return _real_socket(family, *a, **k)

    def blocked(*a, **k):
        raise NetworkBlocked("outbound connection attempt")

    monkeypatch.setattr(socket, "socket", guarded)
    monkeypatch.setattr(socket, "create_connection", blocked)
    monkeypatch.setattr(socket, "getaddrinfo", blocked)
    return NetworkBlocked


# ── isolated storage ─────────────────────────────────────────────────────────
@pytest.fixture
def akili_paths(tmp_path, monkeypatch):
    """
    Point settings at a per-test temp dir and drop lance_driver's cached
    connection so the next get_db() re-opens against the temp path.
    """
    from app_local.config import settings
    from app_local.storage import lance_driver

    db_path = tmp_path / "akili_db"
    meta_path = tmp_path / "memory" / "metadata_links.json"

    monkeypatch.setattr(settings, "LANCE_DB_PATH", str(db_path))
    monkeypatch.setattr(settings, "METADATA_LINKS_PATH", str(meta_path))
    monkeypatch.setattr(
        settings, "EMBEDDING_STAMP_PATH", str(tmp_path / "embedding_stamp.json")
    )
    # The suite's hand-built vectors are DIM-wide, so the configured embedder
    # for a test is a DIM-wide one. Without this, every storage test would trip
    # the E3 dimension check against the real 384-dim default.
    monkeypatch.setattr(settings, "EMBEDDING_DIM", DIM)
    monkeypatch.setattr(settings, "EMBEDDING_MODEL", "test/stub-embedder")
    monkeypatch.setattr(lance_driver, "_db", None)

    yield {"db": str(db_path), "meta": str(meta_path), "root": tmp_path}

    lance_driver._db = None


@pytest.fixture(autouse=True)
def clean_l1():
    """L1 is a module-level singleton; wipe it between tests."""
    from app_local.mmu import cache_l1
    cache_l1.flush_all()
    yield
    cache_l1.flush_all()


@pytest.fixture
def stub_embeddings(monkeypatch):
    """
    Replace the embedder with a deterministic stub returning V_A.

    Both agent.py and controller.py now go through llm_provider.get_embedder(),
    so the patch lands there. Using a hand-built vector keeps these tests
    deterministic and independent of whether the ONNX model is present on the
    machine running them -- the real model is exercised separately in
    test_local_embedder.py.
    """
    import llm_provider

    calls = []

    class StubEmbedder:
        async def aembed_query(self, text):
            calls.append(("embed", text))
            return list(V_A)

        def embed_query(self, text):
            calls.append(("embed_sync", text))
            return list(V_A)

        def embed_documents(self, texts):
            calls.append(("embed_documents", list(texts)))
            return [list(V_A) for _ in texts]

    stub = StubEmbedder()
    # Patch the CACHED INSTANCE, never the factory function.
    #
    # Patching `llm_provider.get_embedder` is a trap: any module doing
    # `from llm_provider import get_embedder` that is imported for the FIRST
    # time while the patch is active binds the stub permanently, and
    # monkeypatch's teardown restores llm_provider but not that module. A later
    # test then silently talks to a previous test's stub.
    #
    # get_embedder() reads this global on every call, so patching it covers
    # every call site regardless of how the name was imported.
    monkeypatch.setattr(llm_provider, "_embedder", stub)
    return calls


@pytest.fixture
def real_local_embedder(monkeypatch):
    """
    Use the actual ONNX embedder, skipping if the model cache is absent.

    The suite must run on a machine that has not fetched the model, so any test
    needing real vectors opts in through this fixture rather than the whole
    suite depending on a 240MB download.

    Restores the REAL model id and dimension over whatever `akili_paths` set:
    that fixture configures a DIM-wide stub embedder for hand-built vectors, and
    a test using real 384-wide vectors must be configured for 384 or the E3
    dimension check will (correctly) refuse to search.
    """
    import embedding_config
    import llm_provider
    from app_local.config import settings

    monkeypatch.setattr(settings, "EMBEDDING_MODEL", embedding_config.EMBEDDING_MODEL)
    monkeypatch.setattr(settings, "EMBEDDING_DIM", embedding_config.EMBEDDING_DIM)
    monkeypatch.setattr(
        settings, "EMBEDDING_CACHE_DIR", embedding_config.EMBEDDING_CACHE_DIR
    )

    llm_provider.reset_embedder()
    try:
        embedder = llm_provider.get_embedder()
    except RuntimeError as e:
        pytest.skip(f"local embedding model not available: {str(e).splitlines()[0]}")
    yield embedder
    llm_provider.reset_embedder()


# ── DLL builders ─────────────────────────────────────────────────────────────
def make_node(node_id, node_type="temp", is_fixed=False, **extra):
    node = {
        "id": node_id,
        "label": node_id,
        "type": node_type,
        "is_fixed": is_fixed,
        "created_by": "test",
        "keywords": [],
        "active": True,
        "access_count": 0,
        "last_accessed": "2026-01-01T00:00:00",
        "last_modified": "2026-01-01T00:00:00",
        "prev": None,
        "next": None,
    }
    node.update(extra)
    return node


def make_chain(ids, types=None, fixed=()):
    """
    Build a well-formed DLL over `ids` in HEAD->TAIL order.
    `fixed` is the set of ids marked is_fixed.
    """
    types = types or {}
    nodes = {}
    for i, nid in enumerate(ids):
        nodes[nid] = make_node(
            nid,
            node_type=types.get(nid, "temp"),
            is_fixed=nid in fixed,
            prev=ids[i - 1] if i > 0 else None,
            next=ids[i + 1] if i < len(ids) - 1 else None,
        )
    return {
        "agent_id": "agent-test",
        "head_id": ids[0],
        "tail_id": ids[-1],
        "dynamic_block_count": sum(1 for n in ids if n not in fixed),
        "dynamic_block_max": 5,
        "course_selection": {"class": "6eme", "subject": "math"},
        "nodes": nodes,
    }


# ── invariant checker, shared by the DLL tests ───────────────────────────────
def check_dll_invariants(dll):
    """
    Returns a list of violated invariants (empty list == healthy chain).

    I1  HEAD->TAIL traversal terminates and reaches tail_id
    I2  TAIL->HEAD traversal terminates and reaches head_id
    I3  both traversals visit the same SET of ids
    I4  head_id has prev == None
    I5  tail_id has next == None
    I6  no node in `nodes` is orphaned (unreachable from HEAD)
    I7  every prev/next pointer targets an id that exists in `nodes`
    """
    problems = []
    nodes = dll["nodes"]
    head, tail = dll.get("head_id"), dll.get("tail_id")

    def walk(start, link):
        order, seen, cur = [], set(), start
        while cur is not None and cur not in seen:
            if cur not in nodes:
                order.append(cur)
                break
            seen.add(cur)
            order.append(cur)
            cur = nodes[cur].get(link)
        return order

    fwd = walk(head, "next")
    bwd = walk(tail, "prev")

    if not nodes:
        return problems

    if head is not None and fwd and fwd[-1] != tail:
        problems.append(f"I1 HEAD->TAIL ends at {fwd[-1]!r}, tail_id is {tail!r}")
    if tail is not None and bwd and bwd[-1] != head:
        problems.append(f"I2 TAIL->HEAD ends at {bwd[-1]!r}, head_id is {head!r}")
    if set(fwd) != set(bwd):
        problems.append(
            f"I3 forward set {sorted(set(fwd))} != backward set {sorted(set(bwd))}"
        )
    if head is not None and head in nodes and nodes[head].get("prev") is not None:
        problems.append(f"I4 head {head!r} has prev={nodes[head]['prev']!r}")
    if tail is not None and tail in nodes and nodes[tail].get("next") is not None:
        problems.append(f"I5 tail {tail!r} has next={nodes[tail]['next']!r}")
    orphans = set(nodes) - set(fwd)
    if orphans:
        problems.append(f"I6 orphaned nodes unreachable from HEAD: {sorted(orphans)}")
    for nid, n in nodes.items():
        for link in ("prev", "next"):
            tgt = n.get(link)
            if tgt is not None and tgt not in nodes:
                problems.append(f"I7 {nid!r}.{link} -> {tgt!r} which does not exist")

    return problems
