"""
E3 — the guard against a silent vector-space mismatch.

Target: app_local/storage/lance_driver.py (verify_embedding_space, read/write_stamp)
        embedding_config.py (single source of truth)

A mismatch does not fail on its own: it returns confidently ranked nonsense.
These tests assert it becomes a readable refusal instead.
"""

import json

import pytest

from app_local.config import settings
from app_local.storage import lance_driver
from conftest import V_A, DIM


def _row(rid, vector, content="c"):
    return {
        "id": rid, "chapter": rid, "content": content, "block_type": "cours",
        "class_level": "6eme", "subject": "math", "vector": list(vector),
        "updated_at": "2026-01-01T00:00:00",
    }


# ── the dimension check ──────────────────────────────────────────────────────

async def test_a_wider_registry_is_refused_not_searched(akili_paths, monkeypatch):
    """
    The live failure: the published registry held 3072-dim Gemini vectors while
    the client was configured for 384. Searching that returns noise.
    """
    db = lance_driver.get_db()
    db.create_table("edu_registry", data=[_row("ch1", [0.1] * 16)])
    monkeypatch.setattr(settings, "EMBEDDING_DIM", 8)

    with pytest.raises(lance_driver.EmbeddingMismatch) as exc:
        await lance_driver.search_block_index([0.1] * 8, class_level="6eme",
                                              subject="math")

    msg = str(exc.value)
    assert "16-dimension" in msg
    assert "8" in msg
    assert "migrate_embeddings.py" in msg


async def test_the_refusal_names_the_configured_model(akili_paths, monkeypatch):
    """An operator must be able to read which model the client expects."""
    db = lance_driver.get_db()
    db.create_table("user_memory", data=[_row("a", [0.1] * 16)])
    monkeypatch.setattr(settings, "EMBEDDING_MODEL", "acme/tiny-embed")

    with pytest.raises(lance_driver.EmbeddingMismatch) as exc:
        await lance_driver.search_block_index(list(V_A))

    assert "acme/tiny-embed" in str(exc.value)


async def test_a_matching_dimension_searches_normally(akili_paths):
    db = lance_driver.get_db()
    db.create_table("edu_registry", data=[_row("ch1", V_A)])

    results = await lance_driver.search_block_index(
        list(V_A), class_level="6eme", subject="math"
    )
    assert [r["block_id"] for r in results] == ["ch1"]


# ── the model check ──────────────────────────────────────────────────────────

async def test_a_different_model_at_the_same_dimension_is_refused(
    akili_paths, monkeypatch
):
    """
    Dimension alone is not enough: two models can share a width and still be
    different vector spaces. That is the case the sidecar exists for.
    """
    await lance_driver.upsert_local_block(
        block_id="student_profile", content="Marc", block_type="fondamental",
        class_level="6eme", subject="math", vector=list(V_A),
    )
    assert lance_driver.read_stamp("user_memory")["model"] == "test/stub-embedder"

    monkeypatch.setattr(settings, "EMBEDDING_MODEL", "other/same-width-model")

    with pytest.raises(lance_driver.EmbeddingMismatch) as exc:
        await lance_driver.search_block_index(list(V_A))

    msg = str(exc.value)
    assert "test/stub-embedder" in msg
    assert "other/same-width-model" in msg
    assert "Same dimension does not mean the same vector space" in msg


# ── the stamp itself ─────────────────────────────────────────────────────────

async def test_writing_a_block_records_the_stamp(akili_paths):
    assert lance_driver.read_stamp("user_memory") is None

    await lance_driver.upsert_local_block(
        block_id="a", content="x", block_type="temp",
        class_level="6eme", subject="math", vector=list(V_A),
    )

    stamp = lance_driver.read_stamp("user_memory")
    assert stamp["model"] == "test/stub-embedder"
    assert stamp["dim"] == DIM
    assert "updated_at" in stamp


def test_a_corrupt_stamp_is_treated_as_absent(akili_paths):
    """A truncated sidecar must not crash the read path on every turn."""
    with open(settings.EMBEDDING_STAMP_PATH, "w") as f:
        f.write("{not json")
    assert lance_driver.read_stamp("user_memory") is None


async def test_no_stamp_and_no_tables_is_not_an_error(akili_paths):
    """A fresh install has nothing to verify."""
    assert await lance_driver.search_block_index(list(V_A)) == []


# ── the single source of truth ───────────────────────────────────────────────

def test_client_and_pipeline_read_the_same_embedding_config():
    """
    The root cause of the dim=3072 publication: cloud_registry declared its own
    EMBEDDING_MODEL literal, independent of the client's setting.
    """
    import embedding_config
    from app_local.config import settings as local_settings
    from cloud_registry.config import settings as cloud_settings

    assert local_settings.EMBEDDING_MODEL is embedding_config.EMBEDDING_MODEL
    assert cloud_settings.EMBEDDING_MODEL is embedding_config.EMBEDDING_MODEL
    assert cloud_settings.EMBEDDING_DIM == embedding_config.EMBEDDING_DIM


def test_cloud_settings_does_not_redeclare_the_model():
    """Guards against someone reintroducing a literal there."""
    import pathlib
    from test_import_time import REPO_ROOT

    import re

    text = pathlib.Path(REPO_ROOT, "cloud_registry/config/settings.py").read_text()
    # A literal assignment is the defect; mentioning the old value in a comment
    # explaining why is not.
    literal = re.search(r"^EMBEDDING_(MODEL|DIM)\s*=\s*['\"0-9]", text, re.M)
    assert literal is None, f"cloud settings redeclares the model: {literal.group(0)!r}"
    assert "from embedding_config import" in text


# ── R3: stamps are per table, not per store ──────────────────────────────────

async def test_tables_are_stamped_independently(akili_paths):
    """
    edu_registry comes from the cloud, user_memory is written on-device. They
    can legitimately end up in different vector spaces -- re-embedding local
    memory without re-downloading courses is the realistic case -- and a single
    store-wide stamp reported the configured model for both, hiding it.
    """
    await lance_driver.upsert_local_block(
        block_id="a", content="x", block_type="temp",
        class_level="6eme", subject="math", vector=list(V_A),
    )
    assert lance_driver.read_stamp("user_memory") is not None
    assert lance_driver.read_stamp("edu_registry") is None


async def test_a_stale_registry_is_caught_at_the_same_dimension(
    akili_paths, monkeypatch
):
    """
    The exact gap R3 describes: same width, different model, registry never
    re-downloaded. The dimension check cannot see this one.
    """
    db = lance_driver.get_db()
    db.create_table("edu_registry", data=[_row("ch1", V_A)])
    lance_driver.write_stamp("edu_registry")

    # local memory re-embedded with a new model of the same width...
    monkeypatch.setattr(settings, "EMBEDDING_MODEL", "new/same-width")
    await lance_driver.upsert_local_block(
        block_id="a", content="x", block_type="temp",
        class_level="6eme", subject="math", vector=list(V_A),
    )
    # ...user_memory now current, edu_registry stale
    assert lance_driver.read_stamp("user_memory")["model"] == "new/same-width"
    assert lance_driver.read_stamp("edu_registry")["model"] == "test/stub-embedder"

    with pytest.raises(lance_driver.EmbeddingMismatch) as exc:
        await lance_driver.search_block_index(list(V_A), class_level="6eme",
                                              subject="math")
    assert "edu_registry" in str(exc.value)


def test_a_legacy_flat_stamp_is_honoured(akili_paths):
    """A sidecar written before R3 applied to the whole store; don't discard it."""
    with open(settings.EMBEDDING_STAMP_PATH, "w") as f:
        json.dump({"model": "old/model", "dim": 8}, f)

    assert lance_driver.read_stamp("user_memory")["model"] == "old/model"
    assert lance_driver.read_stamp("edu_registry")["model"] == "old/model"
