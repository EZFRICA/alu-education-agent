"""
R1/R2 — the download path: table detection and the embedding stamp.

Target: app_local/sync/sync_manager.py:download_course
Network is never touched; the GCS client and manifest fetch are stubbed.
"""

import json
import pathlib

import pytest

from app_local.config import settings
from app_local.storage import lance_driver
from app_local.sync import sync_manager
from conftest import V_A, DIM


def _manifest(model="test/stub-embedder", dim=DIM):
    m = {"files": [{"id": "6eme_math", "class": "6eme", "subject": "math",
                    "filename": "6eme_math_v1.parquet", "hash": "sha256:x"}]}
    if model is not None:
        m["embedding"] = {"model": model, "dim": dim}
    return m


def _stub_download(monkeypatch, akili_paths, manifest, rows=None):
    import pandas as pd

    async def fake_manifest(blob):
        return manifest

    monkeypatch.setattr(sync_manager, "_fetch_remote_json", fake_manifest)
    monkeypatch.setattr(
        sync_manager, "LOCAL_MANIFEST_PATH",
        str(pathlib.Path(akili_paths["root"]) / "local_manifest.json"),
    )

    rows = rows if rows is not None else [{
        "id": "ch1", "chapter": "ch1", "class_level": "6eme", "subject": "math",
        "block_type": "manual_chapter", "content": "Les fractions.",
        "keywords": "k", "vector": list(V_A),
    }]

    class _Blob:
        def download_to_filename(self, path):
            pd.DataFrame(rows).to_parquet(path, index=False)

    class _Bucket:
        def blob(self, name): return _Blob()

    class _Client:
        def bucket(self, name): return _Bucket()

    async def fake_client():
        return _Client()

    monkeypatch.setattr(sync_manager, "_get_storage_client", fake_client)


async def test_a_download_stamps_the_registry(akili_paths, monkeypatch):
    """R2: nothing wrote edu_registry's stamp before; only memory writes did."""
    _stub_download(monkeypatch, akili_paths, _manifest())
    assert lance_driver.read_stamp("edu_registry") is None

    ok, msg = await sync_manager.download_course("6eme", "math")
    assert ok, msg

    stamp = lance_driver.read_stamp("edu_registry")
    assert stamp["model"] == "test/stub-embedder"
    assert stamp["dim"] == DIM


async def test_a_registry_built_with_another_model_is_refused_before_download(
    akili_paths, monkeypatch
):
    """R2: the manifest carries the stamp, so refuse now, not at first question."""
    _stub_download(monkeypatch, akili_paths,
                   _manifest(model="legacy/remote-embedder", dim=3072))

    ok, msg = await sync_manager.download_course("6eme", "math")
    assert ok is False
    assert "legacy/remote-embedder" in msg
    assert "3072" in msg
    assert "test/stub-embedder" in msg
    assert "edu_registry" not in lance_driver.list_table_names()


async def test_an_unstamped_manifest_still_imports(akili_paths, monkeypatch, capsys):
    """A registry predating stamping must not become undownloadable."""
    _stub_download(monkeypatch, akili_paths, _manifest(model=None))

    ok, msg = await sync_manager.download_course("6eme", "math")
    assert ok, msg
    assert "no embedding stamp" in capsys.readouterr().out


async def test_a_second_download_replaces_rather_than_duplicating(
    akili_paths, monkeypatch
):
    """
    R1: `"edu_registry" not in db.list_tables()` was always True, so the replace
    branch was dead and every download went through the exception fallback.
    """
    _stub_download(monkeypatch, akili_paths, _manifest())
    assert (await sync_manager.download_course("6eme", "math"))[0]
    assert (await sync_manager.download_course("6eme", "math"))[0]

    df = lance_driver.get_db().open_table("edu_registry").to_pandas()
    assert len(df) == 1, f"duplicated rows: {len(df)}"


def test_sync_manager_does_not_call_list_tables_directly():
    """The 5th site missed in Batch A. Guards against a 6th."""
    import ast
    from test_import_time import REPO_ROOT

    src = pathlib.Path(REPO_ROOT, "app_local/sync/sync_manager.py").read_text()
    for node in ast.walk(ast.parse(src)):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            assert node.func.attr != "list_tables", (
                "use lance_driver.list_table_names(); db.list_tables() returns a "
                "response model whose __contains__ never matches (S5-A)"
            )
