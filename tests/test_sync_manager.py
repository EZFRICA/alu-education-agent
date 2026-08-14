"""
Sync manager control flow. No GCS, no HTTP -- the fetchers are stubbed.

Target: app_local/sync/sync_manager.py:217-259 (sync_with_registry)
"""

import pytest

from app_local.sync import sync_manager


@pytest.fixture
def stub_fetch(monkeypatch):
    """Replace both fetchers; returns a dict you can mutate per test."""
    state = {"manifest": None, "blob": None}

    async def fake_fetch_json(url):
        return state["manifest"]

    async def fake_fetch_remote_json(blob_name):
        return state["blob"]

    monkeypatch.setattr(sync_manager, "_fetch_json", fake_fetch_json)
    monkeypatch.setattr(sync_manager, "_fetch_remote_json", fake_fetch_remote_json)
    return state


async def test_unreachable_registry_reports_it(stub_fetch, no_network):
    """
    Was `..._returns_cleanly`, asserting None. sync_with_registry now returns
    (ok, message) so the UI can say what happened instead of silently doing
    nothing.
    """
    stub_fetch["blob"] = None
    ok, msg = await sync_manager.sync_with_registry()
    assert ok is False
    assert "Unable to reach" in msg


async def test_a_manifest_without_prompts_is_not_an_error(stub_fetch, no_network):
    """
    Was `test_manifest_without_a_prompts_key_raises_unboundlocalerror`.
    `updated` was assigned only two `if`s deep and read unconditionally, so
    every path that skipped the assignment raised -- and the dashboard's
    "Check for Updates" button was not wrapped.
    """
    stub_fetch["blob"] = {"files": []}
    ok, msg = await sync_manager.sync_with_registry()
    assert ok is True
    assert "nothing to update" in msg


async def test_a_failed_prompt_download_is_reported(
    stub_fetch, no_network, akili_paths, monkeypatch
):
    """Was `test_prompts_present_but_download_fails_also_raises`."""
    calls = {"n": 0}

    async def fetch(blob_name):
        calls["n"] += 1
        return {"prompts": {"hash": "sha256:deadbeef"}} if calls["n"] == 1 else None

    monkeypatch.setattr(sync_manager, "_fetch_remote_json", fetch)
    ok, msg = await sync_manager.sync_with_registry()
    assert ok is False
    assert "Could not download" in msg


async def test_prompts_are_written_when_they_change(
    stub_fetch, no_network, akili_paths, monkeypatch
):
    """Was `test_the_only_path_that_does_not_raise`."""
    import json, os

    payload = {"system_tutor": "You are Akili.", "6eme": "Be gentle."}
    calls = {"n": 0}

    async def fetch(blob_name):
        calls["n"] += 1
        return {"prompts": {"hash": "sha256:deadbeef"}} if calls["n"] == 1 else payload

    monkeypatch.setattr(sync_manager, "_fetch_remote_json", fetch)
    ok, msg = await sync_manager.sync_with_registry()
    assert ok is True, msg

    path = os.path.join(
        os.path.dirname(sync_manager.settings.LANCE_DB_PATH), "prompts.json"
    )
    with open(path) as f:
        assert json.load(f) == payload


async def test_an_unchanged_hash_skips_the_download(
    stub_fetch, no_network, akili_paths, monkeypatch
):
    """
    Was `test_local_manifest_hash_match_skips_the_update_and_raises`: the steady
    state -- everything already current -- was the path that raised on every
    press of the button after the first.
    """
    import json, os

    path = os.path.join(
        os.path.dirname(sync_manager.settings.LANCE_DB_PATH), "prompts.json"
    )
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump({"system_tutor": "x"}, f)

    local_hash = sync_manager.get_file_hash(path)
    stub_fetch["blob"] = {"prompts": {"hash": local_hash}}

    ok, msg = await sync_manager.sync_with_registry()
    assert ok is True
    assert "up to date" in msg


def test_sync_with_registry_has_no_duplicate_of_download_prompts():
    """
    The inline re-implementation is gone: download_prompts was already correct
    and was never called. Two copies of the same logic is how one of them rotted.
    """
    import ast, inspect, textwrap

    tree = ast.parse(textwrap.dedent(inspect.getsource(sync_manager.sync_with_registry)))
    assigned = {
        t.id for node in ast.walk(tree) if isinstance(node, ast.Assign)
        for t in node.targets if isinstance(t, ast.Name)
    }
    # The flag that was read before being assigned. Mentioning it in a comment
    # explaining the history is not the defect; assigning it again would be.
    assert "updated" not in assigned


def test_manifest_bucket_name_is_parsed_by_url_splitting():
    """
    sync_manager.py:50 and :149 do MANIFEST_URL.split("/")[3]. There is no
    GCS_BUCKET_NAME setting, despite README.md:104 and .env.example telling the
    user to set one.
    """
    from app_local.config import settings
    assert settings.MANIFEST_URL.split("/")[3] == "akili-registry"
    assert not hasattr(settings, "GCS_BUCKET_NAME")
