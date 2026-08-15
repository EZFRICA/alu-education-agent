"""
Import-time side effects.

Target: app_local/runtime/agent.py:21-22 (module-scope get_main_llm/get_extractor_llm)
        sys.path mutation at import across app_local/
"""

import os
import subprocess
import sys
import textwrap

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _run(code, env_overrides):
    env = dict(os.environ)
    env.update(env_overrides)
    env["PYTHONPATH"] = REPO_ROOT
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        capture_output=True, text=True, cwd=REPO_ROOT, env=env, timeout=180,
    )


def test_importing_the_agent_module_without_a_key_succeeds(tmp_path):
    """
    Was `..._fails_at_import`: agent.py called get_main_llm()/get_extractor_llm()
    at module scope, so the provider client was built as a side effect of
    `import` and a missing key made the module unimportable. Worse once
    llm_provider started raising on an unknown LLM_PROVIDER: a typo in .env
    stopped the app from starting instead of failing at the first question.
    The clients are built lazily now.
    """
    proc = _run(
        """
        import os
        os.environ["GEMINI_API_KEY"] = ""
        os.environ["GOOGLE_API_KEY"] = ""
        try:
            import app_local.runtime.agent
            print("IMPORT_OK")
        except Exception as e:
            print("IMPORT_FAILED:", type(e).__name__)
        """,
        {"GEMINI_API_KEY": "", "GOOGLE_API_KEY": "", "LLM_PROVIDER": "gemini"},
    )
    assert "IMPORT_OK" in proc.stdout, proc.stdout + proc.stderr


def test_llm_provider_is_frozen_at_import_of_llm_provider_module():
    """
    llm_provider.py:19 reads LLM_PROVIDER into a module global at import.
    Changing os.environ afterwards has no effect -- a fact any fix touching
    provider selection has to account for.
    """
    proc = _run(
        """
        import os
        os.environ["LLM_PROVIDER"] = "ollama"
        import llm_provider
        print("AT_IMPORT:", llm_provider.LLM_PROVIDER)
        os.environ["LLM_PROVIDER"] = "gemini"
        print("AFTER_CHANGE:", llm_provider.LLM_PROVIDER)
        """,
        {"LLM_PROVIDER": "ollama", "GEMINI_API_KEY": "x" * 30},
    )
    assert "AT_IMPORT: ollama" in proc.stdout
    assert "AFTER_CHANGE: ollama" in proc.stdout


def test_every_app_local_module_mutates_sys_path_at_import():
    """
    Records the sys.path.append sites. They are load-bearing: the package has no
    top-level __init__ chain that would make `from logger import get_logger`
    resolve otherwise.

    Asserts WHICH modules do it, not on which line. The line numbers were pinned
    originally, but they shift on any unrelated import edit -- they churned three
    times in one migration while telling us nothing. The finding (C14) is the set
    of modules carrying the hack, and that is what regresses if someone adds a
    ninth or introduces a proper package.
    """
    import pathlib
    hits = set()
    for p in pathlib.Path(REPO_ROOT, "app_local").rglob("*.py"):
        text = p.read_text(encoding="utf-8", errors="ignore")
        if "sys.path.append" in text or "sys.path.insert" in text:
            hits.add(str(p.relative_to(REPO_ROOT)))
    assert hits == {
        "app_local/config/settings.py",
        "app_local/core/block_detector.py",
        "app_local/core/scheduler.py",
        "app_local/mmu/controller.py",
        "app_local/runtime/agent.py",
        "app_local/storage/lance_driver.py",
        "app_local/sync/sync_manager.py",
        "app_local/teu/tools.py",
        "app_local/ui/dashboard.py",
    }, sorted(hits)


def test_app_local_has_no_package_init_files():
    """The reason the sys.path hack is needed."""
    import pathlib
    pkg = pathlib.Path(REPO_ROOT, "app_local")
    inits = sorted(str(p.relative_to(REPO_ROOT)) for p in pkg.rglob("__init__.py"))
    assert inits == [], f"expected none, found {inits}"


def test_llm_provider_default_is_gemma():
    """
    The documented default and the code default must agree. This used to be
    'gemini' while the project was developed and demonstrated against
    gemma-4-26b-a4b-it.
    """
    import subprocess, sys, os
    env = {**os.environ}
    env.pop("LLM_PROVIDER", None)
    proc = subprocess.run(
        [sys.executable, "-c",
         "import llm_provider; print('DEFAULT:', llm_provider.LLM_PROVIDER)"],
        cwd=REPO_ROOT, capture_output=True, text=True, env=env,
    )
    assert "DEFAULT: gemma" in proc.stdout, proc.stdout + proc.stderr


def test_an_unknown_llm_provider_raises_instead_of_falling_through_to_gemma():
    """
    A typo in .env used to land in the `else` branch and silently select Gemma,
    sending student messages to a backend nobody chose, with no message anywhere.
    """
    import importlib, os
    import llm_provider

    original = llm_provider.LLM_PROVIDER
    try:
        llm_provider.LLM_PROVIDER = "gemmma"
        for factory in (llm_provider.get_main_llm, llm_provider.get_extractor_llm):
            with pytest.raises(ValueError, match="Unknown LLM_PROVIDER"):
                factory()
    finally:
        llm_provider.LLM_PROVIDER = original


def test_every_known_provider_name_is_accepted():
    """KNOWN_LLM_PROVIDERS must stay in sync with the branches that exist."""
    import llm_provider

    original = llm_provider.LLM_PROVIDER
    try:
        for name in llm_provider.KNOWN_LLM_PROVIDERS:
            llm_provider.LLM_PROVIDER = name
            llm_provider.get_main_llm()
            llm_provider.get_extractor_llm()
    finally:
        llm_provider.LLM_PROVIDER = original
