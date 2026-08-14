# Akili (alu-education-agent) — Audit Findings

Read-only audit. No production module was modified. Scope: `app_local/`, `llm_provider.py`,
`cloud_registry/`, README, dependency manifests.

---

## Status table

Status ∈ {confirmed, refuted, partial, untestable}. C-rows are the reviewer's
claims (Section 4); S5-rows are mine (Section 5).

| # | Claim | Status | Evidence | Test |
|---|---|---|---|---|
| C1 | `delete_local_block` called, never defined | confirmed | `block_factory.py:75`; no def in `lance_driver.py` | — (call site unreachable) |
| C2 | `upsert_local_block` appends; rows accumulate; `.iloc[0]`; unbounded | confirmed (`.iloc[0]` currently masked) | `lance_driver.py:124`, `:92-95` | `test_three_writes_to_one_id_produce_three_rows`, `test_upsert_keeps_one_row_and_returns_the_latest` (xfail) |
| C3 | zero vector written; executor never supplies one | confirmed | `block_factory.py:159`, `:214-222` | `test_a_correctly_shaped_proposal_does_land`, `test_a_created_block_is_retrievable_by_search` (xfail) |
| C4 | `block_type` vs `type`; no `initial_content` | confirmed (facts) / **wrong consequence** — creates a corrupt block and returns `True` | `block_detector.py:62`, `block_factory.py:217-218` | `test_auto_execute_reports_success_and_creates_a_typeless_node` |
| C5 | `1-dist/2` assumes cosine; wrong scale | **partial** — metric is squared-L2, but `1-d/2` == cosine exactly for unit vectors; and the comparison never runs | `lance_driver.py:62,70` | `test_lancedb_default_metric_is_squared_l2_not_cosine`, `test_certainty_formula_on_the_actual_metric` |
| C6 | inline embeddings bypass `llm_provider.py` | confirmed | `agent.py:95-96`, `controller.py:268-269` | `test_llm_provider_ollama_does_not_remove_the_embedding_network_call` |
| C7 | 3 BMJ helpers never called; no bidirectional traversal | confirmed | `controller.py:197,320,331` | Section 2b survey |
| C8 | `LocalScheduler` never started; no cold path | confirmed | `scheduler.py:20,25`; imports at `dashboard.py:13`, `block_factory.py:5` | Section 1 trace |
| C9 | block-type drift; silent fall-through | confirmed, wider (6 spellings; `projet` is docstring-only) | Section 2a | `test_unknown_types_silently_get_the_default_ttl`, `test_insert_unknown_type_falls_through_to_the_projet_branch` |
| C10 | ~6 network round trips, all awaited | confirmed — exactly 6, floor 4 | Section 1 trace | — (measured, not asserted) |
| C11 | extractor documents temp=0, passes 0.7 | confirmed — 3 of 4 branches | `llm_provider.py:83,92,108,116` | `test_extractor_llm_is_deterministic_on_every_branch` (xfail) |
| C12 | no tests | confirmed (at audit start) | no `tests/`, no CI | this suite |
| C13 | `requirements.txt` is a pip freeze duplicating pyproject | **partial** — it is `uv pip compile`, and it *omits* `lancedb`/`langchain-ollama`/`langchain-openai` | Section 2d | — |
| C14 | `sys.path` mutation at import | confirmed, load-bearing (no `__init__.py`) | 8 sites | `test_every_app_local_module_mutates_sys_path_at_import` |
| C15 | service-account JSON on student device | confirmed | `sync_manager.py:20-40` | `test_sync_requires_a_service_account_on_the_student_device` |
| C16 | L1 TTL-only, no cap or LRU | confirmed, worse (no sweep; `_metrics` unbounded) | `cache_l1.py:27-68` | `test_cache_is_bounded` (xfail), `test_metrics_dict_grows_with_every_distinct_id_ever_queried` |
| C17 | system prompt is a `HumanMessage` | confirmed | `agent.py:167` | `test_the_system_prompt_is_sent_as_a_humanmessage` |
| C18 | f-string LanceDB predicates, one from UI state | confirmed as written; latent (selectbox bounded by catalog) | `lance_driver.py:59,92`; `sync_manager.py:171,176` | `test_filter_predicate_is_built_by_string_interpolation` |
| C19 | README describes behaviour the code lacks | confirmed | 6 specific divergences, Section 4 | — |
| **S5-A** | **`db.list_tables()` membership always False → L3 tier inert** | **confirmed** | `lance_driver.py:51,88,115` | `test_list_tables_does_not_return_a_list_of_strings` + 5 more |
| **S5-B** | `sync_with_registry` raises `UnboundLocalError` | confirmed | `sync_manager.py:251` vs `:256` | `tests/test_sync_manager.py` (3 cases) |
| **S5-C** | **BMJ promotion reverted within the same turn** | **confirmed** | `agent.py:100` vs `:171`; `controller.py:287` | `test_the_promotion_survives_the_turn` (xfail) |
| S5-D | `agent.py` unimportable without an API key | confirmed | `agent.py:21-22` | `test_importing_the_agent_module_without_a_key_fails_at_import` |
| S5-E | `requirements.txt` omits `lancedb` | confirmed | Section 2d | — |
| S5-F | TEU imported by nothing; no tools bound | confirmed | `agent.py:191-193` | Section 2b survey |
| S5-G | `last_accessed`/`access_count` never updated | confirmed | 6 write sites, 0 update sites | `test_eviction_picks_the_least_recently_accessed_block` (xfail) |
| S5-H | eviction raises `TypeError` at the cap | confirmed | `block_factory.py:144,172` | `test_working_set_stays_bounded_past_the_cap` (xfail) |
| S5-I | `move_to_front` KeyError vs `page_out` no-op | confirmed | `controller.py:297` | `test_move_to_front_of_unknown_id_is_a_noop` (xfail) |
| S5-J | dashboard discards the executor's return value | confirmed | `dashboard.py:366-368` | — |
| S5-K | `math` vs `maths` default drift | confirmed | `settings.py:22` vs `dashboard.py:155` | — |
| S5-L | `.gitignore` bare `main.py`/`config.py` patterns | confirmed | `.gitignore` | verified in scratch repo |
| S5-M | phantom workspace member `travel-agent-dll` | confirmed | `pyproject.toml:28-29` | — |
| S5-N | `weaviate` imported for an unused helper | confirmed | `lance_driver.py:6` | — |
| S5-O | `update_block_content` uncalled; live path takes no lock | confirmed | `block_factory.py:184`; `controller.py:250` | — |
| O1 | are gemini embeddings L2-normalised? | **untestable** offline — needs a live API call | C5 depends on it | — |

**Refuted / not defects:** paging out the last remaining node (correct);
`asyncio.Lock` reuse across `asyncio.run` loops (no error on 3.13);
`move_to_front` pointer surgery (correct in all positions);
`auto_execute_block_proposal` given a correctly-shaped proposal (works).

---

## Environment

Recorded before any test was written, because several claims below depend on
library defaults that move across versions.

| Component | Version |
|---|---|
| Python | 3.13.2 (CPython, Clang 20.1.0) — `.venv` |
| lancedb | 0.30.2 |
| langchain | 1.2.15 |
| langchain-core | 1.4.0 |
| langgraph | 1.1.9 |
| streamlit | 1.56.0 |
| pandas | 3.0.2 |
| pyarrow | 24.0.0 |
| langchain-google-genai | 4.2.2 |
| langchain-ollama | 1.1.0 |
| numpy | 2.4.4 |
| pytest | **not installed** at audit start |

Repo state: branch `main`, clean at `dfd7d70`.

Dev tooling added for this audit (allowed by scope): a `[dependency-groups] dev`
block in `pyproject.toml` with `pytest>=8.3.0` and `pytest-asyncio>=0.24.0`, plus a
`[tool.pytest.ini_options]` stanza setting `asyncio_mode = "auto"` and
`testpaths = ["tests"]`. Resolved to pytest 9.1.1 / pytest-asyncio 1.4.0.

Notable environment facts that matter later:

- `.env` exists in the working tree and **is** correctly covered by `.gitignore`
  (`.env`, `.env.*`, `!.env.example`). It was not read during this audit.
- `app_local/storage/akili_db/` already contains a populated `user_memory.lance` with
  6 data fragments and 6 transaction records, and `edu_registry.lance` with 1 fragment.
  This is the developer's real storage; no test in this audit touches it.
- `app_local/memory/metadata_links.json` holds live state: `head_id=current_session`,
  `tail_id=student_profile`, `dynamic_block_count=0`, course `5eme/math`.

---

## Section 1 — Request path trace

Method: not a paper trace. `scratchpad/trace_run.py` imports the real
`app_local.runtime.agent`, redirects `settings.LANCE_DB_PATH` and
`settings.METADATA_LINKS_PATH` to a temp dir, blocks `AF_INET`/`AF_INET6` sockets,
replaces `GoogleGenerativeAIEmbeddings`, `agent._llm` and `agent._extractor_llm`
with recording fakes, and wraps `save_dll`, `upsert_local_block`,
`search_block_index`, `load_dll`, `update_node_content` and the L1 cache with spies.
Then it awaits `planner_node` once.

> Note on the socket fixture: a blanket `socket.socket` ban is not usable here.
> `import lancedb` constructs a background event loop at module scope
> (`lancedb/background_loop.py:33`), which calls `socket.socketpair()` → `AF_UNIX`.
> Blocking all families makes the repo unimportable. The fixture therefore blocks
> only `AF_INET`/`AF_INET6` plus `socket.create_connection`. This is still a true
> "no network" condition.

### Ordered trace of one student turn

Entry: `dashboard.py:390` `asyncio.run(graph.ainvoke(state))` → LangGraph node
`Planner` → `agent.planner_node`.

| # | Event | Kind | Location |
|---|---|---|---|
| 1 | `planner_node` entered | fn | `app_local/runtime/agent.py:84` |
| 2 | `GoogleGenerativeAIEmbeddings(model="models/gemini-embedding-2")` constructed **inline** | client ctor | `agent.py:95-96` |
| 3 | `aembed_query(user_query)` | **NET 1 — embedding 1** | `agent.py:97` |
| 4 | `controller.load_dll()` | disk read (`metadata_links.json`) | `agent.py:100` → `controller.py:155` |
| 4a | cold start only: `init_dll()` → `save_dll()` | **disk write** | `controller.py:124` |
| 5 | `controller.search_memory(...)` | fn | `agent.py:103` → `controller.py:203` |
| 6 | `lance_driver.search_block_index(...)` | disk read (local, no network) | `lance_driver.py:39` |
| 6a | `db.list_tables()` guard | **returns `[]` — see below** | `lance_driver.py:51` |
| 7 | BMJ `move_to_front` + `save_dll` — *never reached*, `filtered` is always empty | (dead) | `controller.py:227-236` |
| 8 | `cache_l1.get()` ×3 (`student_profile`, `learning_preferences`, `current_session`) | RAM | `agent.py:116` |
| 9 | `os.path.exists(prompts.json)` + optional `open()` | disk read | `agent.py:137-148` |
| 10 | `_llm.ainvoke(messages)` — **the student's answer** | **NET 2 — main LLM** | `agent.py:168` |
| 11 | `_update_student_memory(...)` entered | fn | `agent.py:171` → `agent.py:35` |
| 12 | `_extractor_llm.ainvoke(...)` | **NET 3 — extractor LLM** | `agent.py:69` |
| 13 | `json.loads(raw)` after strip of ```` ```json ```` fences | parse | `agent.py:73-74` |
| 14 | per non-empty key → `controller.update_node_content(...)` | fn | `agent.py:78` → `controller.py:250` |
| 14a | `cache_l1.invalidate(block_id)` | RAM | `controller.py:265` |
| 14b | `GoogleGenerativeAIEmbeddings(...)` constructed **again, inline** | client ctor | `controller.py:268-269` |
| 14c | `aembed_query(content)` | **NET 4/5/6 — embeddings 2,3,4** | `controller.py:270` |
| 14d | `lance_driver.upsert_local_block(...)` → `table.add()` | **disk write** (new fragment + txn + manifest) | `lance_driver.py:98` |
| 14e | `cache_l1.set(...)` | RAM | `controller.py:282` |
| 14f | `save_dll(dll)` | **disk write** | `controller.py:287` |
| 15 | `detect_new_block_opportunity(history, dll)` | pure local | `agent.py:177` → `block_detector.py:14` |
| 16 | return | — | `agent.py:180` |
| 17 | dashboard: `controller.load_dll()` again | disk read | `dashboard.py:395` |
| 18 | dashboard: `cache_l1.set()` per node with content | RAM | `dashboard.py:398` |

### Counts per turn (observed, not inferred)

```
>>> network round trips: 6
     - embed_query("I don't understand fractions...")   [EMBEDDING]
     - main_llm.ainvoke(2 msgs)                         [LLM]
     - extractor_llm.ainvoke(1 msgs)                    [LLM]
     - embed_query('The student is named Marc...')      [EMBEDDING]
     - embed_query('Prefers concrete analogies...')     [EMBEDDING]
     - embed_query('The student is studying...')        [EMBEDDING]
>>> disk writes: 7
     - save_dll                          (cold-start init)
     - lance upsert_local_block('student_profile')
     - save_dll
     - lance upsert_local_block('learning_preferences')
     - save_dll
     - lance upsert_local_block('current_session')
     - save_dll
```

**Network: 6 round trips, all awaited before the response reaches the user.**
Four embeddings, two LLM calls. Round trip 1 (query embedding) blocks *before* any
retrieval. Round trip 2 produces the answer. Round trips 3–6 are memory write-back
and happen **after** the answer text exists but **before** `planner_node` returns —
`await` at `agent.py:171`, and the dashboard blocks on `asyncio.run(graph.ainvoke(...))`
inside `st.spinner`. So the student waits for all six.

The floor is 4 round trips (extraction prompt at `agent.py:59` says
`current_session` MUST always be filled, so at least one write-back embedding always
fires). The ceiling is 6.

**Disk: 7 writes per turn** (3 steady-state `save_dll` + 3 LanceDB fragment appends,
+1 on cold start). Every one of them is on the blocking path. Each LanceDB
`table.add()` is a *new data fragment plus a new transaction record plus a new
manifest version* — visible in the developer's real store, which already has 6
fragments and 6 transactions for 3 logical memory blocks.

### The finding that dominates the trace

Step 6a. In **lancedb 0.30.2**, `db.list_tables()` no longer returns a `list[str]`.
It returns a `ListTablesResponse` model:

```
type:   <class 'lance_namespace_urllib3_client.models.list_tables_response.ListTablesResponse'>
repr:   ListTablesResponse(tables=['edu_registry', 'user_memory'], page_token=None)
list(): [('tables', ['edu_registry', 'user_memory']), ('page_token', None)]
't1' in lt  ->  False
```

`lance_driver.py` tests membership against it in three places
(`:51`, `:88`, `:115`). Membership is **always False**. Observed against a temp DB
that genuinely contains both tables and a row whose vector matches the query exactly:

```
table_names ->  ['edu_registry', 'user_memory']
search_block_index results: []
get_block_content('student_profile'): None
--- same query issued directly against the table ---
    id  _distance
0  ch1        0.0
```

Consequences on the read path:

- `search_block_index` (`lance_driver.py:51`) `continue`s past every table →
  **returns `[]` on every call**. `context_text` at `agent.py:109` is always the
  empty string. **The L3 course retrieval never returns anything. There is no RAG.**
  The agent answers 6ème math from the base model alone.
- `get_block_content` (`:88`) **always returns `None`** → the dashboard L2 fallback
  (`dashboard.py:311`) always falls through to keywords, and the TEU tool
  `load_course_chapter` always answers *"chapitre introuvable"*.
- `upsert_local_block` (`:115`) always takes the "table does not exist" branch, so
  every single write attempts `create_table` first, raises, and lands in the
  `except` → `open_table().add()` fallback. That fallback is commit `c8fdb2f`
  *"fix: add fallback to table append in lance_driver"* — the symptom was patched,
  the cause was not diagnosed.
- `dashboard.py:335` `[t for t in db.list_tables() if isinstance(t, str)]` iterates
  the response model, gets `('tables', [...])` tuples, filters them all out →
  the L3 panel **always** renders "No tables yet. Download a course to populate."

The correct accessor on this version is `db.table_names()`, which returns
`['edu_registry', 'user_memory']`.

This single defect invalidates several downstream claims in Section 4: the certainty
threshold comparison, the BMJ move-to-front, and the distance-metric mismatch are all
unreachable, because `filtered` is empty before any of them run.


---

## Section 2 — Static survey

### 2a. Block-type string literals

Two disjoint vocabularies exist. One is written by the DLL, one by the content
pipeline, and the readers only know the first.

**Written:**

| Literal | Written at | Meaning intended |
|---|---|---|
| `"temp"` | `controller.py:68` (`current_session`) | recent context |
| `"cours"` | `controller.py:82` (`active_course`) | course context |
| `"fondamental"` | `controller.py:96`, `controller.py:110` | permanent knowledge |
| `"course"` | `block_detector.py:62` — **under the key `block_type`** | proposal for a new block |
| `"manual_chapter"` | `cloud_registry/pipeline/batch_pipeline.py:95`, `embedding_pipeline.py:55` | every row of every course parquet → `edu_registry` |
| `"memory"` / `"cours"` | `lance_driver.py:75` — read-time default when the row has no `block_type` | per source table |
| `"projet"` | **docstring only** (`block_factory.py:15`). No literal anywhere; the `else` branch at `block_factory.py:43` catches it implicitly. |

**Read:**

| Reader | Keys it recognises | Fallback |
|---|---|---|
| `CERTAINTY_THRESHOLDS` (`controller.py:35-39`) | `fondamental`, `cours`, `temp` | `MIN_RELEVANCE_CERTAINTY` = 0.70 at `controller.py:220` |
| `get_head_threshold` (`controller.py:200`) | same three | `0.55` — a **fourth**, different default, and this function is never called |
| `_TTL_BY_TYPE` (`cache_l1.py:13-17`) | `temp`, `cours`, `fondamental` | `_TTL_DEFAULT` = 300 s |
| `insert_node_by_type` (`block_factory.py:21,29,43`) | `temp`, `fondamental` | everything else → "insert after HEAD" |
| dashboard badge colour (`dashboard.py:305-306`) | `fondamental`, `temp` | `badge-blue` |

**Values that no reader ever matches:** `"course"`, `"manual_chapter"`, `"memory"`,
`"projet"`. Concretely: every row the cloud pipeline ships carries
`block_type="manual_chapter"`, so `controller.py:219-220` resolves it to the 0.70
default and the per-type threshold table is decorative for all course content.
`"course"` from `block_detector` would create a node whose type matches no TTL entry
(300 s default) and whose insert falls into the `projet` branch.

Three spellings of the same concept — `cours` / `course` / `courses` — plus
`manual_chapter`, are live in the repo at once.

### 2b. Functions defined in `app_local/` that are never called

Derived by AST-collecting every `def`/`async def` under `app_local/` and regex-counting
call sites across the whole repo, then hand-verifying each candidate.

| Function | Defined at | Note |
|---|---|---|
| `get_head_threshold` | `controller.py:197` | never called; carries the only `0.55` default |
| `_head_to_tail_order` | `controller.py:320` | never called |
| `_tail_to_head_order` | `controller.py:331` | never called — **so no bidirectional traversal executes anywhere** |
| `toggle_block` | `controller.py:241` | never called; no UI surface for it |
| `delete_block_stitching` | `block_factory.py:60` | never called — and would raise `AttributeError` if it were (2c) |
| `download_prompts` | `sync_manager.py:197` | never called; `sync_with_registry` re-implements it inline at `:243-254` |
| `LocalScheduler.push` | `scheduler.py:20` | never called |
| `LocalScheduler.start` | `scheduler.py:25` | never called |
| `load_course_chapter` | `teu/tools.py:15` | never called |
| `google_search` | `teu/tools.py:27` | never called |

Additional dead-wiring facts:

- `app_local/teu/` is **not imported by any module in the repo**. There is no
  `bind_tools`, no `ToolNode`, no tool list anywhere. The Tool Execution Unit is not
  connected to the graph; the LangGraph workflow is a single `Planner` node with one
  edge to `END` (`agent.py:191-193`). The agent cannot call a tool.
- `scheduler` is *imported* twice — `dashboard.py:13`, `block_factory.py:5` — and
  used zero times in both. Neither import is referenced again in the file.
- `lance_driver.reset_local_db` (`:28`) is called only from a `research/` script,
  which `.gitignore` excludes.

### 2c. Network client construction sites, and read-path reachability

| Site | Client | On the read path? |
|---|---|---|
| `llm_provider.py:51,59,67,74` (`get_main_llm`) | ChatOllama / ChatOpenAI / ChatGoogleGenerativeAI | **Yes** — constructed at *import* of `agent.py` (`agent.py:21`), invoked at `agent.py:168` |
| `llm_provider.py:89,96,105,114` (`get_extractor_llm`) | same three | **Yes** — `agent.py:22`, invoked at `agent.py:69` |
| `agent.py:96` | `GoogleGenerativeAIEmbeddings(model="models/gemini-embedding-2")` | **Yes — hardcoded, bypasses `llm_provider.py` entirely** |
| `controller.py:269` | `GoogleGenerativeAIEmbeddings(model="models/gemini-embedding-2")` | **Yes — same, re-constructed on every single write-back** |
| `sync_manager.py:34,37` | `google.cloud.storage.Client` | No — sidebar / sync only |
| `sync_manager.py:63` | `httpx.AsyncClient` | No — sync only |
| `teu/tools.py:41` | `google.genai.Client` | No — TEU is unreachable (2b) |
| `cloud_registry/storage_client.py:31` | `weaviate.use_async_with_weaviate_cloud` | No — cloud side |
| `cloud_registry/pipeline/*.py:19,44,237,241` | embeddings + GCS | No — cloud side |
| `lance_driver.py:6` | `from weaviate.util import generate_uuid5` | No call, but **forces `weaviate-client` to be importable for the local read path to work at all** — a cloud SDK imported for one unused helper |

The two embedding constructions are the ones that matter. They are *inline, hardcoded
to Google*, and not routed through `llm_provider.py`. Setting `LLM_PROVIDER=ollama`
changes `get_main_llm`/`get_extractor_llm` and nothing else — the query embedding at
`agent.py:97` and every write-back embedding at `controller.py:270` still go to
`generativelanguage.googleapis.com`. There is no local embedding path in the repo.

### 2d. `requirements.txt` dependencies never imported

`requirements.txt` self-identifies at line 1-2 as `uv pip compile pyproject.toml`
output — a fully-resolved transitive lock, **297 pinned distributions**. Mapping each
distribution to its `top_level.txt` modules and intersecting with the 37 top-level
imports actually present in the repo:

- **15 distributions are imported**: `google-ai-generativelanguage`, `google-api-core`,
  `google-auth`, `google-genai`, `google-generativeai`, `googleapis-common-protos`,
  `httpx`, `langchain-core`, `langchain-google-genai`, `langgraph`, `pandas`,
  `python-dotenv`, `pyyaml`, `streamlit`, `weaviate-client`.
- **282 are not** — including direct declarations that are simply unused
  (`letta`, `uvicorn`, `watchdog`, `streamlit-chat`) alongside the transitive
  closure (`llama-index-*` ×12, `mistralai`, `openai`, `tavily-python`, `temporalio`,
  `exa-py`, `anthropic`, `datadog`, `ddtrace`, `sentry-sdk`, `opentelemetry-*` ×11,
  `matplotlib`, `nltk`, `onnxruntime`, `python-pptx`, `pdfplumber`, `xlsxwriter`, …).

The more serious problem is not the extras, it is what is **missing**:

| Package | In `pyproject.toml` | In `requirements.txt` |
|---|---|---|
| `lancedb` | ✅ `>=0.30.2` | ❌ **absent** |
| `langchain-ollama` | ✅ `>=1.1.0` | ❌ **absent** |
| `langchain-openai` | ✅ `>=1.2.1` | ❌ **absent** |

`pip install -r requirements.txt` yields an environment where
`app_local/storage/lance_driver.py:2` (`import lancedb`) and `llm_provider.py:13-15`
both fail at import. The file was last committed 2026-03-16; `pyproject.toml`
2026-07-19. It is four months stale, not merely redundant.

Also declared but never imported anywhere in the repo, from `pyproject.toml` itself:
`letta`, `uvicorn`, `watchdog`, `streamlit-chat`, `google-cloud-storage`
(imported only lazily inside `sync_manager._get_storage_client`, so it is real but
optional).


---

## Section 3 — Characterization tests

Setup: `tests/conftest.py` + `[dependency-groups] dev` and
`[tool.pytest.ini_options] asyncio_mode = "auto"` in `pyproject.toml`.

Suite-wide guarantees, enforced by fixtures:

- `GEMINI_API_KEY` / `GOOGLE_API_KEY` / `OPENROUTER_API_KEY` are blanked in
  `conftest.py` at import, so a real `.env` key cannot leak into a test.
- `akili_paths` monkeypatches `settings.LANCE_DB_PATH` and
  `settings.METADATA_LINKS_PATH` to `tmp_path` and resets `lance_driver._db`.
  **The developer's `app_local/storage/` and `app_local/memory/` are never opened.**
- `no_network` blocks `AF_INET`/`AF_INET6` sockets, `create_connection` and
  `getaddrinfo`.
- `clean_l1` is autouse and flushes the L1 singleton around every test.
- Vectors are 8-dim, hand-written: `V_A=(1,0,…)`, `V_B=(0,1,0,…)`,
  `V_A_SCALED=(3,0,…)`, `V_A_OPPOSITE=(-1,0,…)`. Nothing is generated.

`check_dll_invariants()` in `conftest.py` returns a list of violated invariants:
I1 HEAD→TAIL reaches `tail_id`; I2 TAIL→HEAD reaches `head_id`; I3 both traversals
visit the same set; I4 head has no prev; I5 tail has no next; I6 no orphan
unreachable from HEAD; I7 no pointer to a non-existent id.

### 3.1 `tests/test_dll_invariants.py` — **22 passed, 1 xfailed**

What passes (locked-in behaviour):

- `init_dll` produces a healthy 4-block chain; forward order is
  `current_session → active_course → learning_preferences → student_profile`,
  reverse is the exact mirror.
- `move_to_front` preserves all seven invariants for every position (head, middle,
  tail), including repeated and idempotent moves.
- `move_to_front` on the TAIL correctly repoints `tail_id` to the old `prev`.
- `page_out_block` preserves all invariants when removing a middle node, **the
  current HEAD**, and **the current TAIL**. After paging out HEAD `a` from
  `a→b→c`: `head_id="b"`, `b.prev is None`, both traversals agree. After paging out
  TAIL `c`: `tail_id="b"`, `b.next is None`.
- `page_out_block` refuses `is_fixed` nodes and no-ops on unknown ids.
- A 7-step mixed sequence (insert temp, move, insert fondamental, page out, move,
  page out HEAD, page out TAIL) ends healthy.
- Paging out the **last remaining node** leaves `head_id=None`, `tail_id=None`,
  `nodes={}` — I suspected this was broken; it is not. Now pinned.

Two structural facts the tests record rather than judge:

- `insert_node_by_type("fondamental", …)` inserts *before* TAIL, never *at* TAIL
  (`test_insert_fondamental_lands_just_before_tail_never_at_tail`). A dynamic
  `fondamental` block therefore cannot become the TAIL by insertion.
- `insert_node_by_type("course", …)` — the exact literal `block_detector.py:62`
  emits — matches no branch and silently takes the `else` (insert-after-HEAD)
  path: `["a","new","b","c"]`
  (`test_insert_unknown_type_falls_through_to_the_projet_branch`).

**Defect pinned (xfail strict):**
`test_move_to_front_of_unknown_id_is_a_noop` —
`controller.move_to_front` indexes `nodes[block_id]` at `controller.py:297` with no
membership guard and raises `KeyError` for an unknown id, while its sibling
`page_out_block` (`block_factory.py:100`) treats an unknown id as a no-op. The two
mutators disagree on the same contract. Reachable from `search_memory`
(`controller.py:233`) only because that call site pre-filters against
`dll["nodes"]` — the guard lives at the caller, not the function.


### 3.2 `tests/test_block_cap.py` — **5 passed, 2 xfailed**

Passing (locked in): `MAX_DYNAMIC_BLOCKS == 5`; `init_dll` copies it into
`dynamic_block_max` with `dynamic_block_count == 0`; creating exactly 5 dynamic
blocks leaves 9 nodes (4 fixed + 5 dynamic), count 5, all invariants intact;
duplicate `block_id` raises `ValueError("already exists")`; and eviction *does*
work in the one configuration where it can — `dynamic_block_max = 1`, where
`min()` over a single-element list never performs a comparison.

**Defect 1 — the cap is not enforced (xfail strict).**
`test_working_set_stays_bounded_past_the_cap`. Observed:

```
app_local/mmu/block_factory.py:144: TypeError
E  TypeError: '<' not supported between instances of 'NoneType' and 'NoneType'
     lru_node = min(dynamic_nodes, key=lambda x: x.get("last_accessed", "1970-01-01T00:00:00"))
```

`block_factory.py:172` stores `"last_accessed": None` on every new dynamic node.
`dict.get(key, default)` returns the **stored** `None` — the key exists — so the
default `"1970-01-01T00:00:00"` is never used, every candidate sorts to `None`, and
`min()` compares `None < None`. The exception propagates out of
`create_dynamic_block` *before* the cap is applied and before the new block is
created. So the sixth block is not created and the fifth is not evicted: the
operation just fails. The working set is bounded only by that failure.

**Defect 2 — "least recently accessed" is not tracked at all (xfail strict).**
`test_eviction_picks_the_least_recently_accessed_block`. `last_accessed` and
`access_count` are written exactly once — `controller.py:74/88/102/116` for the four
fixed nodes, `block_factory.py:171-172` for dynamic ones — and updated by **no code
path in `app_local/`** (verified by grep: those are the only six occurrences).
Reading a block via `cache_l1.get`, `search_memory`, or `update_node_content` never
records an access. The test drives all three read paths against a block and then
asserts `last_accessed is not None`; it stays `None`. The eviction key is a
constant, so even with the `TypeError` fixed the victim would be dictionary-order,
not LRU.

### 3.3 `tests/test_lance_driver.py` — **10 passed, 4 xfailed**

**Distance metric, verified against the installed lancedb 0.30.2, not assumed.**
`test_lancedb_default_metric_is_squared_l2_not_cosine` queries a table holding four
hand-built vectors with `V_A = (1,0,0,…)`:

| row | vector | observed `_distance` | cosine would give |
|---|---|---|---|
| `same` | `(1,0,…)` | **0.0** | 0.0 |
| `orth` | `(0,1,0,…)` | **2.0** | 1.0 |
| `opposite` | `(-1,0,…)` | **4.0** | 2.0 |
| `scaled` | `(3,0,…)` | **4.0** | 0.0 |

The metric is **squared L2** (`‖a−b‖²`), not cosine and not plain L2 (which would
give `√2 = 1.414` for orthogonal). No metric is set anywhere in `lance_driver.py`,
so the table default applies.

**Certainty scale, actual numbers** (`test_certainty_formula_on_the_actual_metric`),
`certainty = 1 - dist/2` at `lance_driver.py:70`:

| case | distance | certainty | vs. thresholds 0.70 / 0.75 / 0.80 |
|---|---|---|---|
| identical vector | 0.0 | **1.0** | passes all |
| orthogonal | 2.0 | **0.0** | fails all |
| antiparallel | 4.0 | **−1.0** | fails all |
| same direction, 3× magnitude | 4.0 | **−1.0** | fails all — *despite being semantically identical* |

The important nuance, which the test proves algebraically over 0°/45°/60°/90°/180°:
for **unit-normalised** vectors, squared-L2 `= 2 − 2·cos`, so `1 − d/2` equals
cosine similarity **exactly**. The formula is therefore correct if and only if the
embeddings are L2-normalised, and degrades without bound if they are not — the
`scaled` row is the counter-example. Since `lance_driver` never normalises and
never asserts normalisation, correctness depends entirely on an undocumented
property of whatever `models/gemini-embedding-2` returns. That is an open question,
not a settled defect — see Section 5.

**Search and read, current behaviour locked in:**

- `test_search_block_index_returns_nothing_even_with_an_exact_match` — with both
  tables populated and a distance-0.0 row present, `search_block_index` returns
  `[]`, while the same query issued directly on the table returns `ch1` at
  `_distance == 0.0`.
- `test_get_block_content_returns_none_even_when_the_row_exists` — returns `None`
  with the row sitting in `user_memory`.
- `test_list_tables_does_not_return_a_list_of_strings` — pins the root cause:
  `type(resp).__name__ == "ListTablesResponse"`, `"edu_registry" not in resp` is
  True, `"edu_registry" in db.table_names()` is True.

Three xfail-strict tests specify the fix: `test_search_block_index_finds_an_exact_match`
(expects certainty 1.0), `test_orthogonal_query_returns_a_low_certainty_row`
(expects certainty 0.0 so the threshold filter has something to reject), and
`test_get_block_content_returns_the_stored_content`.

> Caveat for the fix phase: `db.table_names()` emits
> `DeprecationWarning: table_names() is deprecated, use list_tables() instead`
> on 0.30.2. The library is mid-migration — `list_tables()` changed its return
> type *and* the old accessor is deprecated. The stable read is the response
> model's `.tables` attribute.

**Upsert semantics, exact observed values:**

- `test_three_writes_to_one_id_produce_three_rows` — three writes to
  `student_profile` → **3 rows**, all with `id == "student_profile"`, contents
  `["revision one", "revision three", "revision two"]`. No deduplication.
- `test_row_count_grows_linearly_with_writes` — 10 writes → **10 rows**.
- `test_get_block_content_after_three_writes_is_still_none` — `get_block_content`
  returns `None`, so the `.iloc[0]` arbitrary-revision problem is currently *masked*
  by the `list_tables` defect. Fixing one exposes the other; the xfail
  `test_upsert_keeps_one_row_and_returns_the_latest` specifies both halves
  (1 row, newest content).


### 3.4 `tests/test_read_path_offline.py` — **5 passed, 1 xfailed**

`test_read_path_dies_on_the_query_embedding_before_any_retrieval` walks the
traceback and asserts the failing frame is `agent.py:96-97`. Recorded: with sockets
blocked, `planner_node` **never reaches retrieval, never reaches the DLL, never
reaches L1, never reaches the LLM**. The first awaited statement after entry is the
Google embedding call. Spies confirm `search_memory` was not called and the LLM was
invoked zero times.

`test_everything_after_the_embedding_works_offline` stubs *only* the embedding class
and the two LLMs: the entire rest of the turn then completes with sockets still
blocked, and `current_session` content lands in the DLL. So the embedding is not
incidentally networked — it is the one structurally unavoidable network dependency
on the read path.

`test_llm_provider_ollama_does_not_remove_the_embedding_network_call` sets
`LLM_PROVIDER=ollama` and still fails offline, because `agent.py:95-96` hardcodes
`GoogleGenerativeAIEmbeddings` and never consults `llm_provider.py`.

`test_course_context_is_empty_even_with_a_populated_registry` captures the prompt
the model actually receives, with an exact-match row sitting in `edu_registry`. The
slice between `COURSE CONTEXT (Search Results):` and `STUDENT MEMORY` is the empty
string, and `"A fraction represents a part of a whole."` does not appear anywhere in
the prompt. This is the Section 1 defect, observed at the prompt boundary.

`test_the_system_prompt_is_sent_as_a_humanmessage` — `main.seen[0]` is a
`HumanMessage`; no `SystemMessage` is present anywhere in the list (`agent.py:167`).

**Defect pinned (xfail strict):** `test_a_turn_completes_offline_using_local_memory_only`
— there is no offline branch on the read path at all.

### 3.5 `tests/test_import_time.py` — **4 passed**

Run in subprocesses so the parent suite's environment is not disturbed.

- `test_importing_the_agent_module_without_a_key_fails_at_import` — with
  `GEMINI_API_KEY=""`, `import app_local.runtime.agent` raises
  `pydantic ValidationError: API key required for Gemini Developer API`, because
  `agent.py:21-22` call `get_main_llm()` / `get_extractor_llm()` at **module scope**.
  The module cannot be imported, let alone run, without a key — even to inspect it.
  This is why `conftest.py` sets an obviously-fake non-empty key rather than `""`.
- `test_llm_provider_is_frozen_at_import_of_llm_provider_module` —
  `llm_provider.py:19` snapshots `LLM_PROVIDER` into a module global at import;
  changing `os.environ` afterwards has no effect.
- `test_every_app_local_module_mutates_sys_path_at_import` — pins the exact eight
  sites: `block_detector.py:5`, `scheduler.py:7`, `controller.py:18`,
  `agent.py:10`, `lance_driver.py:10`, `sync_manager.py:10`, `tools.py:8`,
  `dashboard.py:11`.
- `test_app_local_has_no_package_init_files` — there are **zero** `__init__.py`
  files under `app_local/`. It resolves as a PEP 420 namespace package, which is why
  `from app_local.config import settings` works but `from logger import get_logger`
  needs the repo root on `sys.path`. The `sys.path` mutations are load-bearing, not
  vestigial.

### 3.6 `tests/test_proposal_contract.py` — **9 passed, 2 xfailed**

I expected the detector/executor key mismatch to make `auto_execute_block_proposal`
fail and return `False`. **It does not.** Observed end to end:

```
PROPOSAL: {"proposed_id": "dynamic_block_1",
           "label": "Topic currently being learned",
           "block_type": "course",
           "reason": "Student showed 6 active learning signals."}
auto_execute returned: True
NODE IN DLL: {"id": "dynamic_block_1", "type": null, "keywords": [],
              "created_by": "Akili", "last_accessed": null,
              "prev": "current_session", "next": "active_course"}
order: ['current_session','dynamic_block_1','active_course',
        'learning_preferences','student_profile']
dynamic_block_count: 1
LANCE rows: 1
    id              content  block_type  class_level  subject
0   dynamic_block_1 None     None        local        local
vector dim: 768   all zero: True
```

So the mismatch is worse than a silent no-op: a **corrupt block is created and
reported as success**. `proposal.get("type")` → `None` (the detector writes
`block_type`), `proposal.get("initial_content")` → `None` (the detector never
produces it). The resulting node has `type=None`, which matches no branch of
`insert_node_by_type` (lands after HEAD via the `else`), no entry in
`_TTL_BY_TYPE` (300 s default), no entry in `CERTAINTY_THRESHOLDS` (0.70 default),
and no badge colour. The LanceDB row has `content=None`, `block_type=None`,
`class_level="local"`, `subject="local"` (hardcoded at `block_factory.py:157-158`),
and a 768-dim **all-zero** vector. `dashboard.py:366-368` then renders
`st.success("Entry created!")`.

Also recorded:
- `test_detector_fires_and_records_its_exact_output_shape` — the proposal dict has
  exactly `{proposed_id, label, block_type, reason}`. No `type`, no
  `initial_content`, no `keywords`.
- `test_a_correctly_shaped_proposal_does_land` — feeding the executor the shape it
  actually reads works fine. **The executor is not broken; the contract is.**
- `test_proposed_id_collides_after_a_page_out` — `proposed_id` is derived from
  `dynamic_block_count` (`block_detector.py:57`), not from the ids present. A page-out
  decrements the counter, so ids are reused and the next `create_dynamic_block` hits
  `ValueError("already exists")`.
- `test_trigger_list_matches_substrings_inside_unrelated_words` — `"how"` and
  `"why"` are matched with `trigger in msg.lower()` (`block_detector.py:52`), so
  *"Show me the shower schedule somehow"* scores as multiple learning signals.

Two xfail-strict specs: the proposal should land with `type == "course"`, and a
created block should not be stored with a zero vector (which is equidistant from
everything — squared-L2 distance exactly 1.0 to any unit query → certainty 0.5,
below every threshold, so the block is unretrievable forever).

### 3.7 `tests/test_cache_l1.py` — **19 passed, 1 xfailed**

TTL, pinned with a monkeypatched `time.monotonic`: `temp`=300 s, `cours`=600 s,
`fondamental`=1800 s, default 300 s. Expiry is **exclusive** — at exactly `t+TTL`
the entry is already gone (`now < expiry` at `cache_l1.py:45`).
`test_unknown_types_silently_get_the_default_ttl` records that `course`,
`manual_chapter`, `projet` and `None` all silently collapse to 300 s.

Invalidate-then-get is a clean miss. `invalidate` is safe on an absent key. But
`test_invalidate_keeps_the_metrics_row` records that `invalidate` pops from `_cache`
and never from `_metrics`.

Growth, the part that matters for the constrained-hardware goal:

- `test_expiry_is_only_evaluated_on_read` — there is **no sweep**. An expired entry
  stays resident in `_cache` indefinitely; `get_all_cached()` filters it from the
  listing but does **not** evict it. Only a `get()` for that exact id frees it.
  So blocks that are written once and never read again are never reclaimed.
- `test_cache_has_no_entry_cap` — 2000 distinct ids → 2000 resident entries.
- `test_metrics_dict_grows_with_every_distinct_id_ever_queried` — 1000 pure
  **misses** (nothing cached at all) → `len(_cache) == 0` but
  `len(_metrics) == 1000`. `_ensure_metrics` runs before the lookup
  (`cache_l1.py:39`), so merely *asking* about an id allocates a permanent row.

**Defect pinned (xfail strict):** `test_cache_is_bounded` — no entry cap, no LRU,
TTL-only lazy eviction, and an unbounded metrics dict. "Bounded RAM" is not enforced
anywhere in L1.

### 3.8 `tests/test_extractor_parsing.py` — **13 passed, 2 xfailed**

The parsing logic is not factored out of `_update_student_memory`, so these drive
the real function with a stub extractor and observe what lands in the DLL.

| Model output shape | Result |
|---|---|
| bare JSON object | ✅ parses, both non-empty keys land |
| ` ```json … ``` ` fenced | ✅ parses |
| bare ` ``` … ``` ` fence | ✅ parses |
| values ≤ 5 chars | ✅ parses, short values dropped by `len(...) > 5` at `agent.py:77` |
| **trailing prose** after the object | ❌ **swallowed** — 1 log line, nothing written |
| **leading prose** before the object | ❌ **swallowed** |
| **truncated object** (token limit) | ❌ **swallowed** |
| **fence embedded in prose** | ❌ **swallowed** |
| single-quoted pseudo-JSON | ❌ **swallowed** |
| JSON **array** instead of object | ❌ parses, then `.items()` raises — **swallowed**, indistinguishable from a syntax error |
| **unknown key** (`favourite_colour`) | ❌ discarded with **no log line at all** (`controller.py:259-260` returns early) |
| non-string value (`{"name": …}`) | ❌ `.strip()` raises — **swallowed** |
| one bad key + one good key | ❌ **both lost** — the loop is inside the `try`, so extraction is all-or-nothing per turn |

Every swallowed case produces exactly one `logger.error("Error during memory
extraction: …")` and the turn still returns a normal answer. A student sees no
indication that memory silently stopped updating.

**Defects pinned (xfail strict):**
`test_realistic_malformed_output_still_updates_memory` (prose + fence + prose — the
single most common real model output) and
`test_extractor_llm_is_deterministic_on_every_branch`, which reads
`get_extractor_llm`'s own source: the docstring at `llm_provider.py:83` promises
`temperature=0`, and three of four branches pass **0.7** (`:92` gemini, `:108`
ollama, `:116` gemma). Only the openrouter branch passes `0.0`. Note the default
provider is `gemini` (`llm_provider.py:19`) and its branch also sets no JSON
response-format — the openrouter branch sets `response_format=json_object` and the
ollama branch sets `format="json"`, so the **default** provider is both the least
constrained and the most likely to emit prose.

### 3.9 `tests/test_sync_manager.py` — **7 passed**

`test_manifest_without_a_prompts_key_raises_unboundlocalerror` and two siblings
record a crash I did not find on the reviewer's list. `updated` is assigned only at
`sync_manager.py:251`, nested inside `if needs_update:` inside `if "prompts" in
manifest:`, and read unconditionally at `:256`. Reproduced:

```
Starting synchronization with Akili registry...
RAISED: UnboundLocalError - cannot access local variable 'updated'
        where it is not associated with a value
```

It raises on **three** paths: manifest has no `prompts` key; the prompt blob
download fails; and — the important one —
`test_local_manifest_hash_match_skips_the_update_and_raises`: once a first sync has
succeeded, the local `prompts.json` hash matches the remote, `needs_update` becomes
`False`, and the next run raises. So "🔄 Check for Updates" works **once** and then
throws on every subsequent press. `dashboard.py:242` does not wrap it in
`try/except`, so the Streamlit page shows a traceback.

`test_sync_requires_a_service_account_on_the_student_device` reads
`_get_storage_client`'s source: service-account JSON from
`GOOGLE_APPLICATION_CREDENTIALS`, or `google.auth.default()`. No anonymous path, no
public-URL path, even though `MANIFEST_URL` is a plain
`https://storage.googleapis.com/...` object. Every registry read goes through it.

`test_manifest_bucket_name_is_parsed_by_url_splitting` — the bucket is
`MANIFEST_URL.split("/")[3]` at `:50` and `:149`; there is no `GCS_BUCKET_NAME`
setting, despite `README.md:104` and `.env.example` telling the user to set one.

### Suite total

```
87 passed, 13 xfailed, 0 failed  (16.35s)
```

No API key required, no INET socket opened, no file written outside `tmp_path`.
Verified after the run: `app_local/storage/akili_db/user_memory.lance/data/` still
holds its original 6 fragments, and `git status` shows only `pyproject.toml`
(dev group), `REVIEW_FINDINGS.md`, and `tests/`.

### 3.10 `tests/test_bmj_promotion.py` — **11 passed, 1 xfailed**

These stub `lance_driver.search_block_index`, because on 0.30.2 it returns `[]`
unconditionally and BMJ would never fire at all. The point is that BMJ is broken for
a **second, independent** reason.

- `test_search_memory_alone_does_promote_to_head` — in isolation, BMJ works:
  HEAD goes `current_session` → `student_profile` and is persisted.
- `test_the_promotion_is_reverted_before_the_turn_ends` — after a full
  `planner_node` turn with that same search hit, HEAD is back to `current_session`
  and the order is byte-for-byte the original.
- `test_bmj_promotes_at_most_one_node_per_search` — the `break` at
  `controller.py:236` means only the first matching DLL node is promoted.
- `test_course_blocks_below_threshold_are_filtered_out` — with
  `block_type="manual_chapter"` (what every shipped course row actually carries),
  the comparison is against the 0.70 default: 0.71 kept, 0.69 dropped. A `temp`
  block at 0.79 is dropped because that type needs 0.80.

**Defect pinned (xfail strict):** `test_the_promotion_survives_the_turn`.
`planner_node` captures `dll` at `agent.py:100`. `search_memory` does its *own*
`load_dll` (`controller.py:228`), `move_to_front`, and `save_dll` (`:234`).
`planner_node` then hands its **stale line-100 handle** to `_update_student_memory`
(`agent.py:171`), and `update_node_content` finishes with `save_dll(dll)`
(`controller.py:287`) — writing the pre-BMJ `head_id`, `tail_id` and every
`prev`/`next` back over the promotion. Since the extraction prompt guarantees
`current_session` is always written, **every turn undoes the routing decision it
just made**. Fixing `lance_driver` will not fix this.

Final suite: **98 passed, 14 xfailed, 0 failed.**

---

## Section 4 — Claims to verify

Treated as hypotheses. Where a claim is wrong or incomplete I say so; three are
materially wrong in ways that matter for the fix plan.

---

**C1. `lance_driver.delete_local_block` is called by `block_factory.delete_block_stitching` but never defined.** — **confirmed**

`block_factory.py:75` awaits `lance_driver.delete_local_block(block_id)`. No `def`
of that name exists anywhere in the repo (`lance_driver.py` defines only `get_db`,
`reset_local_db`, `search_block_index`, `get_block_content`, `upsert_local_block`).
Latent rather than live: `delete_block_stitching` itself has no call site
(Section 2b), so the `AttributeError` is never reached today. It would fire on the
first line of real work if anything ever wired up block deletion — and it would fire
*after* `get_dll_lock` is acquired, at `block_factory.py:75`, before the chain is
stitched, leaving the DLL untouched but the operation half-attempted.

---

**C2. `upsert_local_block` appends rather than upserts; repeated writes to one id accumulate rows; `get_block_content` returns an arbitrary revision via `.iloc[0]`; the table grows without bound.** — **confirmed**, with one correction

Append/accumulate/unbounded: confirmed and measured.
`test_three_writes_to_one_id_produce_three_rows` → **3 rows** for one `block_id`;
`test_row_count_grows_linearly_with_writes` → **10 writes = 10 rows**.
`lance_driver.py:124` is a bare `table.add(data)`; the comment on `:123` says
*"Simplified Upsert: we add (we could delete before for the same ID)"*, so this is
acknowledged in-code. Growth is worse than row count suggests: each `add()` creates a
new LanceDB data fragment, transaction record and manifest version, ~3 per student
turn. The developer's own store already holds 6 fragments for 3 logical blocks.

The correction: **`.iloc[0]` is not currently reached.** `get_block_content` bails
out at `lance_driver.py:88` before the query, because of the `list_tables` defect
(S5-A), and returns `None` unconditionally
(`test_get_block_content_after_three_writes_is_still_none`). The `.iloc[0]` problem
is real as written — `where(id = …).limit(1)` with no `ORDER BY updated_at` — but it
is currently *masked*. Fixing the table guard exposes it. Both halves are pinned by
`test_upsert_keeps_one_row_and_returns_the_latest`.

---

**C3. `create_dynamic_block` writes a zero vector when none is supplied, and `auto_execute_block_proposal` never supplies one.** — **confirmed**

`block_factory.py:159`: `vector=vector or ([0.0] * 768)`.
`auto_execute_block_proposal` (`:214-222`) passes no `vector=` argument and contains
no embeddings call of any kind. Observed row: `vector dim: 768, all zero: True`
(`test_a_correctly_shaped_proposal_does_land`).

Consequence worth stating: a zero vector is not merely "bad", it is *degenerate*.
Under the squared-L2 metric its distance to any unit query is exactly 1.0, so
`certainty = 1 - 1/2 = 0.5` — below every threshold in `CERTAINTY_THRESHOLDS` and
below `MIN_RELEVANCE_CERTAINTY`. Every auto-created block is permanently
unretrievable by semantic search. Pinned by `test_a_created_block_is_retrievable_by_search`.
The literal `768` is also hardcoded and tied to nothing in `settings.py`.

---

**C4. `block_detector` returns key `block_type` while `auto_execute_block_proposal` reads `type`; `initial_content` is never produced.** — **confirmed on the facts, wrong on the consequence**

Both key facts hold: `block_detector.py:62` emits `"block_type": "course"`;
`block_factory.py:217` reads `proposal.get("type")`; `initial_content` and
`keywords` are never in the detector's output
(`test_detector_fires_and_records_its_exact_output_shape` asserts the key set is
exactly `{proposed_id, label, block_type, reason}`).

The consequence is **not** a failed creation. Observed:

```
auto_execute returned: True
NODE IN DLL: {"id": "dynamic_block_1", "type": null, "keywords": [], ...}
LANCE rows: 1   content=None  block_type=None  class_level=local  subject=local
vector dim 768, all zero
```

Nothing raises. `type=None` and `content=None` flow straight through
`create_dynamic_block` into both the DLL and LanceDB, and the executor returns
`True`. `dashboard.py:366-368` then renders `st.success("Entry created!")`. So the
student is shown a memory block that is empty, typeless and unsearchable — a
corrupt write reported as a success, which is strictly worse than the silent no-op
the claim implies. `test_auto_execute_reports_success_and_creates_a_typeless_node`.

---

**C5. `certainty = 1 - dist/2` assumes cosine distance, but no metric is set on the LanceDB search, so the thresholds compare against a different scale.** — **partial**

Confirmed: no metric is set anywhere in `lance_driver.py`, and the default on
lancedb 0.30.2 is **squared L2**, not cosine. Measured, not assumed
(`test_lancedb_default_metric_is_squared_l2_not_cosine`): identical 0.0, orthogonal
**2.0**, antiparallel **4.0**, same-direction-3× **4.0**. Cosine would give
0 / 1 / 2 / 0; plain L2 would give 0 / 1.414 / 2 / 2.

But the inference does not follow. For **unit-normalised** vectors,
squared-L2 `= 2 − 2·cos`, so `1 − d/2 = cos` **exactly** — the formula reproduces
cosine similarity, and 0.70/0.75/0.80 are sensible cosine thresholds. Verified over
0°/45°/60°/90°/180° in `test_certainty_formula_on_the_actual_metric`. So the scale
is correct if and only if the embeddings are L2-normalised, and degrades without
bound if they are not: the `scaled` row is semantically identical to the query and
scores **−1.0**.

Nothing in the code normalises or asserts normalisation, so correctness rests on an
undocumented property of `models/gemini-embedding-2`. **Open question O1** below.

Second correction: this comparison **never executes**. `filtered` at
`controller.py:217` iterates a list that is always empty (S5-A), so
`CERTAINTY_THRESHOLDS` is unreachable configuration today.

---

**C6. Embeddings are constructed inline with `GoogleGenerativeAIEmbeddings` in `runtime/agent.py` and `mmu/controller.py`, bypassing `llm_provider.py`, so `LLM_PROVIDER=ollama` does not remove the network dependency from the read path.** — **confirmed**

`agent.py:95-96` and `controller.py:268-269`, both function-local imports, both
hardcoded to `models/gemini-embedding-2`, neither routed through `llm_provider.py`.
`llm_provider.py` exports only `get_main_llm` and `get_extractor_llm`; there is no
embeddings factory in the repo.

Tested: `test_llm_provider_ollama_does_not_remove_the_embedding_network_call` sets
`LLM_PROVIDER=ollama` and the turn still fails with sockets blocked.
`test_read_path_dies_on_the_query_embedding_before_any_retrieval` shows it fails at
`agent.py:96-97`, before the DLL, before LanceDB, before L1, before the LLM.
`controller.py:269` re-constructs the client on **every** write-back — 1–3 times per
turn — rather than reusing one.

---

**C7. `get_head_threshold`, `_head_to_tail_order` and `_tail_to_head_order` are never called; the bidirectional traversal decision of BMJ does not execute.** — **confirmed**

All three have no call site (`controller.py:197`, `:320`, `:331`). Confirmed by AST
call-site counting plus grep across the whole repo. `get_head_threshold` is the only
place the `0.55` default appears, so that constant is dead too.

What `search_memory` actually does (`controller.py:203-238`) is: one flat LanceDB
vector search, a threshold filter, then a single `move_to_front` on the first
matching DLL node, with a `break`. There is no traversal, no direction choice, no
head-vs-tail distance comparison — nothing that would justify the name
*Bidirectional Metadata Jump*. The two traversal helpers exist but are used only by
this audit's tests. The DLL's `prev` pointers are written and maintained but **never
read by any production code path**.

---

**C8. `LocalScheduler` is never started and never receives a task; there is no cold path and all memory writes are synchronous and inline in `planner_node`.** — **confirmed**

`scheduler` is imported at `dashboard.py:13` and `block_factory.py:5` and referenced
zero further times in either file. `LocalScheduler.start` (`scheduler.py:25`) and
`.push` (`:20`) have no call sites. The queue is never drained; `_handle_task` knows
one task type (`GC_OPTIMIZE`) that nothing ever enqueues.

The hot/cold split does not exist. Section 1's trace shows all three write-back
embeddings, all three LanceDB appends and all three `save_dll` calls happening inline
inside `_update_student_memory`, awaited at `agent.py:171` before `planner_node`
returns, inside the dashboard's `st.spinner`. Every byte of memory persistence is on
the student's critical path.

---

**C9. Block-type strings have drifted across `cours`, `projet` and `course`; unknown values fall through to defaults silently.** — **confirmed**, and wider than stated

Full inventory in Section 2a. The drift spans **six** spellings, not three:
`cours` (DLL), `course` (`block_detector.py:62`), `manual_chapter` (every row the
cloud pipeline ships), `memory` (`lance_driver.py:75` default), `fondamental`,
`temp`. `projet` appears **only in a docstring** (`block_factory.py:15`) — there is
no such literal in the codebase; the `else` branch catches it implicitly.

Silent fall-through confirmed at four readers, each with a *different* default:
`CERTAINTY_THRESHOLDS` → 0.70, `get_head_threshold` → 0.55 (dead),
`_TTL_BY_TYPE` → 300 s, `insert_node_by_type` → insert-after-HEAD, dashboard badge →
blue. No reader validates, warns, or logs. `test_unknown_types_silently_get_the_default_ttl`
and `test_insert_unknown_type_falls_through_to_the_projet_branch` pin this.

The live consequence: `manual_chapter` is what *all* course content carries, so the
per-type threshold table never applies to course content at all.

---

**C10. A single student turn performs roughly six network round trips, all awaited before the response returns.** — **confirmed**

Measured, Section 1: **6** round trips — 1 query embedding, 1 main LLM, 1 extractor
LLM, 3 write-back embeddings. The floor is **4** (the extraction prompt at
`agent.py:59` states `current_session` MUST always be filled, so at least one
write-back embedding always fires); the ceiling is 6.

All six are awaited before `planner_node` returns, and `dashboard.py:390` blocks on
`asyncio.run(graph.ainvoke(state))`. The student waits for all of them, including the
four that have nothing to do with producing the answer.

---

**C11. `get_extractor_llm` documents `temperature=0` but passes `0.7` on most branches, while its output is parsed as JSON.** — **confirmed**

Docstring `llm_provider.py:83`: *"deterministic memory-extraction LLM
(temperature=0)"*. Actual: `:92` gemini **0.7**, `:100` openrouter **0.0**,
`:108` ollama **0.7**, `:116` gemma **0.7**. Three of four.

The output is parsed by `json.loads` at `agent.py:74`. Compounding it: the openrouter
branch sets `response_format={"type":"json_object"}` and the ollama branch sets
`format="json"`, but the **default** provider (`gemini`, `llm_provider.py:19`) sets
neither — so the default path is simultaneously the least constrained and the most
likely to emit prose. `test_extractor_llm_is_deterministic_on_every_branch`.

Related: `get_main_llm`'s ollama branch passes `num_predict=2048` and the extractor's
`num_predict=1024`; a truncated JSON object is one of the swallowed cases
(`test_truncated_object_is_swallowed_silently`).

---

**C12. There are no tests.** — **confirmed** (at audit start)

No `tests/` directory, no `test_*.py` or `*_test.py` anywhere outside
`.gemini/antigravity/…/scratch/test_async_robustness.py`, which is agent scratch
output in a gitignored directory. No pytest/unittest dependency in `pyproject.toml`.
No CI config anywhere in the repo.

This session added `tests/` (9 files, 98 passing + 14 xfail-strict).

---

**C13. `requirements.txt` is a full pip freeze duplicating `pyproject.toml`.** — **partial; the real problem is the opposite of duplication**

It is not a `pip freeze` — line 1-2 self-identify it as
`uv pip compile pyproject.toml -o requirements.txt`, a resolved transitive lock of
297 pins. Only 15 of those distributions are imported anywhere (Section 2d).

But it does **not** duplicate `pyproject.toml` — it contradicts it. `lancedb`,
`langchain-ollama` and `langchain-openai` are all **absent**. `pip install -r
requirements.txt` produces an environment where `lance_driver.py:2` and
`llm_provider.py:13-15` both fail at import. Last committed 2026-03-16 vs
`pyproject.toml` 2026-07-19: four months stale, spanning the commits that introduced
LanceDB and OpenRouter. This is an install-path break, not tidiness.

---

**C14. Modules mutate `sys.path` at import time instead of relying on an installed package.** — **confirmed**, and it is load-bearing

Eight sites, pinned exactly by `test_every_app_local_module_mutates_sys_path_at_import`:
`block_detector.py:5`, `scheduler.py:7`, `controller.py:18`, `agent.py:10`,
`lance_driver.py:10`, `sync_manager.py:10`, `teu/tools.py:8`, `dashboard.py:11`.

Worth adding: `app_local/` contains **zero** `__init__.py` files
(`test_app_local_has_no_package_init_files`). `from app_local.config import settings`
works only via PEP 420 namespace packages, and `from logger import get_logger`
requires the repo root on `sys.path`. `pyproject.toml` declares no `[tool.setuptools]`
packages or build backend, so `app_local` is not an installable package at all —
removing the `sys.path` lines without adding packaging would break every module.

---

**C15. `sync_manager` expects a service-account JSON on the end-user device to read a read-only content registry.** — **confirmed**

`_get_storage_client` (`sync_manager.py:20-40`) authenticates with
`service_account.Credentials.from_service_account_file(GOOGLE_APPLICATION_CREDENTIALS)`,
falling back to `google.auth.default()`. There is no anonymous client and no
public-URL path. Every registry read routes through it: `get_remote_catalog`
(`:79`), `download_course` (`:126`, `:148`), `sync_with_registry` (`:247`).
`README.md:105` instructs the student to set `GOOGLE_APPLICATION_CREDENTIALS`.

`MANIFEST_URL` (`settings.py:10`) is a plain
`https://storage.googleapis.com/akili-registry/manifest.json`, which for a
public-read bucket needs no credentials at all — and `_fetch_json` (`:60-72`) is an
unauthenticated `httpx` fetcher already present in the file, used by
`sync_with_registry` and `download_prompts` but **not** by the catalog or course
download. Both mechanisms coexist; the authenticated one is on the path that
matters. `test_sync_requires_a_service_account_on_the_student_device`.

Shipping a service-account key to every student device also means one leaked key
compromises the whole registry, and no student can be revoked individually.

---

**C16. `cache_l1` evicts on TTL only, with no entry cap or LRU.** — **confirmed**, and worse than stated

No cap, no LRU: 2000 distinct ids → 2000 resident entries
(`test_cache_has_no_entry_cap`). Two additions:

1. **There is no sweep.** Expiry is evaluated only inside `get()` for the one id
   being read (`cache_l1.py:43-52`). `get_all_cached()` filters expired entries out
   of its *return value* but does not evict them. A block written once and never
   read again occupies RAM for the process lifetime
   (`test_expiry_is_only_evaluated_on_read`).
2. **`_metrics` is separately unbounded and never pruned** — not by TTL, not by
   `invalidate()`, only by `flush_all()`. `_ensure_metrics` runs *before* the lookup
   (`cache_l1.py:39`), so 1000 pure misses on ids that were never cached leave
   `len(_cache) == 0` and `len(_metrics) == 1000`
   (`test_metrics_dict_grows_with_every_distinct_id_ever_queried`).

---

**C17. The system prompt is passed as a `HumanMessage` rather than a `SystemMessage`.** — **confirmed**

`agent.py:167`: `messages = [HumanMessage(content=system_prompt)] + state["messages"]`.
`SystemMessage` is not imported anywhere in `app_local/`.
`test_the_system_prompt_is_sent_as_a_humanmessage` asserts `main.seen[0]` is a
`HumanMessage` and no `SystemMessage` appears in the list.

Consequence beyond correctness: the persona, class guidelines, course context and
student memory all arrive as *user turn 1*, then the student's real question arrives
as *user turn 2* — two consecutive user messages with no assistant turn between.
Instruction-following and role separation are weaker, and the whole prompt becomes
cacheable/steerable as user content.

---

**C18. LanceDB predicates are built by f-string interpolation, including one value originating from UI state.** — **confirmed as written; latent rather than live**

Four sites: `lance_driver.py:59`
(`f"class_level = '{class_level}' AND subject = '{subject}'"`), `lance_driver.py:92`
(`f"id = '{block_id}'"`), `sync_manager.py:171` and `:176` (same class/subject
shape). No parameter binding anywhere; LanceDB's Python API does not expose
placeholders for `.where()`, so this needs quoting or validation.

On provenance: `class_level`/`subject` do originate in Streamlit state
(`dashboard.py:182`, `:187`) but the `st.selectbox` options are the *keys of the
remote catalog* (`:180`, `:185`), so the value is constrained to whatever the
registry publishes — an operator-controlled set, not free student text. `block_id` at
`:92` comes from DLL node ids and chapter ids. So there is no live injection vector
from a student today; the exposure is that a compromised or careless registry entry
(a subject containing an apostrophe is enough) breaks or rewrites the predicate.
`test_filter_predicate_is_built_by_string_interpolation` shows a quote in the value
raises. Real, but rank it as robustness, not an open door.

---

**C19. The README describes behaviour the code does not currently exhibit.** — **confirmed**

Specific divergences:

| README | Reality |
|---|---|
| `:67` *"Merges real-time course search with long-term student history"* | Course search returns `[]` on every call (S5-A). Only student history reaches the prompt. |
| `:42` mermaid edge `Sync --> L2` | `sync_manager` writes `edu_registry` (L3), `local_manifest.json` and `prompts.json`. It never touches `metadata_links.json`. The edge does not exist. |
| `:58` *"**Dynamic Learning Layer (DLL)**"* | Every code comment and docstring says **doubly linked list** (`controller.py:2`, `:5`, `:293`, `:310`). Two different expansions of the same acronym in the same project. |
| `:64` *"Powered by **Google Gemini**"* | `pyproject.toml:4` says *"powered by Gemma 4 via Ollama"*. `llm_provider.py:19` defaults to `gemini`. Three sources, three answers. |
| `:104` `GCS_BUCKET_NAME=your_bucket` | No such setting exists. The bucket is `MANIFEST_URL.split("/")[3]` (`sync_manager.py:50`, `:149`). `test_manifest_bucket_name_is_parsed_by_url_splitting`. |
| `:126` *"click 'Check for Updates' to download the registry"* | Raises `UnboundLocalError` on most paths, including every press after the first successful one (S5-B). |
| `:128` *"Restart the app; the L2 Storage persists your profile"* | This one holds — `metadata_links.json` does persist node `content`. |
| `:129` *"Ask about a historical event. Akili will explain it using a basketball analogy"* | Plausible: memory context does reach the prompt. But with course search dead, the *historical event* itself comes from the base model, not the downloaded curriculum. |
| `:59` *"L1 Cache (RAM): ~0ms"* | Holds. |

The README's L1/L2 claims are broadly accurate. Its L3, sync and course-retrieval
claims are not.


---

## Section 5 — Findings not on the list

### S5-A. `db.list_tables()` membership is always False on lancedb 0.30.2 — the L3 tier is inert

`lance_driver.py:51`, `:88`, `:115` all test `name in db.list_tables()`. On 0.30.2
that returns a `ListTablesResponse` model, not `list[str]`, and `in` never matches.
Full evidence in Section 1.

- `search_block_index` returns `[]` on every call → **no course content ever reaches
  the prompt**. The system is a chatbot with a student-profile string, not a RAG
  tutor.
- `get_block_content` returns `None` on every call → dashboard L2 fallback and the
  TEU tool are both permanently broken.
- `upsert_local_block` always takes the create-then-fail-then-append path.
- `dashboard.py:335` always renders "No tables yet."

Tests: `test_list_tables_does_not_return_a_list_of_strings`,
`test_search_block_index_returns_nothing_even_with_an_exact_match`,
`test_get_block_content_returns_none_even_when_the_row_exists`,
`test_course_context_is_empty_even_with_a_populated_registry`.

**Not deliberate**, and there is evidence of it being misdiagnosed: commit `c8fdb2f`
*"fix: add fallback to table append in lance_driver"* added the
`try create_table / except → open_table().add()` fallback at `:116-120`. That is the
symptom of this bug being papered over. The `isinstance(t, str)` filter at
`dashboard.py:335` is a second symptom-level workaround.

Note for the fix: `db.table_names()` works but emits
`DeprecationWarning: table_names() is deprecated, use list_tables() instead`. The
library changed the return type *and* deprecated the old accessor in the same
release. The stable read is the response's `.tables` attribute.

### S5-B. `sync_with_registry` raises `UnboundLocalError` on most paths, including the steady state

`updated` is assigned only at `sync_manager.py:251`, two `if`s deep, and read
unconditionally at `:256`. Reproduced:

```
RAISED: UnboundLocalError - cannot access local variable 'updated'
        where it is not associated with a value
```

Three triggering paths, of which the third is the one users hit:
manifest has no `prompts` key; the prompt blob download fails; **the local
`prompts.json` hash already matches the remote**, so `needs_update` is `False`.
After one successful sync, every subsequent "🔄 Check for Updates" press throws.
`dashboard.py:242` has no `try/except`, so Streamlit renders a traceback.
Tests: `tests/test_sync_manager.py`, 3 cases.

**Not deliberate.** `download_prompts` (`:197-214`) is a correct, complete
implementation of the same logic that `sync_with_registry` re-implements inline and
gets wrong — and `download_prompts` is never called.

### S5-C. The BMJ promotion is reverted by the same turn that makes it

Detailed in Section 3.10. `planner_node`'s stale `dll` handle from `agent.py:100` is
written back over the promotion by `save_dll` at `controller.py:287`. Independent of
S5-A: fixing the storage layer will not make BMJ work.
Test: `test_the_promotion_is_reverted_before_the_turn_ends` /
`test_the_promotion_survives_the_turn` (xfail).

This is the same stale-handle hazard in general form: `planner_node` holds a
long-lived `dll` dict across three awaits, and two different code paths persist it.

### S5-D. `app_local.runtime.agent` cannot be imported without an API key

`agent.py:21-22` call `get_main_llm()` / `get_extractor_llm()` at module scope. With
`GEMINI_API_KEY=""`, `import app_local.runtime.agent` raises
`pydantic ValidationError: API key required for Gemini Developer API` — before any
function runs. The module cannot be imported to inspect, lint, or unit-test it.
This is why `conftest.py` must inject a fake key. Test:
`test_importing_the_agent_module_without_a_key_fails_at_import`.

It also means the provider is selected once, at import, from a snapshot of the
environment (`llm_provider.py:19`), so nothing can switch providers at runtime.

### S5-E. `requirements.txt` omits `lancedb` — the documented install path is broken

Section 2d / C13. `lancedb`, `langchain-ollama` and `langchain-openai` are absent
from a file that is otherwise a 297-pin transitive lock. Anyone following a
`pip install -r requirements.txt` route gets an environment that cannot import the
storage layer or the LLM provider. (The README's own instruction is `uv sync`, which
reads `pyproject.toml` and works — so this bites the pip user, not the uv user.)

### S5-F. The Tool Execution Unit is not connected to anything

`app_local/teu/tools.py` defines two `@tool` functions and is **imported by no module
in the repo**. There is no `bind_tools`, no `ToolNode`, no tool list. The LangGraph
workflow is one `Planner` node with a single edge to `END` (`agent.py:191-193`). The
agent cannot call a tool, so the "isolated Tool Execution Unit" of the architecture
has no runtime existence. `load_course_chapter` would return
*"chapitre introuvable"* in every case anyway, via S5-A.

**Possibly deliberate** — see open question O2.

### S5-G. `last_accessed` / `access_count` are written once and never updated

Six write sites total (`controller.py:73/74/87/88/101/102/115/116` at init,
`block_factory.py:171-172` at creation), zero update sites. No read path records an
access. This makes the LRU eviction key constant, the dashboard's implied recency
ordering meaningless, and `access_count` permanently `0` for every block.
Test: `test_eviction_picks_the_least_recently_accessed_block` (xfail).

### S5-H. `create_dynamic_block`'s eviction raises `TypeError` at the cap

Section 3.2. `x.get("last_accessed", default)` returns the stored `None`, so
`min()` compares `None < None`. The cap is "enforced" only by the operation failing.
Test: `test_working_set_stays_bounded_past_the_cap` (xfail).

### S5-I. `move_to_front` and `page_out_block` disagree on unknown ids

`move_to_front` (`controller.py:297`) raises `KeyError`; `page_out_block`
(`block_factory.py:100-102`) returns the DLL unchanged. Same conceptual operation,
opposite contracts. Currently safe only because `search_memory` pre-filters against
`dll["nodes"]` at the call site.
Test: `test_move_to_front_of_unknown_id_is_a_noop` (xfail).

### S5-J. `dashboard.py` reports success for a proposal that may have failed

`dashboard.py:366-368`:

```python
asyncio.run(auto_execute_block_proposal(proposal))
st.session_state.pending_block_proposal = None
st.success("Entry created!")
```

The return value is discarded. `auto_execute_block_proposal` catches every exception
(`block_factory.py:224`) and returns `False`, and the UI reports success either way.
Combined with C4, the current behaviour is that it returns `True` for a corrupt
block — but the discarded return value means a genuine failure would also be
invisible.

### S5-K. Default course strings disagree between modules

`settings.EDU_DEFAULT_SUBJECT = "math"` (`settings.py:22`);
`dashboard.py:155` falls back to `{"class": "6eme", "subject": "maths"}`. The
developer's `local_manifest.json` contains **both** `6eme_math` and `6eme_maths` as
separately downloaded courses, so the divergence has already produced duplicate
content on disk. Same class of drift as C9, on a different axis.

### S5-L. `.gitignore` contains bare `main.py` and `config.py` patterns

`.gitignore` lines `main.py` and `config.py` have no leading slash, so they match at
**any** depth — verified in a scratch repo: pattern `main.py` matches `sub/main.py`.
`app_local/main.py` and `app_local/config/settings.py` survive only because they are
already tracked. Any `git rm --cached` + re-add, or a contributor adding
`app_local/core/config.py`, would silently drop the file. Low severity, easy to trip
over.

### S5-M. `pyproject.toml` declares a workspace member that does not exist

`[tool.uv.workspace] members = ["travel-agent-dll"]` (`pyproject.toml:28-29`) — there
is no such directory. `uv sync` tolerates it in 0.6.10, but it is a leftover from a
different project and will confuse anyone reading the manifest. Related: the
`description` field still says *"powered by Gemma 4 via Ollama"* while the default
provider is Gemini.

### S5-N. `lance_driver` imports `weaviate` for one unused helper

`lance_driver.py:6`: `from weaviate.util import generate_uuid5 # Keeping UUID utility
for consistency`. `generate_uuid5` is never called in the file. The effect is that
`weaviate-client` — a cloud vector-DB SDK the local edition does not use — is a hard
import dependency of the local read path. Removing the line would drop a large
dependency from the constrained-hardware install.

### S5-O. `update_block_content` is also uncalled

Missed in my first pass at Section 2b: `block_factory.update_block_content`
(`:184-205`) has no call site either. It is the only function that actually takes
`get_dll_lock` around a content update; the live path (`controller.update_node_content`,
called from `agent.py:78`) takes **no lock at all**, despite the lock machinery
existing at `controller.py:26-32`. So concurrent writes on the live path are
unserialised, and the serialised path is dead.

---

### Things that look deliberate — flagging rather than filing

- **`upsert_local_block` appending.** The inline comment at `lance_driver.py:123`
  (*"Simplified Upsert: we add (we could delete before for the same ID)"*) says the
  author knew. This reads as a knowingly-deferred simplification, not an oversight.
  It is still the largest unbounded-storage risk on the stated hardware target, so I
  have kept it in the severity list — but as a deferred decision, not a surprise.
- **`class_level="local"` / `subject="local"`** at `block_factory.py:157-158` looks
  like an intentional sentinel to keep user-created blocks out of the
  `edu_registry` class/subject filter, rather than a bug. It happens to be
  consistent with `search_block_index` applying no filter to `user_memory`.
- **`break` after the first BMJ promotion** (`controller.py:236`) is commented
  (*"Only promote the first matching DLL node"*) and matches move-to-front semantics.
  Deliberate.
- **The `is_fixed` guard** preventing the four core blocks from being paged out or
  deleted is consistent everywhere it appears. Deliberate and correct.
- **`_head_to_tail_order` / `_tail_to_head_order` being unused.** These may be
  scaffolding for a BMJ implementation that was never finished, rather than dead code
  to delete. Worth deciding which before anyone removes them — see O2.

---

### Open questions — intent, not implementation

**O1. Are `models/gemini-embedding-2` vectors L2-normalised?**
The whole certainty scale hinges on it (C5). If they are, `1 - dist/2` is exactly
cosine similarity and the thresholds are fine; if they are not, certainty is
magnitude-sensitive and unbounded below. I could not determine this without making a
real API call, which is out of scope for this session. Characterising it properly
needs either one live embedding call to inspect the norm, or a documented statement
from the provider. **This is the one open item I could not close with a test.**

**O2. Is the TEU (`app_local/teu/`) meant to be wired in, or is it aspirational?**
Two complete, well-formed `@tool` functions exist and nothing references them. Same
question for `get_head_threshold` and the two traversal helpers: unfinished BMJ
scaffolding to build on, or dead code to delete? The answer changes whether S5-F and
C7 are "bugs to fix" or "code to remove", and I did not want to record a design
decision as a defect.

**O3. Is the service-account-on-device model (C15) a deliberate trade-off?**
`_fetch_json` — an unauthenticated `httpx` path — already exists in the same file and
is used for the manifest, while the catalog and course download use the authenticated
GCS client. That looks like a migration in progress rather than a settled design. If
the bucket is meant to be public-read, the auth path can go entirely, which also
removes `google-cloud-storage` and `google-auth` from the student install.

**O4. Should memory write-back block the response at all?**
`LocalScheduler` exists, is complete enough to run, and is imported in two places
without being used. Was the intent to push the write-back onto it (the "cold path"
of the architecture), and it simply never got connected? This is the single biggest
lever on perceived latency (4 of 6 round trips), so it matters whether the design
already anticipated it.

**O5. Which block-type vocabulary is canonical?**
`cours` vs `course` vs `manual_chapter` (C9). Picking one is a small change; picking
the wrong one silently changes which thresholds and TTLs apply to shipped content.
This needs a decision, not a guess.

---

### Test I did not write

**Streamlit dashboard end-to-end.** `dashboard.py` executes top-to-bottom at import
with 12 `asyncio.run` calls, `st.set_page_config`, `@st.cache_resource`, and
`st.session_state` access at module scope. Driving it requires either
`streamlit.testing.v1.AppTest` with the whole agent stack stubbed, or a live browser
session — comfortably past 60 lines of setup either way, and the parts worth
asserting (`dashboard.py:335` table listing, `:366-368` success message, `:242` sync
crash) are already covered indirectly by unit tests on the functions they call. The
one thing left unverified as a result is the actual rendered UI state.


---

## Section 6 — Prioritized summary

Ordered by severity. "Blast radius" is the surface a fix would touch.
No patches proposed — this is the queue, not the plan.

### P0 — the product does not do the thing it exists to do

**1. L3 course retrieval returns nothing (S5-A).**
*Symptom:* a student downloads the 6ème maths course, asks a 6ème maths question,
and gets an answer generated entirely from the base model. The curriculum on disk is
never consulted. The dashboard's L3 panel says "No tables yet" while
`edu_registry.lance` sits next to it on disk. `load_course_chapter` reports every
chapter as missing.
*Blast radius:* three lines in `lance_driver.py` (`:51`, `:88`, `:115`) plus
`dashboard.py:335`. Small change, but fixing it *exposes* items 3 and 5 below, which
are currently masked — so it should not land alone.
*Pinned:* yes — 3 xfail-strict + 3 passing characterization tests.

**2. BMJ routing is undone by the turn that performs it (S5-C).**
*Symptom:* the DLL ordering the architecture is named after never changes. Memory
routing is a no-op; blocks are always consulted in creation order. Invisible to the
student, fatal to the claim.
*Blast radius:* `agent.py` `planner_node` handle lifetime, and whichever of
`search_memory` / `update_node_content` gets to own persistence. Touches the
controller's save discipline generally — the riskiest fix on this list.
*Pinned:* yes — `test_the_promotion_survives_the_turn` (xfail) plus a passing test
proving the promotion happens and is then reverted.

### P1 — silent data corruption and unbounded growth

**3. `upsert_local_block` appends forever (C2).**
*Symptom:* `user_memory.lance` grows by ~3 rows and ~3 fragments per student turn,
permanently. On a Raspberry Pi with bounded storage this is the failure that
eventually ends the deployment. Once item 1 lands, `get_block_content` also starts
returning an arbitrary historical revision — so a student could be greeted with a
profile from three weeks ago.
*Blast radius:* `lance_driver.upsert_local_block` and `get_block_content`; needs a
delete-then-add or a merge-insert, plus a decision about compacting the existing
store. Flagged as a knowingly-deferred simplification (inline comment at `:123`).
*Pinned:* yes — exact row counts recorded, 1 xfail-strict spec.

**4. Auto-created blocks are corrupt and reported as successful (C3 + C4 + S5-J).**
*Symptom:* Akili offers "💡 Memory Proposal", the student clicks Confirm, sees
"Entry created!", and gets a block that is empty, typeless, and — because of the
zero vector — can never be retrieved by search. It counts against the 5-block cap
forever.
*Blast radius:* the `block_detector` → `auto_execute_block_proposal` contract
(2 files, ~6 lines), plus an embeddings call the executor currently does not make,
plus the discarded return value in `dashboard.py`. Contained but crosses three
modules.
*Pinned:* yes — full observed node and row recorded, 2 xfail-strict specs.

**5. Memory extraction fails silently on ordinary model output (C11 + the parsing
tests).**
*Symptom:* the student says "my name is Marc and I love basketball", the model wraps
its JSON in a sentence of prose, and nothing is saved. The turn looks completely
normal. Next session, Akili does not know who they are. Nine distinct realistic
output shapes are swallowed; one bad key also discards the good keys in the same
response.
*Blast radius:* the parse block at `agent.py:68-80` plus the three `temperature=0.7`
values and the missing JSON response-format on the default gemini branch in
`llm_provider.py`. Self-contained.
*Pinned:* yes — 13 passing cases tabulating exactly what survives, 2 xfail-strict.

### P2 — bounded-resource claims not actually enforced

**6. The dynamic block cap raises instead of evicting (S5-H + S5-G).**
*Symptom:* on the 6th dynamic block the operation dies with a `TypeError`. Because
`auto_execute_block_proposal` swallows exceptions, the student sees "Entry created!"
and no block appears. The "least recently accessed" policy does not exist —
`last_accessed` is never updated by anything.
*Blast radius:* `block_factory.py:140-145` for the crash; adding real access
tracking touches every read path (`cache_l1.get`, `search_memory`,
`update_node_content`) and the DLL schema.
*Pinned:* yes — 2 xfail-strict, exact `TypeError` recorded.

**7. L1 has no cap, no LRU, and no sweep; `_metrics` is separately unbounded (C16).**
*Symptom:* a long-running Streamlit session on a mini-PC grows RAM without bound.
Entries written once and never re-read are never reclaimed at all, since expiry only
fires inside `get()` for that specific id.
*Blast radius:* `cache_l1.py` only. The cleanest fix on the list.
*Pinned:* yes — 1 xfail-strict plus measured growth (1000 misses → 1000 metrics rows,
0 cache entries).

**8. Six network round trips per turn, all blocking (C10 + C6 + C8).**
*Symptom:* on a weak connection the student waits through four round trips that have
nothing to do with the answer they are reading. With no connection, the turn fails at
the *first* statement — before L1, L2 or L3 is consulted — so there is no degraded
offline mode at all, which contradicts the project's stated premise.
*Blast radius:* large. Needs an embeddings factory in `llm_provider.py` (does not
exist), a local embedding provider, an offline branch in `planner_node`, and the
write-back moved onto `LocalScheduler` (which exists and is already imported in two
places, unused). This is a design change, not a bug fix — and O1/O4 should be
answered first.
*Pinned:* partially — offline failure point is pinned exactly; the six-round-trip
count is recorded in Section 1 but not asserted by a test.

### P3 — install, tooling and correctness-adjacent

**9. `pip install -r requirements.txt` produces a non-working environment (C13 / S5-E).**
*Symptom:* `ModuleNotFoundError: lancedb`. Only bites the pip user; `uv sync` (the
README's actual instruction) works.
*Blast radius:* regenerate one file. Trivial.
*Pinned:* no — asserted by inspection in Section 2d, no test.

**10. "Check for Updates" throws after the first successful sync (S5-B).**
*Symptom:* Streamlit traceback in the sidebar, every press after the first.
*Blast radius:* one uninitialised variable; or delete the inline re-implementation
and call the already-correct `download_prompts`. Trivial.
*Pinned:* yes — 3 passing tests, including the steady-state path.

**11. System prompt sent as `HumanMessage` (C17).**
*Symptom:* weaker instruction-following; the Socratic persona is easier for a student
to talk the model out of, since it arrives as user content. Two consecutive user
messages with no assistant turn between.
*Blast radius:* one line (`agent.py:167`). Behavioural change to model output, so it
wants an eval, not just a test.
*Pinned:* yes — 1 passing test asserting current behaviour.

**12. Block-type vocabulary drift (C9 + S5-K).**
*Symptom:* the per-type threshold and TTL tables never apply to shipped course
content (`manual_chapter` matches nothing); `math` vs `maths` has already produced
duplicate downloads in the developer's own manifest.
*Blast radius:* wide but shallow — a shared vocabulary constant, the cloud pipeline,
and a migration for existing rows. Needs decision O5 first.
*Pinned:* yes — the fall-through is characterized in 3 passing tests.

**13. `delete_local_block` does not exist (C1).**
*Symptom:* none today — `delete_block_stitching` is unreachable. Would be an
`AttributeError` the moment block deletion is wired up.
*Blast radius:* one function.
*Pinned:* no — no test, since the call site is unreachable.

**14. `sys.path` mutation × 8 and no packaging (C14).**
*Symptom:* none at runtime; blocks installability, breaks tooling that imports
modules outside the repo root.
*Blast radius:* add `__init__.py` files and a build backend, remove 8 lines. Touches
every module's import block.
*Pinned:* yes — the 8 sites and the absence of `__init__.py` are both asserted.

**15. Service-account key on every student device (C15).**
*Symptom:* none functionally. Security posture: one leaked key exposes the whole
registry, with no per-device revocation.
*Blast radius:* depends entirely on O3.
*Pinned:* yes — 1 test asserting the auth path has no anonymous branch.

**16. f-string SQL predicates (C18); TEU unwired (S5-F); `agent.py` unimportable
without a key (S5-D); `weaviate` imported for an unused helper (S5-N);
`update_node_content` takes no lock while the locked path is dead (S5-O);
`.gitignore` bare filename patterns (S5-L); phantom workspace member (S5-M);
README divergences (C19).**
Low severity individually. Items S5-D and S5-N are the two most worth doing early:
both are small, and both make everything else easier to test and lighter to install.

---

### Not defects

- Paging out the last remaining node leaves a consistent empty DLL. I expected this
  to be broken; it is correct. Now pinned.
- `asyncio.Lock` reuse across the dashboard's repeated `asyncio.run` calls. I
  expected a cross-event-loop `RuntimeError` on Python 3.13; there is none.
- `move_to_front` pointer surgery is correct for head, middle and tail positions,
  including repeated and idempotent moves — 22 passing invariant tests.
- The `is_fixed` guard is consistent everywhere it appears.
- `auto_execute_block_proposal` works correctly when given a correctly-shaped
  proposal. The executor is sound; only the contract is broken.

---

*End of findings. Suite: 98 passed, 14 xfailed, 0 failed. No module under
`app_local/`, `cloud_registry/` or `llm_provider.py` was modified; `pyproject.toml`
gained only a dev dependency group and a pytest config stanza; `.env` was neither
read nor written.*
