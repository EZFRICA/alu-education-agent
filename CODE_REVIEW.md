# Akili — Code review of the current working tree

Second pass, after the fixes recorded in `REVIEW_FINDINGS.md`. Nothing is committed;
`HEAD` is `dfd7d70` and all work below sits in the index.

**Final suite: 193 passed / 0 xfailed / 0 failed.** Every one of the 14 xfail
markers from the audit is gone — the defects they described are fixed, not
re-scoped. Stable across consecutive runs, per file in isolation, and with no
`.env` and no API key present.

This pass reviews **my own changes as well**, which is where the first three
findings come from.

---

## Status

| # | Finding | Severity | Status | Evidence |
|---|---|---|---|---|
| R1 | `list_tables()` bug survives at a 5th site | high | **fixed** | `sync_manager.py:164` |
| R2 | Course download neither writes nor checks the embedding stamp | high | **fixed** | `sync_manager.py:119-194` |
| R3 | One stamp for two tables — a same-width model swap is undetected on `edu_registry` | medium | **fixed** — stamped per table | `lance_driver.py:read_stamp` |
| R4 | A stale table blocks the whole read path, not just itself | medium | **confirmed as designed** | `lance_driver.py:verify_embedding_space` |
| R5 | Dashboard reported success for a failed proposal | medium | **fixed** | `dashboard.py` |
| R6 | Zero vector written into a searchable index | medium | **fixed** — embeds or refuses | `block_factory.py` |
| R7 | Extraction all-or-nothing per turn, failures invisible | medium | **fixed** | `core/extraction.py` |
| R8 | Block cap raised `TypeError`; accesses never recorded | medium | **fixed** | `block_factory.py`, `controller.record_access` |
| R9 | L1 unbounded, `_metrics` unbounded, no sweep | medium | **fixed** — LRU cap 256 | `cache_l1.py` |
| R10 | `sync_with_registry` raised `UnboundLocalError` | medium | **fixed** — duplicate deleted | `sync_manager.py` |
| R11 | System prompt sent as `HumanMessage` | low | **fixed** — needs manual eval | `agent.py` |
| R12 | LLMs built at module scope | low | **fixed** — lazy | `agent.py` |
| R13 | Function-calling support for the configured model | — | **closed** | model supports it natively; `bind_tools` accepted |
| R14 | Credential was invalid (401) — now replaced and working end to end | — | **closed** | live 200; tool call round-tripped |
| R15 | Key *format* validation is unfixable and was wrong twice | low | **fixed** — presence check only | `llm_provider.py` |
| R16 | **Retrieval thresholds rejected every real result, including exact matches** | **high** | **fixed** — recalibrated | `settings.CERTAINTY_THRESHOLDS` |

---

## R1 — The `list_tables()` defect survives at a fifth site

`app_local/sync/sync_manager.py:164`:

```python
all_tables = db.list_tables()
if "edu_registry" not in all_tables:
```

Batch A routed four sites through `lance_driver.list_table_names()`
(`lance_driver.py` ×3, `dashboard.py` ×1). **This one was missed** — the original
audit enumerated the four and I did not re-scan `sync/` afterwards.

Verified: `"edu_registry" not in db.list_tables()` is `True` even when the table
exists, so the condition is always taken and the `else` branch at `:173-177` is
**dead code**. Downloads only work because of the `try/except` fallback added in
commit `c8fdb2f` — the same papering-over the audit flagged as a symptom of S5-A.

Consequence today: a second download of the same course takes the exception path
rather than the intended one. Same outcome, but the "replace old entries for this
class/subject" logic in the live branch never runs as written.

Fix: one line, route through `lance_driver.list_table_names(db)`. No test covers
`download_course` end to end because it needs GCS; a test can cover the branch
selection with a fake db.

## R2 — `download_course` neither writes nor verifies the embedding stamp

E3 specified the stamp be written "when a course is downloaded **or** a user block
is written". Only the second half was implemented — `write_stamp()` is called from
`upsert_local_block` and from `migrate_embeddings.py`, and from nowhere in `sync/`.

Two gaps follow:

1. **The manifest stamp is published but never read.** `batch_pipeline` now writes
   `manifest["embedding"] = {"model": ..., "dim": ...}`, and no client code looks
   at it. The refusal only fires later, at search time, from the Arrow schema.
2. **A device that only downloads courses never gets a stamp file at all** — it
   is written on the first *memory write*, which happens after the first turn.

The dimension check still catches the dangerous case, so this is not a
correctness hole today. It is a missed opportunity to refuse at the moment of
download, with the manifest in hand, instead of at the student's first question.

Fix: read `manifest["embedding"]` in `download_course` and refuse before
importing the parquet; call `write_stamp()` after a successful import.

## R3 — One stamp, two tables

`read_stamp()` records a single model id for the whole store, but there are two
independently-sourced tables: `edu_registry` (from the cloud) and `user_memory`
(local). Scenario that slips through:

- change `EMBEDDING_MODEL` to a different model of the **same width**
- run `migrate_embeddings.py` — `user_memory` is re-embedded, stamp updated
- do **not** re-download courses

`edu_registry` is now stale, the dimension matches, and the stamp says the
configured model. No refusal, and course retrieval silently degrades — precisely
the failure E3 exists to prevent, in the one case the dimension check cannot see.

Fix: stamp per table (a small JSON keyed by table name), and have
`download_course` write the registry's stamp from the manifest.

## R4 — A stale table blocks everything, not just itself

`verify_embedding_space` is called per table inside the search loop, and raising
aborts the whole call. So a stale `user_memory` also stops `edu_registry` from
being searched, and vice versa.

I believe this is right — a partial answer that silently omits half the memory
hierarchy is worse than a refusal — but it is a deliberate trade and should be a
conscious one. The alternative is to skip the offending table with a warning and
return what is usable.

## R5 / R6 — Proposal path (Batch B1, not started)

`dashboard.py:366` still discards the executor's return value:

```python
asyncio.run(auto_execute_block_proposal(proposal))
st.session_state.pending_block_proposal = None
st.success("Entry created!")
```

`auto_execute_block_proposal` returns `False` on failure, and the UI says
"Entry created!" either way. Combined with R6 — `create_dynamic_block` still
writes `[0.0] * EMBEDDING_DIM` when no vector is supplied, and the executor never
supplies one — a confirmed proposal writes an unretrievable row and reports
success.

The `768` literal is now `settings.EMBEDDING_DIM`, so the vector is at least the
right *width*. It is still a zero vector in a searchable index.

Pinned by `test_a_created_block_is_retrievable_by_search` and
`test_a_detector_proposal_lands_in_the_dll_and_lancedb` (both xfail).

## R7 — Extraction remains all-or-nothing (Batch B2, not started)

`agent.py:109-121`: the `for` loop over `updates.items()` is inside the `try`, so
one bad key discards every good key in the same turn, and the whole failure is a
single `logger.error` the student never sees. Nine realistic malformed shapes are
tabulated in `tests/test_extractor_parsing.py`.

Unchanged by this phase, and now slightly more consequential: with tools wired,
the extractor sees a longer, tool-augmented exchange.

## R8 / R9 — Bounded-resource claims (Batch C, not started)

`block_factory.py:143` still does
`min(dynamic_nodes, key=lambda x: x.get("last_accessed", "1970-..."))`, which
returns the stored `None` and raises `TypeError` comparing `None < None`.
`last_accessed` and `access_count` are still written at creation and updated by
no code path.

`cache_l1` still has no entry cap, no LRU and no sweep; `_metrics` still grows on
every distinct id ever queried, including pure misses.

Both are the explicit product requirements ("bounded RAM", "bounded storage
growth") and both remain unmet.

## R10 — `sync_with_registry` still raises (Batch D)

`sync_manager.py:251` assigns `updated` two `if`s deep and `:256` reads it
unconditionally. The steady state — everything already up to date — raises
`UnboundLocalError`. The dashboard's "Check for Updates" button hits this.

Note `download_prompts` at `:197` is a correct implementation of the same logic
and is never called.

## R11 / R12 — Batch D leftovers

The system prompt is still passed as `HumanMessage` (`_generate`). With tool
calling now in play this also means the first message of a tool-augmented
exchange is a human turn carrying instructions, which some providers handle less
well than a proper system message.

`agent.py:20-21` still builds both LLMs at import. Since `llm_provider` now
*raises* on an unknown `LLM_PROVIDER`, a typo in `.env` stops the app from
starting rather than failing at the first question. Loud, but earlier than ideal.

## R13 — Tool binding: closed

`gemma-4-26b-a4b-it` supports function calling natively (it is an
instruction-tuned Gemma 4; MoE 26B total / 4B active per token, which is also why
it suits the constrained-hardware thesis). Verified on this side that
`llm.bind_tools(get_tools())` is accepted by `langchain-google-genai` for this
model — binding succeeded; the subsequent request failed on authentication, not
on tools. See R14.

## R14 — Credential: closed, and the tool loop is proven end to end

The original credential was rejected `401 ACCESS_TOKEN_TYPE_UNSUPPORTED` in both
auth modes (query param and bearer), verified against the REST endpoint with
langchain bypassed — so neither a library issue nor interference from the
`gcloud auth application-default login` used for the registry sync.

Replaced by the operator. Now verified working, against the live API:

```
model: gemma-4-26b-a4b-it | bind_tools: OK
tool_calls: [{'name': 'calculate',
              'args': {'expression': '(3/4 + 2/5) * 20'}, ...}]
resultat de l'outil: 23
```

The model selected the right tool, produced a well-formed argument, and the TEU
returned the exact answer. **This is the first end-to-end confirmation of the
inference path in this whole effort** — everything before it was test-verified
only.

## R15 — Key format cannot be validated, and I got it wrong in both directions

`_GEMINI_KEY_VALID` originally checked `len > 20` while its own comment claimed
*"starts with AIza"*. I "fixed" it to enforce the comment — and that was **wrong**:
the working key is 53 characters starting `AQ.A`, so my stricter check would have
rejected a valid credential.

The underlying reason is that the check cannot work at all:

- AI Studio issues keys as both `AIza…` (~39 chars) and `AQ.…` (~53 chars); both
  were verified returning `200`.
- A short-lived OAuth access token is *also* `AQ.…` and is not usable here.

So a valid key and an expired token are **indistinguishable by shape**. Any
prefix or length rule produces false verdicts in one direction or the other.

Now a presence check only, with the reasoning recorded in the code so nobody
reintroduces a shape rule. The warning fires when the key is *absent* and the
provider needs one, and stays silent for a working key — both confirmed.

---

## What is in good shape

Stated plainly, because a review that only lists problems is misleading about
where the risk is.

- **Single source of truth for embeddings.** `embedding_config.py` is read by
  both the client and the pipeline; `test_client_and_pipeline_read_the_same_embedding_config`
  asserts object identity and a regex test forbids reintroducing a literal in
  `cloud_registry/config/settings.py`. This is the defect that produced
  `dim=3072` and it is structurally closed.
- **The read path is genuinely offline.** Verified with the real ONNX model and
  sockets blocked, not with a stub. ~2.4 ms per query embedding.
- **Semantic quality is measured, not assumed.** French paraphrase 0.911 vs
  unrelated -0.007, plus a cross-lingual case — this is what catches a broken
  tokenizer or pooling config.
- **Normalisation is symmetric.** One implementation in `embedding_config`,
  applied by the pipeline before publishing and by the client on query and local
  write. Published parquets verified at norm 1.0.
- **The TEU is local-only and structurally enforced.** An AST test forbids any
  LLM SDK import and any hardcoded model id in `teu/tools.py`; `calculate` is
  AST-interpreted rather than `eval()`, with hostile-input tests.
- **Test isolation was a real problem and is fixed.** `stub_embeddings` used to
  patch the factory, which permanently froze a stale stub into any module
  imported during the patch window. Tests were passing while talking to a
  previous test's stub.

---

## Recommended order

1. **R1** — one line, and it is the same class of defect that made the L3 tier
   inert for months.
2. **R2 + R3** — completes E3. Without them the stamp is written but never read,
   and one realistic migration path is undetected.
3. **Batch B** (R5, R6, R7) — the proposal contract and silent extraction
   failures are both user-visible.
4. **Batch C** (R8, R9) — the bounded-resource requirements.
5. **Batch D** (R10, R11, R12) — low risk, land together.
6. **R13** — confirm on first launch; no code change unless it fails.

---

## R16 — The retrieval thresholds rejected everything, including the exact match

**Found only by running against the real model and real course content.** Every
test in the suite uses hand-built vectors scoring 1.0 or 0.0, which clear or fail
any threshold trivially. Real embeddings do not behave that way.

Measured on a fresh install, 6eme/math downloaded from the live registry, query
*"Explique-moi les fractions."*:

| Chapter | Certainty | Old threshold | Verdict |
|---|---|---|---|
| `chapter_1_simple_fractions` | **0.670** | 0.70 | **rejected** |
| `chapter_2_decimal_numbers` | 0.507 | 0.70 | rejected |
| `chapter_3_angles_and_geometry` | 0.284 | 0.70 | rejected |
| `current_session` (DLL node) | 0.693 | 0.80 (`temp`) | rejected |

The **ranking is exactly right** — the correct chapter leads, the related one
follows, the unrelated one trails. Nothing was wrong with retrieval. The cut-off
was calibrated for a scale this embedding model never produces, so
`COURSE CONTEXT` reached the prompt **empty**, on a system where S5-A, the
`list_tables` defect, normalisation and dimension stamping had all been repaired.

In other words: the RAG was still dead after every previous fix, for a fourth
independent reason, and no test caught it.

Recalibrated in `settings.py`, preserving the original ordering
(fondamental < cours < temp) and with the measurements recorded next to the
values:

```python
CERTAINTY_THRESHOLDS = {
    "fondamental":    0.40,
    "cours":          0.45,
    "manual_chapter": 0.45,   # what shipped course rows actually carry
    "temp":           0.50,
}
MIN_RELEVANCE_CERTAINTY = 0.45
```

`manual_chapter` is now an explicit key instead of falling through to the
default — that is the C9 vocabulary drift showing up in production, though
renaming the literals remains out of scope (O5).

**These values are model-dependent.** Changing `EMBEDDING_MODEL` requires
re-measuring them, and that is stated in the code.

Verified after the change, same fresh install and query: **2020 characters** of
course context injected, `chapter_1_simple_fractions` and
`chapter_2_decimal_numbers` retained, the geometry chapter correctly dropped.

Tests updated with reason: `test_course_blocks_below_threshold_are_filtered_out`
and `test_the_typeless_node_matches_no_ttl_and_no_threshold` both pinned the old
numbers.

> **Open question.** DLL memory nodes (`current_session`) surface inside the
> `COURSE CONTEXT` block, because `search_block_index` merges `edu_registry` and
> `user_memory` into one result list. It is not wrong — the content is relevant —
> but the prompt labels student memory as course material, and there is a
> separate `STUDENT MEMORY` section right below it. Intentional or accidental?

---

## Final state

All findings are closed. The 14 xfail markers from the original audit are gone,
each because the defect it described was fixed.

### Verified end to end, on a clean checkout

Not "the tests pass" — a fresh directory with no `.env`, no model cache and no
local data, walked through the README:

| Step | Result |
|---|---|
| `scripts/fetch_embedding_model.py` | 240 MB in 54 s, cache-only load verified |
| Remote catalog from GCS | 6 courses across 2 levels |
| `download_course("6eme","math")` | 3 chapters, 384 dims, stamped |
| Full turn, real Gemma | `SystemMessage`, 1926 chars of course context, answer in 28 s |
| Tool call, real Gemma | chose `calculate("(3/4 + 2/5) * 20")` → `23` |
| Memory write-back | `current_session` updated, `memory_problems` empty |
| Access recording | `access_count` incremented on the read path |

The registry on `gs://akili-registry` was regenerated and republished at 384
dimensions with an embedding stamp.

### R4, confirmed as designed

A table whose vectors are in the wrong space aborts the whole search rather than
being skipped. A partial answer that silently omits half the memory hierarchy is
worse than a refusal the operator can read, and the refusal names the exact
command that fixes it. Recorded here so it reads as a decision, not an oversight.

### What still needs a human

- **`SystemMessage` changes model behaviour.** No test can show the answers got
  better, only that the correct message type is sent. Worth an eyeball on answer
  quality before this reaches students.
- **~28 s per turn**, almost entirely Gemma's thinking blocks. Local embedding is
  2.4 ms and retrieval is instant. Acceptable or not is a product call.
- **DLL nodes still surface inside the `COURSE CONTEXT` block** (see R16's open
  question) — `search_block_index` merges both tables, so student memory is
  labelled as course material even though a `STUDENT MEMORY` section follows.

### Deliberately out of scope

Issues, not patches: the cold path / `LocalScheduler` (O4), degraded offline
generation, the service-account model (O3), the block-type vocabulary migration
(O5), and `delete_local_block` — called from `delete_block_stitching`, which has
no reachable call site of its own.
