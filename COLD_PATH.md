# Cold path proposal — moving the memory write-back off the hot path

**Status: measured and prototyped, deliberately not shipped.**
The write-back stays synchronous and inline in `planner_node`. This document
records what the change is worth, and what shipping it would cost, so the
decision can be taken on numbers rather than on intuition.

---

## The measurement

`scripts/bench_latency.py` instruments each stage of a turn and drives the real
graph against the real model. Eight turns, distinct questions so nothing is
served from a cache, `gemma-4-26b-a4b-it` via Google AI Studio.

| Stage | Median | Min | Max | Nature |
|---|---|---|---|---|
| Query embedding | 15.7 ms | 4.4 ms | 61.8 ms | local |
| DLL load (L2) | 0.5 ms | 0.1 ms | 7.9 ms | local |
| L3 search + BMJ routing | 11.8 ms | 9.9 ms | 27.1 ms | local |
| Memory write (L3) | 54.5 ms | 35.6 ms | 72.2 ms | local |
| **Main LLM** | **19.0 s** | 17.0 s | 24.5 s | network |
| **Extraction LLM** | **9.9 s** | 9.5 s | 11.8 s | network |

**Turn: 29.0 s median (24.4 – 32.3).** All local work totals **0.25 s — 0.9 %**.

The architecture is not the cost. The two LLM calls are 99.1 % of the turn, and
one of them produces nothing the student sees.

## What the change is worth

The extraction call re-reads the exchange to decide what to remember. It is
awaited before `planner_node` returns, so the answer is held for its full
duration. Nothing requires that: a student does not need their profile updated
in order to read the explanation they just asked for.

Prototyped and re-measured under the same protocol:

| | Inline (shipped) | Cold path (prototype) | Δ |
|---|---|---|---|
| Perceived turn | 29.03 s | **18.98 s** | **−34.6 %** |
| Observed range | 24.4 – 32.3 s | 17.0 – 24.5 s | |
| Local work | 0.25 s | 0.12 s | |

**Ten seconds per question**, with no feature removed. The extraction still runs
and still costs 9.9 s — it stops being on the student's critical path.

## Why it is not shipped

The prototype worked, and building it surfaced three consequences that are not
visible from the design sketch. None is a blocker; together they are more than a
latency optimisation should carry without a deliberate decision.

### 1. Threads, not asyncio tasks

Streamlit drives each interaction through its own `asyncio.run(...)`. The loop is
torn down the moment the graph returns, so a fire-and-forget `create_task` is
cancelled before it runs. The cold path has to be a **daemon thread with its own
event loop**, which outlives the request that queued the work.

That is a real concurrency model added to a codebase that currently has none.

### 2. The handler must reload the DLL, not carry the turn's handle

Passing `planner_node`'s `dll` object across a thread boundary reintroduces the
exact mechanism behind S5-C: two in-memory views of one state, and whichever
writes last wins. The handler has to `load_dll()` fresh — the turn is over by
then and its BMJ promotion is already persisted, so this is correct, but it makes
the write a read-modify-write against a file another turn may be touching.

### 3. Failures surface a turn late

`memory_problems` is currently returned in the turn that produced it, and the
dashboard renders it immediately. Off the hot path the answer returns before the
extraction has run, so a failed write is reported on a later rerun. Acceptable,
but it weakens the B2 guarantee — *a student whose memory silently stopped
updating has no way to know* — from "in this turn" to "shortly after".

### 4. Tests have to wait for it, and the wait is load-bearing

The background thread reads `settings.METADATA_LINKS_PATH` **at execution time**.
If a write is still in flight when a test's `monkeypatch` restores the real
paths, it lands in the developer's real store. The prototype needed a
`scheduler.drain()` in `akili_paths` teardown, before the restore, that raises if
the queue does not empty. Two tests caught this immediately — which is
reassuring, and also the sign that this change has sharper edges than its
one-line description suggests.

## The prototype

Below is the code that produced the 18.98 s measurement. It ran, and the full
suite passed against it. It is reproduced here rather than shipped so the
decision stays open without the work having to be redone.

### 1. `app_local/core/scheduler.py` — replaces the current stub

```python
"""
Cold path — work that must happen, but that the student must not wait for.

Threads, not asyncio tasks. Streamlit drives each interaction through its own
`asyncio.run(...)`, so the loop is torn down the moment the graph returns — a
fire-and-forget `create_task` would be cancelled before it ran. A daemon thread
with its own loop outlives the request that queued the work.
"""

import asyncio
import os
import queue
import sys
import threading
import time
from typing import Any, Callable, Dict, List

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from logger import get_logger

logger = get_logger(__name__)


class LocalScheduler:
    """Single background worker consuming a FIFO of tasks."""

    def __init__(self) -> None:
        self._queue: "queue.Queue" = queue.Queue()
        self._handlers: Dict[str, Callable] = {}
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._problems: List[str] = []

    def register(self, task_type: str, handler: Callable) -> None:
        """Bind an async handler to a task type."""
        self._handlers[task_type] = handler

    def submit(self, task_type: str, payload: Dict[str, Any]) -> None:
        """Queue a task and return immediately. Starts the worker on demand."""
        self.start()
        self._queue.put((task_type, payload))

    def start(self) -> None:
        """Idempotent. Safe to call from any thread."""
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._thread = threading.Thread(
                target=self._run, name="akili-cold-path", daemon=True
            )
            self._thread.start()

    def _run(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        while True:
            task_type, payload = self._queue.get()
            try:
                handler = self._handlers.get(task_type)
                if handler is None:
                    logger.warning("Scheduler: no handler for %s", task_type)
                    continue
                result = loop.run_until_complete(handler(payload))
                if result:
                    # Handlers return a list of problems; keep them for the UI,
                    # which now learns about a failed write on a later rerun
                    # rather than in the turn that caused it.
                    self._problems.extend(result)
            except Exception as e:
                logger.error("Scheduler: %s failed: %s", task_type, e)
                self._problems.append(f"background task {task_type} failed: {e}")
            finally:
                self._queue.task_done()

    def drain(self, timeout: float = 30.0) -> bool:
        """
        Block until the queue is empty. For tests and for shutdown — never on
        the request path, which is the whole point of this class.
        """
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if self._queue.unfinished_tasks == 0:
                return True
            time.sleep(0.02)
        return self._queue.unfinished_tasks == 0

    def take_problems(self) -> List[str]:
        """Problems recorded since the last call, then cleared."""
        out, self._problems = self._problems, []
        return out

    @property
    def pending(self) -> int:
        return self._queue.unfinished_tasks


scheduler = LocalScheduler()
```

### 2. `app_local/runtime/agent.py` — the handler and the submit

```python
from app_local.core.scheduler import scheduler

MEMORY_TASK = "MEMORY_WRITEBACK"


async def _memory_writeback_task(payload: dict):
    """
    Cold-path handler. Reloads the DLL instead of using the turn's handle: the
    turn has ended, its BMJ promotion is already on disk, and holding a stale
    snapshot across a thread boundary is how S5-C happened in the first place.
    """
    dll = await controller.load_dll()
    return await _update_student_memory(
        payload["user_query"], payload["agent_response"], dll
    )


scheduler.register(MEMORY_TASK, _memory_writeback_task)
```

Then, in `_generate`, this:

```python
    # Final answer — write memory back once, on the completed turn.
    memory_problems = await _update_student_memory(user_query, response.content, dll)
```

becomes this:

```python
    # Final answer. The memory write-back costs a SECOND LLM call — measured at
    # 9.9 s median, 34% of a 29 s turn — and produces nothing the student sees.
    # It goes to the cold path so the answer is not held hostage to it.
    scheduler.submit(MEMORY_TASK, {
        "user_query": user_query,
        "agent_response": response.content,
    })
```

and the returned state carries problems from write-backs that have *finished*,
rather than from this turn:

```python
        "memory_problems": scheduler.take_problems(),
```

### 3. `tests/conftest.py` — the drain that is not optional

Without this, a write still in flight when `monkeypatch` restores the real
settings paths lands in the developer's real store. Two tests caught it on the
first run.

```python
    yield {"db": str(db_path), "meta": str(meta_path), "root": tmp_path}

    # Drain the cold path BEFORE monkeypatch restores the real settings paths.
    # The background thread reads settings at execution time, so a write still
    # in flight when the redirect is undone would land in the developer's real
    # store instead of tmp_path.
    from app_local.core.scheduler import scheduler
    if not scheduler.drain(timeout=15):
        raise RuntimeError("cold path did not drain; a write may leak to real paths")
    scheduler.take_problems()
```

### 4. Tests asserting a write-back must wait for it

```python
    out = await agent.planner_node(_state())

    # The write-back is on the cold path now: the answer returns before the
    # extraction LLM has been called at all. That is the point (34% of turn
    # latency), so the test waits for it explicitly rather than assuming.
    from app_local.core.scheduler import scheduler
    assert scheduler.drain(timeout=15)

    assert extractor.calls == 1
```

`scripts/bench_latency.py` needs the same treatment between runs, so turns stay
independent — and that wait must not be counted in the turn.

Roughly 150 lines including tests.

## The other 19 seconds

The main LLM call is the remaining cost, and it is not architectural. Gemma 4
emits reasoning blocks before its answer — pedagogically useful, expensive in
time. It is tuned by choosing a model, which the system already supports through
`LLM_PROVIDER`, including a locally hosted one that would remove the network from
the turn entirely.

Re-run `scripts/bench_latency.py` after any inference-model change. The numbers
above are only true for the model they were measured on — the same caveat that
applies to `CERTAINTY_THRESHOLDS` and the embedding model.
