"""
Per-stage latency of one student turn, over N runs.

Run it after changing the inference model — the numbers in the architecture
notes are only true for the model they were measured on.

    uv run python scripts/bench_latency.py 8

Wraps the real functions with timers and drives the real graph against the real
model. Distinct questions each run so nothing is served from a cache.
"""
import asyncio, statistics, sys, time

import llm_provider
from langchain_core.messages import HumanMessage
from app_local.mmu import controller
import app_local.runtime.agent as agent

QUESTIONS = [
    "Explique-moi les fractions.",
    "Comment additionner deux fractions de dénominateurs différents ?",
    "Je ne comprends pas les nombres décimaux, aide-moi.",
    "À quoi sert de simplifier une fraction ?",
    "Quelle est la différence entre un angle aigu et un angle obtus ?",
]

T = {}


def _timed(name):
    def deco(fn):
        async def wrapper(*a, **k):
            t0 = time.perf_counter()
            try:
                return await fn(*a, **k)
            finally:
                T.setdefault(name, []).append(time.perf_counter() - t0)
        return wrapper
    return deco


async def main(n):
    # ── instrument the real call sites ───────────────────────────────────────
    emb = llm_provider.get_embedder()
    real_embed = emb.aembed_query
    emb.aembed_query = _timed("embedding")(real_embed)

    real_search = controller.search_memory
    controller.search_memory = _timed("recherche L3 + BMJ")(real_search)

    real_load = controller.load_dll
    controller.load_dll = _timed("chargement DLL (L2)")(real_load)

    real_update = controller.update_node_content
    controller.update_node_content = _timed("ecriture memoire (L3)")(real_update)

    main_llm = agent._get_llm()
    extractor = agent._get_extractor_llm()

    class Wrap:
        def __init__(self, inner, label):
            self.inner, self.label = inner, label
        async def ainvoke(self, m):
            t0 = time.perf_counter()
            try:
                return await self.inner.ainvoke(m)
            finally:
                T.setdefault(self.label, []).append(time.perf_counter() - t0)

    agent._llm = Wrap(main_llm, "LLM principal (reseau)")
    agent._extractor_llm = Wrap(extractor, "LLM extraction (reseau)")

    graph = agent.create_agent_graph()
    totals = []

    for i in range(n):
        q = QUESTIONS[i % len(QUESTIONS)]
        t0 = time.perf_counter()
        await graph.ainvoke({
            "messages": [HumanMessage(content=q)],
            "agent_id": "bench", "class_level": "6eme", "subject": "math",
            "memory_only_mode": False, "needs_new_block": "False",
            "proposed_block_config": {},
        })
        totals.append(time.perf_counter() - t0)
        print(f"  tour {i+1}/{n} : {totals[-1]:6.2f} s", flush=True)

    # ── report ───────────────────────────────────────────────────────────────
    print("\n%-26s %8s %8s %8s %7s %7s" % ("ETAPE", "median", "min", "max", "n", "% tour"))
    print("-" * 70)
    med_total = statistics.median(totals)
    order = ["embedding", "chargement DLL (L2)", "recherche L3 + BMJ",
             "LLM principal (reseau)", "LLM extraction (reseau)",
             "ecriture memoire (L3)"]
    local = 0.0
    for name in order:
        v = T.get(name)
        if not v:
            continue
        # per-turn cost: some stages fire more than once per turn
        per_turn = sum(v) / len(totals)
        med = statistics.median(v)
        print("%-26s %7.1f%s %7.1f%s %7.1f%s %7d %6.1f%%" % (
            name,
            med * 1000 if med < 1 else med, "ms" if med < 1 else "s ",
            min(v) * 1000 if med < 1 else min(v), "ms" if med < 1 else "s ",
            max(v) * 1000 if med < 1 else max(v), "ms" if med < 1 else "s ",
            len(v), 100 * per_turn / med_total))
        if "reseau" not in name:
            local += per_turn

    print("-" * 70)
    print("%-26s %7.2fs  (min %.2f / max %.2f)" % (
        "TOUR COMPLET", med_total, min(totals), max(totals)))
    print("%-26s %7.2fs  %5.1f%% du tour" % ("dont local", local, 100 * local / med_total))
    print("%-26s %7.2fs  %5.1f%% du tour" % (
        "dont reseau", med_total - local, 100 * (med_total - local) / med_total))


if __name__ == "__main__":
    asyncio.run(main(int(sys.argv[1]) if len(sys.argv) > 1 else 5))
