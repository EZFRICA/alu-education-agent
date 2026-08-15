"""
The real ONNX embedder — semantics, not plumbing.

Target: llm_provider.LocalOnnxEmbedder, embedding_config

Skipped when the model cache is absent, so the suite still runs on a machine
that has not fetched the 240MB model.
"""

import pytest

from app_local.storage import lance_driver


async def test_a_paraphrase_ranks_above_an_unrelated_sentence(real_local_embedder):
    """
    The failure this catches is a broken tokenizer or the wrong pooling config,
    which does not raise -- it just produces mediocre results that look like the
    model being weak. Multilingual French, because that is the deployment target.
    """
    query = "Comment additionner deux fractions ?"
    paraphrase = "De quelle manière fait-on la somme de deux fractions ?"
    unrelated = "Le chat dort sur le tapis devant la cheminée."

    vectors = await real_local_embedder.aembed_documents(
        [query, paraphrase, unrelated]
    )
    q, para, other = (lance_driver.normalize_vector(v) for v in vectors)

    def cosine(a, b):
        return sum(x * y for x, y in zip(a, b))

    sim_paraphrase = cosine(q, para)
    sim_unrelated = cosine(q, other)

    assert sim_paraphrase > sim_unrelated, (
        f"paraphrase {sim_paraphrase:.3f} did not outrank unrelated "
        f"{sim_unrelated:.3f} -- suspect tokenizer or pooling"
    )
    assert sim_paraphrase > 0.6, f"paraphrase similarity only {sim_paraphrase:.3f}"


async def test_cross_lingual_pairs_are_closer_than_unrelated(real_local_embedder):
    """The model is multilingual; the curriculum mixes French and English."""
    vectors = await real_local_embedder.aembed_documents([
        "The student is learning fractions.",
        "L'élève apprend les fractions.",
        "The weather is cold today.",
    ])
    en, fr, other = (lance_driver.normalize_vector(v) for v in vectors)

    def cosine(a, b):
        return sum(x * y for x, y in zip(a, b))

    assert cosine(en, fr) > cosine(en, other)


async def test_the_embedder_produces_the_configured_dimension(real_local_embedder):
    import embedding_config

    vector = await real_local_embedder.aembed_query("vérification")
    assert len(vector) == embedding_config.EMBEDDING_DIM


async def test_embedding_is_deterministic(real_local_embedder):
    """Two calls on the same text must give the same vector, or nothing caches."""
    a = await real_local_embedder.aembed_query("les nombres relatifs")
    b = await real_local_embedder.aembed_query("les nombres relatifs")
    assert a == pytest.approx(b)


def test_the_client_embedder_never_downloads():
    """
    The client path must be cache-only: a download attempt on a disconnected
    classroom device hangs at the first student question instead of failing.
    """
    import llm_provider

    with pytest.raises(RuntimeError, match="not present in"):
        llm_provider.LocalOnnxEmbedder(
            model_name="sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
            cache_dir="/nonexistent/cache/dir",
            expected_dim=384,
        )


def test_the_missing_model_message_is_actionable():
    import llm_provider

    msg = llm_provider._missing_model_message("some/model", "/some/cache")
    assert "scripts/fetch_embedding_model.py" in msg
    assert "EMBEDDING_PROVIDER=google" in msg
