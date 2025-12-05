import asyncio
import tempfile

import pytest
from llama_index.core.embeddings.mock_embed_model import MockEmbedding

import byoeb.utils.embedding_cache as embedding_cache_module
from byoeb.utils.embedding_cache import EmbeddingCache


@pytest.fixture
def new_cache(monkeypatch):
    with tempfile.TemporaryDirectory() as tmpdir:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        future = loop.create_future()
        future.set_result(tmpdir)

        import byoeb.chat_app.configuration.config as config_module

        monkeypatch.setattr(config_module, "app_tempdir", future, raising=False)
        monkeypatch.setattr(embedding_cache_module, "app_tempdir", future, raising=False)
        monkeypatch.setenv("MILVUS_LOGS_DIR", tmpdir)
        monkeypatch.setenv("MILVUS_LITE_HOME", tmpdir)

        def factory(name: str, capacity: int = 3) -> EmbeddingCache:
            return EmbeddingCache(name, dim=4, capacity=capacity)

        yield factory
        loop.close()


def test_store_and_query_round_trip(new_cache):
    cache = new_cache("sq", capacity=4)
    embed_model = MockEmbedding(embed_dim=4)

    embedding = embed_model.get_text_embedding("hello-world")
    stored_id = cache.store(embedding, {"text": "hello"})[1]

    assert cache.get(stored_id) == {"text": "hello"}

    _, queried_id, value = cache.query(embedding, thresh=0)
    assert queried_id == stored_id
    assert value == {"text": "hello"}


def test_query_below_threshold_returns_none(new_cache):
    cache = new_cache("thresh", capacity=2)
    embed_model = MockEmbedding(embed_dim=4)

    embedding = embed_model.get_text_embedding("threshold")
    cache.store(embedding, {"text": "hello"})

    res = cache._search(embedding)
    thresh = res[1] + 0.1

    q = cache.query(embedding, thresh=thresh)
    assert q[1] is None
    assert q[2] is None


def test_store_reuses_existing_embedding(new_cache):
    cache = new_cache("reuse", capacity=3)
    embed_model = MockEmbedding(embed_dim=4)
    embedding = embed_model.get_text_embedding("duplicate-entry")

    first_id = cache.store(embedding, {"count": 1})[1]
    second_id = cache.store(embedding, {"count": 2})[1]

    assert cache.get(second_id) == {"count": 2}


def test_update_overwrites_existing_value(new_cache):
    cache = new_cache("update", capacity=3)
    embed_model = MockEmbedding(embed_dim=4)
    embedding = embed_model.get_text_embedding("update-me")

    stored_id = cache.store(embedding, {"value": "old"})[1]
    cache.update(stored_id, {"value": "new"})

    assert cache.get(stored_id) == {"value": "new"}


def test_update_marks_item_recent_for_eviction(new_cache):
    cache = new_cache("touch", capacity=2)
    embed_model = MockEmbedding(embed_dim=4)

    emb_one = embed_model.get_text_embedding("one")
    emb_two = embed_model.get_text_embedding("two")
    emb_three = embed_model.get_text_embedding("three")

    id_one = cache.store(emb_one, {"value": "one"})[1]
    id_two = cache.store(emb_two, {"value": "two"})[1]

    cache.update(id_one, {"value": "one-updated"})

    id_three = cache.store(emb_three, {"value": "three"})[1]

    assert cache.get(id_one) == {"value": "one-updated"}
    assert str(id_two) not in cache.kv.keys()
    result = cache.query(emb_two, thresh=0)
    assert result[1] != id_two


def test_eviction_removes_least_recently_used(new_cache):
    cache = new_cache("evict", capacity=2)
    embed_model = MockEmbedding(embed_dim=4)

    emb_one = embed_model.get_text_embedding("one")
    emb_two = embed_model.get_text_embedding("two")
    emb_three = embed_model.get_text_embedding("three")

    id_one = cache.store(emb_one, {"value": "one"})[1]
    id_two = cache.store(emb_two, {"value": "two"})[1]

    # Touch first entry so the second becomes LRU.
    assert cache.get(id_one) == {"value": "one"}

    id_three = cache.store(emb_three, {"value": "three"})[1]

    assert str(id_two) not in cache.kv.keys()
    assert cache.get(id_one) == {"value": "one"}
    assert cache.get(id_three) == {"value": "three"}

    # Evicted embedding should not be returned by query.
    result = cache.query(emb_two, thresh=0)
    assert result[1] != id_two


def test_traverse_lists_all_entries(new_cache):
    cache = new_cache("trav", capacity=3)
    embed_model = MockEmbedding(embed_dim=4)

    emb_one = embed_model.get_text_embedding("one")
    emb_two = embed_model.get_text_embedding("two")

    cache.store(emb_one, {"value": "one"})
    cache.store(emb_two, {"value": "two"})

    entries = sorted(list(cache.traverse()))
    assert entries == [(cache._preprocess_id(0), {"value": "one"}), (cache._preprocess_id(1), {"value": "two"})]


def test_store_and_query_string_embedding(new_cache):
    cache = new_cache("str-basic", capacity=2)

    _, stored_id, _ = cache.store("string-key", {"value": "string"})
    assert cache.get(stored_id) == {"value": "string"}

    dist, queried_id, value = cache.query("string-key", thresh=0)
    assert dist == 1.0
    assert queried_id == stored_id
    assert value == {"value": "string"}


def test_string_embedding_eviction_respects_lru(new_cache):
    cache = new_cache("str-evict", capacity=2)

    id_one = cache.store("one", {"value": "one"})[1]
    cache.store("two", {"value": "two"})

    # Touch "one" so it is the most recent entry before inserting a third value.
    cache.query("one", thresh=0)

    _, id_three, _ = cache.store("three", {"value": "three"})

    assert cache.query("two", thresh=0) == (None, None, None)
    assert cache.get(id_one) == {"value": "one"}
    assert cache.get(id_three) == {"value": "three"}


def test_string_embedding_hash_is_case_insensitive(new_cache):
    cache = new_cache("str-case", capacity=2)

    _, stored_id, _ = cache.store("KeyUpper", {"value": "upper"})

    dist, queried_id, value = cache.query("keyupper", thresh=0)
    assert dist == 1.0
    assert queried_id == stored_id
    assert value == {"value": "upper"}
