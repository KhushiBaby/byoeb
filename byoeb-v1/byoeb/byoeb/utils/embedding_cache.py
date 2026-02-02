import dbm
import hashlib
import os
import pickle
from typing import Any, Optional, TypeAlias, Union
import lancedb
import pyarrow as pa
from byoeb.chat_app.configuration.config import app_tempdir

CacheResult: TypeAlias = Union[tuple[float, bytes, Any], tuple[float, None, None], tuple[None, bytes, Any], tuple[None, None, None]]
Embedding: TypeAlias = list[float] | bytes

class EmbeddingCache:
    TABLE_NAME = "embeddings"

    def __init__(self, name: str, dim: int, capacity: int):
        self.name = name
        self.dim = dim
        self.capacity = capacity
        self.lru = {}
        self.next_id = 0
        self.db = None
        self.table = None

    def _preprocess_id(self, id: int | str) -> bytes:
        if isinstance(id, int):
            return id.to_bytes(4, byteorder="big", signed=False)
        return hashlib.sha256(id.lower().encode()).digest()

    def _touch(self, id: bytes):
        if id in self.lru:
            del self.lru[id]
        self.lru[id] = None

    def _evict(self):
        if len(self.lru) <= self.capacity:
            return

        old = next(iter(self.lru))
        del self.lru[old]
        if old in self.kv:
            del self.kv[old]

        if len(old) == 4:
            id = int.from_bytes(old, byteorder="big", signed=False)
            assert self.table is not None
            self.table.delete(f"id = {id}")

    def _init_db(self):
        if self.db is not None:
            return

        self.kv = dbm.open(os.path.join(app_tempdir.result(), f"{self.name}-kv.db"), "c")
        db_path = os.path.join(app_tempdir.result(), f"{self.name}-lance.db")
        self.db = lancedb.connect(db_path)
        if self.TABLE_NAME in self.db.table_names():
            self.table = self.db.open_table(self.TABLE_NAME)
            return
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("vector", pa.list_(pa.float32(), self.dim))])
        self.table = self.db.create_table(self.TABLE_NAME, schema=schema)

    def _search(self, emb: Embedding) -> Optional[tuple[bytes, float]]:
        self._init_db()
        if isinstance(emb, str):
            id = self._preprocess_id(emb)
            return (id, 1.0) if id in self.kv else None

        results = self.table.search(emb).metric("cosine").limit(1).to_list()
        if not results:
            return None

        record = results[0]
        id = self._preprocess_id(record["id"])
        score = float(record["_distance"])
        similarity = 1.0 - score
        return id, similarity

    def store(self, emb: Embedding, val: Any) -> CacheResult:
        res = self._search(emb)
        assert self.table is not None

        # For string embeddings, exact match means we can reuse the ID
        # For vector embeddings, we should NOT reuse IDs even with high similarity
        # because different embeddings need different IDs
        if isinstance(emb, str) and res and res[1] >= 0.999:
            # String key hit - reuse existing ID
            id = res[0]
        elif isinstance(emb, str):
            # String key miss - create deterministic ID from hash
            id = self._preprocess_id(emb)
        else:
            # Vector embedding - always create new entry with new ID
            id_ = self.next_id
            id = self._preprocess_id(id_)
            self.next_id += 1
            self.table.add([{"id": id_, "vector": emb}])

        # Store value and update LRU
        self.kv[id] = pickle.dumps(val)
        self._touch(id)
        self._evict()
        return None, id, val

    def query(self, emb: Embedding, thresh: float) -> CacheResult:
        res = self._search(emb)
        if res is None:
            return None, None, None
        id, similarity = res
        if similarity < thresh:
            return similarity, None, None
        self._touch(id)
        return similarity, id, pickle.loads(self.kv[id])

    def update(self, id: bytes, val: Any):
        self.kv[id] = pickle.dumps(val)
        self._touch(id)

    def get(self, id: bytes) -> Optional[Any]:
        assert self.table is not None
        self._touch(id)
        return pickle.loads(self.kv[id]) if id in self.kv else None

    def traverse(self):
        for key in self.kv.keys():
            yield key, pickle.loads(self.kv[key])

    def purge(self) -> int:
        n = len(self.lru)
        self.lru = {}
        if self.db is not None:
            self.db.drop_table(self.TABLE_NAME)
            self.table = None
            self.db = None
        if hasattr(self, "kv"):
            for key in self.kv.keys():
                del self.kv[key]
            self.kv.close()
            delattr(self, "kv")
        return n