import dbm
import hashlib
import os
import pickle
from typing import Any, Optional, TypeAlias, Union
from pymilvus import connections, FieldSchema, CollectionSchema, DataType, Collection
from pymilvus.orm.future import SearchResult
from byoeb.chat_app.configuration.config import app_tempdir

CacheResult: TypeAlias = Union[tuple[float, bytes, Any], tuple[float, None, None], tuple[None, bytes, Any], tuple[None, None, None]]
Embedding: TypeAlias = list[float] | bytes

class EmbeddingCache:
    FIELD_ID = "id"
    FIELD_INDEX = "vec"

    def __init__(self, name: str, dim: int, capacity: int):
        self.name = name
        self.dim = dim
        self.capacity = capacity
        self.lru = {}
        self.next_id = 0
        self.col = None

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
            assert self.col is not None
            self.col.delete(f"{self.FIELD_ID}=={id}")
            self.col.flush()

    def _search(self, emb: Embedding) -> Optional[tuple[bytes, float]]:
        if self.col is None:
            self.kv = dbm.open(os.path.join(app_tempdir.result(), f"{self.name}-kv.db"), "c")
            path_emb = os.path.join(app_tempdir.result(), f"{self.name}-emb.db")
            connections.connect(uri=path_emb, alias=path_emb)
            self.col = Collection("c", CollectionSchema([
                FieldSchema(self.FIELD_ID, DataType.INT64, is_primary=True),
                FieldSchema(self.FIELD_INDEX, DataType.FLOAT_VECTOR, dim=self.dim)
            ]), using=path_emb)
            _ = self.col.create_index(self.FIELD_INDEX, {"index_type": "AUTOINDEX", "metric_type": "COSINE"})

        if isinstance(emb, str):
            id = self._preprocess_id(emb)
            return (id, 1.0) if id in self.kv else None

        res = self.col.search([emb], self.FIELD_INDEX, {}, limit=1, output_fields=[self.FIELD_ID])
        assert isinstance(res, SearchResult)
        if not res or not res[0]:
            return None

        record = res[0][0]
        return self._preprocess_id(record["entity"][self.FIELD_ID]), record["distance"]

    def store(self, emb: Embedding, val: Any) -> CacheResult:
        res = self._search(emb)
        assert self.col is not None

        if res and 1 - res[1] >= 0.999:  # hit
            id = res[0]
        elif isinstance(emb, str):  # miss, emb is str
            id = self._preprocess_id(emb)
        else:  # miss, emb is vector
            id_ = self.next_id
            id = self._preprocess_id(id_)
            self.next_id += 1
            self.col.insert([[id_], [emb]])
            self.col.flush()

        self.kv[id] = pickle.dumps(val)
        self._touch(id)
        self._evict()
        return None, id, val

    def query(self, emb: Embedding, thresh: float) -> CacheResult:
        res = self._search(emb)
        if res is None:
            return None, None, None
        id, dist = res
        if dist < thresh:
            return dist, None, None
        self._touch(id)
        return dist, id, pickle.loads(self.kv[id])

    def update(self, id: bytes, val: Any):
        self.kv[id] = pickle.dumps(val)
        self._touch(id)

    def get(self, id: bytes) -> Optional[Any]:
        assert self.col is not None
        self._touch(id)
        return pickle.loads(self.kv[id]) if id in self.kv else None

    def traverse(self):
        for key in self.kv.keys():
            yield key, pickle.loads(self.kv[key])

    def purge(self) -> int:
        n = len(self.lru)
        self.lru = {}
        if self.col is not None:
            self.col.drop()
            self.col = None
        if hasattr(self, "kv"):
            for key in self.kv.keys():
                del self.kv[key]
            self.kv.close()
            delattr(self, "kv")
        return n