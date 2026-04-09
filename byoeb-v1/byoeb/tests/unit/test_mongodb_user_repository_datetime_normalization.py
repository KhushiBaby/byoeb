from datetime import datetime
from typing import Any, Dict, List

import pytest

from byoeb.repositories.mongodb_user_repository import MongoUserRepository


class DummyCollection:
    def __init__(self, docs: List[Dict[str, Any]]):
        self._docs = docs

    async def find(self, filter_dict: Dict[str, Any], *args, **kwargs):
        # Very small subset: just yield all stored docs; tests assert normalization only.
        for doc in self._docs:
            yield doc


class DummyMongoUserRepository(MongoUserRepository):
    def __init__(self, docs: List[Dict[str, Any]]):
        # Bypass base class initialisation; only set _collection needed for find_all
        self._collection = DummyCollection(docs)


@pytest.mark.asyncio
async def test_find_users_by_type_normalizes_naive_datetimes():
    naive_ts = datetime(2025, 1, 1, 12, 0, 0)
    docs = [
        {
            "User": {
                "user_id": "u1",
                "user_type": "asha",
                "created_timestamp": naive_ts,
            }
        }
    ]
    repo = DummyMongoUserRepository(docs)

    results = []
    async for doc in repo.find_users_by_type("asha"):
        results.append(doc)

    assert len(results) == 1
    created = results[0]["User"]["created_timestamp"]
    assert created.tzinfo is not None

