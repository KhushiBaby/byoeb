from typing import Any, AsyncIterator, List, Literal, Set
from byoeb.constants.user_enums import LanguageCode
from fastmcp import Client
import os
import pytest
import requests


BASE_URL = os.getenv("RECIEVE_URL")
PHONE_NUMBER_ID = os.getenv("PHONE_NUMBER_ID")
USER_NAME = os.getenv("USER_NAME", "byoeb-user")
if BASE_URL is None:
    raise RuntimeError("Environment variable (BASE_URL) is missing")
if PHONE_NUMBER_ID is None:
    raise RuntimeError("Environment variable (PHONE_NUMBER_ID) is missing")

BASE_URL = BASE_URL.replace("receive", "")
MCP_URL = BASE_URL + "mcp?phone_number=" + PHONE_NUMBER_ID
PURGE_URL = BASE_URL + "purge_request_cache"
REGISTER_URL = BASE_URL + "register_users"
DELETE_URL = BASE_URL + "delete_users"

def get_cache_hit(resp: Any) -> bool:
    return next((v for k, v in resp.additional_info if k == "Cache hit"), False)

def has_devanagari(text: str) -> bool:
    return any("\u0900" <= ch <= "\u097f" for ch in text)

async def run_queries(queries: List[str], features: Set[Literal["audio", "history"]] = set()) -> AsyncIterator[Any]:
    async with Client(MCP_URL) as client:
        for q in queries:
            r = await client.call_tool("asha_chat", {"message": q, "features": features})
            yield r.data

@pytest.mark.asyncio
async def test_repeated_query_hits_cache():
    requests.post(PURGE_URL).raise_for_status()
    queries = ["what is antara injection?"] * 2

    responses = [resp async for resp in run_queries(queries)]
    assert not get_cache_hit(responses[0])
    assert get_cache_hit(responses[1])


@pytest.mark.asyncio
@pytest.mark.parametrize("lang,query,features", [
    (LanguageCode.ENGLISH, "what is antara injection", ["purge"]),
    (LanguageCode.HINDI, "antara injection kya hai", ["devanagari"]),
    (LanguageCode.ENGLISH, "what is antara injection", ["hit"]),
    (LanguageCode.HINDI, "antara injection kya hai", ["devanagari", "hit"]),
])
async def test_cached_response_respects_lang(lang: LanguageCode, query: str, features: set[str]):
    if "purge" in features:
        requests.post(PURGE_URL).raise_for_status()

    requests.delete(DELETE_URL, json=[PHONE_NUMBER_ID]).raise_for_status()

    user = {
        "phone_number_id": PHONE_NUMBER_ID,
        "user_location": {"district": "Test District"},
        "user_type": "asha",
        "user_language": lang.value,
        "user_name": USER_NAME,
        "test_user": True,
    }

    requests.post(REGISTER_URL, json=[user]).raise_for_status()

    responses = [resp async for resp in run_queries([query])]
    assert responses
    resp = responses[0]

    assert get_cache_hit(resp) == ("hit" in features)
    assert has_devanagari(resp.text) == ("devanagari" in features)
