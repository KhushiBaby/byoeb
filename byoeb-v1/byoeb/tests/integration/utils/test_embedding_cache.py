from pydantic import AnyHttpUrl
from typing import Any, AsyncIterator, List, Literal, Set

import pytest
from fastmcp import Client

from byoeb.constants.user_enums import LanguageCode, UserType

# Timeout for HTTP calls (integration server may be slow)
HTTP_TIMEOUT_S = 60

def get_cache_hit(resp: Any) -> bool:
    return next((v for k, v in resp.additional_info if k == "Cache hit"), False)

def has_devanagari(text: str) -> bool:
    return any("\u0900" <= ch <= "\u097f" for ch in text)

async def run_queries(base_url: AnyHttpUrl, access_token: str, queries: List[str], features: List[Literal["audio", "history"]] | None = None) -> AsyncIterator[Any]:
    if features is None:
        features = []
    async with Client(f"{base_url}/mcp", auth=access_token) as client:
        for q in queries:
            r = await client.call_tool("asha_chat", {"message": q, "features": features})
            yield r.data

@pytest.mark.asyncio
async def test_repeated_query_hits_cache(envs, auth_access_token, auth_session, temp_user):
    auth_session.post(f"{envs.base_url}/purge_request_cache").raise_for_status()

    queries = ["what is antara injection?"] * 2
    with temp_user(user_type=UserType.ASHA, lang=LanguageCode.ENGLISH, test_user=True):
        responses = [resp async for resp in run_queries(envs.base_url, auth_access_token, queries)]

    assert not get_cache_hit(responses[0])
    assert get_cache_hit(responses[1])


@pytest.mark.asyncio
@pytest.mark.parametrize("lang,query,features", [
    (LanguageCode.ENGLISH, "what is antara injection", ["purge"]),
    (LanguageCode.HINDI, "antara injection kya hai", ["devanagari"]),
    (LanguageCode.ENGLISH, "what is antara injection", ["hit"]),
    (LanguageCode.HINDI, "antara injection kya hai", ["devanagari", "hit"]),
])
async def test_cached_response_respects_lang(lang: LanguageCode, query: str, features: set[str], envs, auth_access_token, auth_session, temp_user):
    if "purge" in features:
        auth_session.post(f"{envs.base_url}/purge_request_cache").raise_for_status()

    with temp_user(user_type=UserType.ASHA, lang=lang, test_user=True):
        responses = [resp async for resp in run_queries(envs.base_url, auth_access_token, [query])]

    assert responses
    resp = responses[0]
    assert get_cache_hit(resp) == ("hit" in features)
    assert has_devanagari(resp.text) == ("devanagari" in features)
