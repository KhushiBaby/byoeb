from byoeb.constants.user_enums import LanguageCode
from byoeb.models.message_category import MessageCategory

import pytest
from fastmcp import Client
from mcp.types import TextContent

@pytest.mark.asyncio
async def test_health(envs, auth_access_token):
    async with Client(f"{envs.base_url}/mcp", auth=auth_access_token) as client:
        result = await client.call_tool("asha_service_health", {})
        assert len(result.content) == 1
        assert isinstance(result.content[0], TextContent)
        assert result.content[0].text == "Chat bot is running"


@pytest.mark.asyncio
async def test_chat(envs, auth_access_token, temp_user):
    with temp_user():
        async with Client(f"{envs.base_url}/mcp", auth=auth_access_token) as client:
            result = await client.call_tool("asha_chat", {"message": "What is the antara injection?"})
            assert result.structured_content is not None
            assert "category" in result.structured_content
            assert result.structured_content["category"] == MessageCategory.BOT_TO_USER_RESPONSE.value
            assert "text" in result.structured_content


@pytest.mark.asyncio
async def test_registration(envs, auth_access_token, auth_session, auth_me):
    auth_session.delete(f"{envs.base_url}/delete_users", json=[auth_me.phone_number_id]).raise_for_status()
    async with Client(f"{envs.base_url}/mcp", auth=auth_access_token) as client:
        result = await client.call_tool("asha_chat", {"message": "What is the antara injection?"})
        assert len(result.content) == 1
        assert isinstance(result.content[0], TextContent)
        assert result.data.category == "unknown_user"

        result = await client.call_tool("asha_register_user", {"data": {
            "name": auth_me.username,
            "language": LanguageCode.ENGLISH.value,
            "state": "Karnataka"
        }})
        assert result.structured_content is not None and "message" in result.structured_content
        assert len(result.structured_content["message"]) == 1
        message = result.structured_content["message"][0]
        assert message["phone_number_id"] == auth_me.phone_number_id

        result = await client.call_tool("asha_chat", {"message": "What is the antara injection?"})
        assert result.structured_content is not None
        assert "category" in result.structured_content
        assert result.structured_content["category"] == MessageCategory.BOT_TO_USER_RESPONSE.value
        assert "text" in result.structured_content