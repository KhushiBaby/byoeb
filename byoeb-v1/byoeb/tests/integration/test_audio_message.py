import base64
from pathlib import Path

from byoeb.constants.user_enums import LanguageCode, UserType
from byoeb.models.message_category import MessageCategory
import pytest

from fastmcp import Client


@pytest.mark.parametrize(("lang", "audio_file"), [
    (LanguageCode.ENGLISH, "english.ogg"),
    (LanguageCode.HINDI, "hindi.ogg"),
])
async def test_audio_message(lang: LanguageCode, audio_file: str, envs, auth_access_token, temp_user):
    path = Path(__file__).resolve().parent / "resources" / audio_file
    data = base64.b64encode(path.read_bytes())

    with temp_user(user_type=UserType.ASHA, lang=lang):
        async with Client(f"{envs.base_url}/mcp", auth=auth_access_token) as client:
            response = await client.call_tool("asha_chat", {"message": {"data": data, "mime_type": "audio/ogg"}})

    assert response.data.category == MessageCategory.BOT_TO_USER_RESPONSE.value  # ensure it is not IDK
