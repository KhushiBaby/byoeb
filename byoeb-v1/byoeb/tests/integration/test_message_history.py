from byoeb.constants.user_enums import LanguageCode
from byoeb.models.message_category import MessageCategory
import pytest

from fastmcp import Client


@pytest.mark.parametrize(("lang", "messages"), [
    (LanguageCode.ENGLISH, ("what is antara injection?", "what are its side effects?", "how often is it given?")),
    (LanguageCode.HINDI, ("अंतरा इंजेक्शन क्या है?", "इसके दुष्प्रभाव क्या हैं?", "यह कितनी बार दिया जाता है?")),
    (LanguageCode.MARATHI, ("अंतरा इंजेक्शन म्हणजे काय?", "त्याचे दुष्परिणाम काय आहेत?", "ते किती वेळा दिले जाते?")),
    (LanguageCode.TELUGU, ("అంతర ఇంజెక్షన్ అంటే ఏమిటి?", "దాని దుష్ప్రభావాలు ఏమిటి?", "అది ఎంత తరచుగా ఇవ్వబడుతుంది?")),
])
async def test_conversation_history_is_captured(lang: LanguageCode, messages: tuple[str], envs, auth_access_token, temp_user):
    histories = []
    with temp_user(test_user=True, lang=lang):
        async with Client(f"{envs.base_url}/mcp", auth=auth_access_token) as client:
            for message in messages:
                response = await client.call_tool("asha_chat", {"message": message, "features": ["history"]})
                assert response.data.category == MessageCategory.BOT_TO_USER_RESPONSE.value  # ensure it is not IDK

                history = next((v for k, v in response.data.additional_info if k == "Conversation history"))
                histories.append(history)

    for i, history in enumerate(histories[1:], 1):
        assert history[-1].startswith("query%d: %s" % (i, messages[i - 1]))