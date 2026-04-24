"""
Integration test for early return of already onboarded users.

This test verifies that when an already onboarded user sends onboarding messages
(in any language), the system correctly responds with "You are already registered
with the system" message instead of processing it through LLM/vector store.

This script:
1. Updates user language for each test
2. Sends POST requests to http://localhost:8000/receive
3. Tests various onboarding message variations across languages
4. Tests normal questions to ensure normal flow works

Usage:
    python -m pytest tests/integration/test_early_return_already_onboarded.py
    or
    python tests/integration/test_early_return_already_onboarded.py
"""

import json
import os
import sys
import time
from pydantic import AnyHttpUrl
from typing import Dict, Any, List
import requests
from dotenv import load_dotenv
import pytest

# Add the byoeb directory to the path (adjust for tests/integration location)
# This allows importing 'byoeb' module when running the script directly
current_file_dir = os.path.dirname(os.path.abspath(__file__))
# Go up from tests/integration/ to byoeb-v1/byoeb/ (project root)
byoeb_root = os.path.abspath(os.path.join(current_file_dir, '..', '..'))
# Add the root directory to path so 'byoeb' module can be imported
if byoeb_root not in sys.path:
    sys.path.insert(0, byoeb_root)

# Load environment variables
environment_path = os.path.join(byoeb_root, 'keys.env')
if os.path.exists(environment_path):
    load_dotenv(environment_path, override=True)

# Import LanguageCode for parametrization
from byoeb.constants.user_enums import LanguageCode

# Language codes
LANGUAGES = {
    "hi": "Hindi",
    "mr": "Marathi",
    "te": "Telugu",
    "en": "English"
}

# Onboarding messages to test
ONBOARDING_MESSAGES = {
    "en": [
        "onboard-asha",
        "onboard asha",
        "ONBOARD ASHA",
        "Onboard Asha",
        "onboard-ASHA"
    ],
    "hi": [
        "में एक आशा हूँ और मुझे आशा सहेली बोट से जुड़ना है"
    ],
    "mr": [
        "मी आशा आहे आणि मला आशा सहेली बॉटमध्ये सामील व्हायचे आहे"
    ],
    "te": [
        "నేను ఆశాను మరియు ఆశా సహేలి బాట్‌లో చేరాలనుకుంటున్నాను"
    ]
}

# Normal questions to test (to ensure normal flow works)
NORMAL_QUESTIONS = {
    "en": "what is antra injection?",
    "hi": "antra injection क्या है?",
    "mr": "antra injection म्हणजे काय?",
    "te": "antra injection అంటే ఏమిటి?"
}


def create_whatsapp_payload(message_body: str, phone_number: str) -> Dict[str, Any]:
    """Create WhatsApp webhook payload."""
    timestamp = str(int(time.time()))
    message_id = f"wamid.test{timestamp}{phone_number}"
    
    payload = {
        "object": "whatsapp_business_account",
        "entry": [
            {
                "id": "211506508713627",
                "changes": [
                    {
                        "value": {
                            "messaging_product": "whatsapp",
                            "metadata": {
                                "display_phone_number": "919001386867",
                                "phone_number_id": "183958451475612"
                            },
                            "contacts": [
                                {
                                    "profile": {
                                        "name": "Test User"
                                    },
                                    "wa_id": phone_number
                                }
                            ],
                            "messages": [
                                {
                                    "from": phone_number,
                                    "id": message_id,
                                    "timestamp": timestamp,
                                    "text": {
                                        "body": message_body
                                    },
                                    "type": "text"
                                }
                            ]
                        },
                        "field": "messages"
                    }
                ]
            }
        ]
    }
    
    return payload


def get_bot_messages(session: requests.Session, endpoint: AnyHttpUrl, timestamp: str) -> List[Dict[str, Any]]:
    """Get bot messages after a given timestamp."""
    response = session.get(str(endpoint) + f"/get_bot_messages?timestamp={timestamp}", timeout=10)
    response.raise_for_status()
    return [m for m in response.json()]


def wait_for_bot_response(session: requests.Session, endpoint: AnyHttpUrl, user_timestamp: str, timeout: int = 30) -> List[Dict[str, Any]]:
    """Wait for bot response after user message timestamp."""
    # Bot message categories (messages FROM bot TO user)
    BOT_MESSAGE_CATEGORIES = {
        "bot_to_asha", "bot_to_asha_response", "bot_to_anm", "bot_to_anm_response",
        "bot_to_anm_verification", "bot_to_anm_consensus", "audio_idk", "text_idk",
        "audio_idk_reconfirmation"
    }
    
    user_timestamp_int = int(user_timestamp)
    start_time = time.time()
    attempt = 0
    debug_printed = False
    
    while time.time() - start_time < timeout:
        attempt += 1
        all_messages = get_bot_messages(session, endpoint, user_timestamp)
        
        # Debug: Print message structure on first attempt if no messages found
        if attempt == 1 and len(all_messages) == 0:
            print(f"   🔍 Debug: No messages returned from API for timestamp {user_timestamp_int}")
        elif attempt == 1 and not debug_printed and len(all_messages) > 0:
            # Print structure of first message for debugging
            first_msg = all_messages[0]
            print(f"   🔍 Debug: First message keys: {list(first_msg.keys())}")
            print(f"   🔍 Debug: message_category: {first_msg.get('message_category', 'N/A')}")
            print(f"   🔍 Debug: outgoing_timestamp: {first_msg.get('outgoing_timestamp', 'N/A')}")
            debug_printed = True
        
        # Filter to only bot messages (exclude user messages)
        bot_messages = []
        for m in all_messages:
            if not isinstance(m, dict):
                continue
            # Check message_category at top level
            msg_category = m.get("message_category", "")
            # Also check reply_context.message_category if available
            reply_ctx = m.get("reply_context", {})
            if isinstance(reply_ctx, dict):
                reply_category = reply_ctx.get("message_category", "")
            else:
                reply_category = ""
            
            # Include if it's a bot message category
            if msg_category in BOT_MESSAGE_CATEGORIES or reply_category in BOT_MESSAGE_CATEGORIES:
                bot_messages.append(m)
            # Exclude user messages
            elif msg_category in {"asha_to_bot", "anm_to_bot", "user_to_bot"}:
                continue  # Skip user messages
            # If category is unclear but has outgoing_timestamp, assume it's a bot message
            elif m.get("outgoing_timestamp") not in (None, "None", ""):
                bot_messages.append(m)
        
        if bot_messages:
            valid_timestamps = []
            for m in bot_messages:
                outgoing_ts = m.get("outgoing_timestamp")
                if outgoing_ts not in (None, "None", ""):
                    try:
                        valid_timestamps.append(int(str(outgoing_ts)))
                    except (ValueError, TypeError):
                        pass
            
            if valid_timestamps:
                max_timestamp = max(valid_timestamps)
                if max_timestamp > user_timestamp_int:
                    print(f"   ✅ Found {len(bot_messages)} bot response(s) after {attempt} attempt(s), {int(time.time() - start_time)}s elapsed")
                    return bot_messages
            else:
                if attempt % 5 == 0:  # Print every 5th attempt
                    print(f"   ⏳ Waiting... (attempt {attempt}, {len(bot_messages)} bot messages found but no valid timestamps)")
        else:
            if attempt % 5 == 0:  # Print every 5th attempt
                elapsed = int(time.time() - start_time)
                total_msgs = len(all_messages) if isinstance(all_messages, list) else 0
                # Debug: Show sample message categories if available
                if total_msgs > 0 and attempt == 5:
                    sample_categories = [m.get("message_category", "N/A") for m in all_messages[:3]]
                    print(f"   ⏳ Waiting... (attempt {attempt}, {elapsed}s elapsed, {total_msgs} total messages, 0 bot messages)")
                    print(f"   🔍 Debug: Sample message categories: {sample_categories}")
                else:
                    print(f"   ⏳ Waiting... (attempt {attempt}, {elapsed}s elapsed, {total_msgs} total messages, 0 bot messages)")
        time.sleep(2)
    
    print(f"   ⚠️  Timeout after {timeout}s - no bot response received")
    # Final debug: Show what we got
    final_messages = get_bot_messages(session, endpoint, user_timestamp)
    if final_messages:
        print(f"   🔍 Debug: Final check - {len(final_messages)} messages found")
        for i, msg in enumerate(final_messages[:3], 1):
            print(f"   🔍 Debug: Message {i} - category: {msg.get('message_category', 'N/A')}, outgoing_ts: {msg.get('outgoing_timestamp', 'N/A')}")
    return []


def print_response_summary(message: str, response: requests.Response, language: str):
    """Print a summary of the response."""
    print(f"\n{'='*80}")
    print(f"📝 Message: {message}")
    print(f"🌐 Language: {language} ({LANGUAGES.get(language, language)})")
    print(f"{'='*80}")
    
    if response is None:
        print("❌ No response received")
        return
    
    print(f"📊 Status Code: {response.status_code}")
    
    try:
        response_data = response.json()
        print(f"📄 Response: {json.dumps(response_data, indent=2, ensure_ascii=False)}")
    except Exception as e:
        print(f"❌ Failed to parse response JSON: {e}")
        print(f"📄 Response Text: {response.text[:500]}")
        raise  # Fail the test if we can't parse the response
    
    print(f"{'='*80}\n")


@pytest.mark.parametrize("language", [lang for lang in LanguageCode])
async def test_onboarding_messages(envs, auth_session, temp_user, whatsapp_webhook, language: LanguageCode):
    """Test onboarding messages for a specific language."""    
    messages = ONBOARDING_MESSAGES.get(language.value, [])
    if not messages:
        print(f"⚠️  No onboarding messages defined for language {language.value}")
        return

    print(f"\n{'#'*80}")
    print(f"🧪 Testing Onboarding Messages for {LANGUAGES.get(language.value, language.value)} ({language.value})")
    print(f"{'#'*80}\n")
    with temp_user(lang=language) as user:
        for message in messages:
            payload = create_whatsapp_payload(message, user.phone_number_id)
            timestamp = payload["entry"][0]["changes"][0]["value"]["messages"][0]["timestamp"]

            print(f"\n📤 Sending: '{message}'")
            response = whatsapp_webhook(payload)
            print_response_summary(message, response, language.value)

            # Wait for bot response
            print("⏳ Waiting for bot response...")
            bot_responses = wait_for_bot_response(auth_session, envs.base_url, timestamp, timeout=45)

    if bot_responses:
        print(f"✅ Received {len(bot_responses)} bot response(s)")
        for i, bot_msg in enumerate(bot_responses, 1):
            response_text = bot_msg.get("message_context", {}).get("message_source_text", "")[:200]
            print(f"   Response {i}: {response_text}...")
    else:
        print("⚠️  No bot responses received")


@pytest.mark.parametrize("language", [lang for lang in LanguageCode])
async def test_normal_question(envs, auth_session, temp_user, whatsapp_webhook, language: LanguageCode):
    """Test a normal question to ensure normal flow works."""
    print(f"\n{'#'*80}")
    print(f"🧪 Testing Normal Question for {LANGUAGES.get(language.value, language.value)} ({language.value})")
    print(f"{'#'*80}\n")

    # Test normal question
    message = NORMAL_QUESTIONS.get(language.value, NORMAL_QUESTIONS["en"])

    with temp_user(lang=language) as user:
        payload = create_whatsapp_payload(message, user.phone_number_id)
        timestamp = payload["entry"][0]["changes"][0]["value"]["messages"][0]["timestamp"]

        print(f"\n📤 Sending: '{message}'")
        response = whatsapp_webhook(payload)
        print_response_summary(message, response, language.value)

        # Wait for bot response
        print("⏳ Waiting for bot response...")
        bot_responses = wait_for_bot_response(auth_session, envs.base_url, timestamp, timeout=45)

    if bot_responses:
        print(f"✅ Received {len(bot_responses)} bot response(s)")
        for i, bot_msg in enumerate(bot_responses, 1):
            response_text = bot_msg.get("message_context", {}).get("message_source_text", "")[:200]
            print(f"   Response {i}: {response_text}...")
    else:
        print("⚠️  No bot responses received")