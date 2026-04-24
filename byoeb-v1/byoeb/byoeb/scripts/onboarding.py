import argparse
import ast
import asyncio
import logging
import re

from byoeb.services.auth.models import AuthPermission
from byoeb.utils.utils import auth_session
import pandas as pd
import requests
from datetime import datetime, timezone, date
from typing import Any, List, Optional

from byoeb.chat_app.configuration.dependency_setup import channel_client_factory

logger = logging.getLogger(__name__)
from byoeb.services.channel.whatsapp import WhatsAppService
from byoeb.services.chat import constants
from byoeb_core.models.byoeb.message_context import ByoebMessageContext, MessageContext, MessageTypes
from byoeb_core.models.byoeb.user import User
from byoeb_integrations.channel.whatsapp.meta.async_whatsapp_client import StatusCode
from byoeb.constants.user_enums import LanguageCode, UserType
from byoeb.constants.onboarding_text import THANK_YOU_DICT


def clean_template_param(text: str) -> str:
    """Make template parameter safe for WhatsApp: no newlines/tabs, no 4+ spaces."""
    # Replace newlines/tabs with single space
    text = re.sub(r"[\r\n\t]+", " ", text)
    # Collapse multiple spaces to single
    text = re.sub(r"\s{2,}", " ", text)
    return text.strip()


def _created_timestamp_to_date(ts: Optional[Any]) -> Optional[date]:
    """Convert created_timestamp to date; supports datetime, epoch (int/float), or ISO str (e.g. from JSON)."""
    if ts is None:
        return None
    if isinstance(ts, datetime):
        return ts.date()
    if isinstance(ts, (int, float)):
        try:
            return datetime.fromtimestamp(int(ts), tz=timezone.utc).date()
        except (ValueError, OSError, OverflowError):
            return None
    if isinstance(ts, str):
        try:
            # ISO format from JSON (e.g. "2021-10-01T00:00:00Z" or "2021-10-01T00:00:00+00:00")
            normalized = ts.replace("Z", "+00:00")
            return datetime.fromisoformat(normalized).date()
        except (ValueError, TypeError):
            return None
    return None


async def send_welcome_message(
    whatsapp_service: WhatsAppService,
    user: User,
    user_type: str,
    language: str
) -> bool:
    """
    Send a welcome template message to a newly onboarded user.
    
    Args:
        whatsapp_service: WhatsApp service instance
        user: User object
        user_type: User type (ASHA, ANM, etc.)
        language: User language code (en, hi, mr, te)
    
    Returns:
        True if message was sent successfully, False otherwise
    """
    try:
        # Normalize and validate language code
        language = language.lower().strip() if language else LanguageCode.ENGLISH.value
        valid_languages = [lc.value for lc in LanguageCode]
        if language not in valid_languages:
            logger.warning("Invalid language '%s', defaulting to English", language)
            language = LanguageCode.ENGLISH.value
        
        # Get LanguageCode enum from string value
        lang_code = LanguageCode(language)
        mapped_user_type = UserType.ASHA.value if user_type and user_type.lower() == UserType.OTHERS.value else (user_type or UserType.ASHA.value)
        
        logger.info("Looking for welcome text: user_type=%s, language=%s", mapped_user_type, lang_code.value)
        
        # Get welcome message from THANK_YOU_DICT
        welcome_text = THANK_YOU_DICT.get(mapped_user_type, {}).get(lang_code.value, "")
        
        if not welcome_text:
            # Fallback to English if language not found
            logger.warning("No welcome text found for %s/%s, using English fallback", mapped_user_type, lang_code.value)
            welcome_text = THANK_YOU_DICT.get(mapped_user_type, {}).get(LanguageCode.ENGLISH.value, "Welcome! You have been successfully onboarded.")
        
        logger.debug("Welcome text (first 50 chars): %s...", welcome_text[:50])
        
        # Clean and prepare template parameter
        template_parameters = [clean_template_param(welcome_text)]
        
        # Create timestamp
        ts = int(datetime.now(timezone.utc).timestamp())
        
        # Create ByoebMessageContext for WhatsApp template message
        byoeb_message = ByoebMessageContext(
            channel_type="whatsapp",
            message_category="onboarding_welcome",
            user=user,
            message_context=MessageContext(
                message_id=f"onboard-welcome-{user.user_id}",
                message_type=MessageTypes.TEMPLATE_TEXT.value,
                message_source_text=None,
                message_english_text=None,
                media_info=None,
                additional_info={
                    constants.TEMPLATE_NAME: "onboard_welcome_v2",
                    constants.TEMPLATE_LANGUAGE: lang_code.value,
                    constants.TEMPLATE_PARAMETERS: template_parameters,
                },
            ),
            reply_context=None,
            cross_conversation_id=None,
            cross_conversation_context=None,
            incoming_timestamp=ts,
            outgoing_timestamp=ts
        )
        
        # Prepare and send template message
        requests = whatsapp_service.prepare_requests(byoeb_message)
        if not requests:
            logger.warning("Failed to prepare welcome message for user %s", user.phone_number_id)
            return False
        
        # Find template request (should be the last one or the one with template type)
        template_request = None
        for req in requests:
            if req.get("type") == "template":
                template_request = req
                break
        
        if not template_request:
            logger.warning("No template request found for user %s", user.phone_number_id)
            return False
        
        # Send template message
        responses, message_ids = await whatsapp_service.send_requests([template_request])
        
        if len(responses) > 0 and int(responses[0].response_status.status) == StatusCode.SUCCESS.value:
            logger.info("Welcome message sent to %s (lang: %s)", user.phone_number_id, lang_code.value)
            return True
        else:
            error_msg = responses[0].response_status.error if len(responses) > 0 else "Unknown error"
            logger.error("Failed to send welcome message to %s: %s", user.phone_number_id, error_msg)
            return False
            
    except Exception as e:
        logger.exception("Error sending welcome message to %s: %s", user.phone_number_id, e)
        return False


async def send_welcome_messages_to_users(
    registered_users: List[dict],
    original_users_data: List[dict]
) -> None:
    """
    Send welcome template messages to all registered users.
    
    Args:
        registered_users: List of user dictionaries from API response
        original_users_data: List of original user data from Excel (to get language/type)
        url: API base URL
    """
    if not registered_users:
        logger.info("No users to send welcome messages to.")
        return
    
    # Create a mapping of phone_number_id to original user data
    original_data_map = {}
    for orig_user in original_users_data:
        phone = str(orig_user.get("phone_number_id", "")).strip()
        if phone:
            original_data_map[phone] = orig_user
    
    # Initialize WhatsApp service
    from byoeb.chat_app.configuration.dependency_setup import whatsapp_service
    
    success_count = 0
    failure_count = 0
    
    logger.info("Sending welcome messages to %s users...", len(registered_users))
    
    for user_data in registered_users:
        try:
            # Handle nested User structure if present (API might return {"User": {...}})
            if "User" in user_data and isinstance(user_data["User"], dict):
                actual_user_data = user_data["User"]
            else:
                actual_user_data = user_data
            
            # Create User object from API response
            user = User(**actual_user_data)
            
            # Skip if user doesn't have required fields
            if not user.phone_number_id:
                logger.warning("Skipping user %s: missing phone_number_id", user.user_id)
                failure_count += 1
                continue
            
            # Get user type and language - prioritize original Excel data, then API response
            phone_key = str(user.phone_number_id).strip()
            original_data = original_data_map.get(phone_key, {})
            
            # Get user type - check original data first, then API response
            user_type = (
                original_data.get("user_type") or 
                user.user_type or 
                actual_user_data.get("user_type") or 
                UserType.ASHA.value
            )
            
            # Get language - prioritize original Excel data, then API response
            language = (
                original_data.get("user_language") or
                user.user_language or 
                actual_user_data.get("user_language")
            )
            
            if not language:
                language = LanguageCode.ENGLISH.value
                logger.warning("No language found for user %s, defaulting to English", user.phone_number_id)
            else:
                # Normalize language code (ensure it's lowercase and valid)
                language = str(language).lower().strip()
                # Validate it's a supported language code
                valid_languages = [lc.value for lc in LanguageCode]
                if language not in valid_languages:
                    logger.warning("Invalid language code '%s' for user %s, defaulting to English", language, user.phone_number_id)
                    language = LanguageCode.ENGLISH.value
            
            logger.info("User %s: type=%s, language=%s (from original_data=%s, user.user_language=%s)", user.phone_number_id, user_type, language, original_data.get("user_language"), user.user_language)
            
            # Send welcome message
            success = await send_welcome_message(
                whatsapp_service=whatsapp_service,
                user=user,
                user_type=user_type,
                language=language
            )
            
            if success:
                success_count += 1
            else:
                failure_count += 1
                
            # Small delay to avoid rate limiting
            await asyncio.sleep(0.1)
            
        except Exception as e:
            logger.exception("Error processing user %s: %s", user_data.get("user_id", "unknown"), e)
            failure_count += 1
    
    logger.info("Welcome message summary: Success=%s Failed=%s Total=%s", success_count, failure_count, len(registered_users))


def main(session: Optional[requests.Session] = None):
    parser = argparse.ArgumentParser(description="Upload users from Excel files.")
    parser.add_argument("--file", required=True, help="Input Excel file path.")
    parser.add_argument("--url", default="http://127.0.0.1:8000", help="API endpoint URL")
    parser.add_argument("--update", action="store_true", help="If set, update users using the API endpoint")
    parser.add_argument("--sheet", help="output sheet name")
    parser.add_argument("--skip-welcome", action="store_true", help="Skip sending welcome messages after registration")

    args = parser.parse_args()
    if session is None:
        session = auth_session(args.url, scopes=[AuthPermission.USERS_MANAGE])

    file_path = args.file
    df = pd.read_excel(file_path, header=0)
    
    # Handle different column name variations
    if "phone" in df.columns and "phone_number_id" not in df.columns:
        df["phone_number_id"] = df["phone"]
    if "location" in df.columns and "user_location" not in df.columns:
        df["user_location"] = df["location"]
    if "language" in df.columns and "user_language" not in df.columns:
        df["user_language"] = df["language"]
    
    df["phone_number_id"] = df["phone_number_id"].astype(str).apply(lambda x: "91" + x if len(x) == 10 else x)

    users_onboarded = df.to_dict(orient="records") 
    phone_numbers = []  
    for row in users_onboarded:
        # Remove columns that are not needed for registration (like user_id, onboarding_date)
        row.pop("user_id", None)
        row.pop("onboarding_date", None)
        
        # Convert date/timestamp objects to strings if present
        for key, value in list(row.items()):
            if isinstance(value, pd.Timestamp):
                row[key] = value.isoformat()
            elif hasattr(value, 'isoformat'):  # datetime objects
                row[key] = value.isoformat()
        
        # Handle user_location - ensure it has district field
        if "user_location" in row.keys() and row["user_location"]:
            if isinstance(row["user_location"], str):
                try:
                    row["user_location"] = ast.literal_eval(row["user_location"])
                except:
                    # If parsing fails, create a default location dict
                    row["user_location"] = {"district": "Unknown"}
        else:
            # If user_location is missing or empty, create a default one with district
            row["user_location"] = {"district": "Test District"}
        
        # Ensure district is present in user_location
        if not isinstance(row.get("user_location"), dict) or "district" not in row["user_location"]:
            if isinstance(row.get("user_location"), dict):
                row["user_location"]["district"] = "Test District"
            else:
                row["user_location"] = {"district": "Test District"}
        
        phone_numbers.append(row["phone_number_id"])

    response = session.post(args.url + "/register_users", json=users_onboarded)
    if response.status_code != 200:
        logger.error("Registration failed with status %s", response.status_code)
        logger.error("Response: %s", response.text)
        try:
            error_details = response.json()
            logger.error("Error details: %s", error_details)
        except Exception:
            pass
        response.raise_for_status()
    logger.info("Successfully registered")
    
    # Get registered users from response
    registered_users = response.json() if isinstance(response.json(), list) else []
    
    # Debug: Log user data to verify language is in response
    if registered_users:
        logger.debug("API returned %s user(s)", len(registered_users))
        for idx, user_data in enumerate(registered_users):
            logger.debug("User %s: phone=%s, language=%s, type=%s", idx + 1, user_data.get("phone_number_id"), user_data.get("user_language"), user_data.get("user_type"))
    
    # Send welcome messages to all registered users (unless --skip-welcome flag is set)
    if not args.skip_welcome and registered_users:
        try:
            # Pass original users_onboarded data to preserve language/type from Excel
            asyncio.run(send_welcome_messages_to_users(registered_users, users_onboarded))
        except Exception as e:
            logger.warning("Error sending welcome messages: %s. Continuing with other operations...", e)

    if args.update:
        update_response = session.post(args.url + "/update_users", json=users_onboarded)
        update_response.raise_for_status()
        logger.info("Successfully updated")

    if args.sheet:
        response = session.post(args.url + "/get_users", json=phone_numbers)
        response.raise_for_status()
        users = response.json()
        logger.info("Successfully extracted")

        df = pd.DataFrame([{
        	"user_id": user_data.get("user_id"),
        	"user_name": user_data.get("user_name"),
        	"phone": user_data.get("phone_number_id"),
        	"location": user_data.get("user_location"),
        	"user_type": user_data.get("user_type"),
        	"test_user": str(user_data.get("test_user")),
        	"onboarding_date": _created_timestamp_to_date(user_data.get("created_timestamp")),
		    "language":user_data.get("user_language")
        } for user_data in users])
        df.to_excel(args.sheet, index=False)


if __name__ == "__main__":
    main()
