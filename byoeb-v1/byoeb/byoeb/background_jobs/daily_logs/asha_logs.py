from typing import Any, AsyncIterator, Optional, Union
from datetime import datetime
from byoeb.chat_app.configuration.config import app_config
from byoeb.chat_app.configuration.dependency_setup import user_db_service, mongo_db_factory
from byoeb.models.message_category import MessageCategory
from motor.motor_asyncio import AsyncIOMotorCollection

db_provider = app_config["app"]["db_provider"]
message_collection_name = app_config["databases"]["mongo_db"]["message_collection"]

async def get_user_infos(batch, user_info_dict):
    user_ids = set()
    for entry in batch:
        user = entry['message_data'].get("user", {})
        user_id = user.get("user_id")
        if user_id and user_id not in user_info_dict:
            user_ids.add(user_id)

    # Skip DB call if all users already cached
    if user_ids:
        user_info_list = await user_db_service.get_users(list(user_ids))
        user_info_dict.update({user.user_id: user for user in user_info_list})

    return user_info_dict


def _to_date_str(ts: Union[None, int, float, datetime]) -> Optional[str]:
    """Format timestamp as dd-mm-yyyy. Accepts Unix timestamp (int/float) or datetime."""
    if ts is None:
        return None
    if isinstance(ts, datetime):
        return ts.strftime("%d-%m-%Y")
    return datetime.fromtimestamp(ts).strftime("%d-%m-%Y")


def extract_fields(entry, user_info_dict) -> Optional[dict[str, Any]]:
    user = entry.get("user", {})
    user_id = user.get("user_id")
    if user_id not in user_info_dict:
        return None

    user = user_info_dict[user_id]
    message_context = entry.get("message_context", {})
    message_additional_info = message_context.get("additional_info", {})
    reply_context = entry.get("reply_context", {})
    reply_additional_info = reply_context.get("additional_info", {})
    
    # Handle empty or missing user_location
    user_location = user.user_location

    incoming_ts = entry.get("incoming_timestamp")
    outgoing_ts = entry.get("outgoing_timestamp")
    onboarding_ts = user.created_timestamp

    # Convert timestamp to day format (dd-mm-yyyy); support both numeric and datetime
    day = _to_date_str(incoming_ts)
    onboarding_date = _to_date_str(onboarding_ts)

    status = message_additional_info.get("status")
    if not status:
        status = "resolved"

    return {
        "user_id": user.user_id,
        "phone_number_id": user.phone_number_id,
        "test_user": user.test_user,
        "user_language": user.user_language,
        "onboarding_date": onboarding_date,
        "district": user_location.get("district"),
        "block": user_location.get("block"),
        "sector": user_location.get("sector"),
        "sub_center": user_location.get("sub_center"),
        "message_type": reply_context.get("reply_type"),
        "message_category": entry.get("message_category"),
        "query_type": reply_additional_info.get("query_type"),
        "status": status,
        "query_source": reply_context.get("reply_source_text"),
        "query_en": reply_additional_info.get("query_en"),
        "rewritten_query": reply_context.get("reply_english_text"),
        "answer_english": message_context.get("message_english_text"),
        "answer_source": message_context.get("message_source_text"),
        "incoming_timestamp": incoming_ts,
        "outgoing_timestamp": outgoing_ts,
        "log_date": day
    }

_PROJECTION = {
    "message_data.user.user_id": 1,
    "message_data.incoming_timestamp": 1,
    "message_data.outgoing_timestamp": 1,
    "message_data.message_category": 1,
    "message_data.message_context.additional_info": 1,
    "message_data.message_context.message_english_text": 1,
    "message_data.message_context.message_source_text": 1,
    "message_data.reply_context.reply_type": 1,
    "message_data.reply_context.reply_source_text": 1,
    "message_data.reply_context.reply_english_text": 1,
    "message_data.reply_context.additional_info": 1,
}

_MESSAGE_CATEGORIES = [
    MessageCategory.AUDIO_IDK.value,
    MessageCategory.TEXT_IDK.value,
    MessageCategory.AUDIO_DISAMBIGUATION.value,
    MessageCategory.TEXT_DISAMBIGUATION.value,
    MessageCategory.BOT_TO_USER_RESPONSE.value,
]

async def fetch_and_process_user_messages(start_timestamp: int, end_timestamp: int, message_category: list[str], message_collection: AsyncIOMotorCollection) -> AsyncIterator[dict[str, Any]]:
    query = {
        "message_data.incoming_timestamp": {"$gte": start_timestamp, "$lte": end_timestamp},
        "message_data.message_category": {"$in": message_category}
    }
    cursor = message_collection.find(query, _PROJECTION)
    user_info_dict = {}
    while True:
        batch = await cursor.to_list(length=5000)
        if not batch:
            break
        user_info_dict = await get_user_infos(batch, user_info_dict)
        for entry in batch:
            row = extract_fields(entry['message_data'], user_info_dict)
            if row is not None:
                yield row

async def fetch_daily_logs(start_timestamp: int, end_timestamp: int) -> AsyncIterator[dict[str, Any]]:
    mongo_db = await mongo_db_factory.get(db_provider)
    message_collection = mongo_db.get_collection(message_collection_name)
    async for row in fetch_and_process_user_messages(
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        message_category=_MESSAGE_CATEGORIES,
        message_collection=message_collection,
    ):
        yield row