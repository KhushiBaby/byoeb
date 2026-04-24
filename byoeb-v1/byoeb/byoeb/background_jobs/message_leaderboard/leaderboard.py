import asyncio
import json
import os
import logging
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Dict, Any, List, Optional
from collections import Counter, defaultdict
import pandas as pd
import re

from byoeb.application_logger.azure_app_insights import AppInsightsLogHandler
from byoeb.chat_app.configuration.config import app_config
from byoeb.chat_app.configuration.dependency_setup import get_leaderboard_service, user_db_service, message_db_service
from byoeb.services.leaderboard.time_window_strategies import TimeWindowFactory
from byoeb.services.user.utils import get_user_district, get_user_block, has_location_info

IST = ZoneInfo("Asia/Kolkata")

# Azure App Insights Loggers
run_logger = AppInsightsLogHandler.getLogger("leaderboard_run")
send_logger = AppInsightsLogHandler.getLogger("leaderboard_send")

# Console Logger for local development and production console output
console_logger = logging.getLogger("leaderboard_console")
console_logger.setLevel(logging.INFO)
# Only add handler if it doesn't already exist
if not console_logger.handlers:
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    console_handler.setFormatter(formatter)
    console_logger.addHandler(console_handler)
    console_logger.propagate = False

# TEST MODE: Set to True to send only to your test phone number
# Set to False to send to all users (production mode)
TEST_MODE_SEND_TO_ME_ONLY = False  # Set to True for testing, False for production

# Your test phone number (read from keys.env using PHONE_NUMBER_ID)
TEST_PHONE_NUMBER = os.getenv("PHONE_NUMBER_ID") if TEST_MODE_SEND_TO_ME_ONLY else None

def validate_and_format_phone_number(phone: str) -> Optional[str]:
    """
    Validate and format phone number for WhatsApp.
    
    Args:
        phone: Phone number in any format
        
    Returns:
        Formatted phone number (digits only, 11-13 digits) or None if invalid
        
    Examples:
        "917567071072" -> "917567071072"
        "+91 75670 71072" -> "917567071072"
        "07567071072" -> None (missing country code)
    """
    # Remove all non-digit characters
    digits_only = re.sub(r'\D', '', phone)
    
    # Check length (WhatsApp requires 11-13 digits with country code)
    if len(digits_only) < 11 or len(digits_only) > 13:
        return None
    
    # Common validation: India numbers should start with 91
    if digits_only.startswith('91') and len(digits_only) == 12:
        return digits_only
    elif len(digits_only) == 10:
        # Missing country code for India - add it
        return "91" + digits_only
    elif len(digits_only) >= 11:
        return digits_only
    
    return None

# Load translations from JSON file
def _load_leaderboard_translations() -> Dict[str, Dict[str, str]]:
    """
    Load leaderboard translations from translate.json file in utils folder.
    
    Returns:
        Dictionary with language codes as keys and translation dictionaries as values
    """
    # Get the path to translate.json relative to this file (similar to bot_config.json pattern)
    current_dir = Path(__file__).resolve().parent
    translate_file_path = current_dir / ".." / ".." / "utils" / "translate.json"
    translate_file_path = translate_file_path.resolve()
    
    try:
        translations = json.loads(translate_file_path.read_text(encoding="utf-8"))
        return translations
    except FileNotFoundError:
        console_logger.error(f"Translation file not found at {translate_file_path}. Using default English translations.")
        # Return default English translations as fallback
        return {
            "en": {
                "block_singular": "Block ",
                "district_singular": "District ",
                "blocks_plural": "Blocks",
                "districts_plural": "Districts"
            }
        }
    except json.JSONDecodeError as e:
        console_logger.error(f"Error parsing translation file: {e}. Using default English translations.")
        return {
            "en": {
                "block_singular": "Block ",
                "district_singular": "District ",
                "blocks_plural": "Blocks",
                "districts_plural": "Districts"
            }
        }

# Unified translation dictionary for leaderboard text
LEADERBOARD_TRANSLATIONS = _load_leaderboard_translations()

def get_leaderboard_translation(is_block_leaderboard: bool, user_language: str = "en", plural: bool = False) -> str:
    """
    Get translated text for block/district leaderboard labels.
    
    Args:
        is_block_leaderboard: If True, returns block translation, otherwise district translation
        user_language: User's language code (en, hi, mr, te)
        plural: If True, returns plural form (Blocks/Districts), otherwise singular with space (Block /District )
        
    Returns:
        Translated string
    """
    lang_dict = LEADERBOARD_TRANSLATIONS.get(user_language, LEADERBOARD_TRANSLATIONS["en"])
    
    if plural:
        return lang_dict["blocks_plural"] if is_block_leaderboard else lang_dict["districts_plural"]
    else:
        return lang_dict["block_singular"] if is_block_leaderboard else lang_dict["district_singular"]

async def fetch_phone_numbers_for_asha_and_test_users() -> List[str]:
    """
    Retrieves phone numbers for all ASHA workers and test users from the database.

    Returns:
        List[str]: Phone numbers of ASHA workers and test users
    """
    # Selection (all vs test-only) is controlled inside the service function
    # Use service layer; service internally respects TEST_USERS_ONLY env flag
    return await user_db_service.fetch_phone_numbers_for_asha_and_test_users()

async def build_district_leaderboard_last_week_ist(message_categories: Optional[List[str]] = None, processing_batch_size: int = 1000) -> pd.DataFrame:
    """
    Builds a leaderboard of districts based on message activity from the previous week in IST timezone.
    
    Args:
        message_categories: Optional list of message categories to filter by
        processing_batch_size: Number of documents to process in each batch
        
    Returns:
        pd.DataFrame: Sorted leaderboard with district statistics
    """
    leaderboard_service = await get_leaderboard_service()
    # Use week strategy explicitly - addressing review comment #5
    week_strategy = TimeWindowFactory.create_strategy('week')
    return await leaderboard_service.build_district_leaderboard(message_categories, processing_batch_size, week_strategy)

async def build_district_leaderboard_with_strategy(
    strategy_type: str,
    message_categories: Optional[List[str]] = None,
    processing_batch_size: int = 1000,
    **strategy_kwargs
) -> pd.DataFrame:
    """
    Builds a leaderboard using a specific time window strategy.

    Args:
        strategy_type: Type of strategy ('week', 'month', 'year', 'custom')
        message_categories: Optional list of message categories to filter by
        processing_batch_size: Number of documents to process in each batch
        **strategy_kwargs: Additional arguments for strategy creation (e.g., days_back for custom)

    Returns:
        pd.DataFrame: Sorted leaderboard with district statistics
    """
    leaderboard_service = await get_leaderboard_service()
    strategy = TimeWindowFactory.create_strategy(strategy_type, **strategy_kwargs)
    return await leaderboard_service.build_district_leaderboard(message_categories, processing_batch_size, strategy)

async def build_monthly_leaderboard(message_categories: Optional[List[str]] = None, processing_batch_size: int = 1000) -> pd.DataFrame:
    """
    Builds a leaderboard for the previous month.

    Args:
        message_categories: Optional list of message categories to filter by
        processing_batch_size: Number of documents to process in each batch

    Returns:
        pd.DataFrame: Sorted leaderboard with district statistics
    """
    return await build_district_leaderboard_with_strategy('month', message_categories, processing_batch_size)

async def build_yearly_leaderboard(message_categories: Optional[List[str]] = None, processing_batch_size: int = 1000) -> pd.DataFrame:
    """
    Builds a leaderboard for the previous year.

    Args:
        message_categories: Optional list of message categories to filter by
        processing_batch_size: Number of documents to process in each batch

    Returns:
        pd.DataFrame: Sorted leaderboard with district statistics
    """
    return await build_district_leaderboard_with_strategy('year', message_categories, processing_batch_size)

async def build_custom_leaderboard(days_back: int, message_categories: Optional[List[str]] = None, processing_batch_size: int = 1000) -> pd.DataFrame:
    """
    Builds a leaderboard for a custom time period.

    Args:
        days_back: Number of days to look back
        message_categories: Optional list of message categories to filter by
        processing_batch_size: Number of documents to process in each batch

    Returns:
        pd.DataFrame: Sorted leaderboard with district statistics
    """
    return await build_district_leaderboard_with_strategy('custom', message_categories, processing_batch_size, days_back=days_back)

async def build_all_block_leaderboards(
    message_categories: Optional[List[str]] = None,
    processing_batch_size: int = 1000
) -> Dict[str, pd.DataFrame]:
    """
    Builds block leaderboards for ALL districts in a single pass for optimal performance.
    
    This function fetches messages and hydrates users once, then computes block leaderboards
    for all districts in a single pass, avoiding redundant database queries.
    
    Args:
        message_categories: Optional list of message categories to filter by
        processing_batch_size: Number of documents to process in each batch
        
    Returns:
        Dict[str, pd.DataFrame]: Dictionary mapping district (normalized, lowercase) to block leaderboard DataFrame
    """
    week_strategy = TimeWindowFactory.create_strategy('week')
    start_timestamp, end_timestamp = week_strategy.calculate_window()
    
    # Get repository instances from message_db_service
    repository_factory = await message_db_service._get_repository_factory()
    message_repository = await repository_factory.get_message_repository()
    
    # Define projection for required fields only
    required_fields_only = {"_id": 0, "message_data.user.user_id": 1, "message_data.incoming_timestamp": 1}
    
    # Sort by timestamp descending in database for better performance
    sort_by_timestamp = [("message_data.incoming_timestamp", -1)]
    
    # Get messages using repository with database-level sorting - FETCH ONCE
    message_iterator = await message_repository.find_messages_by_time_range(
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        message_categories=message_categories,
        projection=required_fields_only,
        sort=sort_by_timestamp
    )
    message_documents = [doc async for doc in message_iterator]
    
    if not user_db_service:
        raise ValueError("user_db_service must be provided for leaderboard functionality")
    
    # HYDRATE USERS ONCE for all messages
    user_objects_cache = {}
    if message_documents:
        await user_db_service.hydrate_users(message_documents, user_objects_cache)
    
    # Initialize counters for all districts: district -> block -> stats
    # Structure: {district: {block: {message_count, unique_users, first_seen, last_seen}}}
    district_block_stats = defaultdict(lambda: defaultdict(lambda: {
        "message_count": 0,
        "unique_users": set(),
        "first_seen": None,
        "last_seen": None
    }))
    
    # Process all messages in a single pass
    for message_document in message_documents:
        message_data = message_document.get("message_data", {})
        user_id = message_data.get("user", {}).get("user_id")
        message_timestamp = message_data.get("incoming_timestamp")
        
        if not isinstance(message_timestamp, int) or message_timestamp < start_timestamp or message_timestamp > end_timestamp:
            continue
        
        user_object = user_objects_cache.get(user_id)
        user_district = get_user_district(user_object)
        user_block = get_user_block(user_object)
        
        # Only process if user has both district and block info
        if not user_district or not user_block:
            continue
        
        # Normalize district name for comparison (case-insensitive)
        district_normalized = user_district.strip().lower()
        block_normalized = user_block.strip()
        
        # Update stats for this district-block combination
        stats = district_block_stats[district_normalized][block_normalized]
        stats["message_count"] += 1
        if user_id:
            stats["unique_users"].add(user_id)
        
        if stats["first_seen"] is None or message_timestamp < stats["first_seen"]:
            stats["first_seen"] = message_timestamp
        if stats["last_seen"] is None or message_timestamp > stats["last_seen"]:
            stats["last_seen"] = message_timestamp
    
    # Convert aggregated stats to DataFrames for each district
    district_block_leaderboards = {}
    for district, block_stats in district_block_stats.items():
        leaderboard_rows = [
            {
                "block": block_name,
                "message_count": stats["message_count"],
                "unique_users": len(stats["unique_users"]),
                "first_seen": datetime.fromtimestamp(stats["first_seen"]).strftime("%d-%m-%Y %H:%M:%S") if stats["first_seen"] else None,
                "last_seen": datetime.fromtimestamp(stats["last_seen"]).strftime("%d-%m-%Y %H:%M:%S") if stats["last_seen"] else None
            }
            for block_name, stats in block_stats.items()
        ]
        
        if not leaderboard_rows:
            district_block_leaderboards[district] = pd.DataFrame(
                columns=["block", "message_count", "unique_users", "first_seen", "last_seen"]
            )
        else:
            # Sort by message_count and unique_users, then take top 3
            df = pd.DataFrame(leaderboard_rows).sort_values(
                by=["message_count", "unique_users"], 
                ascending=False, 
                ignore_index=True
            )
            district_block_leaderboards[district] = df.head(3)
    
    return district_block_leaderboards

async def build_block_leaderboard_for_district(
    district: str,
    message_categories: Optional[List[str]] = None,
    processing_batch_size: int = 1000
) -> pd.DataFrame:
    """
    Builds a leaderboard of top 3 blocks within a specific district based on message activity from the previous week.
    
    Args:
        district: District name to filter blocks by
        message_categories: Optional list of message categories to filter by
        processing_batch_size: Number of documents to process in each batch
        
    Returns:
        pd.DataFrame: Sorted leaderboard with top 3 blocks in the district
    """
    week_strategy = TimeWindowFactory.create_strategy('week')
    start_timestamp, end_timestamp = week_strategy.calculate_window()
    
    # Get repository instances from message_db_service
    repository_factory = await message_db_service._get_repository_factory()
    message_repository = await repository_factory.get_message_repository()
    
    # Define projection for required fields only
    required_fields_only = {"_id": 0, "message_data.user.user_id": 1, "message_data.incoming_timestamp": 1}
    
    # Sort by timestamp descending in database for better performance
    sort_by_timestamp = [("message_data.incoming_timestamp", -1)]
    
    # Get messages using repository with database-level sorting
    message_iterator = await message_repository.find_messages_by_time_range(
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        message_categories=message_categories,
        projection=required_fields_only,
        sort=sort_by_timestamp
    )
    message_documents = [doc async for doc in message_iterator]
    
    if not user_db_service:
        raise ValueError("user_db_service must be provided for leaderboard functionality")
    
    user_objects_cache = {}
    block_message_counts = Counter()
    block_unique_users = defaultdict(set)
    block_first_message_timestamp = {}
    block_last_message_timestamp = {}
    
    # Normalize district name for comparison (case-insensitive)
    district_normalized = district.strip().lower()
    
    # Process messages in batches
    for i in range(0, len(message_documents), processing_batch_size):
        message_batch = message_documents[i:i + processing_batch_size]
        
        await user_db_service.hydrate_users(message_batch, user_objects_cache)
        
        for message_document in message_batch:
            message_data = message_document.get("message_data", {})
            user_id = message_data.get("user", {}).get("user_id")
            message_timestamp = message_data.get("incoming_timestamp")
            
            if not isinstance(message_timestamp, int) or message_timestamp < start_timestamp or message_timestamp > end_timestamp:
                continue
            
            user_object = user_objects_cache.get(user_id)
            user_district = get_user_district(user_object)
            user_block = get_user_block(user_object)
            
            # Only process if user is in the specified district and has block info
            if not user_district or not user_block:
                continue
            
            if user_district.strip().lower() != district_normalized:
                continue
            
            block_message_counts[user_block] += 1
            if user_id:
                block_unique_users[user_block].add(user_id)
            
            block_first_message_timestamp[user_block] = min(block_first_message_timestamp.get(user_block, message_timestamp), message_timestamp)
            block_last_message_timestamp[user_block] = max(block_last_message_timestamp.get(user_block, message_timestamp), message_timestamp)
    
    leaderboard_rows = [
        {
            "block": block_name,
            "message_count": message_count,
            "unique_users": len(block_unique_users[block_name]),
            "first_seen": datetime.fromtimestamp(block_first_message_timestamp[block_name]).strftime("%d-%m-%Y %H:%M:%S"),
            "last_seen": datetime.fromtimestamp(block_last_message_timestamp[block_name]).strftime("%d-%m-%Y %H:%M:%S")
        }
        for block_name, message_count in block_message_counts.items()
    ]
    
    if not leaderboard_rows:
        return pd.DataFrame(
            columns=["block", "message_count", "unique_users", "first_seen", "last_seen"]
        )
    
    # Sort by message_count and unique_users, then take top 3
    df = pd.DataFrame(leaderboard_rows).sort_values(by=["message_count", "unique_users"], ascending=False, ignore_index=True)
    return df.head(3)

async def format_leaderboard_as_template_parameters(
    top3_df: pd.DataFrame, 
    is_block_leaderboard: bool = False,
    user_language: str = "en"
) -> List[str]:
    """
    Format leaderboard data as template parameters for WhatsApp template message.

    Template format:
    📊 Top 3 {{4}} with Highest Interactions:
    {{1}}
    {{2}}
    {{3}}
    Ask me any questions...

    - {{1}}, {{2}}, {{3}}: Up to 3 lines (only real data; single space for missing slots).
    - {{4}}: Type indicator (e.g. "Block" or "District" - translated).

    Each line format: "1) Block Nayagaon had 71 messages from 25 users."

    Args:
        top3_df: DataFrame with leaderboard data (either districts or blocks)
        is_block_leaderboard: If True, expects 'block' column, otherwise 'district'
        user_language: User's language code (en, hi, mr, te) for translating type indicator

    Returns:
        List of 4 parameters: [line1, line2, line3, type_indicator]
    """
    type_indicator = get_leaderboard_translation(is_block_leaderboard, user_language, plural=False).strip()
    prefix = get_leaderboard_translation(is_block_leaderboard, user_language, plural=False)

    name_col = "block" if is_block_leaderboard else "district"
    lines = []

    for _, row in top3_df.iterrows():
        raw_name = str(row[name_col]).strip() if pd.notna(row[name_col]) else ""
        message_count = int(row["message_count"]) if pd.notna(row["message_count"]) else 0
        unique_users = int(row["unique_users"]) if pd.notna(row["unique_users"]) else 0

        if not raw_name or raw_name == "N/A":
            continue
        if message_count == 0 and unique_users == 0:
            continue

        name = prefix + raw_name
        lines.append((name, message_count, unique_users))

    # Build exactly 3 line strings (only real entries; single space for missing slots)
    formatted_lines = [
        f"{rank}) {name} had {message_count} messages from {unique_users} users."
        for rank, (name, message_count, unique_users) in enumerate(lines, start=1)
    ]
    # Pad to 3 slots; use single space for missing (WhatsApp may reject empty params)
    while len(formatted_lines) < 3:
        formatted_lines.append(" ")

    parameters = [formatted_lines[0], formatted_lines[1], formatted_lines[2], type_indicator]

    assert len(parameters) == 4, f"Expected 4 parameters, got {len(parameters)}"
    assert all(isinstance(p, str) for p in parameters), f"All parameters must be strings. Got: {parameters}"
    assert len(parameters[3]) > 0, f"Type indicator (4th parameter) must be non-empty. Got: {parameters}"

    return parameters

async def send_leaderboard_template_messages(
    phone_numbers: List[str],
    top3_df: pd.DataFrame,
    user_db_service,
    message_db_service
):
    """
    Send leaderboard messages as WhatsApp template messages to all users.

    NOTE: This function ONLY READS from the database (no modifications).
    It uses: find_messages_by_time_range, get_users, hydrate_users - all read-only operations.

    Template uses 4 parameters: {{1}}, {{2}}, {{3}} = up to 3 lines; {{4}} = type (Block/Districts).
    """
    import hashlib
    from byoeb.chat_app.configuration.dependency_setup import whatsapp_service
    from byoeb_core.models.byoeb.message_context import ByoebMessageContext, MessageContext, ReplyContext, MessageTypes
    from byoeb_core.models.byoeb.user import User
    from byoeb.services.chat import constants
    
    # Get user information for all phone numbers
    user_ids = [hashlib.md5(phone.encode()).hexdigest() for phone in phone_numbers]
    users = await user_db_service.get_users(user_ids)
    user_map = {user.phone_number_id: user for user in users if user}
    
    # Pre-calculate block leaderboards for all districts in a single optimized pass
    # This fetches messages and hydrates users once, then computes all district block leaderboards
    console_logger.info("📊 Pre-calculating block leaderboards for all districts in a single pass...")
    run_logger.info("Pre-calculating block leaderboards", extra={AppInsightsLogHandler.DETAILS: {
        "context": "pre_calculate_block_leaderboards_optimized"
    }})
    
    try:
        # Build all block leaderboards in a single pass - much more efficient!
        all_block_leaderboards = await build_all_block_leaderboards(
            message_categories=None,
            processing_batch_size=1000
        )
        
        # Extract unique districts from users for logging
        unique_districts = set()
        for user in users:
            if user and has_location_info(user):
                district = get_user_district(user)
                if district:
                    unique_districts.add(district.strip().lower())
        
        # Create cache from the optimized results
        block_leaderboard_cache = {}
        for district in unique_districts:
            block_df = all_block_leaderboards.get(district)
            if block_df is not None and len(block_df) > 0:
                block_leaderboard_cache[district] = block_df
                run_logger.info(f"Block leaderboard calculated for district", extra={AppInsightsLogHandler.DETAILS: {
                    "context": "build_block_leaderboard",
                    "district": district,
                    "blocks_found": len(block_df)
                }})
            else:
                block_leaderboard_cache[district] = None
        
        successful_districts = len([v for v in block_leaderboard_cache.values() if v is not None])
        console_logger.info(f"✅ Calculated {successful_districts}/{len(unique_districts)} district block leaderboards (optimized single-pass)")
        run_logger.info("Block leaderboard cache ready", extra={AppInsightsLogHandler.DETAILS: {
            "context": "block_leaderboard_cache_ready",
            "successful_districts": successful_districts,
            "total_districts": len(unique_districts),
            "optimization": "single_pass"
        }})
    except Exception as e:
        # Fallback: if optimized version fails, log error and continue without block leaderboards
        run_logger.error(f"Error building block leaderboards (optimized)", extra={AppInsightsLogHandler.DETAILS: {
            "context": "build_block_leaderboard_error",
            "error": str(e)
        }})
        block_leaderboard_cache = {}  # Empty cache, will fall back to global leaderboard
        console_logger.warning(f"⚠️  Error building block leaderboards: {str(e)}")
    
    results = []
    sent_count = 0
    error_count = 0
    
    for phone in phone_numbers:
        try:
            # Validate and format phone number
            formatted_phone = validate_and_format_phone_number(phone)
            if not formatted_phone:
                error_count += 1
                send_logger.warning("Invalid phone number format", extra={AppInsightsLogHandler.DETAILS: {
                    "context": "validate_phone_number",
                    "phone": phone,
                    "user_id": user.user_id if user else None
                }})
                results.append({
                    "phone": phone,
                    "status": "error",
                    "message": "Invalid phone number format"
                })
                continue
            
            if formatted_phone != phone:
                phone = formatted_phone  # Use formatted version
            
            # Get user language, default to 'en' if user not found
            user = user_map.get(phone)
            user_language = user.user_language if user and user.user_language else 'en'
            
            # Determine which leaderboard to use for this user
            # If user has district and block info, show top 3 blocks in their district
            # Otherwise, show global top 3 districts
            user_has_location = has_location_info(user)
            is_block_leaderboard = False
            user_leaderboard_df = top3_df  # Default to global districts

            if user_has_location:
                user_district = get_user_district(user)
                if user_district:
                    district_key = user_district.strip().lower()
                    # Use cached block leaderboard if available
                    if district_key in block_leaderboard_cache:
                        cached_block_df = block_leaderboard_cache[district_key]
                        if cached_block_df is not None and len(cached_block_df) > 0:
                            user_leaderboard_df = cached_block_df
                            is_block_leaderboard = True
                            send_logger.debug("Using block leaderboard for user", extra={AppInsightsLogHandler.DETAILS: {
                                "context": "user_block_leaderboard",
                                "phone": phone,
                                "user_id": user.user_id if user else None,
                                "district": user_district,
                                "blocks_count": len(cached_block_df)
                            }})
                        else:
                            send_logger.debug("No blocks found, using global leaderboard", extra={AppInsightsLogHandler.DETAILS: {
                                "context": "user_fallback_global",
                                "phone": phone,
                                "user_id": user.user_id if user else None,
                                "district": user_district,
                                "reason": "no_blocks_found"
                            }})
                    else:
                        send_logger.debug("District not in cache, using global leaderboard", extra={AppInsightsLogHandler.DETAILS: {
                            "context": "user_fallback_global",
                            "phone": phone,
                            "user_id": user.user_id if user else None,
                            "district": user_district,
                            "reason": "not_in_cache"
                        }})
                else:
                    send_logger.debug("User has location but no district", extra={AppInsightsLogHandler.DETAILS: {
                        "context": "user_fallback_global",
                        "phone": phone,
                        "user_id": user.user_id if user else None,
                        "reason": "no_district"
                    }})
            else:
                send_logger.debug("User has no location info", extra={AppInsightsLogHandler.DETAILS: {
                    "context": "user_fallback_global",
                    "phone": phone,
                    "user_id": user.user_id if user else None,
                    "reason": "no_location_info"
                }})
            
            # Format template parameters based on user's leaderboard
            template_parameters = await format_leaderboard_as_template_parameters(
                user_leaderboard_df, 
                is_block_leaderboard=is_block_leaderboard,
                user_language=user_language
            )
            
            # Validate template parameters before sending (expect 4: line1, line2, line3, type)
            if not template_parameters or len(template_parameters) != 4:
                error_count += 1
                send_logger.error("Invalid template parameters", extra={AppInsightsLogHandler.DETAILS: {
                    "context": "invalid_template_params",
                    "phone": phone,
                    "user_id": user.user_id if user else None,
                    "expected": 4,
                    "got": len(template_parameters) if template_parameters else 0
                }})
                continue

            # Ensure all parameters are non-empty strings (WhatsApp may reject empty)
            validated_parameters = []
            for i, param in enumerate(template_parameters):
                if param is None:
                    validated_parameters.append(" " if i < 3 else "Block")
                elif not isinstance(param, str):
                    validated_parameters.append(str(param).strip() or (" " if i < 3 else "Block"))
                elif len(param.strip()) == 0:
                    validated_parameters.append(" " if i < 3 else "Block")
                else:
                    validated_parameters.append(param.strip())

            template_parameters = validated_parameters

            # Build a text representation of the leaderboard (for logging/fallback)
            # Template: {{1}}, {{2}}, {{3}} = lines; {{4}} = type
            line1, line2, line3 = template_parameters[0], template_parameters[1], template_parameters[2]
            type_indicator = template_parameters[3]
            lines_block = "\n".join(s for s in [line1, line2, line3] if s.strip()) or "No data available for this period."
            leaderboard_text = f"📊 Top 3 {type_indicator} with Highest Interactions:\n\n{lines_block}\n\nAsk me any questions about childbirth, maternal health, childcare, or health in general."
            
            # Create ByoebMessageContext with template information
            # Following the consensus pattern: create with REGULAR_TEXT and text fields,
            # then prepare_requests will create both text and template requests
            byoeb_message = ByoebMessageContext(
                channel_type="whatsapp",
                message_category="leaderboard",
                user=User(
                    user_id=user.user_id if user else hashlib.md5(phone.encode()).hexdigest(),
                    user_type=user.user_type if user else "asha",
                    user_language=user_language,
                    phone_number_id=phone,
                    test_user=user.test_user if user else False,
                ),
                message_context=MessageContext(
                    message_id=f"leaderboard-{phone}-{int(datetime.now(timezone.utc).timestamp())}",
                    message_type=MessageTypes.REGULAR_TEXT.value,  # Start with REGULAR_TEXT like consensus does
                    message_source_text=leaderboard_text,  # Set text fields (like consensus does)
                    message_english_text=leaderboard_text,  # Set text fields (like consensus does)
                    additional_info={
                        constants.TEMPLATE_NAME: "leaderboardv2",
                        constants.TEMPLATE_LANGUAGE: user_language,
                        constants.TEMPLATE_PARAMETERS: template_parameters
                    }
                ),
                reply_context=None,
                cross_conversation_id=None,
                cross_conversation_context=None,
                incoming_timestamp=int(datetime.now(timezone.utc).timestamp()),
                outgoing_timestamp=int(datetime.now(timezone.utc).timestamp())
            )
            
            # Prepare requests - this will create both text and template requests (like consensus does)
            requests = whatsapp_service.prepare_requests(byoeb_message)
            
            # Select only the template request
            # prepare_requests returns: [text_message, template_message] when both are present
            # But we should find it by type, not by index, to be more robust
            template_request = None
            for req in requests:
                if req.get("type") == "template":
                    template_request = req
                    break
            
            if not template_request:
                error_count += 1
                send_logger.error("No template request found", extra={AppInsightsLogHandler.DETAILS: {
                    "context": "no_template_request",
                    "phone": phone,
                    "user_id": user.user_id if user else None,
                    "request_types": [req.get('type', 'unknown') for req in requests]
                }})
                continue
            
            # Change message type to TEMPLATE_TEXT (like consensus does for inactive users)
            byoeb_message.message_context.message_type = MessageTypes.TEMPLATE_TEXT.value
            
            # Send only the template request (like consensus does for inactive users)
            if template_request:
                responses, message_ids = await whatsapp_service.send_requests([template_request])
                
                # Check response status
                if responses and len(responses) > 0:
                    response = responses[0]
                    status = response.response_status.status if hasattr(response, 'response_status') else 'unknown'
                    error = response.response_status.error if hasattr(response, 'response_status') and hasattr(response.response_status, 'error') else None
                    message_id = message_ids[0] if message_ids else None
                    
                    # Check message status
                    msg_status = 'unknown'
                    if hasattr(response, 'messages') and response.messages:
                        msg_status = response.messages[0].message_status if hasattr(response.messages[0], 'message_status') else 'unknown'
                        
                        # Log based on message status
                        if msg_status in ['accepted', 'sent', 'delivered', 'read']:
                            sent_count += 1
                            send_logger.info(f"Leaderboard message {msg_status}", extra={AppInsightsLogHandler.DETAILS: {
                                "context": f"message_{msg_status}",
                                "phone": phone,
                                "user_id": user.user_id if user else None,
                                "message_id": message_id,
                                "language": user_language,
                                "is_block_leaderboard": is_block_leaderboard,
                                "whatsapp_status": status
                            }})
                        else:
                            error_count += 1
                            send_logger.warning("Leaderboard message status issue", extra={AppInsightsLogHandler.DETAILS: {
                                "context": "message_status_warning",
                                "phone": phone,
                                "user_id": user.user_id if user else None,
                                "message_id": message_id,
                                "message_status": msg_status,
                                "whatsapp_status": status,
                                "error": error
                            }})
                    
                    results.append({
                        "phone": phone,
                        "status": "success" if status == "200" else "warning",
                        "message_id": message_id,
                        "language": user_language,
                        "whatsapp_status": status,
                        "message_status": msg_status if 'msg_status' in locals() else None,
                        "error": error if error and error != 'None' else None
                    })
                    
                    # Show progress every 10 messages
                    if len(results) % 10 == 0:
                        console_logger.info(f"Progress: {sent_count} sent, {error_count} errors ({len(results)}/{len(phone_numbers)})")
                else:
                    error_count += 1
                    send_logger.error("No response from WhatsApp API", extra={AppInsightsLogHandler.DETAILS: {
                        "context": "no_whatsapp_response",
                        "phone": phone,
                        "user_id": user.user_id if user else None,
                        "language": user_language
                    }})
                    results.append({
                        "phone": phone,
                        "status": "error",
                        "message": "No response from WhatsApp API"
                    })
            else:
                error_count += 1
                send_logger.error("Failed to prepare template request", extra={AppInsightsLogHandler.DETAILS: {
                    "context": "prepare_request_failed",
                    "phone": phone,
                    "user_id": user.user_id if user else None,
                    "language": user_language
                }})
                results.append({
                    "phone": phone,
                    "status": "error",
                    "message": "Failed to prepare request"
                })
        except Exception as e:
            error_count += 1
            send_logger.error("Error sending leaderboard message", extra={AppInsightsLogHandler.DETAILS: {
                "context": "send_error",
                "phone": phone,
                "user_id": user.user_id if user else None,
                "error": str(e),
                "language": user_language if 'user_language' in locals() else None
            }})
            results.append({
                "phone": phone,
                "status": "error",
                "message": str(e)
            })
    
    # Log final summary
    console_logger.info("📊 Sending Summary:")
    console_logger.info(f"✅ Successfully sent: {sent_count}/{len(phone_numbers)}")
    console_logger.info(f"❌ Errors: {error_count}/{len(phone_numbers)}")
    
    return results

async def main():
    console_logger.info("="*70)
    console_logger.info("🏆 LEADERBOARD MESSAGE SENDER".center(70))
    console_logger.info("="*70)
    
    run_logger.info("Starting leaderboard job", extra={AppInsightsLogHandler.DETAILS: {
        "context": "leaderboard_job_start",
        "test_mode": TEST_MODE_SEND_TO_ME_ONLY,
        "test_phone": TEST_PHONE_NUMBER if TEST_MODE_SEND_TO_ME_ONLY else None
    }})
    
    # Build global district leaderboard (used as fallback for users without location info)
    console_logger.info("📊 Building global district leaderboard...")
    leaderboard_df = await build_district_leaderboard_last_week_ist()
    top3_df = leaderboard_df.head(3)
    console_logger.info(f"✅ Found {len(leaderboard_df)} districts with activity")
    console_logger.info("Top 3 Districts (Global):")
    for idx, row in top3_df.iterrows():
        console_logger.info(f"   {idx+1}. {row['district']}: {row['message_count']} messages, {row['unique_users']} users")
    
    run_logger.info("Global district leaderboard built", extra={AppInsightsLogHandler.DETAILS: {
        "context": "build_global_leaderboard",
        "total_districts": len(leaderboard_df),
        "top3_districts": top3_df.to_dict('records') if len(top3_df) > 0 else []
    }})

    console_logger.info("📝 Message Configuration:")
    console_logger.info("   Template: leaderboardv2")
    console_logger.info("   Parameters: 4 (3 lines + type indicator)")
    console_logger.info("   Languages: Translated per user (en, hi, mr, te)")
    console_logger.info("📍 Personalization:")
    console_logger.info("   • Users WITH district & block → Top 3 blocks in their district")
    console_logger.info("   • Users WITHOUT location → Top 3 districts (global)")
    
    # Collect phone numbers based on mode
    console_logger.info("="*70)
    if TEST_MODE_SEND_TO_ME_ONLY:
        if not TEST_PHONE_NUMBER:
            console_logger.error("❌ ERROR: TEST_MODE_SEND_TO_ME_ONLY is True but PHONE_NUMBER_ID not set")
            console_logger.error("   Please add PHONE_NUMBER_ID=your_phone_number to keys.env")
            return
        
        console_logger.info("🧪 TEST MODE ENABLED")
        console_logger.info(f"   Recipient: {TEST_PHONE_NUMBER}")
        console_logger.info("   Count: 1 user (test only)")
        phone_numbers = [TEST_PHONE_NUMBER]
        mode = "test"
    else:
        console_logger.info("🚀 PRODUCTION MODE ENABLED")
        phone_numbers = await fetch_phone_numbers_for_asha_and_test_users()
        console_logger.info(f"   Recipients: {len(phone_numbers)} users")
        mode = "production"
    
    console_logger.info("="*70)
    
    # Send messages to collected phone numbers
    results = await send_leaderboard_template_messages(
        phone_numbers, 
        top3_df, 
        user_db_service, 
        message_db_service
    )
    
    # Calculate and report results
    success_count = sum(1 for r in results if r.get("status") == "success")
    failure_count = len(results) - success_count
    
    console_logger.info("="*70)
    console_logger.info("📈 FINAL RESULTS".center(70))
    console_logger.info("="*70)
    console_logger.info(f"   Total: {len(results)} messages")
    console_logger.info(f"   ✅ Success: {success_count}")
    console_logger.info(f"   ❌ Failed: {failure_count}")
    console_logger.info(f"   Mode: {mode.upper()}")
    if TEST_MODE_SEND_TO_ME_ONLY:
        console_logger.info("   💡 Tip: Set TEST_MODE_SEND_TO_ME_ONLY = False for production")
    console_logger.info("="*70)
    
    # Log completion
    log_details = {
        "context": "leaderboard_job_complete",
        "mode": mode,
        "total_recipients": len(results),
        "success_count": success_count,
        "failure_count": failure_count
    }
    if TEST_MODE_SEND_TO_ME_ONLY:
        log_details["test_phone"] = TEST_PHONE_NUMBER
    
    run_logger.info(f"Leaderboard job completed ({mode} mode)", extra={AppInsightsLogHandler.DETAILS: log_details})

if __name__ == "__main__":
    asyncio.run(main())
