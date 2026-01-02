import asyncio
import json
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Dict, Any, List, Optional
import pandas as pd
import re

from byoeb.chat_app.configuration.config import app_config
from byoeb.chat_app.configuration.dependency_setup import get_leaderboard_service, user_db_service, message_db_service
from byoeb.services.leaderboard.time_window_strategies import TimeWindowFactory

IST = ZoneInfo("Asia/Kolkata")

# TEST MODE: Set to True to use 3 parameters (district, count, users) for testing
# Set to False to use full 9 parameters (3 districts * 3 fields each)
TEST_MODE_3_PARAMS = False  # Changed to False to use real leaderboard data with 9 parameters

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
        print(f"⚠️  Invalid phone number length: {phone} -> {digits_only} ({len(digits_only)} digits)")
        print(f"   Expected: 11-13 digits (country code + number)")
        return None
    
    # Common validation: India numbers should start with 91
    if digits_only.startswith('91') and len(digits_only) == 12:
        return digits_only
    elif len(digits_only) == 10:
        # Missing country code for India
        print(f"⚠️  Phone number missing country code: {phone}")
        print(f"   Adding India country code (91)...")
        return "91" + digits_only
    elif len(digits_only) >= 11:
        return digits_only
    
    return None

def print_delivery_troubleshooting(phone: str, message_id: str, status: str):
    """
    Print troubleshooting information for message delivery issues.
    """
    phone_digits = re.sub(r'\D', '', phone)
    phone_length = len(phone_digits)
    
    print(f"\n🔍 TROUBLESHOOTING for {phone}:")
    print(f"   Message ID: {message_id}")
    print(f"   API Status: {status}")
    print(f"\n   Common reasons messages aren't delivered:")
    print(f"   1. Phone number not registered with WhatsApp")
    print(f"      → Verify {phone} has WhatsApp installed and active")
    print(f"   2. Phone number format issue")
    print(f"      → Current format: {phone} ({phone_length} digits)")
    print(f"      → Should be: country code + number (11-13 digits, no + or spaces)")
    print(f"   3. Template not fully approved")
    print(f"      → Check WhatsApp Business Manager for template status")
    print(f"   4. Delivery delay")
    print(f"      → Template messages can take 1-5 minutes to deliver")
    print(f"   5. Number not in business contact list")
    print(f"      → Some accounts require numbers to be added first")
    print(f"   6. Check WhatsApp Business Manager")
    print(f"      → Go to Message Templates → View delivery reports")
    print(f"      → Look for message ID: {message_id}")
    print(f"\n   Next steps:")
    print(f"   - Wait 5-10 minutes and check WhatsApp again")
    print(f"   - Verify phone number format is correct")
    print(f"   - Check if template is fully approved (not just 'Active')")
    print(f"   - Try sending to a number that previously received messages")

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

async def format_leaderboard_as_template_parameters(top3_df: pd.DataFrame, test_mode_3_params: bool = True) -> List[str]:
    """
    Format leaderboard data as template parameters for WhatsApp template message.
    
    Args:
        test_mode_3_params: If True, returns only 3 parameters for testing (district, count, users)
                          If False, returns 9 parameters (3 districts * 3 fields each)
    
    Returns:
        List of parameters for the template
    """
    # TEST MODE: Return only 3 parameters (first district only)
    if test_mode_3_params:
        if len(top3_df) == 0:
            return ["Test District", "28", "1"]  # Default test values
        
        # Get first district data
        first_row = top3_df.iloc[0]
        district = str(first_row['district']).strip() if pd.notna(first_row['district']) else "Test District"
        message_count = str(int(first_row['message_count'])) if pd.notna(first_row['message_count']) else "28"
        unique_users = str(int(first_row['unique_users'])) if pd.notna(first_row['unique_users']) else "1"
        
        parameters = [
            district if district else "Test District",
            message_count if message_count else "28",
            unique_users if unique_users else "1"
        ]
        
        # Validate: ensure exactly 3 parameters, all non-empty strings
        assert len(parameters) == 3, f"Expected 3 parameters, got {len(parameters)}"
        assert all(isinstance(p, str) and len(p) > 0 for p in parameters), \
            f"All parameters must be non-empty strings. Got: {parameters}"
        
        return parameters
    
    # PRODUCTION MODE: Return 9 parameters (3 districts * 3 fields each)
    parameters = []
    
    # Add parameters for existing districts
    for idx, row in top3_df.iterrows():
        district = str(row['district']).strip() if pd.notna(row['district']) else "N/A"
        message_count = str(int(row['message_count'])) if pd.notna(row['message_count']) else "0"
        unique_users = str(int(row['unique_users'])) if pd.notna(row['unique_users']) else "0"
        
        # Ensure no empty strings or None values
        parameters.append(district if district else "N/A")
        parameters.append(message_count if message_count else "0")
        parameters.append(unique_users if unique_users else "0")
    
    # If less than 3 districts, pad with placeholder values
    # WhatsApp requires all parameters to be non-empty strings
    # Use "N/A" for missing district names and "0" for missing counts
    while len(parameters) < 9:
        if len(parameters) % 3 == 0:  # District name position (0, 3, 6)
            parameters.append("N/A")
        else:  # Count or users position (1, 2, 4, 5, 7, 8)
            parameters.append("0")
    
    # Validate: ensure exactly 9 parameters, all non-empty strings
    assert len(parameters) == 9, f"Expected 9 parameters, got {len(parameters)}"
    assert all(isinstance(p, str) and len(p) > 0 for p in parameters), \
        f"All parameters must be non-empty strings. Got: {parameters}"
    
    return parameters[:9]  # Ensure exactly 9 parameters (3 districts * 3 fields each)

async def send_leaderboard_template_messages(
    phone_numbers: List[str],
    top3_df: pd.DataFrame,
    user_db_service,
    message_db_service,
    test_mode_3_params: bool = True
):
    """
    Send leaderboard messages as WhatsApp template messages to all users.
    
    Args:
        test_mode_3_params: If True, uses 3 parameters (district, count, users) for testing
                           If False, uses 9 parameters (3 districts * 3 fields each)
    """
    from byoeb.chat_app.configuration.dependency_setup import channel_client_factory
    from byoeb.services.channel.whatsapp import WhatsAppService
    from byoeb_core.models.byoeb.message_context import ByoebMessageContext, MessageContext, ReplyContext, MessageTypes
    from byoeb_core.models.byoeb.user import User
    from byoeb.services.chat import constants
    import hashlib
    
    whatsapp_service = WhatsAppService(channel_client_factory)
    
    # Get user information for all phone numbers
    user_ids = [hashlib.md5(phone.encode()).hexdigest() for phone in phone_numbers]
    users = await user_db_service.get_users(user_ids)
    user_map = {user.phone_number_id: user for user in users if user}
    
    # Format template parameters
    # Format template parameters (using test mode if enabled)
    template_parameters = await format_leaderboard_as_template_parameters(top3_df, test_mode_3_params=test_mode_3_params)
    
    results = []
    for phone in phone_numbers:
        try:
            # Validate and format phone number
            formatted_phone = validate_and_format_phone_number(phone)
            if not formatted_phone:
                print(f"❌ Invalid phone number format: {phone}")
                results.append({
                    "phone": phone,
                    "status": "error",
                    "message": "Invalid phone number format"
                })
                continue
            
            if formatted_phone != phone:
                print(f"📞 Phone number formatted: {phone} → {formatted_phone}")
                phone = formatted_phone  # Use formatted version
            
            # Get user language, default to 'en' if user not found
            user = user_map.get(phone)
            user_language = user.user_language if user and user.user_language else 'en'
            
            # Validate template parameters before sending
            expected_params = 3 if test_mode_3_params else 9
            if not template_parameters or len(template_parameters) != expected_params:
                print(f"❌ ERROR: Invalid template parameters for {phone}")
                print(f"   Expected {expected_params} parameters, got: {len(template_parameters) if template_parameters else 0}")
                print(f"   Parameters: {template_parameters}")
                continue
            
            # Ensure all parameters are non-empty strings (no None or empty values)
            validated_parameters = []
            for i, param in enumerate(template_parameters):
                if param is None:
                    print(f"⚠️  WARNING: Parameter {i+1} is None, replacing with 'N/A'")
                    validated_parameters.append("N/A")
                elif not isinstance(param, str):
                    validated_parameters.append(str(param) if param else "N/A")
                elif len(param.strip()) == 0:
                    print(f"⚠️  WARNING: Parameter {i+1} is empty, replacing with 'N/A'")
                    validated_parameters.append("N/A")
                else:
                    validated_parameters.append(param.strip())
            
            template_parameters = validated_parameters
            
            # Build a text representation of the leaderboard (for logging/fallback)
            if test_mode_3_params:
                # Test mode: Only 3 parameters (district, count, users)
                leaderboard_text = f"Top District: {template_parameters[0]}: {template_parameters[1]} msgs, {template_parameters[2]} users"
            else:
                # Production mode: 9 parameters (3 districts * 3 fields)
                leaderboard_text = "📊 Top 3 Districts with Highest Interactions:\n\n"
                for i in range(0, 9, 3):
                    district = template_parameters[i]
                    count = template_parameters[i+1]
                    users = template_parameters[i+2]
                    if district != "N/A":
                        leaderboard_text += f"{i//3 + 1}) {district}: {count} messages from {users} users\n"
            
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
            
            # Debug: Show what requests were generated
            print(f"\n🔍 DEBUG: Generated {len(requests)} request(s) for {phone}")
            for i, req in enumerate(requests):
                req_type = req.get("type", "unknown")
                print(f"   Request {i}: type={req_type}")
                if req_type == "template":
                    print(f"      Template name: {req.get('template', {}).get('name', 'N/A')}")
                    print(f"      Template language: {req.get('template', {}).get('language', {}).get('code', 'N/A')}")
            
            # Select only the template request
            # prepare_requests returns: [text_message, template_message] when both are present
            # But we should find it by type, not by index, to be more robust
            template_request = None
            for req in requests:
                if req.get("type") == "template":
                    template_request = req
                    break
            
            if not template_request:
                print(f"❌ ERROR: No template request found in {len(requests)} request(s) for {phone}")
                print(f"   Request types: {[req.get('type', 'unknown') for req in requests]}")
                continue
            
            # Change message type to TEMPLATE_TEXT (like consensus does for inactive users)
            byoeb_message.message_context.message_type = MessageTypes.TEMPLATE_TEXT.value
            
            # Verify template request is correct
            if template_request:
                print(f"\n📤 Preparing to send template to {phone} (lang: {user_language})")
                print(f"   Template: {constants.TEMPLATE_NAME}={byoeb_message.message_context.additional_info[constants.TEMPLATE_NAME]}")
                print(f"   Parameters ({len(template_parameters)}): {template_parameters}")
                print(f"   Parameter validation: All parameters are non-empty strings: {all(isinstance(p, str) and len(p) > 0 for p in template_parameters)}")
                
                if test_mode_3_params:
                    print(f"\n🧪 TEST MODE: Using 3 parameters for testing")
                    print(f"   Template should have exactly 3 variables: {{1}}, {{2}}, {{3}}")
                else:
                    # ⚠️ IMPORTANT: Check template character limit
                    print(f"\n⚠️  TEMPLATE CHARACTER LIMIT WARNING:")
                    print(f"   Your template body is 172 characters, but WhatsApp limit is 106 characters.")
                    print(f"   This may prevent message delivery even if API accepts it.")
                    print(f"   Please shorten the template body in WhatsApp Business Manager.")
                    print(f"   Suggested: Remove emoji or shorten text to fit within 106 characters.")
                
                # Log the exact payload being sent (for debugging)
                print(f"\n   📋 WhatsApp API Payload:")
                print(f"      {json.dumps(template_request, indent=6)}")
                
                # Send only the template request (like consensus does for inactive users)
                responses, message_ids = await whatsapp_service.send_requests([template_request])
                
                # Check response status
                if responses and len(responses) > 0:
                    response = responses[0]
                    status = response.response_status.status if hasattr(response, 'response_status') else 'unknown'
                    error = response.response_status.error if hasattr(response, 'response_status') and hasattr(response.response_status, 'error') else None
                    message_id = message_ids[0] if message_ids else None
                    
                    print(f"   Response Status: {status}")
                    if error and error != 'None':
                        print(f"   ⚠️  Error: {error}")
                    if message_id:
                        print(f"   Message ID: {message_id}")
                    
                    # Check message status
                    if hasattr(response, 'messages') and response.messages:
                        msg_status = response.messages[0].message_status if hasattr(response.messages[0], 'message_status') else 'unknown'
                        print(f"   Message Status: {msg_status}")
                        
                        # Check if message was actually accepted
                        if msg_status == 'accepted':
                            print(f"✅ Template message accepted by WhatsApp API for {phone}")
                            print(f"   ⏳ Delivery may take 1-5 minutes")
                            print(f"   📱 Please check your WhatsApp after a few minutes")
                            # Print troubleshooting info
                            if message_id:
                                print_delivery_troubleshooting(phone, message_id, status)
                        elif msg_status == 'sent':
                            print(f"✅ Template message sent to {phone}")
                        elif msg_status == 'delivered':
                            print(f"✅✅ Message delivered to {phone}!")
                        elif msg_status == 'read':
                            print(f"✅✅✅ Message read by {phone}!")
                        else:
                            print(f"⚠️  Message status: {msg_status} (may indicate delivery issue)")
                            if message_id:
                                print_delivery_troubleshooting(phone, message_id, status)
                    
                    results.append({
                        "phone": phone,
                        "status": "success" if status == "200" else "warning",
                        "message_id": message_id,
                        "language": user_language,
                        "whatsapp_status": status,
                        "message_status": msg_status if 'msg_status' in locals() else None,
                        "error": error if error and error != 'None' else None
                    })
                else:
                    print(f"❌ No response received from WhatsApp API for {phone}")
                    results.append({
                        "phone": phone,
                        "status": "error",
                        "message": "No response from WhatsApp API"
                    })
            else:
                print(f"❌ Failed to prepare template request for {phone}")
                results.append({
                    "phone": phone,
                    "status": "error",
                    "message": "Failed to prepare request"
                })
        except Exception as e:
            print(f"❌ Error sending to {phone}: {str(e)}")
            results.append({
                "phone": phone,
                "status": "error",
                "message": str(e)
            })
    
    return results

async def main():
    leaderboard_df = await build_district_leaderboard_last_week_ist()
    top3_df = leaderboard_df.head(3)
    print("\nTop 3 Districts:\n", top3_df.to_string(index=False))

    phone_numbers = await fetch_phone_numbers_for_asha_and_test_users()
    print(f"Total recipients found: {len(phone_numbers)}")
    
    # Display what will be sent
    template_params = await format_leaderboard_as_template_parameters(top3_df, test_mode_3_params=TEST_MODE_3_PARAMS)
    print(f"\nTemplate Parameters: {template_params}")
    print(f"Template Name: leaderboardv2")
    if TEST_MODE_3_PARAMS:
        print(f"🧪 TEST MODE: Using 3 parameters (district, count, users)")
    else:
        print(f"📊 PRODUCTION MODE: Using 9 parameters (3 districts)")
    
    # PRODUCTION MODE: Send to all users (actual sending)
    print(f"\n🚀 PRODUCTION MODE: Sending template messages to {len(phone_numbers)} users")
    results = await send_leaderboard_template_messages(
        phone_numbers, 
        top3_df, 
        user_db_service, 
        message_db_service,
        test_mode_3_params=TEST_MODE_3_PARAMS
    )
    success_count = sum(1 for r in results if r.get("status") == "success")
    print(f"\n✅ Successfully sent {success_count} out of {len(results)} messages")

if __name__ == "__main__":
    asyncio.run(main())
