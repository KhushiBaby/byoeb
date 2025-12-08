from byoeb.models.consensus import Consensus
import byoeb.services.chat.constants as constants
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, TYPE_CHECKING
from byoeb.factory import MongoDBFactory
from byoeb.services.databases.mongo_db.base import BaseMongoDBService
from pydantic import PositiveInt
from byoeb_core.models.byoeb.message_context import ByoebMessageContext
from collections import Counter, defaultdict
from zoneinfo import ZoneInfo
import pandas as pd

if TYPE_CHECKING:
    from byoeb.services.leaderboard.time_window_strategies import TimeWindowStrategy

IST = ZoneInfo("Asia/Kolkata")

class MessageMongoDBService(BaseMongoDBService):
    """
    Service class for message-related MongoDB operations.
    Consolidates all message-related functionality including leaderboard, statistics, and bulk messaging.
    """

    def __init__(self, config, mongo_db_factory: MongoDBFactory, user_db_service=None):
        super().__init__(config, mongo_db_factory)
        self.collection_name = self._config["databases"]["mongo_db"]["message_collection"]
        self._user_db_service = user_db_service
        # Note: _get_repository_factory() is now provided by BaseMongoDBService

    def _district_of(self, user_obj) -> Optional[str]:
        """
        Extract district from user object.

        Args:
            user_obj: User object (can be SimpleNamespace or dict)

        Returns:
            Optional[str]: District name or None if not found/unknown
        """
        if not user_obj:
            return None
        loc = getattr(user_obj, "user_location", None) or {}
        dist = loc.get("district") if hasattr(loc, "get") else getattr(loc, "district", None)
        return str(dist).strip() if dist and str(dist).strip().lower() != "unknown" else None

    async def build_district_leaderboard(
        self, 
        message_categories: Optional[List[str]] = None, 
        processing_batch_size: int = 1000,
        time_window_strategy: Optional['TimeWindowStrategy'] = None
    ) -> pd.DataFrame:
        """
        Builds a leaderboard of districts based on message activity for the specified time window.

        Args:
            message_categories: Optional list of message categories to filter by
            processing_batch_size: Number of documents to process in each batch
            time_window_strategy: Optional time window strategy (uses default if not provided)

        Returns:
            pd.DataFrame: Sorted leaderboard with district statistics
        """
        # Lazy import to avoid circular dependency
        from byoeb.services.leaderboard.time_window_strategies import TimeWindowFactory
        strategy = time_window_strategy or TimeWindowFactory.create_strategy('week')
        start_timestamp, end_timestamp = strategy.calculate_window()

        # Get repository instances
        repository_factory = await self._get_repository_factory()
        message_repository = await repository_factory.get_message_repository()

        # Define projection for required fields only
        required_fields_only = {"_id": 0, "message_data.user.user_id": 1, "message_data.incoming_timestamp": 1}

        # Get messages using repository
        message_documents = await message_repository.find_messages_by_time_range(
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            message_categories=message_categories,
            projection=required_fields_only
        )

        # Sort messages by timestamp (descending)
        message_documents.sort(key=lambda x: x.get("message_data", {}).get("incoming_timestamp", 0), reverse=True)

        if not self._user_db_service:
            raise ValueError("user_db_service must be provided for leaderboard functionality")

        user_objects_cache = {}
        district_message_counts = Counter()
        district_unique_users = defaultdict(set)
        district_first_message_timestamp = {}
        district_last_message_timestamp = {}

        # Process messages in batches
        for i in range(0, len(message_documents), processing_batch_size):
            message_batch = message_documents[i:i + processing_batch_size]

            await self._user_db_service.hydrate_users(message_batch, user_objects_cache)

            for message_document in message_batch:
                message_data = message_document.get("message_data", {})
                user_id = message_data.get("user", {}).get("user_id")
                message_timestamp = message_data.get("incoming_timestamp")

                if not isinstance(message_timestamp, int) or message_timestamp < start_timestamp or message_timestamp > end_timestamp:
                    continue

                user_object = user_objects_cache.get(user_id)
                user_district = self._district_of(user_object)
                if not user_district:
                    continue

                district_message_counts[user_district] += 1
                if user_id:
                    district_unique_users[user_district].add(user_id)

                district_first_message_timestamp[user_district] = min(district_first_message_timestamp.get(user_district, message_timestamp), message_timestamp)
                district_last_message_timestamp[user_district] = max(district_last_message_timestamp.get(user_district, message_timestamp), message_timestamp)

        leaderboard_rows = [
            {
                "district": district_name,
                "message_count": message_count,
                "unique_users": len(district_unique_users[district_name]),
                "first_seen": datetime.fromtimestamp(district_first_message_timestamp[district_name]).strftime("%d-%m-%Y %H:%M:%S"),
                "last_seen": datetime.fromtimestamp(district_last_message_timestamp[district_name]).strftime("%d-%m-%Y %H:%M:%S")
            }
            for district_name, message_count in district_message_counts.items()
        ]

        if not leaderboard_rows:
            return pd.DataFrame(
                columns=["district", "message_count", "unique_users", "first_seen", "last_seen"]
            )

        return pd.DataFrame(leaderboard_rows).sort_values(by=["message_count", "unique_users"], ascending=False, ignore_index=True)

    async def build_district_leaderboard_last_week_ist(
        self, 
        message_categories: Optional[List[str]] = None, 
        processing_batch_size: int = 1000
    ) -> pd.DataFrame:
        """
        Builds a leaderboard of districts based on message activity from the previous week in IST timezone.
        This method is kept for backward compatibility.

        Args:
            message_categories: Optional list of message categories to filter by
            processing_batch_size: Number of documents to process in each batch

        Returns:
            pd.DataFrame: Sorted leaderboard with district statistics
        """
        return await self.build_district_leaderboard(message_categories, processing_batch_size)

    async def get_message_statistics_by_district(
        self,
        start_timestamp: int,
        end_timestamp: int,
        message_categories: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Get message statistics grouped by district for a specific time range.

        Args:
            start_timestamp: Start timestamp for the query
            end_timestamp: End timestamp for the query
            message_categories: Optional list of message categories to filter by

        Returns:
            Dict[str, Any]: Statistics including total messages, unique users, etc.
        """
        repository_factory = await self._get_repository_factory()
        message_repository = await repository_factory.get_message_repository()

        # Get messages using repository
        message_documents = await message_repository.find_messages_by_time_range(
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            message_categories=message_categories
        )

        # Process statistics
        total_messages = len(message_documents)
        unique_users = set()
        districts = set()

        for message_document in message_documents:
            message_data = message_document.get("message_data", {})
            user_id = message_data.get("user", {}).get("user_id")
            if user_id:
                unique_users.add(user_id)

        return {
            "total_messages": total_messages,
            "unique_users": len(unique_users),
            "time_range": {
                "start": datetime.fromtimestamp(start_timestamp).strftime("%Y-%m-%d %H:%M:%S"),
                "end": datetime.fromtimestamp(end_timestamp).strftime("%Y-%m-%d %H:%M:%S")
            },
            "message_categories": message_categories or "all"
        }

    async def get_messages_by_user(
        self,
        user_id: str,
        start_timestamp: Optional[int] = None,
        end_timestamp: Optional[int] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get messages for a specific user within a time range.

        Args:
            user_id: The user ID to query
            start_timestamp: Optional start timestamp
            end_timestamp: Optional end timestamp
            limit: Maximum number of messages to return

        Returns:
            List[Dict[str, Any]]: List of message documents
        """
        repository_factory = await self._get_repository_factory()
        message_repository = await repository_factory.get_message_repository()

        query = {"message_data.user.user_id": user_id}

        if start_timestamp and end_timestamp:
            query["message_data.incoming_timestamp"] = {
                "$gte": start_timestamp,
                "$lte": end_timestamp
            }

        return await message_repository.find_all(query, limit=limit)

    async def get_message_count_by_category(
        self,
        start_timestamp: int,
        end_timestamp: int
    ) -> Dict[str, int]:
        """
        Get message count grouped by category for a specific time range.

        Args:
            start_timestamp: Start timestamp for the query
            end_timestamp: End timestamp for the query

        Returns:
            Dict[str, int]: Category counts
        """
        repository_factory = await self._get_repository_factory()
        message_repository = await repository_factory.get_message_repository()

        # Get all messages in the time range
        message_documents = await message_repository.find_messages_by_time_range(
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp
        )

        category_counts = Counter()
        for message_document in message_documents:
            message_data = message_document.get("message_data", {})
            category = message_data.get("message_category", "unknown")
            category_counts[category] += 1

        return dict(category_counts)

    async def send_bulk_messages(
        self, 
        phone_numbers: List[str], 
        message_text: str,
        debug_mode: bool = True,
        test_mode: bool = False,
        test_phone_number: str = "917567071072"
    ) -> List[Dict[str, Any]]:
        """
        Send bulk messages to multiple phone numbers.

        Args:
            phone_numbers: List of phone numbers to send messages to
            message_text: The text content to send
            debug_mode: If True, prints detailed WhatsApp payloads instead of sending
            test_mode: If True, only sends to test_phone_number instead of all numbers
            test_phone_number: Phone number to use when test_mode is True

        Returns:
            List[Dict[str, Any]]: List of results for each message sent
        """
        from datetime import datetime

        results = []

        # If test_mode is enabled, only process the test phone number
        target_phones = [test_phone_number] if test_mode else phone_numbers

        for phone in target_phones:
            message_payload = {
                "object": "whatsapp_business_account",
                "entry": [
                    {
                        "changes": [
                            {
                                "value": {
                                    "messaging_product": "whatsapp",
                                    "contacts": [{"wa_id": phone}],
                                    "messages": [
                                        {
                                            "from": phone,
                                            "id": f"custom-{phone}-{int(datetime.now().timestamp())}",
                                            "timestamp": str(int(datetime.now().timestamp())),
                                            "type": "text",
                                            "text": {"body": message_text}
                                        }
                                    ]
                                }
                            }
                        ]
                    }
                ]
            }

            if debug_mode:
                # DEMO MODE: Print payload instead of sending
                print("\n--- WhatsApp Message Payload ---")
                print(f"To: {phone}")
                print("Payload:", message_payload)
                print("--- End Payload ---\n")

                results.append({
                    "phone": phone,
                    "status": "debug_mode",
                    "message": "Payload printed (not sent)",
                    "payload": message_payload
                })
            else:
                # ACTUAL SENDING MODE: Send real WhatsApp messages
                try:
                    from byoeb.chat_app.configuration.dependency_setup import channel_client_factory
                    from byoeb_core.models.byoeb.message_context import ByoebMessageContext, MessageContext, ReplyContext, MessageTypes
                    from byoeb_core.models.byoeb.user import User
                    from datetime import datetime, timezone

                    # Create a proper ByoebMessageContext for outgoing message
                    byoeb_message = ByoebMessageContext(
                        channel_type="whatsapp",
                        message_category="leaderboard",
                        user=User(
                            user_id=f"leaderboard-{phone}",
                            user_name="Leaderboard Bot",
                            user_location={},
                            user_language="en",
                            user_type="bot",
                            phone_number_id=phone,
                            test_user=False,
                            experts={},
                            audience=[],
                            created_timestamp=int(datetime.now(timezone.utc).timestamp()),
                            activity_timestamp=int(datetime.now(timezone.utc).timestamp()),
                            last_conversations=[],
                            additional_info={}
                        ),
                        message_context=MessageContext(
                            message_id=f"leaderboard-{phone}-{int(datetime.now().timestamp())}",
                            message_type=MessageTypes.REGULAR_TEXT.value,
                            message_source_text=message_text,
                            message_english_text=message_text,
                            media_info=None,
                            additional_info={}
                        ),
                        reply_context=None,
                        cross_conversation_id=None,
                        cross_conversation_context=None,
                        incoming_timestamp=int(datetime.now(timezone.utc).timestamp()),
                        outgoing_timestamp=int(datetime.now(timezone.utc).timestamp())
                    )

                    # Get WhatsApp service and send the message
                    from byoeb.services.channel.whatsapp import WhatsAppService
                    whatsapp_service = WhatsAppService(channel_client_factory)
                    requests = whatsapp_service.prepare_requests(byoeb_message)

                    if requests:
                        responses, message_ids = await whatsapp_service.send_requests(requests)
                        print(f"✅ Sent to {phone} - Message ID: {message_ids[0] if message_ids else 'Unknown'}")
                        results.append({
                            "phone": phone,
                            "status": "success",
                            "message": f"Message sent successfully",
                            "message_id": message_ids[0] if message_ids else None,
                            "response": str(responses[0]) if responses else None
                        })
                    else:
                        print(f"❌ No requests generated for {phone}")
                        results.append({
                            "phone": phone,
                            "status": "error",
                            "message": "No requests generated"
                        })

                except Exception as e:
                    print(f"❌ Error sending to {phone}: {str(e)}")
                    import traceback
                    traceback.print_exc()
                    results.append({
                        "phone": phone,
                        "status": "error",
                        "message": f"Exception occurred: {str(e)}",
                        "error": str(e)
                    })

        return results

    async def get_bot_messages_by_ids(self, bot_message_ids: List[str]) -> List[ByoebMessageContext]:
        """Fetch multiple bot messages from the database via repository."""
        repository_factory = await self._get_repository_factory()
        message_repository = await repository_factory.get_message_repository()
        messages_obj = await message_repository.find_all({"_id": {"$in": bot_message_ids}})
        return [ByoebMessageContext(**msg_obj["message_data"]) for msg_obj in messages_obj]
    
    async def get_bot_messages_by_status(self, status: str) -> List[ByoebMessageContext]:
        """Fetch bot messages with the given status via repository."""
        repository_factory = await self._get_repository_factory()
        message_repository = await repository_factory.get_message_repository()
        messages_obj = await message_repository.find_all({"message_data.message_context.additional_info.status": status})
        return [ByoebMessageContext(**msg_obj["message_data"]) for msg_obj in messages_obj]

    async def get_latest_bot_messages_by_timestamp(self, timestamp: str, length: PositiveInt):
        """Fetch bot messages with timestamps greater than the given timestamp; preserve prior in-Python sort behavior."""
        repository_factory = await self._get_repository_factory()
        message_repository = await repository_factory.get_message_repository()
        messages_obj = await message_repository.find_all({"timestamp": {"$gt": timestamp}}, limit=length)
        messages_obj_sorted = sorted(messages_obj, key=lambda msg: msg.get("timestamp"), reverse=True)
        return [ByoebMessageContext(**msg_obj["message_data"]) for msg_obj in messages_obj_sorted]

    def correction_update_query(
        self,
        byoeb_user_messages: List[ByoebMessageContext],
        byoeb_expert_message: ByoebMessageContext
    ):
        if byoeb_expert_message.reply_context is None:
            return []
        for byoeb_user_message in byoeb_user_messages:
            reply_context = byoeb_user_message.reply_context
            update_id = reply_context.additional_info.get(constants.UPDATE_ID)
            reply_context.reply_id = update_id
            byoeb_user_message.reply_context = reply_context
        expert_modified_timestamp = byoeb_expert_message.reply_context.additional_info.get(constants.MODIFIED_TIMESTAMP)
        update_data = {
            "$set":{
                "message_data.message_context.additional_info.correction_en_text": byoeb_expert_message.reply_context.additional_info.get(constants.CORRECTION_EN),
                "message_data.message_context.additional_info.correction_source_text": byoeb_expert_message.reply_context.additional_info.get(constants.CORRECTION_SOURCE),
                "message_data.message_context.additional_info.modified_timestamp": expert_modified_timestamp,
            }
        }
        expert_update_queries = [({"_id": byoeb_expert_message.reply_context.reply_id}, update_data)]
        user_update_queries = []
        for byoeb_user_message in byoeb_user_messages:
            update_data = {
                "$set":{
                    "message_data.message_context.additional_info.corrected_en_text": byoeb_user_message.message_context.message_english_text,
                    "message_data.message_context.additional_info.corrected_source_text": byoeb_user_message.message_context.message_source_text
                }
            }
            user_update_queries.append(({"_id": byoeb_user_message.reply_context.reply_id}, update_data))
        return expert_update_queries + user_update_queries
    
    def idk_status_update_query(
        self,
        byoeb_message: ByoebMessageContext,
    ):
        message_id = byoeb_message.message_context.message_id
        status = byoeb_message.message_context.additional_info.get(constants.STATUS, None)
        consensus_answer_en = ""
        consensus_answer_source = ""
        if status is None:
            return
        if status == constants.RESOLVED:
            consensus_answer_en = byoeb_message.message_context.additional_info.get(constants.CONSENSUS_ANSWER_EN)
            consensus_answer_source = byoeb_message.message_context.additional_info.get(constants.CONSENSUS_ANSWER_SOURCE)
        update_data = {
            "$set":{
                f"message_data.message_context.additional_info.{constants.STATUS}": status,
                f"message_data.message_context.additional_info.{constants.CONSENSUS_ANSWER_EN}": consensus_answer_en,
                f"message_data.message_context.additional_info.{constants.CONSENSUS_ANSWER_SOURCE}": consensus_answer_source,
                f"message_data.message_context.additional_info.{constants.MODIFIED_TIMESTAMP}": str(int(datetime.now(timezone.utc).timestamp()))
            }
        }
        return ({"_id": message_id}, update_data)
    
    def audio_idk_status_update_query(
        self,
        byoeb_user_message: ByoebMessageContext,
    ):
        message_id = byoeb_user_message.reply_context.additional_info.get(constants.BOT_AUDIO_IDK_MESSAGE_ID)
        print("message_id", message_id)
        status = byoeb_user_message.reply_context.additional_info.get(constants.STATUS, None)
        print("status", status)
        if status is None:
            return []
        update_data = {
            "$set":{
                f"message_data.message_context.additional_info.{constants.STATUS}": status,
                f"message_data.message_context.additional_info.{constants.MODIFIED_TIMESTAMP}": str(int(datetime.now(timezone.utc).timestamp()))
            }
        }
        return [({"_id": message_id}, update_data)]
        
    def verification_status_update_query(
        self,
        byoeb_user_messages: List[ByoebMessageContext],
        byoeb_expert_message: ByoebMessageContext
    ):
        for byoeb_user_message in byoeb_user_messages:
            reply_context = byoeb_user_message.reply_context
            update_id = reply_context.additional_info.get(constants.UPDATE_ID)
            reply_context.reply_id = update_id
            byoeb_user_message.reply_context = reply_context
        verification_status_param = constants.VERIFICATION_STATUS
        expert_verification_status = byoeb_expert_message.reply_context.additional_info.get(verification_status_param)
        expert_modified_timestamp = byoeb_expert_message.reply_context.additional_info.get(constants.MODIFIED_TIMESTAMP)
        user_verification_status = byoeb_user_messages[0].reply_context.additional_info.get(verification_status_param)
        user_modified_timestamp = byoeb_user_messages[0].reply_context.additional_info.get(constants.MODIFIED_TIMESTAMP)
        update_data = {
            "$set":{
                "message_data.message_context.additional_info.verification_status": expert_verification_status,
                "message_data.message_context.additional_info.modified_timestamp": expert_modified_timestamp,
                "message_data.cross_conversation_context.messages_context.$[].message_context.additional_info.verification_status": user_verification_status
            }
        }
        expert_update_queries = [({"_id": byoeb_expert_message.reply_context.reply_id}, update_data)]
        user_update_queries = []
        for byoeb_user_message in byoeb_user_messages:
            update_data = {
                "$set":{
                    "message_data.message_context.additional_info.verification_status": user_verification_status,
                    "message_data.message_context.additional_info.modified_timestamp": user_modified_timestamp
                }
            }
            user_update_queries.append(({"_id": byoeb_user_message.reply_context.reply_id}, update_data))
        return expert_update_queries + user_update_queries
    
    def consensus_update_query(self, user_message: ByoebMessageContext, cross_convs: List[ByoebMessageContext]):
        consensus_list = user_message.message_context.additional_info.get(constants.CONSENSUS)
        if consensus_list is None:
            consensus_list = []
        for cross_conv in cross_convs:
            consensus = Consensus(
                user_id = cross_conv.user.user_id,
                status = constants.WAITING,
                message_id = cross_conv.message_context.message_id,
            )
            consensus_list.append(consensus.model_dump())
        update_data = {
            "$set":{
                "message_data.message_context.additional_info.consensus": consensus_list
            }
        }
        return [({"_id": user_message.message_context.message_id}, update_data)]

    def message_create_queries(self, byoeb_messages: List[ByoebMessageContext]) -> List[Dict[str, Any]]:
        """Generate create queries for messages."""
        if not byoeb_messages:
            return []
        return [
            {
                "_id": message.message_context.message_id,
                "message_data": message.model_dump(),
                "timestamp": str(int(datetime.now(timezone.utc).timestamp())),
            }
            for message in byoeb_messages
        ]
    
    def aggregate_queries(
        self,
        results: List[Dict[str, Any]]
    ):
        new_message_queries = {
            constants.CREATE: [],
            constants.UPDATE: [],
        }
        for queries, _, err in results:
            if err is not None or queries is None:
                continue
            message_queries = queries.get(constants.MESSAGE_DB_QUERIES, {})
            if message_queries is not None and message_queries != {}:
                message_create_queries = message_queries.get(constants.CREATE,[])
                message_update_queries = message_queries.get(constants.UPDATE,[])
                new_message_queries[constants.CREATE].extend(message_create_queries)
                new_message_queries[constants.UPDATE].extend(message_update_queries)
        
        return new_message_queries
    
    async def execute_queries(self, queries: Dict[str, Any]):
        """Execute message database queries via repository (insert_many, bulk_update)."""
        if not queries:
            return

        repository_factory = await self._get_repository_factory()
        message_repository = await repository_factory.get_message_repository()
        if queries.get("create"):
            await message_repository.insert_many(queries["create"])
        if queries.get("update"):
            await message_repository.bulk_update(queries["update"])