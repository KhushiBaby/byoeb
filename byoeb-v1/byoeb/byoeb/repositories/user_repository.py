from abc import ABC, abstractmethod
from typing import AsyncIterator, Dict, Any, Optional, List
from datetime import datetime

from byoeb.repositories.base_repository import BaseRepository

class UserRepository(BaseRepository, ABC):
    """Repository interface for user-related database operations."""

    @abstractmethod
    async def find_user_by_id(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Find a user by their ID."""
        pass

    @abstractmethod
    async def find_user_by_phone_number(self, phone_number: str) -> Optional[Dict[str, Any]]:
        """Find a user by their phone number."""
        pass

    @abstractmethod
    def find_users_by_type(self, user_type: str) -> AsyncIterator[Dict[str, Any]]:
        """Find users by their type (e.g., 'asha', 'anm', 'others')."""
        pass

    @abstractmethod
    def find_users_by_types(self, user_types: List[str]) -> AsyncIterator[Dict[str, Any]]:
        """Find users by multiple types."""
        pass

    @abstractmethod
    def find_test_users_by_types(self, user_types: List[str]) -> AsyncIterator[Dict[str, Any]]:
        """Find users by multiple types, restricted to test users only."""
        pass

    @abstractmethod
    def find_users_by_district(self, district: str) -> AsyncIterator[Dict[str, Any]]:
        """Find users by district."""
        pass

    @abstractmethod
    def find_test_users(self) -> AsyncIterator[Dict[str, Any]]:
        """Find all ASHA workers and test users."""
        pass

    @abstractmethod
    def find_asha_and_test_users(self) -> AsyncIterator[Dict[str, Any]]:
        """Find all ASHA workers and test users."""
        pass

    @abstractmethod
    def find_users_by_phone_numbers(self, phone_numbers: List[str]) -> AsyncIterator[Dict[str, Any]]:
        """Find users by a list of phone numbers."""
        pass

    @abstractmethod
    def find_users_by_ids(self, user_ids: List[str]) -> AsyncIterator[Dict[str, Any]]:
        """Find users by a list of user IDs."""
        pass

    @abstractmethod
    def find_all_users(self) -> AsyncIterator[Dict[str, Any]]:
        """Find all users (UTC-normalised)."""
        pass

    @abstractmethod
    async def count_users_by_type(self, user_type: str) -> int:
        """Count users by type."""
        pass

    @abstractmethod
    async def count_users_by_district(self, district: str) -> int:
        """Count users by district."""
        pass

    @abstractmethod
    def get_user_statistics_by_district(self) -> AsyncIterator[Dict[str, Any]]:
        """Get aggregated user statistics grouped by district."""
        pass

    @abstractmethod
    def find_active_users_in_timeframe(self, 
                                       start_timestamp: datetime, 
                                       end_timestamp: datetime) -> AsyncIterator[Dict[str, Any]]:
        """Find users who were active within a specific timeframe."""
        pass

    @abstractmethod
    async def update_user_activity_timestamp(self, user_id: str, timestamp: datetime) -> bool:
        """Update user's activity timestamp."""
        pass

    @abstractmethod
    async def update_user_last_conversations(self, user_id: str, conversations: List[Dict[str, Any]]) -> bool:
        """Update user's last conversations."""
        pass
