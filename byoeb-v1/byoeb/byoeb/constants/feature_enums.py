from enum import Enum

class FeatureFlag(Enum):
    # cache user queries in runtime so ASHAbot loads known questions from its local memory
    CACHE_MESSAGES = "cache_messages"