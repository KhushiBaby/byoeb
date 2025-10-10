from enum import Enum

class ErrorMessage(str, Enum):
    INVALID_USER_TYPE = (
        "Invalid user type. Available user types are {regular_types} and {expert_types}"
    )
    USER_TYPE_REQUIRED = "User type must be provided"
    USER_LANGUAGE_REQUIRED = "User language must be provided"
    PHONE_NUMBER_ID_REQUIRED = "Phone number id must be provided"
    CANNOT_HAVE_AUDIENCE = "Cannot have list of audience"
    CANNOT_HAVE_EXPERTS = "Cannot have list of experts"
    CANNOT_BE_OWN_EXPERT = "Cannot be in their own list of experts"
    CANNOT_BE_OWN_AUDIENCE = "Cannot be in their own list of audience"
    INVALID_EXPERT_TYPE = (
        "Invalid expert user type. Available expert user types are {expert_types}"
    )
    PHONE_NUMBER_IDS_LIST_REQUIRED = "Provide list of phone number ids"