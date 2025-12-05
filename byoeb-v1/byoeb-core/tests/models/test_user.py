import pytest
from pydantic import TypeAdapter, ValidationError

from byoeb_core.models.byoeb.user import PhoneNumberId


adapter = TypeAdapter(PhoneNumberId)

def test_10_digit_number_fails():
    with pytest.raises(ValidationError):
        adapter.validate_python("9876543210")


def test_91_prefixed_number_succeeds():
    phone_number = "919876543210"
    assert adapter.validate_python(phone_number) == phone_number


def test_short_special_numbers_succeed():
    assert adapter.validate_python("9999876543210")
    assert adapter.validate_python("19876543210")


def test_bad_characters_fail():
    with pytest.raises(ValidationError):
        adapter.validate_python("91abc123456")


def test_too_long_number_fails():
    with pytest.raises(ValidationError):
        adapter.validate_python("12345678901234")
