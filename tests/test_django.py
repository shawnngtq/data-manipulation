"""Tests for data_manipulation.django_ (skipped without Django configured)."""

import pytest

from data_manipulation import django_


def test_validate_email(django_settings):
    assert (
        django_.django_validate_email("valid@email.com") == "valid@email.com"
    )
    assert (
        django_.django_validate_email(
            "valid@email.com", whitelist_domains=["email.com"]
        )
        == "valid@email.com"
    )
    assert (
        django_.django_validate_email(
            "valid@email.com", whitelist_domains=["other.com"]
        )
        is None
    )
    assert django_.django_validate_email("not-an-email") is None


def test_validate_url(django_settings):
    assert (
        django_.django_validate_url("https://example.com")
        == "https://example.com"
    )
    assert django_.django_validate_url("not a url") is None


def test_validate_phone(django_settings):
    pytest.importorskip("phonenumber_field")
    assert django_.django_validate_phone("+1234567890") == "+1234567890"
    assert django_.django_validate_phone("invalid") is None


def test_get_django_countries_dict(django_settings):
    pytest.importorskip("django_countries")
    code_name, name_code = django_.get_django_countries_dict()
    assert "US" in code_name
    assert code_name["US"] == code_name["US"].upper()  # names are uppercased
    assert name_code[code_name["US"]] == "US"  # round-trips
