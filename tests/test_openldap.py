"""Tests for data_manipulation.openldap_.

validate_ldap_uri is pure stdlib and always runs; ldapsearch shells out to the
`ldapsearch` binary and is marked as an integration test.
"""

import pytest

from data_manipulation import openldap_


@pytest.mark.parametrize(
    "uri,valid",
    [
        ("ldap://ldap.example.com", True),
        ("ldaps://ldap.example.com:636", True),
        ("http://example.com", False),
        ("ldap://", False),
        ("not a uri", False),
        ("", False),
    ],
)
def test_validate_ldap_uri(uri, valid):
    assert openldap_.validate_ldap_uri(uri) is valid


@pytest.mark.integration
def test_ldapsearch_requires_binary():
    pytest.skip("ldapsearch requires a live LDAP server and the CLI binary")
