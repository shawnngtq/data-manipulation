"""Tests for data_manipulation.kerberos_.

Input validation runs anywhere; the actual kinit call needs Kerberos installed
and a real keytab, so it is an integration skip.
"""

import pytest

from data_manipulation import kerberos_


def test_keytab_valid_missing_args():
    with pytest.raises(ValueError):
        kerberos_.keytab_valid("", "user@REALM.COM")


def test_keytab_valid_missing_file(tmp_path):
    with pytest.raises(ValueError):
        kerberos_.keytab_valid(
            str(tmp_path / "absent.keytab"), "user@REALM.COM"
        )


@pytest.mark.integration
def test_keytab_valid_against_kinit():
    pytest.skip("kinit requires Kerberos and a valid keytab/principal")
