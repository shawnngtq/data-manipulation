"""Tests for data_manipulation.boto3_ (mock the AWS client; no real calls)."""

import pytest

pytest.importorskip("boto3")

from data_manipulation import boto3_  # noqa: E402

pytestmark = pytest.mark.integration


def test_list_s3_bucket_files_mocked(mocker):
    paginator = mocker.Mock()
    paginator.paginate.return_value = [
        {"Contents": [{"Key": "a.txt"}, {"Key": "b.txt"}]},
    ]
    client = mocker.Mock()
    client.get_paginator.return_value = paginator
    mocker.patch("boto3.client", return_value=client)

    files = boto3_.list_s3_bucket_files("my-bucket")
    assert files == ["a.txt", "b.txt"]
