"""Tests for data_manipulation.postgres_.

execute_query is exercised with a mock connection (no driver/DB needed).
create_connection needs a live PostgreSQL server and is an integration skip.
"""

import pytest

from data_manipulation import postgres_


def test_execute_query_commits_and_returns_rowcount(mocker):
    conn = mocker.Mock()
    cursor = conn.cursor.return_value
    cursor.rowcount = 3

    affected = postgres_.execute_query(conn, "UPDATE t SET x = 1")
    assert affected == 3
    cursor.execute.assert_called_once_with("UPDATE t SET x = 1", None)
    conn.commit.assert_called_once()


def test_execute_query_no_commit(mocker):
    conn = mocker.Mock()
    conn.cursor.return_value.rowcount = 0
    postgres_.execute_query(conn, "SELECT 1", commit=False)
    conn.commit.assert_not_called()


@pytest.mark.integration
def test_create_connection_requires_server():
    pytest.skip("create_connection requires a live PostgreSQL server")
