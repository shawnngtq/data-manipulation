"""Tests for data_manipulation.mysql_connector_python_.

execute_query needs the mysql-connector driver importable; the connection itself
is mocked so no MySQL server is contacted.
"""

import pytest

pytest.importorskip("mysql.connector")

from data_manipulation import mysql_connector_python_ as mysql_  # noqa: E402


def test_execute_query_select(mocker):
    conn = mocker.Mock()
    conn.is_connected.return_value = True
    conn.cursor.return_value.fetchall.return_value = [{"id": 1}]

    result = mysql_.execute_query(conn, "SELECT * FROM t", fetch=True)
    assert result == [{"id": 1}]


def test_execute_query_inactive_connection(mocker):
    conn = mocker.Mock()
    conn.is_connected.return_value = False
    with pytest.raises(mysql_.DatabaseError):
        mysql_.execute_query(conn, "SELECT 1")
