"""Tests for data_manipulation.sqlalchemy_ (skipped without sqlalchemy).

Only the pure URL builder is unit-tested; create_sqlalchemy_engine needs a live
database and is covered by an integration test elsewhere.
"""

import pytest

pytest.importorskip("sqlalchemy")

from data_manipulation import sqlalchemy_  # noqa: E402


def test_create_sqlalchemy_url():
    url = sqlalchemy_.create_sqlalchemy_url(
        drivername="postgresql+psycopg",
        host="localhost",
        dbname="mydb",
        user="admin",
        password="secret",
        port=5432,
    )
    # str(url) masks the password; render with hide_password=False to see it.
    assert (
        url.render_as_string(hide_password=False)
        == "postgresql+psycopg://admin:secret@localhost:5432/mydb"
    )
    assert url.username == "admin" and url.password == "secret"
    assert url.host == "localhost" and url.port == 5432
    assert url.database == "mydb"


def test_create_sqlalchemy_url_with_query():
    url = sqlalchemy_.create_sqlalchemy_url(
        drivername="mysql+mysqlconnector",
        host="db",
        dbname="app",
        user="u",
        password="p",
        query={"ssl_ca": "/etc/ca.pem"},
    )
    assert url.query["ssl_ca"] == "/etc/ca.pem"
    assert url.database == "app"
