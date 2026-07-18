from data_manipulation import (
    base,
    beautifulsoup_,
    boto3_,
    cryptography_,
    django_,
    kerberos_,
    mysql_connector_python_,
    openldap_,
    pandas_,
    postgres_,
    pyspark_,
    smtplib_,
    sqlalchemy_,
)

try:
    from ._version import __version__
except ImportError:
    try:
        from importlib.metadata import version

        __version__ = version("data_manipulation")
    except Exception:
        __version__ = "0.0.0"
