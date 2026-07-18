"""Repo-root pytest configuration.

``--doctest-modules`` (see ``[tool.pytest.ini_options]``) collects doctests from
every ``data_manipulation/*.py``. We only want to *execute* examples that are
robustly runnable, so ignore the rest here. Their ``Examples:`` blocks still
render in the mkdocs API reference — they are just not auto-verified.

Auto-verified modules (not ignored): base.py, beautifulsoup_.py, sqlalchemy_.py.
"""

collect_ignore = [
    "data_manipulation/__init__.py",
    "data_manipulation/_version.py",
    "data_manipulation/pandas_.py",
    "data_manipulation/pyspark_.py",
    "data_manipulation/boto3_.py",
    "data_manipulation/kerberos_.py",
    "data_manipulation/openldap_.py",
    "data_manipulation/cryptography_.py",
    "data_manipulation/mysql_connector_python_.py",
    "data_manipulation/postgres_.py",
    "data_manipulation/smtplib_.py",
    "data_manipulation/polars_.py",
]
