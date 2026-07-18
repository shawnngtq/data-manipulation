"""Shared pytest fixtures for the data_manipulation test suite.

Heavy/optional third-party libraries (pyspark, …) are pulled in lazily
via ``pytest.importorskip`` so the suite degrades to skips instead of errors when
a dependency is not installed.
"""

import pytest


@pytest.fixture
def sample_files(tmp_path):
    """Recreate the old ``test_base_folder`` fixture: 5 empty ``.py`` files.

    Replaces the temp-folder setup that used to live in ``base.py``'s
    ``if __name__ == "__main__"`` block.
    """
    for i in range(1, 6):
        (tmp_path / f"test{i}.py").touch()
    return tmp_path


@pytest.fixture(scope="session")
def spark():
    """A local single-threaded SparkSession, or skip if PySpark is absent."""
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("data_manipulation-tests")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
