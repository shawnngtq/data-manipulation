"""Import-smoke tests for the data_manipulation.iceberg subpackage.

The toolkit's import-time dependencies (polars, pyiceberg) are pulled in
lazily via ``pytest.importorskip`` so the suite degrades to skips instead of
errors when a dependency is not installed.
"""

import pytest

pytest.importorskip("polars")
pytest.importorskip("pyiceberg")


def test_subpackage_imports():
    from data_manipulation import iceberg

    assert iceberg.__all__


def test_public_api_is_reachable():
    from data_manipulation import iceberg

    for name in iceberg.__all__:
        assert hasattr(iceberg, name), f"{name} missing from iceberg namespace"


def test_key_classes_exposed():
    from data_manipulation.iceberg import (
        IcebergCatalog,
        IngestionCoordinator,
        PostgresIngestionRepository,
        SourcePlan,
        TableSpec,
    )

    assert all(
        callable(obj)
        for obj in (
            IcebergCatalog,
            IngestionCoordinator,
            PostgresIngestionRepository,
            SourcePlan,
            TableSpec,
        )
    )
