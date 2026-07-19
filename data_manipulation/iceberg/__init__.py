"""Generic, framework-free Iceberg ingestion toolkit.

This subpackage has no Django or host-application dependencies, so it
can be lifted into another project as a directory copy. It provides three layers:

- :mod:`.ledger` - the S3 object registry + partition ingestion ledger (types,
  the :class:`~data_manipulation.iceberg.ledger.IngestionRepository` protocol, and a psycopg
  PostgreSQL adapter whose partition identity is declared per application).
- :mod:`.catalog` - :class:`~data_manipulation.iceberg.catalog.IcebergCatalog`, a PyIceberg
  SQL-catalog manager, plus :class:`~data_manipulation.iceberg.catalog.TableSpec`.
- :mod:`.ingest` - :class:`~data_manipulation.iceberg.ingest.IngestionCoordinator`, the
  restatement-aware state machine, driven by an injected
  :class:`~data_manipulation.iceberg.ingest.SourcePlan`.
"""

from .catalog import DEFAULT_MAINTENANCE_PROPERTIES, IcebergCatalog, TableSpec
from .ingest import IngestionCoordinator, SourcePlan
from .ledger import (
    CONNECT_OPTION_KEYS,
    ERROR_CODES,
    UNVERIFIABLE_COMMIT,
    DimensionColumn,
    IngestionOperation,
    IngestionRepository,
    IngestionStatus,
    IngestionTarget,
    PartitionLoad,
    PostgresIngestionRepository,
    S3ObjectRevision,
)

__all__ = [
    "CONNECT_OPTION_KEYS",
    "DEFAULT_MAINTENANCE_PROPERTIES",
    "ERROR_CODES",
    "UNVERIFIABLE_COMMIT",
    "DimensionColumn",
    "IcebergCatalog",
    "IngestionCoordinator",
    "IngestionOperation",
    "IngestionRepository",
    "IngestionStatus",
    "IngestionTarget",
    "PartitionLoad",
    "PostgresIngestionRepository",
    "S3ObjectRevision",
    "SourcePlan",
    "TableSpec",
]
