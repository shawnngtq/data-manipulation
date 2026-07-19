"""Generic PyIceberg SQL-catalog manager.

Self-contained: no Django, no host-application imports. Configuration (catalog
name, warehouse URI, metadata-store DB config, and already-built PyIceberg
``s3.*`` FileIO properties) is passed to the constructor. A host application
subclasses :class:`IcebergCatalog` to resolve that configuration from its own
settings while inheriting all catalog plumbing.

A table's layout is described by a :class:`TableSpec` (schema, partition spec,
sort order, properties, and the sort column names used to physically cluster
writes). ``create_table``/``append``/``overwrite_partition``/``validate_layout``
all drive off a spec, so no market-specific table shape is baked in here.
"""

from __future__ import annotations

import datetime
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

try:
    import polars as pl

    HAS_POLARS = True
except ImportError:
    pl = None
    HAS_POLARS = False

# Iceberg metadata-retention properties (PyIceberg honors these on commit).
# ``delete-after-commit`` trims old metadata.json on each write; the cap is how
# many previous versions to keep. Snapshot/data-file cleanup is separate and
# explicit (see ``expire_snapshots``).
DEFAULT_MAINTENANCE_PROPERTIES = {
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "100",
}


@dataclass(frozen=True)
class TableSpec:
    """Declarative layout for one Iceberg table.

    ``sort_columns`` are the frame columns writes are pre-sorted by (PyIceberg
    does not auto-sort appends to the table's sort order), which is what keeps
    each data file's per-column min/max tight for effective pruning.
    """

    identifier: str
    schema: Any
    partition_spec: Any
    sort_order: Any
    properties: Mapping[str, str] = field(default_factory=dict)
    sort_columns: Sequence[str] = ()


class IcebergCatalog:
    """PyIceberg SQL-catalog manager (metadata store + warehouse plumbing)."""

    def __init__(
        self,
        catalog_name: str,
        warehouse: str,
        db_config: Mapping[str, Any],
        s3_properties: Mapping[str, str],
        snapshot_retention_days: int = 7,
        maintenance_properties: Mapping[str, str] | None = None,
    ):
        self.catalog_name = catalog_name
        self.warehouse = warehouse
        self.db_config = dict(db_config)
        self.s3_properties = dict(s3_properties)
        self.snapshot_retention_days = snapshot_retention_days
        self.maintenance_properties = dict(
            maintenance_properties
            if maintenance_properties is not None
            else DEFAULT_MAINTENANCE_PROPERTIES
        )
        self._catalog = None

    def _catalog_uri(self) -> str:
        """Build a SQLAlchemy URI for the SQL catalog's metadata store.

        Credentials are URL-encoded so special characters cannot corrupt the URI.
        """
        from urllib.parse import quote_plus, urlencode

        # The connect-option allowlist lives with the ledger. Relative import so
        # the package stays free of host-application coupling.
        from .ledger import CONNECT_OPTION_KEYS

        pg = self.db_config
        user = quote_plus(str(pg.get("USER", "")))
        password = quote_plus(str(pg.get("PASSWORD", "")))
        host = pg.get("HOST", "localhost")
        port = pg.get("PORT", 5432)
        name = pg.get("NAME", "")
        uri = f"postgresql+psycopg://{user}:{password}@{host}:{port}/{name}"
        options = {
            key: value
            for key, value in pg.get("OPTIONS", {}).items()
            if key in CONNECT_OPTION_KEYS
        }
        return f"{uri}?{urlencode(options)}" if options else uri

    def load_catalog(self):
        """Load (and cache) the PyIceberg SQL catalog."""
        if self._catalog is not None:
            return self._catalog
        if not self.warehouse:
            raise ValueError(
                "No warehouse location configured; set one "
                "(e.g. s3://your-bucket/iceberg/)."
            )
        from pyiceberg.catalog.sql import SqlCatalog

        properties = {
            "uri": self._catalog_uri(),
            "warehouse": self.warehouse,
            **self.s3_properties,
        }
        self._catalog = SqlCatalog(self.catalog_name, **properties)
        return self._catalog

    def ensure_namespace(self, namespace: str) -> None:
        """Create the namespace if it does not already exist."""
        self.load_catalog().create_namespace_if_not_exists(namespace)

    def load_table(self, table_identifier: str):
        """Load a fresh writable table handle from the configured catalog."""
        return self.load_catalog().load_table(table_identifier)

    def create_table(self, spec: TableSpec):
        """Create the table described by ``spec`` if it does not exist.

        Only the initial layout is defined. If the table exists, PyIceberg returns
        it unchanged; schema/partition/sort evolution must be an explicit migration
        (check drift with :meth:`validate_layout`).
        """
        catalog = self.load_catalog()
        namespace = spec.identifier.rpartition(".")[0]
        if namespace:
            self.ensure_namespace(namespace)
        return catalog.create_table_if_not_exists(
            spec.identifier,
            schema=spec.schema,
            partition_spec=spec.partition_spec,
            sort_order=spec.sort_order,
            properties=dict(spec.properties),
        )

    def list_tables(self) -> list[str]:
        """List all table identifiers across all namespaces in the catalog."""
        catalog = self.load_catalog()
        identifiers: list[str] = []
        for namespace in catalog.list_namespaces():
            for ident in catalog.list_tables(namespace):
                identifiers.append(".".join(ident))
        return identifiers

    def metadata_location(self, table_identifier: str) -> str:
        """Return the current metadata JSON location for a table."""
        return self.load_table(table_identifier).metadata_location

    # -- layout signatures / validation -----------------------------------

    @staticmethod
    def _schema_signature(schema) -> list[dict[str, Any]]:
        return [
            {
                "id": field_.field_id,
                "name": field_.name,
                "type": str(field_.field_type),
                "required": field_.required,
            }
            for field_ in schema.fields
        ]

    @staticmethod
    def _partition_signature(spec) -> list[dict[str, Any]]:
        return [
            {
                "source_id": field_.source_id,
                "field_id": field_.field_id,
                "transform": str(field_.transform),
                "name": field_.name,
            }
            for field_ in spec.fields
        ]

    @staticmethod
    def _sort_signature(sort_order) -> list[dict[str, Any]]:
        return [
            {
                "source_id": field_.source_id,
                "transform": str(field_.transform),
                "direction": str(field_.direction),
                "null_order": str(field_.null_order),
            }
            for field_ in sort_order.fields
        ]

    def validate_layout(self, spec: TableSpec) -> dict[str, Any]:
        """Report active table-layout drift without changing table metadata.

        Only the current schema, default partition spec, default sort order, and
        the spec's own properties are compared. Historical specs and unrelated
        engine properties are valid Iceberg metadata and are not treated as drift.
        """
        table = self.load_table(spec.identifier)
        expected = {
            "schema": self._schema_signature(spec.schema),
            "partition_spec": self._partition_signature(spec.partition_spec),
            "sort_order": self._sort_signature(spec.sort_order),
            "properties": dict(spec.properties),
        }
        actual = {
            "schema": self._schema_signature(table.schema()),
            "partition_spec": self._partition_signature(table.spec()),
            "sort_order": self._sort_signature(table.sort_order()),
            "properties": {
                key: table.properties.get(key)
                for key in expected["properties"]
            },
        }
        mismatches = [
            {
                "component": component,
                "expected": value,
                "actual": actual[component],
            }
            for component, value in expected.items()
            if actual[component] != value
        ]
        return {
            "table": spec.identifier,
            "valid": not mismatches,
            "mismatches": mismatches,
        }

    # -- maintenance ------------------------------------------------------

    def ensure_maintenance_properties(self, table_identifier: str) -> None:
        """Idempotently set metadata-retention properties on an existing table."""
        table = self.load_table(table_identifier)
        with table.transaction() as tx:
            tx.set_properties(**self.maintenance_properties)

    def expire_snapshots(
        self, table_identifier: str, older_than_days: int | None = None
    ) -> int:
        """Expire snapshots older than ``older_than_days`` and drop their files."""
        if older_than_days is None:
            older_than_days = self.snapshot_retention_days
        table = self.load_table(table_identifier)
        before = len(table.metadata.snapshots)
        cutoff = datetime.datetime.now(datetime.UTC) - datetime.timedelta(
            days=older_than_days
        )
        table.maintenance.expire_snapshots().older_than(cutoff).commit()
        after = len(table.refresh().metadata.snapshots)
        return before - after

    def maintain(
        self, table_identifier: str, older_than_days: int | None = None
    ) -> dict:
        """Ensure metadata-retention properties, then expire old snapshots."""
        if older_than_days is None:
            older_than_days = self.snapshot_retention_days
        cutoff = datetime.datetime.now(datetime.UTC) - datetime.timedelta(
            days=older_than_days
        )
        self.ensure_maintenance_properties(table_identifier)
        expired = self.expire_snapshots(table_identifier, older_than_days)
        return {
            "table": table_identifier,
            "older_than_days": older_than_days,
            "expired_before": cutoff.isoformat(),
            "snapshots_expired": expired,
        }

    # -- writes -----------------------------------------------------------

    @staticmethod
    def _commit(table, write, snapshot_properties: dict[str, str] | None):
        """Run a table mutation and return the snapshot IDs it published.

        Diffing the snapshot log around the commit is exact and costs no extra
        metadata fetch (PyIceberg updates ``table.metadata`` in place on commit).
        """
        before = {snapshot.snapshot_id for snapshot in table.snapshots()}
        write(snapshot_properties or {})
        return tuple(
            snapshot.snapshot_id
            for snapshot in table.snapshots()
            if snapshot.snapshot_id not in before
        )

    @staticmethod
    def _sorted_arrow(df: pl.DataFrame, table, sort_columns: Sequence[str]):
        """Return ``df`` as an Arrow table cast to the Iceberg schema.

        Pre-sorts to the table sort order (PyIceberg does not sort appends or
        overwrites), so the written data files are physically clustered and carry
        tight per-file min/max stats. The cast aligns column order and types.
        """
        columns = [column for column in sort_columns if column in df.columns]
        if columns and len(columns) == len(sort_columns):
            df = df.sort(list(columns))
        return df.to_arrow().cast(table.schema().as_arrow())

    def append(
        self,
        df: pl.DataFrame,
        spec: TableSpec,
        snapshot_properties: dict[str, str] | None = None,
        table=None,
    ) -> tuple[int, ...]:
        """Append a Polars DataFrame to the table described by ``spec``."""
        if table is None:
            table = self.load_table(spec.identifier)
        arrow_table = self._sorted_arrow(df, table, spec.sort_columns)
        return self._commit(
            table,
            lambda properties: table.append(
                arrow_table, snapshot_properties=properties
            ),
            snapshot_properties,
        )

    def overwrite_partition(
        self,
        df: pl.DataFrame,
        spec: TableSpec,
        partition_filter,
        snapshot_properties: dict[str, str] | None = None,
        minimum_row_ratio: float | None = None,
        table=None,
    ) -> tuple[int, ...]:
        """Atomically replace the rows matching ``partition_filter`` with ``df``.

        A filtered overwrite publishes deletion and replacement through the catalog
        commit rather than exposing an intermediate empty partition. When
        ``minimum_row_ratio`` is set and the partition currently holds rows, the
        new frame must clear that fraction of the existing count (a sanity floor
        against a truncated or corrupt restatement).
        """
        if table is None:
            table = self.load_table(spec.identifier)
        if minimum_row_ratio is not None:
            existing_count = table.scan(row_filter=partition_filter).count()
            if (
                existing_count
                and df.height < existing_count * minimum_row_ratio
            ):
                raise ValueError(
                    f"Overwrite has {df.height} rows against {existing_count} "
                    f"existing; below the {minimum_row_ratio:.0%} floor"
                )
        arrow_table = self._sorted_arrow(df, table, spec.sort_columns)
        return self._commit(
            table,
            lambda properties: table.overwrite(
                arrow_table,
                overwrite_filter=partition_filter,
                snapshot_properties=properties,
            ),
            snapshot_properties,
        )

    def partition_has_rows(self, table, partition_filter) -> bool:
        """Return whether the partition described by ``partition_filter`` holds rows."""
        if table.current_snapshot() is None:
            return False
        rows = table.scan(
            row_filter=partition_filter,
            limit=1,
        ).to_arrow()
        return len(rows) > 0

    def snapshot_ids_with_property(
        self, table_identifier: str, key: str, value: str
    ) -> tuple[int, ...]:
        """Return snapshot IDs whose summary carries ``key == value``.

        Reserved for the ingestion recovery path: it re-reads ``metadata.json`` and
        scans every snapshot, so it must not be called in the steady path.
        """
        table = self.load_table(table_identifier)
        return tuple(
            snapshot.snapshot_id
            for snapshot in table.snapshots()
            if snapshot.summary and snapshot.summary.get(key) == value
        )

    # -- inventory --------------------------------------------------------

    def inventory(self) -> pl.DataFrame:
        """Return an Iceberg-native inventory of catalog tables (one row each)."""
        catalog = self.load_catalog()
        rows: list[dict[str, Any]] = []
        for identifier in self.list_tables():
            namespace, _, table_name = identifier.rpartition(".")
            try:
                table = catalog.load_table(identifier)
                snapshot = table.current_snapshot()
                snapshot_id = snapshot.snapshot_id if snapshot else None
                record_count = (
                    int(snapshot.summary.get("total-records", 0))
                    if snapshot
                    else 0
                )
                last_updated = (
                    datetime.datetime.fromtimestamp(
                        snapshot.timestamp_ms / 1000, tz=datetime.UTC
                    )
                    if snapshot
                    else None
                )
                partition_spec = (
                    ", ".join(field_.name for field_ in table.spec().fields)
                    or "(unpartitioned)"
                )
            except Exception:
                import logging

                logging.getLogger(__name__).warning(
                    "Failed to read Iceberg table %s", identifier
                )
                snapshot_id = None
                record_count = 0
                last_updated = None
                partition_spec = ""
            rows.append(
                {
                    "namespace": namespace,
                    "table": table_name,
                    "partition_spec": partition_spec,
                    "snapshot_id": str(snapshot_id) if snapshot_id else "",
                    "record_count": record_count,
                    "last_updated": last_updated,
                }
            )
        return pl.DataFrame(
            rows,
            schema={
                "namespace": pl.Utf8,
                "table": pl.Utf8,
                "partition_spec": pl.Utf8,
                "snapshot_id": pl.Utf8,
                "record_count": pl.Int64,
                "last_updated": pl.Datetime(time_zone="UTC"),
            },
        )
