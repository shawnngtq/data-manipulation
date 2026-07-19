"""Generic S3 object registry and Iceberg ingestion ledger.

This module is self-contained: it imports nothing from Django or the host
application, so it can be lifted into another project as-is. The domain types and
the :class:`IngestionRepository` protocol are plain Python so the ingestion state
machine can be unit-tested without a database. The PostgreSQL adapter uses psycopg
directly rather than an ORM because the claim path depends on
``INSERT ... ON CONFLICT ... WHERE ... RETURNING`` and partial indexes.

Concurrency model: exactly one scheduled batch writer mutates a given Iceberg
table. The only real contention is cron overlap - a hung run still holding a
partition when the next run starts - and that is handled by the ``object_load``
primary key plus a lease, with no advisory locks required.

Partition identity is generic. A project declares the columns that identify one
partition load via :class:`DimensionColumn` (name + SQL type); those columns form
the ``object_load`` primary key and every ledger statement is generated from them.
An :class:`IngestionTarget` carries the concrete values as a mapping. A market app
might declare ``[source, data_type, trading_day]``; another project might declare
``[tenant, day]``.
"""

from __future__ import annotations

import datetime
import hashlib
import json
import logging
import uuid
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol

logger = logging.getLogger(__name__)

# Set on a load whose Iceberg commit can no longer be proven either way, because
# the snapshot carrying its ingestion ID may have been removed by snapshot expiry
# rather than never written. Re-ingesting could duplicate rows, so these are
# excluded from automatic re-claim and require an operator decision.
UNVERIFIABLE_COMMIT = "UnverifiableCommit"

ERROR_CODES = frozenset(
    {
        "IngestionFailed",
        "ObjectChanged",
        "ValidationFailed",
        UNVERIFIABLE_COMMIT,
    }
)

# libpq option keys safe to pass through from a Django ``DATABASES[...]["OPTIONS"]``
# (or equivalent) mapping to a connection. Shared: the repository builds psycopg
# kwargs from it, and the catalog manager filters the same OPTIONS into its
# SQLAlchemy URI.
CONNECT_OPTION_KEYS = frozenset(
    {
        "application_name",
        "connect_timeout",
        "keepalives",
        "keepalives_count",
        "keepalives_idle",
        "keepalives_interval",
        "options",
        "sslcert",
        "sslkey",
        "sslmode",
        "sslrootcert",
    }
)


class IngestionStatus(StrEnum):
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    ADOPTED = "ADOPTED"
    FAILED = "FAILED"


class IngestionOperation(StrEnum):
    APPEND = "APPEND"
    OVERWRITE = "OVERWRITE"
    ADOPT = "ADOPT"


@dataclass(frozen=True)
class DimensionColumn:
    """One declared partition-identity column: a SQL name and type.

    ``sql_type`` is emitted verbatim into DDL, so it must be a trusted developer
    constant (e.g. ``"TEXT"``, ``"DATE"``), never user input.
    """

    name: str
    sql_type: str = "TEXT"


@dataclass(frozen=True)
class S3ObjectRevision:
    """Immutable identity for one observed revision of an S3 object."""

    storage_client: str
    bucket: str
    key: str
    etag: str
    size: int
    last_modified: datetime.datetime
    version_id: str | None = None

    def __post_init__(self) -> None:
        normalized_etag = self.etag.strip().strip('"')
        if not normalized_etag:
            raise ValueError("S3 object ETag is required")
        if self.size < 0:
            raise ValueError("S3 object size cannot be negative")
        modified = self.last_modified
        if modified.tzinfo is None:
            modified = modified.replace(tzinfo=datetime.UTC)
        else:
            modified = modified.astimezone(datetime.UTC)
        object.__setattr__(self, "etag", normalized_etag)
        object.__setattr__(self, "last_modified", modified)

    @property
    def fingerprint(self) -> str:
        """Return a deterministic revision token for change detection.

        ETags are not guaranteed to be cryptographic content hashes, so the
        fingerprint also includes the object location, size, modification time,
        and version ID when the provider supplies one. ``sort_keys`` plus the
        compact ``separators`` give one canonical byte string per identity;
        ``json.dumps`` escapes any special characters inside the string values,
        so nothing in a key/etag can corrupt the JSON or the hash.
        """
        payload = json.dumps(
            {
                "storage_client": self.storage_client,
                "bucket": self.bucket,
                "key": self.key,
                "etag": self.etag,
                "size": self.size,
                "last_modified": self.last_modified.isoformat(),
                "version_id": self.version_id,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class IngestionTarget:
    """One Iceberg partition slice, identified by declared dimension values.

    ``dimensions`` is stored as a sorted tuple of ``(name, value)`` pairs so the
    target is hashable (usable as a dict key) and equal regardless of the order
    the mapping was built in. Values must be hashable (str/int/date/None).
    """

    table_identifier: str
    dimensions: tuple[tuple[str, Any], ...]

    def __post_init__(self) -> None:
        dims = self.dimensions
        if isinstance(dims, Mapping):
            dims = tuple(dims.items())
        object.__setattr__(
            self, "dimensions", tuple(sorted(dims, key=lambda kv: kv[0]))
        )

    def __getitem__(self, name: str) -> Any:
        for key, value in self.dimensions:
            if key == name:
                return value
        raise KeyError(name)

    def __getattr__(self, name: str) -> Any:
        # Convenience: expose each declared dimension as an attribute
        # (``target.trading_day`` == ``target["trading_day"]``). Only reached when
        # normal attribute lookup fails, so the real fields resolve first.
        if (
            name == "dimensions"
        ):  # not yet set during __init__; avoid recursion
            raise AttributeError(name)
        for key, value in self.dimensions:
            if key == name:
                return value
        raise AttributeError(name)

    def get(self, name: str, default: Any = None) -> Any:
        for key, value in self.dimensions:
            if key == name:
                return value
        return default

    @property
    def dimension_map(self) -> dict[str, Any]:
        return dict(self.dimensions)


@dataclass(frozen=True)
class PartitionLoad:
    """The single ledger row describing one partition's ingestion state.

    ``ingestion_id`` is regenerated on every successful claim and serves two
    purposes: it is stamped into the Iceberg snapshot summary as commit lineage,
    and it fences :meth:`IngestionRepository.complete` so a run whose lease
    expired cannot finalize work another run has since re-claimed.
    """

    target: IngestionTarget
    ingestion_id: uuid.UUID
    fingerprint: str
    operation: IngestionOperation
    status: IngestionStatus
    snapshot_ids: tuple[int, ...] = ()
    rows_written: int | None = None
    attempt_count: int = 0
    lease_expires_at: datetime.datetime | None = None
    error_code: str | None = None
    started_at: datetime.datetime | None = None

    @property
    def is_terminal(self) -> bool:
        return self.status in {
            IngestionStatus.COMPLETED,
            IngestionStatus.ADOPTED,
        }

    @property
    def lease_is_expired(self) -> bool:
        if self.status is not IngestionStatus.IN_PROGRESS:
            return False
        if self.lease_expires_at is None:
            return True
        return self.lease_expires_at < datetime.datetime.now(datetime.UTC)


class IngestionRepository(Protocol):
    """Persistence contract consumed by the Iceberg ingestion coordinator.

    The protocol bodies are ``...`` because a Protocol is only a typed interface;
    the concrete SQL lives in :class:`PostgresIngestionRepository`. The seam is
    what lets the coordinator run against an in-memory fake in tests, and what
    lets another project supply its own backend.

    Provisioning (``initialize_schema``) deliberately stays off this protocol: it
    needs a DDL-capable role, runs once from a management command, and is never
    called by the coordinator.
    """

    def upsert_inventory(self, objects: Sequence[S3ObjectRevision]) -> int: ...

    def sync_inventory(
        self,
        storage_client: str,
        bucket: str,
        prefix: str,
        objects: Sequence[S3ObjectRevision],
    ) -> dict[str, int]: ...

    def load_for(self, target: IngestionTarget) -> PartitionLoad | None: ...

    def claim(
        self,
        obj: S3ObjectRevision,
        target: IngestionTarget,
        operation: IngestionOperation,
        lease_seconds: int,
    ) -> PartitionLoad | None: ...

    def adopt(
        self,
        obj: S3ObjectRevision,
        target: IngestionTarget,
        snapshot_ids: Sequence[int] = (),
    ) -> PartitionLoad: ...

    def reconcile_committed(
        self,
        ingestion_id: uuid.UUID,
        snapshot_ids: Sequence[int],
    ) -> None: ...

    def complete(
        self,
        ingestion_id: uuid.UUID,
        snapshot_ids: Sequence[int],
        rows_written: int,
    ) -> None: ...

    def fail(
        self,
        ingestion_id: uuid.UUID,
        error_code: str,
        error_message: str = "",
    ) -> None: ...


# Non-dimension ledger columns returned by SELECT/RETURNING, in order after the
# table_identifier + declared dimension columns.
_TAIL_COLUMNS = (
    "ingestion_id",
    "fingerprint",
    "operation",
    "status",
    "snapshot_ids",
    "rows_written",
    "attempt_count",
    "lease_expires_at",
    "error_code",
    "started_at",
)

_S3_OBJECT_DDL = """
CREATE TABLE IF NOT EXISTS ingestion.s3_object (
    storage_client TEXT NOT NULL,
    bucket TEXT NOT NULL,
    object_key TEXT NOT NULL,
    etag TEXT NOT NULL,
    object_size BIGINT NOT NULL CHECK (object_size >= 0),
    object_last_modified TIMESTAMPTZ NOT NULL,
    version_id TEXT,
    fingerprint CHAR(64) NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    is_missing BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (storage_client, bucket, object_key)
);
"""

_OBJECT_LOAD_TAIL_DDL = """
    ingestion_id UUID NOT NULL,
    fingerprint CHAR(64) NOT NULL,
    operation TEXT NOT NULL CHECK (operation IN ('APPEND', 'OVERWRITE', 'ADOPT')),
    status TEXT NOT NULL CHECK (status IN ('IN_PROGRESS', 'COMPLETED', 'ADOPTED', 'FAILED')),
    snapshot_ids BIGINT[] NOT NULL DEFAULT '{}',
    rows_written BIGINT CHECK (rows_written IS NULL OR rows_written >= 0),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    lease_expires_at TIMESTAMPTZ,
    error_code TEXT,
    error_message TEXT,
    started_at TIMESTAMPTZ,
    resolved_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
"""

_OBJECT_LOAD_INDEXES_DDL = """
CREATE UNIQUE INDEX IF NOT EXISTS object_load_ingestion_id
ON ingestion.object_load (ingestion_id);

CREATE INDEX IF NOT EXISTS object_load_stale_leases
ON ingestion.object_load (lease_expires_at)
WHERE status = 'IN_PROGRESS';
"""


class PostgresIngestionRepository:
    """psycopg adapter backed by the catalog metadata database.

    One long-lived connection is reused for every call. The ingestion writer is a
    single-threaded batch process, so a connection pool would add background
    threads for no benefit; the connection is reopened transparently if it drops.

    The partition-identity columns are declared per application via
    ``dimension_columns``; the ``object_load`` primary key and every ledger
    statement are generated from them.
    """

    def __init__(
        self,
        database_config: Mapping[str, Any],
        dimension_columns: Sequence[DimensionColumn],
    ):
        if not dimension_columns:
            raise ValueError("At least one dimension column is required")
        self.database_config = dict(database_config)
        self.dimension_columns = tuple(dimension_columns)
        self._conn = None

    # -- identifier/SQL fragment helpers ---------------------------------

    @property
    def _dim_names(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.dimension_columns)

    def _dim_idents(self):
        from psycopg import sql

        return [sql.Identifier(name) for name in self._dim_names]

    def _identity_idents(self):
        from psycopg import sql

        return [sql.Identifier("table_identifier"), *self._dim_idents()]

    def _load_columns_sql(self):
        """Composed ``table_identifier, <dims...>, <tail...>`` column list."""
        from psycopg import sql

        idents = [
            *self._identity_idents(),
            *[sql.Identifier(name) for name in _TAIL_COLUMNS],
        ]
        return sql.SQL(", ").join(idents)

    # -- DDL --------------------------------------------------------------

    @staticmethod
    def _object_load_ddl(dimension_columns: Sequence[DimensionColumn]):
        from psycopg import sql

        identity = [
            sql.Identifier("table_identifier"),
            *[sql.Identifier(column.name) for column in dimension_columns],
        ]
        column_defs = sql.SQL(",\n    ").join(
            sql.SQL("{} {} NOT NULL").format(
                sql.Identifier(column.name), sql.SQL(column.sql_type)
            )
            for column in dimension_columns
        )
        return sql.SQL(
            "CREATE TABLE IF NOT EXISTS ingestion.object_load (\n"
            "    table_identifier TEXT NOT NULL,\n"
            "    {column_defs},\n"
            "{tail},\n"
            "    PRIMARY KEY ({pk}),\n"
            "    CHECK (status <> 'IN_PROGRESS' OR lease_expires_at IS NOT NULL)\n"
            ");"
        ).format(
            column_defs=column_defs,
            tail=sql.SQL(_OBJECT_LOAD_TAIL_DDL.strip("\n")),
            pk=sql.SQL(", ").join(identity),
        )

    @classmethod
    def _schema_statements(cls, dimension_columns: Sequence[DimensionColumn]):
        from psycopg import sql

        return [
            sql.SQL("CREATE SCHEMA IF NOT EXISTS ingestion;"),
            sql.SQL(_S3_OBJECT_DDL),
            cls._object_load_ddl(dimension_columns),
            sql.SQL(_OBJECT_LOAD_INDEXES_DDL),
        ]

    @classmethod
    def base_schema_sql(
        cls, dimension_columns: Sequence[DimensionColumn]
    ) -> str:
        """Return the core registry+ledger DDL for the given dimension columns.

        A host adapter that pins its dimension columns can call this to render the
        fresh-install DDL without constructing a repository (e.g. to append its own
        application-specific views).
        """
        return "\n".join(
            statement.as_string()
            for statement in cls._schema_statements(dimension_columns)
        )

    def schema_sql(self) -> str:
        """Return fresh-install DDL for review and external provisioning."""
        return self.base_schema_sql(self.dimension_columns)

    # -- connection -------------------------------------------------------

    def _connect_kwargs(self) -> dict[str, Any]:
        pg = self.database_config
        connection_options = {
            key: value
            for key, value in pg.get("OPTIONS", {}).items()
            if key in CONNECT_OPTION_KEYS
        }
        connection_options.setdefault(
            "application_name", "lakehouse-ingestion"
        )
        connection_options.setdefault("connect_timeout", 10)
        return {
            "dbname": pg.get("NAME"),
            "user": pg.get("USER"),
            "password": pg.get("PASSWORD"),
            "host": pg.get("HOST", "localhost"),
            "port": pg.get("PORT", 5432),
            **connection_options,
        }

    def _connect(self):
        """Return the shared connection, reopening it if it has dropped."""
        import psycopg

        if self._conn is None or self._conn.closed:
            self._conn = psycopg.connect(**self._connect_kwargs())
        return self._conn

    @contextmanager
    def _cursor(self):
        """Yield a cursor in a transaction: commit on success, roll back on error."""
        conn = self._connect()
        try:
            with conn.cursor() as cursor:
                yield cursor
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                logger.exception("Failed to roll back ingestion transaction")
            raise

    def close(self) -> None:
        """Close the shared connection. Safe to call more than once."""
        if self._conn is not None and not self._conn.closed:
            self._conn.close()
        self._conn = None

    def initialize_schema(self) -> None:
        """Create the ``ingestion`` schema, object registry, and ledger table."""
        with self._cursor() as cursor:
            for statement in self._schema_statements(self.dimension_columns):
                cursor.execute(statement)

    # -- inventory --------------------------------------------------------

    @staticmethod
    def _like_prefix(prefix: str) -> str:
        """Escape LIKE wildcards so a prefix match stays a literal prefix match."""
        escaped = (
            prefix.replace("\\", "\\\\")
            .replace("%", "\\%")
            .replace("_", "\\_")
        )
        return f"{escaped}%"

    @staticmethod
    def _upsert_inventory(cursor, objects: Sequence[S3ObjectRevision]) -> int:
        if not objects:
            return 0
        # unnest keeps this to a single round-trip; a per-object loop cost two
        # statements each, i.e. 50k round-trips for a full 25k-object listing.
        cursor.execute(
            """
            INSERT INTO ingestion.s3_object (
                storage_client, bucket, object_key, etag, object_size,
                object_last_modified, version_id, fingerprint
            )
            SELECT * FROM unnest(
                %s::text[], %s::text[], %s::text[], %s::text[], %s::bigint[],
                %s::timestamptz[], %s::text[], %s::text[]
            )
            ON CONFLICT (storage_client, bucket, object_key) DO UPDATE SET
                etag = EXCLUDED.etag,
                object_size = EXCLUDED.object_size,
                object_last_modified = EXCLUDED.object_last_modified,
                version_id = EXCLUDED.version_id,
                fingerprint = EXCLUDED.fingerprint,
                last_seen_at = clock_timestamp(),
                is_missing = FALSE
            """,
            (
                [obj.storage_client for obj in objects],
                [obj.bucket for obj in objects],
                [obj.key for obj in objects],
                [obj.etag for obj in objects],
                [obj.size for obj in objects],
                [obj.last_modified for obj in objects],
                [obj.version_id for obj in objects],
                [obj.fingerprint for obj in objects],
            ),
        )
        return len(objects)

    def upsert_inventory(self, objects: Sequence[S3ObjectRevision]) -> int:
        """Record the current revision of each observed object."""
        with self._cursor() as cursor:
            return self._upsert_inventory(cursor, objects)

    @staticmethod
    def _mark_missing(
        cursor,
        storage_client: str,
        bucket: str,
        prefix: str,
        observed_keys: Sequence[str],
    ) -> int:
        cursor.execute(
            """
            UPDATE ingestion.s3_object
            SET is_missing = TRUE, last_seen_at = clock_timestamp()
            WHERE storage_client = %s AND bucket = %s AND NOT is_missing
              AND object_key LIKE %s ESCAPE '\\'
              AND NOT (object_key = ANY(%s::text[]))
            """,
            (
                storage_client,
                bucket,
                PostgresIngestionRepository._like_prefix(prefix),
                list(observed_keys),
            ),
        )
        return cursor.rowcount

    def sync_inventory(
        self,
        storage_client: str,
        bucket: str,
        prefix: str,
        objects: Sequence[S3ObjectRevision],
    ) -> dict[str, int]:
        """Apply one complete prefix listing: upsert present, retire absent.

        Both statements share a transaction so the inventory never shows a state
        in which objects have been retired but their replacements are not visible.
        """
        with self._cursor() as cursor:
            synced = self._upsert_inventory(cursor, objects)
            retired = self._mark_missing(
                cursor,
                storage_client,
                bucket,
                prefix,
                [obj.key for obj in objects],
            )
        return {"objects_synced": synced, "objects_retired": retired}

    # -- ledger -----------------------------------------------------------

    def _target_values(self, target: IngestionTarget) -> tuple[Any, ...]:
        return (
            target.table_identifier,
            *(target[name] for name in self._dim_names),
        )

    def _load_from_row(self, row) -> PartitionLoad:
        dim_count = len(self.dimension_columns)
        dimensions = {
            name: row[1 + index] for index, name in enumerate(self._dim_names)
        }
        tail = row[1 + dim_count :]
        return PartitionLoad(
            target=IngestionTarget(
                table_identifier=row[0],
                dimensions=dimensions,
            ),
            ingestion_id=tail[0],
            fingerprint=tail[1],
            operation=IngestionOperation(tail[2]),
            status=IngestionStatus(tail[3]),
            snapshot_ids=tuple(tail[4] or ()),
            rows_written=tail[5],
            attempt_count=tail[6],
            lease_expires_at=tail[7],
            error_code=tail[8],
            started_at=tail[9],
        )

    def load_for(self, target: IngestionTarget) -> PartitionLoad | None:
        """Return the single ledger row for a partition, if one exists."""
        from psycopg import sql

        where = sql.SQL(" AND ").join(
            sql.SQL("{} = %s").format(ident)
            for ident in self._identity_idents()
        )
        query = sql.SQL(
            "SELECT {columns} FROM ingestion.object_load WHERE {where}"
        ).format(columns=self._load_columns_sql(), where=where)
        with self._cursor() as cursor:
            cursor.execute(query, self._target_values(target))
            row = cursor.fetchone()
        return self._load_from_row(row) if row else None

    def claim(
        self,
        obj: S3ObjectRevision,
        target: IngestionTarget,
        operation: IngestionOperation,
        lease_seconds: int,
    ) -> PartitionLoad | None:
        """Atomically take ownership of a partition, or return None.

        The WHERE clause on the conflict branch is the whole concurrency story: a
        partition is claimable only when it is not actively held, or when the
        holder's lease has expired. An UnverifiableCommit load is never re-claimed
        automatically because re-ingesting it could duplicate rows.
        """
        from psycopg import sql

        identity = self._identity_idents()
        identity_cols = sql.SQL(", ").join(identity)
        identity_placeholders = sql.SQL(", ").join(
            sql.SQL("%s") for _ in identity
        )
        conflict_cols = sql.SQL(", ").join(identity)
        query = sql.SQL(
            """
            INSERT INTO ingestion.object_load (
                {identity_cols}, ingestion_id,
                fingerprint, operation, status, attempt_count, lease_expires_at,
                started_at
            ) VALUES (
                {identity_placeholders}, %s, %s, %s, 'IN_PROGRESS', 1,
                clock_timestamp() + make_interval(secs => %s), clock_timestamp()
            )
            ON CONFLICT ({conflict_cols})
            DO UPDATE SET
                ingestion_id = EXCLUDED.ingestion_id,
                fingerprint = EXCLUDED.fingerprint,
                operation = EXCLUDED.operation,
                status = 'IN_PROGRESS',
                attempt_count = ingestion.object_load.attempt_count + 1,
                lease_expires_at = EXCLUDED.lease_expires_at,
                snapshot_ids = '{{}}',
                rows_written = NULL,
                error_code = NULL,
                error_message = NULL,
                started_at = clock_timestamp(),
                resolved_at = NULL,
                updated_at = clock_timestamp()
            WHERE (
                    ingestion.object_load.status <> 'IN_PROGRESS'
                    OR ingestion.object_load.lease_expires_at < clock_timestamp()
                  )
              AND ingestion.object_load.error_code IS DISTINCT FROM %s
            RETURNING {columns}
            """
        ).format(
            identity_cols=identity_cols,
            identity_placeholders=identity_placeholders,
            conflict_cols=conflict_cols,
            columns=self._load_columns_sql(),
        )
        with self._cursor() as cursor:
            cursor.execute(
                query,
                (
                    *self._target_values(target),
                    uuid.uuid4(),
                    obj.fingerprint,
                    operation.value,
                    lease_seconds,
                    UNVERIFIABLE_COMMIT,
                ),
            )
            row = cursor.fetchone()
        return self._load_from_row(row) if row else None

    def adopt(
        self,
        obj: S3ObjectRevision,
        target: IngestionTarget,
        snapshot_ids: Sequence[int] = (),
    ) -> PartitionLoad:
        """Record pre-ledger partition data as already ingested, without a rewrite."""
        from psycopg import sql

        identity = self._identity_idents()
        identity_cols = sql.SQL(", ").join(identity)
        identity_placeholders = sql.SQL(", ").join(
            sql.SQL("%s") for _ in identity
        )
        conflict_cols = sql.SQL(", ").join(identity)
        query = sql.SQL(
            """
            INSERT INTO ingestion.object_load (
                {identity_cols}, ingestion_id,
                fingerprint, operation, status, snapshot_ids, attempt_count,
                resolved_at
            ) VALUES (
                {identity_placeholders}, %s, %s, 'ADOPT', 'ADOPTED', %s, 0,
                clock_timestamp()
            )
            ON CONFLICT ({conflict_cols})
            DO NOTHING
            RETURNING {columns}
            """
        ).format(
            identity_cols=identity_cols,
            identity_placeholders=identity_placeholders,
            conflict_cols=conflict_cols,
            columns=self._load_columns_sql(),
        )
        with self._cursor() as cursor:
            cursor.execute(
                query,
                (
                    *self._target_values(target),
                    uuid.uuid4(),
                    obj.fingerprint,
                    list(snapshot_ids),
                ),
            )
            row = cursor.fetchone()
        if row:
            return self._load_from_row(row)
        existing = self.load_for(target)
        if existing is None:
            raise RuntimeError(f"Could not adopt partition {target}")
        return existing

    def reconcile_committed(
        self,
        ingestion_id: uuid.UUID,
        snapshot_ids: Sequence[int],
    ) -> None:
        """Finalize a load proven committed by Iceberg snapshot metadata."""
        self._resolve(
            ingestion_id,
            snapshot_ids=snapshot_ids,
            rows_written=None,
            failure="Active ingestion disappeared during reconciliation",
        )

    def complete(
        self,
        ingestion_id: uuid.UUID,
        snapshot_ids: Sequence[int],
        rows_written: int,
    ) -> None:
        self._resolve(
            ingestion_id,
            snapshot_ids=snapshot_ids,
            rows_written=rows_written,
            failure="Ingestion lease was lost before completion",
        )

    def _resolve(
        self,
        ingestion_id: uuid.UUID,
        snapshot_ids: Sequence[int],
        rows_written: int | None,
        failure: str,
    ) -> None:
        with self._cursor() as cursor:
            cursor.execute(
                """
                UPDATE ingestion.object_load
                SET status = 'COMPLETED',
                    snapshot_ids = %s,
                    rows_written = COALESCE(%s, rows_written),
                    lease_expires_at = NULL,
                    resolved_at = clock_timestamp(),
                    updated_at = clock_timestamp()
                WHERE ingestion_id = %s AND status = 'IN_PROGRESS'
                RETURNING ingestion_id
                """,
                (list(snapshot_ids), rows_written, ingestion_id),
            )
            if cursor.fetchone() is None:
                raise RuntimeError(failure)

    def fail(
        self,
        ingestion_id: uuid.UUID,
        error_code: str,
        error_message: str = "",
    ) -> None:
        if error_code not in ERROR_CODES:
            raise ValueError(f"Unsupported ingestion error code: {error_code}")
        with self._cursor() as cursor:
            cursor.execute(
                """
                UPDATE ingestion.object_load
                SET status = 'FAILED',
                    lease_expires_at = NULL,
                    error_code = %s,
                    error_message = %s,
                    resolved_at = clock_timestamp(),
                    updated_at = clock_timestamp()
                WHERE ingestion_id = %s AND status = 'IN_PROGRESS'
                """,
                (error_code, error_message[:2000], ingestion_id),
            )
