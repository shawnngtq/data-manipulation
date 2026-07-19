"""Generic Iceberg ingestion coordinator.

Self-contained: no Django, no host-application imports. The coordinator owns the
restatement-aware state machine (skip unchanged, adopt pre-ledger partitions,
append new partitions, overwrite changed ones) and crash recovery, driving off an
:class:`~data_manipulation.iceberg.ledger.IngestionRepository` and an
:class:`~data_manipulation.iceberg.catalog.IcebergCatalog`. Everything domain-specific -
how a key maps to a partition, how a source object is read and normalized, how a
frame is validated, and how a partition is expressed as an Iceberg row filter - is
injected through a :class:`SourcePlan`.
"""

from __future__ import annotations

import datetime
import logging
import uuid
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

try:
    import polars as pl

    HAS_POLARS = True
except ImportError:
    pl = None
    HAS_POLARS = False

from .catalog import IcebergCatalog, TableSpec
from .ledger import (
    UNVERIFIABLE_COMMIT,
    IngestionOperation,
    IngestionRepository,
    IngestionTarget,
    PartitionLoad,
    S3ObjectRevision,
)

logger = logging.getLogger(__name__)


def _noop_assert(obj: S3ObjectRevision) -> None:
    return None


@dataclass(frozen=True)
class SourcePlan:
    """The domain-specific strategy the coordinator injects at each step.

    Callables:
        key_to_target: Map an object to its partition target, or ``None`` to
            quarantine it as unparseable.
        read: Read a source object into a Polars frame (should fetch at most
            ``max_rows + 1`` rows so the row cap can be enforced).
        normalize: Reshape a read frame to the table schema.
        validate_frame: Raise if a normalized frame is unfit to ingest.
        partition_filter: Build the Iceberg row filter for a target (used to test
            whether the partition already holds rows).
        append: Commit ``df`` as a new partition; returns the published snapshot
            IDs. Receives an optional pre-loaded table handle to reuse.
        overwrite: Atomically replace the target partition with ``df`` (this is
            where a restatement row-count floor lives, if any); returns the
            published snapshot IDs.
        assert_object_unchanged: Optional guard raised if the object changed
            between listing and read (defaults to a no-op).

    Config:
        max_object_bytes / max_rows: hard per-object limits (quarantine on breach).
        lease_seconds: claim lease length.
        snapshot_property_namespace: prefix for the lineage keys written into each
            snapshot summary (``{ns}.ingestion-id`` etc.). Must stay stable for a
            given table so crash recovery can still find historical snapshots.
    """

    key_to_target: Callable[[S3ObjectRevision], IngestionTarget | None]
    read: Callable[[S3ObjectRevision, IngestionTarget], pl.DataFrame]
    normalize: Callable[[pl.DataFrame, IngestionTarget], pl.DataFrame]
    validate_frame: Callable[[pl.DataFrame, IngestionTarget], None]
    partition_filter: Callable[[IngestionTarget], Any]
    append: Callable[..., tuple[int, ...]]
    overwrite: Callable[..., tuple[int, ...]]
    assert_object_unchanged: Callable[[S3ObjectRevision], None] = _noop_assert
    max_object_bytes: int = 2 * 1024 * 1024 * 1024
    max_rows: int = 3_000_000
    lease_seconds: int = 3600
    snapshot_property_namespace: str = "lakehouse"

    @property
    def ingestion_id_property(self) -> str:
        return f"{self.snapshot_property_namespace}.ingestion-id"


class IngestionCoordinator:
    """Restatement-aware ingestion state machine for one Iceberg table."""

    def __init__(
        self,
        catalog: IcebergCatalog,
        repository: IngestionRepository,
        spec: TableSpec,
        plan: SourcePlan,
    ):
        self.catalog = catalog
        self.repository = repository
        self.spec = spec
        self.plan = plan

    # -- planning ---------------------------------------------------------

    def _plan_objects(
        self, objects: Sequence[S3ObjectRevision]
    ) -> tuple[
        list[tuple[S3ObjectRevision, IngestionTarget]], list[dict[str, str]]
    ]:
        """Pair each object with its target, setting aside ones that cannot be ingested.

        Unusable objects are quarantined rather than raised. A rolling-lookback
        schedule has to keep making progress when the provider drops an unexpected
        file into the prefix; aborting the run would stall ingestion indefinitely.
        """
        quarantined: list[dict[str, str]] = []
        targeted: list[tuple[S3ObjectRevision, IngestionTarget]] = []
        for obj in objects:
            target = self.plan.key_to_target(obj)
            if target is None:
                quarantined.append(
                    {"key": obj.key, "reason": "UnparseableKey"}
                )
                continue
            if obj.size > self.plan.max_object_bytes:
                quarantined.append(
                    {"key": obj.key, "reason": "ObjectTooLarge"}
                )
                continue
            targeted.append((obj, target))

        # A partition mapping to several objects is ambiguous: there is no way to
        # know which one should own it, so none of them may.
        target_counts = Counter(target for _, target in targeted)
        planned: list[tuple[S3ObjectRevision, IngestionTarget]] = []
        for obj, target in targeted:
            if target_counts[target] > 1:
                quarantined.append(
                    {
                        "key": obj.key,
                        "reason": f"DuplicatePartition:{target.dimension_map}",
                    }
                )
                continue
            planned.append((obj, target))
        return planned, quarantined

    # -- recovery ---------------------------------------------------------

    def _snapshot_ids_for_ingestion(
        self, ingestion_id: uuid.UUID
    ) -> tuple[int, ...]:
        """Find snapshots stamped with an ingestion ID (recovery path only).

        Delegates to :meth:`IcebergCatalog.snapshot_ids_with_property`, which
        re-reads ``metadata.json`` and scans every snapshot - quadratic if called
        per file. The steady path gets its snapshot IDs from the commit instead.
        """
        return self.catalog.snapshot_ids_with_property(
            self.spec.identifier,
            self.plan.ingestion_id_property,
            str(ingestion_id),
        )

    def _commit_is_unverifiable(self, load: PartitionLoad) -> bool:
        """Report whether a missing ingestion snapshot is ambiguous rather than absent.

        Once a load is older than the snapshot retention window, "no snapshot
        carries this ingestion ID" stops meaning "the write never happened" - the
        snapshot may simply have been expired by scheduled maintenance.
        """
        if load.started_at is None:
            return False
        age = datetime.datetime.now(datetime.UTC) - load.started_at
        return age > datetime.timedelta(
            days=self.catalog.snapshot_retention_days
        )

    def _recover_stuck_load(self, load: PartitionLoad) -> PartitionLoad | None:
        """Resolve a load left IN_PROGRESS by a run that died mid-flight."""
        snapshot_ids = self._snapshot_ids_for_ingestion(load.ingestion_id)
        if snapshot_ids:
            logger.info(
                "Reconciling committed-but-unrecorded ingestion %s for %s",
                load.ingestion_id,
                load.target.dimension_map,
            )
            self.repository.reconcile_committed(
                load.ingestion_id, snapshot_ids
            )
            return self.repository.load_for(load.target)
        if self._commit_is_unverifiable(load):
            logger.warning(
                "Ingestion %s for %s is older than snapshot retention with no "
                "snapshot; marking %s for operator review rather than risking "
                "duplicate rows",
                load.ingestion_id,
                load.target.dimension_map,
                UNVERIFIABLE_COMMIT,
            )
            self.repository.fail(
                load.ingestion_id,
                UNVERIFIABLE_COMMIT,
                "Lease expired and no ingestion snapshot survives to prove the "
                "commit either way",
            )
            return self.repository.load_for(load.target)
        # Nothing committed and the snapshot could not have been expired yet, so
        # the partition is safe to re-claim.
        return load

    def _resolve_failed_write(
        self, load: PartitionLoad, error: Exception
    ) -> None:
        """Record the outcome of a failed write without risking duplicate rows.

        The Iceberg commit may have landed and only the ledger update failed, so
        the snapshot log is checked before anything is marked FAILED.
        """
        try:
            snapshot_ids = self._snapshot_ids_for_ingestion(load.ingestion_id)
            if snapshot_ids:
                self.repository.reconcile_committed(
                    load.ingestion_id, snapshot_ids
                )
            else:
                self.repository.fail(
                    load.ingestion_id, "IngestionFailed", str(error)
                )
        except Exception:
            logger.exception(
                "Failed to persist ingestion outcome for %s", load.ingestion_id
            )

    # -- write ------------------------------------------------------------

    def _write_claimed_object(
        self,
        obj: S3ObjectRevision,
        target: IngestionTarget,
        claim: PartitionLoad,
        table=None,
    ) -> tuple[str, int]:
        """Read, validate, and commit one claimed object. Returns (outcome, rows)."""
        ns = self.plan.snapshot_property_namespace
        snapshot_properties = {
            self.plan.ingestion_id_property: str(claim.ingestion_id),
            f"{ns}.object-fingerprint": obj.fingerprint,
            f"{ns}.source-key": obj.key,
        }
        # Bracket the read: the listing may be stale, and a provider that swaps the
        # object mid-read would otherwise be committed as if it were the revision
        # the ledger claimed.
        self.plan.assert_object_unchanged(obj)
        df = self.plan.read(obj, target)
        self.plan.assert_object_unchanged(obj)
        if df.height > self.plan.max_rows:
            raise ValueError(
                f"Source object exceeds {self.plan.max_rows} rows"
            )
        df = self.plan.normalize(df, target)
        self.plan.validate_frame(df, target)

        if claim.operation is IngestionOperation.APPEND:
            snapshot_ids = self.plan.append(
                df, target, snapshot_properties, table
            )
            outcome = "appended"
        else:
            snapshot_ids = self.plan.overwrite(df, target, snapshot_properties)
            outcome = "overwritten"
        if not snapshot_ids:
            raise RuntimeError("Iceberg commit published no snapshot")
        self.repository.complete(claim.ingestion_id, snapshot_ids, df.height)
        return outcome, df.height

    def _ingest_one_object(
        self, obj: S3ObjectRevision, target: IngestionTarget
    ) -> tuple[str, int]:
        """Bring one object's partition up to date. Returns (outcome, rows)."""
        load = self.repository.load_for(target)
        if load is not None and load.lease_is_expired:
            load = self._recover_stuck_load(load)

        if (
            load is not None
            and load.is_terminal
            and load.fingerprint == obj.fingerprint
        ):
            return "skipped", 0

        # Choose APPEND vs OVERWRITE from whether the partition physically holds
        # rows, not from the ledger status. A failed or recovered restatement
        # leaves a non-terminal row while the prior committed rows are still in the
        # partition; appending onto them would duplicate it and bypass the
        # overwrite row-count floor. An overwrite is a filtered delete-then-write,
        # so it is always safe when rows are present.
        partition_filter = self.plan.partition_filter(target)
        table = None
        if load is None:
            # Nothing in the ledger, but the partition may predate it. Adopt the
            # existing data instead of re-reading and rewriting an unchanged slice.
            table = self.catalog.load_table(self.spec.identifier)
            if self.catalog.partition_has_rows(table, partition_filter):
                snapshot = table.current_snapshot()
                self.repository.adopt(
                    obj,
                    target,
                    snapshot_ids=(snapshot.snapshot_id,) if snapshot else (),
                )
                return "adopted", 0
            operation = IngestionOperation.APPEND
        elif load.is_terminal:
            # Terminal (COMPLETED/ADOPTED) means the partition already holds rows.
            operation = IngestionOperation.OVERWRITE
        else:
            table = self.catalog.load_table(self.spec.identifier)
            operation = (
                IngestionOperation.OVERWRITE
                if self.catalog.partition_has_rows(table, partition_filter)
                else IngestionOperation.APPEND
            )
        claim = self.repository.claim(
            obj, target, operation, lease_seconds=self.plan.lease_seconds
        )
        if claim is None:
            # Another run holds a live lease, or the partition needs an operator.
            return "contended", 0

        try:
            return self._write_claimed_object(obj, target, claim, table=table)
        except Exception as error:
            self._resolve_failed_write(claim, error)
            raise

    # -- entry point ------------------------------------------------------

    def ingest(self, objects: Sequence[S3ObjectRevision]) -> dict[str, Any]:
        """Ingest a batch of already-listed objects into the coordinator's table.

        Registers the planned objects in the inventory (without retiring absent
        keys - that is ``sync_inventory``'s job), then brings each partition up to
        date. One bad object is quarantined and reported, never fatal.
        """
        planned, quarantined = self._plan_objects(objects)
        self.repository.upsert_inventory([obj for obj, _ in planned])

        outcomes: Counter[str] = Counter()
        processed: list[str] = []
        rows_appended = 0
        for obj, target in planned:
            try:
                outcome, rows = self._ingest_one_object(obj, target)
            except Exception as error:
                logger.exception("Quarantining %s", obj.key)
                quarantined.append(
                    {"key": obj.key, "reason": type(error).__name__}
                )
                outcomes["quarantined"] += 1
                continue
            outcomes[outcome] += 1
            if outcome in {"appended", "overwritten"}:
                processed.append(obj.key)
                rows_appended += rows

        summary = {
            "files_processed": len(processed),
            "files_appended": outcomes["appended"],
            "files_overwritten": outcomes["overwritten"],
            "files_adopted": outcomes["adopted"],
            "files_skipped": outcomes["skipped"],
            "files_contended": outcomes["contended"],
            "files_quarantined": len(quarantined),
            "rows_appended": rows_appended,
            "keys": processed,
            "quarantined": quarantined,
        }
        logger.info(
            "ingest %s: %d written (%d appended, %d overwritten, %d rows), "
            "%d adopted, %d skipped, %d contended, %d quarantined",
            self.spec.identifier,
            summary["files_processed"],
            summary["files_appended"],
            summary["files_overwritten"],
            summary["rows_appended"],
            summary["files_adopted"],
            summary["files_skipped"],
            summary["files_contended"],
            summary["files_quarantined"],
        )
        return summary
