# Copyright 2025-2026 EUMETSAT
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""ChunkManager facade for Firecube chunk state under the `.firecube/` control-plane root."""

from __future__ import annotations

# pyright: reportCallIssue=false, reportAttributeAccessIssue=false
import contextlib
import contextvars
import hashlib
import json
import logging
import random
import time
from collections.abc import Callable, Generator, Iterable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast

from firecube.core.config import StorageConfig
from firecube.core.controlplane._compact import _stored_path_to_uri
from firecube.core.controlplane._paths import run_dir_for
from firecube.core.controlplane.claims import ClaimHandle
from firecube.core.controlplane.deletion import DeletionEngine
from firecube.core.controlplane.events import ConsolidatedTimeCoord
from firecube.core.controlplane.repo import ClaimOwner, ManifestRepository, _owner_candidates
from firecube.core.controlplane.types import (
    EVENT_SLOT_INDEX_MODEL_RECORDED,
    EVENT_SLOT_INDEX_MODEL_VERIFIED,
    INDEX_CURRENT_FILENAME,
    INDEX_DIRNAME,
    RESOLVED_INDEX_ATTR,
    RESOLVED_INDEX_IDENTITY_HASH_ATTR,
    RUN_JOURNAL_FILENAME,
    RUNS_DIRNAME,
    SCHEMA_FILENAME,
    SLOT_INDEX_CURRENT_FILENAME,
    SLOT_INDEX_DIRNAME,
    TIME_COORD_CONSOLIDATION_RUN_ID,
    AbandonSweepResult,
    ChunkInfo,
    ClaimInfo,
    ClearSweepResult,
    DeletionPlan,
    IndexEnsuredEvent,
    ResolvedIndexRecord,
    RunInfo,
    SlotIndexModelRecord,
    SpanCoverage,
    WriteDomain,
    canonical_index_bytes,
    exclusive_maintenance_domain,
)
from firecube.core.errors import (
    ClaimConflictError,
    ConfigurationError,
    ExclusiveMaintenanceRefusedError,
    LegacyIndexRecordError,
    ManifestError,
    Message,
    Operation,
    Placeholder,
    ProductUnderMaintenanceError,
    ResolvedIndexAttrsConflictError,
    ResolvedIndexClaimTimeoutError,
    ResolvedIndexConflictError,
    SlotIndexModelClaimTimeoutError,
    SlotIndexModelConflictError,
    SlotIndexUnmanagedStoreError,
)
from firecube.core.filesystem import StorageFilesystem
from firecube.core.filesystem.protocol import StorageFilesystemFull
from firecube.core.slot_index import (
    SLOT_INDEX_MODEL_ATTR,
    SLOT_INDEX_MODEL_IDENTITY_HASH_ATTR,
    SlotIndexModel,
)
from firecube.core.storage.binding import StorageBinding
from firecube.core.zarr.guard._unit_locks import _store_identity
from firecube.core.zarr.guard.metadata_authority import (
    HeldClaim,
    MetadataScope,
    adopt_held_claim,
    ensure_group,
    hold_authority,
    set_attributes,
)

if TYPE_CHECKING:
    # Methods import the snapshot module: it imports core.zarr.metadata_documents, which imports
    # this package, so a module-level import would be circular.
    from firecube.core.controlplane.metadata_snapshots import (
        Snapshot,
        SnapshotConsistency,
        SnapshotDiff,
        SnapshotRecord,
    )
    from firecube.core.storage.uri import StorageUri
    from firecube.core.zarr.metadata_documents import MetadataDocument

log = logging.getLogger(__name__)

# The claim a winner holds while it mirrors; unset for the Row-1 losers.
_HELD_CLAIM: contextvars.ContextVar[HeldClaim | None] = contextvars.ContextVar(
    "firecube_held_mirror_claim", default=None
)


def _format_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.0f}s"
    if seconds < 3600:
        return f"{seconds / 60:.0f}m"
    if seconds < 86400:
        return f"{seconds / 3600:.0f}h {(seconds % 3600) / 60:.0f}m"
    return f"{seconds / 86400:.0f}d {(seconds % 86400) / 3600:.0f}h"


def _slice_dedupe_key(product: str, span_meta: dict[str, Any]) -> tuple[str, Any, Any, Any] | None:
    group = span_meta.get("group")
    t_min = span_meta.get("time_min")
    t_max = span_meta.get("time_max")
    if group is None or t_min is None or t_max is None:
        return None
    return (product, group, t_min, t_max)


def _has_active_span_slice_collision(chunks: list[ChunkInfo]) -> bool:
    """Return ``True`` when two or more active spans share a slice key.

    Pre-scan used to decide whether the caller needs to build the
    ``(product, run_id) -> started_at`` lookup at all. Without a collision
    ``_dedupe_active_spans`` is a no-op and the lookup is unused, so we
    can skip the ``list_runs`` WAL read entirely.
    """
    seen: set[tuple[str, Any, Any, Any]] = set()
    for chunk in chunks:
        if chunk.chunk_type != "span" or chunk.status != "active":
            continue
        meta = chunk.meta or {}
        key = _slice_dedupe_key(chunk.product, meta)
        if key is None:
            continue
        if key in seen:
            return True
        seen.add(key)
    return False


def _dedupe_active_spans(
    chunks: list[ChunkInfo],
    run_started_at_by_id: dict[tuple[str, str], float] | None = None,
) -> list[ChunkInfo]:
    """Drop duplicate active spans that cover the same slice during force-reingest in-flight.

    During the brief window when a force-reingest run has emitted its new
    span (``status="active"``) but has not yet emitted
    ``replacement_committed`` for the prior span, both spans appear active.
    To avoid showing double coverage in the read models, this helper keeps
    the span whose owning run has the highest ``started_at`` per
    ``(product, group, time_min, time_max)`` key and drops the others.
    The most recently started run wins, matching the temporal semantics of
    force-reingest replacement.

    ``run_started_at_by_id`` is a ``(product, run_id) -> started_at`` lookup
    supplied by the caller. When absent (or when a run_id is missing from the
    map), ``started_at`` defaults to ``0.0``. Ties on ``started_at`` fall back
    to ``run_id`` lexicographic ordering for determinism — never for temporal
    correctness.

    Non-span chunks and spans whose status is not ``"active"`` pass through
    unchanged.  Active spans missing any of ``group``/``time_min``/
    ``time_max`` in their meta also pass through (they cannot be grouped
    into a slice key).
    """
    lookup = run_started_at_by_id or {}
    winners: dict[tuple[str, Any, Any, Any], tuple[float, str]] = {}
    for chunk in chunks:
        if chunk.chunk_type != "span" or chunk.status != "active":
            continue
        meta = chunk.meta or {}
        key = _slice_dedupe_key(chunk.product, meta)
        if key is None:
            continue
        run_id = str(meta.get("run_id", ""))
        started_at = float(lookup.get((chunk.product, run_id), 0.0))
        candidate = (started_at, run_id)
        current = winners.get(key)
        if current is None or current < candidate:
            winners[key] = candidate

    deduped: list[ChunkInfo] = []
    for chunk in chunks:
        if chunk.chunk_type != "span" or chunk.status != "active":
            deduped.append(chunk)
            continue
        meta = chunk.meta or {}
        key = _slice_dedupe_key(chunk.product, meta)
        if key is None:
            deduped.append(chunk)
            continue
        run_id = str(meta.get("run_id", ""))
        started_at = float(lookup.get((chunk.product, run_id), 0.0))
        if winners.get(key) == (started_at, run_id):
            deduped.append(chunk)
    return deduped


def _json_repr(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _group_axis_summary(group_payload: Any) -> dict[str, Any]:
    if not isinstance(group_payload, dict):
        return {"kind": None, "size": None, "params": group_payload}
    return {
        "kind": group_payload.get("kind"),
        "size": group_payload.get("size"),
        "params": group_payload.get("params", {}),
    }


def _format_resolved_index_diff(stored: dict[str, Any], incoming: dict[str, Any]) -> str:
    """Format a field-level diff between two resolved-index payloads."""

    lines = ["resolved index field diff:"]

    for field in ("schema_version", "name"):
        stored_value = stored.get(field)
        incoming_value = incoming.get(field)
        if stored_value != incoming_value:
            lines.append(
                f"- {field}: stored={_json_repr(stored_value)} "
                f"incoming={_json_repr(incoming_value)}"
            )

    stored_groups_raw = stored.get("groups", {})
    incoming_groups_raw = incoming.get("groups", {})
    stored_groups = stored_groups_raw if isinstance(stored_groups_raw, dict) else {}
    incoming_groups = incoming_groups_raw if isinstance(incoming_groups_raw, dict) else {}
    stored_group_names = set(stored_groups)
    incoming_group_names = set(incoming_groups)
    only_stored = sorted(stored_group_names - incoming_group_names)
    only_incoming = sorted(incoming_group_names - stored_group_names)
    if only_stored:
        lines.append(f"- groups.only_in_stored={_json_repr(only_stored)}")
    if only_incoming:
        lines.append(f"- groups.only_in_incoming={_json_repr(only_incoming)}")

    for group in sorted(stored_group_names & incoming_group_names):
        stored_axis = _group_axis_summary(stored_groups[group])
        incoming_axis = _group_axis_summary(incoming_groups[group])
        for field in ("kind", "size", "params"):
            stored_value = stored_axis.get(field)
            incoming_value = incoming_axis.get(field)
            if stored_value != incoming_value:
                lines.append(
                    f"- group {group!r}.{field}: stored={_json_repr(stored_value)} "
                    f"incoming={_json_repr(incoming_value)}"
                )

    stored_hash = hashlib.sha256(canonical_index_bytes(stored)).hexdigest()[:16]
    incoming_hash = hashlib.sha256(canonical_index_bytes(incoming)).hexdigest()[:16]
    lines.append(f"stored_hash={stored_hash}")
    lines.append(f"incoming_hash={incoming_hash}")
    return "\n".join(lines)


class ChunkManager:
    """Public facade for managing Firecube chunk state."""

    __slots__ = ("_filesystem", "deletion_engine", "log", "product_name", "repo", "workspace")

    def __init__(
        self,
        binding: StorageBinding,
        workspace: Path | None = None,
        *,
        filesystem: StorageFilesystem | None = None,
        time_dim_name: str = "timestamp",
    ) -> None:
        import tempfile

        if workspace is None:
            workspace = Path(tempfile.mkdtemp(prefix="firecube_chunks_"))

        self.workspace = workspace
        self._filesystem = filesystem
        self.repo = ManifestRepository(
            binding=binding,
            workspace=workspace,
            filesystem=filesystem,
        )
        self.log = logging.getLogger(f"{__name__}.ChunkManager")
        self.deletion_engine = DeletionEngine(
            self.repo, filesystem=filesystem, time_dim_name=time_dim_name
        )

    @property
    def base_uri(self) -> str:
        """Current base URI for product storage."""
        return self.repo.base_uri

    @property
    def storage_config(self) -> Any | None:
        """Current storage configuration (e.g. S3 credentials)."""
        return self.repo.storage_config

    def get_product_root(self, product: str) -> str:
        """Return the resolved product root URI."""
        return self.repo.get_product_root_uri(product)

    def get_control_root(self, product: str) -> str:
        """Return the resolved .firecube/ control root URI."""
        return self.repo.get_control_root_uri(product)

    def get_latest_pointer(self, product: str) -> str:
        """Return the resolved compact/LATEST.json pointer URI."""
        return self.repo.get_latest_pointer_uri(product)

    def close(self) -> None:
        """Close the underlying repository, flushing writers and releasing resources."""
        if hasattr(self, "repo"):
            self.repo.close()

    def record_run_started(
        self,
        *,
        product: str,
        run_id: str,
        output_path: str,
        output_format: str,
        size: int,
        meta: dict[str, Any],
        slot_range: tuple[int, int] | None = None,
        slot_group: str | None = None,
    ) -> None:
        """Record the start of a new run, then check the product's exclusive maintenance claim.

        Raises:
            ProductUnderMaintenanceError: Another owner holds that claim, or it cannot be
                read. The run is recorded failed with the refusal text before the raise.
        """
        self.repo.record_run_started(
            product=product,
            run_id=run_id,
            output_path=output_path,
            output_format=output_format,
            size=size,
            meta=meta,
            slot_range=slot_range,
            slot_group=slot_group,
        )
        try:
            self.check_exclusive_maintenance(product=product, owner_id=run_id)
        except ProductUnderMaintenanceError as exc:
            self._record_refused_start(
                product=product,
                run_id=run_id,
                output_path=output_path,
                output_format=output_format,
                size=size,
                meta=meta,
                slot_range=slot_range,
                slot_group=slot_group,
                error=str(exc),
            )
            raise

    def _record_refused_start(
        self,
        *,
        product: str,
        run_id: str,
        output_path: str,
        output_format: str,
        size: int,
        meta: dict[str, Any],
        error: str,
        slot_range: tuple[int, int] | None = None,
        slot_group: str | None = None,
    ) -> None:
        """Record a run that the exclusive maintenance claim refused as failed; log a failure."""
        try:
            self.repo.record_run_terminal(
                product=product,
                run_id=run_id,
                output_path=output_path,
                output_format=output_format,
                size=size,
                meta=meta,
                status="failed",
                error=error,
                slot_range=slot_range,
                slot_group=slot_group,
            )
        except Exception:
            # Log, never raise: the refusal is the error the caller must see.
            self.log.exception("Failed to record refused run %s of %s as failed", run_id, product)

    def check_exclusive_maintenance(self, *, product: str, owner_id: str) -> None:
        """Refuse a writer while another owner holds the product's exclusive maintenance claim.

        Reads the one claim file by name. An absent claim, or one that `owner_id` owns, passes.

        Raises:
            ProductUnderMaintenanceError: Another owner holds the claim, or it cannot be read.
        """
        check_exclusive_maintenance(repo=self.repo, product=product, owner_id=owner_id)

    def acquire_exclusive_maintenance(self, *, product: str, run_id: str) -> ClaimHandle:
        """Acquire the product's exclusive maintenance claim for `run_id`, then verify it is alone.

        Under the claim it lists the product's claims and reads every run from fresh
        listings. Any other claim, or any other run that is not terminal or cannot be read,
        refuses. A listing or read error propagates after the claim is released. The caller
        releases the returned handle; abandoning `run_id`, when it is a recorded run,
        releases it too.

        Raises:
            ClaimConflictError: Another owner holds the exclusive claim.
            ExclusiveMaintenanceRefusedError: Other claims or runs exist; the claim is released.
        """
        domain = exclusive_maintenance_domain(product)
        handle = self.acquire_claim(product=product, domain=domain, owner_id=run_id)
        try:
            claims = [
                claim
                for claim in self.repo.list_product_claims(product=product)
                if claim.domain != domain.identifier
            ]
            runs, unreadable = self.repo.scan_runs(product=product)
            blocking_runs = [run for run in runs if not run.is_terminal and run.run_id != run_id]
            unread = [(other, error) for other, error in unreadable if other != run_id]
            if claims or blocking_runs or unread:
                details: list[str] = []
                if claims:
                    details.append(
                        "active write claim(s) exist: "
                        + ", ".join(f"{claim.domain} (owner={claim.owner_id})" for claim in claims)
                    )
                if blocking_runs or unread:
                    details.append(
                        "non-terminal or unreadable run(s) exist: "
                        + ", ".join(
                            [f"{run.run_id} (status={run.status})" for run in blocking_runs]
                            + [f"{other} (unreadable: {error})" for other, error in unread]
                        )
                    )
                raise ExclusiveMaintenanceRefusedError(
                    maintenance_refusal_message(
                        repo=self.repo,
                        product=product,
                        operation="exclusive maintenance",
                        detail="; ".join(details),
                        claims=claims,
                    ),
                    product=product,
                    claims=[claim.domain for claim in claims],
                    runs=[run.run_id for run in blocking_runs] + [other for other, _ in unread],
                )
        except BaseException:
            try:
                handle.release()
            except Exception:
                # Log, never raise: the refusal or listing error is what the caller must see.
                self.log.exception("Failed to release claim %s", domain.identifier)
            raise
        return handle

    def record_run_terminal(
        self,
        *,
        product: str,
        run_id: str,
        output_path: str,
        output_format: str,
        size: int,
        meta: dict[str, Any],
        status: str,
        error: str | None = None,
        slot_range: tuple[int, int] | None = None,
        slot_group: str | None = None,
    ) -> None:
        """Record a terminal run state (complete/failed/abandoned)."""
        self.repo.record_run_terminal(
            product=product,
            run_id=run_id,
            output_path=output_path,
            output_format=output_format,
            size=size,
            meta=meta,
            status=status,
            error=error,
            slot_range=slot_range,
            slot_group=slot_group,
        )

    def record_run_started_with_replacement(
        self,
        *,
        product: str,
        run_id: str,
        replaces: list[str],
    ) -> None:
        """Record informational replacement intent for a run, then check exclusive maintenance.

        Raises:
            ProductUnderMaintenanceError: Another owner holds the product's exclusive
                maintenance claim, or it cannot be read. The run is recorded failed first.
        """
        self.repo.record_run_started_with_replacement(
            product=product,
            run_id=run_id,
            replaces=replaces,
        )
        try:
            self.check_exclusive_maintenance(product=product, owner_id=run_id)
        except ProductUnderMaintenanceError as exc:
            # The run started earlier; its terminal record names what abandon would name.
            entry: dict[str, Any] = {}
            with contextlib.suppress(Exception):
                entry = self.repo._get_run_entry(product=product, run_id=run_id)
            self._record_refused_start(
                product=product,
                run_id=run_id,
                output_path=str(entry.get("output_path") or entry.get("run_uri") or ""),
                output_format=str(entry.get("output_format") or "control"),
                size=int(entry.get("size", 0) or 0),
                meta={"operation": "replacement"},
                error=str(exc),
            )
            raise

    def record_replacement_committed(
        self,
        *,
        product: str,
        run_id: str,
        replacing_run_id: str,
        replaced_span_keys: list[str],
    ) -> None:
        """Record terminal replacement commit for prior spans."""
        self.repo.record_replacement_committed(
            product=product,
            run_id=run_id,
            replacing_run_id=replacing_run_id,
            replaced_span_keys=replaced_span_keys,
        )

    def record_maintenance_started(
        self,
        *,
        product: str,
        run_id: str,
        op: str,
        scope_meta: dict[str, Any] | None = None,
    ) -> None:
        """Record the start of a maintenance run (delete/scrub/archive_restore)."""
        self.repo.record_maintenance_started(
            product=product,
            run_id=run_id,
            op=op,
            scope_meta=scope_meta,
        )

    def record_maintenance_completed(
        self,
        *,
        product: str,
        run_id: str,
        op: str,
        scope_meta: dict[str, Any] | None = None,
    ) -> None:
        """Record terminal completion of a maintenance run."""
        self.repo.record_maintenance_completed(
            product=product,
            run_id=run_id,
            op=op,
            scope_meta=scope_meta,
        )

    def record_maintenance_failed(
        self,
        *,
        product: str,
        run_id: str,
        op: str,
        scope_meta: dict[str, Any] | None = None,
        error: str,
    ) -> None:
        """Record terminal failure of a maintenance run."""
        self.repo.record_maintenance_failed(
            product=product,
            run_id=run_id,
            op=op,
            scope_meta=scope_meta,
            error=error,
        )

    def record_run_failed(
        self,
        *,
        product: str,
        run_id: str,
        output_path: str,
        output_format: str,
        size: int,
        meta: dict[str, Any],
        error: str,
    ) -> None:
        """Record a failed run (convenience wrapper for record_run_terminal with status=failed)."""
        self.repo.record_run_terminal(
            product=product,
            run_id=run_id,
            output_path=output_path,
            output_format=output_format,
            size=size,
            meta=meta,
            status="failed",
            error=error,
        )

    def record_span(
        self,
        product: str,
        run_id: str,
        batch_id: str,
        group: str,
        status: str,
        reason: str | None = None,
        coverage: SpanCoverage | None = None,
        meta: dict[str, Any] | None = None,
    ) -> None:
        """Record a span event (chunk write or state change) within a run."""
        self.repo.record_span_event(
            product=product,
            run_id=run_id,
            batch_id=batch_id,
            group=group,
            status=status,
            reason=reason,
            coverage=coverage,
            meta=meta,
        )

    def record_schema_verification(
        self,
        product: str,
        run_id: str,
        group: str,
        plugin: str,
        schema_hash: str,
        verified_at: str,
        expected_time_count: int,
        meta: dict[str, Any] | None = None,
    ) -> None:
        """Record a schema verification audit event for observability.

        Audit records are NOT consulted for skip logic. Each pod writes its own
        record (verified_at differs).
        """
        self.repo.record_schema_verification_event(
            product=product,
            run_id=run_id,
            group=group,
            plugin=plugin,
            schema_hash=schema_hash,
            verified_at=verified_at,
            expected_time_count=expected_time_count,
            meta=meta,
        )

    def record_index_ensured_event(self, event: IndexEnsuredEvent) -> None:
        """Record an ``index_ensured`` audit event summarising the resolved index.

        Fired once at DirectZarr pod startup and once per ``firecube zarr index
        rebuild`` invocation. The payload carries only ``identity_hash``,
        ``axis_kinds``, ``groups`` and ``outcome`` — never the full resolved
        payload — so operators can correlate WAL entries with telemetry without
        duplicating the on-disk record.
        """
        self.repo.record_index_ensured_event(event)

    def record_time_coord_consolidation(
        self,
        groups: tuple[str, ...],
        timestamp_iso: str,
    ) -> None:
        """Record that time coordinate consolidation has sealed these groups."""

        event = ConsolidatedTimeCoord(
            run_id=TIME_COORD_CONSOLIDATION_RUN_ID,
            timestamp_iso=timestamp_iso,
            groups=groups,
        )
        self.repo.record_time_coord_consolidation(event)

    def list_time_coord_consolidations(self, *, product: str) -> list[ConsolidatedTimeCoord]:
        """Return WAL events that sealed time coordinates for a product."""

        return self.repo.list_time_coord_consolidations(product=product)

    def discover_manifests(self) -> list[str]:
        """Scan the workspace for products with control-plane roots."""
        return self.repo.discover_manifests()

    def parse_manifest(self, manifest_uri: str) -> Generator[ChunkInfo, None, None]:
        """Yield ChunkInfo records from a manifest URI."""
        return self.repo.parse_manifest(manifest_uri)

    def list_chunks(
        self,
        pattern: str | None = None,
        product: str | None = None,
        before: datetime | None = None,
        after: datetime | None = None,
        chunk_type: str | None = None,
        status: str | None = None,
        include_replaced: bool = False,
        *,
        meta: dict[str, Any] | None = None,
        time_min_after: str | None = None,
        time_max_before: str | None = None,
        time_overlaps: tuple[str, str] | None = None,
        filter_fn: Callable[[ChunkInfo], bool] | None = None,
        refuse_unparseable: bool = False,
    ) -> list[ChunkInfo]:
        """Return projected chunk records with optional filtering.

        With ``refuse_unparseable`` a time filter raises
        ``UnparseableSpanBoundsError`` when a span's compared bounds do not
        parse; by default such spans are left out of the result.

        During a force-reingest run there is a brief window where both prior
        spans and new spans have ``status="active"`` (before
        ``replacement_committed`` lands).  To prevent double coverage in
        callers, this method deduplicates active spans by
        ``(product, group, time_min, time_max)``: when multiple active spans
        share the same slice key, only the span whose owning run has the
        highest ``started_at`` is returned (most-recently-started run wins).
        Non-span records and ``replaced``/``failed`` spans pass through
        unchanged, so this is a no-op outside the force-reingest in-flight
        window.

        A private in-tree caller (the replacement recorder in
        ``firecube.ingestor.runtime.recording``) reaches through
        ``self.repo.list_chunks(...)`` to see EVERY active span rather
        than the dedupe winner; that bypass is intentional and marked at
        its call site.
        """
        chunks = self.repo.list_chunks(
            pattern=pattern,
            product=product,
            before=before,
            after=after,
            chunk_type=chunk_type,
            status=status,
            include_replaced=include_replaced,
            meta=meta,
            time_min_after=time_min_after,
            time_max_before=time_max_before,
            time_overlaps=time_overlaps,
            filter_fn=filter_fn,
            refuse_unparseable=refuse_unparseable,
        )
        run_started_at_by_id = (
            self._build_run_started_at_lookup(chunks)
            if _has_active_span_slice_collision(chunks)
            else None
        )
        return _dedupe_active_spans(chunks, run_started_at_by_id=run_started_at_by_id)

    def _build_run_started_at_lookup(self, chunks: list[ChunkInfo]) -> dict[tuple[str, str], float]:
        """Return ``(product, run_id) -> started_at`` for products with active spans.

        Only products that appear in an active span are queried, so the
        common case (no active-span duplicates) pays at most one
        ``list_runs`` call per distinct product in the result set. Callers
        outside the dedupe path should not use this helper.

        Emits a ``DEBUG`` log line on every invocation so operators (and
        tests) can observe the run-started lookup miss on the ``list_chunks``
        hot path without patching the method itself. Line count == number of
        WAL reads triggered by the dedupe path.
        """
        products_with_active_spans = {
            chunk.product
            for chunk in chunks
            if chunk.chunk_type == "span" and chunk.status == "active"
        }
        log.debug(
            "run-started cache miss: reading WAL for run_started_at lookup across %d product(s) with active-span slice collision",
            len(products_with_active_spans),
        )
        lookup: dict[tuple[str, str], float] = {}
        for prod in products_with_active_spans:
            for run in self.list_runs(product=prod):
                lookup[(prod, run.run_id)] = run.started_at
        return lookup

    def mark_chunks_replaced(
        self,
        chunk_keys: list[str],
        product: str,
        timestamp: float,
        *,
        meta_updates_by_key: dict[str, dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Mark chunk records as replaced with a timestamp."""
        return self.repo.mark_chunks_replaced(
            chunk_keys,
            product,
            timestamp,
            meta_updates_by_key=meta_updates_by_key,
        )

    def list_runs(
        self,
        *,
        product: str,
        status: str | None = None,
        non_terminal: bool = False,
    ) -> list[RunInfo]:
        """List runs for a product with optional status/terminal filtering.

        ``RunInfo.timestamps_skipped`` is read from the projected run entry
        (``run.json``) and does not require scanning the run WAL.
        """
        return self.repo.list_runs(product=product, status=status, non_terminal=non_terminal)

    def claim_coord_materialization_window(
        self,
        *,
        product: str,
        run_id: str,
        output_path: str,
        output_format: str,
        windows_by_group: dict[str, tuple[int, int]],
        coord_chunk_sizes: dict[str, int],
        slot_group: str | None = None,
        meta: dict[str, Any] | None = None,
        whole_extent_groups: Mapping[str, int] | None = None,
    ) -> ClaimHandle:
        """Atomically reject overlapping coord-chunk peers and return a live claim.

        Under a single write-domain claim lock, this checks every windowed
        group against active peer runs using that group's own coordinate
        chunk geometry, rejects any window that touches a peer-owned
        coordinate chunk, then records this run as started while the claim is
        still held.

        ``whole_extent_groups`` maps a group to its slot count when this run
        writes the group's engine-owned arrays whole: every live ranged peer
        on that group conflicts, whatever its window, and the recorded run
        range covers ``[0, slot count)``.

        If this method raises, the claim is released and no terminalization is
        needed: a peer refusal registers no run, and an exclusive maintenance
        refusal (`ProductUnderMaintenanceError`) has already recorded the run
        failed. If it returns normally, the caller must release the returned
        handle after materialization and record a terminal run state on every
        exit path.
        """

        # Function-scope import: avoid module-level cycle with core.zarr.
        from firecube.core.zarr.guard.chunk_geometry import chunk_axis_range

        domain = WriteDomain(
            product=product,
            category="coord_materialization",
            name="all",
        )

        handle = self.acquire_claim(product=product, domain=domain, owner_id=run_id)
        try:
            all_peers = [
                peer
                for peer in self.list_runs(product=product, non_terminal=True)
                if peer.run_id != run_id
            ]
            serial_peers = [peer for peer in all_peers if peer.slot_range is None]
            ranged_peers = [peer for peer in all_peers if peer.slot_range is not None]
            # Serial (non-range) runs own the full extent by convention; keep
            # symmetric with resume_guard._check_non_terminal_runs (reverse leg).
            if serial_peers:
                serial_peer = serial_peers[0]
                elapsed = "unknown"
                if serial_peer.started_at:
                    elapsed_s = datetime.now(tz=UTC).timestamp() - float(serial_peer.started_at)
                    elapsed = f"{elapsed_s:.0f}s"
                product_uri = self.get_product_root(product)
                raise ConfigurationError(
                    Message(
                        "conflicting serial ingest run {run_id} is live "
                        "(holds full extent; state: {status}, running for {elapsed}); "
                        "if this run is confirmed dead, abandon it explicitly, then re-run "
                        "preallocate. Automatic timeout is not implemented.\n  {abandon}",
                        run_id=serial_peer.run_id,
                        status=serial_peer.status,
                        elapsed=elapsed,
                        abandon=_abandon_operation(product_uri, serial_peer.run_id),
                        product=product,
                        product_uri=product_uri,
                    )
                )
            for group, window in windows_by_group.items():
                chunk_size = coord_chunk_sizes.get(group)
                if chunk_size is None:
                    continue
                proposed_chunks = set(chunk_axis_range(window[0], window[1], chunk_size))
                for peer in ranged_peers:
                    peer_range = peer.slot_range
                    assert peer_range is not None
                    if peer.slot_group is not None and peer.slot_group != group:
                        continue
                    peer_chunks = set(chunk_axis_range(peer_range[0], peer_range[1], chunk_size))
                    overlap = sorted(proposed_chunks & peer_chunks)
                    if overlap:
                        elapsed = "unknown"
                        if peer.started_at:
                            elapsed_s = datetime.now(tz=UTC).timestamp() - float(peer.started_at)
                            elapsed = f"{elapsed_s:.0f}s"
                        product_uri = self.get_product_root(product)
                        raise ConfigurationError(
                            Message(
                                "group {group!r} window [{slot_window[0]}, {slot_window[1]}) "
                                "touches coordinate chunk(s) {overlap} owned by run {run_id} "
                                "(state: {status}, materializing for {elapsed}); "
                                "if this run is confirmed dead, abandon it explicitly, then "
                                "re-run preallocate. Automatic timeout is not implemented.\n"
                                "  {abandon}",
                                group=group,
                                slot_window=(window[0], window[1]),
                                overlap=overlap,
                                run_id=peer.run_id,
                                status=peer.status,
                                elapsed=elapsed,
                                abandon=_abandon_operation(product_uri, peer.run_id),
                                product=product,
                                product_uri=product_uri,
                            )
                        )
            whole = dict(whole_extent_groups or {})
            for group in whole:
                for peer in ranged_peers:
                    if peer.slot_group is not None and peer.slot_group != group:
                        continue
                    peer_range = peer.slot_range
                    assert peer_range is not None
                    product_uri = self.get_product_root(product)
                    raise ConfigurationError(
                        Message(
                            "group {group!r} has engine-owned arrays, which preallocate writes "
                            "whole, but run {run_id} is live on window "
                            "[{slot_window[0]}, {slot_window[1]}) (state: {status}); wait for "
                            "it to finish, or if it is confirmed dead abandon it explicitly, "
                            "then re-run preallocate. Automatic timeout is not implemented.\n"
                            "  {abandon}",
                            group=group,
                            run_id=peer.run_id,
                            slot_window=(peer_range[0], peer_range[1]),
                            status=peer.status,
                            abandon=_abandon_operation(product_uri, peer.run_id),
                            product=product,
                            product_uri=product_uri,
                        )
                    )

            claimed = [*windows_by_group.values(), *((0, extent) for extent in whole.values())]
            hull_start = min(window[0] for window in claimed)
            hull_end = max(window[1] for window in claimed)
            self.record_run_started(
                product=product,
                run_id=run_id,
                output_path=output_path,
                output_format=output_format,
                size=hull_end - hull_start,
                meta=meta or {},
                slot_range=(hull_start, hull_end),
                slot_group=slot_group,
            )
        except BaseException:
            handle.release()
            raise
        return handle

    def time_coverage_summary(
        self, product: str, *, meta: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Return per-group time range boundaries and span counts for a product.

        Each entry is ``{"group": str, "time_min": str, "time_max": str,
        "span_count": int, "total_timestamps_written": int}``, sorted by group.
        Used by ``ResumeGuard`` for overlap detection and by operators for
        coverage diagnostics.

        Inherits the active-span dedupe from `list_chunks`: during a
        force-reingest in-flight window, only the highest-``run_id`` active
        span per slice contributes to the per-group totals, so coverage is
        not double-counted.
        """
        spans = self.list_chunks(
            product=product, chunk_type="span", include_replaced=False, meta=meta
        )
        groups: dict[str, dict[str, Any]] = {}
        for span in spans:
            span_meta = span.meta or {}
            group = span_meta.get("group", "unknown")
            t_min = span_meta.get("time_min")
            t_max = span_meta.get("time_max")
            tw = 0
            if isinstance(span.record, dict):
                span_data = span.record.get("span", {})
                if isinstance(span_data, dict):
                    tw = int(span_data.get("timestamps_written", 0))

            if group not in groups:
                groups[group] = {
                    "group": group,
                    "time_min": t_min,
                    "time_max": t_max,
                    "span_count": 0,
                    "total_timestamps_written": 0,
                }
            entry = groups[group]
            entry["span_count"] += 1
            entry["total_timestamps_written"] += tw
            if t_min and (entry["time_min"] is None or t_min < entry["time_min"]):
                entry["time_min"] = t_min
            if t_max and (entry["time_max"] is None or t_max > entry["time_max"]):
                entry["time_max"] = t_max
        return sorted(groups.values(), key=lambda x: x["group"])

    def abandon_run(
        self,
        *,
        product: str,
        run_id: str,
        reason: str,
        meta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Abandon a run and release the write claims it owns; data stays untouched.

        It records a non-terminal run as abandoned first; a terminal run keeps its status.
        Either way, it then deletes every claim of the product whose owner resolves to this
        run alone.

        A claim owner id resolves through its candidates: the id itself and each prefix
        ending just before a `:`. The single candidate recorded as a run owns the claim.

        Args:
            product: Product name.
            run_id: Run to abandon.
            reason: Operator reason, recorded with the abandon.
            meta: Extra metadata for the terminal record.

        Returns:
            `product`, `run_id`, `status` and `abandoned` (False when the run was already
            terminal), plus `released_claims`, the deleted claim domains, and
            `skipped_claims`, the domains left because their owner is ambiguous or changed.
            Both lists are sorted and always present.

        Raises:
            ManifestError: The run is not recorded for the product, or the claims cannot be
                listed after the abandon is recorded; the message then says to run it again.
        """
        outcome = self.repo.abandon_run(
            product=product,
            run_id=run_id,
            reason=reason,
            meta=meta,
        )
        released, skipped = self._release_run_claims(
            product=product, run_id=run_id, status=str(outcome.get("status"))
        )
        return {**outcome, "released_claims": released, "skipped_claims": skipped}

    def _resolve_claim_owner(self, *, product: str, owner_id: str) -> ClaimOwner:
        """Return the recorded runs among the candidates of a claim owner id, via the repository."""
        return self.repo.resolve_claim_owner(product=product, owner_id=owner_id)

    def _list_claims_to_release(
        self, *, product: str, recovery: Callable[[], str]
    ) -> list[ClaimInfo]:
        """List the product's claims from a fresh listing, for a release.

        A listing error raises ManifestError naming the product, the error and then the text
        `recovery` returns. A ManifestError, including a malformed claim file, propagates.
        """
        try:
            return self.repo.list_product_claims(product=product)
        except ManifestError:
            raise
        except Exception as exc:
            raise ManifestError(
                Message(
                    "The write claims of {product} could not be listed: {error}.\n{recovery}",
                    product=product,
                    error=str(exc),
                    recovery=recovery(),
                )
            ) from exc

    def _release_claims(
        self,
        *,
        product: str,
        claims: Iterable[ClaimInfo],
        decide: Callable[[ClaimOwner], Literal["release", "skip"] | None],
        stale_only: bool = False,
        dry_run: bool = False,
    ) -> tuple[list[str], list[str]]:
        """Release or skip each listed claim as `decide` says for its resolved owner.

        Resolves each distinct owner id once. A claim to release that changed since the
        listing is skipped instead; one already gone is in neither list. With `stale_only`,
        a claim that is fresh when re-read, or whose heartbeat moved, is skipped too.

        With `dry_run` it deletes nothing and judges `stale_only` on the listed claim.
        Returns the released and the skipped domains, sorted.
        """
        service = self.repo.claims
        assert service is not None
        owners: dict[str, ClaimOwner] = {}
        released: list[str] = []
        skipped: list[str] = []
        for claim in claims:
            owner = owners.get(claim.owner_id)
            if owner is None:
                owner = self._resolve_claim_owner(product=product, owner_id=claim.owner_id)
                owners[claim.owner_id] = owner
            decision = decide(owner)
            if decision == "skip":
                skipped.append(claim.domain)
            elif decision == "release" and dry_run:
                kept = stale_only and not claim.stale
                (skipped if kept else released).append(claim.domain)
            elif decision == "release":
                outcome = service.release_claim_if_unchanged(
                    product=product, claim=claim, stale_only=stale_only
                )
                if outcome == "released":
                    released.append(claim.domain)
                elif outcome == "changed":
                    skipped.append(claim.domain)
        return sorted(released), sorted(skipped)

    def _release_run_claims(
        self, *, product: str, run_id: str, status: str
    ) -> tuple[list[str], list[str]]:
        """Delete the claims whose owner resolves to `run_id` alone, after its abandon.

        Considers only owners equal to `run_id` or starting with `run_id:`; any other recorded
        run among an owner's candidates leaves the claim skipped. Lists the claims strictly:
        a listing error raises ManifestError saying the run is recorded `status` and that
        running the abandon again releases its claims.
        """

        def recovery() -> Message:
            product_uri = self.get_product_root(product)
            return Message(
                "Run {run_id} is recorded {status}; run the abandon again to release its "
                "claims:\n  {abandon}",
                run_id=run_id,
                status=status,
                abandon=_abandon_operation(product_uri, run_id),
                product_uri=product_uri,
            )

        claims = [
            claim
            for claim in self._list_claims_to_release(product=product, recovery=recovery)
            if claim.owner_id == run_id or claim.owner_id.startswith(f"{run_id}:")
        ]

        def decide(owner: ClaimOwner) -> Literal["release", "skip"]:
            return "release" if owner.run is not None and owner.run.run_id == run_id else "skip"

        return self._release_claims(product=product, claims=claims, decide=decide)

    def create_deletion_plan(
        self,
        pattern: str | None = None,
        product: str | None = None,
        before: datetime | None = None,
        after: datetime | None = None,
        chunk_type: str | None = None,
        status: str | None = None,
        include_metadata: bool = False,
        *,
        meta: dict[str, Any] | None = None,
        filter_fn: Callable[[ChunkInfo], bool] | None = None,
        time_overlaps: tuple[str, str] | None = None,
    ) -> DeletionPlan:
        """Build a deletion plan for chunks matching the given criteria."""
        return self.deletion_engine.create_deletion_plan(
            pattern,
            product,
            before,
            after,
            chunk_type,
            status,
            include_metadata,
            meta=meta,
            filter_fn=filter_fn,
            time_overlaps=time_overlaps,
        )

    def execute_deletion(
        self,
        plan: DeletionPlan,
        delete_storage: bool = True,
        delete_manifest: bool = True,
        storage_config: StorageConfig | None = None,
        dry_run: bool = False,
        *,
        holder_run_id: str | None = None,
    ) -> dict[str, Any]:
        """Execute a deletion plan, optionally removing storage and manifest entries.

        Pass `holder_run_id` to delete as the owner of each product's exclusive maintenance
        claim. See :meth:`DeletionEngine.execute_deletion`.
        """
        return self.deletion_engine.execute_deletion(
            plan,
            delete_storage,
            delete_manifest,
            storage_config,
            dry_run,
            holder_run_id=holder_run_id,
        )

    def delete_spans(
        self,
        spans: Iterable[ChunkInfo],
        *,
        dry_run: bool = False,
        force: bool = False,
        yes_i_really_mean_it: bool = False,
        update_manifest: bool = True,
        update_state: bool = True,
        time_dim_name: str | None = None,
        holder_run_id: str | None = None,
    ) -> dict[str, Any]:
        """Delete specific span records with optional dry-run and manifest updates.

        ``yes_i_really_mean_it`` acknowledges the collateral-span guard that
        blocks force-deletion when other active spans share the same physical
        chunk keys. Pass ``holder_run_id`` to delete as the owner of the product's
        exclusive maintenance claim. See :meth:`DeletionEngine.delete_spans`.
        """
        return self.deletion_engine.delete_spans(
            spans,
            dry_run=dry_run,
            force=force,
            yes_i_really_mean_it=yes_i_really_mean_it,
            update_manifest=update_manifest,
            update_state=update_state,
            time_dim_name=time_dim_name,
            holder_run_id=holder_run_id,
        )

    def rebuild_compact(self, product: str) -> dict[str, Any]:
        """Compact completed-run WAL events into the compact cache for faster reads."""
        return self.repo.rebuild_compact(product)

    def compact_status(self, product: str) -> dict[str, Any]:
        """Report the age, generation and record count of the product's compact cache."""
        self.repo._ensure_bound()
        latest = self.repo._read_latest_pointer(product)
        if latest is None:
            return {"exists": False}

        cutoff = float(latest.get("completed_before", 0.0) or 0.0)
        age_seconds = time.time() - cutoff if cutoff > 0 else 0.0
        completed_before_iso: str | None = None
        if cutoff > 0:
            completed_before_iso = datetime.fromtimestamp(cutoff, tz=UTC).isoformat()

        compact_meta_path = str(latest.get("snapshot_meta_path") or "")
        records = 0
        repo_fs = self.repo._fs
        if compact_meta_path and repo_fs is not None:
            try:
                meta_uri = _stored_path_to_uri(compact_meta_path)
                with repo_fs.open(meta_uri, "r") as fh:
                    meta = json.load(fh)
                records = int(meta.get("records", meta.get("record_count", 0)))
            except (OSError, ValueError, TypeError, AttributeError, OverflowError) as exc:
                raise ManifestError(
                    f"The compact cache metadata file {compact_meta_path} is missing or "
                    f"unreadable: {exc}"
                ) from exc

        return {
            "exists": True,
            "completed_before": completed_before_iso,
            "age_seconds": age_seconds,
            "age_human": _format_duration(age_seconds),
            "generation": latest.get("generation"),
            "records": records,
        }

    def acquire_claim(
        self, *, product: str, domain: WriteDomain, owner_id: str, describe_conflict: bool = True
    ):
        """Acquire an exclusive write claim for a domain.

        Pass ``describe_conflict=False`` when the caller catches the conflict and retries.

        Raises:
            ClaimConflictError: Another owner holds the claim. With ``describe_conflict`` the
                message names that owner's run and status, then the commands that release it.
        """
        try:
            return self.repo.acquire_claim(product=product, domain=domain, owner_id=owner_id)
        except ClaimConflictError as exc:
            if not describe_conflict:
                raise
            raise ClaimConflictError(
                Message(
                    "{conflict}\n{detail}",
                    conflict=str(exc) if exc.message is None else exc.message,
                    detail=self._claim_conflict_detail(product=product, domain=domain),
                )
            ) from exc

    def _claim_conflict_detail(self, *, product: str, domain: WriteDomain) -> str:
        """Describe who holds a conflicting claim and how to release it; never raises.

        Re-reads the claim, resolves its owner to a recorded run and names the clear
        operation, plus the abandon operation when exactly one run owns the claim.
        """
        try:
            product_uri = self.get_product_root(product)
        except Exception as exc:
            return Message(
                "The product root could not be resolved: {error}.",
                error=str(exc),
                product=product,
            )
        try:
            service = self.repo.claims
            assert service is not None
            claim = service.read_claim_by_domain(product=product, domain=domain.identifier)
        except Exception as exc:
            return Message(
                "The claim could not be read: {error}.\n"
                "If no process of its owner is running, clear the claim:\n  {clear}",
                error=str(exc),
                clear=_clear_operation(product_uri, domain.identifier, force=False),
                product=product,
                product_uri=product_uri,
                claim_domain=domain.identifier,
            )
        if claim is None:
            return "The claim was released after the conflict; retry."
        clear = _clear_operation(product_uri, domain.identifier, force=not claim.stale)
        abandon: str = ""
        try:
            owner = self._resolve_claim_owner(product=product, owner_id=claim.owner_id)
            if owner.run is not None:
                run = owner.run
                who = Message(
                    "Owner {claim_owner} is run {run_id} (status: {status}{stale}).",
                    claim_owner=claim.owner_id,
                    run_id=run.run_id,
                    status=run.status,
                    stale=", stale" if run.stale else "",
                )
                abandon = Message(
                    "\nor abandon run {run_id}, which releases the claims it alone owns:"
                    "\n  {abandon}",
                    run_id=run.run_id,
                    abandon=_abandon_operation(product_uri, run.run_id),
                )
            elif owner.ambiguous:
                who = Message(
                    "Owner {claim_owner} matches more than one recorded run: {candidates}.",
                    claim_owner=claim.owner_id,
                    candidates=", ".join(run.run_id for run in owner.runs),
                )
            else:
                who = Message(
                    "Owner {claim_owner} is not a recorded run of this product.",
                    claim_owner=claim.owner_id,
                )
        except Exception as exc:
            who = Message(
                "The run status of owner {claim_owner} could not be read: {error}.",
                claim_owner=claim.owner_id,
                error=str(exc),
            )
            abandon = ""
        return Message(
            "{who}\nIf no process of that owner is running, clear the claim:\n  {clear}{abandon}",
            who=who,
            clear=clear,
            abandon=abandon,
            product=product,
            product_uri=product_uri,
            claim_domain=domain.identifier,
            claim_owner=claim.owner_id,
        )

    def list_claims(self, *, product: str | None = None):
        """List active write claims, optionally filtered by product."""
        return self.repo.list_claims(product=product)

    def list_stale_claims(self, *, product: str) -> list[ClaimInfo]:
        """List stale write claims for one product."""
        return self.repo.list_stale_claims(product=product)

    def list_stale_runs(self, *, product: str) -> list[RunInfo]:
        """List stale non-terminal runs for one product."""
        return self.repo.list_stale_runs(product=product)

    def clear_claim(self, *, product: str, domain_id: str, force: bool = False) -> bool:
        """Release a write claim by domain ID."""
        return self.repo.clear_claim(product=product, domain_id=domain_id, force=force)

    def clear_stale_claims(self, *, product: str, dry_run: bool = True) -> ClearSweepResult:
        """Bulk-clear stale write claims for a product with mutation-time re-check.

        Between preview (``list_stale_claims``) and mutation (``clear_claim``),
        a live pod could refresh a previously stale claim or another operator
        could delete it. Each mutation re-reads the current domain directly and
        only clears domains that remain stale AND present; live claims are never
        forcibly overridden. Errors are collected per-claim, and the sweep
        continues past any single-claim race.
        """
        result = ClearSweepResult()
        stale = self.list_stale_claims(product=product)
        result.previewed = [claim.domain for claim in stale]
        if dry_run:
            return result
        if self.repo.claims is None:
            self.repo._ensure_bound()
        assert self.repo.claims is not None
        for claim in stale:
            current = self.repo.claims.read_claim_by_domain(product=product, domain=claim.domain)
            if current is None:
                result.skipped_missing.append(claim.domain)
                continue
            if current.last_heartbeat_at != claim.last_heartbeat_at or not current.stale:
                result.skipped_fresh.append(claim.domain)
                continue
            try:
                cleared = self.clear_claim(product=product, domain_id=claim.domain, force=False)
            except ClaimConflictError:
                result.skipped_fresh.append(claim.domain)
                continue
            if cleared:
                result.cleared.append(claim.domain)
            else:
                result.skipped_missing.append(claim.domain)
        return result

    # ------------------------------------------------------------------
    # Resolved index: control-plane authoritative current.json.
    # ------------------------------------------------------------------

    def get_resolved_index(self, *, product: str) -> ResolvedIndexRecord | None:
        """Read the stored resolved-index record, or ``None`` if absent."""

        control_root_uri = self.repo.get_control_root_uri(product)
        _fs, control_root = self.repo._get_fs(control_root_uri)
        current_json = control_root.join(INDEX_DIRNAME).join(INDEX_CURRENT_FILENAME)
        try:
            with _fs.open(current_json, "rb") as fh:
                data = fh.read()
        except FileNotFoundError:
            return None
        return ResolvedIndexRecord.from_json_bytes(data)

    def ensure_resolved_index(
        self,
        *,
        product: str,
        record: ResolvedIndexRecord,
        run_id: str | None = None,
        max_retries: int = 5,
        initial_backoff_s: float = 0.1,
    ) -> tuple[ResolvedIndexRecord, Literal["created", "matched_existing"]]:
        """Ensure ``.firecube/index/current.json`` matches ``record``.

        This is the engine's resolved-index API. It uses a dedicated
        ``resolved_index:current`` write claim and implements the fresh-store
        and matched-existing precedence needed for startup negotiation.

        Raises:
            ProductUnderMaintenanceError: Another owner than ``record.recorded_by_run_id``
                holds the product's exclusive maintenance claim, or it cannot be read.
                The ``resolved_index:current`` claim is released first; no record is written.
        """

        if run_id is not None and run_id != record.recorded_by_run_id:
            raise ValueError(
                "run_id must match record.recorded_by_run_id: "
                f"run_id={run_id!r} recorded_by_run_id={record.recorded_by_run_id!r}"
            )
        if not record.recorded_by_run_id:
            raise ValueError("recorded_by_run_id must be non-empty")
        domain = WriteDomain(product=product, category="resolved_index", name="current")
        backoff = float(initial_backoff_s)
        for attempt in range(int(max_retries) + 1):
            try:
                with (
                    self.acquire_claim(
                        product=product,
                        domain=domain,
                        owner_id=record.recorded_by_run_id,
                        describe_conflict=False,
                    ),
                    self._holding(domain, record.recorded_by_run_id),
                ):
                    # Leaving the block on a refusal releases resolved_index:current.
                    self.check_exclusive_maintenance(
                        product=product, owner_id=record.recorded_by_run_id
                    )
                    return self._apply_resolved_index_precedence(product, record)
            except ClaimConflictError:
                # A loser reads and writes nothing before this check, as the
                # winner checks right after it acquires.
                self.check_exclusive_maintenance(
                    product=product, owner_id=record.recorded_by_run_id
                )
                # Unified 5-row convergence policy (see also
                # ``ensure_slot_index_model``). The two loser-branch policies
                # must stay identical so operators do not see one primitive
                # self-heal via re-mirror while the other refuses loudly.
                #
                # +-----+------------+---------------------+-----------------------+
                # | Row | CP record  | Attrs hash          | Action                |
                # +=====+============+=====================+=======================+
                # | 1   | matches    | absent (None)       | re-mirror + accept    |
                # | 2   | matches    | transient read error| propagate (raise)     |
                # | 3   | matches    | matches             | accept                |
                # | 4   | matches    | mismatches          | reject (drift error)  |
                # | 5   | mismatches | *                   | reject (CP conflict)  |
                # +-----+------------+---------------------+-----------------------+
                #
                # Row 2 is enforced implicitly: ``read_resolved_index_attrs_hash``
                # returns ``None`` for absent stores / missing attrs and re-raises
                # ``PermissionError``/``OSError``/``TimeoutError``/parse errors
                # (transient read failures are categorized separately from absent attrs).
                cp_record = self.get_resolved_index(product=product)
                if cp_record is not None and cp_record.identity_hash != record.identity_hash:
                    # Row 5: CP mismatch — refuse and audit.
                    self._record_conflict_refused_index_ensured_event(
                        product=product,
                        record=record,
                    )
                    raise ResolvedIndexConflictError(
                        "concurrent write detected with incompatible resolved index: "
                        f"stored={cp_record.identity_hash[:16]} "
                        f"declared={record.identity_hash[:16]}"
                    ) from None
                if cp_record is not None:
                    attrs_hash = self.read_resolved_index_attrs_hash(product=product)
                    if attrs_hash == record.identity_hash:
                        # Row 3: full convergence.
                        return cp_record, "matched_existing"
                    if attrs_hash is None:
                        # Row 1: pre-mirror compat — re-mirror from authoritative
                        # CP record so older cubes without an attrs mirror can
                        # start up cleanly instead of failing loudly.
                        self._mirror_resolved_index_attrs(product, cp_record)
                        return cp_record, "matched_existing"
                    # Row 4: attrs drifted from the authoritative CP record.
                    raise ManifestError(
                        "zarr root attrs have drifted from authoritative resolved-index record: "
                        f"cp_hash={cp_record.identity_hash[:16]} "
                        f"attrs_hash={str(attrs_hash)[:16]!r}"
                    ) from None
                if attempt == int(max_retries):
                    raise ResolvedIndexClaimTimeoutError(
                        f"resolved_index claim held for >{max_retries} retries; "
                        "current.json convergence not observed within retry budget"
                    ) from None
                time.sleep(backoff + random.random() * backoff)
                backoff *= 2
        raise ResolvedIndexClaimTimeoutError(
            "exhausted retries without converging on resolved_index"
        )

    def _apply_resolved_index_precedence(
        self, product: str, record: ResolvedIndexRecord
    ) -> tuple[ResolvedIndexRecord, Literal["created", "matched_existing"]]:
        """Apply the resolved-index precedence matrix under the current claim."""

        cp_record = self.get_resolved_index(product=product)
        attrs_hash = self.read_resolved_index_attrs_hash(product=product)

        if cp_record is None and attrs_hash is None:
            control_root_uri = self.repo.get_control_root_uri(product)
            _fs, control_root = self.repo._get_fs(control_root_uri)
            index_dir = control_root.join(INDEX_DIRNAME)
            current_json = index_dir.join(INDEX_CURRENT_FILENAME)
            try:
                _fs.makedirs(index_dir, exist_ok=True)
            except Exception:
                self.log.debug(
                    "resolved_index.makedirs_noop",
                    extra={"product": product, "path": str(index_dir)},
                )
            _fs.atomic_writer.write_atomic(current_json, record.to_json_bytes())
            self._mirror_resolved_index_attrs(product, record)
            return record, "created"

        if (
            cp_record is not None
            and cp_record.identity_hash == record.identity_hash
            and attrs_hash == record.identity_hash
        ):
            return cp_record, "matched_existing"

        if (
            cp_record is not None
            and cp_record.identity_hash == record.identity_hash
            and attrs_hash is None
        ):
            self._mirror_resolved_index_attrs(product, cp_record)
            return cp_record, "created"

        if cp_record is None and attrs_hash is not None:
            raise ResolvedIndexAttrsConflictError(
                "zarr root attrs have resolved-index identity hash but no control-plane record"
            )

        if cp_record is not None and cp_record.identity_hash != record.identity_hash:
            self._record_conflict_refused_index_ensured_event(
                product=product,
                record=record,
            )
            raise ResolvedIndexConflictError(
                "plugin declares incompatible resolved index:\n"
                f"{_format_resolved_index_diff(cp_record.index, record.index)}"
            )

        if cp_record is not None and attrs_hash != record.identity_hash:
            raise ResolvedIndexAttrsConflictError(
                "zarr root attrs have drifted from authoritative resolved-index record: "
                f"cp_hash={cp_record.identity_hash[:16]} "
                f"attrs_hash={str(attrs_hash)[:16]!r}"
            )

        raise ManifestError(
            "unhandled resolved-index precedence state (cp_record present, "
            "hashes agree on identity yet no earlier branch matched): "
            f"cp_record={cp_record!r} attrs_hash={attrs_hash!r}"
        )

    def _record_conflict_refused_index_ensured_event(
        self, *, product: str, record: ResolvedIndexRecord
    ) -> None:
        groups_by_name = record.index.get("groups", {}) or {}
        axis_kinds = tuple(sorted({str(g.get("kind", "")) for g in groups_by_name.values()}))
        group_names = tuple(sorted(groups_by_name.keys()))
        try:
            self.record_index_ensured_event(
                IndexEnsuredEvent(
                    run_id=record.recorded_by_run_id,
                    product=product,
                    identity_hash=record.identity_hash,
                    axis_kinds=axis_kinds,
                    groups=group_names,
                    outcome="conflict_refused",
                    timestamp=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                )
            )
        except Exception as exc:
            self.log.error(
                "Failed to record conflict_refused index_ensured WAL audit event for product %s: %s",
                product,
                exc,
            )

    def abandon_stale_runs(
        self, *, product: str, reason: str, dry_run: bool = True
    ) -> AbandonSweepResult:
        """Bulk-abandon stale non-terminal runs for a product with mutation-time re-check.

        Between preview (``list_runs``) and mutation (``abandon_run``), a
        live pod could refresh the run's heartbeat or the run could reach a
        terminal status. Each mutation re-reads the current run entry and only
        abandons runs that remain stale AND non-terminal.
        Runs already terminal are recorded as ``skipped_already_terminal``;
        runs whose heartbeat refreshed are recorded as ``skipped_fresh``. The
        sweep continues past any single-run race so one live pod cannot block
        cleanup of unrelated stale runs.

        The sweep lists the product's runs once, then its claims once, from a fresh listing,
        before it abandons anything. A claim listing error raises ManifestError and abandons
        nothing.

        After the abandons it resolves a claim's owner only when the owner id, or a `:`
        prefix, names a run listed terminal or one it abandoned. Other claims stay unreported.
        It releases a claim when its owner resolves to one terminal run alone and the claim is
        stale when re-read before the delete. A fresh claim, a moved heartbeat or an ambiguous
        owner leaves the claim in `skipped_claims`. Releasing a stale claim finishes a release
        a crash interrupted.

        A dry run treats the stale runs as abandoned and judges each claim as listed. It
        reports in `released_claims` and `skipped_claims` what a committed sweep would do, and
        deletes and abandons nothing.
        """
        result = AbandonSweepResult()
        runs = self.repo.list_runs(product=product)
        stale = [run for run in runs if not run.is_terminal and run.stale]
        terminal = {run.run_id for run in runs if run.is_terminal}
        result.previewed = [run.run_id for run in stale]
        claims = self._list_claims_to_release(
            product=product,
            recovery=lambda: "No run was abandoned; run the sweep again.",
        )
        if dry_run:
            return self._release_sweep_claims(
                product=product,
                result=result,
                claims=claims,
                ended=terminal | set(result.previewed),
                dry_run=True,
            )
        self.repo._ensure_bound()
        assert self.repo._resolver is not None
        assert self.repo._wal_reader is not None
        for run in stale:
            run_dir, run_uri = run_dir_for(self.repo._resolver, product, run.run_id)
            current_entry = self.repo._wal_reader.read_run_entry(
                product=product,
                run_dir=run_dir,
                run_uri=run_uri,
                run_id=run.run_id,
            )
            if current_entry is None:
                result.skipped_already_terminal.append(run.run_id)
                continue
            current = self.repo._run_info_from_entry(product, current_entry)
            if current.is_terminal:
                result.skipped_already_terminal.append(run.run_id)
                continue
            if not current.stale:
                result.skipped_fresh.append(run.run_id)
                continue
            try:
                outcome = self.repo.abandon_run(product=product, run_id=run.run_id, reason=reason)
            except ManifestError:
                result.skipped_already_terminal.append(run.run_id)
                continue
            if not outcome.get("abandoned"):
                result.skipped_already_terminal.append(run.run_id)
                continue
            result.abandoned.append(run.run_id)

        # Outside the ManifestError guard above: a corrupt claim file fails loudly.
        return self._release_sweep_claims(
            product=product,
            result=result,
            claims=claims,
            ended=terminal | set(result.abandoned),
            dry_run=False,
        )

    def _release_sweep_claims(
        self,
        *,
        product: str,
        result: AbandonSweepResult,
        claims: list[ClaimInfo],
        ended: set[str],
        dry_run: bool,
    ) -> AbandonSweepResult:
        """Release a sweep's stale claims whose owner resolves to one ended run alone.

        Resolves only owners with a candidate in `ended`, so live runs' claims cost no reads.
        A committed sweep trusts the resolved run status; a dry run also counts `ended` runs
        as terminal. Fills the claim fields of `result` and returns it.
        """
        candidates = [
            claim
            for claim in claims
            if any(candidate in ended for candidate in _owner_candidates(claim.owner_id))
        ]

        def has_ended(run: RunInfo) -> bool:
            return run.is_terminal or (dry_run and run.run_id in ended)

        def decide(owner: ClaimOwner) -> Literal["release", "skip"] | None:
            if owner.run is not None:
                return "release" if has_ended(owner.run) else None
            return "skip" if any(has_ended(run) for run in owner.runs) else None

        result.released_claims, result.skipped_claims = self._release_claims(
            product=product, claims=candidates, decide=decide, stale_only=True, dry_run=dry_run
        )
        return result

    # ------------------------------------------------------------------
    # Slot-index model: control-plane authoritative record + attrs mirror.
    # ------------------------------------------------------------------

    def get_slot_index_model(self, *, product: str) -> SlotIndexModelRecord | None:
        """Read the stored slot-index model record, or ``None`` if absent.

        Returns ``None`` when the on-disk ``current.json`` is missing. Other
        I/O errors propagate. ``ManifestError`` propagates on corruption (the
        on-disk record's identity-hash cross-check is enforced by
        `SlotIndexModelRecord.from_json_bytes`).
        """
        control_root_uri = self.repo.get_control_root_uri(product)
        _fs, control_root = self.repo._get_fs(control_root_uri)
        current_json = control_root.join(SLOT_INDEX_DIRNAME).join(SLOT_INDEX_CURRENT_FILENAME)
        try:
            with _fs.open(current_json, "rb") as fh:
                data = fh.read()
        except FileNotFoundError:
            return None
        return SlotIndexModelRecord.from_json_bytes(data)

    def ensure_slot_index_model(
        self,
        *,
        product: str,
        model: SlotIndexModel,
        run_id: str,
        max_retries: int = 5,
        initial_backoff_s: float = 0.1,
    ) -> SlotIndexModelRecord:
        """Negotiate the slot-index model for ``product``, returning the persisted record.

        Acquires the ``slot_index_model:current`` write claim and dispatches into
        `_apply_slot_model_precedence`. Loser threads (``ClaimConflictError``)
        apply the unified 5-row convergence policy documented inline in the
        loser branch below (mirrored on ``ensure_resolved_index``): CP+attrs
        match accepts, CP-only match with absent attrs re-mirrors from the
        authoritative CP record before accepting (pre-mirror-cube backward
        compatibility), CP-only match with drifted attrs raises drift,
        transient attrs read errors propagate, CP mismatch refuses, and a
        missing CP record keeps retrying with jittered exponential backoff
        until the budget is spent.
        """
        if not run_id:
            raise ValueError("run_id must be non-empty")
        domain = WriteDomain(product=product, category="slot_index_model", name="current")
        backoff = float(initial_backoff_s)
        for attempt in range(int(max_retries) + 1):
            try:
                with (
                    self.acquire_claim(
                        product=product,
                        domain=domain,
                        owner_id=run_id,
                        describe_conflict=False,
                    ),
                    self._holding(domain, run_id),
                ):
                    return self._apply_slot_model_precedence(product, model, run_id)
            except ClaimConflictError:
                # Unified 5-row convergence policy (mirrors
                # ``ensure_resolved_index``). The two loser-branch policies
                # must stay identical so operators do not see one primitive
                # self-heal via re-mirror while the other refuses loudly.
                #
                # +-----+------------+---------------------+-----------------------+
                # | Row | CP record  | Attrs hash          | Action                |
                # +=====+============+=====================+=======================+
                # | 1   | matches    | absent (None)       | re-mirror + accept    |
                # | 2   | matches    | transient read error| propagate (raise)     |
                # | 3   | matches    | matches             | accept                |
                # | 4   | matches    | mismatches          | reject (drift error)  |
                # | 5   | mismatches | *                   | reject (CP conflict)  |
                # +-----+------------+---------------------+-----------------------+
                #
                # Row 2 is enforced implicitly: ``read_slot_index_attrs_hash``
                # returns ``None`` for absent stores / missing attrs and re-raises
                # ``PermissionError``/``OSError``/``TimeoutError``/parse errors
                # (transient read failures are categorized separately from absent attrs).
                cp_record = self.get_slot_index_model(product=product)
                if cp_record is not None and cp_record.identity_hash != model.identity_hash:
                    # Row 5: CP mismatch — refuse.
                    raise SlotIndexModelConflictError(
                        "concurrent write detected with incompatible model: "
                        f"stored={cp_record.identity_hash[:16]} "
                        f"declared={model.identity_hash[:16]}"
                    ) from None
                if cp_record is not None:
                    attrs_hash = self.read_slot_index_attrs_hash(product=product)
                    if attrs_hash == model.identity_hash:
                        # Row 3: full convergence.
                        self.repo.record_slot_index_model_event(
                            product=product,
                            run_id=run_id,
                            event_type=EVENT_SLOT_INDEX_MODEL_VERIFIED,
                            identity_hash=model.identity_hash,
                            model_name=model.name,
                        )
                        return cp_record
                    if attrs_hash is None:
                        # Row 1: pre-mirror compat — re-mirror from authoritative
                        # CP record so older cubes without an attrs mirror can
                        # start up cleanly instead of failing loudly.
                        self._mirror_attrs(product, cp_record)
                        self.repo.record_slot_index_model_event(
                            product=product,
                            run_id=run_id,
                            event_type=EVENT_SLOT_INDEX_MODEL_VERIFIED,
                            identity_hash=model.identity_hash,
                            model_name=model.name,
                        )
                        return cp_record
                    # Row 4: attrs drifted from the authoritative CP record.
                    raise SlotIndexModelConflictError(
                        "zarr root attrs have drifted from authoritative CP record: "
                        f"cp_hash={cp_record.identity_hash[:16]} "
                        f"attrs_hash={str(attrs_hash)[:16]!r}"
                    ) from None
                if attempt == int(max_retries):
                    raise SlotIndexModelClaimTimeoutError(
                        f"slot_index_model claim held for >{max_retries} retries; "
                        "full convergence (CP+attrs) not observed within retry budget"
                    ) from None
                time.sleep(backoff + random.random() * backoff)
                backoff *= 2
        raise SlotIndexModelClaimTimeoutError(
            "exhausted retries without converging on slot_index_model"
        )

    def _apply_slot_model_precedence(
        self, product: str, model: SlotIndexModel, run_id: str
    ) -> SlotIndexModelRecord:
        """Apply the 6-row fresh-store precedence matrix.

        Must be called INSIDE the ``slot_index_model:current`` claim. The matrix
        is the only legitimate writer of ``current.json`` and the reserved zarr
        root attrs. Rows are enumerated explicitly to keep the decision table
        auditable from the source:

        +-----+----+-------+--------+-------------------------------------------+
        | Row | CP | Attrs | Plugin | Action                                    |
        +=====+====+=======+========+===========================================+
        | 1   |  - |   -   |   X    | Write CP + mirror attrs + emit RECORDED   |
        | 2   |  X |   X   |   X    | Happy path; emit VERIFIED                 |
        | 3   |  X |   -   |   X    | Crash-recovery: re-mirror attrs + VERIFIED|
        | 4   |  X |   Y   |   X    | Drift detected; refuse                    |
        | 5   |  X |   *   |   Y    | Plugin incompatibility; refuse            |
        | 6   |  - | present|  X    | Unmanaged store; refuse                   |
        +-----+----+-------+--------+-------------------------------------------+
        """
        cp_record = self.get_slot_index_model(product=product)
        attrs_hash = self.read_slot_index_attrs_hash(product=product)

        if cp_record is None and attrs_hash is None:
            # Row 1: fresh store, declare + mirror + RECORDED.
            recorded_at = datetime.now(tz=UTC).isoformat()
            new_record = SlotIndexModelRecord(
                model=model,
                identity_hash=model.identity_hash,
                schema_version="v1",
                recorded_at=recorded_at,
                recorded_by_run_id=run_id,
            )
            control_root_uri = self.repo.get_control_root_uri(product)
            _fs, control_root = self.repo._get_fs(control_root_uri)
            slot_index_dir = control_root.join(SLOT_INDEX_DIRNAME)
            current_json = slot_index_dir.join(SLOT_INDEX_CURRENT_FILENAME)
            try:
                _fs.makedirs(slot_index_dir, exist_ok=True)
            except Exception:
                self.log.debug(
                    "slot_index_model.makedirs_noop",
                    extra={"product": product, "path": str(slot_index_dir)},
                )
            _fs.atomic_writer.write_atomic(current_json, new_record.to_json_bytes())
            self._mirror_attrs(product, new_record)
            self.repo.record_slot_index_model_event(
                product=product,
                run_id=run_id,
                event_type=EVENT_SLOT_INDEX_MODEL_RECORDED,
                identity_hash=model.identity_hash,
                model_name=model.name,
            )
            self.log.info(
                "slot_index_model.recorded",
                extra={
                    "product": product,
                    "identity_hash": model.identity_hash[:16],
                    "model_name": model.name,
                },
            )
            return new_record

        elif (
            cp_record is not None
            and cp_record.identity_hash == model.identity_hash
            and attrs_hash == model.identity_hash
        ):
            # Row 2: happy path; CP and attrs both already match.
            self.repo.record_slot_index_model_event(
                product=product,
                run_id=run_id,
                event_type=EVENT_SLOT_INDEX_MODEL_VERIFIED,
                identity_hash=model.identity_hash,
                model_name=model.name,
            )
            return cp_record

        elif (
            cp_record is not None
            and cp_record.identity_hash == model.identity_hash
            and attrs_hash is None
        ):
            # Row 3: crash-recovery; CP is authoritative, re-mirror missing attrs.
            self._mirror_attrs(product, cp_record)
            self.repo.record_slot_index_model_event(
                product=product,
                run_id=run_id,
                event_type=EVENT_SLOT_INDEX_MODEL_VERIFIED,
                identity_hash=model.identity_hash,
                model_name=model.name,
            )
            return cp_record

        elif (
            cp_record is not None
            and cp_record.identity_hash == model.identity_hash
            and attrs_hash != model.identity_hash
        ):
            # Row 4: drift detected — attrs disagree with authoritative CP record.
            raise SlotIndexModelConflictError(
                "zarr root attrs have drifted from authoritative CP record: "
                f"cp_hash={cp_record.identity_hash[:16]} "
                f"attrs_hash={str(attrs_hash)[:16]!r}"
            )

        elif cp_record is not None and cp_record.identity_hash != model.identity_hash:
            # Row 5: plugin declares an incompatible model (different algorithm/epoch).
            raise SlotIndexModelConflictError(
                "plugin declares incompatible slot-index model: "
                f"stored={cp_record.identity_hash[:16]} "
                f"declared={model.identity_hash[:16]}"
            )

        elif cp_record is None and attrs_hash is not None:
            # Row 6: unmanaged store — root attrs exist without a control-plane record.
            raise SlotIndexUnmanagedStoreError(
                "zarr root has slot-model attrs but no control-plane record; "
                "this is an unmanaged store — firecube will not adopt it"
            )

        else:  # pragma: no cover - exhaustive guard
            raise AssertionError(
                f"unhandled slot-model precedence state: "
                f"cp_record={cp_record!r} attrs_hash={attrs_hash!r}"
            )

    def _open_store_for_mirror(self, product: str) -> Any:
        """Build the store behind the zarr root for attribute mirroring.

        Only constructs the store: nothing is read or created here. A missing
        root group is created later, under the mirror's metadata authority.
        """
        from zarr.storage import FsspecStore, LocalStore

        from firecube.core.filesystem.store_factory import create_zarr_store
        from firecube.core.uris import is_remote_target, local_path_from_target

        product_root = str(self.get_product_root(product))
        if is_remote_target(product_root):
            storage_config = self.storage_config
            if storage_config is None:
                raise RuntimeError(
                    "storage_config is required to open the remote zarr root for attr mirroring"
                )
            handle = create_zarr_store(uri=product_root, storage_config=storage_config, mode="a")
            kwargs = handle.zarr_kwargs()
            store = kwargs["store"]
            if isinstance(store, str):
                # The fsspec driver hands back a URL plus options; the metadata
                # authority needs the configured Store object itself.
                return FsspecStore.from_url(
                    store, storage_options=kwargs.get("storage_options"), read_only=False
                )
            return store
        return LocalStore(str(local_path_from_target(product_root)))

    @contextlib.contextmanager
    def _holding(self, domain: WriteDomain, owner_id: str) -> Generator[None, None, None]:
        """Name the claim the calling winner holds, for the root mirrors inside it.

        Only a winner, inside its ``with acquire_claim(...)`` block, enters this.
        The mirrors read it to build their authority; code outside it (the
        Row-1 losers) sees nothing and mirrors under a process-local authority.
        """
        token = _HELD_CLAIM.set(HeldClaim(domain_id=domain.identifier, owner_id=owner_id))
        try:
            yield
        finally:
            _HELD_CLAIM.reset(token)

    def _write_root_mirror(self, product: str, category: str, attrs: Mapping[str, Any]) -> None:
        """Write *attrs* to the zarr root through a metadata authority.

        The authority adopts the ``<category>:current`` claim when the caller is
        a winner holding it (see `_holding`); otherwise it is PROCESS_LOCAL (no claim is held).
        """
        store = self._open_store_for_mirror(product)
        scope = MetadataScope(
            _store_identity(str(self.get_product_root(product)), store),
            backing=store,
        )
        held = _HELD_CLAIM.get()
        expected = WriteDomain(product=product, category=category, name="current").identifier
        authority_cm = (
            adopt_held_claim(scope, held)
            if held is not None and held.domain_id == expected
            else hold_authority(scope, claim=None)
        )
        with authority_cm as authority:
            ensure_group(authority, store, "")
            set_attributes(authority, store, "", attrs)

    def _mirror_attrs(self, product: str, record: SlotIndexModelRecord) -> None:
        """Write the slot-index model attrs to the zarr root group.

        Authoritative writer of the reserved slot-index root attrs; bypasses
        the reserved-root-attrs guard by design (the guard blocks external
        user/plugin code paths only). Called by the winner INSIDE the
        ``slot_index_model:current`` claim (Row 1/Row 3 of the precedence
        matrix) and by Row-1 losers OUTSIDE the claim (backward-compatible
        re-mirror for pre-attrs-mirror cubes). Both call sites write the
        same value deterministically from the CP record, so concurrent
        invocations converge on identical attrs.
        """
        self._write_root_mirror(
            product,
            "slot_index_model",
            {
                SLOT_INDEX_MODEL_ATTR: record.model.canonical_bytes().decode("utf-8"),
                SLOT_INDEX_MODEL_IDENTITY_HASH_ATTR: record.identity_hash,
            },
        )

    def _mirror_resolved_index_attrs(self, product: str, record: ResolvedIndexRecord) -> None:
        """Write the resolved-index attrs to the zarr root group.

        Authoritative writer of the reserved resolved-index root attrs;
        bypasses the reserved-root-attrs guard by design (the guard blocks
        external user/plugin code paths only). Called by the winner INSIDE
        the ``resolved_index:current`` claim (Row 1/Row 3 of the precedence
        matrix) and by Row-1 losers OUTSIDE the claim (backward-compatible
        re-mirror for pre-attrs-mirror cubes). Both call sites write the
        same value deterministically from the CP record, so concurrent
        invocations converge on identical attrs.
        """
        self._write_root_mirror(
            product,
            "resolved_index",
            {
                RESOLVED_INDEX_ATTR: canonical_index_bytes(record.index).decode("utf-8"),
                RESOLVED_INDEX_IDENTITY_HASH_ATTR: record.identity_hash,
            },
        )

    def _read_root_index_attrs_hash(
        self, *, product: str, attr_name: str, label: str
    ) -> str | None:
        import zarr
        from zarr.storage import LocalStore

        from firecube.core.filesystem.store_factory import create_zarr_store
        from firecube.core.uris import is_remote_target, local_path_from_target

        product_root = str(self.get_product_root(product))
        try:
            if is_remote_target(product_root):
                storage_config = self.storage_config
                if storage_config is None:
                    return None
                handle = create_zarr_store(
                    uri=product_root, storage_config=storage_config, mode="r"
                )
                root = zarr.open_group(
                    **handle.zarr_kwargs(), mode="r", zarr_format=3, use_consolidated=False
                )
            else:
                local = local_path_from_target(product_root)
                store = LocalStore(str(local))
                root = zarr.open_group(store=store, mode="r", zarr_format=3, use_consolidated=False)
        except FileNotFoundError:
            return None
        except (PermissionError, OSError, TimeoutError, json.JSONDecodeError, ValueError) as exc:
            log.warning("Failed to read %s hash for %s: %s", label, product, exc, exc_info=True)
            raise

        try:
            value = root.attrs[attr_name]
        except KeyError:
            return None
        if value is None:
            return None
        return str(value)

    def read_resolved_index_attrs_hash(self, *, product: str) -> str | None:
        """Read the resolved-index identity hash mirrored on the product's
        Zarr root attributes, or ``None`` if not present.

        Public read-only query used by the ``firecube zarr index verify`` and
        ``firecube zarr index rebuild`` CLI to detect attrs/on-disk-record drift.

        Args:
            product: Logical product name.

        Returns:
            The identity hash string, or ``None`` if the attribute is absent
            (fresh store, or attrs were cleared after the record was written).
            Transient read/parse failures are logged and re-raised.
        """

        return self._read_root_index_attrs_hash(
            product=product,
            attr_name=RESOLVED_INDEX_IDENTITY_HASH_ATTR,
            label="resolved-index",
        )

    def read_slot_index_attrs_hash(self, *, product: str) -> str | None:
        """Read the legacy slot-index identity hash mirrored on root attrs.

        Public read-only query used by the ``firecube zarr index rebuild`` CLI to
        detect slot-index attrs/on-disk-record drift.

        Returns ``None`` when the zarr root group is missing or does not yet
        carry the reserved attr. Missing stores and absent attrs are treated as
        legitimate fresh-store input. Transient read/parse failures are logged
        and re-raised for the caller.
        """
        return self._read_root_index_attrs_hash(
            product=product,
            attr_name=SLOT_INDEX_MODEL_IDENTITY_HASH_ATTR,
            label="slot-index",
        )

    def _metadata_store(self, product: str) -> tuple[StorageFilesystemFull, StorageUri]:
        """Return the driver and the root of the product, where `.firecube/metadata/` lives."""
        fs, product_root = self.repo._get_fs(self.repo.get_product_root_uri(product))
        return cast(StorageFilesystemFull, fs), product_root

    def take_snapshot(
        self,
        product: str,
        *,
        run_id: str | None,
        trigger: str,
        consistency: SnapshotConsistency,
    ) -> SnapshotRecord:
        """Capture every metadata document of the product and publish the record as the latest.

        An `observation` takes `run_id=None`. A `coherent` snapshot reads the exclusive
        maintenance claim before and after the capture; `run_id` must own it.

        Raises:
            ValueError: `run_id` does not fit `consistency`, or `trigger` is empty.
            ManifestError: A coherent snapshot's claim is absent, unreadable or owned by
                another run. Nothing is written.
            ControlPlaneCorruptionError: `refs/latest.json` does not parse. Nothing is written.
        """
        from firecube.core.controlplane import metadata_snapshots

        def verify_holder() -> None:
            refusal = exclusive_maintenance_holder_refusal(
                repo=self.repo,
                product=product,
                operation="coherent metadata snapshot",
                holder_run_id=str(run_id),
            )
            if refusal is not None:
                raise ManifestError(refusal)

        fs, product_root = self._metadata_store(product)
        return metadata_snapshots.take_snapshot(
            fs,
            product_root,
            run_id=run_id,
            trigger=trigger,
            consistency=consistency,
            verify_holder=verify_holder,
        )

    def list_snapshots(self, product: str) -> list[SnapshotRecord]:
        """Return every metadata snapshot record of the product, oldest first."""
        from firecube.core.controlplane import metadata_snapshots

        fs, product_root = self._metadata_store(product)
        return metadata_snapshots.list_records(fs, metadata_snapshots.metadata_root(product_root))

    def read_snapshot(self, product: str, snapshot_id: str) -> Snapshot:
        """Return a snapshot record and its manifest; `latest` names the newest.

        Raises:
            ManifestError: The id is malformed or names no snapshot.
            ControlPlaneCorruptionError: A stored file does not match its id or does not parse.
        """
        from firecube.core.controlplane import metadata_snapshots

        fs, product_root = self._metadata_store(product)
        root = metadata_snapshots.metadata_root(product_root)
        return metadata_snapshots.read_snapshot(fs, root, snapshot_id)

    def diff_snapshots(self, product: str, before: str, after: str) -> SnapshotDiff:
        """Diff two snapshots of the product by path and, for changed nodes, by parsed facets.

        Each id is a record id or `latest`.

        Raises:
            ManifestError: An id is malformed or names no snapshot.
            ControlPlaneCorruptionError: A stored file does not match its id or does not parse.
        """
        from firecube.core.controlplane import metadata_snapshots

        fs, product_root = self._metadata_store(product)
        root = metadata_snapshots.metadata_root(product_root)
        return metadata_snapshots.diff_snapshots(
            metadata_snapshots.read_snapshot(fs, root, before),
            metadata_snapshots.read_snapshot(fs, root, after),
            lambda blob_id: metadata_snapshots.read_blob(fs, root, blob_id),
        )

    def write_metadata_blob(self, product: str, data: bytes) -> str:
        """Store `data` under `.firecube/metadata/blobs/` unless it exists; return its sha256 hex.

        An existing blob is never rewritten.
        """
        from firecube.core.controlplane import metadata_snapshots

        fs, product_root = self._metadata_store(product)
        return metadata_snapshots.write_blob(
            fs, metadata_snapshots.metadata_root(product_root), data
        )

    def read_metadata_blob(self, product: str, blob_id: str) -> bytes:
        """Return the bytes of the blob whose sha256 hex is `blob_id`.

        Raises:
            ManifestError: `blob_id` is not 64 lowercase hex digits.
            ControlPlaneCorruptionError: The blob is missing or its bytes do not match the id.
        """
        from firecube.core.controlplane import metadata_snapshots

        fs, product_root = self._metadata_store(product)
        root = metadata_snapshots.metadata_root(product_root)
        return metadata_snapshots.read_blob(fs, root, blob_id)

    def enumerate_metadata_documents(self, product: str) -> list[MetadataDocument]:
        """Read the product's metadata documents live, as a snapshot captures them, sorted by path.

        The read writes nothing and takes no claim.
        """
        from firecube.core.controlplane import metadata_snapshots
        from firecube.core.zarr.metadata_documents import enumerate_metadata_documents

        fs, product_root = self._metadata_store(product)
        return metadata_snapshots.captured_documents(enumerate_metadata_documents(fs, product_root))

    def _run_journal_uri(self, product: str, run_id: str) -> StorageUri:
        self.repo._ensure_bound()
        assert self.repo._resolver is not None
        run_dir, _run_uri = run_dir_for(self.repo._resolver, product, run_id)
        return run_dir.join(RUN_JOURNAL_FILENAME)

    def write_run_journal(self, product: str, run_id: str, data: bytes) -> None:
        """Create `.firecube/runs/<run_id>/plan.json` with `data` once.

        A journal that already holds the same bytes is kept.

        Raises:
            ValueError: `run_id` is not a single path segment.
            ManifestError: The journal exists with other bytes; nothing is written.
        """
        uri = self._run_journal_uri(product, run_id)
        fs = cast(StorageFilesystemFull, self.repo._fs)
        with contextlib.suppress(Exception):
            fs.makedirs(uri.parent(), exist_ok=True)
        try:
            fs.atomic_writer.write_atomic(uri, data)
        except FileExistsError:
            if fs.read_bytes(uri) != data:
                raise ManifestError(
                    f"run journal {uri.to_str()} already exists with other content"
                ) from None

    def read_run_journal(self, product: str, run_id: str) -> bytes | None:
        """Return the bytes of `.firecube/runs/<run_id>/plan.json`, or `None` when the run has none.

        Raises:
            ValueError: `run_id` is not a single path segment.
        """
        uri = self._run_journal_uri(product, run_id)
        fs = cast(StorageFilesystemFull, self.repo._fs)
        try:
            return fs.read_bytes(uri)
        except FileNotFoundError:
            return None

    def list_run_journals(self, product: str, *, prefix: str) -> list[str]:
        """Return the ids starting with `prefix` of runs that hold a journal, from a fresh listing.

        A journal written before its run's start was recorded is listed too. A failed
        listing of `runs/` raises; an absent `runs/` lists none.
        """
        from firecube.core.controlplane._wal_reader import WalReader

        self.repo._ensure_bound()
        assert self.repo._resolver is not None
        control_path, _control_uri = self.repo._resolver(product)
        runs_dir = control_path.join(RUNS_DIRNAME)
        fs = cast(Any, self.repo._fs)
        try:
            listed = fs.ls(runs_dir, detail=False, refresh=True)
        except FileNotFoundError:
            return []
        run_ids = (
            WalReader._run_id_from_dir(WalReader._entry_to_uri(item, runs_dir), runs_dir)
            for item in listed
        )
        return sorted(
            run_id
            for run_id in run_ids
            if run_id.startswith(prefix) and fs.exists(runs_dir.join(run_id, RUN_JOURNAL_FILENAME))
        )

    def scan_runs(self, *, product: str) -> tuple[list[RunInfo], list[tuple[str, str]]]:
        """Read every run of the product from fresh listings, for an admission that fails closed.

        A failed `runs/` listing raises. Returns the readable runs and `(run_id, error)`
        for each run that cannot be read.
        """
        return self.repo.scan_runs(product=product)

    def has_control_plane(self, product: str) -> bool:
        """Return whether the product has `.firecube/schema.json` or any run, from fresh reads."""
        self.repo._ensure_bound()
        assert self.repo._resolver is not None
        control_path, _control_uri = self.repo._resolver(product)
        fs = cast(StorageFilesystemFull, self.repo._fs)
        if fs.exists(control_path.join(SCHEMA_FILENAME)):
            return True
        runs, unreadable = self.repo.scan_runs(product=product)
        return bool(runs or unreadable)


def _abandon_operation(product_uri: str, run_id: str) -> Operation:
    """Name the abandon of one run, its reason left as a placeholder."""
    return Operation(
        "abandon_run", product_name=product_uri, run_id=run_id, reason=Placeholder('"<reason>"')
    )


def _clear_operation(product_uri: str, domain_id: str, *, force: bool) -> Operation:
    """Name the clear of one claim domain; pass `force=True` for a claim that is not stale."""
    return Operation("clear_claim", product_name=product_uri, domain_id=domain_id, force=force)


def maintenance_refusal_message(
    *,
    repo: ManifestRepository,
    product: str,
    operation: str,
    detail: str,
    claims: Sequence[ClaimInfo],
) -> Message:
    """Return the message that refuses a maintenance operation, with its recovery operations.

    Name each blocking claim and its owner, the clear operation for it (forced when the
    claim is fresh) and, when its owner resolves to exactly one recorded run, the abandon
    operation. Read errors drop the owner line and the abandon, never the clear.
    """
    refusal = "Cannot run {operation} for product {product}: {detail}."
    facts: dict[str, object] = {"operation": operation, "product": product, "detail": detail}
    if not claims:
        return Message(refusal, **facts)
    try:
        product_uri = repo.get_product_root_uri(product)
    except Exception as exc:
        return Message(
            refusal + " The product root could not be resolved: {error}.", **facts, error=str(exc)
        )
    lines: list[str] = [Message(refusal, **facts, product_uri=product_uri)]
    owner_runs: dict[str, Any] = {}
    for claim in claims:
        if claim.owner_id not in owner_runs:
            try:
                owner_runs[claim.owner_id] = repo.resolve_claim_owner(
                    product=product, owner_id=claim.owner_id
                ).run
            except Exception:
                owner_runs[claim.owner_id] = None
        run = owner_runs[claim.owner_id]
        abandon: str = ""
        if run is not None:
            abandon = Message(
                "\nor abandon run {run_id} (status: {status}), which releases the claims "
                "it alone owns:\n  {abandon}",
                run_id=run.run_id,
                status=run.status,
                abandon=_abandon_operation(product_uri, run.run_id),
            )
        lines.append(
            Message(
                "\nIf no process of {claim_owner} is running, clear the claim {claim_domain}:"
                "\n  {clear}{abandon}",
                claim_owner=claim.owner_id,
                claim_domain=claim.domain,
                clear=_clear_operation(product_uri, claim.domain, force=not claim.stale),
                abandon=abandon,
            )
        )
    return Message.join("", lines)


def check_exclusive_maintenance(*, repo: ManifestRepository, product: str, owner_id: str) -> None:
    """Refuse `owner_id` while another owner holds the product's exclusive maintenance claim.

    Reads the one claim file by name. An absent claim, or one that `owner_id` owns, passes.

    Raises:
        ProductUnderMaintenanceError: Another owner holds the claim, or it cannot be read.
    """
    domain = exclusive_maintenance_domain(product)
    try:
        claim = repo.read_claim_by_domain(product=product, domain=domain.identifier)
    except Exception as exc:
        raise ProductUnderMaintenanceError(
            Message(
                "The exclusive maintenance claim {claim_domain} of product {product} could "
                "not be read, so {writer} is refused: {error}",
                claim_domain=domain.identifier,
                product=product,
                writer=owner_id,
                error=str(exc),
            ),
            product=product,
            owner_id=None,
            stale=None,
        ) from exc
    if claim is None or claim.owner_id == owner_id:
        return
    raise ProductUnderMaintenanceError(
        under_maintenance_message(repo=repo, product=product, writer=owner_id, claim=claim),
        product=product,
        owner_id=claim.owner_id,
        stale=claim.stale,
    )


def is_exclusive_maintenance_holder(
    claim: ClaimInfo | None, *, product: str, holder_run_id: str | None
) -> bool:
    """Return whether `claim` is the product's exclusive maintenance claim and `holder_run_id` owns it.

    A `holder_run_id` of `None` owns nothing, so the result is then `False`.
    """
    return (
        holder_run_id is not None
        and claim is not None
        and claim.domain == exclusive_maintenance_domain(product).identifier
        and claim.owner_id == holder_run_id
    )


def holder_absent_message(*, product: str, operation: str, holder_run_id: str) -> Message:
    """Return the message that refuses a holder's operation when no exclusive claim is present."""
    return Message(
        "Cannot run {operation} for product {product} as holder {holder_run_id}: the product "
        "has no exclusive maintenance claim {claim_domain}.",
        operation=operation,
        product=product,
        holder_run_id=holder_run_id,
        claim_domain=exclusive_maintenance_domain(product).identifier,
    )


def exclusive_maintenance_holder_refusal(
    *, repo: ManifestRepository, product: str, operation: str, holder_run_id: str
) -> Message | None:
    """Return why `holder_run_id` does not hold the product's exclusive maintenance claim.

    Reads the one claim file by name. An absent claim, another owner's claim and an
    unreadable claim each refuse. Return `None` when `holder_run_id` owns the claim.
    """
    domain = exclusive_maintenance_domain(product)
    try:
        claim = repo.read_claim_by_domain(product=product, domain=domain.identifier)
    except Exception as exc:
        return Message(
            "The exclusive maintenance claim {claim_domain} of product {product} could "
            "not be read, so {writer} is refused: {error}",
            claim_domain=domain.identifier,
            product=product,
            writer=holder_run_id,
            error=str(exc),
        )
    if is_exclusive_maintenance_holder(claim, product=product, holder_run_id=holder_run_id):
        return None
    if claim is None:
        return holder_absent_message(
            product=product, operation=operation, holder_run_id=holder_run_id
        )
    return under_maintenance_message(repo=repo, product=product, writer=holder_run_id, claim=claim)


def under_maintenance_message(
    *, repo: ManifestRepository, product: str, writer: str, claim: ClaimInfo
) -> Message:
    """Return the message that refuses `writer` under another owner's exclusive maintenance claim.

    Name the claim's owner. For a stale claim, add the clear operation and, when the
    owner resolves to exactly one recorded run, the abandon operation. An unresolved
    product root drops both operations; an owner read error drops the abandon.
    """
    facts: dict[str, object] = {
        "product": product,
        "writer": writer,
        "claim_owner": claim.owner_id,
        "claim_domain": claim.domain,
    }
    refusal = Message(
        "Product {product} is under exclusive maintenance by {claim_owner} "
        "(claim {claim_domain}), so {writer} is refused.",
        **facts,
    )
    if not claim.stale:
        return Message(
            "{refusal} Retry when that maintenance has finished.", refusal=refusal, **facts
        )
    stale = Message(
        "{refusal} The claim is stale: no heartbeat for {age}.",
        refusal=refusal,
        age=_format_duration(max(time.time() - claim.last_heartbeat_at, 0.0)),
        **facts,
    )
    try:
        product_uri = repo.get_product_root_uri(product)
    except Exception:
        return stale
    abandon: str = ""
    try:
        run = repo.resolve_claim_owner(product=product, owner_id=claim.owner_id).run
    except Exception:
        run = None
    if run is not None:
        abandon = Message(
            "\nor abandon run {run_id} (status: {status}), which releases the claims it "
            "alone owns:\n  {abandon}",
            run_id=run.run_id,
            status=run.status,
            abandon=_abandon_operation(product_uri, run.run_id),
        )
    return Message(
        "{stale}\nIf no process of {claim_owner} is running, clear the claim:\n  {clear}{abandon}",
        stale=stale,
        clear=_clear_operation(product_uri, claim.domain, force=False),
        abandon=abandon,
        product_uri=product_uri,
        **facts,
    )


def check_legacy_index_record(
    chunk_manager: ChunkManager,
    *,
    product: str,
    plugin_name: str,
) -> None:
    """Raise ``LegacyIndexRecordError`` when only the legacy slot-index record is present.

    Detects legacy cubes whose control plane still
    carries ``.firecube/slot_index/current.json`` but has not yet produced the
    ``.firecube/index/current.json`` resolved-index record. Called at ``DirectZarrIngestor``
    pod startup and by ``firecube zarr preallocate`` BEFORE any
    `ChunkManager.ensure_resolved_index` call so a legacy cube cannot be
    silently overwritten by a fresh resolved-index stamp.

    Presence check only: the legacy payload is never trusted for policy. Fresh
    cubes (both files absent) and already-migrated cubes (new file present)
    pass through without raising.
    """
    if chunk_manager.get_resolved_index(product=product) is not None:
        return
    control_root_uri = chunk_manager.repo.get_control_root_uri(product)
    fs, control_root_path = chunk_manager.repo._get_fs(control_root_uri)
    current_json = control_root_path.join(SLOT_INDEX_DIRNAME).join(SLOT_INDEX_CURRENT_FILENAME)
    try:
        with fs.open(current_json, "rb"):
            pass
    except FileNotFoundError:
        return
    control_root = chunk_manager.get_control_root(product)
    legacy_path = f"{control_root.rstrip('/')}/{SLOT_INDEX_DIRNAME}/{SLOT_INDEX_CURRENT_FILENAME}"
    product_uri = chunk_manager.get_product_root(product)
    raise LegacyIndexRecordError(
        Message(
            "Legacy index record detected at {legacy_path}. Run `{rebuild}` to migrate.",
            legacy_path=legacy_path,
            rebuild=Operation(
                "rebuild_index", target=product_uri, plugin=plugin_name, product_name=product
            ),
            product=product,
            product_uri=product_uri,
            plugin=plugin_name,
        )
    )
