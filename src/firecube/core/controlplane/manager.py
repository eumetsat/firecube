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
import hashlib
import json
import logging
import random
import time
from collections.abc import Callable, Generator, Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from firecube.core.config import StorageConfig
from firecube.core.controlplane._paths import run_dir_for
from firecube.core.controlplane.claims import ClaimHandle
from firecube.core.controlplane.deletion import DeletionEngine
from firecube.core.controlplane.events import ConsolidatedTimeCoord
from firecube.core.controlplane.repo import ManifestRepository
from firecube.core.controlplane.types import (
    EVENT_SLOT_INDEX_MODEL_RECORDED,
    EVENT_SLOT_INDEX_MODEL_VERIFIED,
    INDEX_CURRENT_FILENAME,
    INDEX_DIRNAME,
    RESOLVED_INDEX_ATTR,
    RESOLVED_INDEX_IDENTITY_HASH_ATTR,
    SLOT_INDEX_CURRENT_FILENAME,
    SLOT_INDEX_DIRNAME,
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
)
from firecube.core.errors import (
    ClaimConflictError,
    ConfigurationError,
    LegacyIndexRecordError,
    ManifestError,
    ResolvedIndexClaimTimeoutError,
    ResolvedIndexConflictError,
    SlotIndexModelClaimTimeoutError,
    SlotIndexModelConflictError,
    SlotIndexUnmanagedStoreError,
)
from firecube.core.filesystem import StorageFilesystem
from firecube.core.slot_index import (
    SLOT_INDEX_MODEL_ATTR,
    SLOT_INDEX_MODEL_IDENTITY_HASH_ATTR,
    SlotIndexModel,
)
from firecube.core.storage.binding import StorageBinding

log = logging.getLogger(__name__)


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


def _dedupe_active_spans(chunks: list[ChunkInfo]) -> list[ChunkInfo]:
    """Drop duplicate active spans that cover the same slice during force-reingest in-flight.

    During the brief window when a force-reingest run has emitted its new
    span (``status="active"``) but has not yet emitted
    ``replacement_committed`` for the prior span, both spans appear active.
    To avoid showing double coverage in the read models, this helper keeps
    the span with the highest ``run_id`` per ``(product, group, time_min,
    time_max)`` key and drops the others.  ``run_id`` values are
    UUID/timestamp strings, so lexicographic comparison is deterministic.

    Non-span chunks and spans whose status is not ``"active"`` pass through
    unchanged.  Active spans missing any of ``group``/``time_min``/
    ``time_max`` in their meta also pass through (they cannot be grouped
    into a slice key).
    """
    winners: dict[tuple[str, Any, Any, Any], str] = {}
    for chunk in chunks:
        if chunk.chunk_type != "span" or chunk.status != "active":
            continue
        meta = chunk.meta or {}
        key = _slice_dedupe_key(chunk.product, meta)
        if key is None:
            continue
        run_id = str(meta.get("run_id", ""))
        if winners.get(key, "") < run_id:
            winners[key] = run_id

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
        if winners.get(key) == str(meta.get("run_id", "")):
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
        """Return the resolved LATEST.json pointer URI."""
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
        """Record the start of a new ingestion run."""
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
        """Record informational replacement intent for a run."""
        self.repo.record_run_started_with_replacement(
            product=product,
            run_id=run_id,
            replaces=replaces,
        )

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
            run_id="time-coord-consolidation",
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
    ) -> list[ChunkInfo]:
        """Return projected chunk records with optional filtering.

        During a force-reingest run there is a brief window where both prior
        spans and new spans have ``status="active"`` (before
        ``replacement_committed`` lands).  To prevent double coverage in
        callers, this method deduplicates active spans by
        ``(product, group, time_min, time_max)``: when multiple active spans
        share the same slice key, only the span with the highest ``run_id``
        (lexicographic order — UUID/timestamp run_ids make this
        deterministic) is returned.  Non-span records and
        ``replaced``/``failed`` spans pass through unchanged, so this is a
        no-op outside the force-reingest in-flight window.
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
        )
        return _dedupe_active_spans(chunks)

    def mark_chunks_replaced(
        self, chunk_keys: list[str], product: str, timestamp: float
    ) -> dict[str, Any]:
        """Mark chunk records as replaced with a timestamp."""
        return self.repo.mark_chunks_replaced(chunk_keys, product, timestamp)

    def list_runs(
        self,
        *,
        product: str,
        status: str | None = None,
        non_terminal: bool = False,
    ) -> list[RunInfo]:
        """List runs for a product with optional status/terminal filtering."""
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
    ) -> ClaimHandle:
        """Atomically reject overlapping coord-chunk peers and return a live claim.

        Under a single write-domain claim lock, this checks every windowed
        group against active peer runs using that group's own coordinate
        chunk geometry, rejects any window that touches a peer-owned
        coordinate chunk, then records this run as started while the claim is
        still held.

        If this method raises, no run is registered and no terminalization is
        needed. If it returns normally, the caller must release the returned
        handle after materialization and record a terminal run state on every
        exit path.
        """

        # Function-scope import: avoid module-level cycle with core.zarr.
        from firecube.core.zarr.chunk_geometry import chunk_axis_range

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
                raise ConfigurationError(
                    f"conflicting serial ingest run {serial_peer.run_id} is live "
                    f"(holds full extent; state: {serial_peer.status}, running for {elapsed}); "
                    f"if this run is confirmed dead, abandon it explicitly: "
                    f"firecube chunks runs abandon {serial_peer.run_id} "
                    f"then re-run preallocate. Automatic timeout is not implemented."
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
                        raise ConfigurationError(
                            f"group {group!r} window [{window[0]}, {window[1]}) touches "
                            f"coordinate chunk(s) {overlap} owned by run {peer.run_id} "
                            f"(state: {peer.status}, materializing for {elapsed}); "
                            f"if this run is confirmed dead, abandon it explicitly: "
                            f"firecube chunks runs abandon {peer.run_id} "
                            f"then re-run preallocate. Automatic timeout is not implemented."
                        )

            hull_start = min(window[0] for window in windows_by_group.values())
            hull_end = max(window[1] for window in windows_by_group.values())
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
        """Mark a non-terminal run as abandoned (control-plane only, no data cleanup)."""
        return self.repo.abandon_run(
            product=product,
            run_id=run_id,
            reason=reason,
            meta=meta,
        )

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
        )

    def execute_deletion(
        self,
        plan: DeletionPlan,
        delete_storage: bool = True,
        delete_manifest: bool = True,
        storage_config: StorageConfig | None = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Execute a deletion plan, optionally removing storage and manifest entries."""
        return self.deletion_engine.execute_deletion(
            plan,
            delete_storage,
            delete_manifest,
            storage_config,
            dry_run,
        )

    def delete_spans(
        self,
        spans: Iterable[ChunkInfo],
        *,
        dry_run: bool = False,
        force: bool = False,
        update_manifest: bool = True,
        update_state: bool = True,
        time_dim_name: str | None = None,
    ) -> dict[str, Any]:
        """Delete specific span records with optional dry-run and manifest updates."""
        return self.deletion_engine.delete_spans(
            spans,
            dry_run=dry_run,
            force=force,
            update_manifest=update_manifest,
            update_state=update_state,
            time_dim_name=time_dim_name,
        )

    def rebuild_snapshot(self, product: str) -> dict[str, Any]:
        """Compact completed-run WAL events into a snapshot for faster reads."""
        return self.repo.rebuild_snapshot(product)

    def snapshot_status(self, product: str) -> dict[str, Any]:
        self.repo._ensure_bound()
        latest = self.repo._read_latest_pointer(product)
        if latest is None:
            return {"exists": False}

        cutoff = float(latest.get("completed_before", 0.0) or 0.0)
        age_seconds = time.time() - cutoff if cutoff > 0 else 0.0
        completed_before_iso: str | None = None
        if cutoff > 0:
            completed_before_iso = datetime.fromtimestamp(cutoff, tz=UTC).isoformat()

        snapshot_meta_path = str(latest.get("snapshot_meta_path") or "")
        records = 0
        repo_fs = self.repo._fs
        if snapshot_meta_path and repo_fs is not None:
            try:
                with repo_fs.open(snapshot_meta_path, "r") as fh:
                    meta = json.load(fh)
                records = int(meta.get("records", meta.get("record_count", 0)))
            except Exception:
                records = 0

        return {
            "exists": True,
            "completed_before": completed_before_iso,
            "age_seconds": age_seconds,
            "age_human": _format_duration(age_seconds),
            "generation": latest.get("generation"),
            "records": records,
        }

    def acquire_claim(self, *, product: str, domain: WriteDomain, owner_id: str):
        """Acquire an exclusive write claim for a domain."""
        return self.repo.acquire_claim(product=product, domain=domain, owner_id=owner_id)

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
                with self.acquire_claim(
                    product=product, domain=domain, owner_id=record.recorded_by_run_id
                ):
                    return self._apply_resolved_index_precedence(product, record)
            except ClaimConflictError:
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
            raise ManifestError(
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
            raise ManifestError(
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

        Between preview (``list_stale_runs``) and mutation (``abandon_run``), a
        live pod could refresh the run's heartbeat or the run could reach a
        terminal status. Each mutation re-reads the current run entry and only
        abandons runs that remain stale AND non-terminal.
        Runs already terminal are recorded as ``skipped_already_terminal``;
        runs whose heartbeat refreshed are recorded as ``skipped_fresh``. The
        sweep continues past any single-run race so one live pod cannot block
        cleanup of unrelated stale runs.
        """
        result = AbandonSweepResult()
        stale = self.list_stale_runs(product=product)
        result.previewed = [run.run_id for run in stale]
        if dry_run:
            return result
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
                outcome = self.abandon_run(product=product, run_id=run.run_id, reason=reason)
            except ManifestError:
                result.skipped_already_terminal.append(run.run_id)
                continue
            if outcome.get("abandoned"):
                result.abandoned.append(run.run_id)
            else:
                result.skipped_already_terminal.append(run.run_id)
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
                with self.acquire_claim(product=product, domain=domain, owner_id=run_id):
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

    def _open_zarr_root_for_mirror(self, product: str) -> Any:
        """Open the zarr root group for attribute mirroring, tolerating the
        ``open_group(mode="a")`` check-then-create race that surfaces when
        the claim-holding winner and Row-1 re-mirror losers race
        to create the group. Returns the opened zarr group.

        The retry is safe because ``mode="a"`` opens the group in place once
        it exists — no divergent state can be written between attempts.
        """
        import zarr
        from zarr.errors import ContainsGroupError
        from zarr.storage import LocalStore

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
            open_kwargs: dict[str, Any] = {**handle.zarr_kwargs(), "mode": "a", "zarr_format": 3}
        else:
            store = LocalStore(str(local_path_from_target(product_root)))
            open_kwargs = {"store": store, "mode": "a", "zarr_format": 3}

        last_exc: BaseException | None = None
        for attempt in range(4):
            try:
                return zarr.open_group(**open_kwargs)
            except ContainsGroupError as exc:
                last_exc = exc
                if attempt == 3:
                    raise
                time.sleep(0.005 * (attempt + 1))
        raise RuntimeError("failed to open zarr root for mirror after retries") from last_exc

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
        root = self._open_zarr_root_for_mirror(product)
        root.attrs.update(
            {
                SLOT_INDEX_MODEL_ATTR: record.model.canonical_bytes().decode("utf-8"),
                SLOT_INDEX_MODEL_IDENTITY_HASH_ATTR: record.identity_hash,
            }
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
        root = self._open_zarr_root_for_mirror(product)
        root.attrs.update(
            {
                RESOLVED_INDEX_ATTR: canonical_index_bytes(record.index).decode("utf-8"),
                RESOLVED_INDEX_IDENTITY_HASH_ATTR: record.identity_hash,
            }
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
                root = zarr.open_group(**handle.zarr_kwargs(), mode="r", zarr_format=3)
            else:
                local = local_path_from_target(product_root)
                store = LocalStore(str(local))
                root = zarr.open_group(store=store, mode="r", zarr_format=3)
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
        f"Legacy index record detected at {legacy_path}. "
        f"Run `firecube zarr index rebuild --target {product_uri} --plugin {plugin_name} "
        f"--product-name {product}` to migrate."
    )
