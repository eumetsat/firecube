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

"""Canonical metric schema and emission service for firecube.

Sole owner of ``RUN_SUMMARY_SCHEMA``. Imported by runtime aggregation and
domain collectors.

Internal metric emission facade. NOT part of ``firecube.core.api``. External
callers should use ``ctx.telemetry`` from within a plugin instead
(per DESIGN.md observability rules).
"""

from __future__ import annotations

import contextlib
import logging
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Literal

from firecube.core.observability.telemetry.sinks import IngestionTelemetry

if TYPE_CHECKING:
    from firecube.core.controlplane import ChunkManager
    from firecube.core.controlplane.types import IndexEnsuredOutcome, ResolvedIndexRecord

# Counter/gauge names (single source of truth).
METRIC_PIPELINE_BATCHES = "firecube_pipeline_batches_total"
METRIC_PIPELINE_BATCHES_FAILED = "firecube_pipeline_batches_failed_total"
METRIC_PIPELINE_HOOK_FAILURES = "firecube_pipeline_hook_failures_total"
METRIC_FILES_PROCESSED = "firecube_files_processed_total"
METRIC_BYTES_INGESTED = "firecube_bytes_ingested_total"
METRIC_ROWS_PROCESSED = "firecube_rows_processed_total"
METRIC_RUN_DURATION = "firecube_run_duration_seconds"
METRIC_PIPELINE_DURATION = "firecube_pipeline_duration_seconds"
METRIC_PIPELINE_BATCH_DURATION = "firecube_pipeline_batch_duration_seconds"
METRIC_PIPELINE_BATCH_CREATION_DURATION = "firecube_pipeline_batch_creation_duration_seconds"
METRIC_PIPELINE_UPLOAD_DURATION = "firecube_pipeline_upload_duration_seconds"
METRIC_PIPELINE_CPU_DURATION = "firecube_pipeline_cpu_duration_seconds"
METRIC_PIPELINE_NON_CPU_WAIT = "firecube_pipeline_non_cpu_wait_seconds"
METRIC_PIPELINE_CPU_UTILIZATION = "firecube_pipeline_cpu_utilization_estimate"
METRIC_PIPELINE_WORKERS = "firecube_pipeline_workers"
METRIC_PIPELINE_BATCH_SIZE = "firecube_pipeline_batch_size"
METRIC_STORAGE_CLIENT_REQUESTS = "firecube_storage_client_requests_total"
METRIC_STORAGE_CLIENT_ERRORS = "firecube_storage_client_errors_total"
METRIC_STORAGE_CLIENT_RETRYABLE_ERRORS = "firecube_storage_client_retryable_errors_total"
METRIC_STORAGE_CLIENT_LATENCY = "firecube_storage_client_latency_seconds"
METRIC_STORAGE_CLIENT_BYTES_READ = "firecube_storage_client_bytes_read_total"
METRIC_STORAGE_CLIENT_BYTES_WRITTEN = "firecube_storage_client_bytes_written_total"
METRIC_WAL_CORRUPTION = "firecube_control_plane_corruption_total"
METRIC_WAL_TORN_TAIL_RECOVERY = "firecube_control_plane_torn_tail_recovery_total"
METRIC_WAL_SNAPSHOT_REBUILD_DURATION = "firecube_control_plane_snapshot_rebuild_duration_seconds"
METRIC_WAL_SNAPSHOT_REBUILD_COUNT = "firecube_control_plane_snapshot_rebuild_total"
METRIC_RESUME_GUARD_ENFORCE_DURATION = "firecube_resume_guard_enforce_duration_seconds"
METRIC_RESUME_GUARD_RUNS_ENUMERATED = "firecube_resume_guard_runs_enumerated_total"
METRIC_RESUME_GUARD_SPANS_SCANNED = "firecube_resume_guard_spans_scanned_total"

# Summary-key constants for domain collectors (imported by instrumentation.py)
FS_SUMMARY_KEY_REQUESTS = "storage_client_requests"
FS_SUMMARY_KEY_ERRORS = "storage_client_errors"
FS_SUMMARY_KEY_RETRYABLE_ERRORS = "storage_client_retryable_errors"
FS_SUMMARY_KEY_LATENCY = "storage_client_latency_s_total"
FS_SUMMARY_KEY_BYTES_READ = "storage_client_bytes_read"
FS_SUMMARY_KEY_BYTES_WRITTEN = "storage_client_bytes_written"
WAL_SUMMARY_KEY_CORRUPTION = "wal_corruption_count"
WAL_SUMMARY_KEY_TORN_TAIL_RECOVERY = "wal_torn_tail_recovery_count"
WAL_SUMMARY_KEY_SNAPSHOT_REBUILD_DURATION = "wal_snapshot_rebuild_duration_s"
WAL_SUMMARY_KEY_SNAPSHOT_REBUILD_COUNT = "wal_snapshot_rebuild_count"
RESUME_GUARD_SUMMARY_KEY_ENFORCE_DURATION = "resume_guard_enforce_duration_s"
RESUME_GUARD_SUMMARY_KEY_RUNS_ENUMERATED = "resume_guard_runs_enumerated"
RESUME_GUARD_SUMMARY_KEY_SPANS_SCANNED = "resume_guard_spans_scanned"

METRIC_INDEX_ENSURED = "firecube_index_ensured"
INDEX_ENSURED_EVENT = "index_ensured"

MetricKind = Literal["counter", "gauge"]


@dataclass(frozen=True, slots=True)
class RunMetricSpec:
    """Mapping entry for one summary key and its emitted metric contract."""

    metric_name: str
    kind: MetricKind


@dataclass(slots=True)
class ResumeGuardMetrics:
    """Process-local resume-guard metrics collected during enforcement."""

    enforce_duration_s: float = 0.0
    runs_enumerated: int = 0
    spans_scanned: int = 0

    def as_summary(self) -> dict[str, int | float]:
        return {
            RESUME_GUARD_SUMMARY_KEY_ENFORCE_DURATION: float(self.enforce_duration_s),
            RESUME_GUARD_SUMMARY_KEY_RUNS_ENUMERATED: int(self.runs_enumerated),
            RESUME_GUARD_SUMMARY_KEY_SPANS_SCANNED: int(self.spans_scanned),
        }


@contextlib.contextmanager
def resume_guard_span():
    """Collect resume-guard metrics for the current enforcement span."""
    metrics = ResumeGuardMetrics()
    start = time.monotonic()
    try:
        yield metrics
    finally:
        metrics.enforce_duration_s = time.monotonic() - start


# One schema for both JSON summary and metric emission.
RUN_SUMMARY_SCHEMA: dict[str, RunMetricSpec] = {
    "workers": RunMetricSpec(METRIC_PIPELINE_WORKERS, "gauge"),
    "batch_size": RunMetricSpec(METRIC_PIPELINE_BATCH_SIZE, "gauge"),
    "batches_total": RunMetricSpec(METRIC_PIPELINE_BATCHES, "counter"),
    "batches_failed": RunMetricSpec(METRIC_PIPELINE_BATCHES_FAILED, "counter"),
    "hook_failures": RunMetricSpec(METRIC_PIPELINE_HOOK_FAILURES, "counter"),
    "files_processed": RunMetricSpec(METRIC_FILES_PROCESSED, "counter"),
    "bytes_ingested": RunMetricSpec(METRIC_BYTES_INGESTED, "counter"),
    "rows_processed": RunMetricSpec(METRIC_ROWS_PROCESSED, "counter"),
    "duration_total_s": RunMetricSpec(METRIC_RUN_DURATION, "gauge"),
    "duration_pipeline_s": RunMetricSpec(METRIC_PIPELINE_DURATION, "gauge"),
    "duration_processing_s": RunMetricSpec(METRIC_PIPELINE_BATCH_DURATION, "gauge"),
    "duration_batch_creation_s": RunMetricSpec(METRIC_PIPELINE_BATCH_CREATION_DURATION, "gauge"),
    "duration_upload_s": RunMetricSpec(METRIC_PIPELINE_UPLOAD_DURATION, "gauge"),
    "duration_cpu_s": RunMetricSpec(METRIC_PIPELINE_CPU_DURATION, "gauge"),
    "non_cpu_wait_s": RunMetricSpec(METRIC_PIPELINE_NON_CPU_WAIT, "gauge"),
    "cpu_utilization_estimate": RunMetricSpec(METRIC_PIPELINE_CPU_UTILIZATION, "gauge"),
    "storage_client_requests": RunMetricSpec(METRIC_STORAGE_CLIENT_REQUESTS, "counter"),
    "storage_client_errors": RunMetricSpec(METRIC_STORAGE_CLIENT_ERRORS, "counter"),
    "storage_client_retryable_errors": RunMetricSpec(
        METRIC_STORAGE_CLIENT_RETRYABLE_ERRORS, "counter"
    ),
    "storage_client_latency_s_total": RunMetricSpec(METRIC_STORAGE_CLIENT_LATENCY, "gauge"),
    "storage_client_bytes_read": RunMetricSpec(METRIC_STORAGE_CLIENT_BYTES_READ, "counter"),
    "storage_client_bytes_written": RunMetricSpec(METRIC_STORAGE_CLIENT_BYTES_WRITTEN, "counter"),
    "wal_corruption_count": RunMetricSpec(METRIC_WAL_CORRUPTION, "counter"),
    "wal_torn_tail_recovery_count": RunMetricSpec(METRIC_WAL_TORN_TAIL_RECOVERY, "counter"),
    "wal_snapshot_rebuild_duration_s": RunMetricSpec(METRIC_WAL_SNAPSHOT_REBUILD_DURATION, "gauge"),
    "wal_snapshot_rebuild_count": RunMetricSpec(METRIC_WAL_SNAPSHOT_REBUILD_COUNT, "counter"),
    "resume_guard_enforce_duration_s": RunMetricSpec(METRIC_RESUME_GUARD_ENFORCE_DURATION, "gauge"),
    "resume_guard_runs_enumerated": RunMetricSpec(METRIC_RESUME_GUARD_RUNS_ENUMERATED, "counter"),
    "resume_guard_spans_scanned": RunMetricSpec(METRIC_RESUME_GUARD_SPANS_SCANNED, "counter"),
}

_INTEGER_SUMMARY_KEYS = {
    "workers",
    "batch_size",
    "batches_total",
    "batches_failed",
    "hook_failures",
    "files_processed",
    "bytes_ingested",
    "rows_processed",
    "storage_client_requests",
    "storage_client_errors",
    "storage_client_retryable_errors",
    "storage_client_bytes_read",
    "storage_client_bytes_written",
    "wal_corruption_count",
    "wal_torn_tail_recovery_count",
    "wal_snapshot_rebuild_count",
    "resume_guard_runs_enumerated",
    "resume_guard_spans_scanned",
}


def _to_number(value: Any, *, as_int: bool) -> int | float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        numeric = 0.0
    if as_int:
        return max(int(numeric), 0)
    return max(float(numeric), 0.0)


def normalize_run_summary(summary: Mapping[str, Any] | None) -> dict[str, int | float]:
    """Normalize a summary dict against RUN_SUMMARY_SCHEMA."""
    source = summary or {}
    normalized: dict[str, int | float] = {}
    for key in RUN_SUMMARY_SCHEMA:
        normalized[key] = _to_number(source.get(key, 0), as_int=key in _INTEGER_SUMMARY_KEYS)

    normalized["cpu_utilization_estimate"] = max(
        0.0,
        min(float(normalized["cpu_utilization_estimate"]), 1.0),
    )
    return normalized


class TelemetryService:
    """Service for emitting ingestion telemetry with strict schemas."""

    def __init__(self, telemetry: IngestionTelemetry | None, plugin_name: str):
        self._telemetry = telemetry
        self._plugin_name = plugin_name
        self._log = logging.getLogger("firecube.ingestor.telemetry")

    def start(self, run_id: str) -> None:
        """Initialize telemetry for a run."""
        if not self._telemetry:
            return
        # No eager 0.0 emission; Pushgateway only needs the final snapshot.
        _ = run_id

    def emit_run_metrics(self, summary: dict[str, Any]) -> None:
        """Emit run-level metrics using the canonical summary schema."""
        if not self._telemetry:
            return

        normalized = normalize_run_summary(summary)
        unknown = sorted(set(summary.keys()) - set(RUN_SUMMARY_SCHEMA)) if summary else []
        if unknown:
            self._log.warning("Ignoring unknown pipeline summary keys: %s", ", ".join(unknown))

        meta = {"plugin": self._plugin_name}
        for key, spec in RUN_SUMMARY_SCHEMA.items():
            self._telemetry.emit(
                spec.metric_name,
                float(normalized[key]),
                kind=spec.kind,
                meta=meta,
            )

    def emit_index_ensured(
        self,
        *,
        product: str,
        identity_hash: str,
        axis_kinds: tuple[str, ...],
        groups: tuple[str, ...],
        outcome: Literal["created", "matched_existing", "conflict_refused", "rebuilt"],
    ) -> None:
        """Emit the ``index_ensured`` telemetry event.

        Fires once per `ChunkManager.ensure_resolved_index` call from
        DirectZarr startup or ``firecube zarr index rebuild``. The counter value is
        always 1; the interesting content is the ``meta`` payload. Multi-value
        ``axis_kinds`` and ``groups`` are joined with commas so Prometheus labels
        stay a single low-cardinality string per emission.
        """
        if not self._telemetry:
            return
        meta = {
            "plugin": self._plugin_name,
            "product": product,
            "outcome": outcome,
            "identity_hash": identity_hash,
            "axis_kinds": ",".join(sorted(axis_kinds)),
            "groups": ",".join(sorted(groups)),
        }
        self._telemetry.emit(METRIC_INDEX_ENSURED, 1.0, kind="counter", meta=meta)

    def flush(self) -> None:
        """Flush telemetry buffer."""
        if self._telemetry:
            self._telemetry.flush()


def emit_index_ensured_full(
    manager: ChunkManager,
    telemetry: TelemetryService | None,
    *,
    product: str,
    run_id: str,
    record: ResolvedIndexRecord,
    outcome: IndexEnsuredOutcome,
    logger: logging.Logger,
) -> None:
    """Record the resolved-index WAL audit event and matching telemetry counter."""
    # Lazy import: importing any controlplane submodule at module-load time
    # triggers controlplane/__init__.py, which closes an 8-hop cycle via
    # filesystem/instrumentation.py back into this module. Keeping this
    # import inside the function body defers execution until first call,
    # after both packages are fully initialised. Test guard:
    # tests/unit/test_metrics_no_import_cycle.py (dual-probe subprocess).
    from firecube.core.controlplane.types import IndexEnsuredEvent

    groups_by_name = record.index.get("groups", {}) or {}
    axis_kinds = tuple(sorted({str(g.get("kind", "")) for g in groups_by_name.values()}))
    group_names = tuple(sorted(groups_by_name.keys()))
    timestamp = datetime.now(UTC).isoformat().replace("+00:00", "Z")

    try:
        manager.record_index_ensured_event(
            IndexEnsuredEvent(
                run_id=run_id,
                product=product,
                identity_hash=record.identity_hash,
                axis_kinds=axis_kinds,
                groups=group_names,
                outcome=outcome,
                timestamp=timestamp,
            )
        )
    except Exception as exc:
        logger.error(
            "Failed to record index_ensured WAL audit event for product %s: %s",
            product,
            exc,
        )

    if telemetry is None:
        return
    try:
        telemetry.emit_index_ensured(
            product=product,
            identity_hash=record.identity_hash,
            axis_kinds=axis_kinds,
            groups=group_names,
            outcome=outcome,
        )
    except Exception as exc:
        logger.warning(
            "Failed to emit index_ensured telemetry for product %s: %s",
            product,
            exc,
        )
