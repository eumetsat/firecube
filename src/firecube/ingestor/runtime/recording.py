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

"""Engine-owned boundary for recording ingestion lifecycle into ChunkManager state."""

from __future__ import annotations

import logging
import threading
from datetime import datetime
from typing import Any

from firecube.core.controlplane import ChunkManager, SpanCoverage
from firecube.core.controlplane.ranges import (
    intersect_index_ranges as _intersect_index_ranges,
)
from firecube.core.controlplane.ranges import merge_index_ranges as _merge_index_ranges
from firecube.core.controlplane.ranges import (
    normalize_index_ranges as _normalize_index_ranges,
)
from firecube.core.controlplane.ranges import (
    subtract_index_ranges as _subtract_index_ranges,
)
from firecube.core.errors import ManifestError
from firecube.ingestor.runtime._metrics_helpers import _extract_timestamps_skipped
from firecube.ingestor.types.context import (
    IngestResult,
    PipelineBatch,
    PipelineResult,
    PluginContext,
    RuntimeIngestContext,
)
from firecube.ingestor.types.result_metrics import ResultMetrics


class SpanRecorder:
    """Map engine lifecycle events into ChunkManager's WAL-backed control-plane records."""

    def __init__(self, chunk_manager: ChunkManager):
        self._chunk_manager = chunk_manager
        self._log = logging.getLogger("firecube.ingestor.recording")

    def register_run_started(
        self,
        *,
        run_id: str,
        product: str,
        output_path: str,
        output_format: str,
        slice_meta: dict[str, Any],
        slot_range: tuple[int, int] | None = None,
        slot_group: str | None = None,
    ) -> None:
        """Record the engine-owned non-terminal start event for one run."""
        if threading.current_thread() is not threading.main_thread():
            raise ManifestError("Run start registration must occur on the main thread.")
        meta = dict(slice_meta)
        meta["run_id"] = run_id
        self._chunk_manager.record_run_started(
            product=product,
            run_id=run_id,
            output_path=output_path,
            output_format=output_format,
            size=0,
            meta=meta,
            slot_range=slot_range,
            slot_group=slot_group,
        )

    def register_run(
        self,
        ctx: RuntimeIngestContext,
        result: IngestResult,
        run_id: str,
        product: str,
        slice_meta: dict[str, Any],
        record_spans: bool = True,
        slot_range: tuple[int, int] | None = None,
        slot_group: str | None = None,
    ) -> None:
        """Record the terminal run event and any final span coverage through ChunkManager."""
        _ = ctx
        if threading.current_thread() is not threading.main_thread():
            raise ManifestError("Run registration must occur on the main thread.")

        if result.registered:
            self._log.warning("Run %s is already registered, skipping.", run_id)
            return

        meta = dict(slice_meta)
        coverage = _span_coverage_from_metrics(
            result.metrics,
            logger=self._log,
            context="run registration",
        )
        time_min, time_max = _time_bounds_from_coverage(coverage)
        if time_min:
            meta["time_min"] = time_min
        if time_max:
            meta["time_max"] = time_max

        timestamps_skipped = _extract_timestamps_skipped(result.metrics)
        if timestamps_skipped > 0:
            meta["timestamps_skipped"] = timestamps_skipped

        storage_bytes: int = 0
        storage_summary = result.metrics.storage
        if storage_summary is not None and storage_summary.bytes is not None:
            storage_bytes = storage_summary.bytes

        self._chunk_manager.record_run_terminal(
            product=product,
            run_id=run_id,
            output_path=str(result.outputs.primary),
            output_format=result.output_format or "unknown",
            size=storage_bytes,
            meta=meta,
            status="complete",
            slot_range=slot_range,
            slot_group=slot_group,
        )
        post_terminal_events_recorded = False
        prior_spans: list[Any] = []
        replaced_span_keys: list[str] = []
        overwrites_by_coverage: dict[int, list[list[int]]] = {}

        if ctx.force_reingest:
            prior_spans = _list_prior_active_spans_for_replacement(
                self._chunk_manager,
                product=product,
                run_id=run_id,
                slice_meta=meta,
            )
            if prior_spans and not coverage:
                raise ManifestError(
                    f"force_reingest refusing to commit replacement for product={product!r}: "
                    f"prior_spans={len(prior_spans)} but new coverage is empty. "
                    "This would erase active coverage. "
                    f"Abandon the run first: firecube chunks runs abandon {run_id}"
                )
            replaced_span_keys, overwrites_by_coverage = _replacement_plan(
                prior_spans=prior_spans,
                coverage=coverage,
            )

        if record_spans and coverage:
            base_meta = dict(meta)
            base_meta["run_id"] = run_id
            for cov_index, cov in enumerate(coverage):
                span_meta = dict(base_meta)
                if cov.time_min:
                    span_meta["time_min"] = cov.time_min
                if cov.time_max:
                    span_meta["time_max"] = cov.time_max
                if overwrites := overwrites_by_coverage.get(cov_index):
                    span_meta["overwrites_index_ranges"] = overwrites
                self._chunk_manager.record_span(
                    product=product,
                    run_id=str(run_id),
                    batch_id="single",
                    group=cov.group,
                    status="active",
                    coverage=cov,
                    meta=span_meta,
                )
            post_terminal_events_recorded = True
        elif coverage and overwrites_by_coverage:
            post_terminal_events_recorded = (
                _record_overwrite_metadata_for_existing_spans(
                    self._chunk_manager,
                    product=product,
                    run_id=run_id,
                    coverage=coverage,
                    overwrites_by_coverage=overwrites_by_coverage,
                )
                or post_terminal_events_recorded
            )

        if ctx.force_reingest and replaced_span_keys:
            self._chunk_manager.record_replacement_committed(  # pyright: ignore[reportAttributeAccessIssue]
                product=product,
                run_id=run_id,
                replacing_run_id=run_id,
                replaced_span_keys=replaced_span_keys,
            )
            post_terminal_events_recorded = True

        if post_terminal_events_recorded:
            _rewrite_terminal_run_metadata(self._chunk_manager, product=product, run_id=run_id)

        result.registered = True

    def register_run_failure(
        self,
        *,
        run_id: str,
        product: str,
        output_path: str,
        output_format: str,
        slice_meta: dict[str, Any],
        error: str,
    ) -> None:
        """Record a terminal run failure event through ChunkManager."""
        if threading.current_thread() is not threading.main_thread():
            raise ManifestError("Run failure registration must occur on the main thread.")
        meta = dict(slice_meta)
        meta["run_id"] = run_id
        self._chunk_manager.record_run_failed(
            product=product,
            run_id=run_id,
            output_path=output_path,
            output_format=output_format,
            size=0,
            meta=meta,
            error=error,
        )

    def record_batch_success(
        self,
        ctx: PluginContext,
        batch: PipelineBatch,
        result: PipelineResult,
        slice_meta: dict[str, Any],
        run_id: str,
        product: str,
    ) -> None:
        """Record one successful batch as active or skipped span records."""
        _ = ctx
        base_meta = dict(slice_meta)
        base_meta["run_id"] = run_id

        coverage_list = _span_coverage_from_metrics(
            result.metrics,
            logger=self._log,
            context=f"batch {batch.batch_id}",
        )
        if not coverage_list:
            for group in batch.groups or ["unknown"]:
                self._chunk_manager.record_span(
                    product=product,
                    run_id=run_id,
                    batch_id=batch.batch_id,
                    group=group,
                    status="skipped",
                    reason="No coverage generated",
                    meta=base_meta,
                )
            return

        self._record_active_spans(
            coverage_list, batch=batch, base_meta=base_meta, run_id=run_id, product=product
        )

    def record_batch_failure(
        self,
        ctx: PluginContext,
        batch: PipelineBatch,
        result: PipelineResult,
        slice_meta: dict[str, Any],
        run_id: str,
        product: str,
    ) -> None:
        """Record one failed batch, one span per group the batch involved.

        A plain failure (no append outcome in ``result.metrics``) records a
        ``failed`` span for every group of the batch. A failure reported by
        the append path records what the store now holds: an ``active`` span
        for each group committed before the failure, a ``failed`` span with
        the touched index ranges and ``meta["repair"]`` for the failed group,
        and a ``failed`` span with a ``not_attempted`` reason for each group
        that was never written.

        Args:
            ctx: Plugin-facing run context (unused).
            batch: The batch that failed.
            result: The failed batch result; ``error`` carries the cause.
            slice_meta: Canonical slice metadata for the run.
            run_id: Identifier of the run.
            product: Product the spans belong to.
        """
        _ = ctx
        error = result.error
        self._log.error("Batch %s failed: %s", batch.batch_id, error)

        base_meta = dict(slice_meta)
        base_meta["run_id"] = run_id
        reason = str(error or "Unknown error")
        metrics = result.metrics
        failed_group = metrics.get("failed_group") if metrics is not None else None
        if not isinstance(failed_group, str):
            for group in batch.groups or ["unknown"]:
                self._chunk_manager.record_span(
                    product=product,
                    run_id=run_id,
                    batch_id=batch.batch_id,
                    group=group,
                    status="failed",
                    reason=reason,
                    meta=base_meta,
                )
            return

        context = f"failed batch {batch.batch_id}"
        committed = _span_coverage_from_metrics(metrics, logger=self._log, context=context)
        if committed:
            self._record_active_spans(
                committed, batch=batch, base_meta=base_meta, run_id=run_id, product=product
            )

        failed_meta = dict(base_meta)
        repair = metrics.get("repair")
        if isinstance(repair, dict):
            failed_meta["repair"] = dict(repair)
        failed_coverage = _span_coverage_from_metrics(
            metrics, key="failed_coverage", logger=self._log, context=context
        )
        if failed_coverage:
            for cov in failed_coverage:
                meta = dict(failed_meta)
                if cov.time_min:
                    meta["time_min"] = cov.time_min
                if cov.time_max:
                    meta["time_max"] = cov.time_max
                self._chunk_manager.record_span(
                    product=product,
                    run_id=run_id,
                    batch_id=batch.batch_id,
                    group=cov.group,
                    status="failed",
                    reason=reason,
                    coverage=cov,
                    meta=meta,
                )
        else:
            self._chunk_manager.record_span(
                product=product,
                run_id=run_id,
                batch_id=batch.batch_id,
                group=failed_group,
                status="failed",
                reason=reason,
                meta=failed_meta,
            )

        not_attempted = metrics.get("not_attempted_groups")
        for group in not_attempted if isinstance(not_attempted, list) else []:
            self._chunk_manager.record_span(
                product=product,
                run_id=run_id,
                batch_id=batch.batch_id,
                group=str(group),
                status="failed",
                reason=f"not_attempted; group {failed_group} failed: {reason}",
                meta=base_meta,
            )

    def _record_active_spans(
        self,
        coverage_list: list[SpanCoverage],
        *,
        batch: PipelineBatch,
        base_meta: dict[str, Any],
        run_id: str,
        product: str,
    ) -> None:
        """Record one ``active`` span per coverage entry of a batch."""
        for cov in coverage_list:
            meta = dict(base_meta)
            if cov.time_min:
                meta["time_min"] = cov.time_min
            if cov.time_max:
                meta["time_max"] = cov.time_max
            self._chunk_manager.record_span(
                product=product,
                run_id=run_id,
                batch_id=batch.batch_id,
                group=cov.group,
                status="active",
                coverage=cov,
                meta=meta,
            )


def _parse_iso8601(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return datetime.fromisoformat(value)
    except Exception:
        return None


def _time_bounds_from_coverage(
    coverage: list[SpanCoverage] | None,
) -> tuple[str | None, str | None]:
    if not coverage:
        return None, None

    min_dt: datetime | None = None
    max_dt: datetime | None = None
    for cov in coverage:
        cov_min = _parse_iso8601(cov.time_min)
        cov_max = _parse_iso8601(cov.time_max)
        if cov_min and (min_dt is None or cov_min < min_dt):
            min_dt = cov_min
        if cov_max and (max_dt is None or cov_max > max_dt):
            max_dt = cov_max
    return (min_dt.isoformat() if min_dt else None, max_dt.isoformat() if max_dt else None)


def _list_prior_active_spans_for_replacement(
    chunk_manager: ChunkManager,
    *,
    product: str,
    run_id: str,
    slice_meta: dict[str, Any],
) -> list[Any]:
    """Return current active spans for the same slice, excluding this run.

    Calls ``chunk_manager.repo.list_chunks(...)`` directly to bypass the
    facade's active-span dedupe. The replacement recorder MUST see EVERY
    prior active span for the slice — including duplicates that dedupe
    would otherwise hide — so that every prior span is marked replaced
    when this run commits.
    """
    query_meta = {
        key: value
        for key, value in slice_meta.items()
        if key not in {"run_id", "time_min", "time_max"}
    }
    time_min = slice_meta.get("time_min")
    time_max = slice_meta.get("time_max")

    query_kwargs: dict[str, Any] = {
        "product": product,
        "chunk_type": "span",
        "include_replaced": False,
    }
    if query_meta:
        query_kwargs["meta"] = query_meta
    if time_min and time_max:
        query_kwargs["time_overlaps"] = (str(time_min), str(time_max))

    return [
        span
        for span in chunk_manager.repo.list_chunks(**query_kwargs)
        if str((span.meta or {}).get("run_id", "") or "") != run_id
    ]


def _replacement_plan(
    *,
    prior_spans: list[Any],
    coverage: list[SpanCoverage] | None,
) -> tuple[list[str], dict[int, list[list[int]]]]:
    """Classify prior spans as fully replaced or partially overwritten.

    Full prior-span coverage keeps the historical replacement behaviour.  A
    partial overlap keeps the prior span active and records the overwritten
    sub-range on the new span that wrote those slots; projection readers subtract
    those ranges from the prior span's effective coverage.
    """
    if not coverage:
        return [], {}

    coverage_ranges_by_group: dict[str, list[list[int]]] = {}
    for cov in coverage:
        ranges = _normalize_index_ranges(cov.time_index_ranges)
        if ranges:
            coverage_ranges_by_group.setdefault(cov.group, []).extend(ranges)

    replaced_span_keys: list[str] = []
    overwrites_by_coverage: dict[int, list[list[int]]] = {}
    for prior_span in prior_spans:
        prior_ranges = _span_index_ranges(prior_span)
        prior_group = str((prior_span.meta or {}).get("group", "") or "")
        new_ranges = _merge_index_ranges(coverage_ranges_by_group.get(prior_group, []))

        if not prior_ranges or not new_ranges:
            replaced_span_keys.append(str(prior_span.key))
            continue

        remaining_ranges = _subtract_index_ranges(prior_ranges, new_ranges)
        if not remaining_ranges:
            replaced_span_keys.append(str(prior_span.key))
            continue

        for cov_index, cov in enumerate(coverage):
            if cov.group != prior_group:
                continue
            intersections = _intersect_index_ranges(
                prior_ranges,
                _normalize_index_ranges(cov.time_index_ranges),
            )
            if intersections:
                overwrites_by_coverage.setdefault(cov_index, []).extend(intersections)

    return replaced_span_keys, {
        cov_index: _merge_index_ranges(ranges)
        for cov_index, ranges in overwrites_by_coverage.items()
    }


def _record_overwrite_metadata_for_existing_spans(
    chunk_manager: ChunkManager,
    *,
    product: str,
    run_id: str,
    coverage: list[SpanCoverage],
    overwrites_by_coverage: dict[int, list[list[int]]],
) -> bool:
    """Append enriched span records for spans already recorded per batch."""
    # private-caller: reach into repo.list_chunks to see every active span
    # for this run (the manager.list_chunks dedupe would hide siblings we
    # must mark replaced).
    current_run_spans = [
        span
        for span in chunk_manager.repo.list_chunks(
            product=product,
            chunk_type="span",
            include_replaced=False,
            meta={"run_id": run_id},
        )
        if span.status == "active"
    ]
    used_keys: set[str] = set()
    recorded = False
    for cov_index, overwrites in overwrites_by_coverage.items():
        if cov_index >= len(coverage):
            continue
        cov = coverage[cov_index]
        span = _match_recorded_span(current_run_spans, cov, used_keys)
        if span is None:
            continue
        used_keys.add(str(span.key))
        span_meta = dict(span.meta or {})
        span_meta["overwrites_index_ranges"] = overwrites
        if cov.time_min:
            span_meta["time_min"] = cov.time_min
        if cov.time_max:
            span_meta["time_max"] = cov.time_max
        chunk_manager.record_span(
            product=product,
            run_id=run_id,
            batch_id=str(span_meta.get("batch_id", "single") or "single"),
            group=cov.group,
            status="active",
            coverage=cov,
            meta=span_meta,
        )
        recorded = True
    return recorded


def _match_recorded_span(
    spans: list[Any],
    coverage: SpanCoverage,
    used_keys: set[str],
) -> Any | None:
    coverage_ranges = _normalize_index_ranges(coverage.time_index_ranges)
    for span in spans:
        if str(span.key) in used_keys:
            continue
        if str((span.meta or {}).get("group", "") or "") != coverage.group:
            continue
        if _span_index_ranges(span) == coverage_ranges:
            return span
    return None


def _span_index_ranges(span: Any) -> list[list[int]]:
    record = span.record if isinstance(getattr(span, "record", None), dict) else {}
    payload = record.get("span") if isinstance(record, dict) else None
    if not isinstance(payload, dict):
        return []
    return _normalize_index_ranges(payload.get("time_index_ranges"))


def _rewrite_terminal_run_metadata(
    chunk_manager: ChunkManager,
    *,
    product: str,
    run_id: str,
) -> None:
    """Re-finalize run metadata after post-terminal WAL appends."""
    repo = chunk_manager.repo
    writer = repo._writer(product, run_id, resume_existing=True)
    writer.finalize(status="complete")
    repo._writers.pop((product, run_id), None)


def _coverage_from_nested_mapping(metrics: Any, *, key: str = "coverage") -> Any | None:
    """Locate span coverage in a metrics mapping.

    Checks the batch-level top-level ``key`` first, then the run-level
    ``zarr.<key>`` / ``pipeline.<key>`` nested locations that
    `merge_batch_metrics` and the pipeline summary populate. Works for
    both plain dicts and the mapping-compatible `ResultMetrics`.
    """
    coverage = metrics.get(key)
    if coverage:
        return coverage
    for nested_key in ("zarr", "pipeline"):
        candidate = metrics.get(nested_key)
        if isinstance(candidate, dict):
            nested = candidate.get(key)
            if nested:
                return nested
    return None


def _span_coverage_from_metrics(
    metrics: ResultMetrics | dict[str, Any] | None,
    *,
    key: str = "coverage",
    logger: logging.Logger | None = None,
    context: str = "batch",
) -> list[SpanCoverage] | None:
    """Extract SpanCoverage objects from typed result metrics.

    Args:
        metrics: Batch- or run-level metrics.
        key: Metrics key holding the coverage list; ``"coverage"`` for
            committed spans, ``"failed_coverage"`` for the slots a failed
            append batch touched.
        logger: Receives a debug line when no coverage is present.
        context: Names the caller in that debug line.

    Returns:
        The coverage entries as `SpanCoverage`, or ``None`` when absent.
    """
    coverage: Any | None = None
    if metrics is None:
        coverage = None
    elif isinstance(metrics, ResultMetrics):
        if key == "coverage":
            coverage = metrics.pipeline.coverage if metrics.pipeline else None
        if not coverage:
            # Run-level aggregation (merge_batch_metrics) stores coverage under
            # ``metrics["zarr"]["coverage"]``; the typed ``pipeline.coverage``
            # only carries per-batch coverage and is empty for run-level metrics
            # (the pipeline field holds the run summary, which has no coverage).
            # Fall back to the nested location so run registration sees it.
            coverage = _coverage_from_nested_mapping(metrics, key=key)
    elif isinstance(metrics, dict):
        coverage = _coverage_from_nested_mapping(metrics, key=key)

    if not coverage:
        if logger:
            logger.debug(
                "No span coverage present in metrics during %s.",
                context,
            )
        return None

    spans: list[SpanCoverage] = []
    for item in coverage:
        if isinstance(item, SpanCoverage):
            spans.append(item)
        elif isinstance(item, dict) and "group" in item:
            spans.append(
                SpanCoverage(
                    group=item["group"],
                    arrays=list(item.get("arrays", [])),
                    time_index_ranges=(
                        list(item["time_index_ranges"]) if "time_index_ranges" in item else None
                    ),
                    aligned=bool(item.get("aligned", True)),
                    state_array=item.get("state_array"),
                    state_deleted_value=int(item.get("state_deleted_value", 2)),
                    time_min=item.get("time_min"),
                    time_max=item.get("time_max"),
                    region_spec=item.get("region_spec"),
                    write_strategy=item.get("write_strategy"),
                    time_dim_name=item.get("time_dim_name"),
                    chunk_len_used=item.get("chunk_len_used"),
                )
            )
    if spans:
        return spans

    if logger:
        logger.debug(
            "Coverage payload during %s contained no usable span records.",
            context,
        )
    return None
