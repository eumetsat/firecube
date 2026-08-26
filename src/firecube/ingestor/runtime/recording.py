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
from firecube.core.errors import ManifestError
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

        storage_bytes: int = 0
        storage_summary = result.metrics.storage
        if storage_summary is not None:
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

        if record_spans and coverage:
            base_meta = dict(meta)
            base_meta["run_id"] = run_id
            for cov in coverage:
                span_meta = dict(base_meta)
                if cov.time_min:
                    span_meta["time_min"] = cov.time_min
                if cov.time_max:
                    span_meta["time_max"] = cov.time_max
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

        if ctx.force_reingest and prior_spans:
            self._chunk_manager.record_replacement_committed(  # pyright: ignore[reportAttributeAccessIssue]
                product=product,
                run_id=run_id,
                replacing_run_id=run_id,
                replaced_span_keys=[span.key for span in prior_spans],
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

    def record_batch_failure(
        self,
        ctx: PluginContext,
        batch: PipelineBatch,
        error: str | None,
        slice_meta: dict[str, Any],
        run_id: str,
        product: str,
    ) -> None:
        """Record one failed batch as failed span records."""
        _ = ctx
        self._log.error("Batch %s failed: %s", batch.batch_id, error)

        base_meta = dict(slice_meta)
        base_meta["run_id"] = run_id
        for group in batch.groups or ["unknown"]:
            self._chunk_manager.record_span(
                product=product,
                run_id=run_id,
                batch_id=batch.batch_id,
                group=group,
                status="failed",
                reason=str(error or "Unknown error"),
                meta=base_meta,
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
    """Return current active spans for the same slice, excluding this run."""
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
        for span in chunk_manager.list_chunks(**query_kwargs)
        if str((span.meta or {}).get("run_id", "") or "") != run_id
    ]


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


def _coverage_from_nested_mapping(metrics: Any) -> Any | None:
    """Locate span coverage in a metrics mapping.

    Checks the batch-level top-level ``coverage`` key first, then the run-level
    ``zarr.coverage`` / ``pipeline.coverage`` nested locations that
    `merge_batch_metrics` and the pipeline summary populate. Works for
    both plain dicts and the mapping-compatible `ResultMetrics`.
    """
    coverage = metrics.get("coverage")
    if coverage:
        return coverage
    for key in ("zarr", "pipeline"):
        candidate = metrics.get(key)
        if isinstance(candidate, dict):
            nested = candidate.get("coverage")
            if nested:
                return nested
    return None


def _span_coverage_from_metrics(
    metrics: ResultMetrics | dict[str, Any] | None,
    *,
    logger: logging.Logger | None = None,
    context: str = "batch",
) -> list[SpanCoverage] | None:
    """Extract SpanCoverage objects from typed result metrics."""
    coverage: Any | None = None
    if metrics is None:
        coverage = None
    elif isinstance(metrics, ResultMetrics):
        coverage = metrics.pipeline.coverage if metrics.pipeline else None
        if not coverage:
            # Run-level aggregation (merge_batch_metrics) stores coverage under
            # ``metrics["zarr"]["coverage"]``; the typed ``pipeline.coverage``
            # only carries per-batch coverage and is empty for run-level metrics
            # (the pipeline field holds the run summary, which has no coverage).
            # Fall back to the nested location so run registration sees it.
            coverage = _coverage_from_nested_mapping(metrics)
    elif isinstance(metrics, dict):
        coverage = _coverage_from_nested_mapping(metrics)

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
