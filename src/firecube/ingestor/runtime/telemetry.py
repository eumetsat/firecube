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

"""Telemetry service for ingestion pipelines."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from firecube.core.observability.metrics import _to_number, normalize_run_summary
from firecube.ingestor.types.context import PipelineResult, PipelineRunState
from firecube.ingestor.types.result_metrics import ResultMetrics


def _nested_get(mapping: Mapping[str, Any] | None, *path: str) -> Any:
    current: Any = mapping
    for key in path:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return current


def _first_present_int(*candidates: Any) -> int:
    for value in candidates:
        if value is None:
            continue
        try:
            return max(int(value), 0)
        except (TypeError, ValueError):
            continue
    return 0


def _first_present_float(*candidates: Any) -> float:
    for value in candidates:
        if value is None:
            continue
        try:
            return max(float(value), 0.0)
        except (TypeError, ValueError):
            continue
    return 0.0


def extract_rows_processed(metrics: ResultMetrics | None) -> int:
    """Best-effort row extraction from typed batch-level metrics."""
    if metrics is None:
        return 0

    pipeline = metrics.pipeline
    if pipeline is None:
        return 0

    return _first_present_int(pipeline.rows_processed, pipeline.rows_ingested)


def result_totals(result: PipelineResult) -> tuple[int, float, float]:
    """Return `(rows_processed, cpu_time_s, non_cpu_wait_s)` for one batch result."""
    cpu_time_total = _to_number(getattr(result, "cpu_time_s", 0.0), as_int=False)
    io_time_total = _to_number(getattr(result, "io_time_s", 0.0), as_int=False)
    rows_processed = extract_rows_processed(result.metrics) if result.success else 0
    return int(rows_processed), float(cpu_time_total), float(io_time_total)


def derive_pipeline_summary(
    state: PipelineRunState,
    merged_metrics: Mapping[str, Any],
) -> dict[str, Any]:
    """Build canonical pipeline summary from run state and merged plugin metrics."""
    existing_pipeline = merged_metrics.get("pipeline")
    pipeline_map = existing_pipeline if isinstance(existing_pipeline, Mapping) else {}

    successful_results = [result for result in state.results if result.success]
    files_default = sum(max(int(result.batch.files_count), 0) for result in successful_results)
    bytes_default = sum(max(int(result.batch.size_bytes), 0) for result in successful_results)
    rows_default = max(int(state.total_rows_processed), 0)

    files_processed = _first_present_int(
        pipeline_map.get("files_processed"),
        merged_metrics.get("files_processed"),
        files_default,
    )
    bytes_ingested = _first_present_int(
        pipeline_map.get("bytes_ingested"),
        _nested_get(merged_metrics, "storage", "bytes"),
        _nested_get(merged_metrics, "zarr", "size_b"),
        merged_metrics.get("bytes_ingested"),
        bytes_default,
    )
    rows_processed = _first_present_int(
        pipeline_map.get("rows_processed"),
        _nested_get(pipeline_map, "performance", "total_rows_processed"),
        merged_metrics.get("rows_processed"),
        merged_metrics.get("rows_ingested"),
        _nested_get(merged_metrics, "performance", "total_rows_processed"),
        rows_default,
    )
    return compute_run_summary(
        state,
        duration_upload_s=0.0,
        files_processed=files_processed,
        bytes_ingested=bytes_ingested,
        rows_processed=rows_processed,
    )


def compute_run_summary(
    state: PipelineRunState,
    *,
    duration_upload_s: float = 0.0,
    files_processed: int,
    bytes_ingested: int,
    rows_processed: int,
) -> dict[str, Any]:
    """Compute canonical run-level summary for JSON output and metric emission.

    ``duration_cpu_s`` is process-wide CPU time (all threads, user+sys) measured
    once over the whole processing window via ``time.process_time()`` in
    ``PipelineRunner.run_state`` — not summed from per-thread per-batch deltas,
    which undercount CPU spent in dask/C-extension worker threads.
    ``non_cpu_wait_s`` is the residual non-CPU wall time over that window:
    ``max(processing_wall_s - cpu_s, 0.0)`` (``0`` when CPU-bound).
    """
    workers = max(int(state.pipeline_workers or 0), 1)
    successful_batches = sum(1 for result in state.results if result.success)
    batches_total = len(state.batches)
    batches_failed = max(batches_total - successful_batches, 0)

    duration_pipeline_s = float(state.total_ingestion_duration or 0.0)
    duration_processing_s = float(state.processing_duration or 0.0)
    duration_batch_creation_s = float(state.batch_creation_duration or 0.0)
    duration_upload_s = float(duration_upload_s or 0.0)
    duration_total_s = duration_pipeline_s + duration_upload_s
    duration_cpu_s = float(state.cpu_time_total or 0.0)
    non_cpu_wait_s = float(state.io_time_total or 0.0)

    safe_processing = max(duration_processing_s, 1e-6)
    cpu_utilization_estimate = duration_cpu_s / (safe_processing * workers)

    raw = {
        "workers": workers,
        "batch_size": int(state.batch_size or 0),
        "batches_total": batches_total,
        "batches_failed": batches_failed,
        "hook_failures": int(state.hook_failures or 0),
        "files_processed": int(files_processed or 0),
        "bytes_ingested": int(bytes_ingested or 0),
        "rows_processed": int(rows_processed or 0),
        "duration_total_s": duration_total_s,
        "duration_pipeline_s": duration_pipeline_s,
        "duration_processing_s": duration_processing_s,
        "duration_batch_creation_s": duration_batch_creation_s,
        "duration_upload_s": duration_upload_s,
        "duration_cpu_s": duration_cpu_s,
        "non_cpu_wait_s": non_cpu_wait_s,
        "cpu_utilization_estimate": cpu_utilization_estimate,
    }
    return normalize_run_summary(raw)
