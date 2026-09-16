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

"""Pipeline scheduler for ingestion plugins.

Two classes handle different levels of the execution stack:

- ``PipelineExecutor`` — top-level entry point.  Owns batching, engine_config
  extraction, and delegates to ``PipelineRunner`` for the actual concurrency loop.
  Also owns ``finalize()``, which assembles the final ``IngestResult``.
  After pipeline execution, ``complete_output()`` handles post-pipeline
  lifecycle: storage write, telemetry correction, and manifest construction.

- ``PipelineRunner`` — concurrency loop only.  Submits batches to a
  ``ThreadPoolExecutor``, collects results, drives the host lifecycle hooks
  (``on_batch_success`` / ``on_batch_failure``), and accumulates run-level
  timing counters.

The split exists so that ``finalize()`` can be called independently in tests.
"""

from __future__ import annotations

import concurrent.futures
import contextlib
import logging
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from firecube.core.controlplane.repo import describe_control_plane
from firecube.core.observability import attach_context, capture_context, detach_context
from firecube.ingestor.config.engine import EngineConfig
from firecube.ingestor.contracts.interfaces import PipelineHost
from firecube.ingestor.runtime._metrics_helpers import _extract_timestamps_skipped
from firecube.ingestor.runtime.aggregation import normalize_plugin_aggregate_metrics
from firecube.ingestor.runtime.index_binding import (
    filter_items_by_index,
)
from firecube.ingestor.runtime.parallel_evidence import log_filter_evidence
from firecube.ingestor.runtime.telemetry import derive_pipeline_summary, result_totals
from firecube.ingestor.types.context import (
    IngestResult,
    PipelineBatch,
    PipelineResult,
    PipelineRunState,
    PluginContext,
    RuntimeIngestContext,
)
from firecube.ingestor.types.result_metrics import OutputPaths, StorageMetrics

if TYPE_CHECKING:
    from firecube.core.storage.session import StorageSession
    from firecube.ingestor.runtime.base import BaseIngestor


class PipelineFailedBatchesError(RuntimeError):
    """Raised when finalization observes failed batch results."""


def _failed_batches_message(
    *, run_id: str, failed: list[PipelineResult], not_attempted: int
) -> str:
    """Compose a failure message without promising that retry bypasses validation."""
    err_summary = "; ".join((res.error or "unknown").rstrip(". ") for res in failed[:3])
    message = f"Pipeline run {run_id!r} had {len(failed)} failed batch(es): {err_summary}. "
    if not_attempted:
        message += (
            f"{not_attempted} later batch(es) were not attempted: this host stops at the "
            "first failed batch so no batch appends past a gap. "
        )
    return message + (
        "Run recorded as status=failed. Recovery: resolve the batch error above before retrying. "
        "For compatible Zarr inputs, --option resume_existing=true skips present timestamps "
        "and refills failed slots; --option force_reingest=true overwrites existing timestamps. "
        "Neither option can insert absent timestamps before the stored maximum or bypass schema validation."
    )


def _output_session(ctx: RuntimeIngestContext) -> StorageSession | None:
    storage = ctx.storage
    return storage.output if storage is not None else None


def _output_product_name(ctx: RuntimeIngestContext, default: str = "") -> str:
    session = _output_session(ctx)
    if session is not None:
        return str(session.product.product_name)
    return default


def _storage_completer() -> Any:
    module = import_module("firecube.core.storage.completion")
    return cast(Any, module).StorageCompleter()


def _total_timestamps_skipped(state: PipelineRunState) -> int:
    return sum(
        _extract_timestamps_skipped(result.metrics) for result in state.results if result.success
    )


@dataclass(slots=True)
class _PipelineRunAccumulator:
    """Main-thread-owned mutable accumulator for run progress."""

    product: str
    pipeline_workers: int
    batch_size: int
    batches: tuple[PipelineBatch, ...]
    ingestion_start_time: float
    batch_creation_duration: float
    processing_start_time: float
    results: list[PipelineResult] = field(default_factory=list)
    not_attempted: list[PipelineBatch] = field(default_factory=list)
    total_rows_processed: int = 0
    hook_failures: int = 0
    cpu_time_total: float = 0.0
    io_time_total: float = 0.0

    def add_result(self, result: PipelineResult) -> None:
        """Record one completed result into cumulative run counters."""
        self.results.append(result)
        rows_processed, cpu_time_total, io_time_total = result_totals(result)
        self.total_rows_processed += rows_processed
        self.cpu_time_total += cpu_time_total
        self.io_time_total += io_time_total

    def mark_not_attempted(self, batch: PipelineBatch) -> None:
        """Record a batch the run halted before attempting."""
        self.not_attempted.append(batch)

    def add_hook_failure(self) -> None:
        """Record a non-fatal lifecycle hook failure."""
        self.hook_failures += 1

    def snapshot(
        self,
        *,
        processing_duration: float = 0.0,
        total_ingestion_duration: float = 0.0,
        cpu_time_total: float | None = None,
        io_time_total: float | None = None,
    ) -> PipelineRunState:
        """Build an immutable PipelineRunState snapshot from current totals.

        ``cpu_time_total``/``io_time_total`` override the per-batch sums when
        provided. The final run snapshot passes a single process-wide CPU
        measurement (see `PipelineRunner.run_state`); interim snapshots
        used by lifecycle hooks fall back to the per-batch accumulation.
        """
        return PipelineRunState(
            product=self.product,
            pipeline_workers=self.pipeline_workers,
            batch_size=self.batch_size,
            batches=self.batches,
            ingestion_start_time=self.ingestion_start_time,
            batch_creation_duration=self.batch_creation_duration,
            processing_start_time=self.processing_start_time,
            processing_duration=processing_duration,
            total_ingestion_duration=total_ingestion_duration,
            results=tuple(self.results),
            total_rows_processed=self.total_rows_processed,
            hook_failures=self.hook_failures,
            cpu_time_total=self.cpu_time_total if cpu_time_total is None else cpu_time_total,
            io_time_total=self.io_time_total if io_time_total is None else io_time_total,
            batches_not_attempted=tuple(self.not_attempted),
        )


def _process_batch_timed(
    host: PipelineHost,
    batch: PipelineBatch,
    ctx: PluginContext,
    pre_batch_hook: Callable[[], None] | None = None,
) -> PipelineResult:
    """Run one batch and attach wall/cpu/io timings to the PipelineResult.

    Per-batch ``cpu_time_s`` uses ``time.thread_time()`` (this thread only) and
    is a diagnostic lower bound — it misses CPU spent in shared dask/C-extension
    worker threads. The authoritative run-level CPU total is measured separately
    over the whole processing window in `PipelineRunner.run_state`.
    """
    start_wall = time.perf_counter()
    start_cpu = time.thread_time()
    try:
        telemetry = getattr(ctx, "telemetry", None)
        span_ctx = (
            telemetry.span("firecube.batch", {"firecube.batch_id": str(batch.batch_id)})
            if telemetry is not None
            else contextlib.nullcontext()
        )
        with span_ctx:
            if pre_batch_hook is not None:
                pre_batch_hook()
            result = host._process_batch(batch, ctx)
    except Exception as exc:
        result = PipelineResult(
            batch=batch,
            outputs=OutputPaths(primary=Path("")),
            success=False,
            error=str(exc),
        )

    duration_s = max(time.perf_counter() - start_wall, 0.0)
    cpu_time_s = max(time.thread_time() - start_cpu, 0.0)
    io_time_s = max(duration_s - cpu_time_s, 0.0)

    result.duration_s = duration_s
    result.cpu_time_s = cpu_time_s
    result.io_time_s = io_time_s
    return result


def _log_halt(
    ingestor: PipelineHost, product: str, failed_batch: PipelineBatch, not_attempted: int
) -> None:
    ingestor._log.warning(
        "Pipeline halted product=%s after failed batch %s: %d pending batch(es) not attempted "
        "(host stops at the first failed batch so no batch lands past a gap).",
        product,
        failed_batch.batch_id,
        not_attempted,
    )


def run_sequential(
    *,
    ctx: RuntimeIngestContext,
    host: PipelineHost,
    product: str,
    batch_size: int,
    engine_config: EngineConfig | None = None,
    log: logging.Logger | None = None,
) -> PipelineRunState:
    """Execute batches sequentially and return the populated PipelineRunState."""
    ingestion_start_time = time.time()

    if engine_config is None:
        engine_config = EngineConfig()
    if log is None:
        log = logging.getLogger(__name__)

    start_batch_time = time.time()
    batches = _create_batches_with_parallel_filter(
        host=host,
        ctx=ctx,
        batch_size=batch_size,
        engine_config=engine_config,
        log=log,
    )
    batch_creation_duration = time.time() - start_batch_time

    runner = PipelineRunner()
    return runner.run_state(
        ingestor=host,
        ctx=ctx,
        product=product,
        pipeline_workers=1,
        batch_size=batch_size,
        batches=batches,
        batch_creation_duration=batch_creation_duration,
        ingestion_start_time=ingestion_start_time,
        execution_mode="sequential",
        emit_progress_logs=False,
    )


class PipelineRunner:
    """Concurrent execution loop for a pre-built batch list.

    Responsibilities:
    - Submit each batch to a ``ThreadPoolExecutor`` via
      ``_run_batch_with_context``, which attaches the parent OTel trace context
      so that worker spans are correctly nested under the main trace.
    - Collect futures as they complete (``as_completed``), so faster batches
      don't block behind slower ones.
    - Drive host lifecycle hooks (``on_pipeline_start``, ``on_batch_success``,
      ``on_batch_failure``) on the **main thread**, never in workers.
    - Aggregate run-level CPU/io/row counters on the main thread from each
      completed ``PipelineResult``.
    - ``on_batch_success`` failures are treated as bookkeeping errors:
      logged and counted, while preserving successful ingest results.
    - Emit coarse progress logs unless the caller passes ``no_progress=true``.
    - Honour ``host.stop_on_batch_failure``: after the first failed batch the
      remaining batches are not attempted (pending futures are cancelled,
      running ones return ``attempted=False`` from the host's closed gate)
      and are reported in ``PipelineRunState.batches_not_attempted``.
    """

    @staticmethod
    def _handle_completed_batch(
        *,
        ingestor: PipelineHost,
        ctx: PluginContext,
        accumulator: _PipelineRunAccumulator,
        batch: PipelineBatch,
        result: PipelineResult,
    ) -> bool:
        """Record one result, drive the batch hooks, and report whether to halt.

        A result with ``attempted=False`` (the host refused the batch after an
        earlier failure) is recorded as not attempted and reaches neither
        hook. Returns ``True`` when the run must stop attempting further
        batches: the batch did not succeed and the host is fail-stop.
        """
        if ctx.telemetry:
            ctx.telemetry.collect_memory_stats()

        if not result.attempted:
            accumulator.mark_not_attempted(batch)
            return True

        accumulator.add_result(result)
        state = accumulator.snapshot()

        if result.success:
            try:
                ingestor.on_batch_success(ctx=ctx, state=state, batch=batch, result=result)
            except Exception as exc:
                ingestor._log.error(
                    "on_batch_success hook failed for %s (batch kept as success): %s",
                    batch.batch_id,
                    exc,
                )
                accumulator.add_hook_failure()
            return False

        try:
            ingestor.on_batch_failure(ctx=ctx, state=state, batch=batch, result=result)
        except Exception as exc:
            ingestor._log.error("on_batch_failure hook failed for %s: %s", batch.batch_id, exc)
        return bool(getattr(ingestor, "stop_on_batch_failure", False))

    def run_state(
        self,
        *,
        ingestor: PipelineHost,
        ctx: RuntimeIngestContext,
        product: str,
        pipeline_workers: int,
        batch_size: int,
        batches: Iterable[PipelineBatch],
        batch_creation_duration: float,
        ingestion_start_time: float,
        execution_mode: str = "parallel",
        emit_progress_logs: bool = True,
    ) -> PipelineRunState:
        """Execute batches and return populated run state without finalizing."""
        batches_list = list(batches)
        plugin_ctx = PluginContext(ctx)
        processing_start_time = time.time()
        # Process-wide CPU clock (all threads, user+sys) captured at the same
        # instant as the wall clock. Per-batch ``time.thread_time()`` only sees
        # the orchestrating thread and misses CPU burned in dask's thread pool
        # and HDF5/netCDF C-extension threads, undercounting real CPU several-
        # fold. Measuring the whole processing window once is accurate for both
        # sequential and parallel modes and cannot double-count concurrent
        # batches the way summed per-batch deltas would.
        processing_start_cpu = time.process_time()
        accumulator = _PipelineRunAccumulator(
            product=product,
            pipeline_workers=pipeline_workers,
            batch_size=batch_size,
            batches=tuple(batches_list),
            ingestion_start_time=ingestion_start_time,
            batch_creation_duration=batch_creation_duration,
            processing_start_time=processing_start_time,
        )

        ingestor.on_pipeline_start(ctx=plugin_ctx, state=accumulator.snapshot())

        force_no_progress = bool(ctx.option("no_progress", False))
        should_log_progress = emit_progress_logs and not force_no_progress
        progress_log_step = max(len(batches_list) // 10, 1) if batches_list else 1
        completed_batches = 0

        if execution_mode == "sequential":
            for position, batch in enumerate(batches_list):
                result = _process_batch_timed(
                    ingestor,
                    batch,
                    plugin_ctx,
                    _zarr_pre_batch_hook(ingestor, plugin_ctx),
                )
                halt = self._handle_completed_batch(
                    ingestor=ingestor,
                    ctx=plugin_ctx,
                    accumulator=accumulator,
                    batch=batch,
                    result=result,
                )
                completed_batches += 1
                if should_log_progress and (
                    completed_batches % progress_log_step == 0
                    or completed_batches == len(batches_list)
                ):
                    ingestor._log.info(
                        "Pipeline progress product=%s completed=%d/%d",
                        product,
                        completed_batches,
                        len(batches_list),
                    )
                if halt:
                    remaining = batches_list[position + 1 :]
                    for skipped in remaining:
                        accumulator.mark_not_attempted(skipped)
                    _log_halt(ingestor, product, batch, len(remaining))
                    break
        else:
            # Create immutable context for workers
            worker_ctx = PluginContext(ctx)

            # Capture OTel context once, then attach in worker threads to preserve trace correlation.
            parent_context = capture_context()

            with concurrent.futures.ThreadPoolExecutor(max_workers=pipeline_workers) as executor:
                future_to_batch = {
                    executor.submit(
                        self._run_batch_with_context,
                        ingestor,
                        b,
                        worker_ctx,
                        parent_context,
                        _zarr_pre_batch_hook(ingestor, worker_ctx),
                    ): b
                    for b in batches_list
                }

                halted = False
                for future in concurrent.futures.as_completed(future_to_batch):
                    if future.cancelled():
                        # Cancelled on halt below; already marked not attempted.
                        continue
                    batch = future_to_batch[future]
                    try:
                        result = future.result()
                    except Exception as exc:
                        ingestor._log.error("Batch %s processing failed: %s", batch.batch_id, exc)
                        result = PipelineResult(
                            batch=batch,
                            outputs=OutputPaths(primary=Path("")),
                            success=False,
                            error=str(exc),
                        )

                    halt = self._handle_completed_batch(
                        ingestor=ingestor,
                        ctx=plugin_ctx,
                        accumulator=accumulator,
                        batch=batch,
                        result=result,
                    )
                    if halt and not halted:
                        halted = True
                        # Pending futures never start; running ones finish on
                        # their own (or are refused by the host's closed gate
                        # and come back with attempted=False).
                        cancelled = 0
                        for pending_future, pending_batch in future_to_batch.items():
                            if pending_future.cancel():
                                accumulator.mark_not_attempted(pending_batch)
                                cancelled += 1
                        _log_halt(ingestor, product, batch, cancelled)
                    completed_batches += 1
                    if should_log_progress and (
                        completed_batches % progress_log_step == 0
                        or completed_batches == len(batches_list)
                    ):
                        ingestor._log.info(
                            "Pipeline progress product=%s completed=%d/%d",
                            product,
                            completed_batches,
                            len(batches_list),
                        )

        processing_duration = max(time.time() - accumulator.processing_start_time, 0.0)
        cpu_time_total = max(time.process_time() - processing_start_cpu, 0.0)
        # Residual non-CPU wall time. When CPU time meets or exceeds wall time
        # (CPU-bound, possibly multi-core), there is no net wait and this is 0.
        io_time_total = max(processing_duration - cpu_time_total, 0.0)
        total_ingestion_duration = max(time.time() - ingestion_start_time, 0.0)
        return accumulator.snapshot(
            processing_duration=processing_duration,
            total_ingestion_duration=total_ingestion_duration,
            cpu_time_total=cpu_time_total,
            io_time_total=io_time_total,
        )

    def run(
        self,
        *,
        ingestor: PipelineHost,
        ctx: RuntimeIngestContext,
        product: str,
        pipeline_workers: int,
        batch_size: int,
        batches: Iterable[PipelineBatch],
        batch_creation_duration: float,
        ingestion_start_time: float,
    ) -> IngestResult:
        state = self.run_state(
            ingestor=ingestor,
            ctx=ctx,
            product=product,
            pipeline_workers=pipeline_workers,
            batch_size=batch_size,
            batches=batches,
            batch_creation_duration=batch_creation_duration,
            ingestion_start_time=ingestion_start_time,
            execution_mode="parallel",
            emit_progress_logs=True,
        )
        return ingestor.finalize_pipeline(ctx=ctx, state=state)

    @staticmethod
    def _run_batch_with_context(
        ingestor: PipelineHost,
        batch: PipelineBatch,
        worker_ctx: PluginContext,
        parent_context,
        pre_batch_hook: Callable[[], None] | None = None,
    ) -> PipelineResult:
        # OTel context does not propagate automatically into new threads.
        # We capture it once on the main thread (``get_current()``) before
        # submitting futures, then ``attach()`` it here so that any spans
        # opened inside ``_process_batch`` are correctly parented to the
        # top-level trace rather than floating as disconnected root spans.
        token = None
        try:
            token = attach_context(parent_context)
            return _process_batch_timed(ingestor, batch, worker_ctx, pre_batch_hook)
        finally:
            if token is not None:
                with contextlib.suppress(Exception):
                    detach_context(token)


def _zarr_pre_batch_hook(
    ingestor: PipelineHost,
    ctx: PluginContext,
) -> Callable[[], None] | None:
    if str(ctx.output_format or "").lower() != "zarr":
        return None

    from firecube.ingestor.runtime.zarr.batch_runner import seed_staged_metadata_pre_batch

    coordinate_arrays: list[str] = [ingestor._resolve_time_dim_name()]

    return lambda: seed_staged_metadata_pre_batch(
        host=ingestor,
        ctx=ctx,
        logger=ingestor._log,
        coordinate_arrays=coordinate_arrays,
    )


def _create_batches_with_parallel_filter(
    *,
    host: PipelineHost,
    ctx: RuntimeIngestContext,
    batch_size: int,
    engine_config: EngineConfig,
    log: logging.Logger,
) -> list[PipelineBatch]:
    """Create batches, optionally pre-filtering source items for slot-range parallel mode.

    In single-pod mode (slot_start/slot_end not set), this is a pass-through
    to ``host._create_batches(ctx, batch_size)`` with no behavior change.
    """
    slot_start = engine_config.slot_start
    slot_end = engine_config.slot_end
    from firecube.ingestor.templates.direct_zarr import DirectZarrIngestor

    if slot_start is not None and slot_end is not None and isinstance(host, DirectZarrIngestor):
        from firecube.ingestor.errors import ConfigurationError

        plugin_ctx = PluginContext(ctx)
        base_host = cast("BaseIngestor", host)
        binding = getattr(host, "_index_binding", None)
        if binding is None:
            raise ConfigurationError(
                "--slot-start/--slot-end require index_spec(); the plugin returned None"
            )

        # The coord-chunk overlap guard is intentionally absent here.
        # Fresh observed shells carry
        # ``firecube_coord_managed`` at creation, so pods take the
        # verify-or-error write path uniformly:
        # - firecube_coord_managed: write_timestamp verifies the stored value;
        #   no cross-slot write, no shared coord-chunk race.
        # - firecube_preallocated: same verify-or-error path against the dense
        #   pre-materialized value.
        # - genuinely pre-marker legacy: only historical shells still take the
        #   per-slot create-or-grow element write path (each pod writes its
        #   own data-array chunk). Its parallel-pod fan-out race is
        #   characterized RED-by-design in
        #   tests/integration/test_multiprocess_race_managed_coord.py
        #   (mark=race, excluded from the default lane); the corresponding
        #   fresh-managed GREEN gate is in test_preallocate_parallel_zero_loss.py.
        # The guard belongs on the preallocate side (claim_coord_materialization_window).

        cached_items = base_host._discovered_source_files
        if cached_items is None:
            all_items = list(base_host.discover_source_files(plugin_ctx))
        else:
            all_items = list(cached_items)
        original_count = len(all_items)

        try:
            filtered = filter_items_by_index(
                all_items,
                binding.resolved,
                slot_start,
                slot_end,
                engine_config.slot_group,
                host.inspect_item,
                plugin_ctx,
            )
        except Exception as exc:
            raise ConfigurationError(f"filter_items_by_index raised an error: {exc}") from exc

        if not isinstance(filtered, (list, tuple)):
            raise TypeError(
                f"filter_items_by_index must return a Sequence, got {type(filtered).__name__}"
            )
        filtered_items = list(filtered)
        filtered_count = len(filtered_items)
        dropped_count = original_count - filtered_count

        log_filter_evidence(
            log,
            stage="pre_batch_filter",
            planned_range=(slot_start, slot_end),
            original_count=original_count,
            filtered_count=filtered_count,
            dropped_count=dropped_count,
        )

        if not filtered_items:
            log.info(
                "Pre-batch filter: no items in slot range [%s,%s). This pod has nothing to write.",
                slot_start,
                slot_end,
            )
            return []

        batches = list(
            base_host._batch_planner.create_batches(
                host,
                PluginContext(ctx),
                batch_size,
                items=filtered_items,
            )
        )
        verify = getattr(host, "_verify_existing_cube_batch_groups", None)
        if verify is not None:
            for batch in batches:
                verify(ctx, batch.groups)
        return batches

    return list(host._create_batches(ctx, batch_size))


class PipelineExecutor:
    """Service for executing ingestion pipelines."""

    def __init__(self):
        self._log = logging.getLogger("firecube.ingestor.engine")

    def run_pipeline(
        self,
        ctx: RuntimeIngestContext,
        host: PipelineHost,
    ) -> IngestResult:
        """Run the pipeline using the provided host."""
        # Enforce No Split Brain: Read engine keys from host.engine_config (initialized by BaseIngestor)
        engine_config = getattr(host, "engine_config", None)
        if engine_config is None:
            engine_config = EngineConfig()

        pipeline_workers = int(engine_config.pipeline_workers)
        batch_size = int(engine_config.pipeline_batch_size)

        product = _output_product_name(ctx, host.name)

        ingestion_start_time = time.time()
        start_batch = time.time()

        # Pre-batch filter for slot-range parallel mode (no-op in single-pod mode)
        batches = _create_batches_with_parallel_filter(
            host=host,
            ctx=ctx,
            batch_size=batch_size,
            engine_config=engine_config,
            log=self._log,
        )
        batch_creation_duration = time.time() - start_batch

        if not batches:
            return IngestResult(
                output_format=str(ctx.output_format or ""),
                metrics={},
                outputs=OutputPaths(primary=str(ctx.target)),
            )

        runner = PipelineRunner()
        return runner.run(
            ingestor=host,
            ctx=ctx,
            product=product,
            pipeline_workers=pipeline_workers,
            batch_size=batch_size,
            batches=batches,
            batch_creation_duration=batch_creation_duration,
            ingestion_start_time=ingestion_start_time,
        )

    def finalize(
        self, ctx: RuntimeIngestContext, state: PipelineRunState, host: PipelineHost
    ) -> IngestResult:
        """Consolidate batch results into a single IngestResult.

        Output path resolution priority (first match wins):
        1. Any successful batch whose ``output_path`` starts with a URI scheme
           (``s3://``, ``https://``, etc.) — indicates a direct-write to remote
           storage; use that as the canonical path.
        2. The first successful batch ``output_path`` — typically a staged temp
           directory for local/staged-write mode.
        3. ChunkManager's canonical product root — derived from the typed
           output product URI and product name; reliable fallback for cases
           where no batch wrote an output path (e.g. dry-run or empty pipeline).
        4. ``ctx.target`` — last resort if ChunkManager is not available.
        """
        from firecube.core.uris import is_remote_target

        self._log.info(
            "Finalizing pipeline: %d batches, %d results", len(state.batches), len(state.results)
        )
        telemetry = getattr(ctx, "telemetry", None)
        finalize_ctx = (
            telemetry.span("firecube.finalize")
            if telemetry is not None
            else contextlib.nullcontext()
        )
        with finalize_ctx:
            # Merge outputs
            merged_outputs = OutputPaths()
            for res in state.results:
                if res.success:
                    if res.outputs.primary is not None:
                        merged_outputs.primary = res.outputs.primary
                    if res.outputs.zarr is not None:
                        merged_outputs.zarr = res.outputs.zarr

            # Aggregate metrics (Hook on host)
            merged_metrics = normalize_plugin_aggregate_metrics(
                host._aggregate_metrics(ctx, state),
                logger=self._log,
                plugin_name=host.name,
            )
            pipeline_summary = derive_pipeline_summary(state, merged_metrics)
            pipeline_summary["timestamps_skipped"] = _total_timestamps_skipped(state)
            merged_metrics["pipeline"] = pipeline_summary

            failed = [res for res in state.results if res.attempted and not res.success]
            if failed:
                raise PipelineFailedBatchesError(
                    _failed_batches_message(
                        run_id=ctx.run_id or "unknown",
                        failed=failed,
                        not_attempted=len(state.batches_not_attempted),
                    )
                )

            # Prefer remote path from results if any
            output_path = None
            output_path_source = "unset"
            for res in state.results:
                if res.success and is_remote_target(str(res.outputs.primary)):
                    output_path = str(res.outputs.primary)
                    output_path_source = "remote_result"
                    break
            # Otherwise use the first successful batch output_path (staged temp) as source
            if not output_path:
                for res in state.results:
                    if res.success and res.outputs.primary:
                        output_path = str(res.outputs.primary)
                        output_path_source = "first_successful_result"
                        break
            # Finally fall back to ChunkManager's canonical product root (accessed via host if needed)
            if not output_path:
                cm = getattr(host, "_chunk_manager", None)
                if cm:
                    product = _output_product_name(ctx, host.name)
                    output_path = cm.get_product_root(product)
                    output_path_source = "chunk_manager_product_root"
                else:
                    output_path = str(ctx.target)  # Fallback
                    output_path_source = "context_target_fallback"
            self._log.debug(
                "Resolved final output path via %s: %s",
                output_path_source,
                output_path,
            )

            storage_metrics = merged_metrics.setdefault("storage", {})
            if not isinstance(storage_metrics, dict):
                self._log.warning(
                    "Plugin '%s' returned non-mapping storage metrics; replacing with engine-owned storage map.",
                    host.name,
                )
                storage_metrics = {}
                merged_metrics["storage"] = storage_metrics

            cm = getattr(host, "_chunk_manager", None)
            if cm is not None:
                try:
                    product = _output_product_name(ctx, host.name)
                    control_plane = describe_control_plane(product_uri=cm.get_product_root(product))
                    storage_metrics.update(
                        {
                            "control_root": control_plane["control_root"],
                            "latest_pointer": control_plane["latest_pointer"],
                        }
                    )
                except Exception:
                    self._log.debug(
                        "Skipping control-plane storage metrics for product '%s'",
                        _output_product_name(ctx, host.name),
                        exc_info=True,
                    )

            merged_outputs.primary = output_path or ""
            if str(ctx.output_format or "") == "zarr":
                merged_outputs.zarr = output_path or ""
            return IngestResult(
                output_format=str(ctx.output_format or ""),
                metrics=merged_metrics,
                outputs=merged_outputs,
                spans_recorded=True,
            )

    def complete_output(
        self,
        result: IngestResult,
        ctx: RuntimeIngestContext,
        host: PipelineHost,
    ) -> IngestResult:
        """Post-pipeline lifecycle: storage write, telemetry correction, manifest.

        Ordering preserved from the original CLI implementation:
        storage upload → telemetry metric correction → manifest construction.
        """
        if _output_session(ctx) is None:
            return result

        from firecube.ingestor.types.manifest import IngestManifest

        effective_write_mode = str(result.metrics.write_mode or ctx.option("write_mode") or "")
        plugin = host.name
        run_id = ctx.run_id or ""
        output_name = _output_product_name(ctx)

        stored = _storage_completer().complete_output(result, ctx)

        result.storage_result = stored
        result.write_mode_applied = effective_write_mode

        # Upload counters describe a staged copy into the final target. Direct
        # runs never stage, whatever the locality, so the counters are null
        # rather than a misleading zero (local) or a path_stats scan (S3).
        # The same triple feeds the typed ``metrics.storage`` block and the
        # top-level manifest keys so the two views cannot disagree; engine-
        # seeded keys such as ``control_root``/``latest_pointer`` survive via
        # the ``ResultMetrics._compat`` merge in ``to_dict``.
        no_staged_upload = effective_write_mode == "direct"
        files = None if no_staged_upload else stored.files_written
        bytes_written = None if no_staged_upload else stored.bytes_written
        duration_s = None if no_staged_upload else stored.duration_s
        result.metrics.storage = StorageMetrics(
            path=stored.path,
            files=files,
            bytes=bytes_written,
            duration_s=duration_s,
        )
        manifest = IngestManifest(
            plugin=plugin,
            output_format=result.output_format,
            stored_at=stored.path,
            files=files,
            bytes=bytes_written,
            duration_s=duration_s,
            metrics=result.metrics.to_dict(),
            run_id=run_id,
            product=output_name,
        )
        result.manifest = manifest.to_dict()
        return result
