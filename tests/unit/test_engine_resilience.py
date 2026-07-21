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

import logging
import threading
import time
from dataclasses import FrozenInstanceError
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from firecube.core.controlplane.manager import ChunkManager
from firecube.ingestor.contracts.interfaces import PipelineHost
from firecube.ingestor.runtime.engine import (
    PipelineExecutor,
    PipelineResult,
    PipelineRunner,
    run_sequential,
)
from firecube.ingestor.types.context import (
    IngestContext,
    OutputPaths,
    PipelineBatch,
    PipelineRunState,
    RuntimeIngestContext,
)
from tests.helpers.storage import make_test_binding, make_test_context


def test_on_batch_success_failure_is_resilient():
    """Verify that a failure in on_batch_success logs error but keeps batch success."""
    host = MagicMock(spec=PipelineHost)
    host.on_batch_success.side_effect = Exception("Hook failed!")
    host._log = MagicMock(spec=logging.Logger)
    host._process_batch.return_value = PipelineResult(
        batch=MagicMock(), outputs=OutputPaths(primary="out"), success=True
    )

    ctx = MagicMock(spec=IngestContext)
    ctx.telemetry = None
    ctx.option.return_value = True  # no_progress
    batch = PipelineBatch(
        batch_id="b1",
        data_path=Path("."),
        items=[],
        size_bytes=0,
        files_count=0,
    )

    runner = PipelineRunner()
    runner.run(
        ingestor=host,
        ctx=ctx,
        product="p",
        pipeline_workers=1,
        batch_size=1,
        batches=[batch],
        batch_creation_duration=0,
        ingestion_start_time=0,
    )

    finalize_call = host.finalize_pipeline.call_args
    state = finalize_call[1]["state"]

    assert state.hook_failures == 1
    assert isinstance(state.results, tuple)
    assert isinstance(state.batches, tuple)
    assert len(state.results) == 1
    assert state.results[0].success is True
    with pytest.raises(FrozenInstanceError):
        state.hook_failures = 9
    host.on_batch_success.assert_called_once()
    host.on_batch_failure.assert_not_called()
    host._log.error.assert_called_once()
    assert "on_batch_success hook failed" in host._log.error.call_args[0][0]


def test_cpu_time_total_counts_non_orchestrating_thread_cpu():
    """Run-level CPU must include CPU burned off the orchestrating thread.

    The hot path fans CPU work out to dask's thread pool and HDF5/netCDF
    C-extension threads. The old per-batch ``time.thread_time()`` saw only the
    main thread (blocked on join here) and reported ~0, undercounting real CPU
    several-fold. A single process-wide ``time.process_time()`` over the whole
    processing window captures it. This test burns CPU in a background thread
    and asserts the run total reflects it — it fails on the thread_time impl.
    """
    batch = PipelineBatch(batch_id="b1", data_path=Path("."), items=[], size_bytes=0, files_count=0)

    def _burn_cpu_in_background(*_args, **_kwargs):
        def _spin() -> None:
            # Bound the loop by this thread's OWN CPU time, not wall time, so it
            # deterministically burns >=0.15 CPU-s regardless of scheduler
            # starvation (a wall-time bound could under-burn on a loaded box).
            start = time.thread_time()
            x = 0
            while time.thread_time() - start < 0.15:
                x += 1  # pure-Python CPU work on a non-orchestrating thread

        worker = threading.Thread(target=_spin)
        worker.start()
        worker.join()  # main thread releases the GIL and idles here
        return PipelineResult(batch=batch, outputs=OutputPaths(primary="out"), success=True)

    host = MagicMock(spec=PipelineHost)
    host._log = MagicMock(spec=logging.Logger)
    host._process_batch.side_effect = _burn_cpu_in_background

    ctx = MagicMock(spec=IngestContext)
    ctx.telemetry = None
    ctx.option.return_value = True  # no_progress

    runner = PipelineRunner()
    state = runner.run_state(
        ingestor=host,
        ctx=ctx,
        product="p",
        pipeline_workers=1,
        batch_size=1,
        batches=[batch],
        batch_creation_duration=0.0,
        ingestion_start_time=time.time(),
        execution_mode="sequential",
        emit_progress_logs=False,
    )

    # The background thread burned ~0.2 CPU-s; main-thread thread_time() would
    # report ~0. The threshold is well above thread_time noise, well below the
    # burned amount, so it is robust on fast and slow machines alike.
    assert state.cpu_time_total >= 0.05


def test_finalize_logs_output_path_resolution_source():
    executor = PipelineExecutor()
    executor._log = MagicMock(spec=logging.Logger)

    host = MagicMock(spec=PipelineHost)
    host._aggregate_metrics.return_value = {}
    host.name = "dummy"
    host._chunk_manager = None

    batch = PipelineBatch(batch_id="b1", data_path=Path("."), items=[], size_bytes=0, files_count=0)
    result = PipelineResult(
        batch=batch, outputs=OutputPaths(primary="s3://bucket/product"), success=True
    )
    state = PipelineRunState(
        product="dummy",
        pipeline_workers=1,
        batch_size=1,
        batches=(batch,),
        ingestion_start_time=0.0,
        batch_creation_duration=0.0,
        processing_start_time=0.0,
        results=(result,),
    )
    ctx = RuntimeIngestContext.from_ingest_context(
        IngestContext(source=".", target="fallback.zarr", output_format="zarr"),
        run_id="run-001",
        temp_root=Path("."),
        materializer=lambda p: Path(p),
    )

    final_result = executor.finalize(ctx, state, host)

    assert final_result.output_path == "s3://bucket/product"
    executor._log.debug.assert_any_call(
        "Resolved final output path via %s: %s",
        "remote_result",
        "s3://bucket/product",
    )


def test_run_sequential_is_resilient_to_on_batch_failure_errors():
    host = MagicMock(spec=PipelineHost)
    host._log = MagicMock(spec=logging.Logger)
    host._create_batches.return_value = [
        PipelineBatch(batch_id="b1", data_path=Path("."), items=[], size_bytes=0, files_count=0)
    ]
    host._process_batch.return_value = PipelineResult(
        batch=host._create_batches.return_value[0],
        outputs=OutputPaths(primary="out"),
        success=False,
        error="boom",
    )
    host.on_batch_failure.side_effect = Exception("failure hook broken")

    ctx = MagicMock(spec=IngestContext)
    ctx.telemetry = None
    ctx.option.return_value = True

    state = run_sequential(ctx=ctx, host=host, product="p", batch_size=1)

    assert len(state.results) == 1
    assert state.results[0].success is False
    host.on_batch_failure.assert_called_once()
    host._log.error.assert_called_once()
    assert "on_batch_failure hook failed" in host._log.error.call_args[0][0]


def test_finalize_ignores_plugin_reserved_pipeline_metrics_key():
    executor = PipelineExecutor()
    executor._log = MagicMock(spec=logging.Logger)

    host = MagicMock(spec=PipelineHost)
    host._aggregate_metrics.return_value = {"pipeline": {"workers": 999}, "custom": 1}
    host.name = "dummy"
    host._chunk_manager = None

    batch = PipelineBatch(batch_id="b1", data_path=Path("."), items=[], size_bytes=0, files_count=0)
    result = PipelineResult(
        batch=batch, outputs=OutputPaths(primary="s3://bucket/product"), success=True
    )
    state = PipelineRunState(
        product="dummy",
        pipeline_workers=3,
        batch_size=10,
        batches=(batch,),
        ingestion_start_time=0.0,
        batch_creation_duration=0.0,
        processing_start_time=0.0,
        results=(result,),
    )
    ctx = RuntimeIngestContext.from_ingest_context(
        IngestContext(source=".", target="fallback.zarr", output_format="zarr"),
        run_id="run-001",
        temp_root=Path("."),
        materializer=lambda p: Path(p),
    )

    final_result = executor.finalize(ctx, state, host)

    assert final_result.metrics["pipeline"]["workers"] == 3
    warning_calls = list(executor._log.warning.call_args_list)
    assert warning_calls
    assert "reserved aggregate metrics key" in warning_calls[0][0][0]


def test_finalize_handles_non_mapping_aggregate_metrics():
    executor = PipelineExecutor()
    executor._log = MagicMock(spec=logging.Logger)

    host = MagicMock(spec=PipelineHost)
    host._aggregate_metrics.return_value = ["bad", "metrics"]
    host.name = "dummy"
    host._chunk_manager = None

    batch = PipelineBatch(batch_id="b1", data_path=Path("."), items=[], size_bytes=0, files_count=0)
    result = PipelineResult(
        batch=batch, outputs=OutputPaths(primary="s3://bucket/product"), success=True
    )
    state = PipelineRunState(
        product="dummy",
        pipeline_workers=1,
        batch_size=1,
        batches=(batch,),
        ingestion_start_time=0.0,
        batch_creation_duration=0.0,
        processing_start_time=0.0,
        results=(result,),
    )
    ctx = RuntimeIngestContext.from_ingest_context(
        IngestContext(source=".", target="fallback.zarr", output_format="zarr"),
        run_id="run-001",
        temp_root=Path("."),
        materializer=lambda p: Path(p),
    )

    final_result = executor.finalize(ctx, state, host)

    assert "pipeline" in final_result.metrics
    warning_calls = list(executor._log.warning.call_args_list)
    assert warning_calls
    assert "non-mapping aggregate metrics" in warning_calls[0][0][0]


def test_finalize_injects_control_plane_storage_metrics(tmp_path):
    executor = PipelineExecutor()
    executor._log = MagicMock(spec=logging.Logger)

    host = MagicMock(spec=PipelineHost)
    host._aggregate_metrics.return_value = {"custom": 1}
    host.name = "dummy"
    host._chunk_manager = ChunkManager(binding=make_test_binding(tmp_path), workspace=tmp_path)

    batch = PipelineBatch(batch_id="b1", data_path=Path("."), items=[], size_bytes=0, files_count=0)
    result = PipelineResult(
        batch=batch, outputs=OutputPaths(primary="s3://bucket/product"), success=True
    )
    state = PipelineRunState(
        product="dummy",
        pipeline_workers=1,
        batch_size=1,
        batches=(batch,),
        ingestion_start_time=0.0,
        batch_creation_duration=0.0,
        processing_start_time=0.0,
        results=(result,),
    )
    ctx = make_test_context(tmp_path, source=".")
    ctx = RuntimeIngestContext.from_ingest_context(
        ctx,
        run_id="run-001",
        temp_root=tmp_path,
        materializer=lambda p: Path(p),
    )

    final_result = executor.finalize(ctx, state, host)
    storage = final_result.metrics["storage"]

    assert storage["control_root"]
    assert storage["latest_pointer"]
    assert storage["control_root"].endswith("/product.zarr/.firecube")
    assert storage["latest_pointer"].endswith("/product.zarr/.firecube/LATEST.json")
    assert "._firecube_manifest.jsonl" not in str(storage.values())
