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

"""Fail-stop hosts: the runner stops after the first failed batch.

Behaviour under test: with ``stop_on_batch_failure=True`` the batches after
the first failure are reported as not attempted (never as failed), never reach
the batch hooks, and the run summary and terminal message account for them.
A host with ``stop_on_batch_failure=False`` attempts every batch.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any, ClassVar

import pytest

from firecube.ingestor.config.engine import EngineConfig
from firecube.ingestor.runtime.engine import (
    PipelineExecutor,
    PipelineFailedBatchesError,
    PipelineRunner,
)
from firecube.ingestor.runtime.telemetry import derive_pipeline_summary
from firecube.ingestor.types.context import (
    IngestContext,
    IngestResult,
    OutputPaths,
    PipelineBatch,
    PipelineResult,
    PipelineRunState,
    PluginContext,
    RuntimeIngestContext,
)

pytestmark = pytest.mark.unit

_TIMEOUT_S = 10.0
_FAIL_INDEX = 1  # batch 2 of 5


class _HaltSignal(logging.Handler):
    """Sets an event once the runner logs that it halted (after cancelling)."""

    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.halted = threading.Event()

    def emit(self, record: logging.LogRecord) -> None:
        if "Pipeline halted" in record.getMessage():
            self.halted.set()


class _StubHost:
    """Minimal PipelineHost whose batch ``_FAIL_INDEX`` fails.

    ``emulate_gate`` mirrors the Zarr template's ordered write gate: once a
    batch has failed, later batches that still get to run come back as
    ``attempted=False``. ``hold_until`` keeps non-failing batches from
    finishing until the runner has halted, which makes the parallel case
    deterministic (pending batches are still pending when they are cancelled).
    """

    stop_on_batch_failure: ClassVar[bool] = True

    def __init__(
        self,
        *,
        emulate_gate: bool = True,
        hold_until: threading.Event | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.name = "stub"
        self._log = logger or logging.getLogger("test.engine_fail_stop")
        self._chunk_manager = None
        self.engine_config = EngineConfig(write_mode="direct")
        self._emulate_gate = emulate_gate
        self._hold_until = hold_until
        self._lock = threading.Lock()
        self.failed_index: int | None = None
        self.attempted: list[int] = []
        self.success_hook_ids: list[str] = []
        self.failure_hook_ids: list[str] = []
        self.pipeline_started = 0

    def _create_batches(
        self, ctx: RuntimeIngestContext, batch_size: int
    ) -> Iterable[PipelineBatch]:
        _ = (ctx, batch_size)
        return []

    def _aggregate_metrics(
        self, ctx: RuntimeIngestContext, state: PipelineRunState
    ) -> Mapping[str, Any]:
        _ = (ctx, state)
        return {}

    def _resolve_time_dim_name(self) -> str:
        return "time"

    def on_pipeline_start(self, ctx: PluginContext, state: PipelineRunState) -> None:
        _ = (ctx, state)
        self.pipeline_started += 1

    def on_batch_success(
        self,
        ctx: PluginContext,
        state: PipelineRunState,
        batch: PipelineBatch,
        result: PipelineResult,
    ) -> None:
        _ = (ctx, state)
        assert result.attempted, "attempted=False result reached on_batch_success"
        self.success_hook_ids.append(batch.batch_id)

    def on_batch_failure(
        self,
        ctx: PluginContext,
        state: PipelineRunState,
        batch: PipelineBatch,
        result: PipelineResult,
    ) -> None:
        _ = (ctx, state)
        assert result.attempted, "attempted=False result reached on_batch_failure"
        self.failure_hook_ids.append(batch.batch_id)

    def finalize_pipeline(self, ctx: RuntimeIngestContext, state: PipelineRunState) -> IngestResult:
        raise NotImplementedError

    def _process_batch(self, batch: PipelineBatch, ctx: PluginContext) -> PipelineResult:
        _ = ctx
        index = int(batch.metadata["batch_index"])
        if index == _FAIL_INDEX:
            with self._lock:
                self.failed_index = index
                self.attempted.append(index)
            return PipelineResult(
                batch=batch,
                outputs=OutputPaths(primary=Path("")),
                success=False,
                error=f"boom in {batch.batch_id}",
            )
        if self._hold_until is not None:
            assert self._hold_until.wait(_TIMEOUT_S), "runner never halted"
        with self._lock:
            refused = (
                self._emulate_gate and self.failed_index is not None and index > self.failed_index
            )
            if not refused:
                self.attempted.append(index)
        if refused:
            return PipelineResult(
                batch=batch,
                outputs=OutputPaths(primary=Path("")),
                success=False,
                attempted=False,
                error="not attempted: run halted after an earlier batch failed",
            )
        return PipelineResult(batch=batch, outputs=OutputPaths(primary="out.zarr"), success=True)


class _AttemptAllHost(_StubHost):
    stop_on_batch_failure: ClassVar[bool] = False


def _batches(count: int = 5) -> list[PipelineBatch]:
    return [
        PipelineBatch(
            batch_id=f"stub_batch_{i:04d}",
            data_path=Path("."),
            items=[f"item-{i}"],
            metadata={"batch_index": i, "is_last": i == count - 1},
            files_count=1,
        )
        for i in range(count)
    ]


def _runtime_ctx(tmp_path: Path) -> RuntimeIngestContext:
    return RuntimeIngestContext.from_ingest_context(
        IngestContext(source=str(tmp_path), target="out.zarr", output_format="zarr"),
        run_id="fail-stop-run",
        temp_root=tmp_path,
        materializer=lambda p: Path(p),
    )


def _run(host: Any, ctx: RuntimeIngestContext, *, workers: int) -> PipelineRunState:
    return PipelineRunner().run_state(
        ingestor=host,
        ctx=ctx,
        product="stub",
        pipeline_workers=workers,
        batch_size=1,
        batches=_batches(),
        batch_creation_duration=0.0,
        ingestion_start_time=0.0,
        execution_mode="sequential" if workers == 1 else "parallel",
        emit_progress_logs=False,
    )


def _ids(indices: Iterable[int]) -> list[str]:
    return [f"stub_batch_{i:04d}" for i in indices]


def test_sequential_fail_stop_marks_remaining_batches_not_attempted(tmp_path: Path) -> None:
    host = _StubHost()

    state = _run(host, _runtime_ctx(tmp_path), workers=1)

    assert [r.batch.batch_id for r in state.results] == _ids([0, 1])
    assert [r.success for r in state.results] == [True, False]
    assert all(r.attempted for r in state.results)
    assert [b.batch_id for b in state.batches_not_attempted] == _ids([2, 3, 4])
    assert host.attempted == [0, 1]
    assert host.success_hook_ids == _ids([0])
    assert host.failure_hook_ids == _ids([1])

    summary = derive_pipeline_summary(state, {})
    assert summary["batches_total"] == 5
    assert summary["batches_failed"] == 1
    assert summary["batches_not_attempted"] == 3


def test_parallel_fail_stop_cancels_pending_and_refuses_in_flight(tmp_path: Path) -> None:
    signal = _HaltSignal()
    logger = logging.getLogger("test.engine_fail_stop.parallel")
    logger.addHandler(signal)
    logger.propagate = False
    host = _StubHost(hold_until=signal.halted, logger=logger)

    try:
        state = _run(host, _runtime_ctx(tmp_path), workers=2)
    finally:
        logger.removeHandler(signal)

    # Batches 0 and 1 were in flight: 0 completes normally, 1 fails.
    # Batch 2 started on the freed worker and was refused by the (emulated)
    # closed gate; 3 and 4 were still pending and got cancelled.
    assert sorted(r.batch.batch_id for r in state.results) == _ids([0, 1])
    assert {r.batch.batch_id: r.success for r in state.results} == {
        "stub_batch_0000": True,
        "stub_batch_0001": False,
    }
    assert all(r.attempted for r in state.results)
    assert sorted(b.batch_id for b in state.batches_not_attempted) == _ids([2, 3, 4])
    assert sorted(host.attempted) == [0, 1]
    assert host.success_hook_ids == _ids([0])
    assert host.failure_hook_ids == _ids([1])

    summary = derive_pipeline_summary(state, {})
    assert summary["batches_failed"] == 1
    assert summary["batches_not_attempted"] == 3


@pytest.mark.parametrize("workers", [1, 2])
def test_host_without_fail_stop_attempts_every_batch(tmp_path: Path, workers: int) -> None:
    host = _AttemptAllHost(emulate_gate=False)

    state = _run(host, _runtime_ctx(tmp_path), workers=workers)

    assert sorted(host.attempted) == [0, 1, 2, 3, 4]
    assert len(state.results) == 5
    assert sum(1 for r in state.results if not r.success) == 1
    assert state.batches_not_attempted == ()
    assert sorted(host.success_hook_ids) == _ids([0, 2, 3, 4])
    assert host.failure_hook_ids == _ids([1])

    summary = derive_pipeline_summary(state, {})
    assert summary["batches_failed"] == 1
    assert summary["batches_not_attempted"] == 0


def test_finalize_message_names_failed_and_not_attempted_counts(tmp_path: Path) -> None:
    host = _StubHost()
    ctx = _runtime_ctx(tmp_path)
    state = _run(host, ctx, workers=1)

    with pytest.raises(PipelineFailedBatchesError) as excinfo:
        PipelineExecutor().finalize(ctx, state, host)  # type: ignore[arg-type]

    message = str(excinfo.value)
    assert "had 1 failed batch(es): boom in stub_batch_0001" in message
    assert "3 later batch(es) were not attempted" in message
    assert "no batch appends past a gap" in message
    assert "status=failed" in message
    assert "--option resume_existing=true" in message
    assert "--option force_reingest=true" in message


def test_finalize_message_omits_not_attempted_clause_when_all_attempted(tmp_path: Path) -> None:
    host = _AttemptAllHost(emulate_gate=False)
    ctx = _runtime_ctx(tmp_path)
    state = _run(host, ctx, workers=1)

    with pytest.raises(PipelineFailedBatchesError) as excinfo:
        PipelineExecutor().finalize(ctx, state, host)  # type: ignore[arg-type]

    message = str(excinfo.value)
    assert "had 1 failed batch(es)" in message
    assert "not attempted" not in message
    assert "--option resume_existing=true" in message
