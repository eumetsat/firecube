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

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from firecube.core.controlplane import SpanCoverage
from firecube.ingestor.runtime.recording import SpanRecorder
from firecube.ingestor.types.context import IngestResult, RuntimeIngestContext
from firecube.ingestor.types.result_metrics import (
    OutputPaths,
    PipelineMetrics,
    ResultMetrics,
    StorageMetrics,
)


def _make_recorder() -> tuple[SpanRecorder, MagicMock]:
    chunk_manager = MagicMock()
    chunk_manager.record_span = MagicMock()
    chunk_manager.record_run_terminal = MagicMock()
    return SpanRecorder(chunk_manager), chunk_manager


def _make_context(tmp_path: Path) -> RuntimeIngestContext:
    return RuntimeIngestContext(source=str(tmp_path), options={})


def test_recording_handles_empty_metrics(tmp_path) -> None:
    recorder, chunk_manager = _make_recorder()
    ctx = _make_context(tmp_path)
    result = IngestResult(
        output_format="test", outputs=OutputPaths(primary="/tmp/empty"), metrics=ResultMetrics()
    )

    recorder.register_run(
        ctx=ctx,
        result=result,
        run_id="run",
        product="product",
        slice_meta={"plugin": "test"},
        record_spans=False,
    )

    assert chunk_manager.record_run_terminal.call_args.kwargs["size"] == 0


def test_recording_reads_coverage_from_typed_metrics(tmp_path) -> None:
    recorder, chunk_manager = _make_recorder()
    ctx = _make_context(tmp_path)
    coverage = [
        SpanCoverage(
            group="g1",
            arrays=["a1"],
            time_min="2024-01-01T00:00:00",
            time_max="2024-01-01T01:00:00",
        )
    ]
    result = IngestResult(
        output_format="test",
        outputs=OutputPaths(primary="/tmp/coverage"),
        metrics=ResultMetrics(pipeline=PipelineMetrics(coverage=coverage)),
    )

    recorder.register_run(
        ctx=ctx,
        result=result,
        run_id="run",
        product="product",
        slice_meta={"plugin": "test"},
    )

    assert chunk_manager.record_span.call_count == 1
    assert chunk_manager.record_span.call_args.kwargs["coverage"] == coverage[0]


def test_recording_reads_storage_from_typed_metrics(tmp_path) -> None:
    recorder, chunk_manager = _make_recorder()
    ctx = _make_context(tmp_path)
    result = IngestResult(
        output_format="test",
        outputs=OutputPaths(primary="/tmp/storage"),
        metrics=ResultMetrics(storage=StorageMetrics(bytes=100)),
    )

    recorder.register_run(
        ctx=ctx,
        result=result,
        run_id="run",
        product="product",
        slice_meta={"plugin": "test"},
        record_spans=False,
    )

    assert chunk_manager.record_run_terminal.call_args.kwargs["size"] == 100


def test_recording_reads_outputs_primary(tmp_path) -> None:
    recorder, chunk_manager = _make_recorder()
    ctx = _make_context(tmp_path)
    result = IngestResult(
        output_format="test",
        outputs=OutputPaths(primary="/tmp/x"),
        metrics=ResultMetrics(),
    )

    recorder.register_run(
        ctx=ctx,
        result=result,
        run_id="run",
        product="product",
        slice_meta={"plugin": "test"},
        record_spans=False,
    )

    assert chunk_manager.record_run_terminal.call_args.kwargs["output_path"] == "/tmp/x"
