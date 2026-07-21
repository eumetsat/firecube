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

import contextlib
from pathlib import Path

from firecube.core.observability.metrics import (
    METRIC_PIPELINE_DURATION,
    METRIC_RUN_DURATION,
    RUN_SUMMARY_SCHEMA,
    TelemetryService,
)
from firecube.ingestor.contracts.interfaces import IngestionTelemetry
from firecube.ingestor.runtime.telemetry import compute_run_summary
from firecube.ingestor.types.context import (
    OutputPaths,
    PipelineBatch,
    PipelineResult,
    PipelineRunState,
)


class MockTelemetry(IngestionTelemetry):
    def __init__(self):
        self.emitted: list[tuple[str, float, str, dict | None]] = []

    def emit(self, name, value, *, kind="gauge", meta=None):
        self.emitted.append((name, value, kind, meta))

    @property
    def run_id(self):
        return "test-run"

    def flush(self):
        return None

    def span(self, name, attributes=None):
        _ = (name, attributes)
        return contextlib.nullcontext()

    def collect_memory_stats(self):
        return None


def test_telemetry_start_no_emit():
    """start() should not emit a synthetic run_duration=0 metric."""
    telemetry = MockTelemetry()
    service = TelemetryService(telemetry, "test-plugin")
    service.start("run-123")

    assert all(name != METRIC_RUN_DURATION for name, *_ in telemetry.emitted)


def test_compute_run_summary_matches_schema_and_includes_cpu_io():
    batch = PipelineBatch(batch_id="b1", data_path=Path("."), files_count=2, size_bytes=20)
    state = PipelineRunState(
        product="p",
        pipeline_workers=2,
        batch_size=4,
        batches=(batch,),
        ingestion_start_time=0.0,
        batch_creation_duration=0.5,
        processing_start_time=0.0,
        processing_duration=10.0,
        total_ingestion_duration=12.0,
        results=(PipelineResult(batch=batch, outputs=OutputPaths(primary="out"), success=True),),
        cpu_time_total=8.0,
        io_time_total=2.0,
        hook_failures=1,
    )

    summary = compute_run_summary(
        state,
        duration_upload_s=0.0,
        files_processed=2,
        bytes_ingested=42,
        rows_processed=77,
    )

    assert set(summary) == set(RUN_SUMMARY_SCHEMA)
    assert RUN_SUMMARY_SCHEMA["duration_pipeline_s"].metric_name == METRIC_PIPELINE_DURATION
    assert "duration_storage_s" not in summary
    assert summary["duration_pipeline_s"] == 12.0
    assert summary["duration_upload_s"] == 0.0
    assert (
        summary["duration_total_s"] == summary["duration_pipeline_s"] + summary["duration_upload_s"]
    )
    assert summary["duration_cpu_s"] == 8.0
    assert summary["non_cpu_wait_s"] == 2.0
    assert summary["cpu_utilization_estimate"] == 0.4  # 8 / (10 * 2)


def test_emit_run_metrics_emits_all_schema_metrics():
    telemetry = MockTelemetry()
    service = TelemetryService(telemetry, "test-plugin")
    summary = dict.fromkeys(RUN_SUMMARY_SCHEMA, 1)

    service.emit_run_metrics(summary)

    emitted_names = {name for name, *_ in telemetry.emitted}
    expected_names = {spec.metric_name for spec in RUN_SUMMARY_SCHEMA.values()}
    assert emitted_names == expected_names
