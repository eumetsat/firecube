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

from firecube.core.observability.metrics import RUN_SUMMARY_SCHEMA
from firecube.ingestor.runtime.telemetry import extract_rows_processed
from firecube.ingestor.types.result_metrics import PipelineMetrics, ResultMetrics


def test_rows_processed_from_typed_metrics() -> None:
    metrics = ResultMetrics(pipeline=PipelineMetrics(rows_processed=100))

    assert extract_rows_processed(metrics) == 100


def test_rows_ingested_fallback() -> None:
    metrics = ResultMetrics(pipeline=PipelineMetrics(rows_ingested=50))

    assert extract_rows_processed(metrics) == 50


def test_empty_pipeline_metrics_yields_zero() -> None:
    metrics = ResultMetrics()

    assert extract_rows_processed(metrics) == 0


def test_emitted_metric_names_stable() -> None:
    metric_names = {spec.metric_name for spec in RUN_SUMMARY_SCHEMA.values()}

    assert metric_names == {
        "firecube_pipeline_workers",
        "firecube_pipeline_batch_size",
        "firecube_pipeline_batches_total",
        "firecube_pipeline_batches_failed_total",
        "firecube_pipeline_hook_failures_total",
        "firecube_files_processed_total",
        "firecube_bytes_ingested_total",
        "firecube_rows_processed_total",
        "firecube_run_duration_seconds",
        "firecube_pipeline_duration_seconds",
        "firecube_pipeline_batch_duration_seconds",
        "firecube_pipeline_batch_creation_duration_seconds",
        "firecube_pipeline_upload_duration_seconds",
        "firecube_pipeline_cpu_duration_seconds",
        "firecube_pipeline_non_cpu_wait_seconds",
        "firecube_pipeline_cpu_utilization_estimate",
        "firecube_storage_client_requests_total",
        "firecube_storage_client_errors_total",
        "firecube_storage_client_retryable_errors_total",
        "firecube_storage_client_latency_seconds",
        "firecube_storage_client_bytes_read_total",
        "firecube_storage_client_bytes_written_total",
        "firecube_control_plane_corruption_total",
        "firecube_control_plane_torn_tail_recovery_total",
        "firecube_control_plane_snapshot_rebuild_duration_seconds",
        "firecube_control_plane_snapshot_rebuild_total",
        "firecube_resume_guard_enforce_duration_seconds",
        "firecube_resume_guard_runs_enumerated_total",
        "firecube_resume_guard_spans_scanned_total",
    }
