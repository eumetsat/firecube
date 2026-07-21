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

from firecube.core.observability.metrics import (
    _INTEGER_SUMMARY_KEYS,
    RUN_SUMMARY_SCHEMA,
)

_EXPECTED_KEYS: frozenset[str] = frozenset(
    {
        "workers",
        "batch_size",
        "batches_total",
        "batches_failed",
        "hook_failures",
        "files_processed",
        "bytes_ingested",
        "rows_processed",
        "duration_total_s",
        "duration_pipeline_s",
        "duration_processing_s",
        "duration_batch_creation_s",
        "duration_upload_s",
        "duration_cpu_s",
        "non_cpu_wait_s",
        "cpu_utilization_estimate",
        "storage_client_requests",
        "storage_client_errors",
        "storage_client_retryable_errors",
        "storage_client_latency_s_total",
        "storage_client_bytes_read",
        "storage_client_bytes_written",
        "wal_corruption_count",
        "wal_torn_tail_recovery_count",
        "wal_snapshot_rebuild_duration_s",
        "wal_snapshot_rebuild_count",
        "resume_guard_enforce_duration_s",
        "resume_guard_runs_enumerated",
        "resume_guard_spans_scanned",
    }
)

_EXPECTED_INTEGER_KEYS: frozenset[str] = frozenset(
    {
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
)


def test_schema_key_set() -> None:
    assert set(RUN_SUMMARY_SCHEMA.keys()) == _EXPECTED_KEYS


def test_integer_summary_keys() -> None:
    assert _INTEGER_SUMMARY_KEYS == _EXPECTED_INTEGER_KEYS


def test_wal_keys_subset() -> None:
    assert {
        "wal_corruption_count",
        "wal_torn_tail_recovery_count",
        "wal_snapshot_rebuild_duration_s",
        "wal_snapshot_rebuild_count",
    }.issubset(RUN_SUMMARY_SCHEMA.keys())


def test_filesystem_keys_subset() -> None:
    assert {
        "storage_client_requests",
        "storage_client_errors",
        "storage_client_retryable_errors",
        "storage_client_latency_s_total",
        "storage_client_bytes_read",
        "storage_client_bytes_written",
    }.issubset(RUN_SUMMARY_SCHEMA.keys())


def test_schema_entry_types() -> None:
    for spec in RUN_SUMMARY_SCHEMA.values():
        assert hasattr(spec, "metric_name")
        assert isinstance(spec.metric_name, str)
        assert spec.kind in {"counter", "gauge"}
