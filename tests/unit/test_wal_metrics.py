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

from firecube.core.controlplane.metrics import (
    WalMetrics,
    collect_wal_metrics,
    record_wal_corruption,
    record_wal_snapshot_rebuild,
    record_wal_torn_tail_recovery,
)
from firecube.core.observability.metrics import (
    METRIC_WAL_CORRUPTION,
    METRIC_WAL_SNAPSHOT_REBUILD_COUNT,
    METRIC_WAL_SNAPSHOT_REBUILD_DURATION,
    METRIC_WAL_TORN_TAIL_RECOVERY,
    RUN_SUMMARY_SCHEMA,
)


def test_collect_context_manager():
    with collect_wal_metrics() as m:
        record_wal_corruption()
        record_wal_torn_tail_recovery()
        record_wal_snapshot_rebuild(1.5)

    assert m.corruption_count == 1
    assert m.torn_tail_recovery_count == 1
    assert m.snapshot_rebuild_duration_s == 1.5
    assert m.snapshot_rebuild_count == 1


def test_noop_outside_context():
    record_wal_corruption()
    record_wal_torn_tail_recovery()
    record_wal_snapshot_rebuild(1.0)


def test_as_summary_keys():
    m = WalMetrics(
        corruption_count=2,
        torn_tail_recovery_count=1,
        snapshot_rebuild_duration_s=3.0,
        snapshot_rebuild_count=1,
    )
    summary = m.as_summary()
    assert set(summary.keys()) == {
        "wal_corruption_count",
        "wal_torn_tail_recovery_count",
        "wal_snapshot_rebuild_duration_s",
        "wal_snapshot_rebuild_count",
    }
    assert summary["wal_corruption_count"] == 2
    assert summary["wal_torn_tail_recovery_count"] == 1
    assert summary["wal_snapshot_rebuild_duration_s"] == 3.0
    assert summary["wal_snapshot_rebuild_count"] == 1


def test_context_isolation():
    with collect_wal_metrics() as m1:
        record_wal_corruption()

    with collect_wal_metrics() as m2:
        record_wal_corruption()
        record_wal_corruption()

    assert m1.corruption_count == 1
    assert m2.corruption_count == 2


def test_corruption_increments():
    with collect_wal_metrics() as m:
        for _ in range(5):
            record_wal_corruption()
    assert m.corruption_count == 5


def test_snapshot_rebuild_records_duration():
    with collect_wal_metrics() as m:
        record_wal_snapshot_rebuild(3.14)
    assert m.snapshot_rebuild_duration_s == 3.14
    assert m.snapshot_rebuild_count == 1


def test_as_summary_keys_match_schema_constants():
    """WAL as_summary() keys must match RUN_SUMMARY_SCHEMA entries and metric constants."""
    m = WalMetrics(
        corruption_count=1,
        torn_tail_recovery_count=2,
        snapshot_rebuild_duration_s=0.5,
        snapshot_rebuild_count=3,
    )
    summary = m.as_summary()

    for key in summary:
        assert key in RUN_SUMMARY_SCHEMA, f"{key} missing from RUN_SUMMARY_SCHEMA"

    assert RUN_SUMMARY_SCHEMA["wal_corruption_count"].metric_name == METRIC_WAL_CORRUPTION
    assert (
        RUN_SUMMARY_SCHEMA["wal_torn_tail_recovery_count"].metric_name
        == METRIC_WAL_TORN_TAIL_RECOVERY
    )
    assert (
        RUN_SUMMARY_SCHEMA["wal_snapshot_rebuild_duration_s"].metric_name
        == METRIC_WAL_SNAPSHOT_REBUILD_DURATION
    )
    assert (
        RUN_SUMMARY_SCHEMA["wal_snapshot_rebuild_count"].metric_name
        == METRIC_WAL_SNAPSHOT_REBUILD_COUNT
    )
