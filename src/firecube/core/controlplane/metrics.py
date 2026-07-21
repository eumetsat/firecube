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

"""Process-local WAL health metrics for ingestion runs.

These metrics are process-local and measure client-observed control-plane events.
They do not represent storage backend internals.
"""

from __future__ import annotations

import contextlib
import contextvars
from dataclasses import dataclass

from firecube.core.observability.metrics import (
    WAL_SUMMARY_KEY_CORRUPTION,
    WAL_SUMMARY_KEY_SNAPSHOT_REBUILD_COUNT,
    WAL_SUMMARY_KEY_SNAPSHOT_REBUILD_DURATION,
    WAL_SUMMARY_KEY_TORN_TAIL_RECOVERY,
)


@dataclass(slots=True)
class WalMetrics:
    """Process-local WAL health counters collected during an ingestion run."""

    corruption_count: int = 0
    torn_tail_recovery_count: int = 0
    snapshot_rebuild_duration_s: float = 0.0
    snapshot_rebuild_count: int = 0

    def as_summary(self) -> dict[str, int | float]:
        return {
            WAL_SUMMARY_KEY_CORRUPTION: int(self.corruption_count),
            WAL_SUMMARY_KEY_TORN_TAIL_RECOVERY: int(self.torn_tail_recovery_count),
            WAL_SUMMARY_KEY_SNAPSHOT_REBUILD_DURATION: float(self.snapshot_rebuild_duration_s),
            WAL_SUMMARY_KEY_SNAPSHOT_REBUILD_COUNT: int(self.snapshot_rebuild_count),
        }


_ACTIVE_WAL_METRICS: contextvars.ContextVar[WalMetrics | None] = contextvars.ContextVar(
    "firecube_active_wal_metrics",
    default=None,
)


def active_wal_metrics() -> WalMetrics | None:
    """Return currently active WAL metrics collector for this context."""
    return _ACTIVE_WAL_METRICS.get()


@contextlib.contextmanager
def collect_wal_metrics():
    """Collect WAL metrics for the current context."""
    metrics = WalMetrics()
    token = _ACTIVE_WAL_METRICS.set(metrics)
    try:
        yield metrics
    finally:
        _ACTIVE_WAL_METRICS.reset(token)


def record_wal_corruption() -> None:
    """Record a WAL corruption event."""
    metrics = _ACTIVE_WAL_METRICS.get()
    if metrics is None:
        return
    metrics.corruption_count += 1


def record_wal_torn_tail_recovery() -> None:
    """Record a torn WAL tail recovery."""
    metrics = _ACTIVE_WAL_METRICS.get()
    if metrics is None:
        return
    metrics.torn_tail_recovery_count += 1


def record_wal_snapshot_rebuild(duration_s: float) -> None:
    """Record a snapshot rebuild with its duration."""
    metrics = _ACTIVE_WAL_METRICS.get()
    if metrics is None:
        return
    metrics.snapshot_rebuild_duration_s = float(duration_s)
    metrics.snapshot_rebuild_count += 1
