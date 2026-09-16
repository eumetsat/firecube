# Copyright 2025-2026 EUMETSAT
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""``list_runs`` is called lazily during ``list_chunks`` dedupe.

In the common case (no in-flight force-reingest, so no two active spans
share ``(product, group, time_min, time_max)``), ``_dedupe_active_spans``
is a no-op and the ``(product, run_id) -> started_at`` lookup is unused.
Reading the WAL to build that lookup on every ``list_chunks`` call is
pure overhead. This regression lock proves the WAL is not read when
there is no slice-key collision, and IS read exactly once when there is
one.

Observation is done through the public ``list_chunks`` output and the
``run-started cache miss`` ``DEBUG`` log emitted by ``ChunkManager``
when the dedupe path opens the WAL. The log emission is the lookup's
public observability contract — count == WAL reads triggered by the
dedupe path.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from firecube.core.controlplane import ChunkManager, SpanCoverage
from tests.helpers.storage import make_test_binding

pytestmark = pytest.mark.unit

_MANAGER_LOGGER = "firecube.core.controlplane.manager"
_CACHE_MISS_TEXT = "run-started cache miss"


def _record_completed_span(
    manager: ChunkManager,
    *,
    product: str,
    run_id: str,
    batch_id: str,
    group: str,
    time_min: str,
    time_max: str,
) -> None:
    output_path = f"{manager.base_uri.rstrip('/')}/{product}"
    meta = {
        "plugin": "test_product",
        "group": group,
        "time_min": time_min,
        "time_max": time_max,
    }
    manager.record_run_started(
        product=product,
        run_id=run_id,
        output_path=output_path,
        output_format="zarr",
        size=0,
        meta={"plugin": "test_product"},
    )
    manager.record_span(
        product=product,
        run_id=run_id,
        batch_id=batch_id,
        group=group,
        status="active",
        coverage=SpanCoverage(
            group=group,
            arrays=[f"{group}/FWI"],
            time_index_ranges=[[0, 1]],
            time_min=time_min,
            time_max=time_max,
        ),
        meta=meta,
    )
    manager.record_run_terminal(
        product=product,
        run_id=run_id,
        output_path=output_path,
        output_format="zarr",
        size=1,
        meta={"plugin": "test_product"},
        status="complete",
    )


def _fresh_manager(tmp_path: Path) -> ChunkManager:
    """Return a ChunkManager whose in-memory state has been dropped.

    Closing and reopening forces subsequent reads to hit the on-disk WAL
    (or its snapshot) rather than any process-local cache, so caplog
    installed after this point measures real read cost.
    """
    binding = make_test_binding(tmp_path)
    manager = ChunkManager(binding=binding, workspace=tmp_path)
    return manager


def _count_cache_misses(caplog: pytest.LogCaptureFixture) -> int:
    return sum(
        1
        for rec in caplog.records
        if rec.name == _MANAGER_LOGGER and _CACHE_MISS_TEXT in rec.getMessage()
    )


def test_list_chunks_skips_list_runs_when_no_slice_collision(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    product = "test_product"
    manager = _fresh_manager(tmp_path)

    n_spans = 5
    for i in range(n_spans):
        _record_completed_span(
            manager,
            product=product,
            run_id=f"run-{i:03d}",
            batch_id=f"batch-{i:03d}",
            group=f"F{i:03d}",
            time_min=f"2024-01-{i + 1:02d}T00:00:00Z",
            time_max=f"2024-01-{i + 1:02d}T01:00:00Z",
        )

    manager.close()
    manager = _fresh_manager(tmp_path)
    manager.rebuild_snapshot(product)

    with caplog.at_level(logging.DEBUG, logger=_MANAGER_LOGGER):
        chunks = manager.list_chunks(product=product, chunk_type="span")

    assert len(chunks) == n_spans, "all unique spans should pass through dedupe"
    miss_count = _count_cache_misses(caplog)
    assert miss_count == 0, (
        "no slice-key collision → run-started lookup must not read the WAL "
        f"(observed {miss_count} cache-miss log lines)"
    )


def test_list_chunks_calls_list_runs_once_when_slice_collision_present(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    product = "test_product"
    manager = _fresh_manager(tmp_path)

    _record_completed_span(
        manager,
        product=product,
        run_id="run-older",
        batch_id="batch-001",
        group="F024",
        time_min="2024-01-01T00:00:00Z",
        time_max="2024-01-02T00:00:00Z",
    )
    _record_completed_span(
        manager,
        product=product,
        run_id="run-newer",
        batch_id="batch-002",
        group="F024",
        time_min="2024-01-01T00:00:00Z",
        time_max="2024-01-02T00:00:00Z",
    )

    manager.close()
    manager = _fresh_manager(tmp_path)
    manager.rebuild_snapshot(product)

    with caplog.at_level(logging.DEBUG, logger=_MANAGER_LOGGER):
        chunks = manager.list_chunks(product=product, chunk_type="span")

    miss_count = _count_cache_misses(caplog)
    assert miss_count == 1, (
        "slice-key collision → run-started lookup must read the WAL exactly once "
        f"(observed {miss_count} cache-miss log lines)"
    )
    assert len(chunks) == 1, "dedupe must keep exactly one winner per colliding slice key"
    assert (chunks[0].meta or {})["run_id"] == "run-newer", (
        "highest started_at (most-recently-started run) must win — proves the lookup was "
        "consulted, not silently skipped"
    )
