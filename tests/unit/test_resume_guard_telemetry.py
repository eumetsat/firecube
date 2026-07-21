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

import logging
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from firecube.core.controlplane.manager import ChunkManager
from firecube.core.controlplane.types import SpanCoverage
from firecube.ingestor.errors import ResumeConflictError
from firecube.ingestor.runtime.resume_guard import ResumeGuard
from tests.helpers.storage import make_test_binding

PRODUCT = "product.zarr"
PLUGIN = "telemetry-plugin"

_EXPECTED_KEYS = frozenset(
    {
        "resume_guard_enforce_duration_s",
        "resume_guard_runs_enumerated",
        "resume_guard_spans_scanned",
    }
)


def _make_ctx(**options: Any) -> MagicMock:
    ctx = MagicMock()
    ctx.force_reingest = bool(options.pop("force_reingest", False))
    ctx.option.side_effect = lambda name, default=None: options.get(name, default)
    return ctx


def _make_manager(tmp_path: Path) -> ChunkManager:
    return ChunkManager(
        binding=make_test_binding(tmp_path, product=PRODUCT),
        workspace=tmp_path,
    )


def _make_guard(manager: ChunkManager) -> ResumeGuard:
    return ResumeGuard(
        plugin_name=PLUGIN,
        chunk_manager=manager,
        log=logging.getLogger(__name__),
        slice_meta_keys=(),
    )


def _seed_completed_runs_with_spans(
    manager: ChunkManager, *, run_count: int, span_count: int
) -> None:
    for index in range(run_count):
        run_id = f"run-{index:02d}"
        meta = {"plugin": PLUGIN, "sequence": index}
        manager.record_run_started(
            product=PRODUCT,
            run_id=run_id,
            output_path=f"file:///tmp/{run_id}",
            output_format="zarr",
            size=1,
            meta=meta,
        )
        if index < span_count:
            manager.record_span(
                PRODUCT,
                run_id,
                f"batch-{index}",
                "group-a",
                "active",
                coverage=SpanCoverage(
                    group="group-a",
                    arrays=["data"],
                    time_index_ranges=[[index * 10, index * 10 + 9]],
                ),
                meta={"plugin": PLUGIN},
            )
        manager.record_run_terminal(
            product=PRODUCT,
            run_id=run_id,
            output_path=f"file:///tmp/{run_id}",
            output_format="zarr",
            size=1,
            meta=meta,
            status="complete",
        )


@pytest.mark.unit
def test_empty_product_emits_zero_counters(tmp_path: Path) -> None:
    manager = _make_manager(tmp_path)
    guard = _make_guard(manager)

    guard.enforce(ctx=_make_ctx(), product=PRODUCT)

    assert guard.last_metrics is not None
    summary = guard.last_metrics.as_summary()
    assert set(summary.keys()) == _EXPECTED_KEYS
    assert summary["resume_guard_runs_enumerated"] == 0
    assert summary["resume_guard_spans_scanned"] == 0
    assert summary["resume_guard_enforce_duration_s"] >= 0.0


@pytest.mark.unit
def test_populated_product_counts_runs_and_spans_slot_range(tmp_path: Path) -> None:
    manager = _make_manager(tmp_path)
    _seed_completed_runs_with_spans(manager, run_count=10, span_count=2)
    guard = _make_guard(manager)

    guard.enforce(
        ctx=_make_ctx(),
        product=PRODUCT,
        slot_range=(500, 600),
        slot_group="group-a",
    )

    assert guard.last_metrics is not None
    summary = guard.last_metrics.as_summary()
    assert summary["resume_guard_runs_enumerated"] == 10
    assert summary["resume_guard_spans_scanned"] == 2
    assert summary["resume_guard_enforce_duration_s"] > 0.0


@pytest.mark.unit
def test_populated_product_counts_runs_and_spans_legacy_path(tmp_path: Path) -> None:
    manager = _make_manager(tmp_path)
    _seed_completed_runs_with_spans(manager, run_count=5, span_count=3)
    guard = _make_guard(manager)

    guard.enforce(
        ctx=_make_ctx(resume_existing=True),
        product=PRODUCT,
    )

    assert guard.last_metrics is not None
    summary = guard.last_metrics.as_summary()
    assert summary["resume_guard_runs_enumerated"] == 5
    assert summary["resume_guard_spans_scanned"] == 3
    assert summary["resume_guard_enforce_duration_s"] >= 0.0


@pytest.mark.unit
def test_metrics_emitted_even_when_enforce_raises(tmp_path: Path) -> None:
    manager = _make_manager(tmp_path)
    manager.record_run_started(
        product=PRODUCT,
        run_id="orphan-run",
        output_path="file:///tmp/orphan-run",
        output_format="zarr",
        size=1,
        meta={"plugin": PLUGIN},
    )
    guard = _make_guard(manager)

    with pytest.raises(ResumeConflictError):
        guard.enforce(ctx=_make_ctx(), product=PRODUCT)

    assert guard.last_metrics is not None
    summary = guard.last_metrics.as_summary()
    assert summary["resume_guard_runs_enumerated"] == 1
    assert summary["resume_guard_enforce_duration_s"] >= 0.0
