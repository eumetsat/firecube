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

import time
from unittest.mock import MagicMock

import pytest

from firecube.core.controlplane.types import RunInfo
from firecube.ingestor.errors import RangeOverlapError, ResumeConflictError
from firecube.ingestor.runtime.resume_guard import ResumeGuard


def _make_ctx(*, force_reingest: bool = False, **options):
    ctx = MagicMock()
    ctx.force_reingest = force_reingest
    ctx.option.side_effect = lambda name, default=None: options.get(name, default)
    return ctx


def _make_run(
    *,
    run_id: str = "run-123",
    slot_range: tuple[int, int] | None = (0, 100),
    slot_group: str | None = None,
    stale: bool = False,
) -> RunInfo:
    now = time.time()
    return RunInfo(
        product="P",
        run_id=run_id,
        status="running",
        run_dir=f"/tmp/{run_id}",
        run_uri=f"file:///tmp/{run_id}",
        started_at=now,
        updated_at=0.0 if stale else now,
        completed_at=None,
        events=1,
        parts=1,
        stale_threshold_s=3600,
        slot_range=slot_range,
        slot_group=slot_group,
    )


def _make_guard(runs: list[RunInfo]) -> ResumeGuard:
    chunk_manager = MagicMock()
    chunk_manager.list_runs.return_value = runs
    chunk_manager.list_chunks.return_value = []
    return ResumeGuard(
        plugin_name="test_product",
        chunk_manager=chunk_manager,
        log=MagicMock(),
        slice_meta_keys=(),
    )


@pytest.mark.unit
def test_non_terminal_disjoint_groups_no_conflict():
    guard = _make_guard([_make_run(slot_range=(0, 100), slot_group="A")])
    guard.enforce(ctx=_make_ctx(), product="P", slot_range=(0, 100), slot_group="B")


@pytest.mark.unit
def test_non_terminal_same_group_overlapping_range_blocks():
    guard = _make_guard([_make_run(slot_range=(0, 100), slot_group="A")])
    with pytest.raises(RangeOverlapError):
        guard.enforce(ctx=_make_ctx(), product="P", slot_range=(50, 150), slot_group="A")


@pytest.mark.unit
def test_non_terminal_omitted_group_blocks_specific_group():
    guard = _make_guard([_make_run(slot_range=(0, 100), slot_group=None)])
    with pytest.raises(RangeOverlapError):
        guard.enforce(ctx=_make_ctx(), product="P", slot_range=(50, 150), slot_group="A")


@pytest.mark.unit
def test_phase3_no_slot_group_preserves_behavior():
    guard = _make_guard([_make_run(slot_range=(0, 100), slot_group=None)])
    with pytest.raises(RangeOverlapError):
        guard.enforce(ctx=_make_ctx(), product="P", slot_range=(50, 150), slot_group=None)


@pytest.mark.unit
def test_non_terminal_stale_disjoint_group_allows():
    guard = _make_guard([_make_run(slot_range=(0, 100), slot_group="A", stale=True)])
    guard.enforce(ctx=_make_ctx(), product="P", slot_range=(0, 100), slot_group="B")


@pytest.mark.unit
def test_non_terminal_same_group_same_range_resume_allows():
    guard = _make_guard([_make_run(slot_range=(0, 100), slot_group="A")])
    guard.enforce(
        ctx=_make_ctx(resume_existing=True), product="P", slot_range=(0, 100), slot_group="A"
    )


@pytest.mark.unit
def test_non_terminal_same_group_same_range_no_resume_blocks():
    guard = _make_guard([_make_run(slot_range=(0, 100), slot_group="A")])
    with pytest.raises(ResumeConflictError, match="same slot_range"):
        guard.enforce(ctx=_make_ctx(), product="P", slot_range=(0, 100), slot_group="A")
