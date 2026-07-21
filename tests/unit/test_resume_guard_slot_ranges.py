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
    run_id: str = "run-123",
    *,
    slot_range: tuple[int, int] | None = None,
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
    )


def _make_guard(chunk_manager: MagicMock) -> ResumeGuard:
    return ResumeGuard(
        plugin_name="test_product",
        chunk_manager=chunk_manager,
        log=MagicMock(),
        slice_meta_keys=(),
    )


def _make_chunk_manager(runs: list[RunInfo]) -> MagicMock:
    chunk_manager = MagicMock()
    chunk_manager.list_runs.return_value = runs
    chunk_manager.list_chunks.return_value = []
    return chunk_manager


@pytest.mark.unit
def test_no_active_runs_allows():
    chunk_manager = _make_chunk_manager([])
    guard = _make_guard(chunk_manager)

    guard.enforce(ctx=_make_ctx(), product="P", slot_range=(0, 100), slot_group=None)

    chunk_manager.list_chunks.assert_called_once()


@pytest.mark.unit
def test_same_range_no_resume_blocks():
    chunk_manager = _make_chunk_manager([_make_run(slot_range=(0, 100))])
    guard = _make_guard(chunk_manager)

    with pytest.raises(ResumeConflictError, match="same slot_range"):
        guard.enforce(ctx=_make_ctx(), product="P", slot_range=(0, 100), slot_group=None)

    chunk_manager.list_chunks.assert_not_called()


@pytest.mark.unit
def test_same_range_with_resume_allows():
    chunk_manager = _make_chunk_manager([_make_run(slot_range=(0, 100))])
    guard = _make_guard(chunk_manager)

    guard.enforce(
        ctx=_make_ctx(resume_existing=True), product="P", slot_range=(0, 100), slot_group=None
    )

    guard.log.warning.assert_called_once()  # pyright: ignore[reportAttributeAccessIssue]
    chunk_manager.list_chunks.assert_called_once()


@pytest.mark.unit
def test_same_range_with_force_reingest_allows():
    chunk_manager = _make_chunk_manager([_make_run(slot_range=(0, 100))])
    guard = _make_guard(chunk_manager)

    guard.enforce(
        ctx=_make_ctx(force_reingest=True), product="P", slot_range=(0, 100), slot_group=None
    )

    guard.log.warning.assert_called_once()  # pyright: ignore[reportAttributeAccessIssue]
    chunk_manager.list_chunks.assert_not_called()


@pytest.mark.unit
def test_overlapping_different_range_blocks():
    chunk_manager = _make_chunk_manager([_make_run(slot_range=(0, 100))])
    guard = _make_guard(chunk_manager)

    with pytest.raises(RangeOverlapError, match="overlaps"):
        guard.enforce(ctx=_make_ctx(), product="P", slot_range=(50, 150), slot_group=None)

    chunk_manager.list_chunks.assert_not_called()


@pytest.mark.unit
def test_disjoint_ranges_allowed():
    chunk_manager = _make_chunk_manager([_make_run(slot_range=(0, 100))])
    guard = _make_guard(chunk_manager)

    guard.enforce(ctx=_make_ctx(), product="P", slot_range=(100, 200), slot_group=None)

    chunk_manager.list_chunks.assert_called_once()


@pytest.mark.unit
def test_non_range_active_blocks_range_invocation():
    chunk_manager = _make_chunk_manager([_make_run(slot_range=None)])
    guard = _make_guard(chunk_manager)

    with pytest.raises(ResumeConflictError, match="Non-range run"):
        guard.enforce(ctx=_make_ctx(), product="P", slot_range=(0, 100), slot_group=None)

    chunk_manager.list_chunks.assert_not_called()


@pytest.mark.unit
def test_stale_non_range_allows_range_invocation():
    chunk_manager = _make_chunk_manager([_make_run(slot_range=None, stale=True)])
    guard = _make_guard(chunk_manager)

    guard.enforce(ctx=_make_ctx(), product="P", slot_range=(0, 100), slot_group=None)

    chunk_manager.list_chunks.assert_called_once()


@pytest.mark.unit
def test_single_pod_mode_blocks_as_before():
    chunk_manager = _make_chunk_manager([_make_run(slot_range=None)])
    guard = _make_guard(chunk_manager)

    with pytest.raises(ResumeConflictError, match="Non-terminal run"):
        guard.enforce(ctx=_make_ctx(), product="P", slot_range=None)

    chunk_manager.list_chunks.assert_not_called()


@pytest.mark.unit
def test_back_to_back_ranges_are_disjoint():
    chunk_manager = _make_chunk_manager([_make_run(slot_range=(0, 100))])
    guard = _make_guard(chunk_manager)

    guard.enforce(ctx=_make_ctx(), product="P", slot_range=(100, 200), slot_group=None)

    chunk_manager.list_chunks.assert_called_once()


@pytest.mark.unit
def test_single_slot_adjacent_ranges():
    chunk_manager = _make_chunk_manager([_make_run(slot_range=(0, 1))])
    guard = _make_guard(chunk_manager)

    guard.enforce(ctx=_make_ctx(), product="P", slot_range=(1, 2), slot_group=None)

    chunk_manager.list_chunks.assert_called_once()
