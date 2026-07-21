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

from unittest.mock import MagicMock

import pytest

from firecube.ingestor.errors import ResumeConflictError
from firecube.ingestor.runtime.resume_guard import ResumeGuard


def _make_ctx(*, force_reingest: bool = False, **options):
    ctx = MagicMock()
    ctx.force_reingest = force_reingest
    ctx.option.side_effect = lambda name, default=None: options.get(name, default)
    return ctx


def _make_span(*, ranges=None, group: str | None = None, key: str = "span-1"):
    span = MagicMock()
    span.key = key
    span.meta = {"plugin": "test_product"}
    if group is not None:
        span.meta["group"] = group
    span.record = {"span": {"time_index_ranges": ranges or []}}
    return span


def _make_guard(spans):
    chunk_manager = MagicMock()
    chunk_manager.list_runs.return_value = []
    chunk_manager.list_chunks.return_value = spans
    return ResumeGuard(
        plugin_name="test_product",
        chunk_manager=chunk_manager,
        log=MagicMock(),
        slice_meta_keys=(),
    )


@pytest.mark.unit
def test_completed_span_overlapping_indices_blocks():
    guard = _make_guard([_make_span(ranges=[[0, 99]])])
    with pytest.raises(ResumeConflictError, match="Completed spans overlap"):
        guard.enforce(ctx=_make_ctx(), product="P", slot_range=(50, 150), slot_group=None)


@pytest.mark.unit
def test_completed_span_disjoint_indices_allows():
    guard = _make_guard([_make_span(ranges=[[0, 99]])])
    guard.enforce(ctx=_make_ctx(), product="P", slot_range=(100, 200), slot_group=None)


@pytest.mark.unit
def test_boundary_inclusive_end_overlaps_halfopen_start():
    guard = _make_guard([_make_span(ranges=[[0, 100]])])
    with pytest.raises(ResumeConflictError):
        guard.enforce(ctx=_make_ctx(), product="P", slot_range=(100, 200), slot_group=None)


@pytest.mark.unit
def test_completed_span_disjoint_groups_allows():
    guard = _make_guard([_make_span(ranges=[[0, 99]], group="A")])
    guard.enforce(ctx=_make_ctx(), product="P", slot_range=(0, 100), slot_group="B")


@pytest.mark.unit
def test_completed_span_omitted_slot_group_checks_all():
    guard = _make_guard([_make_span(ranges=[[0, 99]], group="A")])
    with pytest.raises(ResumeConflictError):
        guard.enforce(ctx=_make_ctx(), product="P", slot_range=(0, 100), slot_group=None)


@pytest.mark.unit
def test_force_reingest_bypasses_completed_span_check():
    guard = _make_guard([_make_span(ranges=[[0, 99]])])
    guard.enforce(
        ctx=_make_ctx(force_reingest=True), product="P", slot_range=(0, 100), slot_group=None
    )


@pytest.mark.unit
def test_resume_existing_does_not_bypass_completed_span_check():
    guard = _make_guard([_make_span(ranges=[[0, 99]])])
    with pytest.raises(ResumeConflictError):
        guard.enforce(
            ctx=_make_ctx(resume_existing=True), product="P", slot_range=(0, 100), slot_group=None
        )


@pytest.mark.unit
def test_multiple_completed_spans_partial_overlap_blocks():
    guard = _make_guard(
        [
            _make_span(ranges=[[0, 9]], key="span-1"),
            _make_span(ranges=[[100, 199]], key="span-2"),
            _make_span(ranges=[[300, 399]], key="span-3"),
        ]
    )
    with pytest.raises(ResumeConflictError, match="span-2"):
        guard.enforce(ctx=_make_ctx(), product="P", slot_range=(150, 250), slot_group=None)


@pytest.mark.unit
def test_no_completed_spans_allows():
    guard = _make_guard([])
    guard.enforce(ctx=_make_ctx(), product="P", slot_range=(0, 100), slot_group=None)


@pytest.mark.unit
def test_span_without_time_index_ranges_skipped():
    guard = _make_guard([_make_span(ranges=[])])
    guard.enforce(ctx=_make_ctx(), product="P", slot_range=(0, 100), slot_group=None)
