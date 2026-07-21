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

from unittest.mock import MagicMock, call, patch

import pytest

from firecube.core.controlplane.types import RunInfo
from firecube.ingestor.errors import ResumeConflictError
from firecube.ingestor.runtime.resume_guard import ResumeGuard


def _make_ctx(*, force_reingest: bool = False, **options):
    ctx = MagicMock()
    ctx.force_reingest = force_reingest
    ctx.option.side_effect = lambda name, default=None: options.get(name, default)
    return ctx


def _make_run(run_id: str, status: str = "running") -> RunInfo:
    return RunInfo(
        product="P",
        run_id=run_id,
        status=status,
        run_dir=f"/tmp/{run_id}",
        run_uri=f"file:///tmp/{run_id}",
        started_at=1.0,
        updated_at=1.0,
        completed_at=None,
        events=1,
        parts=1,
    )


def _make_guard(chunk_manager: MagicMock | None = None) -> ResumeGuard:
    return ResumeGuard(
        plugin_name="test_product",
        chunk_manager=chunk_manager or MagicMock(),
        log=MagicMock(),
        slice_meta_keys=(),
    )


def _make_span(*, meta=None):
    span = MagicMock()
    span.meta = meta or {"plugin": "test_product"}
    span.record = {"span": {}}
    return span


@pytest.mark.unit
def test_enforce_blocks_when_non_terminal_run_exists():
    chunk_manager = MagicMock()
    chunk_manager.list_runs.return_value = [_make_run("run-123")]
    guard = _make_guard(chunk_manager)

    with pytest.raises(ResumeConflictError, match="run-123"):
        guard.enforce(ctx=_make_ctx(), product="P")

    chunk_manager.list_chunks.assert_not_called()


@pytest.mark.unit
def test_enforce_non_terminal_error_includes_abandon_instruction():
    chunk_manager = MagicMock()
    chunk_manager.list_runs.return_value = [_make_run("run-123")]
    guard = _make_guard(chunk_manager)

    with pytest.raises(ResumeConflictError) as excinfo:
        guard.enforce(ctx=_make_ctx(), product="P")

    message = str(excinfo.value)
    assert "abandon" in message
    assert (
        'firecube chunks runs abandon --product-name P --run-id run-123 --reason "<reason>"'
        in message
    )


@pytest.mark.unit
def test_enforce_force_reingest_allows_non_terminal_run():
    chunk_manager = MagicMock()
    chunk_manager.list_runs.return_value = [_make_run("run-123")]
    chunk_manager.list_chunks.return_value = []
    guard = _make_guard(chunk_manager)

    guard.enforce(ctx=_make_ctx(force_reingest=True), product="P")

    guard.log.warning.assert_called_once()  # pyright: ignore[reportAttributeAccessIssue]
    chunk_manager.list_chunks.assert_called_once()


@pytest.mark.unit
def test_enforce_proceeds_when_no_non_terminal_runs_exist():
    chunk_manager = MagicMock()
    chunk_manager.list_runs.return_value = []
    chunk_manager.list_chunks.return_value = []
    guard = _make_guard(chunk_manager)

    guard.enforce(ctx=_make_ctx(), product="P")

    chunk_manager.list_runs.assert_called_once_with(product="P", non_terminal=True)
    chunk_manager.list_chunks.assert_called_once()


@pytest.mark.unit
def test_enforce_resume_existing_still_blocks_on_non_terminal_run():
    chunk_manager = MagicMock()
    chunk_manager.list_runs.return_value = [_make_run("run-123")]
    guard = _make_guard(chunk_manager)

    with pytest.raises(ResumeConflictError, match="run-123"):
        guard.enforce(ctx=_make_ctx(resume_existing=True), product="P")

    chunk_manager.list_chunks.assert_not_called()


@pytest.mark.unit
def test_enforce_fresh_product_without_spans_or_runs_proceeds():
    chunk_manager = MagicMock()
    chunk_manager.list_runs.return_value = []
    chunk_manager.list_chunks.return_value = []
    guard = _make_guard(chunk_manager)

    guard.enforce(ctx=_make_ctx(), product="P")

    chunk_manager.list_runs.assert_called_once_with(product="P", non_terminal=True)
    chunk_manager.list_chunks.assert_called_once_with(
        product="P",
        chunk_type="span",
        include_replaced=False,
        meta={"plugin": "test_product"},
    )


@pytest.mark.unit
def test_enforce_resume_existing_allows_when_overlap_query_finds_no_spans():
    chunk_manager = MagicMock()
    chunk_manager.list_runs.return_value = []
    chunk_manager.list_chunks.return_value = []
    guard = _make_guard(chunk_manager)

    guard.enforce(
        ctx=_make_ctx(resume_existing=True),
        product="P",
        slice_meta={"time_min": "2024-01-01T00:00:00Z", "time_max": "2024-01-02T00:00:00Z"},
    )

    assert chunk_manager.list_chunks.call_args_list == [
        call(
            product="P",
            chunk_type="span",
            include_replaced=False,
            meta={"plugin": "test_product"},
            time_overlaps=("2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z"),
        ),
        call(
            product="P",
            chunk_type="span",
            include_replaced=False,
            meta={"plugin": "test_product"},
        ),
    ]


@pytest.mark.unit
def test_enforce_existing_spans_without_resume_or_force_raises_conflict():
    chunk_manager = MagicMock()
    chunk_manager.list_runs.return_value = []
    chunk_manager.list_chunks.return_value = [_make_span()]
    guard = _make_guard(chunk_manager)

    with pytest.raises(ResumeConflictError, match="Existing entries for product 'P'"):
        guard.enforce(ctx=_make_ctx(), product="P")


@pytest.mark.unit
def test_enforce_force_reingest_bypasses_existing_span_conflict():
    chunk_manager = MagicMock()
    chunk_manager.list_runs.return_value = []
    chunk_manager.list_chunks.return_value = [_make_span()]
    guard = _make_guard(chunk_manager)

    guard.enforce(ctx=_make_ctx(force_reingest=True), product="P")


@pytest.mark.unit
def test_enforce_with_time_bounds_uses_time_overlaps_query():
    chunk_manager = MagicMock()
    chunk_manager.list_runs.return_value = []
    chunk_manager.list_chunks.return_value = []
    guard = _make_guard(chunk_manager)

    guard.enforce(
        ctx=_make_ctx(),
        product="P",
        slice_meta={
            "time_min": "2024-03-01T00:00:00Z",
            "time_max": "2024-03-31T23:59:59Z",
            "test_region": "euro",
        },
    )

    assert chunk_manager.list_chunks.call_args_list == [
        call(
            product="P",
            chunk_type="span",
            include_replaced=False,
            meta={"plugin": "test_product"},
            time_overlaps=("2024-03-01T00:00:00Z", "2024-03-31T23:59:59Z"),
        ),
        call(
            product="P",
            chunk_type="span",
            include_replaced=False,
            meta={"plugin": "test_product"},
        ),
    ]


@pytest.mark.unit
def test_enforce_includes_legacy_spans_in_time_bounded_check():
    chunk_manager = MagicMock()
    chunk_manager.list_runs.return_value = []

    timed_span = _make_span(
        meta={
            "plugin": "test_product",
            "time_min": "2024-01-01T00:00:00Z",
            "time_max": "2024-06-01T00:00:00Z",
        }
    )
    timed_span.key = "span-timed-1"

    legacy_span = _make_span(meta={"plugin": "test_product"})
    legacy_span.key = "span-legacy-1"

    def list_chunks_side_effect(**kwargs):
        if kwargs.get("time_overlaps"):
            return [timed_span]
        return [timed_span, legacy_span]

    chunk_manager.list_chunks.side_effect = list_chunks_side_effect
    guard = _make_guard(chunk_manager)

    with pytest.raises(ResumeConflictError):
        guard.enforce(
            ctx=_make_ctx(),
            product="P",
            slice_meta={
                "time_min": "2024-02-01T00:00:00Z",
                "time_max": "2024-05-01T00:00:00Z",
            },
        )


@pytest.mark.unit
def test_enforce_excludes_legacy_spans_from_other_plugins():
    chunk_manager = MagicMock()
    chunk_manager.list_runs.return_value = []
    chunk_manager.list_chunks.return_value = []
    guard = _make_guard(chunk_manager)

    guard.enforce(
        ctx=_make_ctx(),
        product="P",
        slice_meta={
            "time_min": "2024-02-01T00:00:00Z",
            "time_max": "2024-05-01T00:00:00Z",
        },
    )

    assert chunk_manager.list_chunks.call_args_list == [
        call(
            product="P",
            chunk_type="span",
            include_replaced=False,
            meta={"plugin": "test_product"},
            time_overlaps=("2024-02-01T00:00:00Z", "2024-05-01T00:00:00Z"),
        ),
        call(
            product="P",
            chunk_type="span",
            include_replaced=False,
            meta={"plugin": "test_product"},
        ),
    ]


@pytest.mark.unit
def test_enforce_without_time_bounds_falls_back_to_all_spans_query():
    chunk_manager = MagicMock()
    chunk_manager.list_runs.return_value = []
    chunk_manager.list_chunks.return_value = []
    guard = _make_guard(chunk_manager)

    guard.enforce(ctx=_make_ctx(), product="P", slice_meta={"test_region": "euro"})

    chunk_manager.list_chunks.assert_called_once_with(
        product="P",
        chunk_type="span",
        include_replaced=False,
        meta={"plugin": "test_product"},
    )


@pytest.mark.unit
def test_enforce_no_time_slice_meta_unchanged():
    chunk_manager = MagicMock()
    chunk_manager.list_runs.return_value = []
    legacy_span = _make_span(meta={"plugin": "test_product"})
    legacy_span.key = "span-legacy-1"
    chunk_manager.list_chunks.return_value = [legacy_span]
    guard = _make_guard(chunk_manager)

    with pytest.raises(ResumeConflictError):
        guard.enforce(ctx=_make_ctx(), product="P", slice_meta={})

    assert chunk_manager.list_chunks.call_count == 1


@pytest.mark.unit
def test_enforce_validate_zarr_remains_opt_in():
    chunk_manager = MagicMock()
    chunk_manager.list_runs.return_value = []
    chunk_manager.list_chunks.return_value = [_make_span()]
    guard = _make_guard(chunk_manager)

    with (
        patch.object(ResumeGuard, "_run_optional_validation", return_value=True) as validate,
        pytest.raises(ResumeConflictError),
    ):
        guard.enforce(ctx=_make_ctx(validate_zarr=False), product="P")

    validate.assert_not_called()


@pytest.mark.unit
def test_enforce_logs_proceed_fresh_decision():
    chunk_manager = MagicMock()
    chunk_manager.list_runs.return_value = []
    chunk_manager.list_chunks.return_value = []
    mock_log = MagicMock()
    guard = ResumeGuard(
        plugin_name="test",
        chunk_manager=chunk_manager,
        log=mock_log,
        slice_meta_keys=[],
    )

    guard.enforce(ctx=_make_ctx(), product="P")

    debug_calls = [str(call) for call in mock_log.debug.call_args_list]
    assert any("PROCEED_FRESH" in call or "proceed_fresh" in call for call in debug_calls), (
        f"Expected PROCEED_FRESH in debug log, got: {debug_calls}"
    )


@pytest.mark.unit
def test_enforce_logs_block_stale_run_decision():
    chunk_manager = MagicMock()
    chunk_manager.list_runs.return_value = [_make_run("r-stale", status="started")]
    chunk_manager.list_chunks.return_value = []
    mock_log = MagicMock()
    guard = ResumeGuard(
        plugin_name="test",
        chunk_manager=chunk_manager,
        log=mock_log,
        slice_meta_keys=[],
    )

    with pytest.raises(ResumeConflictError):
        guard.enforce(ctx=_make_ctx(), product="P")

    debug_calls = [str(call) for call in mock_log.debug.call_args_list]
    assert any("BLOCK_STALE_RUN" in call or "block_stale_run" in call for call in debug_calls), (
        f"Expected BLOCK_STALE_RUN in debug log, got: {debug_calls}"
    )


@pytest.mark.unit
def test_enforce_logs_block_conflict_decision():
    chunk_manager = MagicMock()
    chunk_manager.list_runs.return_value = []
    chunk_manager.list_chunks.return_value = [_make_span(meta={"plugin": "test"})]
    mock_log = MagicMock()
    guard = ResumeGuard(
        plugin_name="test",
        chunk_manager=chunk_manager,
        log=mock_log,
        slice_meta_keys=[],
    )

    with pytest.raises(ResumeConflictError):
        guard.enforce(ctx=_make_ctx(), product="P")

    debug_calls = [str(call) for call in mock_log.debug.call_args_list]
    assert any("BLOCK_CONFLICT" in call or "block_conflict" in call for call in debug_calls), (
        f"Expected BLOCK_CONFLICT in debug log, got: {debug_calls}"
    )


@pytest.mark.unit
def test_enforce_external_api_unchanged():
    chunk_manager = MagicMock()
    chunk_manager.list_runs.return_value = []
    chunk_manager.list_chunks.return_value = []
    guard = ResumeGuard(
        plugin_name="test",
        chunk_manager=chunk_manager,
        log=MagicMock(),
        slice_meta_keys=[],
    )

    result = guard.enforce(ctx=_make_ctx(), product="P")

    assert result is None
