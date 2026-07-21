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

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from firecube.core.controlplane import ChunkManager, SpanCoverage
from firecube.core.controlplane.types import EVENT_REPLACEMENT_COMMITTED
from firecube.core.errors import ManifestError, StorageError
from firecube.ingestor.runtime.recording import SpanRecorder
from firecube.ingestor.types.context import (
    IngestResult,
    OutputPaths,
    ResultMetrics,
    RuntimeFlags,
    RuntimeIdentity,
    RuntimeIngestContext,
)
from firecube.ingestor.types.result_metrics import PipelineMetrics, StorageMetrics
from tests.helpers.storage import make_test_binding

pytestmark = pytest.mark.integration


def _make_runtime_ctx(*, run_id: str, target: str, force_reingest: bool) -> RuntimeIngestContext:
    return RuntimeIngestContext(
        source="source",
        target=target,
        output_format="zarr",
        options={"run_id": run_id},
        run_id=run_id,
        identity=RuntimeIdentity(run_id=run_id),
        flags=RuntimeFlags(force_reingest=force_reingest),
    )


def _record_completed_span_run(
    manager: ChunkManager,
    *,
    product: str,
    run_id: str,
    group: str,
    time_min: str,
    time_max: str,
) -> str:
    output_path = f"{manager.base_uri.rstrip('/')}/{product}"
    meta = {"plugin": "test_product", "group": group, "time_min": time_min, "time_max": time_max}
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
        batch_id="batch-001",
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
    return f"span_{run_id}_batch-001_{group}"


def _start_run(manager: ChunkManager, *, product: str, run_id: str, output_path: str) -> None:
    manager.record_run_started(
        product=product,
        run_id=run_id,
        output_path=output_path,
        output_format="zarr",
        size=0,
        meta={"plugin": "test_product"},
    )


def _make_result(*, output_path: str, group: str, time_min: str, time_max: str) -> IngestResult:
    return IngestResult(
        outputs=OutputPaths(primary=output_path),
        output_format="zarr",
        metrics=ResultMetrics(
            storage=StorageMetrics(bytes=123),
            pipeline=PipelineMetrics(
                coverage=[
                    SpanCoverage(
                        group=group,
                        arrays=[f"{group}/FWI"],
                        time_index_ranges=[[0, 1]],
                        time_min=time_min,
                        time_max=time_max,
                    )
                ]
            ),
        ),
    )


def _make_empty_coverage_result(*, output_path: str) -> IngestResult:
    return IngestResult(
        outputs=OutputPaths(primary=output_path),
        output_format="zarr",
        metrics=ResultMetrics(
            storage=StorageMetrics(bytes=123),
            pipeline=PipelineMetrics(coverage=[]),
        ),
    )


def _make_run_level_result(
    *, output_path: str, group: str, time_min: str, time_max: str
) -> IngestResult:
    """An ``IngestResult`` shaped like real run-level metrics.

    ``merge_batch_metrics`` puts coverage under ``zarr.coverage`` and
    ``finalize`` replaces ``pipeline`` with the run summary (no coverage). The
    dict is coerced by ``IngestResult`` exactly as the engine produces it, so
    ``pipeline.coverage`` ends up empty and the only real coverage lives under
    ``zarr``.
    """
    return IngestResult(
        outputs=OutputPaths(primary=output_path),
        output_format="zarr",
        metrics={
            "storage": {"bytes": 123},
            "zarr": {
                "coverage": [
                    {
                        "group": group,
                        "arrays": [f"{group}/FWI"],
                        "time_index_ranges": [[0, 1]],
                        "time_min": time_min,
                        "time_max": time_max,
                    }
                ]
            },
            "pipeline": {"duration_total_s": 1.0},
        },
    )


def _read_run_wal_events(temp_workspace: Path, *, product: str, run_id: str) -> list[dict]:
    run_dir = temp_workspace / product / ".firecube" / "runs" / run_id
    events: list[dict] = []
    for path in sorted(run_dir.glob("events-*.jsonl")):
        events.extend(
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
    return events


def _current_state(manager: ChunkManager, *, product: str) -> dict[str, dict]:
    return manager.repo._load_current_state(product)


def _replacement_events(temp_workspace: Path, *, product: str, run_id: str) -> list[dict]:
    return [
        event
        for event in _read_run_wal_events(temp_workspace, product=product, run_id=run_id)
        if event["event_type"] == EVENT_REPLACEMENT_COMMITTED
    ]


def test_force_reingest_with_empty_coverage_raises_and_does_not_commit_replacement(
    temp_workspace,
):
    product = "test_product"
    output_path = str(temp_workspace / product)
    manager = ChunkManager(binding=make_test_binding(temp_workspace), workspace=temp_workspace)
    recorder = SpanRecorder(manager)

    prior_key = _record_completed_span_run(
        manager,
        product=product,
        run_id="run-old",
        group="F024",
        time_min="2024-01-01T00:00:00Z",
        time_max="2024-01-02T00:00:00Z",
    )
    _start_run(manager, product=product, run_id="run-new", output_path=output_path)

    with pytest.raises(ManifestError, match="new coverage is empty"):
        recorder.register_run(
            ctx=_make_runtime_ctx(run_id="run-new", target=output_path, force_reingest=True),
            result=_make_empty_coverage_result(output_path=output_path),
            run_id="run-new",
            product=product,
            slice_meta={"plugin": "test_product", "group": "F024"},
        )

    current = _current_state(manager, product=product)
    assert current[prior_key]["status"] == "active"
    assert _replacement_events(temp_workspace, product=product, run_id="run-new") == []


def test_force_reingest_with_run_level_zarr_coverage_commits_replacement(temp_workspace):
    """Regression: a force_reingest run with run-level ``zarr.coverage`` and
    spans already recorded per-batch (``record_spans=False``) must commit the
    replacement, not raise.

    This reproduces the OPERA-SEVIRI-NORDLIS benchmark failure that was reported
    as a slot-range/parallel bug. The real cause is general: run-level coverage
    lives under ``zarr.coverage`` (not ``pipeline.coverage``), and ``finalize``
    always leaves ``record_spans=False`` because batches recorded their spans.
    The guard previously fired on ``not (record_spans and coverage)``, erasing
    a legitimate replacement; it must fire only when coverage is genuinely empty.
    """
    product = "test_product"
    output_path = str(temp_workspace / product)
    manager = ChunkManager(binding=make_test_binding(temp_workspace), workspace=temp_workspace)
    recorder = SpanRecorder(manager)

    prior_key = _record_completed_span_run(
        manager,
        product=product,
        run_id="run-old",
        group="F024",
        time_min="2024-01-01T00:00:00Z",
        time_max="2024-01-02T00:00:00Z",
    )
    _start_run(manager, product=product, run_id="run-new", output_path=output_path)

    recorder.register_run(
        ctx=_make_runtime_ctx(run_id="run-new", target=output_path, force_reingest=True),
        result=_make_run_level_result(
            output_path=output_path,
            group="F024",
            time_min="2024-01-01T00:00:00Z",
            time_max="2024-01-02T00:00:00Z",
        ),
        run_id="run-new",
        product=product,
        slice_meta={"plugin": "test_product", "group": "F024"},
        record_spans=False,
    )

    # The replacement is committed and the prior span is no longer active.
    assert _replacement_events(temp_workspace, product=product, run_id="run-new") != []
    current = _current_state(manager, product=product)
    assert current[prior_key]["status"] != "active"


def test_force_reingest_empty_coverage_still_raises_when_spans_not_recorded(temp_workspace):
    """Guard preservation: with no new coverage at all, force_reingest must still
    refuse to erase prior active coverage — even when ``record_spans=False``.

    Loosening the guard to ``not coverage`` must not drop this protection.
    """
    product = "test_product"
    output_path = str(temp_workspace / product)
    manager = ChunkManager(binding=make_test_binding(temp_workspace), workspace=temp_workspace)
    recorder = SpanRecorder(manager)

    prior_key = _record_completed_span_run(
        manager,
        product=product,
        run_id="run-old",
        group="F024",
        time_min="2024-01-01T00:00:00Z",
        time_max="2024-01-02T00:00:00Z",
    )
    _start_run(manager, product=product, run_id="run-new", output_path=output_path)

    with pytest.raises(ManifestError, match="new coverage is empty"):
        recorder.register_run(
            ctx=_make_runtime_ctx(run_id="run-new", target=output_path, force_reingest=True),
            result=_make_empty_coverage_result(output_path=output_path),
            run_id="run-new",
            product=product,
            slice_meta={"plugin": "test_product", "group": "F024"},
            record_spans=False,
        )

    current = _current_state(manager, product=product)
    assert current[prior_key]["status"] == "active"
    assert _replacement_events(temp_workspace, product=product, run_id="run-new") == []


def test_force_reingest_first_time_no_prior_spans_does_not_raise(temp_workspace):
    product = "test_product"
    output_path = str(temp_workspace / product)
    manager = ChunkManager(binding=make_test_binding(temp_workspace), workspace=temp_workspace)
    recorder = SpanRecorder(manager)
    _start_run(manager, product=product, run_id="run-new", output_path=output_path)

    recorder.register_run(
        ctx=_make_runtime_ctx(run_id="run-new", target=output_path, force_reingest=True),
        result=_make_result(
            output_path=output_path,
            group="F024",
            time_min="2024-01-01T00:00:00Z",
            time_max="2024-01-02T00:00:00Z",
        ),
        run_id="run-new",
        product=product,
        slice_meta={"plugin": "test_product", "group": "F024"},
    )

    current = _current_state(manager, product=product)
    assert current["span_run-new_single_F024"]["status"] == "active"


def test_force_reingest_with_coverage_records_spans_before_replacement(temp_workspace):
    product = "test_product"
    output_path = str(temp_workspace / product)
    manager = ChunkManager(binding=make_test_binding(temp_workspace), workspace=temp_workspace)
    recorder = SpanRecorder(manager)

    _record_completed_span_run(
        manager,
        product=product,
        run_id="run-old",
        group="F024",
        time_min="2024-01-01T00:00:00Z",
        time_max="2024-01-02T00:00:00Z",
    )
    _start_run(manager, product=product, run_id="run-new", output_path=output_path)

    recorder.register_run(
        ctx=_make_runtime_ctx(run_id="run-new", target=output_path, force_reingest=True),
        result=_make_result(
            output_path=output_path,
            group="F024",
            time_min="2024-01-01T00:00:00Z",
            time_max="2024-01-02T00:00:00Z",
        ),
        run_id="run-new",
        product=product,
        slice_meta={"plugin": "test_product", "group": "F024"},
    )

    event_types = [
        event["event_type"]
        for event in _read_run_wal_events(temp_workspace, product=product, run_id="run-new")
    ]
    # Replacement must commit after new span coverage exists in the WAL; the
    # previous order could erase prior coverage if no replacement span followed.
    assert event_types.index("span_committed") < event_types.index(EVENT_REPLACEMENT_COMMITTED)


def test_crash_preserves_prior_spans(temp_workspace):
    product = "test_product"
    output_path = str(temp_workspace / product)
    manager = ChunkManager(binding=make_test_binding(temp_workspace), workspace=temp_workspace)
    recorder = SpanRecorder(manager)

    prior_key = _record_completed_span_run(
        manager,
        product=product,
        run_id="run-old",
        group="F024",
        time_min="2024-01-01T00:00:00Z",
        time_max="2024-01-02T00:00:00Z",
    )
    _start_run(manager, product=product, run_id="run-failed", output_path=output_path)

    recorder.register_run_failure(
        run_id="run-failed",
        product=product,
        output_path=output_path,
        output_format="zarr",
        slice_meta={"plugin": "test_product"},
        error="boom",
    )
    manager.close()
    manager = ChunkManager(binding=make_test_binding(temp_workspace), workspace=temp_workspace)

    current = _current_state(manager, product=product)
    assert current[prior_key]["status"] == "active"
    assert not any(
        event["event_type"] == EVENT_REPLACEMENT_COMMITTED
        for event in _read_run_wal_events(temp_workspace, product=product, run_id="run-failed")
    )


def test_success_replaces_prior_spans(temp_workspace):
    product = "test_product"
    output_path = str(temp_workspace / product)
    manager = ChunkManager(binding=make_test_binding(temp_workspace), workspace=temp_workspace)
    recorder = SpanRecorder(manager)

    prior_key = _record_completed_span_run(
        manager,
        product=product,
        run_id="run-old",
        group="F024",
        time_min="2024-01-01T00:00:00Z",
        time_max="2024-01-02T00:00:00Z",
    )
    other_key = _record_completed_span_run(
        manager,
        product=product,
        run_id="run-other",
        group="F048",
        time_min="2024-02-01T00:00:00Z",
        time_max="2024-02-02T00:00:00Z",
    )
    _start_run(manager, product=product, run_id="run-new", output_path=output_path)

    recorder.register_run(
        ctx=_make_runtime_ctx(run_id="run-new", target=output_path, force_reingest=True),
        result=_make_result(
            output_path=output_path,
            group="F024",
            time_min="2024-01-01T00:00:00Z",
            time_max="2024-01-02T00:00:00Z",
        ),
        run_id="run-new",
        product=product,
        slice_meta={"plugin": "test_product", "group": "F024"},
    )
    manager.close()
    manager = ChunkManager(binding=make_test_binding(temp_workspace), workspace=temp_workspace)
    manager.rebuild_snapshot(product)

    current = _current_state(manager, product=product)
    assert current[prior_key]["status"] == "replaced"
    assert current[prior_key]["replaced_by"] == "run-new"
    assert current[other_key]["status"] == "active"
    assert current["span_run-new_single_F024"]["status"] == "active"


def test_replacement_idempotent(temp_workspace):
    product = "test_product"
    output_path = str(temp_workspace / product)
    manager = ChunkManager(binding=make_test_binding(temp_workspace), workspace=temp_workspace)
    recorder = SpanRecorder(manager)

    _record_completed_span_run(
        manager,
        product=product,
        run_id="run-old",
        group="F024",
        time_min="2024-01-01T00:00:00Z",
        time_max="2024-01-02T00:00:00Z",
    )
    _start_run(manager, product=product, run_id="run-new", output_path=output_path)

    result = _make_result(
        output_path=output_path,
        group="F024",
        time_min="2024-01-01T00:00:00Z",
        time_max="2024-01-02T00:00:00Z",
    )
    ctx = _make_runtime_ctx(run_id="run-new", target=output_path, force_reingest=True)

    recorder.register_run(
        ctx=ctx,
        result=result,
        run_id="run-new",
        product=product,
        slice_meta={"plugin": "test_product", "group": "F024"},
    )
    recorder.register_run(
        ctx=ctx,
        result=result,
        run_id="run-new",
        product=product,
        slice_meta={"plugin": "test_product", "group": "F024"},
    )

    replacement_events = [
        event
        for event in _read_run_wal_events(temp_workspace, product=product, run_id="run-new")
        if event["event_type"] == EVENT_REPLACEMENT_COMMITTED
    ]
    assert len(replacement_events) == 1


def test_coverage_dedup(temp_workspace):
    product = "test_product"
    manager = ChunkManager(binding=make_test_binding(temp_workspace), workspace=temp_workspace)

    _record_completed_span_run(
        manager,
        product=product,
        run_id="run-old",
        group="F024",
        time_min="2024-01-01T00:00:00Z",
        time_max="2024-01-02T00:00:00Z",
    )
    _record_completed_span_run(
        manager,
        product=product,
        run_id="run-new",
        group="F024",
        time_min="2024-01-01T00:00:00Z",
        time_max="2024-01-02T00:00:00Z",
    )

    assert not any(
        event["event_type"] == EVENT_REPLACEMENT_COMMITTED
        for event in _read_run_wal_events(temp_workspace, product=product, run_id="run-new")
    )

    current = _current_state(manager, product=product)
    active_spans_for_slice = [
        record
        for record in current.values()
        if record.get("type") == "span"
        and record.get("status") == "active"
        and (record.get("meta") or {}).get("group") == "F024"
    ]
    assert len(active_spans_for_slice) == 2

    summary = manager.time_coverage_summary(product=product)

    assert len(summary) == 1
    assert summary[0]["group"] == "F024"
    assert summary[0]["span_count"] == 1
    assert summary[0]["time_min"] == "2024-01-01T00:00:00Z"
    assert summary[0]["time_max"] == "2024-01-02T00:00:00Z"


def test_replacement_only_at_terminal(temp_workspace):
    product = "test_product"
    output_path = str(temp_workspace / product)
    manager = ChunkManager(binding=make_test_binding(temp_workspace), workspace=temp_workspace)
    recorder = SpanRecorder(manager)

    prior_key = _record_completed_span_run(
        manager,
        product=product,
        run_id="run-old",
        group="F024",
        time_min="2024-01-01T00:00:00Z",
        time_max="2024-01-02T00:00:00Z",
    )
    _start_run(manager, product=product, run_id="run-new", output_path=output_path)

    with (
        patch.object(
            ChunkManager,
            "record_run_terminal",
            side_effect=StorageError("simulated terminal write failure"),
        ),
        pytest.raises(StorageError, match="simulated terminal write failure"),
    ):
        recorder.register_run(
            ctx=_make_runtime_ctx(run_id="run-new", target=output_path, force_reingest=True),
            result=_make_result(
                output_path=output_path,
                group="F024",
                time_min="2024-01-01T00:00:00Z",
                time_max="2024-01-02T00:00:00Z",
            ),
            run_id="run-new",
            product=product,
            slice_meta={"plugin": "test_product", "group": "F024"},
        )

    manager.close()
    manager = ChunkManager(binding=make_test_binding(temp_workspace), workspace=temp_workspace)

    current = _current_state(manager, product=product)
    assert current[prior_key]["status"] == "active"
    assert _replacement_events(temp_workspace, product=product, run_id="run-new") == []


def test_idempotent_replay(temp_workspace):
    product = "test_product"
    output_path = str(temp_workspace / product)
    manager = ChunkManager(binding=make_test_binding(temp_workspace), workspace=temp_workspace)
    recorder = SpanRecorder(manager)

    prior_key = _record_completed_span_run(
        manager,
        product=product,
        run_id="run-old",
        group="F024",
        time_min="2024-01-01T00:00:00Z",
        time_max="2024-01-02T00:00:00Z",
    )
    _start_run(manager, product=product, run_id="run-new", output_path=output_path)

    recorder.register_run(
        ctx=_make_runtime_ctx(run_id="run-new", target=output_path, force_reingest=True),
        result=_make_result(
            output_path=output_path,
            group="F024",
            time_min="2024-01-01T00:00:00Z",
            time_max="2024-01-02T00:00:00Z",
        ),
        run_id="run-new",
        product=product,
        slice_meta={"plugin": "test_product", "group": "F024"},
    )
    manager.record_replacement_committed(
        product=product,
        run_id="run-new",
        replacing_run_id="run-new",
        replaced_span_keys=[prior_key],
    )

    replacement_events = _replacement_events(temp_workspace, product=product, run_id="run-new")
    assert len(replacement_events) == 1


def test_full_state_machine(temp_workspace):
    product = "test_product"
    output_path = str(temp_workspace / product)
    manager = ChunkManager(binding=make_test_binding(temp_workspace), workspace=temp_workspace)
    recorder = SpanRecorder(manager)

    prior_key = _record_completed_span_run(
        manager,
        product=product,
        run_id="run-old",
        group="F024",
        time_min="2024-01-01T00:00:00Z",
        time_max="2024-01-02T00:00:00Z",
    )

    initial_spans = manager.list_chunks(product=product, chunk_type="span")
    assert [span.key for span in initial_spans] == [prior_key]
    assert initial_spans[0].status == "active"

    _start_run(manager, product=product, run_id="run-new", output_path=output_path)
    manager.record_run_started_with_replacement(
        product=product,
        run_id="run-new",
        replaces=[prior_key],
    )

    in_flight_current = _current_state(manager, product=product)
    assert in_flight_current[prior_key]["status"] == "active"
    assert _replacement_events(temp_workspace, product=product, run_id="run-new") == []

    recorder.register_run(
        ctx=_make_runtime_ctx(run_id="run-new", target=output_path, force_reingest=True),
        result=_make_result(
            output_path=output_path,
            group="F024",
            time_min="2024-01-01T00:00:00Z",
            time_max="2024-01-02T00:00:00Z",
        ),
        run_id="run-new",
        product=product,
        slice_meta={"plugin": "test_product", "group": "F024"},
    )
    manager.close()
    manager = ChunkManager(binding=make_test_binding(temp_workspace), workspace=temp_workspace)
    manager.rebuild_snapshot(product)

    final_current = _current_state(manager, product=product)
    assert final_current[prior_key]["status"] == "replaced"
    assert final_current[prior_key]["replaced_by"] == "run-new"
    assert final_current["span_run-new_single_F024"]["status"] == "active"

    final_spans = manager.list_chunks(product=product, chunk_type="span")
    assert [span.key for span in final_spans] == ["span_run-new_single_F024"]
    assert final_spans[0].status == "active"
    assert (final_spans[0].meta or {})["run_id"] == "run-new"

    summary = manager.time_coverage_summary(product=product)

    assert len(summary) == 1
    assert summary[0]["group"] == "F024"
    assert summary[0]["span_count"] == 1
    assert summary[0]["time_min"] == "2024-01-01T00:00:00Z"
    assert summary[0]["time_max"] == "2024-01-02T00:00:00Z"
