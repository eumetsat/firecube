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

import pytest

from firecube.core.controlplane import ChunkManager, SpanCoverage
from firecube.core.controlplane.types import (
    EVENT_REPLACEMENT_COMMITTED,
    EVENT_RUN_COMPLETED,
    EVENT_SPAN_COMMITTED,
)
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


def _make_runtime_ctx(
    *, run_id: str, target: str, force_reingest: bool = True
) -> RuntimeIngestContext:
    return RuntimeIngestContext(
        source="source",
        target=target,
        output_format="zarr",
        options={"run_id": run_id},
        run_id=run_id,
        identity=RuntimeIdentity(run_id=run_id),
        flags=RuntimeFlags(force_reingest=force_reingest),
    )


def _make_result(
    *,
    output_path: str,
    ranges: list[list[int]],
    time_min: str,
    time_max: str,
    group: str = "default",
) -> IngestResult:
    return IngestResult(
        outputs=OutputPaths(primary=output_path),
        output_format="zarr",
        metrics=ResultMetrics(
            storage=StorageMetrics(bytes=123),
            pipeline=PipelineMetrics(
                coverage=[
                    SpanCoverage(
                        group=group,
                        arrays=[f"{group}/precipitation"],
                        time_index_ranges=ranges,
                        time_min=time_min,
                        time_max=time_max,
                    )
                ]
            ),
        ),
    )


def _record_completed_span(
    manager: ChunkManager,
    *,
    product: str,
    run_id: str,
    batch_id: str,
    ranges: list[list[int]],
    time_min: str,
    time_max: str,
    group: str = "default",
) -> str:
    output_path = f"{manager.base_uri.rstrip('/')}/{product}"
    manager.record_run_started(
        product=product,
        run_id=run_id,
        output_path=output_path,
        output_format="zarr",
        size=0,
        meta={"plugin": "precip_daily"},
    )
    manager.record_span(
        product=product,
        run_id=run_id,
        batch_id=batch_id,
        group=group,
        status="active",
        coverage=SpanCoverage(
            group=group,
            arrays=[f"{group}/precipitation"],
            time_index_ranges=ranges,
            time_min=time_min,
            time_max=time_max,
        ),
        meta={
            "plugin": "precip_daily",
            "group": group,
            "time_min": time_min,
            "time_max": time_max,
        },
    )
    manager.record_run_terminal(
        product=product,
        run_id=run_id,
        output_path=output_path,
        output_format="zarr",
        size=1,
        meta={"plugin": "precip_daily"},
        status="complete",
    )
    return f"span_{run_id}_{batch_id}_{group}"


def _start_run(manager: ChunkManager, *, product: str, run_id: str, output_path: str) -> None:
    manager.record_run_started(
        product=product,
        run_id=run_id,
        output_path=output_path,
        output_format="zarr",
        size=0,
        meta={"plugin": "precip_daily"},
    )


def _register_force_reingest(
    manager: ChunkManager,
    *,
    product: str,
    run_id: str,
    output_path: str,
    ranges: list[list[int]],
    time_min: str,
    time_max: str,
    pre_record_spans: bool = False,
) -> None:
    _start_run(manager, product=product, run_id=run_id, output_path=output_path)
    if pre_record_spans:
        manager.record_span(
            product=product,
            run_id=run_id,
            batch_id="batch-0000",
            group="default",
            status="active",
            coverage=SpanCoverage(
                group="default",
                arrays=["default/precipitation"],
                time_index_ranges=ranges,
                time_min=time_min,
                time_max=time_max,
            ),
            meta={
                "plugin": "precip_daily",
                "group": "default",
                "run_id": run_id,
                "time_min": time_min,
                "time_max": time_max,
            },
        )
    SpanRecorder(manager).register_run(
        ctx=_make_runtime_ctx(run_id=run_id, target=output_path),
        result=_make_result(
            output_path=output_path,
            ranges=ranges,
            time_min=time_min,
            time_max=time_max,
        ),
        run_id=run_id,
        product=product,
        slice_meta={"plugin": "precip_daily", "group": "default"},
        record_spans=not pre_record_spans,
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


def _span_ranges(record: dict) -> list[list[int]]:
    return record["span"]["time_index_ranges"]


def _make_partial_replacement_state(
    temp_workspace: Path,
) -> tuple[ChunkManager, str, dict[str, str]]:
    product = "f6_product"
    output_path = str(temp_workspace / product)
    manager = ChunkManager(binding=make_test_binding(temp_workspace), workspace=temp_workspace)
    keys = {
        "old_head": _record_completed_span(
            manager,
            product=product,
            run_id="run-old-head",
            batch_id="batch-0000",
            ranges=[[0, 9]],
            time_min="2024-01-01T00:00:00Z",
            time_max="2024-01-10T00:00:00Z",
        ),
        "old_partial": _record_completed_span(
            manager,
            product=product,
            run_id="run-old-partial",
            batch_id="batch-0001",
            ranges=[[10, 19]],
            time_min="2024-01-11T00:00:00Z",
            time_max="2024-01-20T00:00:00Z",
        ),
        "old_full": _record_completed_span(
            manager,
            product=product,
            run_id="run-old-full",
            batch_id="batch-0002",
            ranges=[[20, 29]],
            time_min="2024-01-21T00:00:00Z",
            time_max="2024-01-30T00:00:00Z",
        ),
    }
    _register_force_reingest(
        manager,
        product=product,
        run_id="run-new",
        output_path=output_path,
        ranges=[[14, 23], [24, 29], [30, 33], [34, 43], [44, 44]],
        time_min="2024-01-15T00:00:00Z",
        time_max="2024-02-14T00:00:00Z",
        pre_record_spans=True,
    )
    return manager, product, keys


def test_force_reingest_partial_overlap_leaves_untouched_slots_covered(
    temp_workspace: Path,
) -> None:
    """timestamp-based region overwrite: days 11-14 still in projected coverage after partial force_reingest."""
    manager, product, keys = _make_partial_replacement_state(temp_workspace)

    current = manager.repo._load_current_state(product)

    assert current[keys["old_head"]]["status"] == "active"
    assert current[keys["old_partial"]]["status"] == "active"
    assert current[keys["old_full"]]["status"] == "replaced"
    assert _span_ranges(current[keys["old_partial"]]) == [[10, 13]]

    untouched = manager.list_chunks(
        product=product,
        chunk_type="span",
        time_overlaps=("2024-01-11T00:00:00Z", "2024-01-14T00:00:00Z"),
    )
    assert keys["old_partial"] in {chunk.key for chunk in untouched}


def test_new_span_records_overwrites_index_ranges(temp_workspace: Path) -> None:
    """WAL event for new span has meta.overwrites_index_ranges."""
    _manager, product, _keys = _make_partial_replacement_state(temp_workspace)

    span_events = [
        event
        for event in _read_run_wal_events(temp_workspace, product=product, run_id="run-new")
        if event["event_type"] == EVENT_SPAN_COMMITTED
    ]

    enriched = [
        event
        for event in span_events
        if event["record"]["meta"].get("overwrites_index_ranges") == [[14, 19]]
    ]
    assert len(enriched) == 1


def test_full_span_overlap_still_fully_retires(temp_workspace: Path) -> None:
    """Verified-correct: full overlap still marks prior span replaced."""
    product = "f6_product"
    output_path = str(temp_workspace / product)
    manager = ChunkManager(binding=make_test_binding(temp_workspace), workspace=temp_workspace)
    prior_key = _record_completed_span(
        manager,
        product=product,
        run_id="run-old",
        batch_id="batch-0000",
        ranges=[[0, 9]],
        time_min="2024-01-01T00:00:00Z",
        time_max="2024-01-10T00:00:00Z",
    )

    _register_force_reingest(
        manager,
        product=product,
        run_id="run-new",
        output_path=output_path,
        ranges=[[0, 9]],
        time_min="2024-01-01T00:00:00Z",
        time_max="2024-01-10T00:00:00Z",
    )

    current = manager.repo._load_current_state(product)
    replacement_events = [
        event
        for event in _read_run_wal_events(temp_workspace, product=product, run_id="run-new")
        if event["event_type"] == EVENT_REPLACEMENT_COMMITTED
    ]
    span_event = next(
        event
        for event in _read_run_wal_events(temp_workspace, product=product, run_id="run-new")
        if event["event_type"] == EVENT_SPAN_COMMITTED
    )

    assert current[prior_key]["status"] == "replaced"
    assert prior_key in replacement_events[0]["record"]["replaced_span_keys"]
    assert "overwrites_index_ranges" not in span_event["record"]["meta"]


def test_wal_replacement_committed_after_run_completed(temp_workspace: Path) -> None:
    """Verified-correct: replacement_committed ordering preserved."""
    _manager, product, _keys = _make_partial_replacement_state(temp_workspace)

    event_types = [
        event["event_type"]
        for event in _read_run_wal_events(temp_workspace, product=product, run_id="run-new")
    ]

    assert event_types.index(EVENT_RUN_COMPLETED) < event_types.index(EVENT_REPLACEMENT_COMMITTED)
    assert event_types.index(EVENT_SPAN_COMMITTED) < event_types.index(EVENT_REPLACEMENT_COMMITTED)
