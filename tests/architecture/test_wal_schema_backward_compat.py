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

"""WAL byte-parity / backward-compatibility guard for optional coverage fields.

Golden fixture: tests/fixtures/wal_v0.1.5/events_sample.jsonl
A real firecube==0.1.5 WAL segment captured from a 0.1.5 ingestion run
(store ``C1_ts_plain.zarr``, run
``precip_daily-host-8ad9d4a2db284ceeaf7b186ba3e61320``). Concatenates the
original ``events-00000.jsonl`` (single ``run_started``) and
``events-00001.jsonl`` (three ``span_committed`` + one ``run_completed``) into
one file so ``WalReader.read_run_segment`` can consume it directly.

The fixture predates the following optional coverage fields:
  - ``SpanCoverage.chunk_len_used`` (optional int, future)
  - ``PipelineMetrics.timestamps_skipped`` (int, default 0, future)
  - span meta ``overwrites_index_ranges`` (optional list, future)
  - span ``write_strategy`` (optional str, future)

These tests must pass both before and after those fields are added.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pytest
from tests.helpers.storage import make_test_binding

from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane._event_processor import apply_events
from firecube.core.controlplane._projection import _apply_overwrite_ranges_to_active_coverage
from firecube.core.controlplane._wal_reader import WalReader
from firecube.core.controlplane.types import (
    EVENT_RUN_COMPLETED,
    EVENT_RUN_STARTED,
    EVENT_SPAN_COMMITTED,
    EVENT_SPAN_FAILED,
    SCHEMA_VERSION,
    SpanCoverage,
)
from firecube.core.filesystem import FsspecFilesystem
from firecube.core.storage.uri import StorageUri

pytestmark = pytest.mark.architecture

_FIXTURE_PRODUCT = "precip_daily"
_FIXTURE_RUN_ID = "precip_daily-host-8ad9d4a2db284ceeaf7b186ba3e61320"

_FIXTURE_DIR = Path(__file__).parent.parent / "fixtures" / "wal_v0.1.5"
_EVENTS_SAMPLE = _FIXTURE_DIR / "events_sample.jsonl"


def _make_reader(tmp_path: Path) -> WalReader:
    binding = make_test_binding(tmp_path, product="product.zarr")
    product_uri = binding.identity.product_uri
    return WalReader(
        fs=FsspecFilesystem(binding),
        resolver=lambda _product: (product_uri, product_uri),
        log=logging.getLogger(__name__),
        run_stale_threshold_s=3600,
    )


def test_v0_1_5_wal_parses(tmp_path: Path) -> None:
    """Golden v0.1.5 WAL segment parses without errors via WalReader.read_run_segment.

    Verifies that the current reader can consume a real 0.1.5 WAL file
    (no chunk_len_used, no timestamps_skipped, no write_strategy) and returns
    the expected event types in order.
    """
    reader = _make_reader(tmp_path)
    segment_uri = StorageUri.from_local_path(_EVENTS_SAMPLE)

    events, recovered_tail = reader.read_run_segment(
        path=segment_uri,
        product=_FIXTURE_PRODUCT,
        run_id=_FIXTURE_RUN_ID,
        allow_torn_tail=False,
    )

    assert not recovered_tail, "Golden fixture must not trigger torn-tail recovery"
    assert len(events) == 5, f"Expected 5 events, got {len(events)}"

    event_types = [e["event_type"] for e in events]
    assert event_types[0] == EVENT_RUN_STARTED
    assert event_types[1] == EVENT_SPAN_COMMITTED
    assert event_types[2] == EVENT_SPAN_COMMITTED
    assert event_types[3] == EVENT_SPAN_COMMITTED
    assert event_types[4] == EVENT_RUN_COMPLETED

    for event in events:
        assert event["schema_version"] == SCHEMA_VERSION, (
            f"Event {event.get('event_id')} has unexpected schema_version: "
            f"{event.get('schema_version')!r}"
        )
        assert event["product"] == _FIXTURE_PRODUCT
        assert event["run_id"] == _FIXTURE_RUN_ID


def test_v0_1_5_snapshot_rebuild_stable(tmp_path: Path) -> None:
    """Snapshot rebuild from golden v0.1.5 WAL produces stable span count.

    Exercises apply_events (the projection engine) against the golden fixture
    and asserts the projected state contains the expected span records.
    """
    reader = _make_reader(tmp_path)
    segment_uri = StorageUri.from_local_path(_EVENTS_SAMPLE)

    events, _ = reader.read_run_segment(
        path=segment_uri,
        product=_FIXTURE_PRODUCT,
        run_id=_FIXTURE_RUN_ID,
        allow_torn_tail=False,
    )

    current: dict[str, dict] = {}
    apply_events(current, events, logging.getLogger(__name__))

    assert len(current) == 4, (
        f"Expected 4 projected records (1 run + 3 spans), got {len(current)}: "
        f"{list(current.keys())}"
    )

    span_keys = [k for k in current if k.startswith("span_")]
    assert len(span_keys) == 3, f"Expected 3 span records, got {len(span_keys)}"

    run_keys = [k for k in current if k.startswith("run_")]
    assert len(run_keys) == 1, f"Expected 1 run record, got {len(run_keys)}"

    run_record = current[run_keys[0]]
    assert run_record["status"] == "complete"


def test_v0_1_5_spans_without_overwrite_ranges_project_unchanged(tmp_path: Path) -> None:
    """Spans predating ``overwrites_index_ranges`` keep their coverage after projection.

    The v0.1.5 golden spans carry no ``overwrites_index_ranges`` in their meta.
    The overwrite-subtraction step of the projection must treat that absence as
    "nothing overwritten" and leave every ``time_index_ranges`` and
    ``timestamps_written`` exactly as recorded.
    """
    reader = _make_reader(tmp_path)
    segment_uri = StorageUri.from_local_path(_EVENTS_SAMPLE)

    events, _ = reader.read_run_segment(
        path=segment_uri,
        product=_FIXTURE_PRODUCT,
        run_id=_FIXTURE_RUN_ID,
        allow_torn_tail=False,
    )

    current: dict[str, dict] = {}
    apply_events(current, events, logging.getLogger(__name__))

    spans = {key: record for key, record in current.items() if key.startswith("span_")}
    assert len(spans) == 3, list(spans)
    for key, record in spans.items():
        assert "overwrites_index_ranges" not in record["meta"], key

    projected = _apply_overwrite_ranges_to_active_coverage(current)

    expected_ranges = {
        "precip_daily_batch_0000": [[0, 9]],
        "precip_daily_batch_0001": [[10, 19]],
        "precip_daily_batch_0002": [[20, 29]],
    }
    observed_ranges = {
        record["meta"]["batch_id"]: record["span"]["time_index_ranges"]
        for record in projected.values()
        if record.get("type") == "span"
    }
    assert observed_ranges == expected_ranges
    for record in projected.values():
        if record.get("type") == "span":
            assert record["status"] == "active"
            assert record["span"]["timestamps_written"] == 10


_FAILED_BATCH_ID = "precip_daily_batch_0003"
_FAILED_SPAN_KEY = f"span_{_FIXTURE_RUN_ID}_{_FAILED_BATCH_ID}_default"
_FAILED_REPAIR = {
    "state_marked_ranges": [[30, 31]],
    "truncated_to": None,
    "group_removed": False,
    "error": None,
}


def _span_failed_event_with_ranges(timestamp: float) -> dict:
    """A ``span_failed`` event shaped like the fixture's spans, plus the A3 fields.

    ``span.time_index_ranges`` names the region slots the failed batch
    touched and ``meta.repair`` records what the repair did; both are
    additive to the v0.1.5 span payload.
    """
    meta = {
        "plugin": "precip_daily",
        "run_id": _FIXTURE_RUN_ID,
        "time_min": "2024-01-31T00:00:00",
        "time_max": "2024-02-01T00:00:00",
        "group": "default",
        "batch_id": _FAILED_BATCH_ID,
        "repair": dict(_FAILED_REPAIR),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "event_id": f"{_FIXTURE_RUN_ID}:00001:000009",
        "event_type": EVENT_SPAN_FAILED,
        "product": _FIXTURE_PRODUCT,
        "run_id": _FIXTURE_RUN_ID,
        "timestamp": timestamp,
        "record": {
            "key": _FAILED_SPAN_KEY,
            "type": "span",
            "size": 0,
            "timestamp": timestamp,
            "status": "failed",
            "meta": meta,
            "span": {
                "arrays": ["default/precipitation"],
                "time_index_ranges": [[30, 31]],
                "timestamps_written": 2,
                "aligned": True,
                "state_array": "default/firecube_timestamp_state",
                "state_deleted_value": 2,
                "reason": "injected tail append failure",
                "write_strategy": "append_failed",
                "time_dim_name": "time",
            },
            "schema_version": SCHEMA_VERSION,
        },
        "meta": meta,
    }


def test_span_failed_with_ranges_and_repair_projects_as_failed(tmp_path: Path) -> None:
    """A ``span_failed`` carrying ``time_index_ranges`` and ``meta.repair`` projects as failed.

    Alongside the golden v0.1.5 spans, the failed span keeps its ranges and
    repair metadata after projection, its status is ``failed``, and the
    three golden ``active`` spans are unaffected.
    """
    reader = _make_reader(tmp_path)
    segment_uri = StorageUri.from_local_path(_EVENTS_SAMPLE)
    events, _ = reader.read_run_segment(
        path=segment_uri,
        product=_FIXTURE_PRODUCT,
        run_id=_FIXTURE_RUN_ID,
        allow_torn_tail=False,
    )
    last_timestamp = float(events[-1]["timestamp"])
    events.append(_span_failed_event_with_ranges(last_timestamp + 1.0))

    current: dict[str, dict] = {}
    apply_events(current, events, logging.getLogger(__name__))
    projected = _apply_overwrite_ranges_to_active_coverage(current)

    failed = projected[_FAILED_SPAN_KEY]
    assert failed["status"] == "failed"
    assert failed["span"]["time_index_ranges"] == [[30, 31]]
    assert failed["span"]["write_strategy"] == "append_failed"
    assert failed["meta"]["repair"] == _FAILED_REPAIR
    active_keys = {
        key
        for key, record in projected.items()
        if record.get("type") == "span" and record["status"] == "active"
    }
    assert len(active_keys) == 3
    assert _FAILED_SPAN_KEY not in active_keys


def test_span_failed_with_ranges_is_excluded_from_active_queries(tmp_path: Path) -> None:
    """Through ``ChunkManager``, a failed span with ranges never answers ``status="active"``."""
    manager = ChunkManager(
        binding=make_test_binding(tmp_path, product="product.zarr"), workspace=tmp_path / "work"
    )
    product = "product.zarr"
    run_id = "run-failed-with-ranges"
    output_path = str(tmp_path / product)
    manager.record_run_started(
        product=product,
        run_id=run_id,
        output_path=output_path,
        output_format="zarr",
        size=0,
        meta={"plugin": "test"},
    )
    manager.record_span(
        product=product,
        run_id=run_id,
        batch_id="batch_0000",
        group="default",
        status="active",
        coverage=SpanCoverage(
            group="default",
            arrays=["default/value"],
            time_index_ranges=[[0, 9]],
            time_min="2024-01-01T00:00:00",
            time_max="2024-01-10T00:00:00",
        ),
        meta={"plugin": "test", "time_min": "2024-01-01T00:00:00"},
    )
    manager.record_span(
        product=product,
        run_id=run_id,
        batch_id="batch_0001",
        group="default",
        status="failed",
        reason="injected tail append failure",
        coverage=SpanCoverage(
            group="default",
            arrays=["default/value"],
            time_index_ranges=[[10, 11]],
            write_strategy="append_failed",
        ),
        meta={"plugin": "test", "repair": dict(_FAILED_REPAIR)},
    )
    manager.record_run_terminal(
        product=product,
        run_id=run_id,
        output_path=output_path,
        output_format="zarr",
        size=0,
        meta={"plugin": "test"},
        status="failed",
        error="boom",
    )

    active = manager.list_chunks(product=product, chunk_type="span", status="active")
    failed = manager.list_chunks(product=product, chunk_type="span", status="failed")

    assert [chunk.key for chunk in active] == [f"span_{run_id}_batch_0000_default"]
    assert [chunk.key for chunk in failed] == [f"span_{run_id}_batch_0001_default"]
    failed_record = failed[0].record or {}
    assert failed_record["span"]["time_index_ranges"] == [[10, 11]]
    assert (failed[0].meta or {})["repair"] == _FAILED_REPAIR
