from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.events import ConsolidatedTimeCoord
from firecube.core.controlplane.types import (
    CONTROL_DIRNAME,
    EVENT_CONSOLIDATED_TIME_COORD,
    RUNS_DIRNAME,
)
from tests.helpers.storage import make_test_binding


def _wal_events(tmp_path: Path, product: str) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for path in sorted(
        (tmp_path / product / CONTROL_DIRNAME / RUNS_DIRNAME).glob("*/events-*.jsonl")
    ):
        events.extend(json.loads(line) for line in path.read_text(encoding="utf-8").splitlines())
    return events


@pytest.mark.unit
def test_consolidated_time_coord_event_roundtrip() -> None:
    event = ConsolidatedTimeCoord(
        run_id="run-1",
        timestamp_iso="2026-08-27T12:00:00+00:00",
        groups=("F024", "F048"),
    )

    payload = json.loads(json.dumps(event.to_dict()))
    restored = ConsolidatedTimeCoord.from_dict(payload)

    assert restored == event
    assert restored.kind == "consolidated_time_coord"
    assert restored.groups == ("F024", "F048")


@pytest.mark.unit
def test_chunk_manager_records_time_coord_consolidation(tmp_path: Path) -> None:
    product = "sealed.zarr"
    timestamp_iso = "2026-08-27T12:00:00+00:00"
    manager = ChunkManager(binding=make_test_binding(tmp_path, product=product))

    try:
        manager.record_time_coord_consolidation(("F024",), timestamp_iso)
        recorded = manager.list_time_coord_consolidations(product=product)
    finally:
        manager.close()

    assert recorded == [
        ConsolidatedTimeCoord(
            run_id="time-coord-consolidation",
            timestamp_iso=timestamp_iso,
            groups=("F024",),
        )
    ]
    wal_events = _wal_events(tmp_path, product)
    assert [event["event_type"] for event in wal_events] == [EVENT_CONSOLIDATED_TIME_COORD]
    assert wal_events[0]["record"]["groups"] == ["F024"]
