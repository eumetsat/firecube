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

"""Tests for ChunkManager.abandon_stale_runs — bulk stale-run abandonment.

Covers:
- Dry-run preview (no WAL mutation).
- Fresh-vs-stale segregation with EVENT_RUN_ABANDONED emission on mutation.
- Mid-sweep race where a previewed run transitions to terminal.
- Mid-sweep race where a previewed run receives a fresh heartbeat.
- Empty-input no-op.
- Repeated-call idempotency.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.types import (
    EVENT_RUN_ABANDONED,
    AbandonSweepResult,
    RunInfo,
)
from firecube.core.storage.uri import StorageUri
from tests.helpers.storage import make_test_binding

pytestmark = pytest.mark.unit


def _manager(tmp_path: Path, *, product: str = "product.zarr") -> ChunkManager:
    workspace = tmp_path / "workspace"
    workspace.mkdir(exist_ok=True)
    return ChunkManager(binding=make_test_binding(tmp_path, product=product), workspace=workspace)


def _runs_dir(tmp_path: Path, product: str) -> Path:
    return tmp_path / product / ".firecube" / "runs"


def _run_dir(tmp_path: Path, product: str, run_id: str) -> Path:
    return _runs_dir(tmp_path, product) / run_id


def _write_run_json(
    tmp_path: Path,
    *,
    product: str,
    run_id: str,
    status: str,
    updated_at: float,
    started_at: float | None = None,
    completed_at: float | None = None,
    stale_threshold_s: int = 3600,
) -> Path:
    run_dir = _run_dir(tmp_path, product, run_id)
    run_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": "v2",
        "product": product,
        "run_id": run_id,
        "status": status,
        "run_dir": str(run_dir),
        "run_uri": StorageUri.from_local_path(run_dir).to_str(),
        "output_path": str(tmp_path / product),
        "output_format": "zarr",
        "started_at": started_at if started_at is not None else updated_at,
        "updated_at": updated_at,
        "completed_at": completed_at,
        "events": 1,
        "parts": 1,
        "run_stale_threshold_s": stale_threshold_s,
    }
    (run_dir / "run.json").write_text(json.dumps(payload), encoding="utf-8")
    return run_dir


def _wal_segment_count(tmp_path: Path, product: str, run_id: str) -> int:
    run_dir = _run_dir(tmp_path, product, run_id)
    return len(list(run_dir.glob("events-*.jsonl")))


def _read_wal_event_types(tmp_path: Path, product: str, run_id: str) -> list[str]:
    types_seen: list[str] = []
    for segment in sorted(_run_dir(tmp_path, product, run_id).glob("events-*.jsonl")):
        for raw in segment.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            evt = event.get("event_type")
            if isinstance(evt, str):
                types_seen.append(evt)
    return types_seen


def test_dry_run_previews_without_abandoning(tmp_path: Path) -> None:
    manager = _manager(tmp_path)
    product = "product.zarr"
    now = time.time()

    for i in range(3):
        _write_run_json(
            tmp_path,
            product=product,
            run_id=f"stale-{i}",
            status="started",
            updated_at=now - 7200,
        )

    pre_counts = {
        f"stale-{i}": _wal_segment_count(tmp_path, product, f"stale-{i}") for i in range(3)
    }

    try:
        result = manager.abandon_stale_runs(product=product, reason="op-timeout", dry_run=True)
    finally:
        manager.close()

    assert isinstance(result, AbandonSweepResult)
    assert sorted(result.previewed) == [f"stale-{i}" for i in range(3)]
    assert result.abandoned == []
    assert result.skipped_fresh == []
    assert result.skipped_already_terminal == []
    for run_id, count in pre_counts.items():
        assert _wal_segment_count(tmp_path, product, run_id) == count, (
            f"dry-run must not touch WAL segments for {run_id}"
        )


def test_abandons_only_stale_runs_leaving_fresh_alone(tmp_path: Path) -> None:
    manager = _manager(tmp_path)
    product = "product.zarr"
    now = time.time()

    stale_ids = [f"stale-{i}" for i in range(3)]
    fresh_ids = [f"fresh-{i}" for i in range(2)]
    for rid in stale_ids:
        _write_run_json(
            tmp_path, product=product, run_id=rid, status="started", updated_at=now - 7200
        )
    for rid in fresh_ids:
        _write_run_json(
            tmp_path, product=product, run_id=rid, status="started", updated_at=now - 60
        )

    fresh_pre_counts = {rid: _wal_segment_count(tmp_path, product, rid) for rid in fresh_ids}

    try:
        result = manager.abandon_stale_runs(product=product, reason="stale-sweep", dry_run=False)
    finally:
        manager.close()

    assert sorted(result.previewed) == sorted(stale_ids)
    assert sorted(result.abandoned) == sorted(stale_ids)
    assert result.skipped_fresh == []
    assert result.skipped_already_terminal == []

    for rid in stale_ids:
        run_meta_path = _run_dir(tmp_path, product, rid) / "run.json"
        payload = json.loads(run_meta_path.read_text(encoding="utf-8"))
        assert payload["status"] == "abandoned", f"{rid} run.json must show terminal status"
        events = _read_wal_event_types(tmp_path, product, rid)
        assert EVENT_RUN_ABANDONED in events, f"{rid} must have EVENT_RUN_ABANDONED"

    for rid in fresh_ids:
        payload = json.loads(
            (_run_dir(tmp_path, product, rid) / "run.json").read_text(encoding="utf-8")
        )
        assert payload["status"] == "started", f"fresh {rid} must remain non-terminal"
        assert _wal_segment_count(tmp_path, product, rid) == fresh_pre_counts[rid]


def test_race_run_becomes_terminal(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Another process finalizes a previewed run between preview and mutation."""
    manager = _manager(tmp_path)
    product = "product.zarr"
    now = time.time()

    r_race = "r_race"
    r_other = "z_other"
    _write_run_json(
        tmp_path, product=product, run_id=r_race, status="started", updated_at=now - 7200
    )
    _write_run_json(
        tmp_path, product=product, run_id=r_other, status="started", updated_at=now - 7200
    )

    original_list_runs = manager.repo.list_runs
    call_count = {"n": 0}

    def racy_list_runs(
        *, product: str, status: str | None = None, non_terminal: bool = False
    ) -> list[RunInfo]:
        call_count["n"] += 1
        # Call #1 is the preview inside list_stale_runs; leave state untouched
        # so both runs appear in ``previewed``. Trigger on the first re-check
        # (call #2) to simulate a concurrent finalizer flipping r_race to
        # terminal between preview and mutation.
        if call_count["n"] == 2:
            _write_run_json(
                tmp_path,
                product=product,
                run_id=r_race,
                status="complete",
                updated_at=time.time(),
                completed_at=time.time(),
            )
        return original_list_runs(product=product, status=status, non_terminal=non_terminal)

    monkeypatch.setattr(manager.repo, "list_runs", racy_list_runs)

    try:
        result = manager.abandon_stale_runs(product=product, reason="race-test", dry_run=False)
    finally:
        manager.close()

    assert r_race in result.previewed and r_other in result.previewed
    assert r_race in result.skipped_already_terminal
    assert r_race not in result.abandoned
    assert r_other in result.abandoned

    race_events = _read_wal_event_types(tmp_path, product, r_race)
    assert EVENT_RUN_ABANDONED not in race_events

    other_payload = json.loads(
        (_run_dir(tmp_path, product, r_other) / "run.json").read_text(encoding="utf-8")
    )
    assert other_payload["status"] == "abandoned"


def test_race_run_receives_fresh_heartbeat(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A live pod refreshes updated_at between preview and mutation → skipped_fresh."""
    manager = _manager(tmp_path)
    product = "product.zarr"
    now = time.time()

    r_race = "r_race"
    r_other = "z_other"
    _write_run_json(
        tmp_path, product=product, run_id=r_race, status="started", updated_at=now - 7200
    )
    _write_run_json(
        tmp_path, product=product, run_id=r_other, status="started", updated_at=now - 7200
    )

    original_list_runs = manager.repo.list_runs
    call_count = {"n": 0}

    def racy_list_runs(
        *, product: str, status: str | None = None, non_terminal: bool = False
    ) -> list[RunInfo]:
        call_count["n"] += 1
        # Trigger on the first re-check (call #2), not the preview (call #1
        # inside list_stale_runs) — otherwise r_race would be filtered out of
        # ``previewed`` and the race condition would never be exercised.
        if call_count["n"] == 2:
            _write_run_json(
                tmp_path,
                product=product,
                run_id=r_race,
                status="started",
                updated_at=time.time(),
            )
        return original_list_runs(product=product, status=status, non_terminal=non_terminal)

    monkeypatch.setattr(manager.repo, "list_runs", racy_list_runs)

    try:
        result = manager.abandon_stale_runs(product=product, reason="race-test", dry_run=False)
    finally:
        manager.close()

    assert r_race in result.previewed and r_other in result.previewed
    assert r_race in result.skipped_fresh
    assert r_race not in result.abandoned
    assert r_other in result.abandoned

    race_events = _read_wal_event_types(tmp_path, product, r_race)
    assert EVENT_RUN_ABANDONED not in race_events

    race_payload = json.loads(
        (_run_dir(tmp_path, product, r_race) / "run.json").read_text(encoding="utf-8")
    )
    assert race_payload["status"] == "started", "refreshed run must remain non-terminal"


def test_no_stale_runs_returns_empty_result(tmp_path: Path) -> None:
    manager = _manager(tmp_path)
    product = "product.zarr"
    now = time.time()

    _write_run_json(
        tmp_path, product=product, run_id="fresh", status="started", updated_at=now - 30
    )

    try:
        result = manager.abandon_stale_runs(product=product, reason="noop", dry_run=False)
    finally:
        manager.close()

    assert isinstance(result, AbandonSweepResult)
    assert result.previewed == []
    assert result.abandoned == []
    assert result.skipped_fresh == []
    assert result.skipped_already_terminal == []


def test_idempotent_second_call(tmp_path: Path) -> None:
    manager = _manager(tmp_path)
    product = "product.zarr"
    now = time.time()

    stale_ids = [f"stale-{i}" for i in range(5)]
    for rid in stale_ids:
        _write_run_json(
            tmp_path, product=product, run_id=rid, status="started", updated_at=now - 7200
        )

    try:
        first = manager.abandon_stale_runs(product=product, reason="first-pass", dry_run=False)
        second = manager.abandon_stale_runs(product=product, reason="second-pass", dry_run=False)
    finally:
        manager.close()

    assert sorted(first.abandoned) == sorted(stale_ids)
    assert second.previewed == []
    assert second.abandoned == []
    assert second.skipped_fresh == []
    assert second.skipped_already_terminal == []

    for rid in stale_ids:
        events = _read_wal_event_types(tmp_path, product, rid)
        assert events.count(EVENT_RUN_ABANDONED) == 1
