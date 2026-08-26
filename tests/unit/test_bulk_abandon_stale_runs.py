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
- Crash-recovery idempotency: partial abandon followed by a rerun.
- Concurrent-operator idempotency: two overlapping --all-stale sweeps against
  the same product must not double-abandon any run.
- Orphan run directory (segments present, run.json missing) behavior lock.
- Enumeration-cost regression (CountingFilesystem): sweep must bound the number
  of runs-directory listings independent of stale count S.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

import pytest

from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.types import (
    EVENT_RUN_ABANDONED,
    AbandonSweepResult,
)
from firecube.core.storage.uri import StorageUri
from tests.helpers.storage import make_test_binding
from tests.unit._helpers.counting_fs import CountingFilesystem, make_counting_local_fs

pytestmark = pytest.mark.unit


def _manager(tmp_path: Path, *, product: str = "product.zarr") -> ChunkManager:
    workspace = tmp_path / "workspace"
    workspace.mkdir(exist_ok=True)
    return ChunkManager(binding=make_test_binding(tmp_path, product=product), workspace=workspace)


class _PathCountingFilesystem(CountingFilesystem):
    """CountingFilesystem variant that records which paths were listed."""

    def __init__(self, fs: Any) -> None:
        super().__init__(fs)
        self.ls_paths: list[str] = []

    def ls(self, uri: StorageUri, detail: bool = False) -> list[Any]:
        self.ls_paths.append(uri.path)
        return super().ls(uri, detail=detail)


def _counting_manager(
    tmp_path: Path, *, product: str = "product.zarr"
) -> tuple[ChunkManager, _PathCountingFilesystem]:
    workspace = tmp_path / "workspace"
    workspace.mkdir(exist_ok=True)
    _wrapped, real_fs = make_counting_local_fs(tmp_path)
    counting_fs = _PathCountingFilesystem(real_fs)
    manager = ChunkManager(
        binding=make_test_binding(tmp_path, product=product),
        workspace=workspace,
        filesystem=counting_fs,
    )
    return manager, counting_fs


def _runs_ls_count(counting_fs: _PathCountingFilesystem) -> int:
    """Count list calls targeting the ``.firecube/runs`` directory."""
    return sum(1 for path in counting_fs.ls_paths if path.endswith("/.firecube/runs"))


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

    manager.repo._ensure_bound()
    wal_reader = manager.repo._wal_reader
    assert wal_reader is not None
    original_read_run_entry = wal_reader.read_run_entry
    read_counts = {r_race: 0}

    def racy_read_run_entry(
        *, product: str, run_dir: StorageUri, run_uri: str, run_id: str | None = None
    ) -> dict[str, Any] | None:
        if run_id == r_race:
            read_counts[r_race] += 1
        # Call #1 for r_race is the preview inside list_stale_runs; leave state
        # untouched so both runs appear in ``previewed``. Trigger on the first
        # targeted re-check to simulate a concurrent finalizer flipping r_race
        # to terminal between preview and mutation.
        if run_id == r_race and read_counts[r_race] == 2:
            _write_run_json(
                tmp_path,
                product=product,
                run_id=r_race,
                status="complete",
                updated_at=time.time(),
                completed_at=time.time(),
            )
        return original_read_run_entry(
            product=product, run_dir=run_dir, run_uri=run_uri, run_id=run_id
        )

    abandon_attempts: list[str] = []
    original_abandon_run = manager.repo.abandon_run

    def tracked_abandon_run(
        *, product: str, run_id: str, reason: str, meta: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        abandon_attempts.append(run_id)
        return original_abandon_run(product=product, run_id=run_id, reason=reason, meta=meta)

    monkeypatch.setattr(wal_reader, "read_run_entry", racy_read_run_entry)
    monkeypatch.setattr(manager.repo, "abandon_run", tracked_abandon_run)

    try:
        result = manager.abandon_stale_runs(product=product, reason="race-test", dry_run=False)
    finally:
        manager.close()

    assert r_race in result.previewed and r_other in result.previewed
    assert r_race in result.skipped_already_terminal
    assert r_race not in result.abandoned
    assert r_race not in abandon_attempts
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

    manager.repo._ensure_bound()
    wal_reader = manager.repo._wal_reader
    assert wal_reader is not None
    original_read_run_entry = wal_reader.read_run_entry
    read_counts = {r_race: 0}

    def racy_read_run_entry(
        *, product: str, run_dir: StorageUri, run_uri: str, run_id: str | None = None
    ) -> dict[str, Any] | None:
        if run_id == r_race:
            read_counts[r_race] += 1
        # Trigger on the first targeted re-check, not the preview inside
        # list_stale_runs; otherwise r_race would be filtered out of
        # ``previewed`` and the race condition would never be exercised.
        if run_id == r_race and read_counts[r_race] == 2:
            _write_run_json(
                tmp_path,
                product=product,
                run_id=r_race,
                status="started",
                updated_at=time.time(),
            )
        return original_read_run_entry(
            product=product, run_dir=run_dir, run_uri=run_uri, run_id=run_id
        )

    abandon_attempts: list[str] = []
    original_abandon_run = manager.repo.abandon_run

    def tracked_abandon_run(
        *, product: str, run_id: str, reason: str, meta: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        abandon_attempts.append(run_id)
        return original_abandon_run(product=product, run_id=run_id, reason=reason, meta=meta)

    monkeypatch.setattr(wal_reader, "read_run_entry", racy_read_run_entry)
    monkeypatch.setattr(manager.repo, "abandon_run", tracked_abandon_run)

    try:
        result = manager.abandon_stale_runs(product=product, reason="race-test", dry_run=False)
    finally:
        manager.close()

    assert r_race in result.previewed and r_other in result.previewed
    assert r_race in result.skipped_fresh
    assert r_race not in result.abandoned
    assert r_race not in abandon_attempts
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


def test_partial_abandon_crash_recoverable_on_rerun(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A crash mid-sweep leaves partial state that a second sweep completes.

    Simulates the operator running abandon-stale-runs, having the process
    killed (SIGKILL, OOM, node lost) after partially abandoning runs, and
    re-invoking the command. Idempotency must hold across the crash: no
    duplicate EVENT_RUN_ABANDONED, no re-abandon attempt on terminal runs,
    and every stale run is terminal after the recovery pass.
    """
    manager = _manager(tmp_path)
    product = "product.zarr"
    now = time.time()

    stale_ids = [f"stale-{i}" for i in range(5)]
    for rid in stale_ids:
        _write_run_json(
            tmp_path, product=product, run_id=rid, status="started", updated_at=now - 7200
        )

    original_abandon_run = manager.repo.abandon_run
    call_count = {"n": 0}
    crash_after = 3

    def crashing_abandon_run(
        *, product: str, run_id: str, reason: str, meta: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        call_count["n"] += 1
        if call_count["n"] > crash_after:
            raise RuntimeError(f"simulated crash after {crash_after} abandonments")
        return original_abandon_run(product=product, run_id=run_id, reason=reason, meta=meta)

    monkeypatch.setattr(manager.repo, "abandon_run", crashing_abandon_run)

    try:
        with pytest.raises(RuntimeError, match=f"simulated crash after {crash_after}"):
            manager.abandon_stale_runs(product=product, reason="first-pass", dry_run=False)
    finally:
        monkeypatch.setattr(manager.repo, "abandon_run", original_abandon_run)

    abandoned_after_crash = []
    still_started_after_crash = []
    for rid in stale_ids:
        payload = json.loads(
            (_run_dir(tmp_path, product, rid) / "run.json").read_text(encoding="utf-8")
        )
        if payload["status"] == "abandoned":
            abandoned_after_crash.append(rid)
        elif payload["status"] == "started":
            still_started_after_crash.append(rid)
    assert len(abandoned_after_crash) == crash_after, (
        f"crash simulation should leave exactly {crash_after} runs abandoned; "
        f"got abandoned={abandoned_after_crash}, still_started={still_started_after_crash}"
    )
    assert len(still_started_after_crash) == len(stale_ids) - crash_after

    try:
        second = manager.abandon_stale_runs(product=product, reason="recovery-pass", dry_run=False)
    finally:
        manager.close()

    assert sorted(second.abandoned) == sorted(still_started_after_crash)
    assert sorted(second.previewed) == sorted(still_started_after_crash)
    assert second.skipped_fresh == []
    assert set(second.abandoned).isdisjoint(abandoned_after_crash)

    total_abandoned = set(abandoned_after_crash) | set(second.abandoned)
    assert total_abandoned == set(stale_ids)
    for rid in stale_ids:
        payload = json.loads(
            (_run_dir(tmp_path, product, rid) / "run.json").read_text(encoding="utf-8")
        )
        assert payload["status"] == "abandoned", (
            f"{rid} must be terminal after recovery pass; got {payload['status']}"
        )
        events = _read_wal_event_types(tmp_path, product, rid)
        assert events.count(EVENT_RUN_ABANDONED) == 1, (
            f"{rid} must have exactly one EVENT_RUN_ABANDONED across two sweeps; "
            f"got events={events}"
        )


def test_concurrent_all_stale_sweeps_are_idempotent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two overlapping --all-stale sweeps must not double-abandon any run.

    Simulates two operators (two ``ChunkManager`` instances against the same
    on-disk control-plane root) running ``abandon_stale_runs`` concurrently.
    Uses deterministic monkeypatch interleaving instead of threads: when
    operator A is about to call ``abandon_run`` on the 5th stale run, operator
    B's full sweep runs first, then A resumes.

    Operator B's ``list_stale_runs`` is captured up-front so its preview holds
    all 10 stale runs — as it would if both operators listed at nearly the
    same wall time in a real race. The mutation-time re-check inside each
    sweep is what must actually keep the sweeps idempotent.

    Invariants proven:
      - Neither operator raises.
      - Every stale run reaches ``status="abandoned"``.
      - Every stale run has exactly ONE ``EVENT_RUN_ABANDONED`` — no duplicate
        emission from the overlap, no run left non-terminal.
      - Runs the other operator already abandoned surface as
        ``skipped_already_terminal`` on the losing side (no exception, no
        silent double-count into ``abandoned``).
    """
    manager_a = _manager(tmp_path)
    manager_b = _manager(tmp_path)
    product = "product.zarr"
    now = time.time()

    n_stale = 10
    stale_ids = [f"stale-{i}" for i in range(n_stale)]
    for rid in stale_ids:
        _write_run_json(
            tmp_path, product=product, run_id=rid, status="started", updated_at=now - 7200
        )

    cached_b_preview = manager_b.list_stale_runs(product=product)
    assert len(cached_b_preview) == n_stale
    assert sorted(run.run_id for run in cached_b_preview) == sorted(stale_ids)

    def cached_list_stale_runs_for_b(*, product: str) -> list[Any]:
        return list(cached_b_preview)

    monkeypatch.setattr(manager_b.repo, "list_stale_runs", cached_list_stale_runs_for_b)

    original_a_abandon_run = manager_a.repo.abandon_run
    a_call_count = {"n": 0}
    b_result_holder: dict[str, Any] = {}
    trigger_at = 5

    def racy_a_abandon_run(
        *, product: str, run_id: str, reason: str, meta: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        a_call_count["n"] += 1
        if a_call_count["n"] == trigger_at and "result" not in b_result_holder:
            b_result_holder["result"] = manager_b.abandon_stale_runs(
                product=product, reason="op-b-sweep", dry_run=False
            )
        return original_a_abandon_run(product=product, run_id=run_id, reason=reason, meta=meta)

    monkeypatch.setattr(manager_a.repo, "abandon_run", racy_a_abandon_run)

    try:
        result_a = manager_a.abandon_stale_runs(product=product, reason="op-a-sweep", dry_run=False)
    finally:
        manager_a.close()
        manager_b.close()

    assert "result" in b_result_holder, "interleave did not fire; test setup is broken"
    result_b = b_result_holder["result"]

    assert isinstance(result_a, AbandonSweepResult)
    assert isinstance(result_b, AbandonSweepResult)

    a_won = set(result_a.abandoned)
    b_won = set(result_b.abandoned)

    assert len(a_won) == trigger_at - 1, (
        f"Op A must abandon exactly {trigger_at - 1} runs before the interleave; "
        f"got {sorted(a_won)}"
    )
    assert len(b_won) == n_stale - (trigger_at - 1), (
        f"Op B must abandon the remaining {n_stale - (trigger_at - 1)} runs; got {sorted(b_won)}"
    )

    assert a_won.isdisjoint(b_won), (
        f"concurrent sweeps must not both claim the same abandonment: "
        f"overlap={sorted(a_won & b_won)}"
    )
    assert a_won | b_won == set(stale_ids), (
        f"union of both operators' abandoned sets must cover all stale ids; "
        f"missing={set(stale_ids) - (a_won | b_won)}"
    )

    assert set(result_a.skipped_already_terminal) == b_won, (
        "Op A must report runs Op B abandoned as skipped_already_terminal"
    )
    assert result_a.skipped_fresh == []

    assert set(result_b.skipped_already_terminal) == a_won, (
        "Op B must report runs Op A abandoned as skipped_already_terminal"
    )
    assert result_b.skipped_fresh == []

    for rid in stale_ids:
        payload = json.loads(
            (_run_dir(tmp_path, product, rid) / "run.json").read_text(encoding="utf-8")
        )
        assert payload["status"] == "abandoned", (
            f"{rid} must be terminal after both concurrent sweeps; got {payload['status']}"
        )
        events = _read_wal_event_types(tmp_path, product, rid)
        assert events.count(EVENT_RUN_ABANDONED) == 1, (
            f"{rid} must have exactly one EVENT_RUN_ABANDONED across concurrent sweeps; "
            f"got events={events}"
        )


def test_orphan_run_directory_handling(tmp_path: Path) -> None:
    """Behavior lock: sweep handling of orphan run dirs (segments, no run.json).

    An "orphan" is a run directory that WalReader.read_run_entry synthesizes when
    segment files exist but run.json is missing (``error="missing_run_meta"``).
    Task 8 will rewrite ``_get_run_entry`` to route through ``read_run_entry``;
    this test pins the current end-to-end sweep outcome so that migration cannot
    silently change how orphans are cleaned up.
    """
    manager = _manager(tmp_path)
    product = "product.zarr"
    now = time.time()

    stale_ids = [f"stale-{i}" for i in range(5)]
    for rid in stale_ids:
        _write_run_json(
            tmp_path, product=product, run_id=rid, status="started", updated_at=now - 7200
        )

    orphan_id = "orphan-run"
    orphan_dir = _run_dir(tmp_path, product, orphan_id)
    orphan_dir.mkdir(parents=True)
    orphan_segment = orphan_dir / "events-00000.jsonl"
    orphan_segment.write_text('{"event_type": "noop"}\n', encoding="utf-8")
    old_ts = now - 7200
    os.utime(orphan_segment, (old_ts, old_ts))

    try:
        result = manager.abandon_stale_runs(product=product, reason="orphan-lock", dry_run=False)
    finally:
        manager.close()

    all_ids = set(stale_ids) | {orphan_id}
    assert set(result.previewed) == all_ids, (
        "Behavior lock: orphan runs appear in previewed because status='orphaned' "
        "is non-terminal and updated_at derived from segment mtime is > threshold "
        "(WalReader.build_orphan_run_entry + RunInfo.is_terminal)."
    )
    assert set(result.abandoned) == all_ids, (
        "Behavior lock: orphan runs are abandoned via abandon_run → "
        "record_run_terminal(status='abandoned') per current implementation "
        "(_wal_writer.abandon_run does not special-case status='orphaned')."
    )
    assert result.skipped_fresh == []
    assert result.skipped_already_terminal == []

    orphan_meta_path = orphan_dir / "run.json"
    assert orphan_meta_path.exists(), (
        "Behavior lock: abandoning an orphan materializes run.json via "
        "RunEventWriter.finalize → _write_run_meta."
    )
    orphan_payload = json.loads(orphan_meta_path.read_text(encoding="utf-8"))
    assert orphan_payload["status"] == "abandoned"

    orphan_events = _read_wal_event_types(tmp_path, product, orphan_id)
    assert EVENT_RUN_ABANDONED in orphan_events


def test_enumeration_bounded_by_stale_count(tmp_path: Path) -> None:
    """abandon_stale_runs must not re-list the runs directory once per stale item.

    The mutation loop re-checks each candidate with a targeted single-file
    read, so the number of runs-directory listings stays bounded by the
    preview regardless of how many runs turn out to be stale. Re-listing per
    candidate would make a sweep cost O(N x S) on a product with thousands of
    runs.
    """
    manager, counting_fs = _counting_manager(tmp_path)
    product = "product.zarr"
    now = time.time()

    stale_ids = [f"stale-{i}" for i in range(10)]
    fresh_ids = [f"fresh-{i}" for i in range(10)]
    for rid in stale_ids:
        _write_run_json(
            tmp_path, product=product, run_id=rid, status="started", updated_at=now - 7200
        )
    for rid in fresh_ids:
        _write_run_json(
            tmp_path, product=product, run_id=rid, status="started", updated_at=now - 60
        )

    counting_fs.reset()
    counting_fs.ls_paths.clear()

    try:
        result = manager.abandon_stale_runs(product=product, reason="bound-test", dry_run=False)
    finally:
        manager.close()

    observed = _runs_ls_count(counting_fs)
    assert observed <= 2, (
        f"abandon_stale_runs listed runs_dir {observed} times for N=20, S=10; "
        "must be bounded (<= 2) independent of stale count. "
        f"Recorded ls paths on runs_dir: "
        f"{[p for p in counting_fs.ls_paths if p.endswith('/.firecube/runs')]}"
    )
    assert len(result.abandoned) == len(stale_ids)


def test_enumeration_bounded_scales_with_N_not_S(tmp_path: Path) -> None:
    """Enumeration cost is constant across differing stale counts.

    Runs (N=50, S=5) and (N=20, S=15) and asserts the number of
    runs-directory listings is bounded in each and varies by at most 1
    between the two, pinning the cost to N rather than S.
    """
    product = "product.zarr"
    now = time.time()

    def _run_case(root: Path, *, stale: int, fresh: int) -> tuple[int, int]:
        root.mkdir()
        mgr, counting = _counting_manager(root, product=product)
        for i in range(stale):
            _write_run_json(
                root,
                product=product,
                run_id=f"stale-{i}",
                status="started",
                updated_at=now - 7200,
            )
        for i in range(fresh):
            _write_run_json(
                root,
                product=product,
                run_id=f"fresh-{i}",
                status="started",
                updated_at=now - 60,
            )
        counting.reset()
        counting.ls_paths.clear()
        try:
            result = mgr.abandon_stale_runs(product=product, reason="scale-test", dry_run=False)
        finally:
            mgr.close()
        return _runs_ls_count(counting), len(result.abandoned)

    ls_a, abandoned_a = _run_case(tmp_path / "case_a", stale=5, fresh=45)
    ls_b, abandoned_b = _run_case(tmp_path / "case_b", stale=15, fresh=5)

    assert abandoned_a == 5
    assert abandoned_b == 15
    assert ls_a <= 2, f"case A (N=50, S=5): {ls_a} runs_dir listings"
    assert ls_b <= 2, f"case B (N=20, S=15): {ls_b} runs_dir listings"
    assert abs(ls_a - ls_b) <= 1, (
        f"listing count scales with stale count S: A(S=5)={ls_a}, B(S=15)={ls_b}"
    )
