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

"""Concurrency tests for WAL write ordering under parallel batch completion.

Multiple writer threads complete distinct ingestion runs simultaneously
against the same product. Each writer thread has its own ``run_id`` (so no
claim contention is exercised here — that race is covered by
``test_concurrent_same_product.py``) and records a deterministic batch of
events through :class:`firecube.core.controlplane.ChunkManager`. The
control plane must guarantee, regardless of interleaving:

* every WAL segment file parses cleanly (no torn lines, no schema drift);
* every event written by a writer thread is present exactly once in the
  WAL JSONL files and in the projected read-model;
* the live projection contains exactly one record per ``(run_id, batch_id)``
  pair the writers produced;
* the snapshot rebuilt from the WAL agrees with the live WAL projection
  (snapshot is a derived cache, never a source of new information);
* a manually torn WAL segment fails fast with
  :class:`firecube.core.errors.ControlPlaneCorruptionError` instead of
  silently corrupting the read model.

Determinism is achieved with :class:`threading.Barrier` so all writer
threads release at the same instant. Join timeouts are generous (30s) so a
wedged WAL writer fails loudly instead of hanging the suite.
"""

from __future__ import annotations

import json
import threading
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from firecube.core.controlplane import ChunkManager, SpanCoverage
from firecube.core.controlplane.types import (
    CONTROL_DIRNAME,
    RUNS_DIRNAME,
    SCHEMA_VERSION,
)
from firecube.core.errors import ControlPlaneCorruptionError
from tests.helpers.storage import make_test_binding

pytestmark = [pytest.mark.integration, pytest.mark.concurrency]

_PRODUCT = "wal_concurrent.zarr"
_GROUP = "F024"
_TIME_MIN = "2024-01-01T00:00:00"
_TIME_MAX = "2024-01-02T00:00:00"
_WRITER_THREAD_COUNT = 8
"""Number of writer threads (must be >= 8 per TODO §14 point 3)."""

_SPANS_PER_WRITER = 18
"""Spans recorded by each writer thread. With the run_started and
run_completed bracketing events this gives 20 WAL events per writer,
meeting the >= 20 events/writer requirement."""

_TOTAL_EVENTS_PER_WRITER = _SPANS_PER_WRITER + 2  # +run_started, +run_completed
_JOIN_TIMEOUT_S = 30.0
_BARRIER_TIMEOUT_S = 30.0


@dataclass(slots=True)
class _WriterOutcome:
    """Outcome captured from a single writer thread."""

    run_id: str
    events_recorded: int = 0
    error: BaseException | None = None
    unexpected_errors: list[BaseException] = field(default_factory=list)


def _writer_run_id(idx: int) -> str:
    return f"run-{idx:02d}"


def _writer_batch_id(run_id: str, span_index: int) -> str:
    return f"batch-{run_id}-{span_index:02d}"


def _expected_run_ids() -> set[str]:
    return {_writer_run_id(idx) for idx in range(_WRITER_THREAD_COUNT)}


def _expected_batch_keys() -> set[tuple[str, str]]:
    """All ``(run_id, batch_id)`` pairs the writers will produce."""
    return {
        (_writer_run_id(idx), _writer_batch_id(_writer_run_id(idx), span))
        for idx in range(_WRITER_THREAD_COUNT)
        for span in range(_SPANS_PER_WRITER)
    }


def _run_writer(
    *,
    tmp_path: Path,
    run_id: str,
    barrier: threading.Barrier,
    outcomes: list[_WriterOutcome],
    lock: threading.Lock,
) -> None:
    """One concurrent writer: record_run_started -> N spans -> record_run_terminal."""
    binding = make_test_binding(tmp_path, product=_PRODUCT)
    workspace = tmp_path / f"workspace-{run_id}"
    workspace.mkdir(exist_ok=True)
    manager = ChunkManager(binding=binding, workspace=workspace)
    output_path = f"{manager.base_uri.rstrip('/')}/{_PRODUCT}"
    base_meta = {"plugin": "test_wal_concurrent_ordering", "run_id": run_id}

    outcome = _WriterOutcome(run_id=run_id)
    try:
        # Synchronise all writers at the contention point so they race the
        # control-plane filesystem ops (makedirs, atomic schema write,
        # WAL append) instead of serialising behind ChunkManager setup.
        barrier.wait(timeout=_BARRIER_TIMEOUT_S)
        try:
            manager.record_run_started(
                product=_PRODUCT,
                run_id=run_id,
                output_path=output_path,
                output_format="zarr",
                size=0,
                meta=base_meta,
            )
            outcome.events_recorded += 1
            for span_index in range(_SPANS_PER_WRITER):
                # Per-writer group name keeps span keys unique across threads
                # so the active-span dedupe in ``list_chunks`` never collapses
                # spans from different writers.
                group_name = f"{_GROUP}-{run_id}-{span_index:02d}"
                manager.record_span(
                    product=_PRODUCT,
                    run_id=run_id,
                    batch_id=_writer_batch_id(run_id, span_index),
                    group=group_name,
                    status="active",
                    coverage=SpanCoverage(
                        group=group_name,
                        arrays=[f"{group_name}/FWI"],
                        time_index_ranges=[[span_index, span_index]],
                        time_min=_TIME_MIN,
                        time_max=_TIME_MAX,
                    ),
                    meta={
                        **base_meta,
                        "group": group_name,
                        "time_min": _TIME_MIN,
                        "time_max": _TIME_MAX,
                        "span_index": span_index,
                    },
                )
                outcome.events_recorded += 1
            manager.record_run_terminal(
                product=_PRODUCT,
                run_id=run_id,
                output_path=output_path,
                output_format="zarr",
                size=_SPANS_PER_WRITER,
                meta=base_meta,
                status="complete",
            )
            outcome.events_recorded += 1
        except BaseException as exc:
            outcome.unexpected_errors.append(exc)
            outcome.error = exc
    finally:
        with lock:
            outcomes.append(outcome)
        manager.close()


def _spawn_writers(tmp_path: Path) -> list[_WriterOutcome]:
    """Start ``_WRITER_THREAD_COUNT`` writers behind one barrier, join all."""
    barrier = threading.Barrier(_WRITER_THREAD_COUNT)
    outcomes: list[_WriterOutcome] = []
    lock = threading.Lock()
    threads = [
        threading.Thread(
            target=_run_writer,
            kwargs={
                "tmp_path": tmp_path,
                "run_id": _writer_run_id(idx),
                "barrier": barrier,
                "outcomes": outcomes,
                "lock": lock,
            },
            name=f"wal-writer-{idx:02d}",
        )
        for idx in range(_WRITER_THREAD_COUNT)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=_JOIN_TIMEOUT_S)
    for thread in threads:
        assert not thread.is_alive(), (
            f"Writer thread {thread.name!r} did not join within {_JOIN_TIMEOUT_S}s; "
            "possible deadlock in WAL writer or filesystem ops"
        )
    return outcomes


def _assert_writer_outcomes(outcomes: list[_WriterOutcome]) -> None:
    assert len(outcomes) == _WRITER_THREAD_COUNT, (
        f"Expected {_WRITER_THREAD_COUNT} writer outcomes, got {len(outcomes)}"
    )
    unexpected = [err for outcome in outcomes for err in outcome.unexpected_errors]
    assert not unexpected, f"Writer threads raised unexpected errors: {unexpected!r}"
    for outcome in outcomes:
        assert outcome.error is None, (
            f"Writer {outcome.run_id!r} failed with {type(outcome.error).__name__}: "
            f"{outcome.error!r}"
        )
        assert outcome.events_recorded == _TOTAL_EVENTS_PER_WRITER, (
            f"Writer {outcome.run_id!r} recorded {outcome.events_recorded} events; "
            f"expected exactly {_TOTAL_EVENTS_PER_WRITER} "
            f"(1 run_started + {_SPANS_PER_WRITER} spans + 1 run_completed)"
        )
    recorded_run_ids = {outcome.run_id for outcome in outcomes}
    assert recorded_run_ids == _expected_run_ids(), (
        f"Writer outcomes cover run_ids {recorded_run_ids!r}; expected {_expected_run_ids()!r}"
    )


def _collect_wal_segment_paths(tmp_path: Path) -> list[Path]:
    runs_dir = tmp_path / _PRODUCT / CONTROL_DIRNAME / RUNS_DIRNAME
    assert runs_dir.is_dir(), f"Expected control-plane runs dir at {runs_dir}, not found on disk"
    return sorted(runs_dir.glob("*/events-*.jsonl"))


def _parse_wal_events(tmp_path: Path) -> list[dict[str, Any]]:
    """Read the WAL straight off disk, asserting every line is intact."""
    events: list[dict[str, Any]] = []
    for path in _collect_wal_segment_paths(tmp_path):
        raw = path.read_text(encoding="utf-8")
        assert raw.endswith("\n"), (
            f"WAL segment {path} is missing the trailing newline that the "
            "writer always emits; this looks like a torn write"
        )
        for line_no, line in enumerate(raw.splitlines(), start=1):
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError as exc:
                raise AssertionError(
                    f"WAL segment {path} contains malformed JSON at line {line_no}: {exc}"
                ) from exc
            assert event.get("schema_version") == SCHEMA_VERSION, (
                f"WAL event in {path}:{line_no} has unexpected schema_version "
                f"{event.get('schema_version')!r}; expected {SCHEMA_VERSION!r}"
            )
            events.append(event)
    return events


def _assert_wal_event_counts(events: list[dict[str, Any]]) -> None:
    expected_total = _WRITER_THREAD_COUNT * _TOTAL_EVENTS_PER_WRITER
    assert len(events) == expected_total, (
        f"WAL contains {len(events)} events; expected exactly {expected_total} "
        f"({_WRITER_THREAD_COUNT} writers x {_TOTAL_EVENTS_PER_WRITER} events)"
    )
    events_by_run: dict[str, list[dict[str, Any]]] = {}
    for event in events:
        events_by_run.setdefault(str(event.get("run_id", "")), []).append(event)
    assert set(events_by_run) == _expected_run_ids(), (
        f"WAL spans run_ids {sorted(events_by_run)!r}; expected {sorted(_expected_run_ids())!r}"
    )
    expected_per_run_histogram = Counter(
        {
            "run_started": 1,
            "span_committed": _SPANS_PER_WRITER,
            "run_completed": 1,
        }
    )
    for run_id, run_events in events_by_run.items():
        assert len(run_events) == _TOTAL_EVENTS_PER_WRITER, (
            f"Run {run_id!r} has {len(run_events)} WAL events; expected {_TOTAL_EVENTS_PER_WRITER}"
        )
        histogram = Counter(str(event.get("event_type", "")) for event in run_events)
        assert histogram == expected_per_run_histogram, (
            f"Run {run_id!r} has event_type histogram {dict(histogram)!r}; "
            f"expected {dict(expected_per_run_histogram)!r}"
        )


def _assert_wal_event_ids_unique(events: list[dict[str, Any]]) -> None:
    event_ids = [str(event.get("event_id", "")) for event in events]
    duplicates = [eid for eid, count in Counter(event_ids).items() if count > 1]
    assert not duplicates, (
        f"WAL contains duplicate event_ids — concurrent writers should never "
        f"share an id namespace: {duplicates!r}"
    )


def _span_keyset(spans: list[Any]) -> set[tuple[str, str]]:
    return {
        (
            str((span.meta or {}).get("run_id", "")),
            str((span.meta or {}).get("batch_id", "")),
        )
        for span in spans
    }


def _collect_live_projection(tmp_path: Path) -> tuple[list[Any], list[Any]]:
    """Read live ``list_runs`` + ``list_chunks`` via a fresh ChunkManager."""
    verifier = ChunkManager(
        binding=make_test_binding(tmp_path, product=_PRODUCT),
        workspace=tmp_path / "workspace-verify-live",
    )
    try:
        runs = verifier.list_runs(product=_PRODUCT)
        spans = verifier.list_chunks(product=_PRODUCT, chunk_type="span", include_replaced=True)
        return runs, spans
    finally:
        verifier.close()


def _collect_snapshot_projection(tmp_path: Path) -> list[Any]:
    verifier = ChunkManager(
        binding=make_test_binding(tmp_path, product=_PRODUCT),
        workspace=tmp_path / "workspace-verify-snapshot",
    )
    try:
        return verifier.list_chunks(product=_PRODUCT, chunk_type="span", include_replaced=False)
    finally:
        verifier.close()


def _assert_runs_complete(runs: list[Any]) -> None:
    assert len(runs) == _WRITER_THREAD_COUNT, (
        f"list_runs returned {len(runs)} runs; expected {_WRITER_THREAD_COUNT}"
    )
    statuses = Counter(run.status for run in runs)
    assert statuses == Counter({"complete": _WRITER_THREAD_COUNT}), (
        f"list_runs status histogram is {dict(statuses)!r}; expected all complete"
    )
    assert {run.run_id for run in runs} == _expected_run_ids(), (
        f"list_runs returned run_ids {{ {sorted(run.run_id for run in runs)!r} }}; "
        f"expected {sorted(_expected_run_ids())!r}"
    )


def _assert_spans_match_expected(spans: list[Any]) -> None:
    expected_keys = _expected_batch_keys()
    actual_keys = _span_keyset(spans)
    assert actual_keys == expected_keys, (
        f"Projected span (run_id, batch_id) set differs from expected; "
        f"missing={sorted(expected_keys - actual_keys)!r}, "
        f"extra={sorted(actual_keys - expected_keys)!r}"
    )
    statuses = Counter(span.status for span in spans)
    assert statuses == Counter({"active": _WRITER_THREAD_COUNT * _SPANS_PER_WRITER}), (
        f"Span status histogram is {dict(statuses)!r}; expected all active"
    )
    duplicates = [
        key for key, count in Counter(_span_keyset_with_dupes(spans)).items() if count > 1
    ]
    assert not duplicates, (
        f"Projection has duplicate (run_id, batch_id) span entries: {duplicates!r}"
    )


def _span_keyset_with_dupes(spans: list[Any]) -> list[tuple[str, str]]:
    """Like ``_span_keyset`` but preserves duplicates so we can spot them."""
    return [
        (
            str((span.meta or {}).get("run_id", "")),
            str((span.meta or {}).get("batch_id", "")),
        )
        for span in spans
    ]


def _rebuild_snapshot(tmp_path: Path) -> dict[str, Any]:
    rebuilder = ChunkManager(
        binding=make_test_binding(tmp_path, product=_PRODUCT),
        workspace=tmp_path / "workspace-rebuild",
    )
    try:
        return rebuilder.rebuild_snapshot(_PRODUCT)
    finally:
        rebuilder.close()


def _assert_snapshot_rebuild_matches_live(
    tmp_path: Path,
    live_spans: list[Any],
) -> None:
    """rebuild_snapshot, then re-read; the snapshot must mirror the live WAL projection."""
    rebuild_result = _rebuild_snapshot(tmp_path)
    expected_records = _WRITER_THREAD_COUNT * (1 + _SPANS_PER_WRITER)
    assert rebuild_result.get("records") == expected_records, (
        f"rebuild_snapshot reported {rebuild_result.get('records')!r} records; "
        f"expected {expected_records} "
        f"({_WRITER_THREAD_COUNT} run records + {_WRITER_THREAD_COUNT * _SPANS_PER_WRITER} span records)"
    )

    snapshot_spans = _collect_snapshot_projection(tmp_path)
    snapshot_keys = _span_keyset(snapshot_spans)
    live_keys = _span_keyset(live_spans)
    assert snapshot_keys == live_keys, (
        f"Snapshot projection diverges from live WAL projection; "
        f"missing-from-snapshot={sorted(live_keys - snapshot_keys)!r}, "
        f"extra-in-snapshot={sorted(snapshot_keys - live_keys)!r}"
    )


def test_wal_event_ordering_under_concurrent_batch_completion(tmp_path: Path) -> None:
    """8 writer threads x 20 events each → WAL stays consistent and complete.

    Asserts that under maximum-concurrency batch completion the control
    plane never loses, duplicates, or corrupts an event, and that the
    snapshot rebuild is a faithful cache of the live projection.
    """
    outcomes = _spawn_writers(tmp_path)
    _assert_writer_outcomes(outcomes)

    events = _parse_wal_events(tmp_path)
    _assert_wal_event_counts(events)
    _assert_wal_event_ids_unique(events)

    runs, spans = _collect_live_projection(tmp_path)
    _assert_runs_complete(runs)
    _assert_spans_match_expected(spans)

    _assert_snapshot_rebuild_matches_live(tmp_path, spans)


def _write_single_run_lifecycle(tmp_path: Path, run_id: str) -> None:
    binding = make_test_binding(tmp_path, product=_PRODUCT)
    manager = ChunkManager(
        binding=binding,
        workspace=tmp_path / f"workspace-{run_id}",
    )
    output_path = f"{manager.base_uri.rstrip('/')}/{_PRODUCT}"
    meta = {"plugin": "test_wal_concurrent_ordering", "run_id": run_id}
    try:
        manager.record_run_started(
            product=_PRODUCT,
            run_id=run_id,
            output_path=output_path,
            output_format="zarr",
            size=0,
            meta=meta,
        )
        manager.record_span(
            product=_PRODUCT,
            run_id=run_id,
            batch_id="batch-0",
            group=_GROUP,
            status="active",
            coverage=SpanCoverage(
                group=_GROUP,
                arrays=[f"{_GROUP}/FWI"],
                time_index_ranges=[[0, 0]],
                time_min=_TIME_MIN,
                time_max=_TIME_MAX,
            ),
            meta={
                **meta,
                "group": _GROUP,
                "time_min": _TIME_MIN,
                "time_max": _TIME_MAX,
            },
        )
        manager.record_run_terminal(
            product=_PRODUCT,
            run_id=run_id,
            output_path=output_path,
            output_format="zarr",
            size=1,
            meta=meta,
            status="complete",
        )
    finally:
        manager.close()


def test_torn_wal_segment_raises_controlplane_corruption_error(
    tmp_path: Path,
) -> None:
    """Negative control: a manually torn WAL line must trip the corruption guard.

    The runtime's torn-tail recovery accepts a missing trailing newline on
    the last line of the last segment (an active-write crash). Any other
    shape — like a malformed line followed by a newline — must raise
    :class:`ControlPlaneCorruptionError` so the read path never silently
    skips a record.
    """
    run_id = "corrupted-run"
    _write_single_run_lifecycle(tmp_path, run_id)

    run_dir = tmp_path / _PRODUCT / CONTROL_DIRNAME / RUNS_DIRNAME / run_id
    segments = sorted(run_dir.glob("events-*.jsonl"))
    assert segments, (
        f"Expected at least one WAL segment under {run_dir}; found none — "
        "the writer never persisted the run_started event"
    )
    target_segment = segments[0]
    original_text = target_segment.read_text(encoding="utf-8")
    assert original_text.endswith("\n"), (
        "Pre-condition: writer-produced segments must end with a newline; "
        f"got tail {original_text[-32:]!r}"
    )
    # Trailing newline disables torn-tail recovery — the corrupt line is
    # treated as a *committed* malformed event, which is the contract we
    # want to assert on.
    target_segment.write_text(
        original_text + "{this is intentionally not valid json}\n",
        encoding="utf-8",
    )

    verifier = ChunkManager(
        binding=make_test_binding(tmp_path, product=_PRODUCT),
        workspace=tmp_path / "workspace-verify-corrupt",
    )
    try:
        with pytest.raises(ControlPlaneCorruptionError) as exc_info:
            verifier.list_chunks(product=_PRODUCT, include_replaced=True)
        message = str(exc_info.value)
        assert "Corrupt WAL event" in message, (
            f"Expected 'Corrupt WAL event' in the corruption error message; got: {message!r}"
        )
        assert target_segment.name in message, (
            f"Expected the corrupted segment filename {target_segment.name!r} "
            f"in the error message; got: {message!r}"
        )
    finally:
        verifier.close()
