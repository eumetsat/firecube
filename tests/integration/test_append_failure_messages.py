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

"""Interior batch failure through the CLI: fail loud, stop, recover truthfully.

30 daily files ingested in batches of 10 with day 15 corrupt: the run stops
at the failed batch, the store holds days 1 to 10 only, the WAL records the
committed span, the failed span and nothing for the batch that was never
attempted, and the two documented recoveries (``resume_existing`` after
repairing the input, ``force_reingest`` over the succeeded span) leave a
unique, monotonic time axis.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path

import numpy as np
import pytest

pytestmark = pytest.mark.integration

_REPO_ROOT = Path(__file__).resolve().parents[2]
_FIXTURE_PLUGINS = _REPO_ROOT / "tests" / "fixtures" / "firecube_test_plugins"
_SYNTHETIC_PRECIP = _REPO_ROOT / "tests" / "fixtures" / "gen_synthetic_precip_daily.py"

_FAILED_BATCH_ID = "precip_daily_batch_0001"


def _generate_days(out_dir: Path, start_day: int, end_day: int) -> None:
    result = subprocess.run(
        [sys.executable, str(_SYNTHETIC_PRECIP), str(out_dir), str(start_day), str(end_day)],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def _corrupt_day_15(source: Path) -> None:
    _generate_days(source, 1, 30)
    (source / "precip_20240115.nc").write_bytes(b"NOT_AN_HDF5\n")


def _repair_day_15(source: Path) -> None:
    _generate_days(source, 15, 15)


def _firecube(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "uv",
            "run",
            "--with-editable",
            str(_REPO_ROOT),
            "--with-editable",
            str(_FIXTURE_PLUGINS),
            "firecube",
            *args,
        ],
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )


def _ingest(source: Path, target: Path, *extra: str) -> subprocess.CompletedProcess[str]:
    return _firecube(
        "ingest",
        "precip_daily",
        "--input-data",
        str(source),
        "--target",
        target.as_uri(),
        "--product-name",
        "precip_daily",
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--output-format",
        "zarr",
        "--write-mode",
        "direct",
        "--option",
        "layout=areastats",
        "--option",
        "pipeline_batch_size=10",
        "--option",
        "no_progress=true",
        *extra,
    )


def _validate(target: Path) -> subprocess.CompletedProcess[str]:
    return _firecube("zarr", "validate", "-p", target.as_uri(), "-g", "default")


def _failed_store(tmp_path: Path) -> tuple[Path, Path, subprocess.CompletedProcess[str]]:
    source = tmp_path / "source"
    target = tmp_path / "precip_daily.zarr"
    _corrupt_day_15(source)
    first = _ingest(source, target)
    assert first.returncode == 1, "corrupt first ingest should fail\n" + first.stdout + first.stderr
    _assert_days(target, range(1, 11))
    return source, target, first


def _timestamps(target: Path) -> np.ndarray:
    import xarray as xr

    ds = xr.open_zarr(str(target), group="default", consolidated=False, zarr_format=3)
    try:
        return np.asarray(ds["time"].values)
    finally:
        ds.close()


def _days_of(timestamps: np.ndarray) -> list[int]:
    """Return the January day numbers of ``timestamps`` (2024-01-01 is day 1)."""
    offsets = timestamps.astype("datetime64[D]") - np.datetime64("2024-01-01")
    return [int(day) + 1 for day in (offsets / np.timedelta64(1, "D")).astype(int)]


def _assert_days(target: Path, days: range) -> None:
    timestamps = _timestamps(target)
    assert _days_of(timestamps) == list(days)


def _store_digest(target: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(item for item in target.rglob("*") if item.is_file()):
        digest.update(str(path.relative_to(target)).encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _wal_events_by_run(target: Path) -> dict[str, list[dict]]:
    """Return every WAL event of the store's control plane, keyed by run id."""
    runs_dir = target / ".firecube" / "runs"
    events: dict[str, list[dict]] = {}
    for run_dir in sorted(path for path in runs_dir.iterdir() if path.is_dir()):
        run_events: list[dict] = []
        for path in sorted(run_dir.glob("events-*.jsonl")):
            run_events.extend(
                json.loads(line)
                for line in path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            )
        events[run_dir.name] = run_events
    return events


def _active_spans(tmp_path: Path, target: Path) -> list[tuple[str, list[list[int]]]]:
    """Return ``(key, time_index_ranges)`` of every active span in the store's control plane."""
    from firecube.core.controlplane import ChunkManager
    from tests.helpers.storage import make_test_binding

    manager = ChunkManager(
        binding=make_test_binding(tmp_path, product=target.name), workspace=tmp_path / "work"
    )
    chunks = manager.list_chunks(product=target.name, chunk_type="span", status="active")
    return [(chunk.key, (chunk.record or {})["span"]["time_index_ranges"]) for chunk in chunks]


def _run_id_from_error(result: subprocess.CompletedProcess[str]) -> str:
    combined = result.stdout + result.stderr
    marker = "Pipeline run '"
    start = combined.index(marker) + len(marker)
    return combined[start : combined.index("'", start)]


def _span_events(events: list[dict]) -> list[tuple[str, str, list[list[int]]]]:
    """Return ``(event_type, batch_id, time_index_ranges)`` per span event, in order."""
    spans: list[tuple[str, str, list[list[int]]]] = []
    for event in events:
        if not event["event_type"].startswith("span_"):
            continue
        record = event["record"]
        spans.append(
            (
                event["event_type"],
                record["meta"]["batch_id"],
                record["span"]["time_index_ranges"],
            )
        )
    return spans


def _terminal_run_record(events: list[dict], status: str) -> dict:
    matches = [
        event["record"]
        for event in events
        if event["event_type"] == f"run_{'completed' if status == 'complete' else status}"
    ]
    assert len(matches) == 1, [event["event_type"] for event in events]
    return matches[0]


def _assert_plain_rerun_refused(result: subprocess.CompletedProcess[str]) -> str:
    combined = result.stdout + result.stderr
    assert result.returncode == 1, combined
    assert "plain re-run is refused" in combined
    assert "resume_existing=true" in combined
    assert "force_reingest=true" in combined
    return combined


def _assert_complete_unique_monotonic_store(target: Path) -> np.ndarray:
    timestamps = _timestamps(target)
    assert len(timestamps) == 30
    assert len(np.unique(timestamps)) == 30
    assert np.all(np.diff(timestamps) > np.timedelta64(0, "s"))
    assert _days_of(timestamps) == list(range(1, 31))
    return timestamps


def test_interior_failure_stops_the_run_and_records_the_truth(tmp_path: Path) -> None:
    """Day 15 corrupt: rc 1, days 1 to 10 in the store, one failed and one not-attempted batch."""
    _source, target, first = _failed_store(tmp_path)
    combined = first.stdout + first.stderr

    assert f"after failed batch {_FAILED_BATCH_ID}" in combined
    assert "1 pending batch(es) not attempted" in combined
    assert "had 1 failed batch(es)" in combined
    assert "1 later batch(es) were not attempted" in combined
    assert "Run recorded as status=failed" in combined
    assert "--option resume_existing=true" in combined
    assert "--option force_reingest=true" in combined

    validate = _validate(target)
    assert validate.returncode == 0, validate.stdout + validate.stderr

    run_id = _run_id_from_error(first)
    events = _wal_events_by_run(target)[run_id]
    assert _span_events(events) == [
        ("span_committed", "precip_daily_batch_0000", [[0, 9]]),
        ("span_failed", _FAILED_BATCH_ID, []),
    ]
    assert _terminal_run_record(events, "failed")["status"] == "failed"


def test_plain_rerun_after_failed_run_raises_resume_conflict_error(tmp_path: Path) -> None:
    source, target, _first = _failed_store(tmp_path)

    result = _ingest(source, target)

    _assert_plain_rerun_refused(result)


def test_message_names_both_resume_existing_and_force_reingest(tmp_path: Path) -> None:
    source, target, _first = _failed_store(tmp_path)

    combined = _assert_plain_rerun_refused(_ingest(source, target))

    assert "Re-run with --option resume_existing=true" in combined
    assert "--option force_reingest=true" in combined


def test_message_does_not_claim_control_plane_will_process(tmp_path: Path) -> None:
    source, target, first = _failed_store(tmp_path)
    initial_message = first.stdout + first.stderr
    refused_message = _assert_plain_rerun_refused(_ingest(source, target))

    assert "control plane will attempt to process" not in initial_message
    assert "control plane will attempt to process" not in refused_message


def test_store_not_mutated_by_refused_rerun(tmp_path: Path) -> None:
    source, target, _first = _failed_store(tmp_path)
    before = _store_digest(target)

    _assert_plain_rerun_refused(_ingest(source, target))

    assert _store_digest(target) == before


def test_resume_existing_after_repair_completes_the_store(tmp_path: Path) -> None:
    """Repair day 15, resume: 30 unique monotonic days, the 10 present days skipped."""
    source, target, first = _failed_store(tmp_path)
    _repair_day_15(source)

    result = _ingest(source, target, "--option", "resume_existing=true")

    assert result.returncode == 0, result.stdout + result.stderr
    _assert_complete_unique_monotonic_store(target)

    validate = _validate(target)
    assert validate.returncode == 0, validate.stdout + validate.stderr

    events_by_run = _wal_events_by_run(target)
    first_run_id = _run_id_from_error(first)
    (resume_run_id,) = [run_id for run_id in events_by_run if run_id != first_run_id]
    resume_events = events_by_run[resume_run_id]
    completed = _terminal_run_record(resume_events, "complete")
    assert completed["meta"]["timestamps_skipped"] == 10
    assert sorted(_span_events(resume_events), key=lambda span: span[1]) == [
        ("span_noop", "precip_daily_batch_0000", []),
        ("span_committed", "precip_daily_batch_0001", [[10, 19]]),
        ("span_committed", "precip_daily_batch_0002", [[20, 29]]),
    ]


def test_force_reingest_after_failure_replaces_the_succeeded_span(tmp_path: Path) -> None:
    """Force-reingest days 1 to 10 over the failed run: monotonic axis, prior span replaced."""
    _source, target, first = _failed_store(tmp_path)
    redo_source = tmp_path / "redo_succeeded_span"
    _generate_days(redo_source, 1, 10)
    before = _store_digest(target)

    result = _ingest(redo_source, target, "--option", "force_reingest=true")

    assert result.returncode == 0, result.stdout + result.stderr
    timestamps = _timestamps(target)
    assert _days_of(timestamps) == list(range(1, 11))
    assert np.all(np.diff(timestamps) > np.timedelta64(0, "s"))
    assert _store_digest(target) != before

    events_by_run = _wal_events_by_run(target)
    first_run_id = _run_id_from_error(first)
    (force_run_id,) = [run_id for run_id in events_by_run if run_id != first_run_id]
    assert _span_events(events_by_run[force_run_id]) == [
        ("span_committed", "precip_daily_batch_0000", [[0, 9]]),
    ]

    # The active projection holds exactly the force run's span over days 1 to
    # 10; the failed run's span over the same slots is no longer active.
    active = _active_spans(tmp_path, target)
    assert active == [(f"span_{force_run_id}_precip_daily_batch_0000_default", [[0, 9]])]


def test_plain_rerun_on_clean_state_still_works(tmp_path: Path) -> None:
    source = tmp_path / "source"
    target = tmp_path / "precip_daily.zarr"
    _generate_days(source, 1, 30)

    result = _ingest(source, target)

    assert result.returncode == 0, result.stdout + result.stderr
    _assert_complete_unique_monotonic_store(target)
