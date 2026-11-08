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

"""Integration tests for staged-mode value-dedup with custom time_dim_name="time".

Tests that CFTimeDimValueDedupIngestor (which declares time_dim_name="time")
correctly deduplicates re-ingests in staged mode because the engine seeds
data/time/c/* workspace chunks (not data/timestamp/c/*).
"""

from __future__ import annotations

import json
from itertools import pairwise
from pathlib import Path
from typing import Any, cast

import numpy as np
import pytest
import zarr
from click.testing import CliRunner, Result

from firecube.cli.main import cli
from firecube.core.zarr.time_decode import decode_time_array
from firecube.ingestor.api import ExecutionParams
from tests.helpers.storage import make_test_context

pytestmark = pytest.mark.integration

_T1 = "2024-10-01T00:00:00"
_T2 = "2024-10-01T01:00:00"
_SENTINEL_A = 100.0
_SENTINEL_B = 200.0
_PRODUCT = "cf_time_dim_value_dedup.zarr"


def _run_staged_ingest(
    tmp_path: Path,
    *,
    ts_iso: str,
    sentinel: float,
    resume_existing: bool,
    write_mode: str = "staged",
) -> None:
    from cf_time_dim_test_plugin import CFTimeDimValueDedupIngestor

    source_dir = tmp_path / "source"
    source_dir.mkdir(exist_ok=True)
    marker = source_dir / "input.nc"
    if not marker.exists():
        marker.touch()

    ctx = make_test_context(
        tmp_path,
        source=str(source_dir),
        product=_PRODUCT,
        options={
            "resume_existing": resume_existing,
            "pipeline_batch_size": 1,
            "pipeline_workers": 1,
            "no_progress": True,
            "cleanup_workspace": True,
            "x_ts_iso": ts_iso,
            "x_sentinel": sentinel,
        },
        execution=ExecutionParams(write_mode=write_mode),
    )
    ingestor = CFTimeDimValueDedupIngestor()
    result = ingestor.run(ctx)
    assert result.output_path, f"Run produced no output_path: {result!r}"


def _open_time_coord(final_zarr: Path) -> Any:
    root = zarr.open_group(str(final_zarr), mode="r")
    assert "data/time" in root
    assert "data/timestamp" not in root
    arr = cast(Any, root["data/time"])
    assert arr.metadata.dimension_names == ("time",)
    assert "_ARRAY_DIMENSIONS" not in dict(arr.attrs)
    return arr


def test_cf_time_dim_idempotent_reingest_staged_value_dedup(tmp_path: Path) -> None:
    """Re-ingesting the same timestamp 2 times in staged mode must stay shape==(1,).

    Proves that time_dim_name="time" flows to the seeder which seeds
    data/time/c/* (not data/timestamp/c/*) enabling value-based dedup.
    """
    _run_staged_ingest(tmp_path, ts_iso=_T1, sentinel=_SENTINEL_A, resume_existing=False)
    _run_staged_ingest(tmp_path, ts_iso=_T1, sentinel=_SENTINEL_A, resume_existing=True)
    _run_staged_ingest(tmp_path, ts_iso=_T1, sentinel=_SENTINEL_A, resume_existing=True)

    final_zarr = tmp_path / _PRODUCT
    arr = _open_time_coord(final_zarr)
    raw = np.asarray(arr[:]).astype("datetime64[s]")

    assert arr.shape == (1,), (
        f"Expected shape (1,) after 2 re-ingests with same timestamp, got {arr.shape}. "
        "Custom time_dim_name='time' did not flow through to staged-mode seeder."
    )
    assert len(set(raw.astype(str))) == 1, (
        f"Expected 1 distinct timestamp value, got {set(raw.astype(str))}"
    )


def test_cf_time_dim_distinct_append_staged_value_dedup(tmp_path: Path) -> None:
    """Appending a distinct timestamp in staged mode must produce shape==(2,)."""
    _run_staged_ingest(tmp_path, ts_iso=_T1, sentinel=_SENTINEL_A, resume_existing=False)
    _run_staged_ingest(tmp_path, ts_iso=_T2, sentinel=_SENTINEL_B, resume_existing=True)

    final_zarr = tmp_path / _PRODUCT
    arr = _open_time_coord(final_zarr)
    raw = np.asarray(arr[:]).astype("datetime64[s]")

    assert arr.shape == (2,), (
        f"Expected shape (2,) after distinct-timestamp append, got {arr.shape}."
    )
    assert raw[0] == np.datetime64(_T1, "s"), f"Slot 0 should be T1 ({_T1}), got {raw[0]}"
    assert raw[1] == np.datetime64(_T2, "s"), f"Slot 1 should be T2 ({_T2}), got {raw[1]}"


def _ingest_calendar(tmp_path: Path, *, mode: str, start: str, **flags: str) -> Result:
    source = tmp_path / "input"
    source.mkdir(exist_ok=True)
    (source / "dummy.nc").touch()
    options = ["no_progress=true", f"x_start={start}", *(f"{k}={v}" for k, v in flags.items())]
    args = [
        "ingest",
        "cf_time_dim_calendar",
        "--input-data",
        source.as_uri(),
        "--product-name",
        "cal",
    ]
    args += ["--target", f"file://{tmp_path / 'cal.zarr'}", "--write-mode", mode]
    args += [a for o in options for a in ("--option", o)]
    result = CliRunner().invoke(cli, args)
    assert result.exit_code == 0, result.output
    return result


def _stored_coord(tmp_path: Path) -> Any:
    root = zarr.open_group(str(tmp_path / "cal.zarr"), mode="r", use_consolidated=False)
    return cast(Any, root["default"])["time"]


@pytest.mark.parametrize("mode", ["direct", "staged"])
def test_360_day_append_over_two_runs_round_trips_through_february_30(
    tmp_path: Path, mode: str
) -> None:
    _ingest_calendar(tmp_path, mode=mode, start="2049-02-26")
    _ingest_calendar(tmp_path, mode=mode, start="2049-03-01", resume_existing="true")

    coord = _stored_coord(tmp_path)
    values = decode_time_array(coord[:], dict(coord.attrs))
    assert values.size == 10
    assert values.dtype.kind == "O"
    assert {v.calendar for v in values} == {"360_day"}
    assert all(a < b for a, b in pairwise(values))
    assert any(v.month == 2 and v.day == 30 for v in values)
    assert str(coord.dtype) == "int64"
    assert coord.attrs["calendar"] == "360_day"
    assert "units" in coord.attrs


def test_staged_force_reingest_over_a_feb_30_span_succeeds(tmp_path: Path) -> None:
    first = _ingest_calendar(tmp_path, mode="staged", start="2049-02-26")
    recorded = json.loads(first.output[first.output.rfind('{\n  "plugin"') :])
    assert recorded["metrics"]["zarr"]["coverage"][0]["time_max"] == "2049-02-30T00:00:00"

    _ingest_calendar(tmp_path, mode="staged", start="2049-03-01", force_reingest="true")

    assert _stored_coord(tmp_path).shape == (10,)
