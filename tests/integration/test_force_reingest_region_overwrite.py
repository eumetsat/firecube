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

"""Force-reingest overwrites matching regions and appends new timestamps."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import numpy as np
import pytest

pytestmark = pytest.mark.integration

_REPO_ROOT = Path(__file__).resolve().parents[2]
_FIXTURE_PLUGINS = _REPO_ROOT / "tests" / "fixtures" / "firecube_test_plugins"
_SYNTHETIC_PRECIP = _REPO_ROOT / "tests" / "fixtures" / "gen_synthetic_precip_daily.py"


def _generate_days(
    out_dir: Path, start_day: int, end_day: int, *, time_resolution: str = "ns"
) -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(_SYNTHETIC_PRECIP),
            str(out_dir),
            str(start_day),
            str(end_day),
            "--time-resolution",
            time_resolution,
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, (
        f"synthetic precipitation generation failed\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )


def _ingest(source: Path, target: Path, *extra: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "uv",
            "run",
            "--with-editable",
            str(_REPO_ROOT),
            "--with-editable",
            str(_FIXTURE_PLUGINS),
            "firecube",
            "ingest",
            "precip_daily",
            "--input-data",
            str(source),
            "--target",
            f"file://{target}",
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
            *extra,
        ],
        capture_output=True,
        text=True,
        timeout=180,
    )


def _assert_ingest_ok(result: subprocess.CompletedProcess[str], label: str) -> None:
    assert result.returncode == 0, (
        f"{label} ingest failed with exit {result.returncode}\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )


def _read_store(target: Path) -> tuple[np.ndarray, np.ndarray]:
    import xarray as xr

    ds = xr.open_zarr(str(target), group="default", consolidated=False, zarr_format=3)
    try:
        timestamps = np.asarray(ds["time"].values)
        precipitation = np.asarray(ds["precipitation"].values)
    finally:
        ds.close()
    return timestamps, precipitation


def _assert_unique_monotonic_days(timestamps: np.ndarray, expected_count: int) -> None:
    assert len(timestamps) == expected_count
    assert len(np.unique(timestamps)) == expected_count
    assert bool(np.all(timestamps[:-1] <= timestamps[1:]))


def test_force_reingest_same_60_days_stays_unique_monotonic(tmp_path: Path) -> None:
    source = tmp_path / "days_1_60"
    target = tmp_path / "precip_daily.zarr"
    _generate_days(source, 1, 60)

    _assert_ingest_ok(_ingest(source, target), "initial 60-day")
    _assert_ingest_ok(
        _ingest(source, target, "--option", "force_reingest=true"),
        "force_reingest 60-day",
    )

    timestamps, precipitation = _read_store(target)
    _assert_unique_monotonic_days(timestamps, 60)
    assert not bool(np.isnan(precipitation).any())


def test_split_batch_30_existing_plus_30_new(tmp_path: Path) -> None:
    initial = tmp_path / "days_1_30"
    overlap_plus_tail = tmp_path / "days_15_45"
    target = tmp_path / "precip_daily.zarr"
    _generate_days(initial, 1, 30)
    _generate_days(overlap_plus_tail, 15, 45)

    _assert_ingest_ok(_ingest(initial, target), "initial 30-day")
    _assert_ingest_ok(
        _ingest(overlap_plus_tail, target, "--option", "force_reingest=true"),
        "split force_reingest",
    )

    timestamps, _precipitation = _read_store(target)
    _assert_unique_monotonic_days(timestamps, 45)


def test_force_reingest_mixed_resolution(tmp_path: Path) -> None:
    """force_reingest with [D] resolution batch into [ns] store produces no duplicates."""
    initial = tmp_path / "days_1_10_ns"
    overlap_plus_tail = tmp_path / "days_6_15_d"
    target = tmp_path / "precip_daily.zarr"
    _generate_days(initial, 1, 10)
    _generate_days(overlap_plus_tail, 6, 15, time_resolution="D")

    _assert_ingest_ok(_ingest(initial, target), "initial mixed-resolution 10-day")
    _assert_ingest_ok(
        _ingest(overlap_plus_tail, target, "--option", "force_reingest=true"),
        "mixed-resolution force_reingest",
    )

    timestamps, precipitation = _read_store(target)
    _assert_unique_monotonic_days(timestamps, 15)
    assert not bool(np.isnan(precipitation).any())


def test_insert_refused_store_unchanged(tmp_path: Path) -> None:
    sparse = tmp_path / "days_1_and_3"
    insert = tmp_path / "day_2"
    target = tmp_path / "precip_daily.zarr"
    _generate_days(sparse, 1, 1)
    _generate_days(sparse, 3, 3)
    _generate_days(insert, 2, 2)

    _assert_ingest_ok(_ingest(sparse, target), "sparse initial")
    before, _precipitation = _read_store(target)

    result = _ingest(insert, target, "--option", "force_reingest=true")

    assert result.returncode != 0, (
        "middle insert with force_reingest should fail\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )
    assert "insert" in (result.stdout + result.stderr).lower()

    after, _precipitation = _read_store(target)
    assert np.array_equal(after, before)
    expected_days = np.array(["2024-01-01", "2024-01-03"], dtype="datetime64[D]")
    assert np.array_equal(after.astype("datetime64[D]"), expected_days)
