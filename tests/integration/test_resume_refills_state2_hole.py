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

"""Resume refills deleted and failed slots in place."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any, cast

import numpy as np
import pytest
import zarr

pytestmark = pytest.mark.integration

_REPO_ROOT = Path(__file__).resolve().parents[2]
_FIXTURE_PLUGINS = _REPO_ROOT / "tests" / "fixtures" / "firecube_test_plugins"
_SYNTHETIC_PRECIP = _REPO_ROOT / "tests" / "fixtures" / "gen_synthetic_precip_daily.py"


def _generate_days(out_dir: Path, start_day: int, end_day: int) -> None:
    result = subprocess.run(
        [sys.executable, str(_SYNTHETIC_PRECIP), str(out_dir), str(start_day), str(end_day)],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


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
            target.as_uri(),
            "--product-name",
            "f3",
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
            "no_progress=true",
            *extra,
        ],
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )


def _assert_ingest_ok(result: subprocess.CompletedProcess[str], label: str) -> None:
    assert result.returncode == 0, (
        f"{label} ingest failed with exit {result.returncode}\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )


def _open_group(target: Path) -> Any:
    return cast(Any, zarr.open_group(str(target), mode="r+", zarr_format=3))["default"]


def _poison_hole(target: Path, *, state_value: int) -> None:
    group = _open_group(target)
    group["firecube_timestamp_state"][3:6] = np.full((3,), state_value, dtype=np.uint8)
    group["precipitation"][3:6, :, :] = np.nan


def _read_result(target: Path) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    group = _open_group(target)
    timestamps = np.asarray(group["time"][:])
    state = np.asarray(group["firecube_timestamp_state"][:])
    precipitation = np.asarray(group["precipitation"][3:6, :, :])
    return timestamps, state, precipitation


def _run_refill(tmp_path: Path, *, state_value: int) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    initial = tmp_path / "days_1_10"
    refill = tmp_path / "days_4_6"
    target = tmp_path / "precip_daily.zarr"
    _generate_days(initial, 1, 10)
    _generate_days(refill, 4, 6)

    _assert_ingest_ok(_ingest(initial, target), "initial 10-day")
    _poison_hole(target, state_value=state_value)
    _assert_ingest_ok(
        _ingest(refill, target, "--option", "resume_existing=true"),
        "resume_existing refill",
    )

    return _read_result(target)


def _assert_unique_monotonic_days(timestamps: np.ndarray, expected_count: int) -> None:
    assert timestamps.size == expected_count
    assert np.unique(timestamps).size == expected_count
    assert bool(np.all(np.diff(timestamps) > np.timedelta64(0, "ns")))


@pytest.mark.parametrize("state_value", [2, 3])
def test_resume_refills_state_hole(tmp_path: Path, state_value: int) -> None:
    """resume_existing dispatches region-write for state=2 (deleted) and state=3 (failed_batch) slots."""
    timestamps, state, precipitation = _run_refill(tmp_path, state_value=state_value)

    _assert_unique_monotonic_days(timestamps, 10)
    np.testing.assert_array_equal(state[3:6], np.ones((3,), dtype=np.uint8))
    assert not bool(np.isnan(precipitation).any())


def test_resume_skip_state1_still_works(tmp_path: Path) -> None:
    """Verified-correct: state=1 present slots still skipped on resume."""
    initial = tmp_path / "days_1_10"
    duplicate = tmp_path / "days_4_6"
    target = tmp_path / "precip_daily.zarr"
    _generate_days(initial, 1, 10)
    _generate_days(duplicate, 4, 6)

    _assert_ingest_ok(_ingest(initial, target), "initial 10-day")
    _assert_ingest_ok(
        _ingest(duplicate, target, "--option", "resume_existing=true"),
        "resume_existing duplicate skip",
    )

    timestamps, state, _precipitation = _read_result(target)
    _assert_unique_monotonic_days(timestamps, 10)
    np.testing.assert_array_equal(state[3:6], np.ones((3,), dtype=np.uint8))
