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

from pathlib import Path
from typing import Any, cast

import numpy as np
import pytest
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.zarr._sealing_markers import ATTR_CONSOLIDATED_AT, ATTR_PREALLOCATED

pytestmark = pytest.mark.integration


def _args(cube: Path) -> list[str]:
    return [
        "zarr",
        "consolidate-time-coord",
        "--target",
        f"file://{cube}",
        "--product-name",
        cube.name,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
    ]


def _values(count: int) -> np.ndarray[Any, Any]:
    start = np.datetime64("2026-01-01T00:00:00", "ns")
    step = np.timedelta64(60, "s").astype("timedelta64[ns]")
    return start + np.arange(count, dtype=np.int64) * step


def _create_legacy_cube(cube: Path, count: int = 100) -> np.ndarray[Any, Any]:
    root = zarr.open_group(store=str(cube), mode="w", zarr_format=3)
    data = root.create_group("data")
    values = _values(count)
    arr = data.create_array(
        "time",
        shape=values.shape,
        dtype=values.dtype,
        chunks=(1,),
        dimension_names=("time",),
    )
    arr[...] = values
    return values


def _create_consolidating(cube: Path, values: np.ndarray[Any, Any]) -> None:
    data = zarr.open_group(store=str(cube), mode="a", path="data", zarr_format=3)
    temp = data.create_array(
        "time.consolidating",
        shape=values.shape,
        dtype=values.dtype,
        chunks=(min(len(values), 64),),
        dimension_names=("time",),
        overwrite=True,
    )
    temp[...] = values


def _time_array(cube: Path) -> Any:
    root = zarr.open_group(store=str(cube), mode="r", zarr_format=3)
    return cast(Any, root["data/time"])


def test_crash_pre_swap_re_run_discards_stale_temp_and_succeeds(tmp_path: Path) -> None:
    cube = tmp_path / "cube.zarr"
    values = _create_legacy_cube(cube, count=100)
    _create_consolidating(cube, values)

    result = CliRunner().invoke(cli, _args(cube))

    assert result.exit_code == 0, result.output
    assert "removed stale time.consolidating" in result.output
    assert not (cube / "data" / "time.consolidating").exists()
    arr = _time_array(cube)
    assert arr.attrs[ATTR_PREALLOCATED] is True
    assert ATTR_CONSOLIDATED_AT in arr.attrs
    assert np.array_equal(np.asarray(arr[:]), values)


def test_crash_post_delete_re_run_promotes_temp_and_succeeds(tmp_path: Path) -> None:
    cube = tmp_path / "cube.zarr"
    values = _create_legacy_cube(cube, count=100)
    _create_consolidating(cube, values)
    import shutil

    shutil.rmtree(cube / "data" / "time")

    result = CliRunner().invoke(cli, _args(cube))

    assert result.exit_code == 0, result.output
    assert "recovered partial consolidation" in result.output
    # A successful recovery must be counted by the summary.
    assert "no group with a time coord found" not in result.output, (
        f"Recovery succeeded but the summary reports no groups were found:\n{result.output}"
    )
    assert not (cube / "data" / "time.consolidating").exists()
    arr = _time_array(cube)
    assert arr.attrs[ATTR_PREALLOCATED] is True
    assert ATTR_CONSOLIDATED_AT in arr.attrs
    assert np.array_equal(np.asarray(arr[:]), values)


def test_ambiguous_backup_present_refuses(tmp_path: Path) -> None:
    cube = tmp_path / "cube.zarr"
    values = _create_legacy_cube(cube, count=100)
    first = CliRunner().invoke(cli, _args(cube))
    assert first.exit_code == 0, first.output
    _create_consolidating(cube, values)

    result = CliRunner().invoke(cli, _args(cube))

    assert result.exit_code != 0
    assert "unexpected" in result.output.lower()
    assert "ambiguous" in result.output.lower()
    assert (cube / "data" / "time").exists()
    assert (cube / "data" / "time.consolidating").exists()
