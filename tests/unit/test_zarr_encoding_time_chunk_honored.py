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

"""The configured time chunk shape reaches the time coordinate and state array.

Before the fix, ``_build_zarr_encoding`` only emitted ``chunks`` in the sharded
path and only for ``ds.data_vars``. The time coordinate (a coord, not a data
var) and the ``firecube_timestamp_state`` array (a numpy-backed data var) were
left to xarray/zarr auto-chunking, which produced a chunk layout that did not
match the configured ``zarr_chunk_shape`` and broke the alignment tracker's
consistency check across arrays.

These tests pin the fix by running an actual ingest via the CLI and reading
back the persisted chunk metadata from the on-disk Zarr store.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import cast

import numpy as np
import pytest
import xarray as xr
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli

pytestmark = pytest.mark.unit


_PLUGIN = "precip_daily"
_PRODUCT = "precip_daily"
_DATA_GROUP = "default"
_TIME_CHUNK = 5
_NUM_DAYS = 12


def _generate_precip_inputs(dest: Path, n_days: int) -> None:
    generator = (
        Path(__file__).resolve().parent.parent / "fixtures" / "gen_synthetic_precip_daily.py"
    )
    result = subprocess.run(
        [sys.executable, str(generator), str(dest), "1", str(n_days)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, f"synthetic input generator failed: {result.stderr}"


def _ingest_args(
    tmp_path: Path,
    target: Path,
    *,
    chunk_shape: dict[str, int] | None,
) -> list[str]:
    source = tmp_path / "inputs"
    source.mkdir(parents=True, exist_ok=True)
    _generate_precip_inputs(source, _NUM_DAYS)
    args = [
        "ingest",
        _PLUGIN,
        "--input-data",
        str(source),
        "--target",
        f"file://{target}",
        "--product-name",
        _PRODUCT,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
        "--output-format",
        "zarr",
    ]
    if chunk_shape is not None:
        args.extend(["--option", f"zarr_chunk_shape={json.dumps(chunk_shape)}"])
    return args


def _open_data_group(target: Path) -> zarr.Group:
    return cast(zarr.Group, zarr.open_group(str(target), mode="r")[_DATA_GROUP])  # type: ignore[index] # zarr dynamic typing


def _run_ingest(tmp_path: Path, target: Path, chunk_shape: dict[str, int] | None) -> None:
    args = _ingest_args(tmp_path, target, chunk_shape=chunk_shape)
    result = CliRunner().invoke(cli, args)
    assert result.exit_code == 0, f"ingest failed:\n{result.output}"
    assert target.exists(), f"target not created; output:\n{result.output}"


def test_time_chunk_applied_to_time_coord(tmp_path: Path) -> None:
    target = tmp_path / "precip.zarr"
    _run_ingest(tmp_path, target, chunk_shape={"time": _TIME_CHUNK})

    time_arr = cast(zarr.Array, _open_data_group(target)["time"])  # type: ignore[index] # zarr dynamic typing
    assert time_arr.chunks[0] == _TIME_CHUNK, (
        f"time coord chunk[0]={time_arr.chunks[0]!r} did not match "
        f"configured zarr_chunk_shape['time']={_TIME_CHUNK}; "
        "E5 regression — zarr_chunk_shape must reach the time coord."
    )


def test_time_coord_keeps_source_cf_encoding_when_chunked(tmp_path: Path) -> None:
    """Adding a chunks entry for the coordinate must not re-encode it.

    The generator writes ``time`` as int32 "days since 2000-01-01"; an
    explicit encoding entry replaces xarray's variable encoding, so the
    engine carries the CF keys forward.
    """
    target = tmp_path / "precip.zarr"
    _run_ingest(tmp_path, target, chunk_shape={"time": _TIME_CHUNK})

    source = sorted((tmp_path / "inputs").glob("*.nc"))[0]
    with xr.open_dataset(source) as src:
        source_dtype = np.dtype(src["time"].encoding["dtype"])
        source_units = src["time"].encoding["units"]

    time_arr = cast(zarr.Array, _open_data_group(target)["time"])  # type: ignore[index] # zarr dynamic typing
    assert time_arr.chunks[0] == _TIME_CHUNK
    assert time_arr.dtype == source_dtype, (time_arr.dtype, source_dtype)
    # xarray normalises the units string on write; compare the epoch it encodes.
    stored_units = str(time_arr.attrs.get("units"))
    assert stored_units.startswith("days since ")
    assert stored_units.removeprefix("days since ").startswith(
        source_units.removeprefix("days since ").split(" ")[0]
    )


def test_time_chunk_applied_to_state_array(tmp_path: Path) -> None:
    target = tmp_path / "precip.zarr"
    _run_ingest(tmp_path, target, chunk_shape={"time": _TIME_CHUNK})

    state_arr = cast(zarr.Array, _open_data_group(target)["firecube_timestamp_state"])  # type: ignore[index] # zarr dynamic typing
    assert state_arr.chunks[0] == _TIME_CHUNK, (
        f"firecube_timestamp_state chunk[0]={state_arr.chunks[0]!r} did not "
        f"match configured zarr_chunk_shape['time']={_TIME_CHUNK}; "
        "E5 regression — state array chunk must match data-var chunk."
    )
