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

"""E2E: ingest with cf_time_dim_test_plugin (declares time_dim_name='time') and verify
the resulting Zarr cube uses 'time' on disk (not 'timestamp'), CF attrs survive,
the internal firecube_timestamp_state array stays named the same but tracks the
configured time dim, and `firecube advise compliance --profile cf-18` reports zero errors.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_CF_TIME_DIM_FIXTURE = _REPO_ROOT / "tests" / "fixtures" / "cf_time_dim_test_plugin"


def _ingest(target: Path, source: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "uv",
            "run",
            "--with-editable",
            str(_CF_TIME_DIM_FIXTURE),
            "firecube",
            "ingest",
            "cf_time_dim",
            "--input-data",
            str(source),
            "--target",
            f"file://{target}",
            "--product-name",
            "cf_e2e_time",
            "--storage-type",
            "local",
            "--storage-driver",
            "fsspec",
            "--output-format",
            "zarr",
            "--write-mode",
            "direct",
        ],
        capture_output=True,
        text=True,
    )


def _find_state_array(store: Any) -> Any:
    import zarr

    def _walk(grp: Any) -> Any:
        for name in grp.array_keys():
            if "firecube_timestamp_state" in name:
                return grp[name]
        for name in grp.group_keys():
            hit = _walk(grp[name])
            if hit is not None:
                return hit
        return None

    if not isinstance(store, zarr.Group):
        return None
    return _walk(store)


@pytest.mark.integration
def test_e2e_time_dim_plugin_ingest_and_readback(tmp_path: Path) -> None:
    """Full E2E: ingest with cf_time_dim plugin, read back, validate CF attrs.

    Confirms T3 (plugin declares time_dim_name='time') and T12 (state array
    dimension_names follow the configured dim) hold end-to-end.
    """
    target = tmp_path / "e2e_time.zarr"
    source = tmp_path / "empty_source"
    source.mkdir()
    (source / "dummy.nc").touch()

    result = _ingest(target, source)
    if result.returncode != 0:
        pytest.fail(
            f"firecube ingest failed (exit {result.returncode})\n"
            f"stdout: {result.stdout}\nstderr: {result.stderr}"
        )
    assert target.exists(), f"target {target} was not created"

    import xarray as xr

    ds = xr.open_zarr(str(target), group="default", consolidated=False, decode_times=False)

    assert "time" in ds.dims, f"expected 'time' in dims, got {list(ds.dims)}"
    assert "timestamp" not in ds.dims, f"unexpected 'timestamp' in dims, got {list(ds.dims)}"

    assert ds.attrs.get("Conventions") == "CF-1.8", (
        f"got Conventions={ds.attrs.get('Conventions')!r}"
    )
    assert "units" in ds["time"].attrs, "time coord missing units"
    assert " since " in ds["time"].attrs["units"], (
        f"time units malformed: {ds['time'].attrs['units']!r}"
    )
    assert ds["time"].attrs.get("standard_name") == "time"
    assert ds["temperature"].attrs.get("units") == "K"
    assert ds["temperature"].attrs.get("standard_name") == "air_temperature"

    import zarr

    store = zarr.open(str(target), mode="r")
    state_arr = _find_state_array(store)
    assert state_arr is not None, (
        "expected firecube_timestamp_state array to exist somewhere in the cube"
    )

    dim_names = tuple(state_arr.metadata.dimension_names or ())
    assert "time" in dim_names, f"state array dim_names={dim_names!r}; expected 'time' in it"
    assert "timestamp" not in dim_names, (
        f"state array dim_names={dim_names!r}; unexpected 'timestamp'"
    )

    advise_result = subprocess.run(
        [
            "uv",
            "run",
            "firecube",
            "advise",
            "compliance",
            "--profile",
            "cf-18",
            "--product",
            f"file://{target}",
            "--group",
            "default",
            "--storage-type",
            "local",
            "--storage-driver",
            "fsspec",
            "--format",
            "json",
        ],
        capture_output=True,
        text=True,
    )
    assert advise_result.returncode == 0, (
        f"advise compliance failed (exit {advise_result.returncode})\n"
        f"stdout: {advise_result.stdout}\nstderr: {advise_result.stderr}"
    )
    advise_out = json.loads(advise_result.stdout)
    assert advise_out["summary"]["errors"] == 0, (
        f"expected zero errors, got: {advise_out['findings']}"
    )


def _delete_span(target: Path, *extra: str) -> subprocess.CompletedProcess[str]:
    import os

    env = {
        **os.environ,
        "FIRECUBE_STORAGE_TYPE": "local",
        "FIRECUBE_STORAGE_DRIVER": "fsspec",
        "FIRECUBE_TARGET_PATH": str(target.parent),
    }
    return subprocess.run(
        [
            "uv",
            "run",
            "firecube",
            "chunks",
            "delete-span",
            "--product-name",
            target.name,
            *extra,
        ],
        capture_output=True,
        text=True,
        env=env,
    )


@pytest.mark.integration
def test_e2e_time_dim_plugin_delete_span(tmp_path: Path) -> None:
    """Span deletion resolves the plugin-declared time dim without --time-dim.

    Maintenance tooling used to be pinned to the default 'timestamp' dim and
    hard-failed on any cube written with a custom time_dim_name. The recorded
    span dim (with state-array discovery as fallback) must make deletion work
    with no extra flags, and an explicit --time-dim that contradicts the
    recorded dim must be refused without deleting anything.
    """
    target = tmp_path / "e2e_time.zarr"
    source = tmp_path / "empty_source"
    source.mkdir()
    (source / "dummy.nc").touch()

    result = _ingest(target, source)
    if result.returncode != 0:
        pytest.fail(
            f"firecube ingest failed (exit {result.returncode})\n"
            f"stdout: {result.stdout}\nstderr: {result.stderr}"
        )

    dry = _delete_span(target, "--dry-run")
    assert dry.returncode == 0, f"dry-run failed\nstdout: {dry.stdout}\nstderr: {dry.stderr}"
    assert "would delete 1 chunk keys" in dry.stdout, f"unexpected dry-run output: {dry.stdout}"

    conflict = _delete_span(target, "--time-dim", "timestamp", "--dry-run")
    assert conflict.returncode != 0, (
        f"contradicting --time-dim should fail\nstdout: {conflict.stdout}"
    )
    combined = conflict.stdout + conflict.stderr
    assert "contradicts" in combined, f"expected conflict error, got: {combined}"

    import zarr

    store = zarr.open(str(target), mode="r")
    data_chunks_before = list((target / "default" / "temperature" / "c").rglob("*"))
    assert data_chunks_before, "expected concrete chunks before deletion"
    del store

    deleted = _delete_span(target, "--yes-i-really-mean-it")
    assert deleted.returncode == 0, (
        f"delete-span failed\nstdout: {deleted.stdout}\nstderr: {deleted.stderr}"
    )
    assert "Deleted 1 chunk keys" in deleted.stdout, f"unexpected output: {deleted.stdout}"
