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

"""Regression guard: ordinary Gregorian time paths are unchanged by CF-calendar support.

The branch under test (``77-request-support-cf-360-day-calendar``) adds a
``calendar`` axis property and a Gregorian/non-Gregorian ``CoordinateEncoding``
split to ``firecube.core.zarr.coord_materialization`` /
``firecube.core.zarr.region_writer``. This file drives the real CLI
(``click.testing.CliRunner``) against real local Zarr stores to prove the
always-worked Gregorian ``datetime64`` paths still behave exactly as before on
both templates:

* ``GenericZarrIngestor`` (xarray append), via the ``precip_daily`` fixture,
  in both ``staged`` and ``direct`` write modes: fresh ingest, resume append
  in order, force-reingest replace of an existing slice, and refusal of a
  non-resumed overlapping (out-of-order/duplicate) batch.
* ``DirectZarrIngestor`` in *legacy serial* mode (``index_spec`` returns
  ``None``), via the ``cf_time_dim_value_dedup`` fixture: the on-disk
  ``datetime64[s]`` timestamp coordinate still grows correctly across
  sequential single-item ingests.

A companion "no CF attrs" check for the DirectZarr coordinate path is
included: unlike ``GenericZarrIngestor`` (whose Gregorian time coordinate is
CF-encoded to ``int64`` + ``units``/``calendar`` by xarray's own
``to_zarr()``, independent of this branch), a DirectZarr-materialized
Gregorian coordinate is stored as raw ``datetime64`` with none of those CF
attrs (see ``coord_materialization.py::_gregorian_encoding``) -- that is the
shape this branch's refactor must keep byte-for-byte.

No mocks: every assertion reads real CLI output and real Zarr/npy arrays on
disk under ``tmp_path``.
"""

from __future__ import annotations

import hashlib
import subprocess
import sys
from pathlib import Path
from typing import Any, cast

import numpy as np
import pytest
import xarray as xr
import zarr
from click.testing import CliRunner, Result

from firecube.cli.main import cli

pytestmark = pytest.mark.integration

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SYNTHETIC_PRECIP = _REPO_ROOT / "tests" / "fixtures" / "gen_synthetic_precip_daily.py"
_LOCAL_STORAGE_FLAGS = ["--storage-type", "local", "--storage-driver", "fsspec"]


def _generate_days(out_dir: Path, start_day: int, end_day: int) -> None:
    """Materialise synthetic daily precipitation NetCDFs for days [start, end]."""
    result = subprocess.run(
        [sys.executable, str(_SYNTHETIC_PRECIP), str(out_dir), str(start_day), str(end_day)],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def _precip_args(
    source: Path,
    target: Path,
    *,
    write_mode: str,
    resume: bool = False,
    force: bool = False,
) -> list[str]:
    args = [
        "ingest",
        "precip_daily",
        "--input-data",
        str(source),
        "--target",
        f"file://{target}",
        "--product-name",
        "precip_daily",
        *_LOCAL_STORAGE_FLAGS,
        "--output-format",
        "zarr",
        "--write-mode",
        write_mode,
        "--option",
        "no_progress=true",
        "--option",
        "layout=areastats",
    ]
    if resume:
        args += ["--option", "resume_existing=true"]
    if force:
        args += ["--option", "force_reingest=true"]
    return args


def _run(args: list[str]) -> Result:
    return CliRunner().invoke(cli, args)


def _open_default(target: Path) -> xr.Dataset:
    return xr.open_zarr(str(target), group="default", consolidated=False, zarr_format=3)


def _store_hash(target: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(p for p in target.rglob("*") if p.is_file()):
        digest.update(path.relative_to(target).as_posix().encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()


# ---------------------------------------------------------------------------
# GenericZarrIngestor (precip_daily): staged resume-append order.
# ---------------------------------------------------------------------------


def test_staged_resume_appends_in_order_and_state_array_unchanged(tmp_path: Path) -> None:
    """Staged fresh ingest + resumed append stay strictly ordered.

    Also pins the always-worked shape of the append-tracking state array
    (``firecube_timestamp_state``): ``uint8``, one entry per time step.
    """
    source = tmp_path / "input"
    target = tmp_path / "precip.zarr"
    _generate_days(source, 1, 10)

    first = _run(_precip_args(source, target, write_mode="staged"))
    assert first.exit_code == 0, first.output

    _generate_days(source, 11, 20)
    second = _run(_precip_args(source, target, write_mode="staged", resume=True))
    assert second.exit_code == 0, second.output

    ds = _open_default(target)
    try:
        times = ds["time"].values
        assert times.size == 20
        assert np.all(np.diff(times) > np.timedelta64(0, "ns")), "time axis must stay monotonic"
        assert np.array_equal(
            times.astype("datetime64[D]"),
            np.arange("2024-01-01", "2024-01-21", dtype="datetime64[D]"),
        )
    finally:
        ds.close()

    root = zarr.open_group(store=str(target), mode="r", zarr_format=3)
    state = cast(Any, root["default"])["firecube_timestamp_state"]
    assert state.dtype == np.dtype("uint8")
    assert state.shape == (20,)


@pytest.mark.contract
def test_direct_zarr_gregorian_coordinate_has_no_cf_time_attrs(tmp_path: Path) -> None:
    """A DirectZarr Gregorian (undeclared-calendar) coordinate stays raw ``datetime64``.

    Pins the on-disk shape ``coord_materialization.py::_gregorian_encoding``
    must keep unchanged: no ``units``/``calendar`` attrs, dtype kind ``"M"``.
    This is the DirectZarr counterpart of the CF-encoded ``int64`` shape
    ``GenericZarrIngestor`` always wrote (xarray's own encoding, unrelated to
    this branch) -- the two templates are NOT expected to match on-disk
    encoding, only their own before/after behavior.
    """
    target = tmp_path / "cube.zarr"
    pre = _run(
        [
            "zarr",
            "preallocate",
            "direct_zarr_capable_test_plugin",
            "--target",
            f"file://{target}",
            "--product-name",
            "greg",
            *_LOCAL_STORAGE_FLAGS,
            "--write-mode",
            "direct",
        ]
    )
    assert pre.exit_code == 0, pre.output
    ing = _run(
        [
            "ingest",
            "direct_zarr_capable_test_plugin",
            "--target",
            f"file://{target}",
            "--product-name",
            "greg",
            *_LOCAL_STORAGE_FLAGS,
            "--write-mode",
            "direct",
            "--option",
            "no_progress=true",
        ]
    )
    assert ing.exit_code == 0, ing.output

    root = zarr.open_group(store=str(target), mode="r", zarr_format=3)
    coord = cast(Any, root["data"])["timestamp"]
    assert coord.dtype.kind == "M"
    attrs = dict(coord.attrs)
    assert "units" not in attrs
    assert "calendar" not in attrs


# ---------------------------------------------------------------------------
# GenericZarrIngestor (precip_daily): direct-mode force_reingest replaces.
# ---------------------------------------------------------------------------


def test_direct_force_reingest_replaces_overlapping_slice_without_growing(tmp_path: Path) -> None:
    """``force_reingest=true`` overwrites an existing slice in place, not appends.

    Regenerating the overlapping source days into a *fresh* input directory
    gives the synthetic generator's fixed-seed RNG a different draw sequence
    than the original run produced for those same days, so a real value
    change proves the overwrite happened (rather than the run being a no-op).
    """
    source = tmp_path / "input"
    target = tmp_path / "precip.zarr"
    _generate_days(source, 1, 10)
    assert _run(_precip_args(source, target, write_mode="direct")).exit_code == 0

    _generate_days(source, 11, 20)
    second = _run(_precip_args(source, target, write_mode="direct", resume=True))
    assert second.exit_code == 0, second.output

    ds_before = _open_default(target)
    try:
        precip_before = ds_before["precipitation"].isel(time=slice(14, 20)).values.copy()
    finally:
        ds_before.close()

    force_source = tmp_path / "input_force"
    _generate_days(force_source, 15, 20)
    force_args = _precip_args(force_source, target, write_mode="direct", force=True)
    third = _run(force_args)
    assert third.exit_code == 0, third.output

    ds_after = _open_default(target)
    try:
        assert ds_after.sizes["time"] == 20, "force_reingest must not grow the store"
        precip_after = ds_after["precipitation"].isel(time=slice(14, 20)).values
    finally:
        ds_after.close()

    assert not np.array_equal(precip_before, precip_after), (
        "force_reingest must have overwritten the overlapping slice with new values"
    )


def test_overlapping_batch_without_resume_refused_and_store_byte_identical(tmp_path: Path) -> None:
    """A non-resumed, overlapping (duplicate + out-of-order) batch is refused as before."""
    source = tmp_path / "input"
    target = tmp_path / "precip.zarr"
    _generate_days(source, 1, 10)
    assert _run(_precip_args(source, target, write_mode="direct")).exit_code == 0

    before_hash = _store_hash(target)

    overlap_source = tmp_path / "input_overlap"
    _generate_days(overlap_source, 8, 15)
    refused = _run(_precip_args(overlap_source, target, write_mode="direct"))
    assert refused.exit_code != 0, refused.output

    after_hash = _store_hash(target)
    assert before_hash == after_hash, "a refused batch must not mutate the store at all"


# ---------------------------------------------------------------------------
# DirectZarr legacy serial mode (index_spec -> None): Gregorian growth.
# ---------------------------------------------------------------------------


def test_direct_zarr_serial_mode_gregorian_growth_unchanged(tmp_path: Path) -> None:
    """Legacy serial-mode DirectZarr (no ``index_spec``) still grows a ``datetime64[s]`` coord.

    ``cf_time_dim_value_dedup`` has no ``index_spec`` override (inherits the
    ``None`` default), so the engine takes the pre-calendar, non-indexed
    write path: each ingest scans the existing ``data/time`` array for the
    incoming ISO timestamp and appends at the next free slot if absent.
    """
    target = tmp_path / "cube.zarr"
    timestamps = ["2049-01-01T00:00:00", "2049-01-02T00:00:00", "2049-01-03T00:00:00"]

    for index, ts_iso in enumerate(timestamps):
        options = ["--option", f"x_ts_iso={ts_iso}", "--option", f"x_sentinel={100.0 + index}"]
        if index > 0:
            options += ["--option", "resume_existing=true"]
        result = _run(
            [
                "ingest",
                "cf_time_dim_value_dedup",
                "--target",
                f"file://{target}",
                "--product-name",
                "dedup",
                *_LOCAL_STORAGE_FLAGS,
                "--write-mode",
                "direct",
                "--option",
                "no_progress=true",
                *options,
            ]
        )
        assert result.exit_code == 0, result.output

    root = zarr.open_group(store=str(target), mode="r", zarr_format=3)
    data_group = cast(Any, root["data"])
    time_arr = data_group["time"]
    val_arr = data_group["val"]
    assert time_arr.dtype == np.dtype("datetime64[s]")

    stored_times = np.asarray(time_arr[:3]).astype("datetime64[s]")
    expected_times = np.array(timestamps, dtype="datetime64[s]")
    assert np.array_equal(stored_times, expected_times)

    stored_vals = np.asarray(val_arr[:3])
    assert np.array_equal(stored_vals, np.array([100.0, 101.0, 102.0], dtype="float32"))
