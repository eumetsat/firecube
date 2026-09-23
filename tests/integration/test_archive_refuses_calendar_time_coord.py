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

"""Archive must fail loudly on a non-Gregorian-calendar time coordinate.

`firecube archive create` opens the source with CF decoding on. A time
coordinate stored as int64 with CF `units`/`calendar` attrs on a
non-Gregorian calendar (e.g. `360_day`) decodes to an object array of
`cftime` scalars. Before this fix, the archive's dtype filter silently
dropped that coordinate (`skipped_vars.add(...); continue`), producing a
`.tgm` archive with no time axis and no error. It must instead raise
`firecube.core.errors.ConfigurationError` naming the coordinate and its
calendar.

Gregorian datetime64 coordinates are unaffected -- see
`tests/integration/test_tensogram_archive.py`, which must keep passing
unchanged.
"""

from __future__ import annotations

import contextlib
import os

import numpy as np
import pytest
import xarray as xr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.errors import ConfigurationError
from firecube.core.tensogram.converter import zarr_to_tgm

_LOCAL_STORAGE_FLAGS = ["--storage-type", "local", "--storage-driver", "fsspec"]


def _local_env(tmp_path) -> dict[str, str]:
    return {"FIRECUBE_STORAGE_TYPE": "local", "FIRECUBE_TARGET_PATH": str(tmp_path)}


def _make_360_day_dataset(n_time: int = 5) -> xr.Dataset:
    """A minimal dataset whose time coordinate is on the CF `360_day` calendar.

    `xr.date_range(..., calendar="360_day", use_cftime=True)` produces
    `cftime.Datetime360Day` values; `Dataset.to_zarr` CF-encodes them to an
    int64 array with `units`/`calendar` attrs on write (verified: writes
    `{"units": "days since 2020-01-01 00:00:00.000000", "calendar":
    "360_day"}`), matching the on-disk shape the archive path must guard
    against.
    """
    times = xr.date_range(
        "2020-01-01", periods=n_time, freq="D", calendar="360_day", use_cftime=True
    )
    data = np.arange(n_time, dtype="float32")
    return xr.Dataset(
        {"FWI": (["timestamp"], data, {"units": "1", "standard_name": "fire_weather_index"})},
        coords={"timestamp": times},
        attrs={"Conventions": "CF-1.8", "title": "Test 360_day calendar"},
    )


@pytest.mark.integration
def test_zarr_to_tgm_raises_configuration_error_for_360_day_time_coordinate(tmp_path):
    src = str(tmp_path / "product.zarr")
    tgm = str(tmp_path / "out.tgm")

    ds = _make_360_day_dataset()
    ds.to_zarr(src)

    with pytest.raises(ConfigurationError) as excinfo:
        zarr_to_tgm(src, tgm)

    message = str(excinfo.value)
    assert "timestamp" in message
    assert "360_day" in message


@pytest.mark.integration
def test_archive_create_cli_fails_loudly_for_360_day_time_coordinate(tmp_path):
    runner = CliRunner()
    env = _local_env(tmp_path)
    src = str(tmp_path / "product.zarr")
    src_uri = f"file://{src}"
    tgm = str(tmp_path / "out.tgm")
    tgm_uri = f"file://{tgm}"

    ds = _make_360_day_dataset()
    ds.to_zarr(src)

    result = runner.invoke(
        cli,
        ["archive", "create", "--source", src_uri, "--archive", tgm_uri, *_LOCAL_STORAGE_FLAGS],
        env=env,
    )

    # The CLI maps the refusal to a click error: non-zero exit, the cause
    # printed for the operator, no traceback.
    assert result.exit_code != 0, result.output
    assert not isinstance(result.exception, ConfigurationError), type(result.exception)
    assert "Error:" in result.output
    assert "timestamp" in result.output
    assert "360_day" in result.output

    # `archive create` does not (yet) special-case cleanup for this failure any
    # more than it does for other in-flight archive failures (e.g. a bad
    # --variables selection also leaves the partially written .tgm on disk --
    # see zarr_to_tgm's ValueError path). We assert consistency with that
    # existing behavior rather than a new invariant this change does not add.
    existing_variable_error_leaves_file = _partial_file_left_by_existing_failure(tmp_path)
    assert os.path.exists(tgm) == existing_variable_error_leaves_file


def _partial_file_left_by_existing_failure(tmp_path) -> bool:
    """Reproduce an existing (unrelated) archive failure to check its file-cleanup behavior.

    Used only to keep the new failure path's on-disk behavior consistent with
    what `zarr_to_tgm` already does today, without asserting a stronger
    cleanup guarantee than the existing code provides.
    """
    src = str(tmp_path / "control.zarr")
    tgm = str(tmp_path / "control.tgm")
    xr.Dataset(
        {"FWI": (["timestamp"], np.arange(3, dtype="float32"))},
        coords={"timestamp": np.arange(3)},
    ).to_zarr(src)

    with contextlib.suppress(ValueError):
        zarr_to_tgm(src, tgm, variables=["does-not-exist"])
    return os.path.exists(tgm)
