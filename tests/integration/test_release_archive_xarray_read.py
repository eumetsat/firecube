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

"""How xarray reads release archives; new module because no existing one covers it."""

from __future__ import annotations

import zipfile

import numpy as np
import pytest
import xarray as xr
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli


def _cli(*args: str) -> None:
    result = CliRunner().invoke(cli, list(args))
    assert result.exit_code == 0, result.output


def _v(spec: str, **kwargs) -> np.ndarray:
    with xr.open_dataset(spec, engine="zarr", **kwargs) as ds:
        return ds["v"].load().values


@pytest.mark.integration
@pytest.mark.filterwarnings("ignore:Object at")
@pytest.mark.filterwarnings("ignore:Consolidated metadata is currently not part")
def test_release_archives_read_back_through_xarray_backup_chain(tmp_path, subtests) -> None:
    product = tmp_path / "P.zarr"
    first = np.arange(8, dtype="float64").reshape(2, 4)
    second = first.copy()
    second[1] = 100.0
    xr.Dataset({"v": (("t", "x"), first)}, coords={"t": [0, 1], "x": [0, 1, 2, 3]}).to_zarr(
        product, zarr_format=3, consolidated=False, encoding={"v": {"chunks": (1, 4)}}
    )
    target = ["--target", product.as_uri(), "--product-name", "P"]
    full, incremental = tmp_path / "P-1.0.0.zip", tmp_path / "P-1.1.0.zip"
    _cli("migrate", "adopt", *target)
    _cli("release", "create", "1.0.0", *target, "--archive", full.as_uri())
    zarr.open_array(product / "v", mode="r+")[1, :] = 100.0
    _cli("release", "create", "1.1.0", *target, "--archive", incremental.as_uri())
    chain = {"group": "/", "consolidated": False, "zarr_format": 3}

    with subtests.test("full archive opens through zip:: and holds the values at the cut"):
        np.testing.assert_array_equal(_v(f"zip::{full}", **chain), first)
    with subtests.test(
        "incremental backup chain restores to the newest values, full archive keeps the old"
    ):
        restored = tmp_path / "restored.zarr"
        _cli("archive", "restore", "--archive", incremental.as_uri(), "--target", restored.as_uri())
        np.testing.assert_array_equal(_v(str(restored), consolidated=False), second)
        np.testing.assert_array_equal(_v(f"zip::{full}", **chain), first)
    with (
        subtests.test("incremental backup omits the unchanged chunk that the full archive holds"),
        zipfile.ZipFile(full) as zf_full,
        zipfile.ZipFile(incremental) as zf_incremental,
    ):
        assert "v/c/0/0" in zf_full.namelist()
        assert "v/c/0/0" not in zf_incremental.namelist()
        assert "v/c/1/0" in zf_incremental.namelist()
    with subtests.test("consolidated product: archive opens with consolidated=True"):
        values = np.arange(4, dtype="float64")
        cons = tmp_path / "C.zarr"
        xr.Dataset({"v": ("x", values)}).to_zarr(cons, zarr_format=3, consolidated=True)
        cons_target = ["--target", cons.as_uri(), "--product-name", "C"]
        cons_zip = tmp_path / "C-1.0.0.zip"
        _cli("migrate", "adopt", *cons_target)
        _cli("release", "create", "1.0.0", *cons_target, "--archive", cons_zip.as_uri())
        np.testing.assert_array_equal(
            _v(f"zip::{cons_zip}", **{**chain, "consolidated": True}), values
        )
