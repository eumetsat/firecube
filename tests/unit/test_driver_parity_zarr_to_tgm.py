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

"""Driver-parity tests for tensogram conversion (T4.2).

Asserts that ``extract_zarr_array_metadata`` and ``zarr_to_tgm`` route
metadata reads through the session's typed filesystem instead of the
legacy ``_open_fsspec_url`` adapter when a session is supplied.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pytest
import xarray as xr

from firecube.core.archive.tensogram.metadata import extract_zarr_array_metadata
from firecube.core.config import StorageConfig
from tests.helpers.rows import table_row
from tests.helpers.storage import assert_no_fsspec_bypass, make_test_session

pytestmark = pytest.mark.unit


def _seed_zarr_group(target: Path, group: str = "G") -> None:
    ds = xr.Dataset(
        {"val": (["t", "x"], np.arange(15, dtype=np.float32).reshape(5, 3))},
        coords={"t": np.arange(5), "x": np.arange(3)},
    )
    ds.to_zarr(str(target), group=group, mode="w", zarr_format=3)


def test_driver_parity_zarr_to_tgm_no_bypass(subtests: Any, tmp_path: Path) -> None:
    with table_row(subtests, "extract_zarr_array_metadata_no_bypass", tmp_path) as (workdir, _):
        target = workdir / "product.zarr"
        _seed_zarr_group(target, "G")

        session = make_test_session(workdir, product="product.zarr")
        config = StorageConfig(storage_type="local")
        config.target_path = str(workdir)  # type: ignore[attr-defined]

        with assert_no_fsspec_bypass():
            meta = extract_zarr_array_metadata(
                str(target),
                "G",
                storage_config=config,
                session=session,
            )

        assert "val" in meta
        assert meta["val"]["chunks"]
        assert meta["val"]["dtype"] == "float32"

    with table_row(subtests, "zarr_to_tgm_no_bypass", tmp_path) as (workdir, _):
        from firecube.core.archive.tensogram.converter import zarr_to_tgm

        src = workdir / "test.zarr"
        tgt = workdir / "test.tgm"
        ds = xr.Dataset(
            {"FWI": (["t", "y"], np.ones((3, 4), dtype="float32"))},
            coords={"t": [0, 1, 2], "y": [1, 2, 3, 4]},
        )
        ds.to_zarr(str(src))

        session = make_test_session(workdir, product="test.zarr")

        with assert_no_fsspec_bypass():
            result = zarr_to_tgm(str(src), str(tgt), session=session)

        assert tgt.exists()
        assert result["variables"] == ["FWI"]
