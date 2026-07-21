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

"""Driver-parity tests for CLI helpers (T4.2).

Covers the staged-metadata seeding helper (with ``session=`` kwarg) and
``session.zarr.open_dataset`` — both of which back common CLI flows.
Each test seeds a real Zarr V3 store under ``tmp_path`` and asserts the
no-fsspec-bypass invariant via ``assert_no_fsspec_bypass``.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest
import xarray as xr

from firecube.ingestor.runtime.zarr.staged_metadata import seed_staged_store_metadata
from tests.helpers.storage import assert_no_fsspec_bypass, make_test_session

pytestmark = pytest.mark.unit


def _seed_zarr_group(target: Path, group: str = "G") -> None:
    ds = xr.Dataset(
        {"val": (["timestamp", "x"], np.arange(30, dtype=np.float32).reshape(10, 3))},
        coords={"timestamp": np.arange(10), "x": np.arange(3)},
    )
    ds.to_zarr(str(target), group=group, mode="w", zarr_format=3)


def test_staged_metadata_no_bypass(tmp_path: Path) -> None:
    final = tmp_path / "final.zarr"
    temp = tmp_path / "temp.zarr"
    _seed_zarr_group(final, "G")

    session = make_test_session(tmp_path, product="final.zarr")

    with assert_no_fsspec_bypass():
        result = seed_staged_store_metadata(
            temp_store_uri=str(temp),
            final_target_uri=str(final),
            groups=["G"],
            session=session,
        )

    assert result["G"]["seeded"] is True
    assert result["G"]["files"] >= 1


def test_session_zarr_open_dataset_no_bypass(tmp_path: Path) -> None:
    target = tmp_path / "product.zarr"
    _seed_zarr_group(target, "G")

    session = make_test_session(tmp_path, product="product.zarr")
    uri = session.product.product_uri

    with assert_no_fsspec_bypass():
        ds = session.zarr.open_dataset(uri, group="G")

    assert "val" in ds.variables
    assert ds["val"].shape == (10, 3)
