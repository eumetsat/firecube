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

from typing import Literal

import numpy as np
import pytest
import xarray as xr

from firecube.core.storage.session import StorageSession
from firecube.core.storage.uri import StorageUri
from firecube.core.tensogram.converter import zarr_to_tgm
from firecube.core.tensogram.restore import tgm_to_zarr
from firecube.core.uris import local_path_from_target
from tests.helpers.storage import make_test_session


def _make_session(target: str, storage_driver: Literal["fsspec", "obstore"]) -> StorageSession:
    uri = StorageUri.parse(target) if "://" in target else StorageUri.from_local_path(target)
    parent_path = local_path_from_target(uri.parent().to_str())
    return make_test_session(
        parent_path, product=uri.path.rsplit("/", 1)[-1], driver=storage_driver
    )


@pytest.mark.integration
@pytest.mark.parametrize("storage_driver", ["fsspec", "obstore"])
def test_archive_create_restore_round_trip_uses_storage_session(
    tmp_path,
    storage_driver: Literal["fsspec", "obstore"],
) -> None:
    source = tmp_path / f"source-{storage_driver}.zarr"
    archive = tmp_path / f"archive-{storage_driver}.tgm"
    restored = tmp_path / f"restored-{storage_driver}.zarr"

    original = xr.Dataset(
        {
            "FWI": (
                ("timestamp", "lat", "lon"),
                np.arange(24, dtype="float32").reshape(2, 3, 4),
            ),
            "DSR": (
                ("timestamp", "lat", "lon"),
                np.linspace(0.0, 1.0, 24, dtype="float32").reshape(2, 3, 4),
            ),
        },
        coords={
            "timestamp": np.array([0, 1], dtype="int64"),
            "lat": np.array([10.0, 20.0, 30.0], dtype="float32"),
            "lon": np.array([1.0, 2.0, 3.0, 4.0], dtype="float32"),
        },
        attrs={"title": "archive driver round-trip"},
    )
    original.to_zarr(source, group="F024")

    create_session = _make_session(str(source), storage_driver)
    restore_session = _make_session(str(restored), storage_driver)

    create_result = zarr_to_tgm(
        str(source),
        str(archive),
        group="F024",
        session=create_session,
    )
    assert create_result["groups"] == ["F024"]

    restore_result = tgm_to_zarr(
        str(archive),
        str(restored),
        session=restore_session,
    )
    assert restore_result["groups"] == ["F024"]

    restored_ds = restore_session.zarr.open_dataset(
        StorageUri.from_local_path(restored), group="F024"
    )
    try:
        assert set(restored_ds.data_vars) == {"FWI", "DSR"}
        np.testing.assert_array_equal(restored_ds["FWI"].values, original["FWI"].values)
        np.testing.assert_array_equal(restored_ds["DSR"].values, original["DSR"].values)
        np.testing.assert_array_equal(restored_ds["timestamp"].values, original["timestamp"].values)
        np.testing.assert_array_equal(restored_ds["latitude"].values, original["lat"].values)
        np.testing.assert_array_equal(restored_ds["longitude"].values, original["lon"].values)
    finally:
        restored_ds.close()
