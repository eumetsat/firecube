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

import numpy as np
import pytest
import xarray as xr

pytestmark = pytest.mark.integration


def test_static_lat_lon_arrays_do_not_block_verification(tmp_path):
    """Regression: static (no time dim) arrays in a group must NOT raise during
    verify_dim_compatibility.
    """
    from firecube.ingestor.runtime.zarr.existing_cube_check import verify_dim_compatibility

    target = str(tmp_path / "with_static.zarr")
    ds = xr.Dataset(
        {
            "values": (
                ("time", "ny", "nx"),
                np.zeros((3, 4, 5), dtype=np.float32),
            ),
            "lat": (("ny", "nx"), np.zeros((4, 5), dtype=np.float64)),
            "lon": (("ny", "nx"), np.zeros((4, 5), dtype=np.float64)),
        },
        coords={"time": np.array([0.0, 1.0, 2.0])},
    )
    ds.to_zarr(target, group="NORDLIS", mode="w", zarr_format=3, consolidated=False)

    verify_dim_compatibility(target, "time", group_paths=["NORDLIS"], storage_config=None)


def test_time_dim_mismatch_still_detected_with_static_arrays(tmp_path):
    """Defence-in-depth: the `continue` change MUST NOT break legitimate mismatch
    detection. A group containing both static lat/lon AND a time-indexed
    array using the wrong time dim must still raise.
    """
    from firecube.ingestor.errors import ConfigurationError
    from firecube.ingestor.runtime.zarr.existing_cube_check import verify_dim_compatibility

    target = str(tmp_path / "mismatch_with_static.zarr")
    ds = xr.Dataset(
        {
            "values": (
                ("timestamp", "ny", "nx"),
                np.zeros((3, 4, 5), dtype=np.float32),
            ),
            "lat": (("ny", "nx"), np.zeros((4, 5), dtype=np.float64)),
            "lon": (("ny", "nx"), np.zeros((4, 5), dtype=np.float64)),
        },
        coords={"timestamp": np.array([0.0, 1.0, 2.0])},
    )
    ds.to_zarr(target, group="NORDLIS", mode="w", zarr_format=3, consolidated=False)

    with pytest.raises(ConfigurationError, match=r"time dimension 'timestamp'.*declared 'time'"):
        verify_dim_compatibility(
            target,
            "time",
            group_paths=["NORDLIS"],
            storage_config=None,
        )
