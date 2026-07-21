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

"""Unit tests for the lat/lon regrid extension.

Focus on the shared binning core and the explicit-axis (``bounds``) path that
lets a fixed grid be reused across inputs.
"""

from __future__ import annotations

import numpy as np

from firecube.ingestor.extensions import grid


def test_explicit_bounds_fix_the_grid_across_inputs():
    """Two inputs with different extents share one grid when bounds are pinned."""
    bounds = (-10.0, 10.0, -10.0, 10.0)  # lat_min, lat_max, lon_min, lon_max

    a = grid.build_latlon_binner(
        lat=np.array([1.0, 2.0]), lon=np.array([1.0, 2.0]), grid_spacing=1.0, bounds=bounds
    )
    b = grid.build_latlon_binner(
        lat=np.array([-5.0, 5.0]), lon=np.array([-5.0, 5.0]), grid_spacing=1.0, bounds=bounds
    )

    assert (a.nlat, a.nlon) == (b.nlat, b.nlon)
    assert np.array_equal(a.lat, b.lat)
    assert np.array_equal(a.lon, b.lon)


def test_bounds_drop_out_of_extent_points():
    """Points outside the fixed bounds are excluded from the mapping."""
    bounds = (0.0, 5.0, 0.0, 5.0)
    binner = grid.build_latlon_binner(
        lat=np.array([2.0, 50.0]),  # second point is well outside the box
        lon=np.array([2.0, 50.0]),
        grid_spacing=1.0,
        bounds=bounds,
    )
    assert binner.valid_point_index.tolist() == [0]


def test_any_aggregation_available_via_shared_core():
    """The shared core gives lat/lon grids the 'any' presence aggregation too."""
    ds = grid.grid_data_to_latlon(
        lat=np.array([0.2, 0.3]),
        lon=np.array([0.2, 0.3]),
        data=np.array([1.0, 0.0]),
        grid_spacing=1.0,
        aggregation="any",
    )
    values = ds["value"].values
    assert values.dtype == np.uint8
    assert values.max() == 1


def test_mean_matches_manual_average():
    """Two source points in one cell mean-aggregate to their average."""
    ds = grid.grid_data_to_latlon(
        lat=np.array([0.1, 0.2]),
        lon=np.array([0.1, 0.2]),
        data=np.array([2.0, 4.0]),
        grid_spacing=1.0,
        aggregation="mean",
    )
    assert float(np.nanmax(ds["value"].values)) == 3.0
