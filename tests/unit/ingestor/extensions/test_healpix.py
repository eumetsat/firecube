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

"""Unit tests for the HEALPix regrid extension.

The aggregation/dataset-shaping logic is tested directly. The test extra
installs ``healpix-geo``; missing Python packages should fail loudly instead of
recording a skip.
"""

from __future__ import annotations

import healpix_geo  # noqa: F401
import numpy as np
import pytest

from firecube.ingestor.extensions import healpix as hp


def test_build_binner_co_registers_cells():
    """A coarse-depth grid maps points to valid nested cell ids."""
    lat = np.array([10.0, 10.0, -20.0], dtype=np.float64)
    lon = np.array([5.0, 5.0, 30.0], dtype=np.float64)

    binner = hp.build_healpix_binner(lat=lat, lon=lon, depth=4)

    assert binner.depth == 4
    assert binner.npix == 12 * 4**4
    # Two coincident points should collapse onto a single occupied cell.
    assert binner.n_cells == 2
    assert binner.cells.dtype == np.uint64


def test_any_aggregation_is_binary_presence():
    """'any' yields a uint8 presence mask from collapsing source pixels onto cells."""
    lat = np.array([10.0, 10.0, -20.0], dtype=np.float64)
    lon = np.array([5.0, 5.0, 30.0], dtype=np.float64)
    binner = hp.build_healpix_binner(lat=lat, lon=lon, depth=4)

    presence = hp.regrid_with_binner(binner=binner, data=np.array([1, 0, 1]), aggregation="any")

    assert presence.dtype == np.uint8
    assert set(np.unique(presence)).issubset({0, 1})
    assert presence.sum() == 2  # the coincident cell (1 or'd with 0) + the lone cell


def test_grid_xarray_dataset_has_xdggs_attrs():
    """Output carries the cell coord + attrs needed for xdggs.decode downstream."""
    import xarray as xr

    ds = xr.Dataset(
        {"flag": (("y",), np.array([1.0, 1.0, 0.0]))},
        coords={
            "latitude": (("y",), np.array([10.0, 10.0, -20.0])),
            "longitude": (("y",), np.array([5.0, 5.0, 30.0])),
        },
    )

    out = hp.grid_xarray_dataset_to_healpix(
        ds=ds,
        lat_var="latitude",
        lon_var="longitude",
        data_vars=["flag"],
        depth=4,
        aggregation="any",
    )

    assert "cell" in out.dims
    assert out["cell_ids"].attrs["grid_name"] == "healpix"
    assert out["cell_ids"].attrs["level"] == 4
    assert out["cell_ids"].attrs["indexing_scheme"] == "nested"


def test_unknown_aggregation_rejected():
    lat = np.array([10.0, -20.0])
    lon = np.array([5.0, 30.0])
    binner = hp.build_healpix_binner(lat=lat, lon=lon, depth=4)
    with pytest.raises(ValueError, match="Unknown aggregation"):
        hp.regrid_with_binner(binner=binner, data=np.array([1.0, 2.0]), aggregation="bogus")


def test_cells_in_bbox_contains_interior_point():
    """A point inside the box maps to one of the enumerated coverage cells."""
    depth = 6
    cells = hp.cells_in_bbox(lon_min=0.0, lat_min=0.0, lon_max=20.0, lat_max=20.0, depth=depth)
    assert cells.dtype == np.uint64
    assert cells.size > 0
    assert np.array_equal(cells, np.unique(cells))  # sorted + unique

    interior = hp.build_healpix_binner(
        lat=np.array([10.0]), lon=np.array([10.0]), depth=depth
    ).cells[0]
    assert interior in cells


def test_cells_in_bbox_handles_region_wider_than_90_degrees():
    """Regions exceeding a HEALPix FOV-coverage query's ~90deg/side limit work."""
    cells = hp.cells_in_bbox(lon_min=-55.0, lat_min=-40.0, lon_max=55.0, lat_max=40.0, depth=6)
    assert cells.size > 0
    assert np.array_equal(cells, np.unique(cells))


def test_target_cells_pins_a_fixed_axis():
    """With target_cells the axis is exactly those cells; outside points drop."""
    depth = 6
    target = hp.cells_in_bbox(lon_min=0.0, lat_min=0.0, lon_max=20.0, lat_max=20.0, depth=depth)

    # One point inside the box, one far outside it.
    lat = np.array([10.0, -80.0])
    lon = np.array([10.0, 170.0])
    binner = hp.build_healpix_binner(lat=lat, lon=lon, depth=depth, target_cells=target)

    # Axis is the fixed target, not the (2) cells the data touched.
    assert binner.n_cells == target.size
    assert np.array_equal(binner.cells, target)
    # The outside point was dropped; only the interior point remains mapped.
    assert binner.valid_point_index.tolist() == [0]


def test_target_cells_enable_appendable_alignment():
    """Two inputs on the same target axis produce identical coordinates."""
    depth = 6
    target = hp.cells_in_bbox(lon_min=0.0, lat_min=0.0, lon_max=20.0, lat_max=20.0, depth=depth)
    import xarray as xr

    def grid(latv: float, lonv: float) -> xr.Dataset:
        ds = xr.Dataset(
            {"flag": (("y",), np.array([1.0]))},
            coords={"latitude": (("y",), [latv]), "longitude": (("y",), [lonv])},
        )
        return hp.grid_xarray_dataset_to_healpix(
            ds=ds,
            lat_var="latitude",
            lon_var="longitude",
            data_vars=["flag"],
            depth=depth,
            aggregation="any",
            target_cells=target,
        )

    a = grid(5.0, 5.0)
    b = grid(15.0, 15.0)
    assert np.array_equal(a["cell_ids"].values, b["cell_ids"].values)
    assert a.sizes["cell"] == target.size
