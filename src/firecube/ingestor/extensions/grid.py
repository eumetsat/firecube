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

"""
Gridding utilities for satellite data.

These helpers bin irregular (lat, lon, value) samples onto a regular lat/lon
grid without interpolation. This is intended for curvilinear geolocation
grids (2D lat/lon) such as MSG products.

Design notes
------------
- The target grid extent can be **derived** from the data
  (``arange(floor(min), ceil(max) + spacing, spacing)``) or **supplied** via
  ``bounds``. Supplying it pins one fixed grid so many granules/timesteps share
  a coordinate and can be appended, instead of each input deriving its own.
- Output coordinates are *centers* aligned to full degrees. Binning uses
  half-cell edges around centers, so all centers correspond to a real cell.
  This fixes the common off-by-one and "extra empty last row/col" issues that
  arise when treating centers as edges.
- The binning/aggregation core is shared with :mod:`.healpix` via
  :mod:`._binning`. Performance: mean/min/max/first/last/any are vectorized;
  median is supported but may be expensive on large grids.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import xarray as xr

from ._binning import aggregate_by_position
from ._binning import as_float_array as _as_float_array
from ._binning import broadcast_latlon as _broadcast_latlon


@dataclass(frozen=True, slots=True)
class LatLonBinner:
    """Precomputed bin mapping from an input grid to a regular lat/lon grid."""

    lat: np.ndarray  # 1D centers
    lon: np.ndarray  # 1D centers
    cell_index: np.ndarray  # 1D flattened cell indices (len == n_valid_points)
    valid_point_index: np.ndarray  # 1D indices into flattened input arrays
    nlat: int
    nlon: int
    spacing: float


def build_latlon_binner(
    *,
    lat: np.ndarray,
    lon: np.ndarray,
    grid_spacing: float,
    bounds: tuple[float, float, float, float] | None = None,
    fill_value: float | None = None,
) -> LatLonBinner:
    """Precompute the mapping from an input lat/lon grid to regular bins.

    Parameters
    ----------
    lat, lon:
        Input geolocation, 1-D coordinate vectors or matching N-D arrays.
    grid_spacing:
        Target cell size in degrees (> 0).
    bounds:
        Optional ``(lat_min, lat_max, lon_min, lon_max)`` fixing the output
        grid extent. When given, the axis is exactly these bounds and input
        points outside them are dropped -- pin one set of bounds to reuse the
        same grid across granules. When ``None``, the extent is derived from
        the data (``floor(min)``/``ceil(max)``).
    fill_value:
        Accepted for signature parity; data masking is applied by callers.
    """
    if grid_spacing <= 0:
        raise ValueError("grid_spacing must be > 0")

    lat_flat, lon_flat, _data_dummy = _broadcast_latlon(
        lat, lon, np.zeros_like(lat, dtype=np.float32)
    )
    lat_flat = _as_float_array(lat_flat)
    lon_flat = _as_float_array(lon_flat)

    valid_mask = ~(np.isnan(lat_flat) | np.isnan(lon_flat))

    lat_valid = lat_flat[valid_mask]
    lon_valid = lon_flat[valid_mask]
    valid_point_index = np.flatnonzero(valid_mask)

    if lat_valid.size == 0:
        raise ValueError("No valid lat/lon points found after filtering NaNs")

    if bounds is not None:
        lat_min, lat_max, lon_min, lon_max = (float(b) for b in bounds)
    else:
        lat_min = float(np.floor(np.nanmin(lat_valid)))
        lat_max = float(np.ceil(np.nanmax(lat_valid)))
        lon_min = float(np.floor(np.nanmin(lon_valid)))
        lon_max = float(np.ceil(np.nanmax(lon_valid)))

    lat_centers = np.arange(lat_min, lat_max + grid_spacing, grid_spacing, dtype=np.float64)
    lon_centers = np.arange(lon_min, lon_max + grid_spacing, grid_spacing, dtype=np.float64)

    # Treat centers as true cell centers; edges are half a spacing away.
    lat0 = lat_centers[0] - (grid_spacing / 2.0)
    lon0 = lon_centers[0] - (grid_spacing / 2.0)

    lat_idx = np.floor((lat_valid - lat0) / grid_spacing).astype(np.int64, copy=False)
    lon_idx = np.floor((lon_valid - lon0) / grid_spacing).astype(np.int64, copy=False)

    nlat = int(lat_centers.size)
    nlon = int(lon_centers.size)
    in_bounds = (lat_idx >= 0) & (lat_idx < nlat) & (lon_idx >= 0) & (lon_idx < nlon)

    lat_idx = lat_idx[in_bounds]
    lon_idx = lon_idx[in_bounds]
    valid_point_index = valid_point_index[in_bounds]

    cell_index = (lat_idx * nlon + lon_idx).astype(np.int64, copy=False)

    return LatLonBinner(
        lat=lat_centers.astype(np.float32),
        lon=lon_centers.astype(np.float32),
        cell_index=cell_index,
        valid_point_index=valid_point_index.astype(np.int64, copy=False),
        nlat=nlat,
        nlon=nlon,
        spacing=float(grid_spacing),
    )


def _aggregate_to_grid(
    *,
    values_flat: np.ndarray,
    binner: LatLonBinner,
    aggregation: str,
) -> np.ndarray:
    """Aggregate flattened values into a (lat, lon) grid using a precomputed binner."""
    flat = aggregate_by_position(
        values_flat=values_flat,
        valid_point_index=binner.valid_point_index,
        position=binner.cell_index,
        n_targets=binner.nlat * binner.nlon,
        aggregation=aggregation,
    )
    return flat.reshape((binner.nlat, binner.nlon))


def regrid_with_binner(
    *,
    binner: LatLonBinner,
    data: np.ndarray,
    aggregation: str = "mean",
) -> np.ndarray:
    """Regrid a 2D field onto the binner's regular grid.

    Parameters
    ----------
    binner:
        Precomputed LatLonBinner mapping for the input geolocation grid.
    data:
        2D array matching the binner's input grid shape.
    aggregation:
        Aggregation method for multiple source pixels per target cell.
    """
    values_flat = np.asarray(data).ravel()
    return _aggregate_to_grid(values_flat=values_flat, binner=binner, aggregation=aggregation)


def grid_data_to_latlon(
    *,
    lat: np.ndarray,
    lon: np.ndarray,
    data: np.ndarray,
    grid_spacing: float = 0.1,
    variable_name: str = "value",
    fill_value: float | None = None,
    aggregation: str = "mean",
    bounds: tuple[float, float, float, float] | None = None,
) -> xr.Dataset:
    """Grid irregular satellite data onto a regular lat/lon grid (no interpolation)."""
    _lat_flat, _lon_flat, data_flat = _broadcast_latlon(lat, lon, data)

    # Determine which data values are valid (coordinates handled by binner).
    if fill_value is not None:
        valid_data = ~(np.isnan(data_flat) | (data_flat == fill_value))
    else:
        valid_data = ~np.isnan(data_flat)

    if valid_data.sum() == 0:
        raise ValueError("No valid data points found after filtering invalid values")

    binner = build_latlon_binner(
        lat=lat, lon=lon, grid_spacing=grid_spacing, bounds=bounds, fill_value=fill_value
    )
    # Apply data mask on top of binner's valid coordinate mask.
    values = np.full_like(data_flat, np.nan, dtype=np.float64)
    values[valid_data] = _as_float_array(data_flat[valid_data])

    data_gridded = _aggregate_to_grid(values_flat=values, binner=binner, aggregation=aggregation)

    n_valid_cells = int(np.sum(~np.isnan(data_gridded)))
    n_total_cells = int(data_gridded.size)

    ds = xr.Dataset(
        data_vars={variable_name: (["lat", "lon"], data_gridded)},
        coords={
            "lat": ("lat", binner.lat),
            "lon": ("lon", binner.lon),
        },
        attrs={
            "title": f"{variable_name} on regular lat/lon grid",
            "description": f"Gridded from irregular data using {aggregation} aggregation (no interpolation)",
            "grid_spacing": f"{grid_spacing}°",
            "aggregation_method": str(aggregation),
            "n_valid_cells": n_valid_cells,
            "n_total_cells": n_total_cells,
            "fill_percentage": float(n_valid_cells / n_total_cells * 100.0)
            if n_total_cells
            else 0.0,
            "lat_range": f"({float(binner.lat.min()):.2f}, {float(binner.lat.max()):.2f})",
            "lon_range": f"({float(binner.lon.min()):.2f}, {float(binner.lon.max()):.2f})",
        },
    )

    return ds


def grid_xarray_dataset(
    *,
    ds: xr.Dataset,
    lat_var: str,
    lon_var: str,
    data_vars: list[str],
    grid_spacing: float = 0.1,
    fill_value: float | None = None,
    aggregation: str = "mean",
    bounds: tuple[float, float, float, float] | None = None,
) -> xr.Dataset:
    """Grid multiple variables from an xarray Dataset onto a regular lat/lon grid."""
    if not data_vars:
        raise ValueError("data_vars must contain at least one variable name")

    lat = ds[lat_var].values
    lon = ds[lon_var].values
    binner = build_latlon_binner(
        lat=lat, lon=lon, grid_spacing=grid_spacing, bounds=bounds, fill_value=fill_value
    )

    out: xr.Dataset | None = None
    for idx, var_name in enumerate(data_vars):
        data_flat = np.asarray(ds[var_name].values).ravel()
        if fill_value is not None:
            valid_data = ~(np.isnan(data_flat) | (data_flat == fill_value))
        else:
            valid_data = ~np.isnan(data_flat)
        values = np.full_like(data_flat, np.nan, dtype=np.float64)
        values[valid_data] = _as_float_array(data_flat[valid_data])
        gridded = _aggregate_to_grid(values_flat=values, binner=binner, aggregation=aggregation)

        if idx == 0:
            out = xr.Dataset(
                data_vars={var_name: (["lat", "lon"], gridded)},
                coords={"lat": ("lat", binner.lat), "lon": ("lon", binner.lon)},
            )
        else:
            assert out is not None
            out[var_name] = (["lat", "lon"], gridded)

    assert out is not None
    out.attrs.update(ds.attrs)
    out.attrs["gridding_info"] = (
        f"Data gridded to {grid_spacing}° resolution using {aggregation} aggregation"
    )
    return out
