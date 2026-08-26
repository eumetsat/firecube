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

"""HEALPix (DGGS) gridding utilities for satellite data.

These helpers bin irregular ``(lat, lon, value)`` samples onto a HEALPix
discrete global grid system (DGGS) without interpolation. They are the
spherical sibling of `grid`: where that module bins onto a regular lat/lon
grid, this one bins onto equal-area HEALPix cells.
Both share the binning/aggregation core in `_binning`.

Design notes
------------
- The lat/lon -> cell mapping is delegated to `healpix_geo` (built on the
  ``cdshealpix`` Rust crate). ``healpix_geo`` is the same backend ``xdggs`` uses,
  so cells produced here align with ``xdggs``-decoded HEALPix cubes at the same
  ``depth`` and ``ellipsoid``.
- ``depth`` is the HEALPix order/level; the sphere has ``12 * 4**depth`` cells.
- The target cell axis can be **derived** from the input (the unique cells it
  touches) or **supplied** via ``target_cells`` -- e.g. from `cells_in_bbox`
  -- so every granule/timestep lands on one fixed axis and results can be
  appended. The result carries the absolute cell ids as a coordinate.
- Aggregations mean/min/max/first/last/median mirror the lat/lon module; ``any``
  is added for boolean presence masks (e.g. "is any source pixel flagged here").

This module is only importable when the optional ``healpix`` extra is installed
(``pip install firecube[healpix]``).
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import xarray as xr

from ._binning import aggregate_by_position, as_float_array, broadcast_latlon

DEFAULT_CELL_DIM = "cell"
DEFAULT_CELL_COORD = "cell_ids"


def _require_healpix_geo() -> None:
    """Raise a helpful error if the optional ``healpix-geo`` dep is missing."""
    try:
        import healpix_geo.nested  # noqa: F401
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised via extra
        raise ModuleNotFoundError(
            "HEALPix regridding requires the optional 'healpix' extra. "
            "Install it with: pip install 'firecube[healpix]'"
        ) from exc


@dataclass(frozen=True, slots=True)
class HealpixBinner:
    """Precomputed bin mapping from an input lat/lon grid to HEALPix cells."""

    cells: np.ndarray  # 1D sorted target cell ids (uint64)
    cell_position: np.ndarray  # 1D index into ``cells`` per kept input point
    valid_point_index: np.ndarray  # 1D indices into the flattened input arrays
    depth: int
    indexing_scheme: str
    ellipsoid: str

    @property
    def n_cells(self) -> int:
        """Number of cells on the target axis."""
        return int(self.cells.size)

    @property
    def npix(self) -> int:
        """Total number of HEALPix cells on the sphere at this ``depth``."""
        return 12 * (4**self.depth)


def _mesh_step_for_depth(depth: int) -> float:
    """Half the mean HEALPix cell side (degrees) at ``depth``.

    The mean cell side is ``sqrt(sphere_area / npix)`` which works out to about
    ``58.63 / 2**depth`` degrees; sampling at half that ensures each interior
    cell of a region receives at least one sample.
    """
    return (58.632 / (2**depth)) / 2.0


def cells_in_bbox(
    *,
    lon_min: float,
    lat_min: float,
    lon_max: float,
    lat_max: float,
    depth: int,
    ellipsoid: str = "WGS84",
    step: float | None = None,
) -> np.ndarray:
    """Return the sorted HEALPix cell ids covering a lon/lat bounding box.

    Useful as the ``target_cells`` axis for a regional product: enumerate the
    footprint once, then bin every granule/timestep onto it.

    The box is sampled on a regular lon/lat mesh and each sample mapped to its
    cell; the unique set is returned. ``step`` (degrees) defaults to half the
    cell size at ``depth`` so every interior cell is hit. This handles boxes of
    any size (HEALPix FOV-coverage queries are limited to ~90° per side).
    """
    _require_healpix_geo()
    from healpix_geo.nested import lonlat_to_healpix

    if step is None:
        step = _mesh_step_for_depth(depth)
    if step <= 0:
        raise ValueError("step must be > 0")

    lons = np.arange(lon_min, lon_max + step, step)
    lats = np.arange(lat_min, lat_max + step, step)
    lon2d, lat2d = np.meshgrid(lons, lats)
    cells = lonlat_to_healpix(lon2d.ravel(), lat2d.ravel(), depth, ellipsoid=ellipsoid)
    return np.unique(np.asarray(cells, dtype=np.uint64))


def build_healpix_binner(
    *,
    lat: np.ndarray,
    lon: np.ndarray,
    depth: int,
    indexing_scheme: str = "nested",
    ellipsoid: str = "WGS84",
    target_cells: np.ndarray | None = None,
) -> HealpixBinner:
    """Precompute the mapping from an input lat/lon grid to HEALPix cells.

    Args:
        lat: Latitude of the input samples, in degrees. Either a 1-D
            coordinate vector (treated as a rectilinear grid) or an N-D array
            of point coordinates matching ``lon``.
        lon: Longitude of the input samples, in degrees. Same shape rules as
            ``lat``.
        depth: HEALPix order/level [0, 29]. Determines the cell size
            (``12 * 4**depth`` cells on the sphere).
        indexing_scheme: HEALPix scheme. Only ``"nested"`` is supported.
        ellipsoid: Reference ellipsoid for the lat/lon -> cell mapping (e.g.
            ``"WGS84"``, ``"sphere"``). Two grids share cell ids only when
            ``depth`` and ``ellipsoid`` match.
        target_cells: Optional fixed cell axis (sorted unique cell ids). When
            given, the axis is exactly these cells and input points outside
            them are dropped; when ``None``, the axis is derived as the unique
            cells the input touches.
    """
    _require_healpix_geo()
    from healpix_geo.nested import lonlat_to_healpix

    if indexing_scheme != "nested":
        raise ValueError(
            f"Unsupported indexing_scheme: {indexing_scheme!r}. Only 'nested' is supported."
        )
    if not 0 <= depth <= 29:
        raise ValueError(f"depth must be in [0, 29], got {depth}")

    lat_flat, lon_flat, _ = broadcast_latlon(lat, lon, np.zeros_like(lat, dtype=np.float32))
    lat_flat = as_float_array(lat_flat)
    lon_flat = as_float_array(lon_flat)

    valid_mask = ~(np.isnan(lat_flat) | np.isnan(lon_flat))
    valid_point_index = np.flatnonzero(valid_mask)
    if valid_point_index.size == 0:
        raise ValueError("No valid lat/lon points found after filtering NaNs")

    cell_ids = np.asarray(
        lonlat_to_healpix(lon_flat[valid_mask], lat_flat[valid_mask], depth, ellipsoid=ellipsoid),
        dtype=np.uint64,
    )

    if target_cells is None:
        # Derive the axis from the cells the input actually touches.
        cells, cell_position = np.unique(cell_ids, return_inverse=True)
    else:
        # Bin onto a fixed, caller-supplied axis; drop points outside it.
        cells = np.asarray(target_cells, dtype=np.uint64)
        pos = np.searchsorted(cells, cell_ids)
        np.clip(pos, 0, max(cells.size - 1, 0), out=pos)
        in_target = (cells.size > 0) & (cells[pos] == cell_ids)
        cell_position = pos[in_target]
        valid_point_index = valid_point_index[in_target]

    return HealpixBinner(
        cells=cells,
        cell_position=cell_position.astype(np.int64, copy=False),
        valid_point_index=valid_point_index.astype(np.int64, copy=False),
        depth=int(depth),
        indexing_scheme=indexing_scheme,
        ellipsoid=ellipsoid,
    )


def regrid_with_binner(
    *,
    binner: HealpixBinner,
    data: np.ndarray,
    aggregation: str = "mean",
) -> np.ndarray:
    """Regrid a field onto the binner's HEALPix cells (1-D, aligned to ``cells``)."""
    return aggregate_by_position(
        values_flat=np.asarray(data),
        valid_point_index=binner.valid_point_index,
        position=binner.cell_position,
        n_targets=binner.n_cells,
        aggregation=aggregation,
    )


def _cell_coord_attrs(binner: HealpixBinner) -> dict[str, object]:
    """xdggs-compatible attributes so the result can be ``xdggs.decode``-d."""
    return {
        "grid_name": "healpix",
        "level": binner.depth,
        "indexing_scheme": binner.indexing_scheme,
        "ellipsoid": binner.ellipsoid,
    }


def _to_dataset(
    *,
    gridded: dict[str, np.ndarray],
    binner: HealpixBinner,
    cell_dim: str,
    cell_coord: str,
) -> xr.Dataset:
    ds = xr.Dataset(
        data_vars={name: ((cell_dim,), values) for name, values in gridded.items()},
        coords={cell_coord: ((cell_dim,), binner.cells)},
    )
    ds[cell_coord].attrs.update(_cell_coord_attrs(binner))
    ds.attrs["n_cells"] = binner.n_cells
    ds.attrs["npix"] = binner.npix
    return ds


def grid_data_to_healpix(
    *,
    lat: np.ndarray,
    lon: np.ndarray,
    data: np.ndarray,
    depth: int,
    variable_name: str = "value",
    indexing_scheme: str = "nested",
    ellipsoid: str = "WGS84",
    aggregation: str = "mean",
    fill_value: float | None = None,
    target_cells: np.ndarray | None = None,
) -> xr.Dataset:
    """Grid irregular satellite data onto HEALPix cells (no interpolation)."""
    binner = build_healpix_binner(
        lat=lat,
        lon=lon,
        depth=depth,
        indexing_scheme=indexing_scheme,
        ellipsoid=ellipsoid,
        target_cells=target_cells,
    )

    _, _, data_flat = broadcast_latlon(lat, lon, data)
    values = as_float_array(data_flat).copy()
    if fill_value is not None:
        values[values == fill_value] = np.nan

    gridded = regrid_with_binner(binner=binner, data=values, aggregation=aggregation)
    return _to_dataset(
        gridded={variable_name: gridded},
        binner=binner,
        cell_dim=DEFAULT_CELL_DIM,
        cell_coord=DEFAULT_CELL_COORD,
    )


def grid_xarray_dataset_to_healpix(
    *,
    ds: xr.Dataset,
    lat_var: str,
    lon_var: str,
    data_vars: list[str],
    depth: int,
    indexing_scheme: str = "nested",
    ellipsoid: str = "WGS84",
    aggregation: str = "mean",
    fill_value: float | None = None,
    target_cells: np.ndarray | None = None,
    cell_dim: str = DEFAULT_CELL_DIM,
    cell_coord: str = DEFAULT_CELL_COORD,
) -> xr.Dataset:
    """Grid multiple variables from an xarray Dataset onto HEALPix cells."""
    if not data_vars:
        raise ValueError("data_vars must contain at least one variable name")

    binner = build_healpix_binner(
        lat=ds[lat_var].values,
        lon=ds[lon_var].values,
        depth=depth,
        indexing_scheme=indexing_scheme,
        ellipsoid=ellipsoid,
        target_cells=target_cells,
    )

    gridded: dict[str, np.ndarray] = {}
    for var_name in data_vars:
        values = as_float_array(np.asarray(ds[var_name].values)).ravel().copy()
        if fill_value is not None:
            values[values == fill_value] = np.nan
        gridded[var_name] = regrid_with_binner(binner=binner, data=values, aggregation=aggregation)

    out = _to_dataset(gridded=gridded, binner=binner, cell_dim=cell_dim, cell_coord=cell_coord)
    out.attrs.update(ds.attrs)
    out.attrs["gridding_info"] = (
        f"Data gridded to HEALPix depth {depth} ({indexing_scheme}, {ellipsoid}) "
        f"using {aggregation} aggregation"
    )
    return out
