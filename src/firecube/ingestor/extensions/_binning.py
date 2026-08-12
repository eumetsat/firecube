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

"""Shared binning primitives for the gridding extensions.

A *binner* maps irregular ``(lat, lon)`` input samples onto a 1-D **target
axis** of cells, recording for each usable input point the integer position of
its target cell. Aggregation then scatters values onto that axis. This pattern
is grid-agnostic: a regular lat/lon grid (:mod:`.grid`) and a HEALPix grid
(:mod:`.healpix`) differ only in how the axis and the point->position mapping
are computed, not in how values are aggregated.

The target axis can be **derived from the data** or **supplied explicitly** by
the caller. Supplying it explicitly is what lets a plugin pin one fixed axis and
reuse it across many granules/timesteps (so results share a coordinate and can
be appended), rather than re-deriving a different axis from each input.
"""

from __future__ import annotations

import numpy as np

AGGREGATIONS = ("mean", "min", "max", "first", "last", "median", "any")


def as_float_array(value: np.ndarray) -> np.ndarray:
    arr = np.asarray(value)
    if arr.dtype.kind == "f":
        return arr
    return arr.astype(np.float64, copy=False)


def broadcast_latlon(
    lat: np.ndarray, lon: np.ndarray, data: np.ndarray
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Normalize lat/lon/data shapes to 1D flattened arrays of equal length."""
    lat_arr = np.asarray(lat)
    lon_arr = np.asarray(lon)
    data_arr = np.asarray(data)

    if lat_arr.ndim == 1 and lon_arr.ndim == 1 and data_arr.ndim == 2:
        # Rectilinear case: data[lat, lon] with 1D coords.
        lon2, lat2 = np.meshgrid(lon_arr, lat_arr)
        return lat2.ravel(), lon2.ravel(), data_arr.ravel()

    if lat_arr.shape != lon_arr.shape or lat_arr.shape != data_arr.shape:
        raise ValueError(
            "lat, lon, and data must have matching shapes (or lat/lon 1D with data 2D). "
            f"Got lat={lat_arr.shape}, lon={lon_arr.shape}, data={data_arr.shape}."
        )
    return lat_arr.ravel(), lon_arr.ravel(), data_arr.ravel()


def aggregate_by_position(
    *,
    values_flat: np.ndarray,
    valid_point_index: np.ndarray,
    position: np.ndarray,
    n_targets: int,
    aggregation: str,
) -> np.ndarray:
    """Scatter flattened values onto a 1-D target axis of length ``n_targets``.

    Args:
        values_flat: Flattened input values (same flattening as the binner was
            built from).
        valid_point_index: Indices into ``values_flat`` for the points the
            binner kept.
        position: For each kept point, the target-axis index in
            ``[0, n_targets)``.
        n_targets: Length of the output axis.
        aggregation: One of :data:`AGGREGATIONS`. ``any`` returns a ``uint8``
            presence mask; all others return ``float32`` with ``NaN`` in empty
            cells.
    """
    aggregation = str(aggregation).lower()
    if aggregation not in AGGREGATIONS:
        raise ValueError(
            f"Unknown aggregation method: {aggregation}. Options: {', '.join(AGGREGATIONS)}"
        )

    values_flat = as_float_array(values_flat).ravel()
    values = values_flat[valid_point_index]

    ok = ~np.isnan(values)
    if ok.sum() == 0:
        if aggregation == "any":
            return np.zeros(n_targets, dtype=np.uint8)
        return np.full(n_targets, np.nan, dtype=np.float32)

    pos = position[ok]
    vals = values[ok]

    if aggregation == "any":
        out = np.zeros(n_targets, dtype=np.uint8)
        np.logical_or.at(out, pos, vals != 0)
        return out

    if aggregation == "mean":
        sums = np.bincount(pos, weights=vals, minlength=n_targets).astype(np.float64, copy=False)
        counts = np.bincount(pos, minlength=n_targets).astype(np.float64, copy=False)
        out = np.full(n_targets, np.nan, dtype=np.float32)
        nonzero = counts > 0
        out[nonzero] = (sums[nonzero] / counts[nonzero]).astype(np.float32)
        return out

    if aggregation == "min":
        out = np.full(n_targets, np.inf, dtype=np.float64)
        np.minimum.at(out, pos, vals)
        out[~np.isfinite(out)] = np.nan
        return out.astype(np.float32)

    if aggregation == "max":
        out = np.full(n_targets, -np.inf, dtype=np.float64)
        np.maximum.at(out, pos, vals)
        out[~np.isfinite(out)] = np.nan
        return out.astype(np.float32)

    if aggregation in {"first", "last"}:
        order = np.argsort(pos, kind="stable")
        pos_sorted = pos[order]
        vals_sorted = vals[order]
        if aggregation == "last":
            pos_sorted = pos_sorted[::-1]
            vals_sorted = vals_sorted[::-1]
        _, first_pos = np.unique(pos_sorted, return_index=True)
        out = np.full(n_targets, np.nan, dtype=np.float32)
        out[pos_sorted[first_pos]] = vals_sorted[first_pos].astype(np.float32, copy=False)
        return out

    # median
    order = np.argsort(pos, kind="stable")
    pos_sorted = pos[order]
    vals_sorted = vals[order]
    unique_pos, start_idx, counts = np.unique(pos_sorted, return_index=True, return_counts=True)
    out = np.full(n_targets, np.nan, dtype=np.float32)
    for cell, start, count in zip(unique_pos, start_idx, counts, strict=False):
        out[int(cell)] = np.median(vals_sorted[start : start + count]).astype(np.float32)
    return out
