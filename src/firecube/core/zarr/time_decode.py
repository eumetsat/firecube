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

"""Self-describing time-array decode helper.

Dispatches on (dtype, attrs) — firecube's vocabulary for encoded time arrays.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

import numpy as np

__all__ = [
    "decode_or_passthrough",
    "decode_time_array",
    "encode_time_array",
    "missing_time_mask",
]


def _xarray_time_codecs() -> tuple[Callable[..., Any], Callable[..., Any]]:
    from xarray.coding.times import decode_cf_datetime, encode_cf_datetime

    return decode_cf_datetime, encode_cf_datetime


def decode_time_array(values: np.ndarray, attrs: Mapping[str, Any]) -> np.ndarray:
    """Decode *values* into a time array using the CF ``units``/``calendar`` in *attrs*.

    For a Gregorian-compatible calendar whose decoded range fits inside
    ``datetime64``, the result is a ``datetime64`` array that preserves its
    native decoded resolution rather than being forced to a fixed granularity.
    Coverage bounds and dedup keys are derived from this output, so coarsening
    to seconds here would silently collapse distinct sub-second timestamps into
    one (corrupting dedup/coverage); coarsening is therefore left to the
    storage layer, which owns the on-disk precision contract. The resolution
    that ``decode_cf_datetime`` selects is range-aware, so this also avoids
    forcing a finer unit that could overflow for out-of-range epochs.

    For any other calendar (for example ``360_day`` or ``noleap``), and for a
    Gregorian calendar whose decoded values fall outside the range
    ``datetime64`` can represent, xarray's CF decoder instead returns an
    object array (``dtype.kind == "O"``) of calendar-valued scalars, the same
    ``cftime`` instances xarray produces with ``use_cftime=True``. Callers that
    branch on this array's dtype, such as ordering checks, missing-value
    detection, or coverage-bounds tracking, must handle both shapes.
    ``decode_time_array`` never imports ``cftime`` itself and never raises to
    force one shape over the other; it returns whatever xarray's decoder
    produces for the given ``units``/``calendar``.
    """

    values = np.asarray(values)

    if values.dtype.kind == "M":
        return values

    if values.dtype.kind in ("f", "i", "u"):
        units = attrs.get("units") if attrs else None
        if units is None:
            raise ValueError(
                f"Cannot decode numeric dtype {values.dtype!r}: no 'units' attr found. "
                "Expected a units string like 'seconds since 1970-01-01'."
            )
        units_str = str(units)
        if "since" not in units_str:
            raise ValueError(
                f"Cannot decode numeric dtype {values.dtype!r}: 'units' attr {units_str!r} "
                "does not contain 'since'. Expected a reference-epoch string like "
                "'seconds since 1970-01-01'."
            )
        decode_cf_datetime, _ = _xarray_time_codecs()
        calendar = str(attrs.get("calendar", "standard"))
        decoded = decode_cf_datetime(values, units=units_str, calendar=calendar)
        return np.asarray(decoded)

    raise ValueError(
        f"Cannot decode time array with dtype {values.dtype!r}: not a datetime64 or "
        "a numeric type with 'units' containing 'since'."
    )


def missing_time_mask(values: np.ndarray) -> np.ndarray:
    """Return a boolean missing-value mask for a *decoded* time-like array.

    Handles every shape :func:`decode_or_passthrough` can return: ``datetime64``
    (``NaT``, via ``np.isnat``), an object array of calendar-valued scalars as
    :func:`decode_time_array` returns for a non-standard or out-of-range
    calendar (a missing slot is ``None`` or a float ``NaN`` element, since
    numpy's ``isnan``/``isnat`` ufuncs reject object dtype). Any other dtype is
    not time-like and reports no missing slots: a caller that treats ``NaN`` as
    missing in a numeric passthrough array must test for it itself.
    """

    array = np.asarray(values)
    if array.dtype.kind == "M":
        return np.isnat(array)
    if array.dtype.kind == "O":
        flat = array.reshape(-1)
        mask = np.fromiter(
            (item is None or (isinstance(item, float) and np.isnan(item)) for item in flat),
            dtype=bool,
            count=flat.size,
        )
        return mask.reshape(array.shape)
    return np.zeros(array.shape, dtype=bool)


def decode_or_passthrough(values: np.ndarray, attrs: Mapping[str, Any]) -> np.ndarray:
    """Decode CF-encoded time values; return numeric arrays unchanged when they lack a CF units.

    Dispatch rules:

    * ``datetime64`` (dtype kind ``"M"``) → return as-is (already time).
    * Numeric (kind ``"f"``, ``"i"``, ``"u"``) with a non-``None`` ``units``
      attr → :func:`decode_time_array`; malformed units (missing ``"since"``
      or a bad reference date) propagate as :class:`ValueError`.
    * Numeric with ``units`` absent → passthrough (bare numeric counter).
    * Other dtypes → passthrough.
    """

    values = np.asarray(values)
    if values.dtype.kind == "M":
        return values
    if values.dtype.kind in ("f", "i", "u"):
        units = attrs.get("units") if attrs else None
        if units is not None:
            return decode_time_array(values, attrs)
    return values


def encode_time_array(values: np.ndarray, attrs: dict[str, Any]) -> tuple[np.ndarray, str, str]:
    """Return CF-encoded datetime values using the stored time-array *attrs*.

    Malformed ``units`` or ``calendar`` attributes propagate as ``ValueError``
    from xarray so callers cannot silently accept a lossy or invalid encoding.
    """

    values = np.asarray(values)
    units = attrs.get("units")
    calendar = attrs.get("calendar")
    _, encode_cf_datetime = _xarray_time_codecs()
    encoded, encoded_units, encoded_calendar = encode_cf_datetime(
        values,
        units=str(units) if units is not None else None,
        calendar=str(calendar) if calendar is not None else None,
    )
    return np.asarray(encoded), str(encoded_units), str(encoded_calendar)
