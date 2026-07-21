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

"""NetCDF-to-Zarr preparation utilities.

Composable helpers that normalize xarray Datasets loaded from NetCDF
sources into the shape expected by Firecube's Zarr writer. Plugin-agnostic.
"""

from __future__ import annotations

import logging
from collections.abc import Collection
from contextlib import suppress

import numpy as np
import xarray as xr

from firecube.core.formats._iso import _iso_strs_to_datetime64


def clean_netcdf_encoding(ds: xr.Dataset) -> xr.Dataset:
    """Strip HDF5 chunk hints from all variable encodings.

    Removes 'chunks', 'chunksizes', and 'preferred_chunks' from each
    variable's encoding dict so they do not conflict with the Zarr chunk
    layout chosen by the core writer. Modifies encoding dicts in place;
    returns the same dataset object.
    """
    for var_name in list(ds.variables):
        enc = ds[var_name].encoding
        enc.pop("chunks", None)
        enc.pop("chunksizes", None)
        enc.pop("preferred_chunks", None)
    return ds


def rename_time_dim(
    ds: xr.Dataset,
    source: str = "time",
    target: str = "timestamp",
) -> xr.Dataset:
    """Rename the time dimension to Firecube's append convention.

    If *source* exists as a dimension or coordinate, rename it to *target*.
    If *source* is not present, return the dataset unchanged (no error).
    """
    if source in ds.dims or source in ds.coords:
        return ds.rename({source: target})
    return ds


def prepare_netcdf_for_zarr(
    ds: xr.Dataset,
    time_dim: str = "time",
    target_time_dim: str = "timestamp",
) -> xr.Dataset:
    """Convenience wrapper: rename time dimension then clean encodings.

    Applies all standard NetCDF→Zarr V3 preparation steps in order:
    1. Rename time dimension from *time_dim* to *target_time_dim*.
    2. Strip HDF5 chunk hints from all variable encodings.
    """
    ds = rename_time_dim(ds, source=time_dim, target=target_time_dim)
    ds = clean_netcdf_encoding(ds)
    return ds


# ─── string variable normalization ─────────────────────────

_ACCEPTED_ELEMENT_TYPES: tuple[type, ...] = (str, bytes, np.str_, np.bytes_)


def _is_string_var_pure(values: np.ndarray) -> bool:
    """Pure predicate: True when values holds a string-typed var we can normalize.

    Returns False for anything we cannot confidently classify; never raises.
    """
    kind = values.dtype.kind
    if kind in ("S", "U"):
        return True
    if kind != "O" or values.size == 0:
        return False
    return all(isinstance(x, _ACCEPTED_ELEMENT_TYPES) for x in values.ravel())


def _values_to_str_list(values: np.ndarray) -> list[str]:
    """Convert a string-shaped ndarray to a flat list of Python str."""
    kind = values.dtype.kind
    if kind == "U":
        return values.ravel().tolist()
    if kind == "S":
        try:
            return np.char.decode(values, "utf-8").ravel().tolist()
        except UnicodeDecodeError as exc:
            raise ValueError(f"invalid UTF-8 bytes in string var: {exc}") from exc
    out: list[str] = []
    for x in values.ravel():
        if isinstance(x, str):
            out.append(x)
        elif isinstance(x, (bytes, np.bytes_)):
            try:
                out.append(x.decode("utf-8"))
            except UnicodeDecodeError as exc:
                raise ValueError(f"invalid UTF-8 bytes in string var: {exc}") from exc
        else:
            out.append(str(x))
    return out


def normalize_string_vars(
    ds: xr.Dataset,
    *,
    iso_targets: Collection[str] | None = None,
    logger: logging.Logger | None = None,
    preserve_cf_time_attrs: bool = False,
) -> xr.Dataset:
    """Normalize string-typed variables in a NetCDF-loaded Dataset for Zarr writes.

    Detects string vars by dtype kind (``S``, ``U``, or ``O`` with all-string
    content) and applies one of:

    - **ISO conversion**: vars named in ``iso_targets`` are converted from ISO-8601
      UTC strings to ``datetime64[s]`` or ``datetime64[us]`` (auto-detected by
      fractional-second presence). By default, ``units`` and ``calendar`` attrs (if
      present on the source variable) are moved from ``.attrs`` to ``.encoding`` on
      the converted variable — matching xarray's convention for decoded CF time
      metadata (metadata consumed during conversion is relocated to encoding rather
      than deleted). Set ``preserve_cf_time_attrs=True`` to keep them verbatim in
      ``.attrs``; note that firecube's CF advisor may then flag the variable. The
      returned ISO-converted variable always receives fresh ``.encoding`` — source
      encoding hints (chunks, dtype, pre-existing time-decode keys) are NOT copied.
    - **UTF-8 decode**: other string vars are decoded to ``<U*>`` (bytes decoded,
      object arrays consolidated to fixed-width).

    Variables whose dtype is ``datetime64`` are silently skipped even if named in
    ``iso_targets`` (already decoded — nothing to do). Variables with non-string
    object content are also silently skipped, with a DEBUG log when ``logger`` is
    provided.

    Raises ``ValueError`` only when a name in ``iso_targets`` exists in the dataset
    AND its data cannot be processed (empty object array, non-string object content,
    unparseable ISO string, or invalid UTF-8 bytes). A name in ``iso_targets`` that
    is absent from the dataset is silently skipped.

    This utility normalizes detectable string variables. It does NOT validate
    whole-dataset Zarr writability; callers who require strict schemas should run a
    separate validator.

    Args:
        ds: Input dataset (post-concat is fine; handles vlen-string widening).
        iso_targets: Variable names to convert to ``datetime64``. ``None`` skips
            ISO conversion (UTF-8 decode is still applied to other string vars).
        logger: Optional logger for DEBUG-level skip diagnostics.
        preserve_cf_time_attrs: If True, keep ``units`` and ``calendar`` attrs on
            ISO-converted variables verbatim in ``.attrs`` and do NOT write them to
            ``.encoding``. Default False moves them to ``.encoding`` (xarray convention).
            Ignored for CF-time attr handling when no variable is ISO-converted; other
            string normalization still runs.

    Returns:
        A new dataset. The input is not mutated.
    """
    iso_target_set: frozenset[str] = frozenset(iso_targets or ())
    replacements: dict[str, xr.DataArray] = {}

    for name, var in ds.data_vars.items():
        var_name = str(name)
        values = np.asarray(var.values)

        if values.dtype.kind == "M":
            continue

        is_str = _is_string_var_pure(values)
        in_targets = var_name in iso_target_set

        if in_targets and not is_str:
            raise ValueError(
                f"variable {var_name!r} is in iso_targets but is not a normalizable "
                f"string var (dtype.kind={values.dtype.kind!r}, size={values.size})"
            )

        if not is_str:
            if values.dtype.kind == "O" and logger is not None:
                logger.debug(
                    "normalize_string_vars: skipping object-dtype var %r (size=%d)",
                    var_name,
                    values.size,
                )
            continue

        strs = _values_to_str_list(values)
        if in_targets:
            new_values = _iso_strs_to_datetime64(strs).reshape(values.shape)
            new_attrs = dict(var.attrs)
            new_encoding: dict[str, object] = {}
            if not preserve_cf_time_attrs:
                # Match xarray's pop_to() convention (xarray/coding/times.py:1407):
                # attrs consumed during type conversion move to .encoding, preserving
                # metadata without polluting .attrs on the decoded array.
                for key in ("units", "calendar"):
                    if key in new_attrs:
                        new_encoding[key] = new_attrs.pop(key)
            new_da = xr.DataArray(new_values, dims=var.dims, attrs=new_attrs, name=var_name)
            new_da.encoding = new_encoding
            replacements[var_name] = new_da
        else:
            new_values = np.asarray(strs, dtype=object).reshape(values.shape)
            with suppress(UnicodeDecodeError, ValueError):
                new_values = new_values.astype(str)
            replacements[var_name] = xr.DataArray(
                new_values, dims=var.dims, attrs=dict(var.attrs), name=var_name
            )

    return ds.assign(replacements)
