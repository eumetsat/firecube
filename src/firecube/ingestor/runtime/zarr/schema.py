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

"""Read-only schema preflight for xarray append and region payloads.

The CF-packed payload view decodes raw packed variables with the stored packing, so that
xarray's re-encode on append and region writes stores the same raw values.
"""

import math
import warnings
from typing import Any

import numpy as np
import xarray as xr
import zarr
from xarray.backends import ZarrStore
from zarr.errors import GroupNotFoundError

from firecube.core.errors import Message
from firecube.core.filesystem.store_factory import ZarrStoreHandle
from firecube.core.zarr.time_decode import decode_time_array, encode_time_array
from firecube.ingestor.errors import ConfigurationError, SchemaDriftError, SchemaDriftReingestError

CF_PACKING_KEYS = ("scale_factor", "add_offset", "_FillValue", "missing_value", "_Unsigned")
"""The CF attributes that xarray's encoders own; they pack raw values on disk."""


def time_array_names(ds: xr.Dataset, time_dim: str, state_var_name: str) -> set[str]:
    return {
        str(name)
        for name, variable in ds.variables.items()
        if time_dim in variable.dims and name not in {time_dim, state_var_name}
    }


def validate_time_array_schema(
    ds: xr.Dataset, group: Any, *, store_uri: str, time_dim: str, state_var_name: str
) -> None:
    """Require the same time-aligned payload arrays before any array can grow."""
    stored = {
        str(name)
        for name in group.array_keys()
        if time_dim in (group[name].metadata.dimension_names or ())
        and name not in {time_dim, state_var_name}
    }
    incoming = time_array_names(ds, time_dim, state_var_name)
    missing = stored - incoming
    extra = incoming - stored
    if missing or extra:
        raise SchemaDriftReingestError(
            store_uri=store_uri,
            dataset_variable=sorted(missing or extra)[0],
            reason="batch_missing_store_variable" if missing else "extra_incoming_variable",
        )
    for name, variable in ds.variables.items():
        if time_dim in variable.dims and variable.dtype.kind == "M" and name in group:
            _validate_datetime_encoding(variable, group[str(name)], str(name))


def _validate_datetime_encoding(variable: xr.Variable, array: Any, name: str) -> None:
    """Refuse a change of time units that xarray would otherwise only warn about."""
    if array.dtype.kind == "M":
        return
    attrs = dict(array.attrs)
    values = np.asarray(variable.values)
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", UserWarning)
            encoded, _, _ = encode_time_array(values, attrs)
        decoded = decode_time_array(np.asarray(encoded).astype(array.dtype), attrs)
        if not np.array_equal(values, decoded, equal_nan=True):
            raise ValueError("datetime values do not round-trip")
    except (ValueError, UserWarning, OverflowError) as exc:
        raise ValueError(
            f"Array {name!r}: incoming dates cannot be represented by the stored time encoding. "
            "Rebuild a new target with datetime encoding that preserves the required precision."
        ) from exc


def decode_cf_packed_payload(
    ds: xr.Dataset, handle: ZarrStoreHandle, group: str, *, time_dim: str, state_var_name: str
) -> xr.Dataset:
    """Return `ds` with each raw CF-packed variable decoded with its stored packing.

    A time-indexed data variable of the payload qualifies when its attrs carry a key of
    `CF_PACKING_KEYS` and `group` of `handle`, the store being written, holds it.

    Returns:
        `ds` itself when no variable qualifies, else a shallow copy; `ds` is never mutated.

    Raises:
        SchemaDriftError: The CF keys, a key's value or the dtype differ from the stored array.
        ConfigurationError: float64 arithmetic cannot restore the raw values of that packing.
    """
    names = [
        str(name)
        for name, variable in ds.data_vars.items()
        if time_dim in variable.dims
        and name not in {time_dim, state_var_name}
        and any(key in variable.attrs for key in CF_PACKING_KEYS)
    ]
    if not names:
        return ds
    path = str(group).strip("/")
    try:
        # Opened by its own path: neither the root nor a child snapshot hides live metadata.
        node = zarr.open_group(**handle.zarr_kwargs(), path=path, mode="r", use_consolidated=False)
    except (FileNotFoundError, GroupNotFoundError):
        return ds
    store = ZarrStore(node, mode="r", use_zarr_fill_value_as_mask=False)
    stored_names = set(store.array_keys())
    decoded = {
        name: _decode_packed(
            name, ds[name].variable, store.open_store_variable(name), path=path, group=str(group)
        )
        for name in names
        if name in stored_names
    }
    return ds.assign(decoded) if decoded else ds


def _decode_packed(
    name: str, variable: xr.Variable, stored: xr.Variable, *, path: str, group: str
) -> xr.Variable:
    """Refuse drift or an inexact packing, then decode the raw values with the stored keys."""
    array_path = f"{path}/{name}" if path else name
    incoming_cf = {key: variable.attrs[key] for key in CF_PACKING_KEYS if key in variable.attrs}
    stored_cf = {key: stored.attrs[key] for key in CF_PACKING_KEYS if key in stored.attrs}
    differing = sorted(
        key
        for key in incoming_cf.keys() | stored_cf.keys()
        if key not in incoming_cf
        or key not in stored_cf
        or not _cf_values_equal(key, stored_cf[key], incoming_cf[key])
    )
    if (variable.dtype.kind, variable.dtype.itemsize) != (stored.dtype.kind, stored.dtype.itemsize):
        differing.append("dtype")
    if differing:
        raise SchemaDriftError(
            Message(
                "Array {array_path!r} in group {group!r} differs from the stored array in "
                "{keys}: stored {stored}, incoming {incoming}. Keep the stored CF packing and "
                "dtype, or write a new target.",
                array_path=array_path,
                group=group,
                keys=", ".join(differing),
                stored=_describe(differing, stored_cf, stored.dtype),
                incoming=_describe(differing, incoming_cf, variable.dtype),
            )
        )
    reason = _inexact_packing_reason(stored.dtype, stored_cf)
    if reason is not None:
        raise ConfigurationError(
            Message(
                "Variable {name!r} (array {array_path!r} in group {group!r}, dtype {dtype}) "
                "carries a CF packing that float64 cannot reproduce exactly: {reason}. "
                "Write it unpacked, or with a packing that float64 reproduces.",
                name=name,
                array_path=array_path,
                group=group,
                dtype=str(stored.dtype),
                reason=reason,
            )
        )
    raw = variable.copy(deep=False)
    raw.encoding = {}  # xarray writes an existing array with the store's encoding.
    raw.attrs = {key: value for key, value in variable.attrs.items() if key not in CF_PACKING_KEYS}
    for key, value in stored_cf.items():
        # Float scale and offset make xarray decode integers in float64, never float32.
        raw.attrs[key] = float(value) if key in ("scale_factor", "add_offset") else value
    # decode_times=False turns timedelta decoding off too.
    decoded = xr.decode_cf(
        xr.Dataset({name: raw}), mask_and_scale=True, decode_times=False, decode_coords=False
    )
    return decoded[name].variable


def _cf_values_equal(key: str, stored: Any, incoming: Any) -> bool:
    """Compare two CF values as float64 numbers with NaN equal to NaN; `_Unsigned` as text."""
    if key == "_Unsigned":
        return str(stored) == str(incoming)
    try:
        left = np.asarray(stored, dtype=np.float64)
        right = np.asarray(incoming, dtype=np.float64)
    except (TypeError, ValueError):
        return str(stored) == str(incoming)
    return left.shape == right.shape and bool(np.array_equal(left, right, equal_nan=True))


def _describe(keys: list[str], cf: dict[str, Any], dtype: np.dtype) -> str:
    """Render one side of a drift: each named key with its value, or as absent."""
    shown = {"dtype": str(dtype), **{key: repr(value) for key, value in cf.items()}}
    return ", ".join(f"{key}={shown[key]}" if key in shown else f"{key} absent" for key in keys)


def _inexact_packing_reason(dtype: np.dtype, cf: dict[str, Any]) -> str | None:
    """Name why float64 cannot restore every raw value of `dtype` under `cf`, or return None."""
    scalar = (int, float, np.integer, np.floating)  # bool is an int; np.bool_ is neither
    numbers = {k: float(v) for k, v in cf.items() if isinstance(v, scalar) and type(v) is not bool}
    not_scalar = sorted(set(cf) - set(numbers) - {"_Unsigned"})
    if not_scalar:
        return f"{', '.join(not_scalar)} is not a scalar number"
    if dtype.kind in "iu" and dtype.itemsize > 4:
        return "the dtype is an integer wider than 32 bits"
    if dtype.kind not in "iu" and (dtype.kind != "f" or {"scale_factor", "add_offset"} & set(cf)):
        return "the packing needs an integer dtype, or a floating dtype that only masks"
    scale, offset = numbers.get("scale_factor", 1.0), numbers.get("add_offset", 0.0)
    if scale == 0.0 or not math.isfinite(scale) or not math.isfinite(offset):
        return "scale_factor is zero or not finite, or add_offset is not finite"
    span = 2.0 ** (8 * dtype.itemsize)
    # An overflow in either bound gives inf, which counts as over the bound.
    if dtype.kind in "iu" and (
        span * abs(scale) + abs(offset) > 2.0**1000 or 4 * span + abs(offset / scale) > 2.0**50
    ):
        return "scale_factor and add_offset leave too few float64 bits for every raw value"
    fill, missing = cf.get("_FillValue"), cf.get("missing_value")
    if fill is not None and missing is not None and not _cf_values_equal("", fill, missing):
        return "_FillValue differs from missing_value"
    return None


def validate_existing_time_array_schema(
    ds: xr.Dataset, handle: ZarrStoreHandle, group: str, *, time_dim: str, state_var_name: str
) -> None:
    """Validate an existing group; a new group has no schema to compare."""
    try:
        root = zarr.open_group(**handle.zarr_kwargs(), mode="r", use_consolidated=False)
    except (FileNotFoundError, GroupNotFoundError):
        return
    if group in root:
        is_root = not str(group).strip("/")
        # `root[""]` reloads the consolidated snapshot, so the root uses the live handle.
        node = root if is_root else root[group]
        if is_root and not any(True for _ in root.array_keys()):
            # The root document exists before any cube array does, and a failed-batch
            # repair of a fresh root group keeps it: a root without arrays is new.
            return
        validate_time_array_schema(
            ds,
            node,
            store_uri=handle.target_uri,
            time_dim=time_dim,
            state_var_name=state_var_name,
        )
