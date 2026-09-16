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

"""Read-only schema preflight for xarray append and region payloads."""

import warnings
from typing import Any

import numpy as np
import xarray as xr
import zarr
from zarr.errors import GroupNotFoundError

from firecube.core.filesystem.store_factory import ZarrStoreHandle
from firecube.core.zarr.time_decode import decode_time_array, encode_time_array
from firecube.ingestor.errors import SchemaDriftReingestError


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


def validate_existing_time_array_schema(
    ds: xr.Dataset, handle: ZarrStoreHandle, group: str, *, time_dim: str, state_var_name: str
) -> None:
    """Validate an existing group; a new group has no schema to compare."""
    try:
        root = zarr.open_group(**handle.zarr_kwargs(), mode="r", use_consolidated=False)
    except (FileNotFoundError, GroupNotFoundError):
        return
    if group in root:
        validate_time_array_schema(
            ds,
            root[group],
            store_uri=handle.target_uri,
            time_dim=time_dim,
            state_var_name=state_var_name,
        )
