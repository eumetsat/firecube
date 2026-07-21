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

"""Metadata mapping between xarray CF conventions and Tensogram CBOR dicts.

No tensogram import at module level - this module works with plain Python dicts
that happen to be compatible with Tensogram's CBOR metadata format.
"""

from __future__ import annotations

import math
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import numpy as np
import xarray as xr

import firecube

if TYPE_CHECKING:
    from firecube.core.storage.session import StorageSession

_NUMPY_DTYPE_TO_TENSOGRAM = {
    "float16": "float16",
    "float32": "float32",
    "float64": "float64",
    "int8": "int8",
    "int16": "int16",
    "int32": "int32",
    "int64": "int64",
    "uint8": "uint8",
    "uint16": "uint16",
    "uint32": "uint32",
    "uint64": "uint64",
    "complex64": "complex64",
    "complex128": "complex128",
}

_SAFE_ATTR_TYPES = (str, int, float, bool)


def numpy_dtype_to_tensogram(dtype: np.dtype) -> str:
    """Convert a numpy dtype to a Tensogram dtype string."""
    return _NUMPY_DTYPE_TO_TENSOGRAM.get(np.dtype(dtype).name, np.dtype(dtype).name)


def _normalize_attr_value(value: Any) -> Any:
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, tuple):
        return [_normalize_attr_value(item) for item in value]
    if isinstance(value, list):
        return [_normalize_attr_value(item) for item in value]
    if isinstance(value, np.ndarray):
        return [_normalize_attr_value(item) for item in value.tolist()]
    if isinstance(value, float) and math.isnan(value):
        return value
    if isinstance(value, _SAFE_ATTR_TYPES):
        return value
    return None


def _collect_safe_attrs(attrs: dict[str, Any]) -> dict[str, Any]:
    safe_attrs: dict[str, Any] = {}
    for key, value in attrs.items():
        normalized = _normalize_attr_value(value)
        if normalized is not None:
            safe_attrs[key] = normalized
    return safe_attrs


def dataset_to_global_meta(
    ds: xr.Dataset,
    *,
    source_uri: str,
    compression: str,
    base: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build a Tensogram metadata dict from an xarray Dataset.

    Tensogram >=0.18 metadata frames are free-form; the wire version lives in
    the file preamble, so no top-level "version" key is written.
    """
    meta: dict[str, Any] = {
        "firecube": {
            "version": getattr(firecube, "__version__", "unknown"),
            "source_uri": source_uri,
            "archived_at": datetime.now(UTC).isoformat(),
            "compression": compression,
        },
    }

    if base is not None:
        meta["base"] = base

    meta.update(_collect_safe_attrs(dict(ds.attrs)))
    return meta


def _datetime64_to_float64(values: np.ndarray) -> np.ndarray:
    return values.astype("datetime64[ns]").astype("float64")


def prepare_array_for_encoding(var: xr.Variable) -> np.ndarray:
    """Convert an xarray variable to a contiguous array for Tensogram encoding."""
    arr = np.asarray(var.values)
    if np.issubdtype(arr.dtype, np.datetime64):
        arr = _datetime64_to_float64(arr)
    return np.ascontiguousarray(arr)


def variable_to_descriptor(
    name: str,
    var: xr.Variable,
    *,
    compression: str,
) -> dict[str, Any]:
    """Build a Tensogram DataObjectDescriptor dict from an xarray Variable."""
    values = np.asarray(var.values)
    if np.issubdtype(values.dtype, np.datetime64):
        values = _datetime64_to_float64(values)
        dtype = "float64"
    else:
        dtype = numpy_dtype_to_tensogram(values.dtype)

    desc: dict[str, Any] = {
        "type": "ntensor",
        "shape": list(values.shape),
        "dtype": dtype,
        "compression": compression,
    }

    attrs = _collect_safe_attrs(dict(var.attrs))
    if attrs:
        desc["attrs"] = attrs

    return desc


def extract_zarr_array_metadata(
    store_path: str,
    group: str | None,
    *,
    storage_config: Any | None = None,
    storage_options: dict[str, Any] | None = None,
    session: StorageSession | None = None,
) -> dict[str, dict[str, Any]]:
    """Extract zarr array metadata (chunks, compressor, fill_value, dtype, order).

    Returns a dict mapping array name to its metadata. Only top-level arrays
    within the group are returned (no recursive descent into subgroups).

    Args:
        store_path: Local path or s3:// URI to the zarr store.
        group: Group path within the store (e.g. "F024"), or None for root.
        storage_config: StorageConfig honored when the store is remote via the
            session Zarr API. Only consulted when ``session`` is not provided.
        storage_options: legacy fsspec storage options dict; only consulted
            when neither ``session`` nor ``storage_config`` is provided.
        session: Optional pre-configured StorageSession. When provided, metadata
            reads use the session's driver (driver-correct path); otherwise a
            fresh session is constructed from ``storage_config`` and may
            silently downgrade to fsspec.

    Returns:
        Dict mapping variable name to metadata dict with keys:
        chunks, compressor (dict or None), fill_value, dtype (str), order.
    """
    import zarr

    from firecube.core.product.identity import ProductIdentity
    from firecube.core.storage.binding import StorageBinding
    from firecube.core.storage.driver_config import StorageDriverConfig
    from firecube.core.storage.session import StorageSession
    from firecube.core.uris import storage_uri_from_target

    store_uri = storage_uri_from_target(store_path)
    if session is None:
        if storage_config is None:
            raise ValueError(
                "storage_config is required for zarr metadata extraction; pass --storage-type and --storage-driver"
            )
        effective_config = storage_config
        session = StorageSession(
            StorageBinding(
                identity=ProductIdentity.from_uri(
                    store_uri,
                    format="zarr",
                    product_name=store_path,
                ),
                driver=StorageDriverConfig.from_storage_config(effective_config),
            )
        )
    handle = session.zarr.create_store(uri=store_uri, mode="r")

    open_kwargs: dict[str, Any] = {**handle.zarr_kwargs(), "mode": "r"}
    if group is not None:
        open_kwargs["path"] = group

    zgroup = zarr.open_group(**open_kwargs)

    result: dict[str, dict[str, Any]] = {}
    for name, item in zgroup.members():
        if not isinstance(item, zarr.Array):
            continue
        compressor_config: dict[str, Any] | None = None
        compressors = item.compressors
        if compressors:
            codec = compressors[0]
            to_dict = getattr(codec, "to_dict", None)
            get_config = getattr(codec, "get_config", None)
            if to_dict is not None:
                compressor_config = to_dict()
            elif get_config is not None:
                compressor_config = get_config()
            else:
                compressor_config = {"id": str(codec)}
        result[name] = {
            "chunks": tuple(item.chunks),
            "compressor": compressor_config,
            "fill_value": item.fill_value,
            "dtype": str(item.dtype),
            "order": item.order,
        }
    return result


def variable_to_base_entry(
    name: str,
    var: xr.Variable,
    *,
    zarr_meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the meta["base"][i] entry for a variable.

    If zarr_meta is provided (from extract_zarr_array_metadata), embeds
    the original zarr chunk shapes, compressor config, and fill_value.
    These are used by archive restore to recreate the original encoding.
    """
    entry: dict[str, Any] = {"name": str(name), "dim_names": list(var.dims)}
    if zarr_meta is not None:
        entry["zarr_chunks"] = list(zarr_meta["chunks"])
        if zarr_meta.get("compressor") is not None:
            entry["zarr_compressor"] = zarr_meta["compressor"]
        entry["zarr_fill_value"] = zarr_meta["fill_value"]
    return entry


def restore_attrs_from_meta(meta: dict[str, Any]) -> dict[str, Any]:
    """Recover xarray attrs from Tensogram-compatible metadata."""
    attrs = dict(meta.get("attrs", {}))
    restored: dict[str, Any] = {}
    for key, value in attrs.items():
        if value == "NaN":
            restored[key] = float("nan")
        elif value == "+Inf":
            restored[key] = float("inf")
        elif value == "-Inf":
            restored[key] = float("-inf")
        else:
            restored[key] = value
    return restored
