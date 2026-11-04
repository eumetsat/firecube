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

"""Deletion Engine: Handles chunk deletion, vacuuming, and storage cleanup."""

from __future__ import annotations

import itertools
import logging
import math
import uuid
from collections.abc import Callable, Iterable, Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, cast

from firecube.core.config import StorageConfig
from firecube.core.controlplane.repo import ManifestRepository
from firecube.core.controlplane.time_dim import resolve_span_time_dims, resolve_time_dim_index
from firecube.core.controlplane.types import (
    EVENT_SCHEMA_VERIFICATION,
    MAINTENANCE_OP_DELETE,
    STATE_DELETED_BY_FIRECUBE,
    ChunkInfo,
    ClaimInfo,
    DeletionPlan,
    WriteDomain,
)
from firecube.core.errors import (
    ClaimConflictError,
    ManifestError,
    Message,
    Param,
    ProductUnderMaintenanceError,
    StorageError,
)
from firecube.core.runtime import identity_from_storage_config
from firecube.core.storage.session import _literal_delete
from firecube.core.storage.uri import StorageUri
from firecube.core.zarr.validation import _STATE_ARRAY_NAME

log = logging.getLogger(__name__)

# Record types whose keys name control-plane records, not storage objects. Storage
# deletion skips them: their data is removed with `delete-span`, not by key.
RECORD_ONLY_TYPES = frozenset({"span", "run", EVENT_SCHEMA_VERIFICATION})


def _span_time_indices(time_ranges: Sequence[Any]) -> list[int]:
    indices: set[int] = set()
    for pair in time_ranges:
        if not (isinstance(pair, (list, tuple)) and len(pair) == 2):
            continue
        try:
            start_i, end_i = int(pair[0]), int(pair[1])
        except (ValueError, TypeError, IndexError):
            continue
        if end_i < start_i:
            continue
        indices.update(range(start_i, end_i + 1))
    return sorted(indices)


def _contiguous_ranges(indices: Sequence[int]) -> list[tuple[int, int]]:
    ordered = sorted({int(i) for i in indices if int(i) >= 0})
    if not ordered:
        return []

    ranges: list[tuple[int, int]] = []
    start = previous = ordered[0]
    for idx in ordered[1:]:
        if idx == previous + 1:
            previous = idx
            continue
        ranges.append((start, previous))
        start = previous = idx
    ranges.append((start, previous))
    return ranges


def _span_group_name(span: ChunkInfo) -> str:
    meta = span.meta if isinstance(span.meta, dict) else {}
    return str(meta["group"]).strip("/")


def _span_array_store_path(array_name: str, group: str, has_array: Callable[[str], bool]) -> str:
    """Return the store path of a data array a span records under its meta ``group``.

    A name with a leading ``/`` is root-relative. Any other name is group-relative,
    unless it already starts with ``<group>/`` (GenericZarr). There ``has_array``
    picks between ``<name>`` and ``<group>/<name>``.

    Raises:
        ValueError: Both candidate arrays exist, so the record cannot name one.
    """
    raw = str(array_name)
    clean = raw.strip("/")
    group_path = group.strip("/")
    if raw.startswith("/") or not group_path:
        return clean
    joined = f"{group_path}/{clean}"
    if not clean.startswith(f"{group_path}/"):
        return joined
    # A DirectZarr array whose name starts with its group's name looks like
    # the GenericZarr `<group>/<name>` record; only the store tells them apart.
    if has_array(joined):
        if has_array(clean):
            raise ValueError(
                f"recorded array {raw!r} of group {group_path!r} matches both {clean} and "
                f"{joined} in the store; refusing to guess which one the span wrote"
            )
        return joined
    return clean


def _span_array_store_paths(
    arrays: Iterable[Any], group: str, has_array: Callable[[str], bool]
) -> list[str]:
    """Resolve a span's recorded array names to unique store paths, in order."""
    return list(
        dict.fromkeys(_span_array_store_path(str(name), group, has_array) for name in arrays)
    )


def _store_array_probe(store_uri: str, storage_config: Any) -> Callable[[str], bool]:
    """Return a cached check of whether the live store holds an array at a path."""
    import zarr

    from firecube.core.filesystem.store_factory import create_zarr_store

    root: list[Any] = []
    known: dict[str, bool] = {}

    def has_array(path: str) -> bool:
        if path not in known:
            if any(part in (".", "..") for part in path.split("/")):
                known[path] = False
                return False
            if not root:
                handle = create_zarr_store(uri=store_uri, storage_config=storage_config, mode="r")
                try:
                    root.append(
                        zarr.open_group(
                            **handle.zarr_kwargs(), mode="r", zarr_format=3, use_consolidated=False
                        )
                    )
                except FileNotFoundError:
                    root.append(None)
            try:
                known[path] = root[0] is not None and isinstance(root[0][path], zarr.Array)
            except KeyError:
                known[path] = False
        return known[path]

    return has_array


def _chunk_object_uri(base_uri: StorageUri, chunk: ChunkInfo) -> StorageUri:
    """Return the URI of a chunk record's object inside its product.

    Raises:
        ValueError: The product or key is empty, absolute, or holds a ``.`` or ``..``
            component, or the URI falls outside the product.
    """
    product, key = str(chunk.product), str(chunk.key)
    for label, value in (("product", product), ("key", key)):
        parts = value.strip("/").split("/")
        if not value.strip("/") or value.startswith("/") or any(p in (".", "..") for p in parts):
            raise ValueError(
                f"Refused to delete chunk record {key!r} of product {product!r}: its {label} "
                "is not a relative path inside the product"
            )
    product_uri = base_uri.join(product)
    uri = product_uri.join(key)
    inside = (
        uri.protocol == product_uri.protocol
        and uri.authority == product_uri.authority
        and uri.path.startswith(f"{product_uri.path.rstrip('/')}/")
        and product_uri.path.startswith(f"{base_uri.path.rstrip('/')}/")
    )
    if not inside:
        raise ValueError(
            f"Refused to delete chunk record {key!r} of product {product!r}: "
            f"{uri.to_str()} is outside {product_uri.to_str()}"
        )
    return uri


def _is_managed_time_coordinate(store_uri: str, path: str, storage_config: Any) -> bool:
    """Return whether the array at ``path`` carries a preallocated or coordinate-managed marker.

    An absent array, a group and an unmarked (legacy) array return ``False``.

    Raises:
        SchemaDriftError: The array carries an inconsistent marker set.
    """
    import zarr

    from firecube.core.filesystem.store_factory import create_zarr_store
    from firecube.core.zarr._coord_lifecycle import (
        CoordLifecycleState,
        raise_if_invalid,
        resolve_coord_lifecycle,
    )

    handle = create_zarr_store(uri=store_uri, storage_config=storage_config, mode="r")
    root = cast(
        Any,
        zarr.open_group(**handle.zarr_kwargs(), mode="r", zarr_format=3, use_consolidated=False),
    )
    try:
        node = root[path]
    except KeyError:
        return False
    if not isinstance(node, zarr.Array):
        return False
    state = resolve_coord_lifecycle(dict(node.attrs))
    raise_if_invalid(state, path)
    return state in (CoordLifecycleState.PREALLOCATED, CoordLifecycleState.COORD_MANAGED)


def _expand_ranges_over_grids(
    time_ranges: Sequence[Any],
    per_array_grids: dict[str, tuple[list[str], list[int], list[int], int]],
) -> list[list[int]]:
    """Return the slots whose chunks a chunk-key deletion removes, over every grid."""
    from firecube.core.zarr.state import expand_time_index_ranges_to_chunk_boundaries

    slots: set[int] = set()
    for _dim_names, shape, chunk_shape, time_dim in per_array_grids.values():
        for start, end in expand_time_index_ranges_to_chunk_boundaries(
            time_ranges,
            chunk_len=int(chunk_shape[time_dim] or 0),
            length=int(shape[time_dim] or 0),
        ):
            slots.update(range(start, end + 1))
    return [[start, end] for start, end in _contiguous_ranges(sorted(slots))]


def _span_state_array_name(
    *, spec: dict[str, Any], group: str, default: str = _STATE_ARRAY_NAME
) -> str:
    state_path = spec.get("state_array")
    if not isinstance(state_path, str):
        return default

    clean = state_path.strip("/")
    if not clean:
        return default

    group_prefix = f"{group.strip('/')}/" if group.strip("/") else ""
    if group_prefix and clean.startswith(group_prefix):
        return clean.removeprefix(group_prefix).split("/", 1)[0]
    return clean.rsplit("/", 1)[-1]


def _is_completed_region_fill(span: ChunkInfo) -> bool:
    """True when ``span`` was already replaced by an in-place NaN/fill.

    Such spans reach ``delete_spans`` only through ``include_replaced``; their
    slots are already filled and their state already marks them deleted, so a
    second fill is a no-op the operator must ask for explicitly.
    """
    if span.status != "replaced":
        return False
    meta = span.meta if isinstance(span.meta, dict) else {}
    return meta.get("write_strategy") == "region_nan_fill"


def _array_path_relative_to_group(array_path: str, group: str) -> str:
    clean = str(array_path).strip("/")
    group = group.strip("/")
    if group and clean.startswith(f"{group}/"):
        return clean[len(group) + 1 :]
    return clean


def _array_dimension_names(array: Any) -> list[str] | None:
    """Return ``array``'s dimension names, or ``None`` when they are unknown.

    Reads zarr v3 ``dimension_names``, falling back to the ``_ARRAY_DIMENSIONS``
    attr. Names are unknown when neither is present, when any name is null,
    or when their count disagrees with ``ndim``; only a 0-d array without
    names is known to have none.
    """
    metadata = getattr(array, "metadata", None)
    raw_dim_names = getattr(metadata, "dimension_names", None)
    if raw_dim_names is None or all(dim is None for dim in raw_dim_names):
        raw_dim_names = getattr(array, "attrs", {}).get("_ARRAY_DIMENSIONS")
    ndim = int(array.ndim)
    if raw_dim_names is None:
        return [] if ndim == 0 else None
    names = list(raw_dim_names)
    if len(names) != ndim or any(dim is None for dim in names):
        return None
    return [str(dim) for dim in names]


def _unknown_dimensions_message(display_path: str, ndim: int) -> str:
    return (
        f"Array {display_path} has {ndim} dimensions but no dimension names; cannot tell "
        "whether it is time-indexed, refusing to delete the span"
    )


def _read_array_grid(
    store_uri: str, array_path: str, storage_config: Any
) -> tuple[list[str] | None, list[int], list[int]]:
    """Return ``(dimension names or None, shape, stored chunk shape)`` for one array.

    ``read_chunk_grid`` synthesises ``dim0..`` when ``dimension_names`` is
    absent, which hides both an ``_ARRAY_DIMENSIONS`` declaration and the
    absence of any names; the names are therefore read from the array itself.
    """
    import zarr

    from firecube.core.filesystem.store_factory import create_zarr_store
    from firecube.core.zarr.validation import _read_chunk_grid_from_zarr_array

    handle = create_zarr_store(uri=store_uri, storage_config=storage_config, mode="r")
    root = cast(
        Any,
        zarr.open_group(**handle.zarr_kwargs(), mode="r", zarr_format=3, use_consolidated=False),
    )
    array = root[array_path.strip("/")]
    _synthesised, shape, chunk_shape = _read_chunk_grid_from_zarr_array(array, array_path)
    return _array_dimension_names(array), shape, chunk_shape


def _skipped_arrays_warning(span_key: str, time_dim_name: str, skipped: Sequence[str]) -> str:
    return (
        f"Span {span_key}: skipped {len(skipped)} arrays without time dimension "
        f"'{time_dim_name}': {', '.join(skipped)}"
    )


def _no_time_indexed_array_message(time_dim_name: str, arrays: Sequence[str]) -> Message:
    # Every array lacking the dim is also what a wrong time-dim name looks
    # like, so the refusal carries the same remediation as the resolver.
    return Message(
        "no array in this span carries time dimension '{time_dim_name}'; refusing to mark "
        "it deleted (span arrays: {arrays}). If the cube was written "
        "with a different time dimension, pass it explicitly (CLI: {time_dim}).",
        time_dim_name=time_dim_name,
        arrays=", ".join(arrays) or "none",
        time_dim=Param("time_dim"),
    )


def _error_text(exc: BaseException) -> str:
    """Return the `Message` an exception was raised with, else its text."""
    first = exc.args[0] if exc.args else None
    return first if isinstance(first, Message) else str(exc)


def _decode_declared_float_fill(array_path: str, dtype: Any, units: str, declared: Any) -> Any:
    """Decode a float ``_FillValue`` attr the way xarray's Zarr backend does."""
    import base64
    import binascii
    import struct

    import numpy as np

    def _refuse(reason: str) -> ValueError:
        return ValueError(
            f"Array {array_path} (dtype {dtype}, units {units!r}) declares _FillValue "
            f"{declared!r}, which {reason}; refusing to delete"
        )

    if not isinstance(declared, str | bytes):
        raise _refuse("is not a base64-encoded little-endian double")
    try:
        payload = base64.b64decode(declared, validate=True)
    except (binascii.Error, ValueError):
        raise _refuse("is not valid base64") from None
    if len(payload) != 8:
        raise _refuse(f"decodes to {len(payload)} bytes, not an 8-byte double")
    (value,) = struct.unpack("<d", payload)
    with np.errstate(over="ignore"):
        cast_value = dtype.type(value)
    if not (math.isnan(value) and np.isnan(cast_value)) and float(cast_value) != value:
        raise _refuse("is not exactly representable in the dtype")
    return cast_value


def _cf_time_nat_sentinel(array_path: str, dtype: Any, units: str, declared: Any) -> Any:
    """Return the value that marks a CF-encoded numeric time slot as missing.

    Not computed through ``encode_time_array``: encoding an all-NaT input
    raises ``TypeError`` for the ``standard``/``gregorian`` calendars. The
    sentinel is chosen by dtype instead. A declared ``_FillValue`` wins:
    for float arrays it must be the Zarr v3 attribute encoding xarray
    decodes (base64 of a little-endian IEEE-754 double) and must be exactly
    representable in the dtype. Without one, floats take NaN and ``int64``
    takes its minimum (both decoded as NaT); any other integer dtype is
    refused.

    Raises:
        ValueError: The declared ``_FillValue`` is malformed or does not fit
            the dtype, or the dtype has no NaT sentinel and none is declared.
    """
    import numpy as np

    if dtype.kind == "f":
        if declared is None:
            return dtype.type(np.nan)
        return _decode_declared_float_fill(array_path, dtype, units, declared)
    if declared is not None:
        if isinstance(declared, bool) or not isinstance(declared, int):
            raise ValueError(
                f"Array {array_path} (dtype {dtype}, units {units!r}) declares _FillValue "
                f"{declared!r}, which is not an integer; cannot represent NaT"
            )
        try:
            return dtype.type(declared)
        except OverflowError as exc:
            raise ValueError(
                f"Array {array_path} (dtype {dtype}, units {units!r}) declares _FillValue "
                f"{declared!r}, which does not fit the dtype; cannot represent NaT"
            ) from exc
    if dtype == np.dtype(np.int64):
        return np.int64(np.iinfo(np.int64).min)
    raise ValueError(
        f"Array {array_path} with dtype {dtype} and units {units!r} cannot represent NaT; "
        "declare _FillValue"
    )


def _fill_value_for_array_write(array: Any) -> Any:
    """Return the value that marks one of ``array``'s slots as deleted.

    CF-encoded numeric time arrays (``units`` containing ``since``) get a NaT
    sentinel, because their zarr ``fill_value`` (``0`` for integers) decodes
    to the reference epoch, a valid date. Every other array keeps the
    precedence zarr ``fill_value``, then NaN/NaT by dtype kind, then zero.

    Raises:
        ValueError: ``array`` is a CF time array whose dtype cannot carry
            NaT and which declares no integer ``_FillValue``.
    """
    import numpy as np

    fill_value = array.fill_value
    dtype = np.dtype(array.dtype)
    attrs = getattr(array, "attrs", {}) or {}
    units = attrs.get("units")
    if units is not None and "since" in str(units) and dtype.kind in ("i", "u", "f"):
        array_path = str(getattr(array, "path", "") or getattr(array, "name", ""))
        return _cf_time_nat_sentinel(array_path, dtype, str(units), attrs.get("_FillValue"))
    if fill_value is not None:
        return fill_value
    if dtype.kind in ("f", "c"):
        return np.nan
    if dtype.kind == "M":
        return np.datetime64("NaT")
    if dtype.kind == "m":
        return np.timedelta64("NaT")
    return dtype.type(0)


def _time_selection(ndim: int, time_dim: int, start: int, end: int) -> tuple[Any, ...]:
    selection: list[Any] = [slice(None)] * ndim
    selection[time_dim] = slice(start, end + 1)
    return tuple(selection)


def _zarr_group_has_array(
    *,
    store_uri: str,
    group: str,
    array_name: str,
    storage_config: StorageConfig,
) -> bool:
    import zarr

    from firecube.core.filesystem.store_factory import create_zarr_store

    handle = create_zarr_store(uri=store_uri, storage_config=storage_config, mode="r")
    try:
        root = cast(
            Any,
            zarr.open_group(
                **handle.zarr_kwargs(), mode="r", zarr_format=3, use_consolidated=False
            ),
        )
    except FileNotFoundError:
        return False
    try:
        zarr_group = cast(Any, root if not group.strip("/") else root[group.strip("/")])
    except KeyError:
        return False
    try:
        zarr_group[array_name]
    except KeyError:
        return False
    return True


@dataclass(frozen=True, slots=True)
class _RegionFillTarget:
    """A time-indexed array the region-fill pre-flight cleared for writing."""

    relative_path: str
    array: Any
    fill_value: Any
    selections: tuple[tuple[Any, ...], ...]


def _open_listed_array(zarr_group: Any, relative_path: str, display_path: str) -> Any:
    import zarr

    try:
        node = zarr_group[relative_path]
    except KeyError:
        raise ValueError(
            f"Array {display_path} is listed in the span but does not exist in the store"
        ) from None
    if not isinstance(node, zarr.Array):
        raise ValueError(f"{display_path} is listed in the span as an array but is a group")
    return node


def _require_ranges_within(
    display_path: str, length: int, ranges: Sequence[tuple[int, int]], time_dim_name: str
) -> None:
    for start, end in ranges:
        if end >= length:
            raise ValueError(
                f"Array {display_path}: span range [{start}, {end}] exceeds its "
                f"'{time_dim_name}' length {length}; refusing a partial fill"
            )


def delete_span_via_region_nan_fill(
    store_uri: str,
    group: str,
    time_indices: list[int],
    storage_config: StorageConfig,
    state_array_name: str = _STATE_ARRAY_NAME,
    *,
    array_paths: Sequence[str] | None = None,
    time_dim_name: str = "timestamp",
    update_state: bool = True,
    state_deleted_value: int = STATE_DELETED_BY_FIRECUBE,
) -> list[str]:
    """Fill the given time indices in place, without deleting chunk keys.

    A pre-flight pass resolves every listed array before anything is
    written: the array must exist, its time axis is found by
    ``time_dim_name``, its deletion fill value must be representable, and
    every range must lie inside its time length (zarr silently clips an
    out-of-range slice). Arrays whose known dimension names lack
    ``time_dim_name`` (static arrays such as ``lat_bnds``, or 0-d scalars)
    are skipped and returned; an array with ndim >= 1 and no dimension names
    is refused, since it cannot be shown to be static. With
    ``update_state``, the state array must be 1-D on ``time_dim_name``,
    cover every range, and hold ``state_deleted_value`` in its dtype. Only then are the fills and the state written.

    A pre-flight failure mutates nothing. This is not a rollback: a storage
    failure during the write pass can still leave some arrays filled and
    the state unchanged.

    Args:
        store_uri: URI of the Zarr store.
        group: Group holding the arrays and the state array.
        time_indices: Time indices to fill.
        storage_config: Storage settings used to open the store.
        state_array_name: Name of the timestamp-state array in ``group``.
        array_paths: Arrays to fill, group-relative or group-prefixed;
            ``None`` means every array in ``group`` except the state array.
        time_dim_name: Name of the time dimension.
        update_state: Whether to mark the indices deleted in the state array.
        state_deleted_value: State value that marks an index deleted.

    Returns:
        Group-relative paths of the arrays skipped for lacking
        ``time_dim_name``.

    Raises:
        ValueError: A pre-flight check failed, including when no listed
            array carries ``time_dim_name``.
        RuntimeError: A filled region did not read back as the fill value.
    """
    import numpy as np
    import zarr

    from firecube.core.filesystem.store_factory import create_zarr_store
    from firecube.core.zarr._fill import _array_is_all_fill

    ranges = _contiguous_ranges(time_indices)
    if not ranges:
        return []

    handle = create_zarr_store(uri=store_uri, storage_config=storage_config, mode="r+")
    root = cast(
        Any,
        zarr.open_group(**handle.zarr_kwargs(), mode="r+", zarr_format=3, use_consolidated=False),
    )
    zarr_group = cast(Any, root if not group.strip("/") else root[group.strip("/")])

    if array_paths is None:
        relative_array_paths = [
            str(name) for name in zarr_group.array_keys() if str(name) != state_array_name
        ]
    else:
        relative_array_paths = []
        for array_path in array_paths:
            relative = _array_path_relative_to_group(str(array_path), group)
            if relative and relative != state_array_name and relative not in relative_array_paths:
                relative_array_paths.append(relative)

    group_prefix = f"{group.strip('/')}/" if group.strip("/") else ""
    targets: list[_RegionFillTarget] = []
    skipped: list[str] = []
    for relative_array_path in relative_array_paths:
        display_path = f"{group_prefix}{relative_array_path}"
        array = _open_listed_array(zarr_group, relative_array_path, display_path)
        dim_names = _array_dimension_names(array)
        if dim_names is None:
            raise ValueError(_unknown_dimensions_message(display_path, int(array.ndim)))
        if time_dim_name not in dim_names:
            skipped.append(relative_array_path)
            continue
        time_dim = resolve_time_dim_index(dim_names, time_dim_name)
        _require_ranges_within(display_path, int(array.shape[time_dim]), ranges, time_dim_name)
        targets.append(
            _RegionFillTarget(
                relative_path=relative_array_path,
                array=array,
                fill_value=_fill_value_for_array_write(array),
                selections=tuple(
                    _time_selection(array.ndim, time_dim, start, end) for start, end in ranges
                ),
            )
        )

    if not targets:
        raise ValueError(_no_time_indexed_array_message(time_dim_name, relative_array_paths))

    state_array: Any = None
    state_value: Any = None
    if update_state:
        state_display_path = f"{group_prefix}{state_array_name}"
        state_array = _open_listed_array(zarr_group, state_array_name, state_display_path)
        state_dims = _array_dimension_names(state_array)
        if state_array.ndim != 1 or state_dims != [time_dim_name]:
            raise ValueError(
                f"State array {state_display_path} has dimensions {state_dims!r}; expected "
                f"exactly ['{time_dim_name}']"
            )
        state_dtype = np.dtype(state_array.dtype)
        try:
            state_value = state_dtype.type(int(state_deleted_value))
        except OverflowError as exc:
            raise ValueError(
                f"State value {state_deleted_value} does not fit state array "
                f"{state_display_path} dtype {state_dtype}"
            ) from exc
        _require_ranges_within(state_display_path, int(state_array.shape[0]), ranges, time_dim_name)

    for target in targets:
        for selection in target.selections:
            target.array[selection] = target.fill_value
            filled = np.asarray(target.array[selection])
            if not _array_is_all_fill(filled, target.fill_value):
                raise RuntimeError(
                    f"Failed to fill {group_prefix}{target.relative_path} with its declared "
                    "fill value"
                )

    if state_array is not None:
        for start, end in ranges:
            state_array[start : end + 1] = state_value
    return skipped


def _local_base_from_storage_config(storage_config: StorageConfig | None) -> Path | None:
    """Derive the local base directory used to materialise chunk paths.

    Mirrors the legacy dict-based contract: when the CLI passes a typed
    StorageConfig, the local base is the ``target_path`` exposed on the
    bridge-extended config (matches ``identity_from_storage_config(...).product_uri.path``).
    """
    if storage_config is None:
        return None
    identity = identity_from_storage_config(storage_config)
    if identity is None:
        return None
    product_uri = identity.product_uri
    if product_uri.protocol != "file":
        return None
    return Path(product_uri.path)


class DeletionEngine:
    """Handles deletion of chunks from storage and manifests."""

    def __init__(
        self,
        repo: ManifestRepository,
        filesystem: Any = None,
        *,
        time_dim_name: str = "timestamp",
    ):
        self.repo = repo
        self.filesystem = filesystem
        self.time_dim_name = time_dim_name
        self.log = logging.getLogger(f"{__name__}.DeletionEngine")

    def create_deletion_plan(
        self,
        pattern: str | None = None,
        product: str | None = None,
        before: datetime | None = None,
        after: datetime | None = None,
        chunk_type: str | None = None,
        status: str | None = None,
        include_metadata: bool = False,
        meta: dict[str, Any] | None = None,
        filter_fn: Callable[[ChunkInfo], bool] | None = None,
        *,
        time_overlaps: tuple[str, str] | None = None,
    ) -> DeletionPlan:
        """Create a plan for deleting chunks based on filters."""
        # Query repo
        chunks = self.repo.list_chunks(
            pattern=pattern,
            product=product,
            before=before,
            after=after,
            chunk_type=chunk_type,
            status=status,
            meta=meta,
            filter_fn=filter_fn,
            time_overlaps=time_overlaps,
        )

        if not include_metadata and chunk_type != "meta":
            chunks = [c for c in chunks if c.chunk_type != "meta"]

        total_size = sum(c.size for c in chunks)
        products = {c.product for c in chunks}
        manifests = {c.manifest_path for c in chunks}

        return DeletionPlan(
            chunks=chunks,
            total_size=total_size,
            products_affected=products,
            manifest_files=manifests,
        )

    def _acquire_maintenance_claims(
        self, *, products: Iterable[str], operation: str, holder_run_id: str | None = None
    ) -> list[tuple[str, WriteDomain, Any]]:
        """Acquire the maintenance claim of every product, refusing while any claim exists.

        Lists each product's claims first, then acquires, then checks the exclusive
        maintenance claim under each acquired claim. With `holder_run_id`, each product's
        exclusive maintenance claim must exist and be owned by it, and is the one claim the
        listing lets through. Every refusal releases the claims this call acquired before
        it raises.

        Returns:
            `(product, domain, handle)` for each acquired claim; pass it to
            `_clear_maintenance_claims`.

        Raises:
            ManifestError: A product has a claim, an acquisition conflicts, the product is
                under exclusive maintenance, or `holder_run_id` does not own its exclusive
                maintenance claim.
        """
        product_list = sorted({product for product in products if product})
        if not product_list:
            return []

        # Function-scope import: the manager module imports this one.
        from firecube.core.controlplane.manager import (
            check_exclusive_maintenance,
            exclusive_maintenance_holder_refusal,
            holder_absent_message,
            is_exclusive_maintenance_holder,
            maintenance_refusal_message,
        )

        owner_id = f"maintenance:{operation}:{uuid.uuid4()}"
        acquired: list[tuple[str, WriteDomain, Any]] = []
        acquiring = product_list[0]
        try:
            for product in product_list:
                active_claims = self.repo.list_claims(product=product)
                blocking_claims = [
                    claim
                    for claim in active_claims
                    if not is_exclusive_maintenance_holder(
                        claim, product=product, holder_run_id=holder_run_id
                    )
                ]
                if blocking_claims:
                    details = ", ".join(
                        f"{claim.domain} (owner={claim.owner_id})" for claim in blocking_claims
                    )
                    raise ManifestError(
                        maintenance_refusal_message(
                            repo=self.repo,
                            product=product,
                            operation=operation,
                            detail=f"active write claim(s) exist: {details}",
                            claims=blocking_claims,
                        )
                    )
                if holder_run_id is not None and not active_claims:
                    raise ManifestError(
                        holder_absent_message(
                            product=product, operation=operation, holder_run_id=holder_run_id
                        )
                    )

            for acquiring in product_list:
                domain = WriteDomain(product=acquiring, category="maintenance", name=operation)
                handle = self.repo.acquire_claim(
                    product=acquiring, domain=domain, owner_id=owner_id
                )
                acquired.append((acquiring, domain, handle))
                # An exclusive claim taken, released or changed between the listing and the
                # acquire refuses here.
                if holder_run_id is None:
                    check_exclusive_maintenance(
                        repo=self.repo, product=acquiring, owner_id=owner_id
                    )
                    continue
                refusal = exclusive_maintenance_holder_refusal(
                    repo=self.repo,
                    product=acquiring,
                    operation=operation,
                    holder_run_id=holder_run_id,
                )
                if refusal is not None:
                    raise ManifestError(refusal)
        except ProductUnderMaintenanceError as exc:
            self._clear_maintenance_claims(acquired)
            raise ManifestError(exc.message or str(exc)) from exc
        except ClaimConflictError as exc:
            self._clear_maintenance_claims(acquired)
            detail = _error_text(exc) or "write claim acquisition failed"
            blocking: list[ClaimInfo] = []
            with suppress(Exception):
                blocking = self.repo.list_claims(product=acquiring)
            raise ManifestError(
                maintenance_refusal_message(
                    repo=self.repo,
                    product=acquiring,
                    operation=operation,
                    detail=detail,
                    claims=blocking,
                )
            ) from exc
        except BaseException:
            # An interrupt releases too: the callers' cleanup starts only after this returns.
            self._clear_maintenance_claims(acquired)
            raise
        return acquired

    def _clear_maintenance_claims(self, claims: Iterable[tuple[str, WriteDomain, Any]]) -> None:
        """Release each acquired claim, newest first; releasing stops its heartbeat too.

        A failed release is logged; deleting the file alone lets the heartbeat write it back.
        """
        for product, domain, handle in reversed(list(claims)):
            try:
                handle.release()
            except Exception:
                self.log.exception(
                    "Failed to release maintenance claim %s of %s", domain.identifier, product
                )

    def _record_maintenance_started(
        self,
        *,
        products: list[str],
        run_id: str,
        op: str,
        scope_for: Callable[[str], dict[str, Any]],
    ) -> list[str]:
        started: list[str] = []
        for product in products:
            try:
                self.repo.record_maintenance_started(
                    product=product,
                    run_id=run_id,
                    op=op,
                    scope_meta=scope_for(product),
                )
            except Exception:
                self.log.exception(
                    "Failed to record maintenance_started for product=%s run=%s",
                    product,
                    run_id,
                )
                continue
            started.append(product)
        return started

    def _record_maintenance_completed(
        self,
        *,
        products: list[str],
        run_id: str,
        op: str,
        scope_for: Callable[[str], dict[str, Any]],
    ) -> None:
        for product in products:
            try:
                self.repo.record_maintenance_completed(
                    product=product,
                    run_id=run_id,
                    op=op,
                    scope_meta=scope_for(product),
                )
            except Exception:
                self.log.exception(
                    "Failed to record maintenance_completed for product=%s run=%s",
                    product,
                    run_id,
                )

    def _record_maintenance_failed(
        self,
        *,
        products: list[str],
        run_id: str,
        op: str,
        scope_for: Callable[[str], dict[str, Any]],
        error: str,
    ) -> None:
        for product in products:
            try:
                self.repo.record_maintenance_failed(
                    product=product,
                    run_id=run_id,
                    op=op,
                    scope_meta=scope_for(product),
                    error=error,
                )
            except Exception:
                self.log.exception(
                    "Failed to record maintenance_failed for product=%s run=%s",
                    product,
                    run_id,
                )

    def execute_deletion(
        self,
        plan: DeletionPlan,
        delete_storage: bool = True,
        delete_manifest: bool = True,
        storage_config: StorageConfig | None = None,
        dry_run: bool = False,
        *,
        holder_run_id: str | None = None,
    ) -> dict[str, Any]:
        """Execute a deletion plan.

        Storage deletion covers only records whose key names a storage object.
        Span, run and schema records are counted in ``records_without_storage``
        and never passed to storage deletion.

        Pass ``holder_run_id`` to delete as the owner of each affected product's exclusive
        maintenance claim. The deletion is refused unless that run owns it. The maintenance
        record names the holder.
        """
        storage_chunks = [c for c in plan.chunks if c.chunk_type not in RECORD_ONLY_TYPES]
        records_without_storage = plan.count - len(storage_chunks)
        if dry_run:
            return {
                "dry_run": True,
                "would_delete_chunks": plan.count,
                "would_delete_size_bytes": plan.total_size,
                "products_affected": list(plan.products_affected),
                "records_without_storage": records_without_storage,
            }

        affected_products = sorted(
            {p for p in plan.products_affected if p}
            or {chunk.product for chunk in plan.chunks if chunk.product}
        )
        claims = self._acquire_maintenance_claims(
            products=affected_products,
            operation="delete_chunks",
            holder_run_id=holder_run_id,
        )

        run_id = f"maintenance-delete-{uuid.uuid4().hex}"

        def _scope_for(product: str) -> dict[str, Any]:
            product_chunks = [c for c in plan.chunks if c.product == product]
            scope: dict[str, Any] = {
                "chunks_count": len(product_chunks),
                "size_bytes": sum(int(c.size or 0) for c in product_chunks),
                "delete_storage": bool(delete_storage),
                "delete_manifest": bool(delete_manifest),
                "products_affected": affected_products,
            }
            if holder_run_id is not None:
                scope["holder_run_id"] = holder_run_id
            return scope

        started_products: list[str] = []
        try:
            started_products = self._record_maintenance_started(
                products=affected_products,
                run_id=run_id,
                op=MAINTENANCE_OP_DELETE,
                scope_for=_scope_for,
            )

            deleted_chunks = 0
            deleted_size = 0
            storage_errors = []
            manifest_errors = []

            if delete_storage and storage_chunks and (storage_config or self.filesystem):
                storage_errors = self._delete_from_storage(storage_chunks, storage_config)
                if not storage_errors:
                    pass

            if delete_manifest:
                by_manifest = {}
                for c in plan.chunks:
                    by_manifest.setdefault(c.manifest_path, []).append(c)

                for m_uri, chunks in by_manifest.items():
                    try:
                        count, size = self.repo.remove_from_manifest(m_uri, chunks)
                        deleted_chunks += count
                        deleted_size += size
                    except Exception as e:
                        msg = f"Failed to update manifest {m_uri}: {e}"
                        self.log.error(msg)
                        manifest_errors.append(msg)

            self._record_maintenance_completed(
                products=started_products,
                run_id=run_id,
                op=MAINTENANCE_OP_DELETE,
                scope_for=_scope_for,
            )

            return {
                "deleted_chunks": deleted_chunks,
                "deleted_size_bytes": deleted_size,
                "storage_errors": storage_errors,
                "manifest_errors": manifest_errors,
                "records_without_storage": records_without_storage,
            }
        except Exception as exc:
            self._record_maintenance_failed(
                products=started_products,
                run_id=run_id,
                op=MAINTENANCE_OP_DELETE,
                scope_for=_scope_for,
                error=str(exc),
            )
            raise
        finally:
            self._clear_maintenance_claims(claims)

    def _delete_from_storage(
        self, chunks: list[ChunkInfo], storage_config: StorageConfig | None
    ) -> list[str]:
        """Dispatch to S3 or Local deletion."""
        if self.filesystem is not None:
            return self._delete_with_filesystem(chunks, self.filesystem)

        from firecube.core.uris import infer_target_protocol

        proto = infer_target_protocol(self.repo.base_uri or "")
        storage_type = storage_config.storage_type if storage_config is not None else None
        if proto == "s3" or storage_type == "s3":
            return self._delete_from_s3(chunks)
        else:
            return self._delete_from_local(chunks, storage_config)

    def _delete_with_filesystem(self, chunks: list[ChunkInfo], filesystem: Any) -> list[str]:
        """Delete the one object each chunk record names, its key taken literally.

        An absent object is not an error. A failed or impossible delete is returned as an
        error string.
        """
        errors: list[str] = []
        if not self.repo.base_uri:
            return ["Missing base URI; cannot delete without a base URI"]
        base_uri = StorageUri.parse(self.repo.base_uri)
        try:
            # The protocol's rm expands glob characters, which are legal in a product
            # name or key, and so can delete objects of another product.
            delete_object = _literal_delete(filesystem)
        except StorageError:
            return [
                f"Cannot delete chunk objects through {type(filesystem).__name__}: "
                "it offers no single-object delete"
            ]
        for chunk in chunks:
            try:
                uri = _chunk_object_uri(base_uri, chunk)
            except ValueError as exc:
                errors.append(str(exc))
                continue
            try:
                if filesystem.exists(uri):
                    delete_object(uri)
            except FileNotFoundError:
                continue
            except Exception as e:
                errors.append(f"Failed to delete {uri.to_str()}: {e}")
        return errors

    def _delete_from_s3(self, chunks: list[ChunkInfo]) -> list[str]:
        """Delete chunks from S3 storage."""
        errors: list[str] = []

        if not self.repo.base_uri:
            return ["Missing base URI; cannot delete from S3 without a base URI"]

        base_uri = self.repo.base_uri.rstrip("/")
        fs, base_path = self.repo._get_fs(base_uri)
        base_path = str(base_path).rstrip("/")

        product_base = StorageUri.parse(self.repo.base_uri)
        s3_paths: list[str] = []
        for chunk in chunks:
            try:
                _chunk_object_uri(product_base, chunk)
            except ValueError as exc:
                errors.append(str(exc))
                continue
            p = f"{base_path}/{chunk.product.strip('/')}/{chunk.key.lstrip('/')}".replace("//", "/")
            s3_paths.append(p)

        batch_size = 1000  # S3 delete limit
        for i in range(0, len(s3_paths), batch_size):
            batch_paths = s3_paths[i : i + batch_size]
            try:
                # TODO(uri-refactor): S3 deletion still operates on raw str paths;
                # migrate to StorageUri once a base-relative URI builder lands.
                existing_paths = [path for path in batch_paths if fs.exists(path)]  # pyright: ignore[reportArgumentType]
                if existing_paths:
                    fs.rm(existing_paths, recursive=False)  # pyright: ignore[reportArgumentType]
                    self.log.info("Deleted %s files from S3", len(existing_paths))
            except Exception as e:
                error_msg = f"Failed to delete S3 batch: {e}"
                self.log.error(error_msg)
                errors.append(error_msg)

        return errors

    def _delete_from_local(
        self, chunks: list[ChunkInfo], storage_config: StorageConfig | None
    ) -> list[str]:
        errors = []
        base = _local_base_from_storage_config(storage_config)
        if base is None:
            return [
                "Missing local target base; cannot delete local storage without a file:// product URI"
            ]
        local_base = StorageUri.from_local_path(base)
        for c in chunks:
            try:
                _chunk_object_uri(local_base, c)
            except ValueError as exc:
                errors.append(str(exc))
                continue
            try:
                p = base / c.product / c.key
                if p.exists():
                    if p.is_file():
                        p.unlink()
                    else:
                        import shutil

                        shutil.rmtree(p)
            except Exception as e:
                errors.append(str(e))
        return errors

    def _measure_span_alignment(
        self,
        *,
        store_uri: str,
        arrays: list[str],
        time_ranges: list[Any],
        expected_time_dim_name: str,
        span_key: str,
        wal_aligned: bool,
    ) -> tuple[bool, dict[str, tuple[list[str], list[int], list[int], int]], list[str], list[str]]:
        """Measure alignment from the STORED chunk grid per data array.

        The WAL-recorded ``aligned`` flag can be wrong: it may have been
        computed against the REQUESTED chunk size rather than the STORED
        chunk size. To keep deletion decisions truthful,
        this helper opens each array's chunk grid metadata and checks
        whether every span time range lands on chunk boundaries along the
        resolved time dimension. Grids are cached so the caller can reuse
        them without re-reading zarr.json inside the deletion loop.
        Arrays whose known dimension names lack ``expected_time_dim_name``
        (static arrays, including 0-d scalars) get no grid and no alignment
        vote. An array with ndim >= 1 and no dimension names raises
        ``ValueError``: it cannot be shown to be static.

        Returns:
            ``(measured_aligned, per_array_grids, read_errors,
            skipped_arrays)`` where ``per_array_grids`` maps ``array_path ->
            (dim_names, shape, chunk_shape, time_dim_index)`` and
            ``skipped_arrays`` lists the arrays without the time dimension.
            If no grid could be read, falls back to the WAL flag rather than
            silently deleting.
        """
        per_array_grids: dict[str, tuple[list[str], list[int], list[int], int]] = {}
        read_errors: list[str] = []
        skipped_arrays: list[str] = []
        array_flags: list[bool] = []

        normalised: list[tuple[int, int]] = []
        for pair in time_ranges:
            if not (isinstance(pair, (list, tuple)) and len(pair) == 2):
                continue
            try:
                s_i, e_i = int(pair[0]), int(pair[1])
            except Exception:
                continue
            if e_i < s_i:
                continue
            normalised.append((s_i, e_i))

        for array_path in arrays:
            try:
                known_dim_names, shape, chunk_shape = _read_array_grid(
                    store_uri, array_path, self.repo.storage_config
                )
            except Exception as exc:
                read_errors.append(f"Failed to read zarr.json for {array_path}: {exc}")
                continue

            # Raised, not collected: an unknown axis must abort before any
            # chunk of this span is removed.
            if known_dim_names is None:
                raise ValueError(_unknown_dimensions_message(array_path, len(shape)))
            dim_names = known_dim_names
            if expected_time_dim_name not in dim_names:
                skipped_arrays.append(array_path)
                continue

            if not shape or not chunk_shape:
                continue

            time_dim = resolve_time_dim_index(dim_names, expected_time_dim_name)
            per_array_grids[array_path] = (dim_names, shape, chunk_shape, time_dim)

            time_chunk = int(chunk_shape[time_dim] or 0)
            time_length = int(shape[time_dim] or 0)
            if time_chunk <= 0 or time_length <= 0 or not normalised:
                continue

            array_aligned = True
            for s_i, e_i in normalised:
                lo_chunk = s_i // time_chunk
                hi_chunk = e_i // time_chunk
                chunk_span_lo = lo_chunk * time_chunk
                chunk_span_hi = min(time_length - 1, (hi_chunk + 1) * time_chunk - 1)
                if chunk_span_lo != s_i or chunk_span_hi != e_i:
                    array_aligned = False
                    break
            array_flags.append(array_aligned)

        if array_flags:
            measured_aligned = all(array_flags)
        else:
            # No grid could be measured. Falling back to the WAL flag is
            # safer than silently allowing deletion: if the caller passes
            # ``force``, they can still override the guard.
            measured_aligned = wal_aligned

        self.log.debug(
            "span %s: WAL alignment=%s, measured alignment=%s",
            span_key,
            wal_aligned,
            measured_aligned,
        )
        return measured_aligned, per_array_grids, read_errors, skipped_arrays

    @staticmethod
    def _render_collateral_entry(span: ChunkInfo) -> str:
        meta = span.meta if isinstance(span.meta, dict) else {}
        t_min = meta.get("time_min")
        t_max = meta.get("time_max")
        if t_min and t_max:
            return f"{span.key} ({t_min}..{t_max})"
        return span.key

    def _find_collateral_spans(
        self,
        *,
        arrays: list[str],
        time_ranges: list[Any],
        per_array_grids: dict[str, tuple[list[str], list[int], list[int], int]],
        product: str,
        target_span_keys: set[str],
        has_array: Callable[[str], bool],
        all_spans: Sequence[ChunkInfo] | None = None,
    ) -> list[ChunkInfo]:
        """Return OTHER active spans whose slot data lives in the physical
        chunks about to be deleted.

        For each array in the target span, the requested time-index ranges are
        expanded to full physical chunk boundaries using the stored
        ``chunk_shape`` on the time axis. Any other active span for the same
        product that (a) writes to the same array AND (b) has a
        ``time_index_ranges`` entry that intersects the expanded physical
        range is treated as collateral: force-deleting the target span will
        also destroy its data because they share the same physical chunk keys
        on disk. ``arrays`` holds store paths; each other span's names are
        resolved under its own meta group before the comparison.
        """
        per_array_physical: dict[str, list[tuple[int, int]]] = {}
        for array_path in arrays:
            grid = per_array_grids.get(array_path)
            if grid is None:
                continue
            _dim_names, shape, chunk_shape, time_dim = grid
            if not shape or not chunk_shape:
                continue
            time_chunk = int(chunk_shape[time_dim] or 0)
            time_length = int(shape[time_dim] or 0)
            if time_chunk <= 0 or time_length <= 0:
                continue

            physical_ranges: list[tuple[int, int]] = []
            for pair in time_ranges:
                if not (isinstance(pair, (list, tuple)) and len(pair) == 2):
                    continue
                try:
                    s_i, e_i = int(pair[0]), int(pair[1])
                except Exception:
                    continue
                if e_i < s_i:
                    continue
                lo_chunk = s_i // time_chunk
                hi_chunk = e_i // time_chunk
                phys_lo = lo_chunk * time_chunk
                phys_hi = min(time_length - 1, (hi_chunk + 1) * time_chunk - 1)
                physical_ranges.append((phys_lo, phys_hi))
            if physical_ranges:
                per_array_physical[array_path] = physical_ranges

        if not per_array_physical:
            return []

        if all_spans is None:
            all_spans = self.repo.list_chunks(product=product, chunk_type="span")

        collateral: list[ChunkInfo] = []
        for other in all_spans:
            if other.key in target_span_keys:
                continue
            if not other.is_active:
                continue
            payload = other.record if isinstance(other.record, dict) else {}
            other_spec = payload.get("span") if isinstance(payload, dict) else None
            if not isinstance(other_spec, dict):
                continue
            other_arrays = other_spec.get("arrays")
            other_ranges = other_spec.get("time_index_ranges")
            if not isinstance(other_arrays, list) or not isinstance(other_ranges, list):
                continue
            other_meta = other.meta if isinstance(other.meta, dict) else {}
            other_paths = set(
                _span_array_store_paths(other_arrays, str(other_meta.get("group") or ""), has_array)
            )

            hit = False
            for array_path, physical_ranges in per_array_physical.items():
                if array_path not in other_paths:
                    continue
                for pair in other_ranges:
                    if not (isinstance(pair, (list, tuple)) and len(pair) == 2):
                        continue
                    try:
                        o_start, o_end = int(pair[0]), int(pair[1])
                    except Exception:
                        continue
                    if o_end < o_start:
                        continue
                    for phys_lo, phys_hi in physical_ranges:
                        if o_start <= phys_hi and o_end >= phys_lo:
                            hit = True
                            break
                    if hit:
                        break
                if hit:
                    break
            if hit:
                collateral.append(other)

        return collateral

    def _delete_span_by_region_fill(
        self,
        *,
        span: ChunkInfo,
        group_name: str,
        store_uri: str,
        fs: Any,
        spec: dict[str, Any],
        dry_run: bool,
        force: bool,
        collateral_spans: Sequence[ChunkInfo],
        arrays: list[Any],
        time_indices: list[int],
        expected_time_dim_name: str,
        update_state: bool,
        update_manifest: bool,
    ) -> dict[str, Any]:
        del fs, force, collateral_spans
        errors: list[str] = []
        warnings: list[str] = []
        replacement_meta_updates_by_key: dict[str, dict[str, Any]] = {}

        state_array_name = _span_state_array_name(spec=spec, group=group_name)
        if not dry_run:
            try:
                skipped = delete_span_via_region_nan_fill(
                    store_uri=store_uri,
                    group=group_name,
                    time_indices=time_indices,
                    storage_config=self.repo.storage_config,
                    state_array_name=state_array_name,
                    array_paths=[str(array_path) for array_path in arrays],
                    time_dim_name=expected_time_dim_name,
                    update_state=update_state,
                    state_deleted_value=int(
                        spec.get("state_deleted_value", STATE_DELETED_BY_FIRECUBE)
                        or STATE_DELETED_BY_FIRECUBE
                    ),
                )
            except Exception as e:
                errors.append(
                    Message(
                        "Failed to region-fill span {span}: {error}",
                        span=span.key,
                        error=_error_text(e),
                    )
                )
                return {"deleted_keys": 0, "deleted_spans": 0, "errors": errors}
            if skipped:
                warnings.append(_skipped_arrays_warning(span.key, expected_time_dim_name, skipped))

        replaced_span_keys: list[str] = []
        if update_manifest:
            replaced_span_keys.append(span.key)
            replacement_meta_updates_by_key[span.key] = {"write_strategy": "region_nan_fill"}
        return {
            "deleted_keys": 0,
            "deleted_spans": 1,
            "region_filled_spans": 1,
            "errors": errors,
            "warnings": warnings,
            "replaced_span_keys": replaced_span_keys,
            "replacement_meta_updates_by_key": replacement_meta_updates_by_key,
        }

    def _delete_span_by_chunk_keys(
        self,
        *,
        span: ChunkInfo,
        group_name: str,
        store_uri: str,
        fs: Any,
        spec: dict[str, Any],
        dry_run: bool,
        force: bool,
        collateral_spans: Sequence[ChunkInfo],
        product: str,
        base_path: str,
        arrays: list[str],
        time_ranges: list[Any],
        expected_time_dim_name: str,
        update_state: bool,
        update_manifest: bool,
        yes_i_really_mean_it: bool,
        all_spans: Sequence[ChunkInfo],
        target_span_keys: set[str],
    ) -> dict[str, Any]:
        from firecube.core.zarr.validation import read_chunk_grid

        has_array = _store_array_probe(store_uri, self.repo.storage_config)
        try:
            store_paths = _span_array_store_paths(arrays, group_name, has_array)
        except ValueError as exc:
            return {
                "deleted_keys": 0,
                "deleted_spans": 0,
                "errors": [f"Span {span.key}: {exc}"],
                "warnings": [],
            }
        # A marked time coordinate is engine-managed: pods never refill its slots,
        # so the span's deletion neither removes nor measures its chunks.
        coord_path = _span_array_store_path(expected_time_dim_name, group_name, has_array)
        if coord_path in store_paths and _is_managed_time_coordinate(
            store_uri, coord_path, self.repo.storage_config
        ):
            store_paths.remove(coord_path)
            self.log.info(
                "span %s: keeping engine-managed time coordinate %s", span.key, coord_path
            )
        deleted_keys = 0
        already_absent_keys = 0
        span_paths_added = 0
        errors: list[str] = []
        warnings: list[str] = []
        collateral_span_keys: list[str] = []
        pending_paths: list[str] = []
        pending_limit = 1000
        wal_aligned = bool(spec.get("aligned", True))
        measured_aligned, per_array_grids, grid_read_errors, skipped_arrays = (
            self._measure_span_alignment(
                store_uri=store_uri,
                arrays=store_paths,
                time_ranges=time_ranges,
                expected_time_dim_name=expected_time_dim_name,
                span_key=span.key,
                wal_aligned=wal_aligned,
            )
        )
        errors.extend(grid_read_errors)
        if skipped_arrays:
            warnings.append(
                _skipped_arrays_warning(span.key, expected_time_dim_name, skipped_arrays)
            )
            if not per_array_grids:
                # Raises like the unresolvable-axis error this replaces, so a
                # wrong time-dim name still aborts delete_spans loudly.
                raise ValueError(
                    Message(
                        "Span {span}: {refusal}",
                        span=span.key,
                        refusal=_no_time_indexed_array_message(
                            expected_time_dim_name, [str(a) for a in arrays]
                        ),
                    )
                )

        if not measured_aligned and not force:
            errors.append(
                Message(
                    "Span {span} is not time-chunk aligned; rerun with {force}",
                    span=span.key,
                    force=Param("force", True),
                )
            )
            return {"deleted_keys": 0, "deleted_spans": 0, "errors": errors, "warnings": warnings}

        if not measured_aligned and force:
            try:
                collateral_spans = self._find_collateral_spans(
                    arrays=store_paths,
                    time_ranges=time_ranges,
                    per_array_grids=per_array_grids,
                    product=product,
                    target_span_keys=target_span_keys,
                    has_array=has_array,
                    all_spans=all_spans,
                )
            except ValueError as exc:
                # Another span's record is ambiguous: its share of the chunks is unknown.
                errors.append(f"Span {span.key}: collateral check refused: {exc}")
                return {
                    "deleted_keys": 0,
                    "deleted_spans": 0,
                    "errors": errors,
                    "warnings": warnings,
                }

        def _flush_pending() -> None:
            nonlocal pending_paths, deleted_keys, already_absent_keys
            if not pending_paths:
                return
            for path in pending_paths:
                uri = StorageUri.parse(path) if "://" in path else StorageUri.from_local_path(path)
                if dry_run:
                    # A preview counts only the keys the store holds, as the deletion would.
                    try:
                        present = fs.exists(uri)
                    except Exception as e:
                        errors.append(f"Failed to check {path}: {e}")
                        continue
                    if present:
                        deleted_keys += 1
                    else:
                        already_absent_keys += 1
                    continue
                try:
                    fs.rm(uri, recursive=False)
                    deleted_keys += 1
                except FileNotFoundError:
                    already_absent_keys += 1
                    self.log.warning(
                        "chunk key already absent (fill-only or already removed); "
                        "not counted as deleted: %s",
                        path,
                    )
                except Exception as e:
                    errors.append(f"Failed to delete {path}: {e}")
            pending_paths = []

        if not measured_aligned and force and collateral_spans:
            collateral_summary = ", ".join(
                self._render_collateral_entry(c) for c in collateral_spans
            )
            if dry_run:
                for c in collateral_spans:
                    warnings.append(
                        f"WARNING: force-deleting {span.key} will also destroy "
                        f"{self._render_collateral_entry(c)}"
                    )
                    self.log.warning(
                        "force-deleting %s will also destroy %s",
                        span.key,
                        self._render_collateral_entry(c),
                    )
            elif not yes_i_really_mean_it:
                errors.append(
                    Message(
                        "Span {span} shares physical chunks with other active spans: "
                        "{collateral}. Re-run with {confirm} to "
                        "acknowledge the collateral destruction.",
                        span=span.key,
                        collateral=collateral_summary,
                        confirm=Param("yes_i_really_mean_it", True),
                    )
                )
                return {
                    "deleted_keys": 0,
                    "deleted_spans": 0,
                    "errors": errors,
                    "warnings": warnings,
                }
            else:
                collateral_span_keys = [c.key for c in collateral_spans]

        for array_path in store_paths:
            grid = per_array_grids.get(array_path)
            if grid is None:
                continue
            _dim_names, shape, chunk_shape, time_dim = grid
            if not shape or not chunk_shape:
                continue
            time_chunk = int(chunk_shape[time_dim] or 0)
            if time_chunk <= 0:
                continue
            expected_chunks = [math.ceil(s / c) for s, c in zip(shape, chunk_shape, strict=False)]
            time_chunk_indices: set[int] = set()
            for pair in time_ranges:
                if len(pair) != 2:
                    continue
                try:
                    start_i, end_i = int(pair[0]), int(pair[1])
                except Exception:
                    continue
                if end_i < start_i:
                    continue
                time_chunk_indices.update(range(start_i // time_chunk, end_i // time_chunk + 1))
            time_chunk_indices = {
                i for i in time_chunk_indices if 0 <= i < expected_chunks[time_dim]
            }
            if not time_chunk_indices:
                continue
            other_ranges = [
                range(exp) for dim_i, exp in enumerate(expected_chunks) if dim_i != time_dim
            ]
            for t_idx in time_chunk_indices:
                for combo in itertools.product(*other_ranges):
                    other_iter = iter(combo)
                    indices = [
                        t_idx if dim_i == time_dim else next(other_iter)
                        for dim_i in range(len(expected_chunks))
                    ]
                    rel = f"{product}/{array_path.strip('/')}/c/" + "/".join(
                        str(i) for i in indices
                    )
                    pending_paths.append(f"{base_path.rstrip('/')}/{rel}")
                    span_paths_added += 1
                    if len(pending_paths) >= pending_limit:
                        _flush_pending()
        _flush_pending()

        if update_state:
            recorded_state_path = spec.get("state_array")
            state_value = spec.get("state_deleted_value", STATE_DELETED_BY_FIRECUBE)
            if recorded_state_path:
                # The recorded state path is store-relative, possibly a sibling of the group.
                state_path = str(recorded_state_path).strip("/")
                try:
                    from firecube.core.zarr.state import (
                        expand_time_index_ranges_to_chunk_boundaries,
                        update_timestamp_state,
                    )

                    effective_ranges = time_ranges
                    expand = not measured_aligned and force
                    if expand and not _zarr_group_has_array(
                        store_uri=store_uri,
                        group="",
                        array_name=state_path,
                        storage_config=self.repo.storage_config,
                    ):
                        # DirectZarr records a state array it never creates: the
                        # removed slots follow the chunk grids of the span's arrays.
                        effective_ranges = (
                            _expand_ranges_over_grids(time_ranges, per_array_grids) or time_ranges
                        )
                    elif expand:
                        try:
                            dim_names, shape, chunk_shape = read_chunk_grid(
                                store_uri, state_path, storage_config=self.repo.storage_config
                            )
                            time_dim = resolve_time_dim_index(dim_names, expected_time_dim_name)
                            time_chunk = int(chunk_shape[time_dim] or 0)
                            time_length = int(shape[time_dim] or 0)
                            if time_chunk > 0 and time_length > 0:
                                expanded = expand_time_index_ranges_to_chunk_boundaries(
                                    time_ranges, chunk_len=time_chunk, length=time_length
                                )
                                if expanded:
                                    effective_ranges = expanded
                        except (FileNotFoundError, KeyError, ValueError) as exc:
                            errors.append(
                                Message(
                                    "Span {span}: could not expand state ranges to chunk "
                                    "boundaries from {array_path}: {error}",
                                    span=span.key,
                                    array_path=state_path,
                                    error=_error_text(exc),
                                )
                            )
                    update_timestamp_state(
                        store_uri=store_uri,
                        array_path=state_path,
                        time_index_ranges=effective_ranges,
                        value=int(state_value),
                        storage_config=self.repo.storage_config,
                    )
                except Exception as e:
                    errors.append(f"Failed to update state: {e}")

        if errors or span_paths_added <= 0:
            deleted_spans = 0
            replaced_span_keys: list[str] = []
        else:
            deleted_spans = 1
            replaced_span_keys = [span.key] if update_manifest else []
        return {
            "deleted_keys": deleted_keys,
            "already_absent_keys": already_absent_keys,
            "deleted_spans": deleted_spans,
            "errors": errors,
            "warnings": warnings,
            "collateral_spans": collateral_span_keys,
            "replaced_span_keys": replaced_span_keys,
        }

    def delete_spans(
        self,
        spans: Iterable[ChunkInfo],
        *,
        dry_run: bool = False,
        force: bool = False,
        yes_i_really_mean_it: bool = False,
        update_manifest: bool = True,
        update_state: bool = True,
        time_dim_name: str | None = None,
        holder_run_id: str | None = None,
    ) -> dict[str, Any]:
        """Delete Zarr concrete chunks from spans.

        Pass ``holder_run_id`` to delete as the owner of the product's exclusive maintenance
        claim. The deletion is refused unless that run owns it. The maintenance record names
        the holder.
        """
        spans_list = list(spans)
        if not spans_list:
            return {
                "deleted_keys": 0,
                "deleted_spans": 0,
                "errors": [],
                "warnings": [],
                "collateral_spans": [],
            }

        products = {s.product for s in spans_list if s.product}
        if len(products) != 1:
            raise ManifestError(
                f"delete_spans requires spans for a single product, got {sorted(products)}"
            )

        product = next(iter(products))
        if not self.repo.base_uri:
            raise ManifestError("No base URI configured")

        claims = self._acquire_maintenance_claims(
            products=[product], operation="delete_spans", holder_run_id=holder_run_id
        )

        run_id = f"maintenance-delete-spans-{uuid.uuid4().hex}"

        def _scope_for(_product: str) -> dict[str, Any]:
            scope: dict[str, Any] = {
                "spans_count": len(spans_list),
                "force": bool(force),
                "update_manifest": bool(update_manifest),
                "update_state": bool(update_state),
            }
            if holder_run_id is not None:
                scope["holder_run_id"] = holder_run_id
            return scope

        started_products: list[str] = []
        try:
            if not dry_run:
                started_products = self._record_maintenance_started(
                    products=[product],
                    run_id=run_id,
                    op=MAINTENANCE_OP_DELETE,
                    scope_for=_scope_for,
                )
            base_uri = self.repo.base_uri.rstrip("/")
            store_uri = f"{base_uri}/{product}"
            span_time_dims = resolve_span_time_dims(
                spans_list,
                store_uri=store_uri,
                storage_config=self.repo.storage_config,
                explicit=time_dim_name,
                default=self.time_dim_name,
            )
            fs, base_uri = self.repo._get_fs(self.repo.base_uri)
            base_uri_obj: Any = base_uri
            base_path = (
                base_uri_obj.path if base_uri_obj.protocol == "file" else base_uri_obj.to_str()
            )
            counts = {"deleted_keys": 0, "deleted_spans": 0, "region_filled_spans": 0}
            errors: list[str] = []
            warnings: list[str] = []
            collateral_span_keys: list[str] = []
            replaced_span_keys: list[str] = []
            replacement_meta_updates_by_key: dict[str, dict[str, Any]] = {}
            target_span_keys = {s.key for s in spans_list}
            all_spans = self.repo.list_chunks(product=product, chunk_type="span")
            group_array_cache: dict[tuple[str, str], bool] = {}
            outcomes: list[dict[str, Any]] = []

            for span in spans_list:
                if _is_completed_region_fill(span) and not force:
                    outcomes.append(
                        {
                            "warnings": [
                                Message(
                                    "Span {span} is already replaced by an in-place fill "
                                    "(write_strategy=region_nan_fill); skipped. "
                                    "Rerun with {force} to fill it again.",
                                    span=span.key,
                                    force=Param("force", True),
                                )
                            ]
                        }
                    )
                    continue
                payload: dict[str, Any] = span.record if isinstance(span.record, dict) else {}
                raw_spec = payload.get("span")
                spec: dict[str, Any] = raw_spec if isinstance(raw_spec, dict) else {}
                expected_time_dim_name = span_time_dims[span.key]
                arrays = spec.get("arrays")
                if not isinstance(arrays, list) or not arrays:
                    outcomes.append({"errors": [f"Span {span.key} missing span.arrays"]})
                    continue
                time_ranges = spec.get("time_index_ranges")
                if not isinstance(time_ranges, list) or not time_ranges:
                    continue
                time_indices = _span_time_indices(time_ranges)
                group_name = _span_group_name(span)
                state_array_name = _span_state_array_name(spec=spec, group=group_name)
                cache_key = (group_name, state_array_name)
                if cache_key not in group_array_cache:
                    group_array_cache[cache_key] = _zarr_group_has_array(
                        store_uri=store_uri,
                        group=group_name,
                        array_name=state_array_name,
                        storage_config=self.repo.storage_config,
                    )
                common = {
                    "span": span,
                    "group_name": group_name,
                    "store_uri": store_uri,
                    "fs": fs,
                    "spec": spec,
                    "dry_run": dry_run,
                    "force": force,
                    "collateral_spans": [],
                }
                if time_indices and group_array_cache[cache_key]:
                    outcomes.append(
                        self._delete_span_by_region_fill(
                            **common,
                            arrays=arrays,
                            time_indices=time_indices,
                            expected_time_dim_name=expected_time_dim_name,
                            update_state=update_state,
                            update_manifest=update_manifest,
                        )
                    )
                    continue
                outcomes.append(
                    self._delete_span_by_chunk_keys(
                        **common,
                        product=product,
                        base_path=base_path,
                        arrays=arrays,
                        time_ranges=time_ranges,
                        expected_time_dim_name=expected_time_dim_name,
                        update_state=update_state,
                        update_manifest=update_manifest,
                        yes_i_really_mean_it=yes_i_really_mean_it,
                        all_spans=all_spans,
                        target_span_keys=target_span_keys,
                    )
                )

            collateral_replaced_keys: set[str] = set()
            for outcome in outcomes:
                errors.extend(outcome.get("errors", []))
                warnings.extend(outcome.get("warnings", []))
                for key in counts:
                    counts[key] += int(outcome.get(key, 0))
                replaced_span_keys.extend(outcome.get("replaced_span_keys", []))
                replacement_meta_updates_by_key.update(
                    outcome.get("replacement_meta_updates_by_key", {})
                )
                for key in outcome.get("collateral_spans", []):
                    if key not in collateral_replaced_keys:
                        collateral_span_keys.append(key)
                        collateral_replaced_keys.add(key)

            all_replaced_keys = list(dict.fromkeys([*replaced_span_keys, *collateral_span_keys]))

            if update_manifest and all_replaced_keys and not dry_run:
                import time

                try:
                    self.repo.mark_chunks_replaced(
                        all_replaced_keys,
                        product,
                        time.time(),
                        meta_updates_by_key=replacement_meta_updates_by_key,
                    )
                except ManifestError as exc:
                    errors.append(f"Manifest update failed: {exc}")

            if not dry_run:
                self._record_maintenance_completed(
                    products=started_products,
                    run_id=run_id,
                    op=MAINTENANCE_OP_DELETE,
                    scope_for=_scope_for,
                )

            return {
                "product": product,
                "deleted_keys": counts["deleted_keys"],
                "deleted_spans": counts["deleted_spans"],
                "region_filled_spans": counts["region_filled_spans"],
                "dry_run": dry_run,
                "errors": errors,
                "warnings": warnings,
                "collateral_spans": collateral_span_keys,
            }
        except Exception as exc:
            self._record_maintenance_failed(
                products=started_products,
                run_id=run_id,
                op=MAINTENANCE_OP_DELETE,
                scope_for=_scope_for,
                error=str(exc),
            )
            raise
        finally:
            self._clear_maintenance_claims(claims)
