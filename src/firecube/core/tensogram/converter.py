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

"""Core Zarr → Tensogram (.tgm) conversion.

The produced .tgm file is openable with:
    xr.open_dataset("archive.tgm", engine="tensogram")
"""

from __future__ import annotations

import logging
from typing import Any

import numpy as np
import xarray as xr
import zarr

from firecube.core.controlplane.manager import ChunkManager
from firecube.core.filesystem.ops import _open_fsspec_url
from firecube.core.product.identity import ProductIdentity
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.session import StorageSession
from firecube.core.storage.uri import StorageUri
from firecube.core.uris import is_remote_target, local_path_from_target, parse_target

from ._compat import require_tensogram
from .controlplane_codec import serialize_controlplane
from .metadata import (
    _collect_safe_attrs,
    extract_zarr_array_metadata,
    prepare_array_for_encoding,
    variable_to_base_entry,
    variable_to_descriptor,
)
from .schema import make_controlplane_meta, make_data_meta

_UNENCODABLE_DTYPE_KINDS = frozenset({"U", "S", "O"})
logger = logging.getLogger(__name__)


def _is_encodable_dtype(dtype) -> bool:
    import numpy as np

    return np.dtype(dtype).kind not in _UNENCODABLE_DTYPE_KINDS


def zarr_to_tgm(
    source: str,
    target: str,
    *,
    group: str | None = None,
    variables: list[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    compression: str = "zstd",
    overwrite: bool = False,
    storage_config: Any | None = None,
    session: StorageSession | None = None,
    allow_nan: bool = True,
    allow_inf: bool = True,
) -> dict[str, Any]:
    """Convert a Zarr store or group into a local Tensogram archive.

    ``source`` and ``target`` accept either a bare absolute path or a
    canonical URI (e.g. ``file:///…``, ``s3://…``).
    """
    require_tensogram("zarr_to_tgm")
    import tensogram  # pyright: ignore[reportMissingImports]

    if is_remote_target(target):
        raise ValueError(
            f"Remote target not yet supported for tgm output: {target}. "
            "Write to a local path and upload separately."
        )

    session = session or _make_archive_session(source, storage_config)
    source_uri = parse_target(source)

    target_path = local_path_from_target(target)
    if target_path.exists() and not overwrite:
        raise FileExistsError(
            f"Target file already exists: {target}. Use overwrite=True to replace it."
        )
    if target_path.exists() and overwrite:
        target_path.unlink()

    if group is not None:
        groups_to_archive: list[str | None] = [group]
    else:
        zroot = session.zarr.open_group(source_uri, mode="r")
        groups_to_archive = [
            name for name, member in zroot.members() if isinstance(member, zarr.Group)
        ]
        if not groups_to_archive:
            groups_to_archive = [None]

    target_path.parent.mkdir(parents=True, exist_ok=True)
    archived_groups: list[str] = []
    archived_variables: set[str] = set()
    archived_coordinates: set[str] = set()
    skipped_vars: set[str] = set()
    time_ranges: dict[str, dict[str, str]] = {}

    with tensogram.TensogramFile.create(str(target_path)) as tensogram_file:  # pyright: ignore[reportAttributeAccessIssue]
        for group_name in groups_to_archive:
            ds = _open_group_dataset(
                session,
                source_uri,
                group=group_name,
            )
            try:
                if start_date is not None or end_date is not None:
                    time_dim = _find_time_dim(ds)
                    if time_dim is None:
                        raise ValueError(
                            f"Cannot apply time filter (start_date={start_date!r}, end_date={end_date!r}): "
                            "dataset has no time dimension. "
                            f"Available dimensions: {list(ds.dims)}"
                        )

                    time_slice = slice(start_date, end_date)
                    ds = ds.sel({time_dim: time_slice})

                if variables is not None:
                    missing = [name for name in variables if name not in ds.data_vars]
                    if missing:
                        raise ValueError(
                            f"Requested variables not found in dataset: {missing}. "
                            f"Available: {list(ds.data_vars)}"
                        )
                    ds = ds[variables]

                coord_names = [str(name) for name in ds.coords]
                zarr_meta = extract_zarr_array_metadata(
                    source,
                    group_name,
                    storage_config=storage_config,
                    session=session,
                )
                objects: list[tuple[dict[str, Any], np.ndarray]] = []
                base: list[dict[str, Any]] = []

                for var_name in ds.data_vars:
                    variable = ds[var_name].variable
                    if not _is_encodable_dtype(variable.dtype):
                        logger.warning(
                            "Skipping variable '%s': unsupported dtype %s", var_name, variable.dtype
                        )
                        skipped_vars.add(str(var_name))
                        continue
                    archived_variables.add(str(var_name))
                    base.append(
                        variable_to_base_entry(
                            str(var_name),
                            variable,
                            zarr_meta=zarr_meta.get(str(var_name)),
                        )
                    )
                    objects.append(
                        (
                            variable_to_descriptor(
                                str(var_name),
                                variable,
                                compression=compression,
                            ),
                            prepare_array_for_encoding(variable),
                        )
                    )

                for coord_name in ds.coords:
                    coordinate = ds.coords[coord_name].variable
                    if not _is_encodable_dtype(coordinate.dtype):
                        logger.warning(
                            "Skipping variable '%s': unsupported dtype %s",
                            coord_name,
                            coordinate.dtype,
                        )
                        skipped_vars.add(str(coord_name))
                        continue
                    archived_coordinates.add(str(coord_name))
                    base.append(
                        variable_to_base_entry(
                            str(coord_name),
                            coordinate,
                            zarr_meta=zarr_meta.get(str(coord_name)),
                        )
                    )
                    objects.append(
                        (
                            variable_to_descriptor(
                                str(coord_name),
                                coordinate,
                                compression="none",
                            ),
                            prepare_array_for_encoding(coordinate),
                        )
                    )

                global_meta = make_data_meta(
                    group=group_name or "",
                    base=base,
                    source_uri=source,
                    compression=compression,
                    coordinates=coord_names,
                    start_date=start_date,
                    end_date=end_date,
                    extra_attrs=_collect_safe_attrs(dict(ds.attrs)),
                )
                tensogram_file.append(
                    global_meta, objects, allow_nan=allow_nan, allow_inf=allow_inf
                )

                group_key = group_name or ""
                archived_groups.append(group_key)
                time_ranges[group_key] = _dataset_time_range(ds)
            finally:
                ds.close()

        if _has_controlplane_root(source, storage_config=storage_config):
            product_identity = session.product
            product = product_identity.product_name
            manager = ChunkManager(
                binding=StorageBinding(identity=product_identity, driver=session.driver),
            )
            try:
                cp_descriptor, cp_array = serialize_controlplane(manager, product, group=group)
                cp_meta = make_controlplane_meta(product)
                tensogram_file.append(
                    cp_meta, [(cp_descriptor, cp_array)], allow_nan=True, allow_inf=True
                )
            finally:
                manager.close()
        else:
            logger.warning("No .firecube/ found at %s — archiving data only", source)

    file_size = target_path.stat().st_size
    single_group_time_range = time_ranges[archived_groups[0]] if len(archived_groups) == 1 else {}

    return {
        "source": source,
        "target": str(target_path),
        "variables": sorted(archived_variables),
        "skipped": sorted(skipped_vars),
        "coordinates": sorted(archived_coordinates),
        "time_range": single_group_time_range,
        "time_ranges": time_ranges,
        "file_size_bytes": file_size,
        "compression": compression,
        "group": group,
        "groups": archived_groups,
        "allow_nan": allow_nan,
        "allow_inf": allow_inf,
    }


def _make_archive_session(source: str, storage_config: Any | None) -> StorageSession:
    source_uri = parse_target(source)
    driver_config = StorageDriverConfig.from_storage_config_or_default(storage_config)
    return StorageSession(
        StorageBinding(
            identity=ProductIdentity.from_uri(source_uri, "zarr", product_name=source),
            driver=driver_config,
        )
    )


def _has_controlplane_root(source: str, *, storage_config: Any | None = None) -> bool:
    """Return True when the source product has a .firecube/ control-plane root."""
    if not is_remote_target(source):
        return (local_path_from_target(source) / ".firecube").exists()

    try:
        fs, root = _open_fsspec_url(source, storage_config=storage_config)
        control_root = f"{str(root).rstrip('/')}/.firecube"
        if hasattr(fs, "isdir"):
            return bool(fs.isdir(control_root))
        if hasattr(fs, "exists"):
            return bool(fs.exists(control_root))
    except Exception as exc:
        logger.debug("Could not inspect control-plane root for %s: %s", source, exc)
    return False


def _open_group_dataset(
    session: StorageSession,
    source_uri: StorageUri,
    *,
    group: str | None,
) -> xr.Dataset:
    """Open a zarr group as xarray, with a session-based fallback for v3 test stores."""
    try:
        return session.zarr.open_dataset(source_uri, group=group or None)
    except KeyError as exc:
        if "dimension_names" not in str(exc):
            raise
        logger.debug(
            "Falling back to raw zarr group open for %s group=%s: %s",
            source_uri.to_str(),
            group,
            exc,
        )

    zroot = session.zarr.open_group(source_uri, mode="r")
    zgroup = zroot if group is None else _group_from_root(zroot, group)

    data_vars: dict[str, xr.Variable] = {}
    for name, member in zgroup.members():
        if not isinstance(member, zarr.Array):
            continue
        dims = tuple(f"dim_{index}" for index in range(member.ndim))
        data_vars[str(name)] = xr.Variable(
            dims=dims,
            data=np.asarray(member),
            attrs=_collect_safe_attrs(dict(member.attrs)),
        )

    return xr.Dataset(data_vars=data_vars, attrs=_collect_safe_attrs(dict(zgroup.attrs)))


def _group_from_root(root: Any, group: str) -> Any:
    current = root
    for part in str(group).split("/"):
        current = current[part]
    if not isinstance(current, zarr.Group):
        raise TypeError(f"Expected zarr.Group at {group!r}, got {type(current)!r}")
    return current


def _find_time_dim(ds: xr.Dataset, preferred_time_dim: str | None = None) -> str | None:
    """Return the dataset time dimension name when present.

    When ``preferred_time_dim`` is supplied and exists in ``ds.dims``, that name
    is returned immediately. This lets plugins declaring a non-default time
    dimension (e.g. ``time_dim_name = 'obs_time'``) override the legacy
    fallback order ``('timestamp', 'time', <first datetime64 coord>)`` even
    when the dataset also exposes one of the legacy names.
    """
    if preferred_time_dim is not None and preferred_time_dim in ds.dims:
        return preferred_time_dim

    for name in ("timestamp", "time"):
        if name in ds.dims:
            return name

    for coord_name, coord in ds.coords.items():
        values = np.asarray(coord.values)
        if np.issubdtype(values.dtype, np.datetime64):
            return str(coord_name)

    return None


def _dataset_time_range(ds: xr.Dataset, preferred_time_dim: str | None = None) -> dict[str, str]:
    """Summarize the current time coverage for the dataset selection."""
    time_dim = _find_time_dim(ds, preferred_time_dim=preferred_time_dim)
    if time_dim is None or time_dim not in ds.coords:
        return {}

    values = np.asarray(ds.coords[time_dim].values)
    if values.size == 0:
        return {}

    first = values[0]
    last = values[-1]
    return {
        "start": _format_time_value(first),
        "end": _format_time_value(last),
        "n": str(values.size),
    }


def _format_time_value(value: Any) -> str:
    """Format datetime-like values consistently for summaries."""
    array_value = np.asarray(value)
    if np.issubdtype(array_value.dtype, np.datetime64):
        return str(np.datetime_as_string(array_value.astype("datetime64[ns]"), unit="s"))
    return str(value)
