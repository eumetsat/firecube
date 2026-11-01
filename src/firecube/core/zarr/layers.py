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

"""Core logic for building multi-resolution Zarr layers."""

from __future__ import annotations

import logging
from collections.abc import Iterable, MutableMapping
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast

import xarray as xr

if TYPE_CHECKING:
    from firecube.core.storage.session import StorageSession

from firecube.core.errors import ConfigurationError, Message, Param
from firecube.core.storage.uri import StorageUri

DEFAULT_MULTIRES_RESOLUTIONS: tuple[float, ...] = (1.0, 0.5)


def _require_axis(ds: xr.Dataset, name: str, option: Param) -> None:
    """Refuse a coordinate that is missing, not 1-D, or not on its own dimension."""
    if name not in ds.coords:
        raise ConfigurationError(
            Message(
                "Coordinate {name!r} is not in the dataset; name the coordinate with {option}.",
                name=name,
                option=option,
            )
        )
    dims = ds[name].dims
    if dims != (name,):
        raise ConfigurationError(
            Message(
                "Coordinate {name!r} must be 1-D on its own dimension, but has dimensions "
                "{dims}; name another coordinate with {option}.",
                name=name,
                dims=dims,
                option=option,
            )
        )


def build_multires_layers(
    zarr_store: str | Path | MutableMapping[str, bytes],
    *,
    session: StorageSession,
    resolutions: Iterable[float] = DEFAULT_MULTIRES_RESOLUTIONS,
    time_dim_name: str = "timestamp",
    silent: bool = True,
    logger: logging.Logger | None = None,
    group: str | None = None,
    strict: bool = False,
    ds_input: xr.Dataset | None = None,
    lat_name: str = "lat",
    lon_name: str = "lon",
) -> list[str]:
    """Build coarsened multi-resolution layers inside an existing Zarr store.

    Args:
        zarr_store: Store path or mapping. When a group is provided, zarr_store
            is assumed to point at the *root* of the product and `group`
            selects the dataset.
        group: Optional group path (e.g. "F048/FWI"). When omitted, empty or
            "/", the function opens the root dataset and writes the levels to
            ``multires/<res>deg``. Pass ``group`` explicitly if the store uses
            a grouped layout.
        lat_name: Name of the latitude coordinate to coarsen.
        lon_name: Name of the longitude coordinate to coarsen.
        strict: When True, failures to open the base dataset are raised instead
            of being converted into an empty result. This is useful for
            CLI/API operations where a failure should abort the request.
        ds_input: Optional xarray Dataset to coarsen. When provided, the
            function coarsens only this data and appends it to multiresolution
            layers (instead of rebuilding everything from the store).

    Raises:
        ConfigurationError: A named coordinate is missing, not 1-D, or not on its
            own dimension. Nothing is written.
    """

    groups_written: list[str] = []
    group = (group or "").strip("/")

    # Normalize resolutions
    if resolutions is True:
        resolutions = list(DEFAULT_MULTIRES_RESOLUTIONS)
    elif isinstance(resolutions, str):
        try:
            # Handle "1.0,0.5" or similar
            parsed = [float(r.strip()) for r in resolutions.split(",") if r.strip()]
        except (ValueError, TypeError) as exc:
            raise ValueError(f"Invalid resolutions: {resolutions!r}") from exc
        if not parsed:
            raise ValueError(f"Invalid resolutions: {resolutions!r}")
        resolutions = parsed

    if not resolutions or not isinstance(resolutions, Iterable):
        return groups_written

    def _get_base_ds(
        z_store: str,
        grp: str | None,
    ) -> xr.Dataset:
        z_uri = StorageUri.parse(z_store)
        open_kwargs = {"chunks": "auto"}
        if grp:
            return session.zarr.open_dataset(z_uri, **open_kwargs, group=grp)
        try:
            return session.zarr.open_dataset(z_uri, **open_kwargs)
        except Exception as exc:
            raise ValueError(
                f"Cannot open root dataset at {z_store!r}. "
                "If the store uses a grouped layout, pass the group explicitly."
            ) from exc

    ds_base: xr.Dataset
    is_incremental = False

    try:
        if ds_input is not None:
            ds_base = ds_input
            is_incremental = True
        else:
            ds_base = _get_base_ds(str(zarr_store), group)
    except Exception as exc:
        if logger and not silent:
            logger.warning("Failed to open base dataset for multires: %s", exc)
        if strict:
            raise
        return groups_written

    _require_axis(ds_base, lat_name, Param("lat_name"))
    _require_axis(ds_base, lon_name, Param("lon_name"))

    ds_base = ds_base.sortby([lat_name, lon_name])

    dlat = (
        float(abs(ds_base[lat_name].diff(lat_name).mean().values))
        if ds_base.sizes.get(lat_name, 0) > 1
        else 1.0
    )
    dlon = (
        float(abs(ds_base[lon_name].diff(lon_name).mean().values))
        if ds_base.sizes.get(lon_name, 0) > 1
        else 1.0
    )

    for res in resolutions:
        lat_factor = max(1, round(res / dlat))
        lon_factor = max(1, round(res / dlon))

        # When a group is provided, write multiresolution layers under that
        # group (e.g. F048/multires/1.0deg). Otherwise, default to a top-level
        # multires/ path.
        prefix = f"{group.rstrip('/')}/" if group else ""
        group_name = f"{prefix}multires/{res}deg"

        # Decide mode: if incremental and group already exists, append.
        mode: Literal["w", "a"] = "w"
        if is_incremental:
            try:
                base_s = str(zarr_store).rstrip("/")
                target_uri = f"{base_s}/{group_name}"
                if session.exists(StorageUri.parse(target_uri)):
                    mode = "a"
            except Exception:
                pass

        if logger and not silent:
            msg = "Building multi-resolution layer"
            if mode == "a":
                msg = "Appending to multi-resolution layer"
            logger.info(
                msg,
                extra={
                    "group": group_name,
                    "lat_factor": lat_factor,
                    "lon_factor": lon_factor,
                    "mode": mode,
                },
            )

        ds_coarse = cast(
            Any,
            ds_base.coarsen({lat_name: lat_factor, lon_name: lon_factor}, boundary="trim"),
        ).mean()  # skipna defaults to True for float data; xarray 2026.9.0 rejects the keyword

        session.zarr.write_dataset(
            ds_coarse,
            StorageUri.parse(str(zarr_store)),
            group=group_name,
            mode=mode,
            append_dim=time_dim_name if mode == "a" else None,
            zarr_format=3,
            safe_chunks=False,
            align_chunks=True,
        )

        if logger and not silent:
            logger.info("Multi-resolution layer written", extra={"layer": group_name})
        groups_written.append(group_name)

    if groups_written:
        session.zarr.refresh_consolidated_metadata(StorageUri.parse(str(zarr_store)))

    return groups_written
