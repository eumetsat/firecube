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

"""Core Tensogram (.tgm) -> Zarr restore."""

from __future__ import annotations

import logging
import os
import shutil
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import xarray as xr

from firecube.core.config import StorageConfig
from firecube.core.product.identity import ProductIdentity
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.session import StorageSession
from firecube.core.uris import is_remote_target, local_path_from_target, parse_target

from ._compat import require_tensogram
from .controlplane_codec import deserialize_controlplane, restore_controlplane
from .schema import ARCHIVE_VERSION, KEY_ARCHIVE_VERSION, ROLE_CONTROLPLANE, ROLE_DATA

logger = logging.getLogger(__name__)


def _make_archive_session(target: str, storage_config: Any | None) -> StorageSession:
    target_uri = parse_target(target)
    if storage_config is None:
        target_path = Path(target_uri.path).resolve()
        storage_config = StorageConfig(storage_type="local")
        storage_config.target_path = str(target_path.parent)  # type: ignore[attr-defined]

    driver_config = StorageDriverConfig.from_storage_config(storage_config)
    return StorageSession(
        StorageBinding(
            identity=ProductIdentity.from_uri(target_uri, "zarr", product_name=target),
            driver=driver_config,
        )
    )


def _prepare_target(target: str, *, overwrite: bool) -> None:
    if is_remote_target(target):
        return

    target_uri = parse_target(target)
    local_target_path = Path(target_uri.path).resolve()
    if not local_target_path.exists():
        return

    # A directory that contains ONLY .firecube/ (control-plane state from claim
    # acquisition) is not a Zarr store yet — do not treat it as an existing store.
    # Only raise if the directory contains actual Zarr metadata (zarr.json or .zarray).
    if local_target_path.is_dir():
        has_zarr_metadata = (local_target_path / "zarr.json").exists() or (
            local_target_path / ".zarray"
        ).exists()
        if not has_zarr_metadata:
            return

    if not overwrite:
        raise FileExistsError(
            f"Target Zarr already exists: {target}. Use overwrite=True to replace it."
        )

    if local_target_path.is_dir():
        shutil.rmtree(local_target_path)
    else:
        local_target_path.unlink()


def _reconstruct_compressor(config: dict[str, Any] | None) -> Any | None:
    """Reconstruct a numcodecs compressor from a config dict."""
    if config is None:
        return None

    try:
        import numcodecs

        return numcodecs.get_codec(config)
    except Exception:
        logger.debug("Could not reconstruct compressor from config: %s", config, exc_info=True)
        return None


def _build_encoding(base_entries: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    encoding: dict[str, dict[str, Any]] = {}
    for base_entry in base_entries:
        var_name = base_entry.get("name")
        if not var_name:
            continue

        variable_encoding: dict[str, Any] = {}
        zarr_chunks = base_entry.get("zarr_chunks")
        if zarr_chunks:
            variable_encoding["chunks"] = tuple(zarr_chunks)

        if "zarr_compressor" in base_entry:
            variable_encoding["compressor"] = _reconstruct_compressor(
                cast(dict[str, Any] | None, base_entry.get("zarr_compressor"))
            )

        if "zarr_fill_value" in base_entry:
            variable_encoding["fill_value"] = base_entry.get("zarr_fill_value")

        if variable_encoding:
            encoding[str(var_name)] = variable_encoding

    return encoding


def _open_message_dataset(
    tensogram_file: Any,
    message_index: int,
    *,
    coord_names: list[str],
) -> tuple[xr.Dataset, str]:
    raw_message = tensogram_file.read_message(message_index)
    with tempfile.NamedTemporaryFile(suffix=".tgm", delete=False) as tmp:
        tmp.write(raw_message)
        tmp_path = tmp.name

    ds = xr.open_dataset(tmp_path, engine="tensogram")
    vars_to_promote = [coord_name for coord_name in coord_names if coord_name in ds.data_vars]
    if vars_to_promote:
        ds = ds.set_coords(vars_to_promote)
    return ds, tmp_path


def _restore_data_message(
    tensogram_file: Any,
    message_index: int,
    *,
    target: str,
    group_name: str | None,
    session: StorageSession,
    first_group: bool,
) -> dict[str, Any]:
    msg_meta = tensogram_file.file_decode_metadata(message_index)
    base_entries = list(msg_meta.base or [])
    extra = (msg_meta.extra or {}) if hasattr(msg_meta, "extra") else {}
    firecube_meta = extra.get("firecube", {})
    coord_names = list(cast(list[str], firecube_meta.get("coordinates", [])))
    encoding = _build_encoding(base_entries)

    ds, tmp_path = _open_message_dataset(
        tensogram_file,
        message_index,
        coord_names=coord_names,
    )
    try:
        mode = "w" if first_group else "a"

        valid_names = set(ds.data_vars) | set(ds.coords)
        encoding = {k: v for k, v in encoding.items() if k in valid_names}
        session.zarr.write_dataset(
            ds,
            parse_target(target),
            mode=mode,
            group=group_name,
            encoding=encoding,
        )
        return {
            "group": group_name,
            "variables": list(ds.data_vars),
            "coordinates": list(ds.coords),
        }
    finally:
        ds.close()
        os.unlink(tmp_path)


def tgm_to_zarr(
    source: str,
    target: str,
    *,
    overwrite: bool = False,
    storage_config: Any | None = None,
    session: StorageSession | None = None,
    on_group_restored: Callable[[str | None], None] | None = None,
) -> dict[str, Any]:
    """Restore a Tensogram .tgm archive to a Zarr store.

    ``source`` and ``target`` accept either a bare absolute path or a
    canonical URI (e.g. ``file:///…``, ``s3://…``).

    Args:
        source: Path to .tgm file (local only for now).
        target: Output Zarr store path (local or s3://).
        overwrite: If True, overwrite an existing Zarr store.
        storage_config: Optional storage config used to derive remote
            ``storage_options``.

    Returns:
        Summary dict describing the restore outcome.

    Raises:
        ImportError: If tensogram extras are not installed.
        FileExistsError: If target exists and overwrite is False.
    """
    require_tensogram("tgm_to_zarr")
    import tensogram as _tensogram  # pyright: ignore[reportMissingImports]

    tensogram = cast(Any, _tensogram)
    _prepare_target(target, overwrite=overwrite)
    session = session or _make_archive_session(target, storage_config)

    source_path = str(local_path_from_target(source))

    with tensogram.TensogramFile.open(source_path) as tensogram_file:  # pyright: ignore[reportAttributeAccessIssue]
        count = tensogram_file.message_count()
        if count == 0:
            return {
                "source": source,
                "target": target,
                "group": None,
                "groups": [],
                "variables": [],
                "coordinates": [],
                "restored": 0,
                "messages_restored": 0,
                "controlplane_restored": False,
            }

        first_meta = tensogram_file.file_decode_metadata(0)
        first_extra = (first_meta.extra or {}) if hasattr(first_meta, "extra") else {}
        firecube_meta = first_extra.get("firecube", {})
        archive_version = firecube_meta.get(KEY_ARCHIVE_VERSION)
        if archive_version != ARCHIVE_VERSION:
            raise ValueError(
                f"Unsupported Firecube archive version: {archive_version!r}. "
                f"Expected {ARCHIVE_VERSION!r}."
            )

        restored_groups: list[str] = []
        restored_variables: list[str] = []
        restored_coordinates: list[str] = []
        controlplane_restored = False
        first_group = True

        for i in range(count):
            msg_meta = tensogram_file.file_decode_metadata(i)
            extra = (msg_meta.extra or {}) if hasattr(msg_meta, "extra") else {}
            firecube_meta = extra.get("firecube", {})
            role = firecube_meta.get("role", ROLE_DATA)

            if role == ROLE_CONTROLPLANE:
                msg = tensogram_file.decode_message(i)
                if not msg.objects:
                    continue
                cp_array = msg.objects[0][1]
                cp_state = deserialize_controlplane(cp_array)
                restore_controlplane(cp_state, target, storage_config=storage_config)
                controlplane_restored = True
                continue

            group_name = cast(str | None, firecube_meta.get("group") or None)
            restored = _restore_data_message(
                tensogram_file,
                i,
                target=target,
                group_name=group_name,
                session=session,
                first_group=first_group,
            )
            first_group = False

            if restored["group"] is not None:
                restored_groups.append(cast(str, restored["group"]))
            restored_variables.extend(cast(list[str], restored["variables"]))
            restored_coordinates.extend(cast(list[str], restored["coordinates"]))

            if on_group_restored is not None:
                on_group_restored(cast(str | None, restored["group"]))

        return {
            "source": source,
            "target": target,
            "group": None,
            "groups": restored_groups,
            "variables": sorted(set(restored_variables)),
            "coordinates": sorted(set(restored_coordinates)),
            "restored": len(restored_groups),
            "messages_restored": len(restored_groups),
            "controlplane_restored": controlplane_restored,
        }
