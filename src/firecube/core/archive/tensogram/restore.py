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
import re
import tempfile
import warnings
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import xarray as xr
from zarr.errors import ZarrUserWarning

from firecube.core.archive._common import clear_target, refuse_other_product_label
from firecube.core.config import StorageConfig
from firecube.core.controlplane.types import CONTROL_DIRNAME
from firecube.core.product.identity import ProductIdentity
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.session import StorageSession
from firecube.core.uris import (
    is_remote_target,
    local_path_from_target,
    parse_target,
    require_target_is_product,
)

from ._compat import require_tensogram
from .controlplane_codec import deserialize_controlplane, restore_controlplane
from .schema import ARCHIVE_VERSION, KEY_ARCHIVE_VERSION, ROLE_CONTROLPLANE, ROLE_DATA

logger = logging.getLogger(__name__)

_CONTROL_NOT_RECOGNIZED = f"Object at {re.escape(CONTROL_DIRNAME)} is not recognized"


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


def _controlplane_label(tensogram_file: Any, source: str) -> str | None:
    """Return the label the control-plane message of an open archive names, or `None`.

    An archive without a control-plane message records no label.

    Raises:
        ValueError: A message's metadata cannot be decoded, or the control-plane
            message names no label.
    """
    # The converter appends the control-plane message last.
    for index in reversed(range(tensogram_file.message_count())):
        meta = tensogram_file.file_decode_metadata(index)
        extra = (meta.extra or {}) if hasattr(meta, "extra") else {}
        firecube_meta = extra.get("firecube", {})
        if firecube_meta.get("role", ROLE_DATA) != ROLE_CONTROLPLANE:
            continue
        label = firecube_meta.get("product")
        if not isinstance(label, str) or not label:
            raise ValueError(
                f"The control-plane message {index} of archive {source} names no product label"
            )
        return label
    return None


def tgm_product_label(source: str) -> str | None:
    """Return the product label a .tgm archive's control-plane message records, or `None`.

    An archive without a control-plane message records none. A file that cannot be opened
    returns `None`; the restore refuses it.

    Raises:
        ImportError: The tensogram extras are not installed.
        ValueError: A message's metadata cannot be decoded, or the control-plane
            message names no label.
    """
    require_tensogram("tgm_product_label")
    import tensogram as _tensogram  # pyright: ignore[reportMissingImports]

    tensogram = cast(Any, _tensogram)
    source_path = str(local_path_from_target(source))
    try:
        opened = tensogram.TensogramFile.open(source_path)
    except OSError:
        return None
    with opened as tensogram_file:
        return _controlplane_label(tensogram_file, source)


def _prepare_target(target: str, *, overwrite: bool, keep_run_id: str | None) -> None:
    if is_remote_target(target):
        return

    parse_target(target)  # refuse a relative target before anything is removed
    local_target_path = local_path_from_target(target)
    if not local_target_path.exists():
        return

    # An empty directory, or one that contains ONLY .firecube/ (control-plane state from
    # claim acquisition), is not a Zarr store yet. Any other content counts as an existing
    # store, whether or not it has a root zarr.json, because the restore writes the root
    # group in append mode and would leave that content beside the restored arrays.
    if local_target_path.is_dir() and all(
        entry.name == CONTROL_DIRNAME for entry in local_target_path.iterdir()
    ):
        return

    if not overwrite:
        raise FileExistsError(
            f"Target Zarr already exists: {target}. Use overwrite=True to replace it."
        )

    if local_target_path.is_dir():
        clear_target(local_target_path, keep_run_id=keep_run_id)
    else:
        local_target_path.unlink()


def _refuse_structural_issues(tensogram: Any, source_path: str) -> None:
    """Raise ValueError when the quick check of ``archive validate`` reports an issue."""
    report = tensogram.validate_file(source_path, level="quick")
    issues = [
        f"[file] offset={issue.get('byte_offset')}: {issue.get('description')}"
        for issue in report.get("file_issues", [])
    ]
    issues.extend(
        f"[msg {index}] {issue}"
        for index, message in enumerate(report.get("messages", []))
        for issue in message.get("issues", [])
    )
    if issues:
        raise ValueError(f"Invalid Tensogram archive {source_path}: {'; '.join(issues)}")


def _reconstruct_compressor(config: dict[str, Any] | None, *, array_name: str = "") -> Any | None:
    """Rebuild a zarr codec from its archived ``{"name", "configuration"}`` dict.

    Return ``None`` for no config. Warn with the array and codec name and return ``None``
    when the codec cannot be rebuilt.
    """
    if config is None:
        return None

    try:
        from zarr.registry import get_codec_class

        return get_codec_class(config["name"]).from_dict(config)
    except Exception:
        logger.warning(
            "Could not rebuild compressor %r of array %r; restoring it without the compressor",
            config.get("name", config.get("id", config)),
            array_name,
            exc_info=True,
        )
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
            codec = _reconstruct_compressor(
                cast(dict[str, Any] | None, base_entry.get("zarr_compressor")),
                array_name=str(var_name),
            )
            # An empty tuple writes no compressor. Omitting the key would let zarr add a
            # default ZstdCodec the archive never held.
            variable_encoding["compressors"] = () if codec is None else (codec,)

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
        # Mode "w" clears the group's path. At the root of a local target that is the whole
        # store, .firecube/ included, so the root group is added to the target the restore has
        # already prepared. A remote target is never prepared and keeps "w".
        local_root = group_name is None and not is_remote_target(target)
        mode = "w" if first_group and not local_root else "a"

        valid_names = set(ds.data_vars) | set(ds.coords)
        encoding = {k: v for k, v in encoding.items() if k in valid_names}
        with warnings.catch_warnings():
            if local_root:
                # the kept control plane (.firecube/) is not part of the zarr hierarchy
                warnings.filterwarnings(
                    "ignore",
                    message=_CONTROL_NOT_RECOGNIZED,
                    category=ZarrUserWarning,
                )
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
    keep_run_id: str | None = None,
) -> dict[str, Any]:
    """Restore a Tensogram .tgm archive to a Zarr store.

    ``source`` and ``target`` accept either a bare absolute path or a
    canonical URI (e.g. ``file:///…``, ``s3://…``).

    The restore validates the archive before it touches the target. It reads
    the message count and checks the archive version of the first message.
    It then runs the quick structural check of every message. An archive
    without messages returns an empty summary and leaves the target untouched.
    It refuses a target that records another product label than the archive's
    control-plane message before any write.

    An overwrite of a local target then removes the existing store. It keeps
    ``.firecube/claims/``, ``.firecube/schema.json``, ``.firecube/product.json``
    and ``.firecube/runs/<keep_run_id>/``. No write to a local target removes
    anything under ``.firecube/``.

    A remote target is never cleared. The first group's write replaces that
    group's path, and at the root that is the whole store.

    Args:
        source: Path to .tgm file (local only for now).
        target: Output Zarr store path (local or s3://).
        overwrite: If True, overwrite an existing Zarr store.
        storage_config: Optional storage config used to derive remote
            ``storage_options``.
        session: Storage session bound to the target product; built from
            ``target`` and ``storage_config`` when omitted.
        on_group_restored: Called with each restored group name, ``None``
            for the root group.
        keep_run_id: Run whose records an overwrite keeps; pass the restore's
            own run so its records survive.

    Returns:
        Summary dict describing the restore outcome.

    Raises:
        ImportError: If tensogram extras are not installed.
        ConfigurationError: If ``session`` is passed and ``target`` is not its
            product, or an overwrite finds a symlink on a kept local path.
        ValueError: If the archive version is unsupported, the structural
            check reports an issue, or the control-plane message names no label.
        ProductLabelMismatchError: If the target records another product label.
        ProductLabelConflictError: If an unstamped target's records name several labels.
        FileExistsError: If target exists and overwrite is False.
    """
    require_tensogram("tgm_to_zarr")
    if session is not None:
        require_target_is_product(target, session.product.product_uri)
    import tensogram as _tensogram  # pyright: ignore[reportMissingImports]

    tensogram = cast(Any, _tensogram)
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
        _refuse_structural_issues(tensogram, source_path)
        session = session or _make_archive_session(target, storage_config)
        # The control-plane replay logs and skips a refused run, so the label is checked here.
        if (label := _controlplane_label(tensogram_file, source)) is not None:
            refuse_other_product_label(session, label)

        _prepare_target(target, overwrite=overwrite, keep_run_id=keep_run_id)

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

            # "/" and "" both name the root group
            recorded_group = cast(str | None, firecube_meta.get("group"))
            group_name = None if recorded_group in (None, "", "/") else recorded_group
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

        if not first_group:
            session.zarr.refresh_consolidated_metadata(parse_target(target))

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
