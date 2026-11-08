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

"""Pack an existing Zarr product into a single-file zip archive.

The archive is readable with ``zarr.storage.ZipStore``. It is written exactly
once: built under a temporary name, closed, verified, then published under its
name. A publication without overwrite never replaces an archive that appeared meanwhile.
"""

from __future__ import annotations

import json
import os
import uuid
import warnings
import zipfile
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import xarray as xr
import zarr
from zarr.buffer import default_buffer_prototype
from zarr.storage import MemoryStore

from firecube.core.archive._common import (
    PRODUCT_LABEL_KEY,
    clear_target,
    refuse_other_product_label,
)
from firecube.core.archive.zip.marker import (
    MARKER_ENTRY,
    MODE_INCREMENTAL,
    marker_problem,
    read_zip_marker,
)
from firecube.core.controlplane.types import CLAIMS_DIRNAME, CONTROL_DIRNAME, RUNS_DIRNAME
from firecube.core.errors import Message, ProductLabelConflictError
from firecube.core.filesystem.store_factory import create_zip_store
from firecube.core.storage.session import StorageSession
from firecube.core.uris import is_remote_target, local_path_from_target, require_target_is_product
from firecube.core.zarr._zarr_compat import sync

_CLAIMS_PREFIX = f"{CONTROL_DIRNAME}/{CLAIMS_DIRNAME}/"
_CONTROL_PREFIX = f"{CONTROL_DIRNAME}/"
_RUNS_PREFIX = f"{CONTROL_DIRNAME}/{RUNS_DIRNAME}/"


def archived_product_label(zf: zipfile.ZipFile, archive: str) -> str | None:
    """Return the product label a zip archive records, or `None` when it records none.

    The `.firecube/product.json` entry decides. Without one, the label every readable
    `.firecube/runs/<run>/run.json` entry names decides.

    Raises:
        ValueError: The stamp entry is not JSON or names no label.
        ProductLabelConflictError: The run entries name more than one label.
    """
    names = set(zf.namelist())
    if PRODUCT_LABEL_KEY in names:
        raw = zf.read(PRODUCT_LABEL_KEY)
        try:
            payload = json.loads(raw)
        except ValueError as exc:
            raise ValueError(
                f"Entry {PRODUCT_LABEL_KEY!r} in archive {archive} is not JSON: {exc}"
            ) from exc
        label = payload.get("product") if isinstance(payload, dict) else None
        if not isinstance(label, str) or not label:
            raise ValueError(
                f"Entry {PRODUCT_LABEL_KEY!r} in archive {archive} names no product label"
            )
        return label
    labels: set[str] = set()
    for name in names:
        if not name.startswith(_RUNS_PREFIX):
            continue
        run, _, leaf = name.removeprefix(_RUNS_PREFIX).partition("/")
        if not run or leaf != "run.json":
            continue
        try:
            payload = json.loads(zf.read(name))
        except ValueError:
            continue  # an unreadable run record names no label, as in a live product
        label = payload.get("product") if isinstance(payload, dict) else None
        if isinstance(label, str) and label:
            labels.add(label)
    if len(labels) > 1:
        ordered = sorted(labels)
        raise ProductLabelConflictError(
            Message(
                "The run records of the archive {archive} name more than one label, "
                "{labels}, and the archive has no label stamp.",
                archive=archive,
                labels=", ".join(repr(label) for label in ordered),
            ),
            labels=ordered,
        )
    return next(iter(labels), None)


def zip_product_label(archive: str) -> str | None:
    """Return the product label the zip archive at `archive` records, or `None`.

    A missing file, or one that is not a zip archive, returns `None`; the zip restore
    refuses it.

    Raises:
        ValueError: The stamp entry is not JSON or names no label.
        ProductLabelConflictError: The run entries name more than one label.
    """
    try:
        zf = zipfile.ZipFile(local_path_from_target(archive))
    except (zipfile.BadZipFile, OSError):
        return None
    with zf:
        return archived_product_label(zf, archive)


def zip_label_stamp(archive: str) -> bytes | None:
    """Return the bytes of the zip archive's `.firecube/product.json` entry, or `None` without one.

    A missing file, or one that is not a zip archive, returns `None`.
    """
    try:
        zf = zipfile.ZipFile(local_path_from_target(archive))
    except (zipfile.BadZipFile, OSError):
        return None
    with zf:
        return zf.read(PRODUCT_LABEL_KEY) if PRODUCT_LABEL_KEY in zf.namelist() else None


@dataclass(frozen=True)
class ZipArchiveResult:
    """Summary of a zip archive creation.

    Attributes:
        source: URI of the archived product.
        target: Local path of the written archive.
        keys_written: Number of entries in the archive.
        file_size_bytes: Size of the archive on disk.
    """

    source: str
    target: str
    keys_written: int
    file_size_bytes: int


def zarr_to_zip(
    session: StorageSession,
    target: str,
    *,
    overwrite: bool = False,
    keys: Iterable[str] | None = None,
    marker: Mapping[str, Any] | None = None,
) -> ZipArchiveResult:
    """Copy the keys of the session's product into a local zip archive.

    Control-plane files are included, except claim files. An existing archive
    is replaced only after the new one has been written and verified.

    Args:
        session: Storage session bound to the source product.
        target: Local path or ``file://`` URI of the archive to create.
        overwrite: Replace an existing archive at ``target``.
        keys: Product keys to archive; None archives every key.
        marker: Written as canonical JSON at the root entry ``firecube-archive.json``.
            The entry is not counted in ``keys_written``.

    Returns:
        A summary of the written archive.

    Raises:
        ValueError: If ``target`` is remote, is a directory, lies inside the
            source store, the source has no Zarr v3 root metadata, the product
            holds the reserved marker entry, or ``keys`` names a key the product
            lacks or the reserved marker entry.
        FileExistsError: If ``target`` exists, or another writer publishes it first, and
            ``overwrite`` is False. The new archive is then discarded.
        RuntimeError: If the written archive fails verification.
    """
    if is_remote_target(target):
        raise ValueError(f"Remote target not yet supported for zip output: {target}")
    target_path = local_path_from_target(target)
    if target_path.is_dir():
        raise ValueError(f"Target is a directory, expected a file path: {target}")
    if target_path.exists() and not overwrite:
        raise FileExistsError(f"Target file already exists: {target}")

    source_uri = session.product.product_uri
    if not source_uri.is_remote():
        source_path = local_path_from_target(source_uri.to_str()).resolve()
        if target_path.resolve().is_relative_to(source_path):
            raise ValueError(f"Target must not be inside the source store: {target}")

    root = source_uri.to_str().rstrip("/") + "/"
    sources = {
        key: uri
        for uri in session.find(source_uri)
        if not (key := uri.to_str()[len(root) :]).startswith(_CLAIMS_PREFIX)
    }
    if "zarr.json" not in sources:
        raise ValueError(f"No Zarr v3 root metadata found at {source_uri.to_str()}")
    if keys is None:
        selected = sorted(sources)
        if MARKER_ENTRY in selected:
            raise ValueError(f"Keys not in the product or reserved for the archive: {MARKER_ENTRY}")
    else:
        selected = sorted(set(keys))
        if unknown := [k for k in selected if k not in sources or k == MARKER_ENTRY]:
            raise ValueError(
                f"Keys not in the product or reserved for the archive: {', '.join(unknown[:5])}"
            )

    target_path.parent.mkdir(parents=True, exist_ok=True)
    partial_path = target_path.with_name(f"{target_path.name}.{uuid.uuid4().hex}.partial")
    try:
        handle = create_zip_store(target=str(partial_path), mode="w")
        make_buffer = default_buffer_prototype().buffer.from_bytes
        try:
            for key in selected:
                with session.open(sources[key], "rb") as src:
                    sync(handle.store.set(key, make_buffer(src.read())))
            if marker is not None:
                payload = json.dumps(
                    dict(marker), sort_keys=True, separators=(",", ":"), ensure_ascii=False
                ).encode()
                sync(handle.store.set(MARKER_ENTRY, make_buffer(payload)))
        finally:
            handle.store.close()
        _verify(str(partial_path), expected_keys=selected, has_marker=marker is not None)
        if overwrite:
            os.replace(partial_path, target_path)
        else:
            # The existence check above reserves nothing: another writer may publish the
            # same path meanwhile. A hard link publishes the verified file whole and raises
            # FileExistsError when the target exists, so a lost race clobbers nothing.
            try:
                os.link(partial_path, target_path)
            except FileExistsError as exc:
                raise FileExistsError(f"Target file already exists: {target}") from exc
            partial_path.unlink()
    except BaseException:
        partial_path.unlink(missing_ok=True)
        raise

    return ZipArchiveResult(
        source=source_uri.to_str(),
        target=str(target_path),
        keys_written=len(selected),
        file_size_bytes=target_path.stat().st_size,
    )


def _prepare_target(target_path: Path, *, overwrite: bool, keep_run_id: str | None) -> None:
    """Same rules as the .tgm restore: a directory holding only .firecube/ is not a store yet."""
    if not target_path.exists():
        return
    if target_path.is_dir() and not (target_path / "zarr.json").exists():
        return
    if not overwrite:
        raise FileExistsError(
            f"Target Zarr already exists: {target_path}. Use overwrite=True to replace it."
        )
    if target_path.is_dir():
        clear_target(target_path, keep_run_id=keep_run_id)
    else:
        target_path.unlink()


def _refuse_unsafe_entries(names: list[str], archive: str) -> None:
    """Raise ValueError for the first entry name that is not a canonical relative key."""
    for name in names:
        # A directory entry ends in one "/"; its segments are checked like a file's.
        segments = name.removesuffix("/").split("/")
        if (
            name.startswith("/")
            or "\\" in name
            or any(segment in ("", ".", "..") for segment in segments)
        ):
            raise ValueError(
                f"Unsafe entry {name!r} in archive {archive}: it is absolute, holds a "
                "backslash, or has an empty, '.' or '..' segment"
            )


def _refuse_invalid_metadata(zf: zipfile.ZipFile, keys: list[str], archive: str) -> None:
    """Raise ValueError for the first ``zarr.json`` entry that is not a valid Zarr v3 document.

    Each document is parsed alone, so a parent's consolidated copy never stands in for a child.
    """
    make_buffer = default_buffer_prototype().buffer.from_bytes
    for key in keys:
        if key.rpartition("/")[2] != "zarr.json":
            continue
        data = zf.read(key)
        try:
            document = json.loads(data)
            if not isinstance(document, dict) or document.get("zarr_format") != 3:
                raise ValueError("not a Zarr v3 metadata document")
            store = MemoryStore({"zarr.json": make_buffer(data)})
            node_type = document.get("node_type")
            if node_type == "array":
                zarr.open_array(store=store, mode="r", zarr_format=3)
            elif node_type == "group":
                zarr.open_group(store=store, mode="r", zarr_format=3)
            else:
                raise ValueError(f"node_type is {node_type!r}, expected 'array' or 'group'")
        except Exception as exc:
            raise ValueError(f"Invalid Zarr metadata {key!r} in archive {archive}: {exc}") from exc


def _walk_live(store_kwargs: dict[str, Any]) -> list[tuple[str, zarr.Group | zarr.Array]]:
    """Return ``(path, node)`` for every array and group below the root, read from live metadata.

    Each group is reopened with ``use_consolidated=False`` and listed at depth 0, so no
    consolidated snapshot at any level stands in for a child document.
    """
    nodes: dict[str, zarr.Group | zarr.Array] = {}
    pending = [""]
    with warnings.catch_warnings():
        # the control plane (.firecube/) is not part of the zarr hierarchy
        warnings.filterwarnings("ignore", message="Object at .* is not recognized")
        while pending:
            parent = pending.pop()
            group = zarr.open_group(
                **store_kwargs,
                path=parent or None,
                mode="r",
                zarr_format=3,
                use_consolidated=False,
            )
            if parent:
                nodes[parent] = group  # the reopened group, not the child the parent listed
            for name, node in group.members():
                path = f"{parent}/{name}" if parent else name
                nodes[path] = node
                if isinstance(node, zarr.Group):
                    pending.append(path)
    return sorted(nodes.items())


def zip_to_zarr(
    archive: str,
    target: str,
    *,
    session: StorageSession,
    overwrite: bool = False,
    keep_run_id: str | None = None,
) -> dict:
    """Restore a zarr zip archive into the session's product by copying every key.

    The session's driver writes every entry. The restore validates the whole
    archive before it touches the target. It refuses an entry name that is
    absolute, holds a backslash, or has an empty, ``.`` or ``..`` segment.
    It then refuses a corrupt entry and a missing root ``zarr.json``. Last, it
    parses every ``zarr.json`` entry on its own as Zarr v3 metadata. It refuses a
    target that records another product label than the archive before any write.

    An overwrite then removes the existing store. It keeps ``.firecube/claims/``,
    ``.firecube/schema.json``, ``.firecube/product.json`` and
    ``.firecube/runs/<keep_run_id>/``. The copy never replaces a target's label stamp.

    Args:
        archive: Local path or ``file://`` URI of the zip archive.
        target: Local path or ``file://`` URI of the product to restore; it
            must name the session's product.
        session: Storage session bound to the target product.
        overwrite: Replace an existing Zarr store at ``target``.
        keep_run_id: Run whose records an overwrite keeps; pass the restore's
            own run so its records survive.

    Returns:
        A summary of the restored groups, variables, coordinates and key count.

    Raises:
        ConfigurationError: If ``target`` is not the session's product, or an
            overwrite finds a symlink at the target or on a kept path.
        FileNotFoundError: If ``archive`` is not a file.
        ValueError: If the archive is corrupt, has no root metadata, holds
            invalid Zarr metadata, a marker that is not a JSON object or a
            malformed label stamp, or holds an entry name outside the target.
        ProductLabelConflictError: If the archive has no label stamp and its run
            records name several labels, or the target's do.
        ProductLabelMismatchError: If the target records another product label.
        FileExistsError: If ``target`` holds a Zarr store and ``overwrite`` is False.
    """
    require_target_is_product(target, session.product.product_uri)
    archive_path = local_path_from_target(archive)
    target_path = local_path_from_target(target)
    if not archive_path.is_file():
        raise FileNotFoundError(f"Archive not found: {archive}")

    with zipfile.ZipFile(archive_path) as zf:
        names = zf.namelist()
        _refuse_unsafe_entries(names, archive)
        if (corrupt := zf.testzip()) is not None:
            raise ValueError(f"Corrupt entry {corrupt!r} in archive: {archive}")
        # claims are never archived, but never restore a stale claim either; the marker
        # is the archive's own entry, never a product key
        keys = sorted(
            k
            for k in names
            if not k.endswith("/") and not k.startswith(_CLAIMS_PREFIX) and k != MARKER_ENTRY
        )
        is_incremental = _is_incremental_backup(read_zip_marker(str(archive_path)))
        if "zarr.json" not in keys and not is_incremental:
            raise ValueError(f"No Zarr v3 root metadata found in archive: {archive}")
        _refuse_invalid_metadata(zf, keys, archive)
        label = archived_product_label(zf, archive)
    if label is not None:
        refuse_other_product_label(session, label)

    _prepare_target(target_path, overwrite=overwrite, keep_run_id=keep_run_id)

    product_uri = session.product.product_uri
    # A target stamp records the archive's label, checked above, and is create-only: it stays.
    kept_stamp = PRODUCT_LABEL_KEY in keys and session.exists(product_uri.join(PRODUCT_LABEL_KEY))
    with zipfile.ZipFile(archive_path) as zf:
        for key in keys:
            if not (kept_stamp and key == PRODUCT_LABEL_KEY):
                session.put_bytes(product_uri.join(key), zf.read(key))

    # verify: the restored store opens and lists the same groups and arrays; an incremental
    # backup without a root document has no hierarchy of its own to open
    members = (
        _walk_live(session.zarr.create_store(product_uri, mode="r").zarr_kwargs())
        if "zarr.json" in keys
        else []
    )

    groups = [name for name, node in members if isinstance(node, zarr.Group)]
    arrays = {name: node for name, node in members if isinstance(node, zarr.Array)}
    coordinates: set[str] = set()
    for name, node in arrays.items():
        group, _, var = name.rpartition("/")
        if tuple(_dimension_names(node)) == (var,):
            coordinates.add(name)
        for coord in str(node.attrs.get("coordinates", "")).split():
            coordinates.add(f"{group}/{coord}" if group else coord)

    return {
        "source": str(archive_path),
        "target": str(target_path),
        "groups": groups,
        "variables": sorted(n for n in arrays if n not in coordinates),
        "coordinates": sorted(n for n in arrays if n in coordinates),
        "keys_restored": len(keys),
        "controlplane_restored": any(k.startswith(_CONTROL_PREFIX) for k in keys),
    }


def zip_info(archive: str) -> dict:
    """Collect metadata of a zarr zip archive."""
    path = archive.removeprefix("file://")
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Archive not found: {archive}")

    with zipfile.ZipFile(path) as zf:
        keys = zf.namelist()
    has_controlplane = any(k.startswith(f"{CONTROL_DIRNAME}/") for k in keys)
    marker = read_zip_marker(path)

    store = create_zip_store(target=str(path), mode="r").store
    try:
        group_vars: dict[str, dict] = {"": {}}  # "" is the root group
        # an incremental backup may hold no root document, so it has no hierarchy to walk
        members = _walk_live({"store": store}) if "zarr.json" in keys else []
        for name, node in members:
            if isinstance(node, zarr.Group):
                group_vars.setdefault(name, {})
                continue
            group, _, var = name.rpartition("/")
            group_vars.setdefault(group, {})[var] = {
                "shape": [int(n) for n in node.shape],
                "dtype": str(node.dtype),
                "dims": _dimension_names(node),
                "zarr_chunks": [int(n) for n in node.chunks],
                "compressors": [_codec_name(c) for c in node.compressors],
            }
    finally:
        store.close()

    return {
        "path": path,
        "format": "zip",
        "size_bytes": os.path.getsize(path),
        "entries": len(keys),
        "groups": sorted(group_vars),
        "has_controlplane": has_controlplane,
        "marker": marker,
        "group_details": [{"group": g, "variables": group_vars[g]} for g in sorted(group_vars)],
    }


def list_zip(archive: str) -> list[tuple[str, str]]:
    """List the contents of a zarr zip archive.

    Returns:
        One ``(group, dataset)`` pair per group that holds arrays, where
        ``dataset`` is the xarray representation of that group. ``""`` is the
        root group.
    """
    path = archive.removeprefix("file://")
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Archive not found: {archive}")
    store = create_zip_store(target=str(path), mode="r").store
    try:
        root = zarr.open_group(store=store, mode="r", zarr_format=3, use_consolidated=False)
        live_groups: dict[str, zarr.Group] = {
            "": root,
            **{
                name: node
                for name, node in _walk_live({"store": store})
                if isinstance(node, zarr.Group)
            },
        }
        listing = []
        for group in sorted(live_groups):
            node = live_groups[group]
            if not any(isinstance(m, zarr.Array) for _, m in node.members()):
                continue
            ds = xr.open_zarr(store, group=group or None, consolidated=False)
            listing.append((group, repr(ds)))
    finally:
        store.close()
    return listing


def validate_zip(archive: str, *, quick: bool = False) -> list[str]:
    """Return the problems found in a zarr zip archive; an empty list means valid.

    quick: check the zip directory and Zarr metadata only, skip the CRC check of every entry.
    A marker must pass the checks a backup chain restore applies to it.
    """
    path = Path(archive.removeprefix("file://"))
    if not path.is_file():
        raise FileNotFoundError(f"Archive not found: {archive}")

    issues: list[str] = []
    try:
        with zipfile.ZipFile(path) as zf:
            keys = zf.namelist()
            if not quick and (corrupt := zf.testzip()) is not None:
                issues.append(f"CRC mismatch in entry {corrupt!r}")
    except zipfile.BadZipFile as exc:
        return [f"not a zip file: {exc}"]

    try:
        marker = read_zip_marker(str(path))
    except ValueError as exc:
        issues.append(str(exc))
        marker = None
    if marker is not None and (problem := marker_problem(marker)) is not None:
        issues.append(f"Entry {MARKER_ENTRY!r} is not a valid archive marker: {problem}")
    is_incremental = _is_incremental_backup(marker)

    if "zarr.json" not in keys:
        if not is_incremental:
            issues.append("no Zarr v3 root metadata (zarr.json)")
            return issues
        # an incremental backup may hold no root document; its entries are checked alone
        return [*issues, *_incremental_backup_metadata_issues(path)]

    store = create_zip_store(target=str(path), mode="r").store
    try:
        root = zarr.open_group(store=store, mode="r", zarr_format=3)
        with warnings.catch_warnings():
            # the control plane (.firecube/) is not part of the zarr hierarchy
            warnings.filterwarnings("ignore", message="Object at .* is not recognized")
            root.members(max_depth=None)  # parses the metadata of every group and array
        _walk_live({"store": store})  # parses every document itself, wherever a snapshot sits
    except Exception as exc:
        issues.append(f"cannot open as Zarr: {type(exc).__name__}: {exc}")
    finally:
        store.close()
    return issues


def _is_incremental_backup(marker: Mapping[str, Any] | None) -> bool:
    """Return True when the marker declares an incremental backup."""
    return marker is not None and marker.get("mode") == MODE_INCREMENTAL


def _incremental_backup_metadata_issues(path: Path) -> list[str]:
    """Return the issue of the first invalid ``zarr.json`` entry of an incremental backup."""
    with zipfile.ZipFile(path) as zf:
        keys = [k for k in zf.namelist() if k != MARKER_ENTRY and not k.endswith("/")]
        try:
            _refuse_invalid_metadata(zf, keys, str(path))
        except ValueError as exc:
            return [str(exc)]
    return []


def _dimension_names(array: zarr.Array) -> list[str]:
    """Return the dimension names of a Zarr v3 array, empty when unnamed."""
    names = getattr(array.metadata, "dimension_names", None) or ()
    return [str(name) for name in names]


def _codec_name(codec: object) -> str:
    """Return the registered name of a Zarr v3 codec."""
    to_dict = getattr(codec, "to_dict", None)
    config = cast(dict[str, Any], to_dict()) if callable(to_dict) else {}
    return str(config.get("name", type(codec).__name__))


def _verify(path: str, *, expected_keys: list[str], has_marker: bool = False) -> None:
    """Check a freshly written archive: valid, and holding exactly the expected keys.

    The marker entry, when ``has_marker`` is true, is expected beside the product keys.
    """
    expected = sorted([*expected_keys, MARKER_ENTRY]) if has_marker else expected_keys
    issues = validate_zip(path)
    if not any(i.startswith("not a zip file") for i in issues):
        with zipfile.ZipFile(path) as zf:
            if sorted(zf.namelist()) != expected:
                issues.append("entries differ from the keys written")
    if issues:
        raise RuntimeError(f"Zip archive verification failed for {path}: {'; '.join(issues)}")
