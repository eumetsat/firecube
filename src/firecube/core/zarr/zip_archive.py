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
once: built under a temporary name, closed, verified, then renamed into place.
"""

from __future__ import annotations

import os
import uuid
import zipfile
from dataclasses import dataclass

import zarr
from zarr.buffer import default_buffer_prototype
from zarr.core.sync import sync

from firecube.core.controlplane.types import CLAIMS_DIRNAME, CONTROL_DIRNAME
from firecube.core.filesystem.store_factory import create_zip_store
from firecube.core.storage.session import StorageSession
from firecube.core.uris import is_remote_target, local_path_from_target

_CLAIMS_PREFIX = f"{CONTROL_DIRNAME}/{CLAIMS_DIRNAME}/"


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
    session: StorageSession, target: str, *, overwrite: bool = False
) -> ZipArchiveResult:
    """Copy every key of the session's product into a local zip archive.

    Control-plane files are included, except claim files. An existing archive
    is replaced only after the new one has been written and verified.

    Args:
        session: Storage session bound to the source product.
        target: Local path or ``file://`` URI of the archive to create.
        overwrite: Replace an existing archive at ``target``.

    Returns:
        A summary of the written archive.

    Raises:
        ValueError: If ``target`` is remote, is a directory, lies inside the
            source store, or the source has no Zarr v3 root metadata.
        FileExistsError: If ``target`` exists and ``overwrite`` is False.
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
    keys = sorted(sources)

    target_path.parent.mkdir(parents=True, exist_ok=True)
    partial_path = target_path.with_name(f"{target_path.name}.{uuid.uuid4().hex}.partial")
    try:
        handle = create_zip_store(target=str(partial_path), mode="w")
        make_buffer = default_buffer_prototype().buffer.from_bytes
        try:
            for key in keys:
                with session.open(sources[key], "rb") as src:
                    sync(handle.store.set(key, make_buffer(src.read())))
        finally:
            handle.store.close()
        _verify(str(partial_path), expected_keys=keys)
        os.replace(partial_path, target_path)
    except BaseException:
        partial_path.unlink(missing_ok=True)
        raise

    return ZipArchiveResult(
        source=source_uri.to_str(),
        target=str(target_path),
        keys_written=len(keys),
        file_size_bytes=target_path.stat().st_size,
    )


def _verify(path: str, *, expected_keys: list[str]) -> None:
    """Check the archive's entries and that zarr can open its root group."""
    with zipfile.ZipFile(path) as archive:
        found = sorted(archive.namelist())
        corrupt = archive.testzip()
    if found != expected_keys or corrupt is not None:
        raise RuntimeError(f"Zip archive verification failed for {path}")
    handle = create_zip_store(target=path, mode="r")
    try:
        zarr.open_group(store=handle.store, mode="r", zarr_format=3)
    finally:
        handle.store.close()
