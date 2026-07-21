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

"""Counting filesystem wrapper used by resume-guard tests."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from firecube.core.filesystem import create_filesystem
from firecube.core.filesystem.protocol import AtomicWriter, StorageFilesystemFull
from firecube.core.product.identity import ProductIdentity
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.uri import StorageUri


class _CountingAtomicWriter:
    def __init__(self, writer: AtomicWriter, counts: dict[str, int]) -> None:
        self._writer = writer
        self._counts = counts

    def write_atomic(self, uri: StorageUri, data: bytes) -> None:
        self._counts["write_atomic"] = self._counts.get("write_atomic", 0) + 1
        self._writer.write_atomic(uri, data)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._writer, name)


class CountingFilesystem:
    """Thin pass-through wrapper that counts filesystem calls."""

    def __init__(self, fs: StorageFilesystemFull) -> None:
        self._fs: Any = fs
        self.counts: dict[str, int] = {"ls": 0, "exists": 0, "open": 0, "rm": 0}
        self._atomic_writer = None
        if hasattr(fs, "atomic_writer"):
            self.counts["write_atomic"] = 0
            self._atomic_writer = _CountingAtomicWriter(fs.atomic_writer, self.counts)

    def reset(self) -> None:
        for key in self.counts:
            self.counts[key] = 0

    def _bump(self, name: str) -> None:
        self.counts[name] = self.counts.get(name, 0) + 1

    def ls(self, uri: StorageUri, detail: bool = False) -> list[Any]:
        self._bump("ls")
        return self._fs.ls(uri, detail=detail)

    def exists(self, uri: StorageUri) -> bool:
        self._bump("exists")
        return self._fs.exists(uri)

    def open(self, uri: StorageUri, mode: str = "rb") -> Any:
        self._bump("open")
        return self._fs.open(uri, mode)

    def read_bytes(self, uri: StorageUri) -> bytes:
        return self._fs.read_bytes(uri)

    def find(self, uri: StorageUri) -> list[StorageUri]:
        return self._fs.find(uri)

    def isdir(self, uri: StorageUri) -> bool:
        return self._fs.isdir(uri)

    def rm(self, uri: StorageUri, recursive: bool = False) -> None:
        self._bump("rm")
        self._fs.rm(uri, recursive=recursive)

    def put(self, src_uri: StorageUri, dst_uri: StorageUri) -> None:
        self._fs.put(src_uri, dst_uri)

    def info(self, uri: StorageUri) -> dict[str, Any]:
        return self._fs.info(uri)

    def capabilities(self) -> set[type]:
        return self._fs.capabilities()

    def makedirs(self, uri: StorageUri, exist_ok: bool = True) -> None:
        self._fs.makedirs(uri, exist_ok=exist_ok)

    def multipart_upload(
        self,
        local_path: str,
        remote_path: str,
        *,
        part_size: int = 64 * 1024 * 1024,
    ) -> None:
        self._fs.multipart_upload(local_path, remote_path, part_size=part_size)

    @property
    def atomic_writer(self) -> AtomicWriter:
        if self._atomic_writer is None:
            raise AttributeError("atomic_writer")
        return self._atomic_writer

    def __getattr__(self, name: str) -> Any:
        return getattr(self._fs, name)


def make_counting_local_fs(tmp_path: Path) -> tuple[CountingFilesystem, StorageFilesystemFull]:
    root_uri = StorageUri.from_local_path(tmp_path)
    binding = StorageBinding(
        identity=ProductIdentity.from_uri(root_uri, "zarr", product_name="counting_fs"),
        driver=StorageDriverConfig(driver="fsspec"),
    )
    real_fs = create_filesystem(binding)
    return CountingFilesystem(real_fs), real_fs
