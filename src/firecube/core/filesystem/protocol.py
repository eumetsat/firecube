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

"""Internal storage write-coordination protocols.

Not part of firecube.core.api or firecube.ingestor.api.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from firecube.core.storage.uri import StorageUri


@runtime_checkable
class StorageFilesystem(Protocol):
    def exists(self, uri: StorageUri) -> bool: ...

    def open(self, uri: StorageUri, mode: str = "rb") -> Any: ...

    def read_bytes(self, uri: StorageUri) -> bytes:
        """Read an object's full contents in a single shot.

        Intended for small metadata objects (e.g. ``zarr.json``).
        Implementations MUST issue a plain single GET and MUST NOT use a
        range-cached or conditional (``If-Match``/``If-None-Match``) fetch.
        Conditional fetches make a concurrent read fail when the object is
        mutated mid-read: s3fs's cached ``open().read()`` adds ``If-Match`` and
        raises a 412 ``PreconditionFailed`` (surfacing as ``OSError(EINVAL)``)
        when another writer changes the object's ETag. Object-store ``PutObject``
        atomicity guarantees a single GET returns a complete old-or-new object,
        never a partial one, so callers reading immutable-once-written metadata
        get a consistent result without the conditional-read hazard.
        """
        ...

    def find(self, uri: StorageUri) -> list[StorageUri]: ...

    def isdir(self, uri: StorageUri) -> bool: ...

    def rm(self, uri: StorageUri, recursive: bool = False) -> None: ...

    def put(self, src_uri: StorageUri, dst_uri: StorageUri) -> None: ...

    def info(self, uri: StorageUri) -> dict[str, Any]: ...

    def capabilities(self) -> set[type]: ...


@runtime_checkable
class AtomicWriter(Protocol):
    def write_atomic(self, uri: StorageUri, data: bytes) -> None:
        """Atomic create-if-not-exists. Raises FileExistsError if uri already exists.

        Implementations MUST use backend-native atomicity primitives
        (e.g. obstore `PutMode.Create`, local `O_EXCL`).
        They MUST NOT implement this as `exists()` followed by `put()`.
        """
        ...


@runtime_checkable
class MultipartUploader(Protocol):
    def upload(
        self,
        local_path: str,
        remote_uri: StorageUri,
        *,
        part_size: int = 64 * 1024 * 1024,
    ) -> None:
        """Streaming upload. Implementations MUST NOT load the full file into memory.

        Forbidden patterns:
          - Path(local_path).read_bytes()
          - io.BytesIO(file.read()) holding the whole file

        Implementations should use:
          - obstore: store.put(path, file_handle, use_multipart=True, chunk_size=part_size)
          - fsspec: fs.put_file(local_path, remote_path)
        """
        ...


@runtime_checkable
class Multipart(Protocol):
    """Large-file upload capability for storage drivers."""

    def multipart_upload(
        self,
        local_path: str,
        remote_path: str,
        *,
        part_size: int = 64 * 1024 * 1024,
    ) -> None: ...


@runtime_checkable
class RangedRead(Protocol):
    """Byte-range read capability for storage drivers."""

    def read_range(self, path: str, start: int, end: int) -> bytes: ...


@runtime_checkable
class Signer(Protocol):
    """Pre-signed URL generation capability for storage drivers."""

    def sign(self, path: str, *, expires_in: int = 3600) -> str: ...


@runtime_checkable
class StorageFilesystemFull(StorageFilesystem, Protocol):
    """Broader compatibility surface kept during the protocol-splitting migration."""

    @property
    def atomic_writer(self) -> AtomicWriter: ...

    def makedirs(self, uri: StorageUri, exist_ok: bool = True) -> None: ...
