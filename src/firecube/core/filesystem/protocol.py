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

from collections.abc import Callable, Mapping
from dataclasses import dataclass
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

    def replace_atomic(self, uri: StorageUri, data: bytes) -> None:
        """Atomic overwrite-or-create. A concurrent reader MUST observe either
        the previous content or the new content in full — never a truncated,
        empty, or partially-written object.

        Implementations MUST use backend-native atomicity primitives (e.g. a
        temp file published via rename locally, a single whole-body PUT on
        object stores). They MUST NOT truncate the destination in place.
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
class AtomicPublish(Protocol):
    """Whole-object publication capability for storage drivers."""

    def publish_atomic(self, dst_uri: StorageUri, write: Callable[[StorageUri], None]) -> None:
        """Run ``write`` so a concurrent reader of ``dst_uri`` sees the old or the new object.

        ``write`` receives the URI to write to and must write the complete
        object there. For a local (``file``) target the driver hands it a
        hidden reserved temporary beside ``dst_uri`` and renames that onto
        ``dst_uri`` afterwards, keeping an existing regular file's permission
        bits; the temporary is removed if ``write`` or the rename fails. For a
        remote target the store already publishes a PUT or completed multipart
        upload whole, so ``write`` receives ``dst_uri`` itself.
        """
        ...


class PreconditionFailed(Exception):
    """A conditional write or delete found its object changed or missing, and changed nothing.

    Attributes:
        uri: The object the operation named, as a string.
        detail: What the driver or the store reported.
    """

    def __init__(self, uri: str, detail: str) -> None:
        super().__init__(f"{uri}: {detail}")
        self.uri = uri
        self.detail = detail

    def __reduce__(self) -> tuple[type[PreconditionFailed], tuple[str, str]]:
        return (type(self), (self.uri, self.detail))


@runtime_checkable
class ConditionalWrite(Protocol):
    """Compare-and-swap of a whole small object against the validator of an earlier read.

    A validator is an opaque string naming one stored generation of an object.
    Compare it only with validators from the same driver and store.
    A refusal can follow a write of the same call that committed before its response was lost.
    The caller re-reads the object to tell the two apart.
    """

    def read_snapshot(self, uri: StorageUri) -> tuple[bytes, str]:
        """Return the object's bytes and their validator, both from one read.

        Raises:
            FileNotFoundError: No object exists at ``uri``.
        """
        ...

    def create_if_absent(self, uri: StorageUri, data: bytes) -> str:
        """Create the object at ``uri`` with ``data`` only when none exists, and return its validator.

        Raises:
            FileExistsError: An object already exists at ``uri``.
        """
        ...

    def compare_replace(self, uri: StorageUri, validator: str, data: bytes) -> str:
        """Replace the object with ``data`` only while its validator is ``validator``.

        Returns the validator of the new object. The call never creates an object.

        Raises:
            PreconditionFailed: The object changed since ``validator`` was read, or is missing.
        """
        ...


def unquoted_etag(validator: str) -> str:
    """Return ``validator`` without one outer pair of double quotes.

    An S3 ``If-Match`` header carries this form: Ceph RGW refuses a quoted ETag there.
    """
    if len(validator) >= 2 and validator.startswith('"') and validator.endswith('"'):
        return validator[1:-1]
    return validator


@runtime_checkable
class CompareDelete(Protocol):
    """Deletion of an object only while it is the generation an earlier read saw.

    Only local bindings report it. Ceph RGW ignores ``If-Match`` on ``DeleteObject``, so
    an S3 delete cannot be made conditional on every supported store. S3 claims are
    released by a conditional replace to a released record instead.
    """

    def compare_delete(self, uri: StorageUri, validator: str) -> None:
        """Delete the object at ``uri`` only while its validator is ``validator``.

        Raises:
            PreconditionFailed: The object changed since ``validator`` was read, or is missing.
        """
        ...


@runtime_checkable
class RangedRead(Protocol):
    """Byte-range read capability for storage drivers."""

    def read_range(self, path: str, start: int, end: int) -> bytes: ...


@dataclass(frozen=True, slots=True)
class ObjectVersion:
    """One version or delete marker of an object in a versioned bucket.

    Attributes:
        key: The object key in the bucket, as the store lists it, without a leading slash.
        version_id: The version id the store reports, or None when it reports none.
        is_latest: True for the current version, or None when the store does not say.
        is_delete_marker: True when the entry is a delete marker, not object bytes.
        size: The object size in bytes, or None for a delete marker.
        etag: The ETag as the store returns it, quotes included, or None for a delete marker.
    """

    key: str
    version_id: str | None
    is_latest: bool | None
    is_delete_marker: bool
    size: int | None
    etag: str | None


@runtime_checkable
class ObjectVersions(Protocol):
    """Object version capability for storage drivers on a versioned bucket.

    A driver reports it from `capabilities()` only when it can serve it.
    The methods exist on a driver that does not report it, and raise there.
    """

    def bucket_versioning(self, uri: StorageUri) -> str:
        """Return the versioning state of the bucket of ``uri``.

        Returns:
            ``"enabled"``, ``"suspended"``, or ``"absent"`` for a bucket that never had versioning.
        """
        ...

    def list_versions(self, prefix_uri: StorageUri) -> list[ObjectVersion]:
        """List every version and every delete marker of the objects below ``prefix_uri``.

        ``prefix_uri`` names a directory: its path is followed by a slash in the listing.
        The listing follows every page until the store says it is complete.
        """
        ...

    def head_version(self, uri: StorageUri, version_id: str) -> ObjectVersion | None:
        """Return version ``version_id`` of the object at ``uri``, or None when the store has none.

        A version that is a delete marker returns an entry with ``is_delete_marker`` true.
        """
        ...

    def lifecycle(self, uri: StorageUri) -> Mapping[str, Any] | None:
        """Return the lifecycle configuration of the bucket of ``uri`` as JSON-ready values.

        Returns:
            ``{"Rules": [...]}``, ``{}`` when the bucket has no configuration,
            or None when the store denies the read.
        """
        ...


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
