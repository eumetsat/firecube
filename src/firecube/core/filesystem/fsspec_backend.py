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

from __future__ import annotations

import contextlib
import errno
import os
import tempfile
from typing import Any

from firecube.core.filesystem.protocol import AtomicWriter, Multipart, MultipartUploader
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.uri import StorageUri


def _is_precondition_failed(exc: BaseException) -> bool:
    """True if ``exc`` (or a cause in its chain) is an S3 412 PreconditionFailed.

    s3fs implements exclusive-create ("xb") as a conditional ``PutObject``
    (``If-None-Match: *``). When a concurrent writer already created the object
    the store returns HTTP 412 ``PreconditionFailed``, which s3fs surfaces as an
    ``OSError(EINVAL)`` whose ``__cause__`` is a botocore ``ClientError`` carrying
    that error code. EINVAL (22) is NOT EEXIST (17), so the plain errno check
    misses it; we duck-type on the botocore response dict instead, which keeps
    this module free of a hard botocore import.
    """
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        response = getattr(current, "response", None)
        if isinstance(response, dict):
            code = response.get("Error", {}).get("Code")
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if code in ("PreconditionFailed", "412") or status == 412:
                return True
        current = current.__cause__ or current.__context__
    return False


class FsspecAtomicWriter:
    """Atomic create-if-not-exists for fsspec backends.

    The publish must be atomic in BOTH existence and *content*: a concurrent
    reader must observe either no file or the fully-written file, never a
    zero-length file mid-write. This matters because losers in
    ``ChunkManager.ensure_slot_index_model`` read ``current.json`` *outside*
    the write claim the instant they observe ``ClaimConflictError``, i.e. while
    the winner is still inside its write.

    - S3 via s3fs: ``open(path, "xb")`` buffers the body and emits a single
      conditional ``PutObject`` (``If-None-Match: *``) on close. The object
      appears whole-or-not-at-all and a lost race returns HTTP 412
      PreconditionFailed, so existence and content are atomic together.
    - Local filesystem: ``open(path, "xb")`` is NOT content-atomic — O_EXCL
      makes the *name* appear atomically but the file is created empty and
      filled incrementally, leaving a window where a reader sees a 0-byte file
      (manifests as ``ManifestError`` "is not valid JSON: ... char 0"). We
      instead write a sibling temp file, fsync it, then ``os.link`` it into
      place: the link publishes the fully-written inode in one step and fails
      with ``FileExistsError`` if the target already exists, preserving the
      create-if-not-exists contract.

    Per the :class:`~firecube.core.filesystem.protocol.AtomicWriter` contract,
    backend-specific "already exists" signals are normalized to
    ``FileExistsError``: local raises ``errno EEXIST``; s3fs wraps the 412 as
    ``OSError(EINVAL)`` with a botocore ``ClientError`` cause (matched by
    :func:`_is_precondition_failed`). Without this translation the control-plane
    claim layer never observes ``FileExistsError`` on S3, so ``ClaimConflictError``
    is never raised and concurrent pods crash at startup instead of converging
    (see ``ChunkManager.ensure_slot_index_model``).
    """

    def __init__(self, fs: Any) -> None:
        self._fs = fs

    def write_atomic(self, uri: StorageUri, data: bytes) -> None:
        path = self._to_path(uri)
        if self._is_local():
            self._write_atomic_local(path, str(uri), data)
            return
        try:
            with self._fs.open(path, "xb") as fh:
                fh.write(data)
        except FileExistsError:
            raise
        except Exception as exc:
            if getattr(exc, "errno", None) == errno.EEXIST or _is_precondition_failed(exc):
                raise FileExistsError(str(uri)) from exc
            raise

    def _is_local(self) -> bool:
        proto = getattr(self._fs, "protocol", None)
        if isinstance(proto, (tuple, list)):
            return "file" in proto or "local" in proto
        return proto in ("file", "local")

    def _write_atomic_local(self, path: str, uri_str: str, data: bytes) -> None:
        """Content-atomic exclusive create for the local filesystem (see class docstring)."""
        directory = os.path.dirname(path) or "."
        fd, tmp = tempfile.mkstemp(dir=directory, prefix=".firecube-atomic-", suffix=".tmp")
        try:
            with os.fdopen(fd, "wb") as fh:
                fh.write(data)
                fh.flush()
                os.fsync(fh.fileno())
            try:
                os.link(tmp, path)
            except FileExistsError as exc:
                raise FileExistsError(uri_str) from exc
        finally:
            with contextlib.suppress(OSError):
                os.unlink(tmp)

    def _to_path(self, uri: StorageUri) -> str:
        """Convert StorageUri to fsspec-native path string."""
        if uri.protocol == "file":
            return uri.path
        return f"{uri.authority}{uri.path}"


class FsspecMultipartUploader:
    """Streaming upload for fsspec backends.

    Uses put_file which streams under the hood.
    """

    def __init__(self, fs: Any) -> None:
        self._fs = fs

    def upload(
        self,
        local_path: str,
        remote_uri: StorageUri,
        *,
        part_size: int = 64 * 1024 * 1024,
    ) -> None:
        remote_path = self._to_path(remote_uri)
        self._fs.put_file(local_path, remote_path)

    def _to_path(self, uri: StorageUri) -> str:
        """Convert StorageUri to fsspec-native path string."""
        if uri.protocol == "file":
            return uri.path
        return f"{uri.authority}{uri.path}"


class FsspecFilesystem:
    """Thin wrapper around an fsspec filesystem object implementing StorageFilesystem."""

    def __init__(
        self,
        binding: StorageBinding,
        *,
        atomic_writer: AtomicWriter | None = None,
        multipart_uploader: MultipartUploader | None = None,
    ) -> None:
        from firecube.core.filesystem.ops import _build_fsspec_filesystem

        self._binding = binding
        kwargs = _fsspec_kwargs_from_binding(binding)
        self._fs = _build_fsspec_filesystem(binding.identity.product_uri.protocol, kwargs)
        # Wire write-coordination helpers at construction time. Defaulting them
        # to the real implementations (rather than None) keeps the object fully
        # usable the instant it exists: capabilities() can never advertise a
        # capability the object cannot actually serve. The keyword args remain a
        # dependency-injection seam for tests/fakes.
        self._atomic_writer: AtomicWriter = (
            atomic_writer if atomic_writer is not None else FsspecAtomicWriter(self._fs)
        )
        self._multipart_uploader: MultipartUploader = (
            multipart_uploader
            if multipart_uploader is not None
            else FsspecMultipartUploader(self._fs)
        )

    def _to_path(self, uri: StorageUri) -> str:
        """Convert StorageUri to fsspec-native path string."""
        if uri.protocol == "file":
            return uri.path
        return f"{uri.authority}{uri.path}"

    def _from_path(self, path: str) -> StorageUri:
        if self._binding.identity.product_uri.protocol == "file":
            return StorageUri.parse(f"file://{path}")
        authority = self._binding.identity.product_uri.authority
        assert authority is not None
        object_path = str(path)
        if object_path.startswith(f"{authority}/"):
            object_path = object_path[len(authority) + 1 :]
        return StorageUri(
            protocol=self._binding.identity.product_uri.protocol,
            authority=authority,
            path=object_path,
        )

    def open(self, uri: StorageUri, mode: str = "rb") -> Any:
        return self._fs.open(self._to_path(uri), mode)

    def read_bytes(self, uri: StorageUri) -> bytes:
        # Single-shot GET via fsspec's `cat_file` — deliberately NOT
        # `open().read()`. On s3fs the latter takes the range-cached fetch that
        # adds an `If-Match` precondition and crashes with a 412 PreconditionFailed
        # when a concurrent writer changes the object's ETag mid-read (see the
        # AtomicWriter read_bytes contract in protocol.py). `cat_file` is a plain
        # GET with no precondition.
        return self._fs.cat_file(self._to_path(uri))

    def exists(self, uri: StorageUri) -> bool:
        return self._fs.exists(self._to_path(uri))

    def ls(self, uri: StorageUri, detail: bool = False) -> list:
        return self._fs.ls(self._to_path(uri), detail=detail)

    def isdir(self, uri: StorageUri) -> bool:
        return self._fs.isdir(self._to_path(uri))

    def makedirs(self, uri: StorageUri, exist_ok: bool = True) -> None:
        self._fs.makedirs(self._to_path(uri), exist_ok=exist_ok)

    def rm(self, uri: StorageUri, recursive: bool = False) -> None:
        self._fs.rm(self._to_path(uri), recursive=recursive)

    def put(self, src_uri: StorageUri, dst_uri: StorageUri) -> None:
        self._fs.put(self._to_path(src_uri), self._to_path(dst_uri))

    def multipart_upload(
        self,
        local_path: str,
        remote_path: str,
        *,
        part_size: int = 64 * 1024 * 1024,
    ) -> None:
        self._require_multipart_uploader().upload(
            local_path, StorageUri.parse(remote_path), part_size=part_size
        )

    def find(self, uri: StorageUri) -> list[StorageUri]:
        return [self._from_path(path) for path in self._fs.find(self._to_path(uri))]

    def info(self, uri: StorageUri) -> dict:
        return self._fs.info(self._to_path(uri))

    @property
    def atomic_writer(self) -> AtomicWriter:
        return self._atomic_writer

    @property
    def claim_writer(self) -> AtomicWriter:
        return self._atomic_writer

    @property
    def multipart_uploader(self) -> MultipartUploader:
        return self._multipart_uploader

    def _require_multipart_uploader(self) -> MultipartUploader:
        return self._multipart_uploader

    def capabilities(self) -> set[type]:
        return {Multipart}

    @classmethod
    def from_binding(cls, binding: StorageBinding) -> FsspecFilesystem:
        return cls(binding)


def _fsspec_kwargs_from_binding(binding: StorageBinding) -> dict[str, Any]:
    if binding.identity.product_uri.protocol != "s3":
        return {}

    driver = binding.driver
    kwargs: dict[str, Any] = {}
    client_kwargs: dict[str, Any] = {}
    config_kwargs: dict[str, Any] = {}

    if driver.endpoint_url:
        client_kwargs["endpoint_url"] = driver.endpoint_url
    if driver.region:
        client_kwargs["region_name"] = driver.region
    if client_kwargs:
        kwargs["client_kwargs"] = client_kwargs

    addressing_style = "path" if driver.path_style else "virtual"
    config_kwargs.setdefault("s3", {})["addressing_style"] = addressing_style
    kwargs["config_kwargs"] = config_kwargs

    credentials = driver.credentials
    if credentials is not None:
        if credentials.access_key is not None:
            kwargs["key"] = credentials.access_key
        if credentials.secret_key is not None:
            kwargs["secret"] = credentials.secret_key
        if credentials.session_token is not None:
            kwargs["token"] = credentials.session_token

    return kwargs
