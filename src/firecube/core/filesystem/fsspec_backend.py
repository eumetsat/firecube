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
import datetime
import errno
import logging
import os
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

from firecube.core.filesystem import publication
from firecube.core.filesystem.protocol import (
    AtomicPublish,
    AtomicWriter,
    Multipart,
    MultipartUploader,
    ObjectVersion,
    ObjectVersions,
)
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.uri import StorageUri

log = logging.getLogger(__name__)

# S3 returns at most 1000 entries per ListObjectVersions page.
LIST_VERSIONS_PAGE_SIZE = 1000

_BUCKET_VERSIONING_STATES = {"Enabled": "enabled", "Suspended": "suspended"}


def _is_listed_publication_temp(entry: Any) -> bool:
    name = entry.get("name", "") if isinstance(entry, dict) else entry
    return publication.is_publication_temp(str(name))


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


def _error_responses(exc: BaseException) -> list[dict[str, Any]]:
    """Return the botocore response dicts in the cause chain of ``exc``, outermost first."""
    responses: list[dict[str, Any]] = []
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        response = getattr(current, "response", None)
        if isinstance(response, dict):
            responses.append(response)
        current = current.__cause__ or current.__context__
    return responses


def _has_error_code(exc: BaseException, code: str) -> bool:
    """Return True when a botocore response in the cause chain of ``exc`` carries ``code``."""
    return any(response.get("Error", {}).get("Code") == code for response in _error_responses(exc))


def _delete_marker_headers(exc: BaseException) -> dict[str, Any] | None:
    """Return the headers of the first error response in the cause chain of ``exc`` that marks a delete marker."""
    for response in _error_responses(exc):
        headers = response.get("ResponseMetadata", {}).get("HTTPHeaders", {})
        if str(headers.get("x-amz-delete-marker", "")).lower() == "true":
            return headers
    return None


def _json_ready(value: Any) -> Any:
    """Return ``value`` with every datetime written as an ISO 8601 string."""
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    if isinstance(value, datetime.date):
        return value.isoformat()
    return value


def _listed_version(entry: dict[str, Any], *, is_delete_marker: bool) -> ObjectVersion:
    """Build an `ObjectVersion` from one ``Versions`` or ``DeleteMarkers`` entry of a listing."""
    size = entry.get("Size")
    return ObjectVersion(
        key=entry["Key"],
        version_id=entry.get("VersionId"),
        is_latest=entry.get("IsLatest"),
        is_delete_marker=is_delete_marker,
        size=None if is_delete_marker or size is None else int(size),
        etag=None if is_delete_marker else entry.get("ETag"),
    )


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

    Per the `AtomicWriter` contract, backend-specific "already exists" signals
    are normalized to ``FileExistsError``: local raises ``errno EEXIST``;
    s3fs wraps the 412 as
    ``OSError(EINVAL)`` with a botocore ``ClientError`` cause (matched by
    `_is_precondition_failed`). Without this translation the control-plane
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

    def replace_atomic(self, uri: StorageUri, data: bytes) -> None:
        """Atomic overwrite-or-create (see `AtomicWriter`).

        - Local filesystem: write a sibling temp file, fsync it, then
          ``os.replace`` it into place — the rename publishes the fully-written
          inode over the old one in a single step, so a concurrent reader sees
          the old or the new content, never a truncated file. A plain
          ``open(path, "w")`` would truncate in place and expose a 0-byte
          window (observed as ``ControlPlaneCorruptionError`` "Expecting
          value: ... char 0" when peer pods list runs during a meta write).
        - Remote object stores: a buffered ``"wb"`` write emits one whole-body
          ``PutObject`` on close, which the store publishes atomically.
        """
        path = self._to_path(uri)
        if self._is_local():
            directory = os.path.dirname(path) or "."
            fd, tmp = tempfile.mkstemp(dir=directory, prefix=".firecube-atomic-", suffix=".tmp")
            try:
                with os.fdopen(fd, "wb") as fh:
                    fh.write(data)
                    fh.flush()
                    os.fsync(fh.fileno())
                os.replace(tmp, path)
            finally:
                with contextlib.suppress(OSError):
                    os.unlink(tmp)
            return
        with self._fs.open(path, "wb") as fh:
            fh.write(data)

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
            return StorageUri.from_local_path(Path("/" + path.lstrip("/")))
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

    def ls(self, uri: StorageUri, detail: bool = False, refresh: bool = False) -> list:
        # Only forward the keyword when asked: every fsspec filesystem accepts it
        # (s3fs answers from a dircache otherwise), but a plain call stays as it was.
        kwargs = {"refresh": True} if refresh else {}
        entries = self._fs.ls(self._to_path(uri), detail=detail, **kwargs)
        return [entry for entry in entries if not _is_listed_publication_temp(entry)]

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
        return [
            self._from_path(path)
            for path in self._fs.find(self._to_path(uri))
            if not publication.is_publication_temp(str(path))
        ]

    def info(self, uri: StorageUri) -> dict:
        return self._fs.info(self._to_path(uri))

    def publish_atomic(self, dst_uri: StorageUri, write: Callable[[StorageUri], None]) -> None:
        publication.publish_atomic(dst_uri, write)

    def bucket_versioning(self, uri: StorageUri) -> str:
        """Return ``"enabled"``, ``"suspended"`` or ``"absent"`` for the bucket of ``uri``.

        Raises:
            ValueError: The binding or ``uri`` is not s3, or the store reports an unknown state.
        """
        bucket, _key = self._s3_bucket_and_key(uri)
        response = self._fs.call_s3("get_bucket_versioning", Bucket=bucket)
        status = response.get("Status")
        if not status:
            return "absent"
        if status not in _BUCKET_VERSIONING_STATES:
            raise ValueError(f"bucket {bucket!r} reports unknown versioning status {status!r}")
        return _BUCKET_VERSIONING_STATES[status]

    def list_versions(self, prefix_uri: StorageUri) -> list[ObjectVersion]:
        """List every version and delete marker below ``prefix_uri``, page by page.

        Entries keep the store's order within a page, versions before delete markers.

        Raises:
            ValueError: The binding or ``prefix_uri`` is not s3.
            OSError: A truncated page names no next markers, or repeats earlier ones.
        """
        bucket, key = self._s3_bucket_and_key(prefix_uri)
        params: dict[str, Any] = {
            "Bucket": bucket,
            "Prefix": f"{key}/" if key else "",
            "MaxKeys": LIST_VERSIONS_PAGE_SIZE,
        }
        entries: list[ObjectVersion] = []
        seen_markers: set[tuple[str, str]] = set()
        while True:
            page = self._fs.call_s3("list_object_versions", **params)
            entries.extend(
                _listed_version(entry, is_delete_marker=False) for entry in page.get("Versions", [])
            )
            entries.extend(
                _listed_version(entry, is_delete_marker=True)
                for entry in page.get("DeleteMarkers", [])
            )
            if not page.get("IsTruncated"):
                return entries
            key_marker = page.get("NextKeyMarker")
            version_marker = page.get("NextVersionIdMarker")
            if not key_marker or not version_marker:
                raise OSError(
                    f"version listing of s3://{bucket}/{params['Prefix']} is truncated "
                    "without a next key and version marker; the listing is incomplete"
                )
            if (key_marker, version_marker) in seen_markers:
                raise OSError(
                    f"version listing of s3://{bucket}/{params['Prefix']} repeats the markers "
                    f"{key_marker!r}, {version_marker!r}; the listing is incomplete"
                )
            seen_markers.add((key_marker, version_marker))
            params["KeyMarker"] = key_marker
            params["VersionIdMarker"] = version_marker

    def head_version(self, uri: StorageUri, version_id: str) -> ObjectVersion | None:
        """Return version ``version_id`` of the object at ``uri``, or None when the store has none.

        Raises:
            ValueError: The binding or ``uri`` is not s3, or ``uri`` names no object.
            OSError: The store answers for a version other than ``version_id``.
        """
        bucket, key = self._s3_bucket_and_key(uri)
        if not key:
            raise ValueError(f"{uri} names a bucket, not an object")
        try:
            response = self._fs.call_s3("head_object", Bucket=bucket, Key=key, VersionId=version_id)
        except FileNotFoundError:
            return None
        except OSError as exc:
            headers = _delete_marker_headers(exc)
            if headers is None:
                raise
            answered = headers.get("x-amz-version-id")
            if answered != version_id:
                raise OSError(
                    f"s3://{bucket}/{key}: the store answered for delete marker {answered!r} "
                    f"when asked for {version_id!r}"
                ) from exc
            return ObjectVersion(
                key=key,
                version_id=version_id,
                is_latest=None,
                is_delete_marker=True,
                size=None,
                etag=None,
            )
        answered = response.get("VersionId")
        if answered != version_id:
            raise OSError(
                f"s3://{bucket}/{key}: the store answered for version {answered!r} "
                f"when asked for {version_id!r}"
            )
        size = response.get("ContentLength")
        return ObjectVersion(
            key=key,
            version_id=version_id,
            is_latest=None,
            is_delete_marker=bool(response.get("DeleteMarker", False)),
            size=None if size is None else int(size),
            etag=response.get("ETag"),
        )

    def lifecycle(self, uri: StorageUri) -> dict[str, Any] | None:
        """Return ``{"Rules": [...]}`` for the bucket of ``uri``, ``{}`` without rules, None if denied.

        Raises:
            ValueError: The binding or ``uri`` is not s3.
        """
        bucket, _key = self._s3_bucket_and_key(uri)
        try:
            response = self._fs.call_s3("get_bucket_lifecycle_configuration", Bucket=bucket)
        except PermissionError:
            return None
        except FileNotFoundError as exc:
            if _has_error_code(exc, "NoSuchLifecycleConfiguration"):
                return {}
            raise
        return {"Rules": _json_ready(response.get("Rules", []))}

    def _s3_bucket_and_key(self, uri: StorageUri) -> tuple[str, str]:
        """Return the bucket and the object key of ``uri`` for an s3-bound filesystem."""
        if self._binding.identity.product_uri.protocol != "s3" or uri.protocol != "s3":
            raise ValueError(f"object versions need an s3 binding and an s3 URI, got {uri}")
        assert uri.authority is not None
        return uri.authority, uri.path.strip("/")

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
        capabilities: set[type] = {Multipart, AtomicPublish}
        # The docs macro asks an instance that has no binding for the driver's own
        # capabilities; such an instance serves no product, so it adds none.
        binding = getattr(self, "_binding", None)
        if binding is not None and binding.identity.product_uri.protocol == "s3":
            capabilities.add(ObjectVersions)
        return capabilities

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
    if driver.anonymous:
        kwargs["anon"] = True
        if credentials is not None and any(
            value is not None
            for value in (
                credentials.access_key,
                credentials.secret_key,
                credentials.session_token,
            )
        ):
            log.warning("S3 anonymous access requested; credentials dropped because anonymous wins")
        return kwargs

    if credentials is not None:
        if credentials.access_key is not None:
            kwargs["key"] = credentials.access_key
        if credentials.secret_key is not None:
            kwargs["secret"] = credentials.secret_key
        if credentials.session_token is not None:
            kwargs["token"] = credentials.session_token

    return kwargs
