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

"""ObstoreFilesystem — StorageFilesystem implementation wrapping obstore."""

# pyright: reportMissingImports=false, reportArgumentType=false

from __future__ import annotations

import io
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urlparse

from firecube.core.filesystem.protocol import AtomicWriter, Multipart, MultipartUploader, RangedRead
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.uri import StorageUri

if TYPE_CHECKING:
    from obstore.store import S3Config


class ObstoreFilesystem:
    """Adapts obstore store primitives to the StorageFilesystem Protocol."""

    def __setattr__(self, name: str, value: Any) -> None:
        object.__setattr__(self, name, value)
        if name != "_store":
            return
        atomic_writer = getattr(self, "_atomic_writer", None)
        if isinstance(atomic_writer, ObstoreAtomicWriter):
            atomic_writer._store = value
        multipart_uploader = getattr(self, "_multipart_uploader", None)
        if isinstance(multipart_uploader, ObstoreMultipartUploader):
            multipart_uploader._store = value

    def __init__(
        self,
        binding: StorageBinding,
        *,
        atomic_writer: AtomicWriter | None = None,
        multipart_uploader: MultipartUploader | None = None,
    ) -> None:
        self._binding = binding
        self._store = _obstore_store_from_binding(binding)
        self._store_prefix = _store_prefix_for(binding)
        # Wire write-coordination helpers at construction time. Defaulting them
        # to the real implementations (rather than None) keeps the object fully
        # usable the instant it exists: capabilities() can never advertise a
        # capability the object cannot actually serve. The keyword args remain a
        # dependency-injection seam for tests/fakes.
        self._atomic_writer: AtomicWriter = (
            atomic_writer
            if atomic_writer is not None
            else ObstoreAtomicWriter(self._store, self._store_prefix)
        )
        self._multipart_uploader: MultipartUploader = (
            multipart_uploader
            if multipart_uploader is not None
            else ObstoreMultipartUploader(self._store, self._store_prefix)
        )

    def _resolve_path(self, uri: StorageUri) -> str:
        """Return the store-relative path for a StorageUri.

        Remote stores (s3/gs) are constructed with a non-empty ``prefix`` equal
        to the bound product's path component; obstore prepends that prefix to
        every operation, so paths passed to ``store.put``/``head``/etc must be
        *relative* to it. Local stores use prefix ``"/"`` and accept absolute
        paths verbatim.
        """
        if uri.authority is not None:
            full = uri.path.lstrip("/")
            if not self._store_prefix:
                return full
            if full == self._store_prefix:
                return ""
            if full.startswith(self._store_prefix + "/"):
                return full[len(self._store_prefix) + 1 :]
            return full
        return uri.path

    @staticmethod
    def _test_resolve_path(uri: StorageUri, store_prefix: str = "") -> str:
        if uri.authority is not None:
            full = uri.path.lstrip("/")
            if not store_prefix:
                return full
            if full == store_prefix:
                return ""
            if full.startswith(store_prefix + "/"):
                return full[len(store_prefix) + 1 :]
            return full
        return uri.path

    def _from_path(self, path: str) -> StorageUri:
        base = self._binding.identity.product_uri
        if base.authority is None:
            return StorageUri.from_local_path(Path("/" + path.lstrip("/")))
        full = self._absolute_key(path)
        return StorageUri(protocol=base.protocol, authority=base.authority, path=full)

    def _absolute_key(self, path: str) -> str:
        """Combine the store's prefix with a list/iter-relative path."""
        rel = path.lstrip("/")
        if not self._store_prefix:
            return rel
        if rel == self._store_prefix or rel.startswith(self._store_prefix + "/"):
            return rel
        return f"{self._store_prefix}/{rel}".strip("/") if rel else self._store_prefix

    @classmethod
    def from_local(cls, root_path: str) -> ObstoreFilesystem:
        from firecube.core.config import StorageConfig
        from firecube.core.product.identity import ProductIdentity

        uri = StorageUri.from_local_path(Path(root_path).resolve())
        synthesized_config = StorageConfig(storage_type="local", storage_driver="obstore")
        binding = StorageBinding(
            identity=ProductIdentity.from_uri(uri, "zarr", product_name=str(root_path)),
            driver=StorageDriverConfig.from_storage_config_or_default(synthesized_config),
        )
        return cls(binding)

    @classmethod
    def from_binding(cls, binding: StorageBinding) -> ObstoreFilesystem:
        return cls(binding)

    def open(self, uri: StorageUri, mode: str = "rb", **kwargs: Any) -> Any:
        encoding = kwargs.get("encoding", "utf-8")
        rel_path = self._resolve_path(uri)
        is_text = "b" not in mode

        if "r" in mode:
            result = self._store.get(rel_path)
            data = bytes(result.bytes())
            buf = io.BytesIO(data)
            if is_text:
                return io.TextIOWrapper(buf, encoding=encoding)
            return buf

        if "x" in mode and self.exists(uri):
            raise FileExistsError(uri.to_str())

        write_buf = StreamingObstoreWriteBuffer(self._store, rel_path)
        if is_text:
            return io.TextIOWrapper(write_buf, encoding=encoding)
        return write_buf

    def read_bytes(self, uri: StorageUri) -> bytes:
        # Single-shot GET; obstore `get().bytes()` materializes the whole object
        # in one fetch with no conditional/range-cached read, so concurrent
        # metadata mutation cannot fail it (matches the protocol contract).
        rel_path = self._resolve_path(uri)
        return bytes(self._store.get(rel_path).bytes())

    def exists(self, uri: StorageUri) -> bool:
        rel_path = self._resolve_path(uri)
        try:
            self._store.head(rel_path)
            return True
        except Exception:
            return self.isdir(uri)

    def ls(self, uri: StorageUri, detail: bool = False) -> list:
        rel_path = self._resolve_path(uri).rstrip("/")
        prefix = rel_path + "/" if rel_path else ""
        result = self._store.list_with_delimiter(prefix=prefix)
        objects = result.get("objects", [])
        prefixes = result.get("common_prefixes", [])
        if detail:
            items: list[dict[str, Any]] = [
                {
                    "name": self._absolute_key(obj["path"]),
                    "size": obj.get("size", 0),
                    "type": "file",
                }
                for obj in objects
            ]
            items.extend(
                {"name": self._absolute_key(p), "size": 0, "type": "directory"} for p in prefixes
            )
            return items
        names = [self._absolute_key(obj["path"]) for obj in objects]
        names.extend(self._absolute_key(p) for p in prefixes)
        return names

    def isdir(self, uri: StorageUri) -> bool:
        """Check if any objects exist under this prefix."""
        rel_path = self._resolve_path(uri).rstrip("/")
        prefix = rel_path + "/" if rel_path else ""
        return any(batch for batch in self._store.list(prefix=prefix))

    def makedirs(self, uri: StorageUri, exist_ok: bool = True) -> None:
        """No-op for object stores (directories are implicit)."""

    def rm(self, uri: StorageUri, recursive: bool = False) -> None:
        rel_path = self._resolve_path(uri)
        if recursive:
            prefix = rel_path.rstrip("/") + "/"
            for batch in self._store.list(prefix=prefix):
                for item in batch:
                    self._store.delete(item["path"])
        else:
            self._store.delete(rel_path)

    def put(self, src_uri: StorageUri, dst_uri: StorageUri) -> None:
        self._require_multipart_uploader().upload(src_uri.path, dst_uri)

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

    def read_range(self, uri: StorageUri, start: int, end: int) -> bytes:
        rel_path = self._resolve_path(uri)
        return bytes(self._store.get_range(rel_path, start=start, end=end))

    def find(self, uri: StorageUri) -> list[StorageUri]:
        rel_path = self._resolve_path(uri).rstrip("/")
        prefix = rel_path + "/" if rel_path else ""
        paths: list[StorageUri] = []
        for batch in self._store.list(prefix=prefix):
            paths.extend(self._from_path(item["path"]) for item in batch)
        return paths

    def info(self, uri: StorageUri) -> dict:
        rel_path = self._resolve_path(uri)
        meta = self._store.head(rel_path)
        return {
            "name": self._absolute_key(meta["path"]),
            "size": meta["size"],
            "type": "file",
        }

    def capabilities(self) -> set[type]:
        return {Multipart, RangedRead}

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


class ObstoreMultipartUploader:
    """Streaming multipart uploader for obstore.

    The local file is opened as a binary file handle and passed directly to
    obstore. This keeps Python memory bounded and lets obstore perform the
    multipart upload without first materializing the object as ``bytes``.
    """

    def __init__(self, store: Any, store_prefix: str = "") -> None:
        self._store = store
        self._store_prefix = store_prefix

    def upload(
        self,
        local_path: str,
        remote_uri: StorageUri,
        *,
        part_size: int = 64 * 1024 * 1024,
    ) -> None:
        rel_path = ObstoreFilesystem._test_resolve_path(remote_uri, self._store_prefix)
        chunk_size = min(part_size, 8 * 1024 * 1024)
        self._store.put(
            rel_path,
            Path(local_path),
            use_multipart=True,
            chunk_size=chunk_size,
            max_concurrency=1,
        )


class ObstoreAtomicWriter:
    """Atomic create-if-not-exists using obstore's PutMode.Create.

    The backend guarantees no two callers can both succeed for the same path.
    PutMode.Create maps to native create-only/conditional PUT semantics.
    """

    def __init__(self, store: Any, store_prefix: str = "") -> None:
        self._store = store
        self._store_prefix = store_prefix

    def write_atomic(self, uri: StorageUri, data: bytes) -> None:
        rel_path = ObstoreFilesystem._test_resolve_path(uri, self._store_prefix)
        from firecube.core.filesystem import _obstore_compat

        try:
            self._store.put(rel_path, data, mode=_obstore_compat.PutMode.Create)
        except _obstore_compat.AlreadyExistsError as exc:
            raise FileExistsError(uri.to_str()) from exc

    def replace_atomic(self, uri: StorageUri, data: bytes) -> None:
        """Atomic overwrite-or-create (see `AtomicWriter`).

        Default-mode ``put`` is atomic on every obstore backend: object stores
        publish a whole-body PUT all-or-nothing, and the local backend stages
        to a temp file and renames it into place.
        """
        rel_path = ObstoreFilesystem._test_resolve_path(uri, self._store_prefix)
        self._store.put(rel_path, data)


def _store_prefix_for(binding: StorageBinding) -> str:
    uri = binding.identity.product_uri
    if uri.protocol == "file":
        return ""
    return uri.path.lstrip("/").rstrip("/")


def _obstore_store_from_binding(binding: StorageBinding) -> Any:
    from firecube.core.filesystem import _obstore_compat

    _obstore_compat.require_obstore()

    uri = binding.identity.product_uri
    if uri.protocol == "file":
        return _obstore_compat.LocalStore(prefix="/", mkdir=True)

    return _obstore_compat.S3Store(
        bucket=uri.authority,
        prefix=_store_prefix_for(binding) or None,
        config=cast("S3Config", _aws_config_from_driver(binding.driver)),
    )


def _aws_config_from_driver(driver: StorageDriverConfig) -> dict[str, Any]:
    config: dict[str, Any] = {}
    credentials = driver.credentials
    if credentials is not None:
        if credentials.access_key:
            config["aws_access_key_id"] = credentials.access_key
        if credentials.secret_key:
            config["aws_secret_access_key"] = credentials.secret_key
        if credentials.session_token:
            config["aws_session_token"] = credentials.session_token
    if driver.endpoint_url:
        config["aws_endpoint"] = driver.endpoint_url
        if urlparse(driver.endpoint_url).scheme == "http":
            config["aws_allow_http"] = "true"
    if driver.region:
        config["aws_region"] = driver.region
    if driver.path_style:
        config["aws_virtual_hosted_style_request"] = "false"
    return config


class StreamingObstoreWriteBuffer(io.RawIOBase):
    """Small-write buffer that uploads accumulated bytes on close.

    ``ObstoreFilesystem.open(..., "wb")`` is used for zarr/control-plane style
    small object writes. Large local-file transfers must go through
    ``ObstoreMultipartUploader``, which streams from a file handle.
    """

    def __init__(self, store: Any, path: str, chunk_size: int = 64 * 1024 * 1024) -> None:
        super().__init__()
        self._store = store
        self._path = path
        self._chunk_size = chunk_size
        self.name = path
        self._buf = io.BytesIO()

    def write(self, b: bytes | bytearray) -> int:  # type: ignore[override]
        return self._buf.write(b)

    def writable(self) -> bool:
        return True

    def readable(self) -> bool:
        return False

    def seekable(self) -> bool:
        return False

    def close(self) -> None:
        if not self.closed:
            data = self._buf.getvalue()
            self._store.put(self._path, data)
        super().close()

    def __enter__(self) -> StreamingObstoreWriteBuffer:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
