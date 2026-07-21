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

import concurrent.futures
import contextvars
import json
import shutil
import time
from collections.abc import Iterator
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING, Any, SupportsIndex, cast

from firecube.core.config import StorageConfig
from firecube.core.duckdb.bridge import (
    _DUCKDB_OBSTORE_REMOTE_MESSAGE,
)
from firecube.core.errors import StorageError
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.results import StorageWriteResult
from firecube.core.storage.uri import StorageUri

if TYPE_CHECKING:
    from firecube.core.controlplane.manager import ChunkManager
    from firecube.core.filesystem import StorageFilesystem
    from firecube.core.product.identity import ProductIdentity
    from firecube.core.storage.driver_config import (
        StorageDriverConfig,
    )

__all__ = [
    "_DUCKDB_OBSTORE_REMOTE_MESSAGE",
    "StorageSession",
    "storage_config_from_binding",
]


def _storage_type_for_uri(uri: StorageUri) -> str:
    if uri.is_remote():
        return "s3"
    return "local"


def storage_config_from_binding(binding: StorageBinding) -> StorageConfig:
    """Compose a plain ``StorageConfig`` from a ``StorageBinding``.

    Returns a vanilla ``StorageConfig`` carrying only driver-relevant fields
    (``storage_type``, credentials, ``endpoint_url``, ``region``,
    ``path_style``, ``storage_driver``).  Location fields (``target_path``,
    ``bucket``, ``target_uri``) are intentionally absent — callers that need
    those should read from ``binding.identity`` (``ProductIdentity``)
    directly.

    This is the explicit boundary helper for lower-level APIs that still
    accept a ``StorageConfig`` (e.g. fsspec/obstore zarr-store factories and
    chunk-state helpers).  It replaces the removed ``_StorageConfigView``
    bridge subclass.
    """
    credentials = binding.driver.credentials
    storage_type = _storage_type_for_uri(binding.identity.product_uri)
    return StorageConfig(
        storage_type=storage_type,
        endpoint_url=binding.driver.endpoint_url,
        access_key=credentials.access_key if credentials is not None else None,
        secret_key=credentials.secret_key if credentials is not None else None,
        region=binding.driver.region,
        path_style=binding.driver.path_style,
        storage_driver=binding.driver.driver,
    )


def _storage_uri_from_path(*, protocol: str, path: str) -> StorageUri:
    raw_path = str(path)
    if "://" in raw_path:
        return StorageUri.parse(raw_path)
    if protocol == "file":
        return StorageUri.parse(raw_path)
    authority, _, object_path = raw_path.lstrip("/").partition("/")
    return StorageUri(protocol=protocol, authority=authority, path=object_path)


def create_filesystem(binding: StorageBinding) -> Any:
    from firecube.core.filesystem import create_filesystem as _create_filesystem

    return _create_filesystem(binding)


class StorageSession:
    def __init__(self, binding: StorageBinding) -> None:
        """Pure constructor. No I/O. Preflight is engine-level (T15)."""
        self._binding = binding
        self._filesystem: StorageFilesystem | None = None

    def __getstate__(self) -> object:
        raise TypeError("StorageSession is process-local and cannot be pickled.")

    def __reduce__(self) -> str | tuple[Any, ...]:
        raise TypeError("StorageSession is process-local and cannot be pickled.")

    def __reduce_ex__(self, protocol: SupportsIndex) -> str | tuple[Any, ...]:
        raise TypeError("StorageSession is process-local and cannot be pickled.")

    @property
    def product(self) -> ProductIdentity:
        return self._binding.identity

    @property
    def driver(self) -> StorageDriverConfig:
        return self._binding.driver

    @property
    def zarr(self) -> Any:
        return cast(Any, import_module("firecube.core.zarr.io")).ZarrIO(self)

    @property
    def duckdb(self) -> Any:
        return cast(Any, import_module("firecube.core.duckdb.io")).DuckDBIO(self)

    def _full_fs(self) -> Any:
        return cast(Any, self.fs())

    def fs(self) -> StorageFilesystem:
        from firecube.core.filesystem.instrumentation import (
            InstrumentedFilesystem,
            active_filesystem_metrics,
        )

        if self._filesystem is None:
            fs = create_filesystem(self._binding)
            # Cache the underlying (unwrapped) adapter so we can re-wrap dynamically.
            # create_filesystem may itself wrap if metrics were active at call time;
            # unwrap here to keep _filesystem as the canonical bare adapter.
            if isinstance(fs, InstrumentedFilesystem):
                fs = cast(Any, fs)._wrapped
            self._filesystem = fs

        # Apply instrumentation dynamically per-call: metrics state may have changed
        # since the cache was populated (e.g., preflight ran fs() before
        # collect_filesystem_metrics() started).
        if active_filesystem_metrics() is not None:
            return cast(Any, InstrumentedFilesystem(self._filesystem))
        return cast(Any, self._filesystem)

    def open(self, uri: StorageUri, mode: str) -> Any:
        return cast(Any, self.fs()).open(uri, mode)

    def exists(self, uri: StorageUri) -> bool:
        return cast(Any, self.fs()).exists(uri)

    def find(self, uri: StorageUri) -> Iterator[StorageUri]:
        for path in cast(Any, self.fs()).find(uri):
            if isinstance(path, StorageUri):
                yield path
            else:
                yield _storage_uri_from_path(protocol=uri.protocol, path=str(path))

    def delete(self, uri: StorageUri) -> None:
        cast(Any, self.fs()).rm(uri, recursive=True)

    def info(self, uri: StorageUri) -> dict[str, Any]:
        return cast(Any, self.fs()).info(uri)

    def isdir(self, uri: StorageUri) -> bool:
        return cast(Any, self.fs()).isdir(uri)

    def makedirs(self, uri: StorageUri, *, exist_ok: bool = True) -> None:
        self._full_fs().makedirs(uri, exist_ok=exist_ok)

    def ls(self, uri: StorageUri) -> Iterator[StorageUri]:
        for entry in self._full_fs().ls(uri):
            if isinstance(entry, StorageUri):
                yield entry
            else:
                yield _storage_uri_from_path(protocol=uri.protocol, path=str(entry))

    def put(
        self,
        local_path: Path,
        dst: StorageUri,
        *,
        multipart_threshold: int = 64 * 1024 * 1024,
    ) -> int:
        from firecube.core.filesystem.protocol import Multipart  # pyright: ignore[reportAttributeAccessIssue]  # noqa: I001

        file_size = local_path.stat().st_size
        fs = self.fs()
        if file_size > multipart_threshold and Multipart in cast(Any, fs).capabilities():
            cast(Any, fs).multipart_upload(str(local_path), dst.to_str())
            return file_size
        with local_path.open("rb") as src_handle, cast(Any, fs).open(dst, "wb") as dst_handle:
            shutil.copyfileobj(src_handle, dst_handle)
        return file_size

    def upload_tree(
        self,
        src: StorageUri,
        dst: StorageUri,
        *,
        preserve_zarr_metadata: bool = True,
        parallel_workers: int = 4,
        multipart_threshold: int = 64 * 1024 * 1024,
    ) -> StorageWriteResult:
        """Upload a local file or directory tree to storage.

        ``parallel_workers`` enables the same thread-pool upload path as the
        legacy S3 writer for directory sources with more than two files. Large
        files route through the filesystem ``Multipart`` capability when it is
        available; obstore accepts ``multipart_threshold`` but handles multipart
        details internally.
        """
        if src.is_remote():
            raise ValueError("upload_tree requires a local source")

        from firecube.core.uris import local_path_from_target

        source_path = local_path_from_target(src.to_str()).resolve()
        if not source_path.exists():
            raise ValueError(f"upload_tree source does not exist: {source_path}")
        if not source_path.is_file() and not source_path.is_dir():
            raise ValueError(f"upload_tree requires a file or directory source: {source_path}")

        dst_fs = self.fs()
        files = _upload_tree_files(source_path)
        start = time.time()
        bytes_written = 0
        files_written = 0

        try:
            if parallel_workers <= 1 or len(files) <= 2:
                for file_path in files:
                    written, uploaded = _upload_tree_upload_file(
                        dst_fs,
                        dst,
                        source_path,
                        file_path,
                        preserve_zarr_metadata=preserve_zarr_metadata,
                        multipart_threshold=multipart_threshold,
                    )
                    bytes_written += written
                    files_written += uploaded
            else:
                actual_workers = min(len(files), parallel_workers)
                first_error: Exception | None = None
                worker_ctx = contextvars.copy_context()
                executor = concurrent.futures.ThreadPoolExecutor(max_workers=actual_workers)
                futures = {
                    executor.submit(
                        worker_ctx.copy().run,
                        _upload_tree_upload_file_for_target,
                        self._binding,
                        dst,
                        source_path,
                        file_path,
                        preserve_zarr_metadata,
                        multipart_threshold,
                    ): file_path
                    for file_path in files
                }

                try:
                    for future in concurrent.futures.as_completed(futures):
                        if first_error is not None:
                            future.cancel()
                            continue
                        try:
                            written, uploaded = future.result()
                            bytes_written += written
                            files_written += uploaded
                        except Exception as exc:
                            first_error = exc
                            for pending in futures:
                                pending.cancel()
                            executor.shutdown(wait=False, cancel_futures=True)
                    if first_error is not None:
                        raise first_error
                finally:
                    if first_error is None:
                        executor.shutdown(wait=True)
        except StorageError:
            raise
        except Exception as exc:
            raise StorageError(str(exc) or exc.__class__.__name__) from exc

        return StorageWriteResult(
            path=dst.to_str(),
            bytes_written=bytes_written,
            files_written=files_written,
            duration_s=time.time() - start,
            storage_type=_storage_type_for_uri(dst),
        )

    def control_plane(self) -> ChunkManager:
        from firecube.core.controlplane.manager import ChunkManager

        manager = cast(Any, ChunkManager)(binding=self._binding, filesystem=self.fs())
        cast(Any, manager).product_name = self.product.product_name
        return manager


def _upload_tree_files(source_path: Path) -> list[Path]:
    if source_path.is_file():
        return [source_path]
    return sorted(
        (path for path in source_path.rglob("*") if path.is_file()), key=lambda p: p.as_posix()
    )


def _upload_tree_destination_uri(
    root: StorageUri, source_path: Path, file_path: Path
) -> StorageUri:
    if source_path.is_file():
        # File source: dst is the EXACT destination URI/path; do not append source.name.
        # Engine passes full product URI (e.g. s3://bucket/data/product.parquet) as dst,
        # so appending file_path.name would produce s3://bucket/data/product.parquet/product.parquet.
        return root
    rel = file_path.relative_to(source_path)
    return root.join(rel.as_posix())


def _upload_tree_upload_file_for_target(
    binding: StorageBinding,
    root: StorageUri,
    source_path: Path,
    file_path: Path,
    preserve_zarr_metadata: bool,
    multipart_threshold: int,
) -> tuple[int, int]:
    fs = create_filesystem(binding)
    return _upload_tree_upload_file(
        fs,
        root,
        source_path,
        file_path,
        preserve_zarr_metadata=preserve_zarr_metadata,
        multipart_threshold=multipart_threshold,
    )


def _upload_tree_upload_file(
    fs: Any,
    root: StorageUri,
    source_path: Path,
    file_path: Path,
    *,
    preserve_zarr_metadata: bool,
    multipart_threshold: int,
) -> tuple[int, int]:
    dst_uri = _upload_tree_destination_uri(root, source_path, file_path)
    if (
        preserve_zarr_metadata
        and file_path.name == "zarr.json"
        and _should_skip_upload_tree_zarr_json(fs, file_path, dst_uri)
    ):
        return 0, 0

    dst_parent = dst_uri.parent()
    if dst_parent != dst_uri and hasattr(fs, "makedirs"):
        fs.makedirs(dst_parent, exist_ok=True)

    file_size = file_path.stat().st_size
    if file_size > multipart_threshold:
        if not _upload_tree_multipart(fs, file_path, dst_uri.to_str()):
            raise RuntimeError(f"multipart upload helper returned False for {file_path!s}")
        return file_size, 1

    with file_path.open("rb") as src_handle, fs.open(dst_uri, "wb") as dst_handle:
        shutil.copyfileobj(src_handle, dst_handle)
    return file_size, 1


def _upload_tree_multipart(fs: Any, local_path: Path, remote_path: str) -> bool:
    from firecube.core.filesystem.fsspec_backend import FsspecFilesystem  # noqa: I001
    from firecube.core.filesystem.instrumentation import InstrumentedFilesystem
    from firecube.core.filesystem.obstore_backend import ObstoreFilesystem
    from firecube.core.filesystem.protocol import Multipart  # pyright: ignore[reportAttributeAccessIssue]

    if isinstance(fs, InstrumentedFilesystem):
        fs = fs._wrapped

    put_file = getattr(fs, "put_file", None)
    if callable(put_file):
        put_file(str(local_path), remote_path)
        return True

    if not isinstance(fs, (FsspecFilesystem, ObstoreFilesystem)):
        return False
    storage_fs = cast(Any, fs)
    if Multipart not in storage_fs.capabilities():
        return False
    storage_fs.multipart_upload(str(local_path), remote_path)
    return True


def _upload_tree_shape0(metadata: dict[str, Any]) -> int | None:
    shape = metadata.get("shape", [])
    if not shape:
        return None
    try:
        return int(shape[0])
    except (TypeError, ValueError, IndexError):
        return None


def _should_skip_upload_tree_zarr_json(fs: Any, source_path: Path, dest: StorageUri) -> bool:
    if source_path.name != "zarr.json":
        return False

    try:
        src_meta = json.loads(source_path.read_text(encoding="utf-8"))
        src_shape = src_meta.get("shape", [])
        if not src_shape:
            return False
        with fs.open(dest, "r") as fh:
            dst_meta = json.load(fh)
        dst_shape = dst_meta.get("shape", [])
        return bool(dst_shape and dst_shape[0] >= src_shape[0])
    except Exception:
        return False
