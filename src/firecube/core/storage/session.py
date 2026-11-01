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
import threading
import time
from collections import Counter
from collections.abc import Callable, Collection, Iterator
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
    from firecube.core.zarr.guard.promotion_plan import PlannedObject, PromotionPlan

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
        anonymous=binding.driver.anonymous,
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
        from firecube.core.filesystem.protocol import Multipart  # pyright: ignore[reportAttributeAccessIssue]

        file_size = local_path.stat().st_size
        fs = cast(Any, self.fs())
        if file_size > multipart_threshold and Multipart in fs.capabilities():

            def _write(target: StorageUri) -> None:
                fs.multipart_upload(str(local_path), target.to_str())

        else:

            def _write(target: StorageUri) -> None:
                with local_path.open("rb") as src_handle, fs.open(target, "wb") as dst_handle:
                    shutil.copyfileobj(src_handle, dst_handle)

        _publish_object(fs, dst, _write)
        return file_size

    def put_bytes(self, uri: StorageUri, payload: bytes) -> None:
        """Publish one whole object at ``uri`` and create its parents.

        Not for payload writes into a live DirectZarr store; those go through ``guarded_write``.
        """
        _put_bytes(self.fs(), uri, payload)

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

    def plan_promotion(
        self,
        src: StorageUri,
        dst: StorageUri,
        *,
        written_units: Collection[str],
        absent_units: Collection[str],
        managed_coordinates: Collection[str] = (),
    ) -> PromotionPlan:
        """Plan the publication of the local staged tree ``src`` onto ``dst``.

        Args:
            src: Local workspace root of the staged product.
            dst: Target product root.
            written_units: Store keys of the stored units the run wrote in
                every cell.
            absent_units: The written units zarr left without an object
                (every cell equals the fill value); they become deletes.
            managed_coordinates: ``group/array`` paths of the time coordinates
                the target marks as preallocated or coordinate-managed.

        Returns:
            The plan of :func:`~firecube.core.zarr.guard.promotion_plan.build_promotion_plan`.
            A data object of a managed coordinate is unchanged, and the plan
            never reads its target object. Any other object the run did not write is
            unchanged when the target holds
            the same bytes (one read of the target object). A ``zarr.json`` is
            also unchanged when the target array is longer on its first axis
            and the two documents are otherwise equal. Any other difference
            is a put, which :meth:`publish_plan` reconciles with the target
            document current at publication.

        Raises:
            ValueError: ``src`` is not a local directory, or the units fail
                :func:`~firecube.core.zarr.guard.promotion_plan.build_promotion_plan`'s
                checks.
        """
        from firecube.core.zarr.guard.promotion_plan import (
            build_promotion_plan,
            list_workspace_objects,
        )

        source_path = _local_tree_root(src, "plan_promotion")
        fs = self.fs()

        def _is_unchanged(key: str) -> bool:
            local = source_path / key
            try:
                current = bytes(fs.read_bytes(dst.join(key)))
            except Exception:
                # Absent or unreadable: unchanged is not proved, so the object
                # is put; a real storage fault surfaces from that put.
                return False
            proposed = local.read_bytes()
            if current == proposed:
                return True
            return local.name == "zarr.json" and _target_document_supersedes(current, proposed)

        return build_promotion_plan(
            workspace_objects=list_workspace_objects(source_path),
            written_units=written_units,
            absent_units=absent_units,
            is_unchanged=_is_unchanged,
            managed_coordinates=managed_coordinates,
        )

    def publish_plan(
        self,
        plan: PromotionPlan,
        src: StorageUri,
        dst: StorageUri,
        *,
        parallel_workers: int = 4,
        multipart_threshold: int = 64 * 1024 * 1024,
        publish_metadata: Callable[[str, bytes], bool | None] | None = None,
    ) -> StorageWriteResult:
        """Execute ``plan``: put workspace objects onto ``dst``, delete dropped units.

        Every key is checked before anything runs: a key that is not a
        canonical relative key under ``dst`` (empty, absolute, an empty, ``.``
        or ``..`` segment, a backslash), a key listed twice, a delete of a
        ``zarr.json`` or a put the workspace lacks refuses the whole plan.

        Data objects go first, in a pool of ``parallel_workers`` threads: a
        put copies the workspace object through the driver's whole-object
        publication, a delete removes exactly that one object (the key is
        never expanded as a pattern, and a store's refusal fails the
        promotion), and a delete of an object the target does not hold is a
        no-op. A driver without such a single-object delete refuses a plan
        that deletes, before anything runs. The ``zarr.json``
        objects follow one by one in plan order, only when every data
        operation succeeded, so an interrupted publication never shows grown
        metadata without its data. Each document is decided against the
        target's current one, read just before it is published: when the
        target array is longer on axis 0 (another run grew it), the
        workspace document is published with the target's axis-0 extent, so
        promotion never shrinks an array; when such a longer target also
        differs in structure (anything but ``shape[0]`` and ``attributes``),
        the promotion is refused naming the key. This check also runs once
        before any operation. Otherwise the workspace document is published
        as is. Documents go through ``publish_metadata(key, payload)`` when
        given (``key`` relative to ``dst``), else through the driver's
        whole-object publication.

        After the first failure no further operation starts; the pool waits
        for those already running and then the call raises.

        Returns:
            ``files_written``/``bytes_written`` count puts (a document by
            the length of the payload handed over); a document for which
            ``publish_metadata`` returns ``False`` (it wrote nothing) is not
            counted, ``None`` counts as written. ``files_deleted``
            the deletes the store carried out without reporting the object
            missing (S3 never reports a missing key, so there an absent
            object counts too).

        Raises:
            StorageError: The plan is refused (nothing ran), or an operation
                failed; the message names k of n operations done and the
                ones not done.
            ValueError: ``src`` is not a local directory.
        """
        from firecube.core.zarr.guard.promotion_plan import PromotionAction

        source_path = _local_tree_root(src, "publish_plan")
        operations = [obj for obj in plan.objects if obj.action is not PromotionAction.UNCHANGED]
        _refuse_unsafe_plan(plan, operations, source_path)
        if any(obj.action is PromotionAction.DELETE for obj in operations):
            _literal_delete(self.fs())  # refuses, before any operation, a driver without one
        data = [obj for obj in operations if not obj.is_metadata]
        metadata = [obj for obj in operations if obj.is_metadata]

        def _metadata_to_publish(fs: Any, key: str) -> bytes:
            return _metadata_payload(
                key, (source_path / key).read_bytes(), _read_if_present(fs, dst.join(key))
            )

        for obj in metadata:  # refuses a structural conflict known before any operation
            try:
                _metadata_to_publish(self.fs(), obj.key)
            except StorageError:
                raise
            except Exception as exc:
                raise StorageError(
                    f"promotion plan refused, cannot read target {obj.key}: {exc}"
                ) from exc

        start = time.time()
        lock = threading.Lock()
        stop = threading.Event()
        done: set[str] = set()
        counters = {"bytes": 0, "puts": 0, "deletes": 0}

        def _apply(fs: Any, obj: PlannedObject) -> None:
            written = uploaded = deleted = 0
            if obj.action is PromotionAction.DELETE:
                deleted = _delete_object(fs, dst.join(obj.key))
            elif obj.is_metadata:
                payload = _metadata_to_publish(fs, obj.key)
                published: bool | None = True
                if publish_metadata is not None:
                    published = publish_metadata(obj.key, payload)
                else:
                    _put_bytes(fs, dst.join(obj.key), payload)
                if published is not False:
                    written, uploaded = len(payload), 1
            else:
                written, uploaded = _upload_tree_upload_file(
                    fs,
                    dst,
                    source_path,
                    source_path / obj.key,
                    preserve_zarr_metadata=False,
                    multipart_threshold=multipart_threshold,
                )
            with lock:
                counters["bytes"] += written
                counters["puts"] += uploaded
                counters["deletes"] += deleted
                done.add(obj.key)

        def _apply_in_worker(obj: PlannedObject) -> None:
            if stop.is_set():
                return
            try:
                _apply(create_filesystem(self._binding), obj)
            except BaseException:
                stop.set()
                raise

        first_error: BaseException | None = None
        if data:
            worker_ctx = contextvars.copy_context()
            workers = max(1, min(parallel_workers, len(data)))
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
                futures = [
                    pool.submit(worker_ctx.copy().run, _apply_in_worker, obj) for obj in data
                ]
                # Nothing is cancelled: every future completes (a task that
                # starts after a failure returns at once), so this drains.
                for future in concurrent.futures.as_completed(futures):
                    error = future.exception()
                    if error is not None and first_error is None:
                        first_error = error
        if first_error is None:
            fs = self.fs()
            for obj in metadata:
                try:
                    _apply(fs, obj)
                except BaseException as exc:
                    first_error = exc
                    break

        if first_error is not None:
            if not isinstance(first_error, Exception):
                raise first_error
            not_done = [
                f"{obj.action.value} {obj.key}" for obj in operations if obj.key not in done
            ]
            shown = ", ".join(not_done[:20])
            more = f" and {len(not_done) - 20} more" if len(not_done) > 20 else ""
            raise StorageError(
                f"promotion to {dst.to_str()} failed after {len(done)} of {len(operations)} "
                f"object operation(s): {str(first_error) or first_error.__class__.__name__}; "
                f"not done ({len(not_done)}): {shown}{more}"
            ) from first_error

        return StorageWriteResult(
            path=dst.to_str(),
            bytes_written=counters["bytes"],
            files_written=counters["puts"],
            duration_s=time.time() - start,
            storage_type=_storage_type_for_uri(dst),
            files_deleted=counters["deletes"],
        )

    def control_plane(self) -> ChunkManager:
        from firecube.core.controlplane.manager import ChunkManager

        manager = cast(Any, ChunkManager)(binding=self._binding, filesystem=self.fs())
        cast(Any, manager).product_name = self.product.product_name
        return manager


def _local_tree_root(src: StorageUri, operation: str) -> Path:
    if src.is_remote():
        raise ValueError(f"{operation} requires a local source")

    from firecube.core.uris import local_path_from_target

    source_path = local_path_from_target(src.to_str()).resolve()
    if not source_path.is_dir():
        raise ValueError(f"{operation} requires a local directory source: {source_path}")
    return source_path


def _is_canonical_key(key: str) -> bool:
    """Whether ``key`` names an object strictly under the product root."""
    if not key or key.startswith("/") or "\\" in key:
        return False
    return all(segment not in ("", ".", "..") for segment in key.split("/"))


def _refuse_unsafe_plan(
    plan: PromotionPlan, operations: list[PlannedObject], source_path: Path
) -> None:
    """Raise ``StorageError`` before any operation when ``plan`` cannot be run safely.

    ``StorageUri.join`` strips a leading ``/`` and keeps ``..``, so a
    malformed key would put or delete an object outside the product (or a
    different one inside it) rather than fail.
    """
    from firecube.core.zarr.guard.promotion_plan import PromotionAction

    malformed = [obj.key for obj in plan.objects if not _is_canonical_key(obj.key)]
    if malformed:
        raise StorageError(f"promotion plan refused, keys not canonical: {malformed[:20]!r}")
    counts = Counter(obj.key for obj in plan.objects)
    repeated = sorted(key for key, count in counts.items() if count > 1)
    if repeated:
        raise StorageError(f"promotion plan refused, keys listed twice: {repeated[:20]!r}")
    metadata_deletes = [
        obj.key for obj in operations if obj.action is PromotionAction.DELETE and obj.is_metadata
    ]
    if metadata_deletes:
        raise StorageError(f"promotion plan refused, deletes zarr.json: {metadata_deletes[:20]!r}")
    missing = [
        obj.key
        for obj in operations
        if obj.action is PromotionAction.PUT and not (source_path / obj.key).is_file()
    ]
    if missing:
        raise StorageError(
            f"promotion plan refused, puts objects the workspace lacks: {missing[:20]!r}"
        )


def _literal_delete(fs: Any) -> Callable[[StorageUri], None]:
    """Return the driver's checked delete of exactly one object, key taken literally.

    The protocol's ``rm`` is not that: fsspec's ``rm`` expands glob characters
    (``*``, ``?``, ``[``, all legal in a zarr key) and s3fs sends a
    DeleteObjects batch whose per-key errors it ignores. fsspec's ``rm_file``
    (s3fs: one DeleteObject, errors raised; local: ``os.remove``) and obstore's
    ``delete`` take the key literally and raise on failure.

    Raises:
        StorageError: The driver offers no such delete.
    """
    from firecube.core.filesystem.fsspec_backend import FsspecFilesystem
    from firecube.core.filesystem.instrumentation import InstrumentedFilesystem
    from firecube.core.filesystem.obstore_backend import ObstoreFilesystem

    raw = fs._wrapped if isinstance(fs, InstrumentedFilesystem) else fs
    if isinstance(raw, FsspecFilesystem):
        fsspec_fs = raw._fs
        to_path = raw._to_path
        return lambda target: fsspec_fs.rm_file(to_path(target))
    if isinstance(raw, ObstoreFilesystem):
        store = raw._store
        resolve = raw._resolve_path
        return lambda target: store.delete(resolve(target))
    raise StorageError(
        f"promotion cannot delete through {type(raw).__name__}: no single-object delete"
    )


def _delete_object(fs: Any, target: StorageUri) -> int:
    """Delete exactly the object ``target``; return 0 if the store reports it missing, else 1.

    A missing object is a no-op, known only from the delete's own not-found
    error (the target is not listed or probed first). Local stores report
    one; S3 does not, so there a delete of an absent key counts as 1. Any
    other error propagates.
    """
    try:
        _literal_delete(fs)(target)
    except FileNotFoundError:
        return 0
    return 1


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

        def _write(target: StorageUri) -> None:
            if not _upload_tree_multipart(fs, file_path, target.to_str()):
                raise RuntimeError(f"multipart upload helper returned False for {file_path!s}")

    else:

        def _write(target: StorageUri) -> None:
            with file_path.open("rb") as src_handle, fs.open(target, "wb") as dst_handle:
                shutil.copyfileobj(src_handle, dst_handle)

    _publish_object(fs, dst_uri, _write)
    return file_size, 1


def _publish_object(fs: Any, dst_uri: StorageUri, write: Callable[[StorageUri], None]) -> None:
    """Hand ``write`` to the driver's ``AtomicPublish`` capability, or run it in place.

    The capability is looked up through ``capabilities()``, not ``isinstance``:
    the metrics proxy (``InstrumentedFilesystem``) forwards attributes with
    ``__getattr__``, which a runtime-protocol ``isinstance`` does not see, so an
    ``isinstance`` test would silently fall back to the in-place write whenever
    filesystem metrics are active.

    The driver decides what atomic means for ``dst_uri`` (a hidden temporary and
    a rename for a local target, a direct write for an object store); see
    ``AtomicPublish.publish_atomic``.
    """
    from firecube.core.filesystem.protocol import AtomicPublish

    if AtomicPublish in fs.capabilities():
        fs.publish_atomic(dst_uri, write)
    else:
        write(dst_uri)


def _upload_tree_multipart(fs: Any, local_path: Path, remote_path: str) -> bool:
    from firecube.core.filesystem.fsspec_backend import FsspecFilesystem
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


def _read_if_present(fs: Any, uri: StorageUri) -> bytes | None:
    try:
        return bytes(fs.read_bytes(uri))
    except FileNotFoundError:
        return None


def _put_bytes(fs: Any, uri: StorageUri, payload: bytes) -> None:
    parent = uri.parent()
    if parent != uri and hasattr(fs, "makedirs"):
        fs.makedirs(parent, exist_ok=True)

    def _write(target: StorageUri) -> None:
        with fs.open(target, "wb") as handle:
            handle.write(payload)

    _publish_object(fs, uri, _write)


def _array_document(raw: bytes) -> dict[str, Any] | None:
    """The parsed ``zarr.json`` when it is an object with a non-empty integer ``shape``."""
    try:
        document = json.loads(raw)
    except ValueError:
        return None
    if not isinstance(document, dict):
        return None
    document = cast(dict[str, Any], document)
    shape = document.get("shape")
    if not isinstance(shape, list) or not shape:
        return None
    if not all(isinstance(n, int) and not isinstance(n, bool) for n in shape):
        return None
    return document


def _metadata_payload(key: str, workspace: bytes, target: bytes | None) -> bytes:
    """The ``zarr.json`` to publish at ``key`` over the target's current ``target``.

    The workspace document, except when both are array documents and the
    target is longer on axis 0: then the workspace document with the target's
    axis-0 extent, so promotion never shrinks an array another run grew.

    Raises:
        StorageError: The target is longer on axis 0 and the documents differ
            in anything but ``shape[0]`` and ``attributes``; merging them
            would be a guess.
    """
    if target is None:
        return workspace
    target_doc = _array_document(target)
    workspace_doc = _array_document(workspace)
    if target_doc is None or workspace_doc is None:
        return workspace
    target_shape: list[int] = target_doc["shape"]
    workspace_shape: list[int] = workspace_doc["shape"]
    if target_shape[0] <= workspace_shape[0]:
        return workspace
    differing = sorted(
        name
        for name in target_doc.keys() | workspace_doc.keys()
        if name not in ("shape", "attributes")
        and (
            name not in target_doc
            or name not in workspace_doc
            or json.dumps(target_doc[name], sort_keys=True)
            != json.dumps(workspace_doc[name], sort_keys=True)
        )
    )
    if len(target_shape) != len(workspace_shape) or target_shape[1:] != workspace_shape[1:]:
        differing.insert(0, "shape")
    if differing:
        raise StorageError(
            f"promotion refused for {key}: the target array is longer on axis 0 "
            f"({target_shape} over {workspace_shape}) and differs in {differing}"
        )
    merged = dict(workspace_doc)
    merged["shape"] = [target_shape[0], *workspace_shape[1:]]
    return json.dumps(merged, indent=2).encode()


def _target_document_supersedes(target: bytes, workspace: bytes) -> bool:
    """Whether the target ``zarr.json`` only grew the workspace's array on its first axis.

    True when the target shape is longer on axis 0, equal on every other
    axis, and the documents are equal once ``shape`` is set aside.
    """
    target_doc = _array_document(target)
    workspace_doc = _array_document(workspace)
    if target_doc is None or workspace_doc is None:
        return False
    target_rest, workspace_rest = dict(target_doc), dict(workspace_doc)
    target_shape: list[int] = target_rest.pop("shape")
    workspace_shape: list[int] = workspace_rest.pop("shape")
    return (
        len(target_shape) == len(workspace_shape)
        and target_shape[0] > workspace_shape[0]
        and target_shape[1:] == workspace_shape[1:]
        and target_rest == workspace_rest
    )


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
