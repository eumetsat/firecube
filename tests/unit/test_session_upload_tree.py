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
import json
from pathlib import Path
from typing import Any

import pytest

from firecube.core.config import StorageConfig
from firecube.core.errors import StorageError
from firecube.core.storage import StorageWriteResult
from firecube.core.storage.session import StorageSession  # pyright: ignore[reportMissingImports]
from firecube.core.storage.uri import StorageUri  # pyright: ignore[reportMissingImports]
from tests.helpers.storage import make_test_session


def _session(uri: str, *, storage_config: StorageConfig | None = None) -> StorageSession:
    path = Path(uri)
    if storage_config and storage_config.storage_type == "s3":
        return make_test_session(
            path.parent,
            product=path.name,
            protocol="s3",
            authority=getattr(storage_config, "bucket", None) or "bucket",
        )
    return make_test_session(path.parent, product=path.name)


def _write_zarr_json(path: Path, shape0: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"shape": [shape0, 50, 50]}), encoding="utf-8")


def test_upload_tree_accepts_file_source_parquet(tmp_path: Path) -> None:
    source = tmp_path / "product.parquet"
    source.write_bytes(b"x" * 1024)
    target = tmp_path / "target.parquet"

    result = _session(str(target)).upload_tree(
        StorageUri.from_local_path(source), StorageUri.from_local_path(target)
    )

    assert target.read_bytes() == b"x" * 1024
    assert result.files_written == 1
    assert result.bytes_written == 1024


def test_upload_tree_accepts_directory_source_zarr(tmp_path: Path) -> None:
    source = tmp_path / "source.zarr"
    _write_zarr_json(source / "zarr.json", 10)
    (source / "data" / "c" / "0").parent.mkdir(parents=True)
    (source / "data" / "c" / "0").write_bytes(b"chunk")
    (source / "data" / "zarr.json").write_text("{}", encoding="utf-8")
    target = tmp_path / "target.zarr"

    result = _session(str(target)).upload_tree(
        StorageUri.from_local_path(source), StorageUri.from_local_path(target)
    )

    assert (target / "zarr.json").exists()
    assert (target / "data" / "c" / "0").read_bytes() == b"chunk"
    assert (target / "data" / "zarr.json").read_text(encoding="utf-8") == "{}"
    assert result.files_written == 3


def test_upload_tree_skips_zarr_json_when_source_shape_is_smaller(tmp_path: Path) -> None:
    source = tmp_path / "source.zarr"
    target = tmp_path / "target.zarr"
    _write_zarr_json(source / "zarr.json", 10)
    (source / "data.bin").write_bytes(b"new")
    _write_zarr_json(target / "zarr.json", 100)

    _session(str(target)).upload_tree(
        StorageUri.from_local_path(source), StorageUri.from_local_path(target)
    )

    assert json.loads((target / "zarr.json").read_text(encoding="utf-8"))["shape"][0] == 100
    assert (target / "data.bin").read_bytes() == b"new"


def test_upload_tree_uploads_zarr_json_when_source_shape_is_larger(tmp_path: Path) -> None:
    source = tmp_path / "source.zarr"
    target = tmp_path / "target.zarr"
    _write_zarr_json(source / "zarr.json", 100)
    _write_zarr_json(target / "zarr.json", 10)

    _session(str(target)).upload_tree(
        StorageUri.from_local_path(source), StorageUri.from_local_path(target)
    )

    assert json.loads((target / "zarr.json").read_text(encoding="utf-8"))["shape"][0] == 100


def test_upload_tree_first_error_raises_storage_error_and_aborts(tmp_path: Path) -> None:
    source = tmp_path / "source.zarr"
    for name, content in [("a.bin", b"a"), ("b.bin", b"b"), ("c.bin", b"c")]:
        (source / name).parent.mkdir(parents=True, exist_ok=True)
        (source / name).write_bytes(content)
    target = tmp_path / "target.zarr"
    (target / "b.bin").parent.mkdir(parents=True, exist_ok=True)
    (target / "b.bin").mkdir()

    with pytest.raises(StorageError):
        _session(str(target)).upload_tree(
            StorageUri.from_local_path(source),
            StorageUri.from_local_path(target),
            parallel_workers=1,
        )

    assert (target / "a.bin").read_bytes() == b"a"
    assert not (target / "c.bin").exists()


def test_upload_tree_result_populates_all_fields(tmp_path: Path) -> None:
    source = tmp_path / "source.h5"
    source.write_bytes(b"12345")
    target = tmp_path / "target.h5"

    result = _session(str(target)).upload_tree(
        StorageUri.from_local_path(source), StorageUri.from_local_path(target)
    )

    assert isinstance(result, StorageWriteResult)
    assert result.path == StorageUri.from_local_path(target).to_str()
    assert target.read_bytes() == b"12345"
    assert result.bytes_written == 5
    assert result.files_written == 1
    assert result.duration_s >= 0
    assert result.storage_type == "local"


def test_upload_tree_preserve_zarr_metadata_false_disables_skip(tmp_path: Path) -> None:
    source = tmp_path / "source.zarr"
    target = tmp_path / "target.zarr"
    _write_zarr_json(source / "zarr.json", 10)
    _write_zarr_json(target / "zarr.json", 100)

    _session(str(target)).upload_tree(
        StorageUri.from_local_path(source),
        StorageUri.from_local_path(target),
        preserve_zarr_metadata=False,
    )

    assert json.loads((target / "zarr.json").read_text(encoding="utf-8"))["shape"][0] == 10


def test_upload_tree_empty_source_directory_succeeds(tmp_path: Path) -> None:
    source = tmp_path / "empty.zarr"
    source.mkdir()
    target = tmp_path / "target.zarr"

    result = _session(str(target)).upload_tree(
        StorageUri.from_local_path(source), StorageUri.from_local_path(target)
    )

    assert result.files_written == 0
    assert result.bytes_written == 0
    assert not target.exists()


class _LocalMirrorFs:
    def __init__(self, base: Path, root: str) -> None:
        self.base = base
        self.root = root.strip("/")

    def _local_path(self, path: Any) -> Path:
        rel_str = path.to_str() if hasattr(path, "to_str") else str(path)
        if "://" in rel_str:
            rel_str = rel_str.split("://", 1)[1]
        rel = rel_str.strip("/")
        if self.root and rel.startswith(self.root):
            rel = rel[len(self.root) :].strip("/")
        return self.base / rel

    def open(self, path: Any, mode: str = "rb") -> Any:
        local_path = self._local_path(path)
        if any(flag in mode for flag in ("w", "a", "+")):
            local_path.parent.mkdir(parents=True, exist_ok=True)
        return local_path.open(mode)

    def makedirs(self, path: Any, exist_ok: bool = False) -> None:
        self._local_path(path).mkdir(parents=True, exist_ok=exist_ok)


def test_upload_tree_file_source_to_remote_destination_uses_open_fs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "product.parquet"
    source.write_bytes(b"remote")
    remote_root = tmp_path / "remote"
    bindings: list[Any] = []
    fake_fs = _LocalMirrorFs(remote_root, "bucket/prefix")

    def _fake_create_filesystem(binding: Any) -> _LocalMirrorFs:
        bindings.append(binding)
        return fake_fs

    monkeypatch.setattr("firecube.core.storage.session.create_filesystem", _fake_create_filesystem)
    storage_config = StorageConfig(storage_type="s3", storage_driver="fsspec")
    storage_config.bucket = "bucket"  # type: ignore[attr-defined]
    session = _session("s3://bucket/prefix/product.parquet", storage_config=storage_config)

    result = session.upload_tree(
        StorageUri.from_local_path(source),
        StorageUri.parse("s3://bucket/prefix/product.parquet"),
    )

    assert (remote_root / "product.parquet").read_bytes() == b"remote"
    assert len(bindings) >= 1
    assert bindings[0].driver.driver == "fsspec"
    assert result.storage_type == "s3"


def test_upload_tree_directory_source_to_local_destination(tmp_path: Path) -> None:
    source = tmp_path / "source.zarr"
    (source / "nested").mkdir(parents=True)
    (source / "nested" / "chunk.bin").write_bytes(b"payload")
    target = tmp_path / "target.zarr"

    result = _session(str(target)).upload_tree(
        StorageUri.from_local_path(source), StorageUri.from_local_path(target)
    )

    assert (target / "nested" / "chunk.bin").read_bytes() == b"payload"
    assert result.path == StorageUri.from_local_path(target).to_str()
    assert result.files_written == 1


def test_upload_tree_rejects_remote_source(tmp_path: Path) -> None:
    session = _session(str(tmp_path / "target.zarr"))

    with pytest.raises(ValueError, match="local source"):
        session.upload_tree(
            StorageUri.parse("s3://bucket/source.zarr"), StorageUri.from_local_path(tmp_path)
        )


def test_upload_tree_parallel_workers_one_uses_sequential_fallback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source.zarr"
    for idx in range(4):
        (source / f"{idx}.bin").parent.mkdir(parents=True, exist_ok=True)
        (source / f"{idx}.bin").write_bytes(str(idx).encode())
    target = tmp_path / "target.zarr"

    def _fail_executor(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("parallel executor should not be used")

    monkeypatch.setattr(
        "firecube.core.storage.session.concurrent.futures.ThreadPoolExecutor", _fail_executor
    )

    result = _session(str(target)).upload_tree(
        StorageUri.from_local_path(source),
        StorageUri.from_local_path(target),
        parallel_workers=1,
    )

    assert result.files_written == 4
    assert (target / "3.bin").read_bytes() == b"3"


def test_upload_tree_two_files_uses_sequential_fallback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source.zarr"
    for idx in range(2):
        (source / f"{idx}.bin").parent.mkdir(parents=True, exist_ok=True)
        (source / f"{idx}.bin").write_bytes(str(idx).encode())
    target = tmp_path / "target.zarr"

    def _fail_executor(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("parallel executor should not be used")

    monkeypatch.setattr(
        "firecube.core.storage.session.concurrent.futures.ThreadPoolExecutor", _fail_executor
    )

    result = _session(str(target)).upload_tree(
        StorageUri.from_local_path(source),
        StorageUri.from_local_path(target),
        parallel_workers=4,
    )

    assert result.files_written == 2
    assert (target / "1.bin").read_bytes() == b"1"


def test_upload_tree_parallel_upload_uses_four_workers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source.zarr"
    for idx in range(5):
        (source / f"{idx}.bin").parent.mkdir(parents=True, exist_ok=True)
        (source / f"{idx}.bin").write_bytes(str(idx).encode())
    target = tmp_path / "target.zarr"
    worker_counts: list[int] = []
    submit_count = 0
    real_executor = concurrent.futures.ThreadPoolExecutor

    class _ExecutorSpy(real_executor):  # type: ignore[misc]
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            worker_counts.append(int(kwargs["max_workers"]))
            super().__init__(*args, **kwargs)

        def submit(self, *args: Any, **kwargs: Any) -> concurrent.futures.Future[Any]:
            nonlocal submit_count
            submit_count += 1
            return super().submit(*args, **kwargs)

    monkeypatch.setattr(
        "firecube.core.storage.session.concurrent.futures.ThreadPoolExecutor", _ExecutorSpy
    )

    result = _session(str(target)).upload_tree(
        StorageUri.from_local_path(source),
        StorageUri.from_local_path(target),
        parallel_workers=4,
    )

    assert worker_counts == [4]
    assert submit_count == 5
    assert result.files_written == 5
    assert (target / "4.bin").read_bytes() == b"4"


def test_upload_tree_custom_parallel_workers_respected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source.zarr"
    for idx in range(10):
        (source / f"{idx}.bin").parent.mkdir(parents=True, exist_ok=True)
        (source / f"{idx}.bin").write_bytes(str(idx).encode())
    target = tmp_path / "target.zarr"
    worker_counts: list[int] = []
    real_executor = concurrent.futures.ThreadPoolExecutor

    class _ExecutorSpy(real_executor):  # type: ignore[misc]
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            worker_counts.append(int(kwargs["max_workers"]))
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(
        "firecube.core.storage.session.concurrent.futures.ThreadPoolExecutor", _ExecutorSpy
    )

    _session(str(target)).upload_tree(
        StorageUri.from_local_path(source),
        StorageUri.from_local_path(target),
        parallel_workers=8,
    )

    assert worker_counts == [8]


def test_upload_tree_large_file_routes_through_multipart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source.zarr"
    source.mkdir()
    large = source / "large.bin"
    large.write_bytes(b"large")
    remote_root = tmp_path / "remote"
    put_file_calls: list[tuple[str, str]] = []

    class _RawMultipartFs(_LocalMirrorFs):
        def put_file(self, local_path: str, remote_path: str) -> None:
            put_file_calls.append((local_path, remote_path))
            with (
                Path(local_path).open("rb") as src_handle,
                self.open(remote_path, "wb") as dst_handle,
            ):
                dst_handle.write(src_handle.read())

    fake_fs = _RawMultipartFs(remote_root, "bucket/prefix")

    def _fake_create_filesystem(binding: Any) -> _RawMultipartFs:
        return fake_fs

    monkeypatch.setattr("firecube.core.storage.session.create_filesystem", _fake_create_filesystem)
    storage_config = StorageConfig(storage_type="s3", storage_driver="fsspec")
    storage_config.bucket = "bucket"  # type: ignore[attr-defined]
    session = _session("s3://bucket/prefix", storage_config=storage_config)

    result = session.upload_tree(
        StorageUri.from_local_path(source),
        StorageUri.parse("s3://bucket/prefix"),
        multipart_threshold=1,
    )

    assert put_file_calls == [(str(large), "s3://bucket/prefix/large.bin")]
    assert (remote_root / "large.bin").read_bytes() == b"large"
    assert result.bytes_written == 5


def test_upload_tree_parallel_first_error_cancels_remaining_futures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source.zarr"
    for idx in range(3):
        (source / f"{idx}.bin").parent.mkdir(parents=True, exist_ok=True)
        (source / f"{idx}.bin").write_bytes(str(idx).encode())
    target = tmp_path / "target.zarr"
    futures: list[concurrent.futures.Future[tuple[int, int]]] = []
    shutdown_calls: list[tuple[bool, bool]] = []

    class _FailingExecutor:
        def __init__(self, *, max_workers: int) -> None:
            self.max_workers = max_workers

        def submit(self, *args: Any, **kwargs: Any) -> concurrent.futures.Future[tuple[int, int]]:
            future: concurrent.futures.Future[tuple[int, int]] = concurrent.futures.Future()
            if not futures:
                future.set_exception(RuntimeError("boom"))
            futures.append(future)
            return future

        def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
            shutdown_calls.append((wait, cancel_futures))

    def _as_completed(items: Any) -> list[concurrent.futures.Future[tuple[int, int]]]:
        return list(items)

    monkeypatch.setattr(
        "firecube.core.storage.session.concurrent.futures.ThreadPoolExecutor", _FailingExecutor
    )
    monkeypatch.setattr(
        "firecube.core.storage.session.concurrent.futures.as_completed", _as_completed
    )

    with pytest.raises(StorageError, match="boom"):
        _session(str(target)).upload_tree(
            StorageUri.from_local_path(source),
            StorageUri.from_local_path(target),
            parallel_workers=4,
        )

    assert shutdown_calls == [(False, True)]
    assert all(future.cancelled() for future in futures[1:])


def test_upload_tree_file_source_does_not_duplicate_filename(tmp_path: Path) -> None:
    """Regression: dst is exact destination, not parent dir, for file source."""
    source = tmp_path / "product.parquet"
    source.write_bytes(b"data")
    target = tmp_path / "out" / "product.parquet"

    _session(str(target)).upload_tree(
        StorageUri.from_local_path(source), StorageUri.from_local_path(target)
    )

    assert target.read_bytes() == b"data"
    assert not target.is_dir()
