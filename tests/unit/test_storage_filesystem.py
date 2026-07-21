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

from pathlib import Path

import pytest

from firecube.core.filesystem.fsspec_backend import FsspecFilesystem
from firecube.core.filesystem.obstore_backend import ObstoreFilesystem
from firecube.core.filesystem.ops import create_filesystem
from firecube.core.filesystem.protocol import StorageFilesystem
from firecube.core.storage.uri import StorageUri
from tests.helpers.storage import make_test_binding


@pytest.mark.unit
class TestFsspecFilesystem:
    def test_isinstance_storage_filesystem(self, tmp_path: Path) -> None:
        fs = FsspecFilesystem.from_binding(make_test_binding(tmp_path))

        assert isinstance(fs, StorageFilesystem)

    def test_exists_and_open_use_storage_uri(self, tmp_path: Path) -> None:
        fs = FsspecFilesystem.from_binding(make_test_binding(tmp_path))
        uri = StorageUri.from_local_path(tmp_path / "test.txt")

        with fs.open(uri, "wb") as fh:
            fh.write(b"hello")

        assert fs.exists(uri)
        with fs.open(uri, "rb") as fh:
            assert fh.read() == b"hello"

    def test_find_returns_storage_uris(self, tmp_path: Path) -> None:
        fs = FsspecFilesystem.from_binding(make_test_binding(tmp_path))
        root = StorageUri.from_local_path(tmp_path)
        uri = root.join("nested", "test.txt")

        fs.makedirs(uri.parent(), exist_ok=True)
        with fs.open(uri, "wb") as fh:
            fh.write(b"hello")

        assert uri in fs.find(root)

    def test_read_bytes_roundtrip(self, tmp_path: Path) -> None:
        fs = FsspecFilesystem.from_binding(make_test_binding(tmp_path))
        uri = StorageUri.from_local_path(tmp_path / "meta.json")
        with fs.open(uri, "wb") as fh:
            fh.write(b'{"k": 1}')

        assert fs.read_bytes(uri) == b'{"k": 1}'

    def test_read_bytes_uses_single_shot_cat_file_not_open(self) -> None:
        """read_bytes must issue a plain GET (cat_file), never the conditional
        range-cached open().read() that crashes on a concurrent-mutation 412.

        Uses an isolated fake raw fs (NOT the shared LocalFileSystem singleton,
        which must never be mutated by a test).
        """
        calls: list[str] = []

        class _FakeRawFs:
            def cat_file(self, path: str) -> bytes:
                calls.append(path)
                return b"payload"

            def open(self, *_args: object, **_kwargs: object):
                raise AssertionError("read_bytes must not use open() (conditional fetch)")

        fs = object.__new__(FsspecFilesystem)
        fs._fs = _FakeRawFs()  # type: ignore[attr-defined]
        uri = StorageUri.parse("s3://bucket/prefix/zarr.json")

        assert fs.read_bytes(uri) == b"payload"
        assert calls == ["bucket/prefix/zarr.json"]

    def test_to_path_uses_bucket_key_for_remote(self) -> None:
        fs = object.__new__(FsspecFilesystem)

        assert (
            fs._to_path(StorageUri.parse("s3://bucket/prefix/file.bin")) == "bucket/prefix/file.bin"
        )


@pytest.mark.unit
class TestObstoreFilesystem:
    def test_isinstance_storage_filesystem(self, tmp_path: Path) -> None:
        fs = ObstoreFilesystem.from_local(str(tmp_path))

        assert isinstance(fs, StorageFilesystem)

    def test_write_and_read_use_storage_uri(self, tmp_path: Path) -> None:
        root = StorageUri.from_local_path(tmp_path)
        fs = ObstoreFilesystem.from_local(str(tmp_path))
        uri = root.join("hello.txt")

        with fs.open(uri, "wb") as fh:
            fh.write(b"obstore works")

        assert fs.exists(uri)
        with fs.open(uri, "rb") as fh:
            assert fh.read() == b"obstore works"

    def test_read_bytes_roundtrip(self, tmp_path: Path) -> None:
        root = StorageUri.from_local_path(tmp_path)
        fs = ObstoreFilesystem.from_local(str(tmp_path))
        uri = root.join("meta.json")
        with fs.open(uri, "wb") as fh:
            fh.write(b'{"k": 2}')

        assert fs.read_bytes(uri) == b'{"k": 2}'

    def test_put_uses_storage_uris(self, tmp_path: Path) -> None:
        root = StorageUri.from_local_path(tmp_path)
        fs = ObstoreFilesystem.from_local(str(tmp_path))
        local_file = tmp_path / "local.txt"
        local_file.write_bytes(b"put test")
        dst = root.join("remote.txt")

        fs.put(StorageUri.from_local_path(local_file), dst)

        assert fs.exists(dst)

    def test_resolve_path_strips_bucket_for_s3(self) -> None:
        key = ObstoreFilesystem._test_resolve_path(StorageUri.parse("s3://bucket/prefix/file.bin"))

        assert key == "prefix/file.bin"
        assert "bucket" not in key

    def test_resolve_path_strips_store_prefix_for_s3(self) -> None:
        key = ObstoreFilesystem._test_resolve_path(
            StorageUri.parse("s3://bucket/data/product.zarr/chunk.0"),
            store_prefix="data/product.zarr",
        )

        assert key == "chunk.0"

    def test_resolve_path_returns_empty_for_root_uri(self) -> None:
        key = ObstoreFilesystem._test_resolve_path(
            StorageUri.parse("s3://bucket/data/product.zarr"),
            store_prefix="data/product.zarr",
        )

        assert key == ""


@pytest.mark.unit
class TestCreateFilesystemDispatch:
    def test_fsspec_driver(self, tmp_path: Path) -> None:
        result = create_filesystem(make_test_binding(tmp_path, driver="fsspec"))

        assert isinstance(result, FsspecFilesystem)

    def test_obstore_driver(self, tmp_path: Path) -> None:
        result = create_filesystem(make_test_binding(tmp_path, driver="obstore"))

        assert isinstance(result, ObstoreFilesystem)
