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

import io
import json
from pathlib import Path
from typing import Any, cast

import boto3
import fsspec
import moto
import pyarrow as pa
import pytest

from firecube.core.config import StorageConfig
from firecube.core.controlplane import ChunkManager
from firecube.core.filesystem.ops import path_stats
from firecube.core.product.identity import ProductIdentity
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.uri import StorageUri
from firecube.core.zarr.validation import discover_groups
from firecube.ingestor.templates.generic import GenericParquetIngestor

pytestmark = pytest.mark.s3


class _FakeFsspecFs:
    def __init__(self, entries: dict[str, bytes]) -> None:
        self._entries = entries

    def exists(self, path: str) -> bool:
        return path in self._entries

    def open(self, path: str, mode: str = "r", encoding: str | None = None) -> Any:
        data = self._entries[path]
        if "b" in mode:
            return io.BytesIO(data)
        return io.StringIO(data.decode(encoding or "utf-8"))

    def ls(self, path: str, detail: bool = False) -> list[dict[str, str]]:
        prefix = path.rstrip("/") + "/"
        children: set[str] = set()
        for key in self._entries:
            if key.startswith(prefix):
                child = key[len(prefix) :].split("/", 1)[0]
                children.add(f"{prefix}{child}")
        return [{"name": child, "type": "directory"} for child in sorted(children)]

    def find(self, path: str, detail: bool = False) -> Any:
        prefix = path.rstrip("/") + "/"
        paths = [key for key in self._entries if key.startswith(prefix)]
        if detail:
            return {key: {"size": len(self._entries[key]), "type": "file"} for key in paths}
        return paths


class _FakeStorageFilesystem:
    def __init__(self, entries: dict[str, bytes] | None = None) -> None:
        self._entries = entries or {}

    def _key(self, uri: StorageUri) -> str:
        return uri.to_str()

    def exists(self, uri: StorageUri) -> bool:
        return self._key(uri) in self._entries

    def open(self, uri: StorageUri, mode: str = "rb", **kwargs: Any) -> Any:
        key = self._key(uri)
        encoding = kwargs.get("encoding", "utf-8")
        if "r" in mode:
            data = self._entries[key]
            if "b" in mode:
                return io.BytesIO(data)
            return io.StringIO(data.decode(encoding))
        return _RecordingWriter(self._entries, key)

    def find(self, uri: StorageUri) -> list[StorageUri]:
        prefix = uri.to_str().rstrip("/") + "/"
        return [StorageUri.parse(key) for key in self._entries if key.startswith(prefix)]

    def isdir(self, uri: StorageUri) -> bool:
        prefix = uri.to_str().rstrip("/") + "/"
        return any(key.startswith(prefix) for key in self._entries)

    def rm(self, uri: StorageUri, recursive: bool = False) -> None:
        del self._entries[self._key(uri)]

    def put(self, src_uri: StorageUri, dst_uri: StorageUri) -> None:
        self._entries[self._key(dst_uri)] = Path(src_uri.path).read_bytes()

    def info(self, uri: StorageUri) -> dict[str, Any]:
        data = self._entries[self._key(uri)]
        return {"name": self._key(uri), "size": len(data), "type": "file"}

    def capabilities(self) -> set[type]:
        return set()


class _RecordingWriter(io.BytesIO):
    def __init__(self, entries: dict[str, bytes], key: str) -> None:
        super().__init__()
        self._entries = entries
        self._key = key

    def close(self) -> None:
        if not self.closed:
            self._entries[self._key] = self.getvalue()
        super().close()


class _MotoS3Writer(io.BytesIO):
    def __init__(self, bucket: str, key: str) -> None:
        super().__init__()
        self._bucket = bucket
        self._key = key

    def close(self) -> None:
        if not self.closed:
            boto3.client("s3", region_name="us-east-1").put_object(
                Bucket=self._bucket,
                Key=self._key,
                Body=self.getvalue(),
            )
        super().close()


class _MotoStorageFilesystem(_FakeStorageFilesystem):
    def open(self, uri: StorageUri, mode: str = "rb", **kwargs: Any) -> Any:
        if "w" in mode:
            assert uri.authority is not None
            return _MotoS3Writer(uri.authority, uri.path.lstrip("/"))
        return super().open(uri, mode, **kwargs)


def _obstore_s3_config() -> StorageConfig:
    return StorageConfig(
        storage_type="s3",
        storage_driver="obstore",
        access_key="test",
        secret_key="test",
        region="us-east-1",
    )


def _patch_url_to_fs(monkeypatch: pytest.MonkeyPatch, fake_fs: _FakeFsspecFs) -> dict[str, int]:
    calls = {"count": 0}

    def _url_to_fs(uri: str, **kwargs: Any) -> tuple[_FakeFsspecFs, str]:
        _ = kwargs
        calls["count"] += 1
        parsed = StorageUri.parse(uri)
        root = f"{parsed.authority}{parsed.path}" if parsed.authority else parsed.path
        return fake_fs, root.rstrip("/")

    monkeypatch.setattr(fsspec.core, "url_to_fs", _url_to_fs)
    return calls


def test_discover_groups_uses_obstore_driver_when_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store_uri = "s3://test-bucket/product.zarr"
    entries = {
        "s3://test-bucket/product.zarr/zarr.json": json.dumps({"node_type": "group"}).encode(),
        "s3://test-bucket/product.zarr/G/zarr.json": json.dumps({"node_type": "group"}).encode(),
    }
    fsspec_entries = {
        "test-bucket/product.zarr/zarr.json": entries["s3://test-bucket/product.zarr/zarr.json"],
        "test-bucket/product.zarr/G/zarr.json": entries[
            "s3://test-bucket/product.zarr/G/zarr.json"
        ],
    }
    url_to_fs_calls = _patch_url_to_fs(monkeypatch, _FakeFsspecFs(fsspec_entries))
    obstore_calls: list[StorageBinding] = []

    def _from_binding(binding: StorageBinding) -> _FakeStorageFilesystem:
        obstore_calls.append(binding)
        return _FakeStorageFilesystem(entries)

    monkeypatch.setattr(
        "firecube.core.filesystem.obstore_backend.ObstoreFilesystem.from_binding",
        _from_binding,
    )

    groups = discover_groups(store_uri, storage_config=_obstore_s3_config())

    assert url_to_fs_calls["count"] == 0
    assert [binding.driver.driver for binding in obstore_calls] == ["obstore"]
    assert groups == ["/", "G"]


def test_path_stats_uses_obstore_driver_when_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    uri = "s3://test-bucket/product.zarr"
    entries = {
        "s3://test-bucket/product.zarr/a.bin": b"abc",
        "s3://test-bucket/product.zarr/nested/b.bin": b"defg",
        "s3://test-bucket/product.zarr/.firecube/ignored.json": b"ignored",
    }
    fsspec_entries = {key.removeprefix("s3://"): value for key, value in entries.items()}
    url_to_fs_calls = _patch_url_to_fs(monkeypatch, _FakeFsspecFs(fsspec_entries))
    obstore_calls: list[StorageBinding] = []

    def _from_binding(binding: StorageBinding) -> _FakeStorageFilesystem:
        obstore_calls.append(binding)
        return _FakeStorageFilesystem(entries)

    monkeypatch.setattr(
        "firecube.core.filesystem.obstore_backend.ObstoreFilesystem.from_binding",
        _from_binding,
    )

    stats = path_stats(uri, storage_config=_obstore_s3_config())

    assert url_to_fs_calls["count"] == 0
    assert [binding.driver.driver for binding in obstore_calls] == ["obstore"]
    assert stats == {"bytes": 7, "files": 2}


def test_generic_parquet_remote_write_uses_obstore_driver_when_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bucket = "test-bucket"
    output_path = f"s3://{bucket}/product.parquet/part-0.parquet"
    open_fsspec_calls = {"count": 0}
    obstore_calls: list[StorageBinding] = []

    def _open_fsspec_url(*args: Any, **kwargs: Any) -> tuple[Any, str]:
        _ = (args, kwargs)
        open_fsspec_calls["count"] += 1
        raise AssertionError("write_parquet must not use open_fsspec_url under obstore")

    def _from_binding(binding: StorageBinding) -> _MotoStorageFilesystem:
        obstore_calls.append(binding)
        return _MotoStorageFilesystem()

    monkeypatch.setattr(
        "firecube.ingestor.templates.generic.open_fsspec_url",
        _open_fsspec_url,
        raising=False,
    )
    monkeypatch.setattr("firecube.core.filesystem.ops._open_fsspec_url", _open_fsspec_url)
    monkeypatch.setattr(
        "firecube.core.filesystem.obstore_backend.ObstoreFilesystem.from_binding",
        _from_binding,
    )

    class _ParquetProbe(GenericParquetIngestor):
        PRODUCT_NAME = "parquet_probe"
        name = "parquet_probe"

        def build_dataset(self, group: str, batch: Any, ctx: Any) -> Any | None:
            _ = (group, batch, ctx)
            return None

    binding = StorageBinding(
        identity=ProductIdentity.from_uri(
            StorageUri.parse(f"s3://{bucket}/product.parquet"),
            "parquet",
            product_name="product.parquet",
        ),
        driver=StorageDriverConfig.from_storage_config(_obstore_s3_config()),
    )
    ingestor = _ParquetProbe(chunk_manager=ChunkManager(binding=binding))
    table = pa.table({"value": [1, 2, 3]})

    with moto.mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=bucket)

        rows = ingestor.write_parquet(
            table,
            output_path=output_path,
            storage_options=None,
            ctx=cast(Any, None),
        )

        s3.head_object(Bucket=bucket, Key="product.parquet/part-0.parquet")

    assert rows == 3
    assert open_fsspec_calls["count"] == 0
    assert [binding.driver.driver for binding in obstore_calls] == ["obstore"]
