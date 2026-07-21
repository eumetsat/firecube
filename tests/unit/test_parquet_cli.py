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

from typing import Any

from firecube.cli.parquet import _list_parquet_files
from firecube.core.storage.uri import StorageUri


class _FakeFs:
    def __init__(self, *, files: set[StorageUri], directories: set[StorageUri]) -> None:
        self._files = files
        self._directories = directories

    def exists(self, uri: StorageUri) -> bool:
        return uri in self._files or uri in self._directories

    def isdir(self, uri: StorageUri) -> bool:
        return uri in self._directories

    def find(self, uri: StorageUri) -> list[StorageUri]:
        prefix = f"{uri.path.rstrip('/')}/"
        return sorted(
            (entry for entry in self._files if entry.path.startswith(prefix)),
            key=lambda u: u.to_str(),
        )

    def open(self, uri: StorageUri, mode: str = "rb") -> Any:
        raise NotImplementedError

    def read_bytes(self, uri: StorageUri) -> bytes:
        raise NotImplementedError

    def rm(self, uri: StorageUri, recursive: bool = False) -> None:
        raise NotImplementedError

    def put(self, src_uri: StorageUri, dst_uri: StorageUri) -> None:
        raise NotImplementedError

    def info(self, uri: StorageUri) -> dict[str, Any]:
        raise NotImplementedError

    def capabilities(self) -> set[type]:
        return set()


def test_list_parquet_files_descends_into_directory_named_parquet() -> None:
    fs = _FakeFs(
        files={
            StorageUri.parse("s3://bucket/product.parquet/mwir_1km/part-000.parquet"),
            StorageUri.parse("s3://bucket/product.parquet/swir_500m/part-001.parquet"),
        },
        directories={
            StorageUri.parse("s3://bucket/product.parquet"),
            StorageUri.parse("s3://bucket/product.parquet/mwir_1km"),
            StorageUri.parse("s3://bucket/product.parquet/swir_500m"),
        },
    )

    files = _list_parquet_files(fs, StorageUri.parse("s3://bucket/product.parquet"))

    assert files == [
        StorageUri.parse("s3://bucket/product.parquet/mwir_1km/part-000.parquet"),
        StorageUri.parse("s3://bucket/product.parquet/swir_500m/part-001.parquet"),
    ]


def test_list_parquet_files_keeps_single_file_path() -> None:
    fs = _FakeFs(
        files={StorageUri.parse("s3://bucket/product.parquet")},
        directories=set(),
    )

    files = _list_parquet_files(fs, StorageUri.parse("s3://bucket/product.parquet"))

    assert files == [StorageUri.parse("s3://bucket/product.parquet")]
