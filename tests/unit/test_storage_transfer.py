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

import importlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

StorageDriverConfig = importlib.import_module(
    "firecube.core.storage.driver_config"
).StorageDriverConfig
StorageUri = importlib.import_module("firecube.core.storage.uri").StorageUri
storage_transfer = importlib.import_module("firecube.core.storage.transfer")
copy_file = storage_transfer.copy_file
session_for_uri = storage_transfer.session_for_uri


@dataclass(slots=True)
class _Product:
    product_uri: Any


class _NoIoSession:
    def __init__(self, product_uri: Any) -> None:
        self.product = _Product(product_uri)
        self.fs_called = False

    def fs(self) -> Any:
        self.fs_called = True
        raise AssertionError("Phase 1 validation should fail before fs() is called")


def _session(uri: Any) -> Any:
    return _NoIoSession(uri)


def test_local_to_local_copy_works(tmp_path: Path) -> None:
    src_path = tmp_path / "src.txt"
    dst_path = tmp_path / "dst.txt"
    src_path.write_text("payload", encoding="utf-8")

    copy_file(
        StorageUri.from_local_path(src_path),
        StorageUri.from_local_path(dst_path),
    )

    assert dst_path.read_text(encoding="utf-8") == "payload"


def test_remote_source_missing_source_session_raises(tmp_path: Path) -> None:
    dst = StorageUri.from_local_path(tmp_path / "dst.txt")

    with pytest.raises(ValueError, match="source_session"):
        copy_file(StorageUri.parse("s3://source-bucket/object.tgm"), dst)


def test_remote_destination_missing_target_session_raises(tmp_path: Path) -> None:
    src_path = tmp_path / "src.txt"
    src_path.write_text("payload", encoding="utf-8")

    with pytest.raises(ValueError, match="target_session"):
        copy_file(
            StorageUri.from_local_path(src_path),
            StorageUri.parse("s3://target-bucket/object.tgm"),
        )


def test_mismatched_source_session_binding_fails_before_io(tmp_path: Path) -> None:
    session = _session(StorageUri.parse("s3://wrong-bucket/object.tgm"))

    with pytest.raises(ValueError, match="source_session is bound"):
        copy_file(
            StorageUri.parse("s3://source-bucket/object.tgm"),
            StorageUri.from_local_path(tmp_path / "dst.txt"),
            source_session=session,
        )

    assert not session.fs_called


def test_mismatched_target_session_binding_fails_before_io(tmp_path: Path) -> None:
    src_path = tmp_path / "src.txt"
    src_path.write_text("payload", encoding="utf-8")
    session = _session(StorageUri.parse("s3://wrong-bucket/object.tgm"))

    with pytest.raises(ValueError, match="target_session is bound"):
        copy_file(
            StorageUri.from_local_path(src_path),
            StorageUri.parse("s3://target-bucket/object.tgm"),
            target_session=session,
        )

    assert not session.fs_called


def test_session_for_uri_creates_session_with_correct_product_uri() -> None:
    uri = StorageUri.parse("s3://archive-bucket/path/archive.tgm")

    session = session_for_uri(uri, StorageDriverConfig(driver="fsspec"))

    assert session.product.product_uri == uri
    assert session.product.control_root_uri == uri.parent().join(".firecube")
    assert session.product.product_name == "archive.tgm"
    assert session.product.format == "tensogram"


def test_local_to_local_copy_creates_destination_subdirectories(tmp_path: Path) -> None:
    src_path = tmp_path / "src.txt"
    dst_path = tmp_path / "nested" / "deeper" / "dst.txt"
    src_path.write_text("payload", encoding="utf-8")

    copy_file(
        StorageUri.from_local_path(src_path),
        StorageUri.from_local_path(dst_path),
    )

    assert dst_path.read_text(encoding="utf-8") == "payload"


def test_session_for_uri_accepts_format_override() -> None:
    uri = StorageUri.from_local_path("/tmp/archive.parquet")

    session = session_for_uri(uri, StorageDriverConfig(driver="fsspec"), format="parquet")

    assert session.product.product_uri == uri
    assert session.product.format == "parquet"
