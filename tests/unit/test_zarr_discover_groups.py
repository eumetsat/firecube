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

import json
from typing import cast

import pytest

from firecube.core.filesystem.protocol import StorageFilesystem
from firecube.core.storage.uri import StorageUri
from firecube.core.zarr.validation import _discover_groups_with_fs, discover_groups


def test_discover_groups_strict_raises_on_listing_failure(monkeypatch) -> None:
    class BrokenFs:
        def exists(self, path):
            return False

        def ls(self, path, detail=True):
            raise RuntimeError("boom")

    monkeypatch.setattr(
        "firecube.core.zarr.validation._open_fs",
        lambda store_uri, storage_config=None, storage_options=None: (BrokenFs(), "root"),
    )

    with pytest.raises(RuntimeError, match="Failed to discover Zarr groups"):
        discover_groups("s3://bucket/store.zarr", strict=True)


def test_discover_groups_traverses_directories_without_chunk_walk(tmp_path) -> None:
    store = tmp_path / "cube.zarr"
    store.mkdir()
    (store / "zarr.json").write_text(json.dumps({"node_type": "group"}), encoding="utf-8")

    public_group = store / "group_a"
    public_group.mkdir()
    (public_group / "zarr.json").write_text(json.dumps({"node_type": "group"}), encoding="utf-8")

    array_dir = public_group / "payload"
    array_dir.mkdir()
    (array_dir / "zarr.json").write_text(json.dumps({"node_type": "array"}), encoding="utf-8")

    chunk_dir = array_dir / "c"
    chunk_dir.mkdir()
    (chunk_dir / "0").write_text("chunk", encoding="utf-8")

    groups = discover_groups(str(store), strict=True)

    assert groups == ["/", "group_a"]


def test_discover_groups_with_fs_strict_raises_on_failure() -> None:
    class BrokenFs:
        def find(self, uri):
            raise ValueError("driver listing failed")

    uri = StorageUri.parse("s3://bucket/store.zarr")

    with pytest.raises(RuntimeError, match="Failed to discover Zarr groups") as exc_info:
        _discover_groups_with_fs(cast(StorageFilesystem, BrokenFs()), uri, strict=True)

    assert isinstance(exc_info.value.__cause__, ValueError)


def test_discover_groups_with_fs_non_strict_returns_empty_on_failure() -> None:
    class BrokenFs:
        def find(self, uri):
            raise ValueError("driver listing failed")

    uri = StorageUri.parse("s3://bucket/store.zarr")

    groups = _discover_groups_with_fs(cast(StorageFilesystem, BrokenFs()), uri, strict=False)

    assert groups == []
