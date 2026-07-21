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

"""Driver-parity tests for control-plane deletion paths."""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any

import numpy as np
import pytest

from firecube.core.config import StorageConfig
from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.deletion import DeletionEngine
from firecube.core.controlplane.types import ChunkInfo, DeletionPlan
from firecube.core.filesystem import StorageFilesystem, create_filesystem
from firecube.core.storage.uri import StorageUri
from tests.helpers.storage import assert_no_fsspec_bypass, make_test_binding

pytestmark = pytest.mark.unit

ManifestRepository = importlib.import_module("firecube.core.controlplane.repo").ManifestRepository


class _RmSpyFilesystem:
    def __init__(self, wrapped: StorageFilesystem) -> None:
        self._wrapped = wrapped
        self.rm_calls: list[Any] = []

    def __getattr__(self, name: str) -> Any:
        return getattr(self._wrapped, name)

    def rm(self, uri: Any, recursive: bool = False) -> None:
        assert isinstance(uri, StorageUri)
        self.rm_calls.append(uri)
        self._wrapped.rm(uri, recursive=recursive)


class _DeletionSpyFilesystem:
    def __init__(self, wrapped: StorageFilesystem, expected_suffix: str) -> None:
        self._wrapped = wrapped
        self.expected_suffix = expected_suffix
        self.rm_calls: list[Any] = []

    def __getattr__(self, name: str) -> Any:
        return getattr(self._wrapped, name)

    def exists(self, uri: StorageUri | str) -> bool:
        if isinstance(uri, str):
            return uri.endswith(self.expected_suffix)
        return self._wrapped.exists(uri)

    def open(self, uri: StorageUri, mode: str = "rb") -> Any:
        return self._wrapped.open(uri, mode)

    def read_bytes(self, uri: StorageUri) -> bytes:
        return self._wrapped.read_bytes(uri)

    def find(self, uri: StorageUri) -> list[StorageUri]:
        return self._wrapped.find(uri)

    def isdir(self, uri: StorageUri) -> bool:
        return self._wrapped.isdir(uri)

    def rm(self, uri: StorageUri | str, recursive: bool = False) -> None:
        self.rm_calls.append(uri)
        if not isinstance(uri, str):
            self._wrapped.rm(uri, recursive=recursive)

    def put(self, src_uri: StorageUri, dst_uri: StorageUri) -> None:
        self._wrapped.put(src_uri, dst_uri)

    def info(self, uri: StorageUri) -> dict[str, Any]:
        return self._wrapped.info(uri)

    def capabilities(self) -> set[type]:
        return self._wrapped.capabilities()


def _single_chunk_plan(product: str, key: str) -> DeletionPlan:
    chunk = ChunkInfo(
        key=key,
        product=product,
        chunk_type="chunk",
        size=1,
        timestamp=1.0,
        manifest_path=f"file:///tmp/{product}/.firecube",
    )
    return DeletionPlan(
        chunks=[chunk],
        total_size=1,
        products_affected={product},
        manifest_files=set(),
    )


def test_chunk_manager_deletion_uses_injected_filesystem(tmp_path: Path) -> None:
    product = "product.zarr"
    key = "data/chunk"
    binding = make_test_binding(tmp_path, product=product)
    fs = _DeletionSpyFilesystem(
        create_filesystem(binding),
        expected_suffix=f"/{product}/{key}",
    )
    manager = ChunkManager(binding=binding, workspace=tmp_path / "workspace", filesystem=fs)

    result = manager.execute_deletion(
        _single_chunk_plan(product, key),
        delete_storage=True,
        delete_manifest=False,
        storage_config=None,
        dry_run=False,
    )

    assert result["storage_errors"] == []
    deleted_chunk_paths = [
        uri for uri in fs.rm_calls if isinstance(uri, str) and uri.endswith(f"/{product}/{key}")
    ]
    assert deleted_chunk_paths, "Injected filesystem was not used for storage deletion"


def test_local_deletion_without_target_base_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    product = "product.zarr"
    key = "data/chunk"
    relative_chunk = tmp_path / product / key
    relative_chunk.parent.mkdir(parents=True)
    relative_chunk.write_text("payload", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    repo = ManifestRepository(
        binding=make_test_binding(tmp_path, product=product), workspace=tmp_path
    )
    engine = DeletionEngine(repo)

    result = engine.execute_deletion(
        _single_chunk_plan(product, key),
        delete_storage=True,
        delete_manifest=False,
        storage_config=StorageConfig(storage_type="local"),
        dry_run=False,
    )

    assert result["storage_errors"], "Deletion without a local target base should fail closed"
    assert relative_chunk.exists(), "Relative-path fallback deleted storage unexpectedly"


def test_delete_spans_no_bypass(tmp_path: Path) -> None:
    import zarr

    product = "product.zarr"
    store_root = tmp_path / product
    store_root.mkdir(parents=True, exist_ok=True)

    root = zarr.open_group(store=str(store_root), mode="w", zarr_format=3)
    grp = root.require_group("data")
    arr = grp.create_array(
        "counts",
        shape=(2, 2),
        chunks=(1, 2),
        dtype="f4",
        dimension_names=("timestamp", "x"),
        overwrite=True,
    )
    arr[:] = np.ones((2, 2), dtype=np.float32)

    repo = ManifestRepository(
        binding=make_test_binding(tmp_path, product=product),
        workspace=tmp_path,
    )
    fs, base_uri = repo._get_fs(repo.base_uri)
    spy_fs = _RmSpyFilesystem(fs)

    def _get_spy_fs(uri: Any) -> tuple[_RmSpyFilesystem, Any]:
        return spy_fs, uri if isinstance(uri, StorageUri) else StorageUri.parse(str(uri))

    repo._get_fs = _get_spy_fs  # type: ignore[method-assign]
    engine = DeletionEngine(repo)
    span = ChunkInfo(
        key="span_run1_b1_data",
        product=product,
        chunk_type="span",
        size=0,
        timestamp=1.0,
        manifest_path=base_uri.join(product).join(".firecube").to_str(),
        record={
            "span": {
                "arrays": ["data/counts"],
                "time_index_ranges": [[0, 0]],
                "aligned": True,
            }
        },
    )

    with assert_no_fsspec_bypass():
        result = engine.delete_spans([span], dry_run=False, update_manifest=False)

    assert result["errors"] == []
    assert result["deleted_keys"] == 1
    chunk_rm_calls = [uri for uri in spy_fs.rm_calls if "/data/counts/c/" in uri.path]
    assert len(chunk_rm_calls) == 1
    assert chunk_rm_calls[0].path.endswith(f"/{product}/data/counts/c/0/0")
