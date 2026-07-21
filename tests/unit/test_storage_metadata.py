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

"""Tests for ``session.upload_tree`` zarr.json metadata preservation."""

from __future__ import annotations

import json
from pathlib import Path

import boto3
import moto  # pyright: ignore[reportMissingImports]
import pytest

from firecube.core.config import StorageConfig
from firecube.core.storage.uri import StorageUri  # pyright: ignore[reportMissingImports]
from tests.helpers.storage import make_test_session

pytestmark = pytest.mark.s3


def _session(uri: str, *, storage_config: StorageConfig | None = None):
    path = Path(uri)
    if storage_config and storage_config.storage_type == "s3":
        return make_test_session(
            path.parent,
            protocol="s3",
            authority=getattr(storage_config, "bucket", None) or "test-bucket",
        )
    return make_test_session(path.parent, product=path.name)


def _s3_storage_config(bucket: str = "test-bucket") -> StorageConfig:
    config = StorageConfig(
        storage_type="s3",
        region="us-east-1",
        access_key="testing",
        secret_key="testing",
        storage_driver="fsspec",
    )
    config.bucket = bucket  # type: ignore[attr-defined]
    return config


def _write_zarr_json(path: Path, shape: list[int]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"node_type": "array", "shape": shape, "data_type": "float32"}))


class _LocalMirrorFs:
    def __init__(self, base: Path, root: str) -> None:
        self.base = base
        self.root = root.strip("/")

    def _local_path(self, path):
        rel_str = path.to_str() if hasattr(path, "to_str") else str(path)
        if "://" in rel_str:
            rel_str = rel_str.split("://", 1)[1]
        rel = rel_str.strip("/")
        if self.root and rel.startswith(self.root):
            rel = rel[len(self.root) :].strip("/")
        return self.base / rel

    def open(self, path, mode: str = "rb"):
        local_path = self._local_path(path)
        if any(flag in mode for flag in ("w", "a", "+")):
            local_path.parent.mkdir(parents=True, exist_ok=True)
        return local_path.open(mode)

    def makedirs(self, path, exist_ok: bool = False) -> None:
        self._local_path(path).mkdir(parents=True, exist_ok=exist_ok)


def test_local_write_preserves_larger_target_shape(tmp_path):
    """``session.upload_tree`` must not overwrite target zarr.json with smaller shape."""
    target = tmp_path / "target.zarr"
    source = tmp_path / "source.zarr"

    # Target has cumulative shape [100, 3]
    _write_zarr_json(target / "G" / "val" / "zarr.json", [100, 3])
    # Source (temp) has only new batch shape [40, 3]
    _write_zarr_json(source / "G" / "val" / "zarr.json", [40, 3])
    # Add a chunk file in source to verify it gets copied
    chunk = source / "G" / "val" / "c"
    chunk.mkdir(parents=True, exist_ok=True)
    (chunk / "0").write_bytes(b"chunk")

    _session(str(target)).upload_tree(
        StorageUri.from_local_path(source), StorageUri.from_local_path(target)
    )

    result = json.loads((target / "G" / "val" / "zarr.json").read_text())
    assert result["shape"][0] >= 100, f"zarr.json shape overwritten! got {result['shape']}"
    assert (target / "G" / "val" / "c" / "0").exists(), "Chunk file not copied"


def test_local_write_fresh_target_uses_source_shape(tmp_path):
    """``session.upload_tree`` to a fresh target uses source metadata as-is."""
    target = tmp_path / "target.zarr"
    source = tmp_path / "source.zarr"
    _write_zarr_json(source / "G" / "val" / "zarr.json", [40, 3])

    _session(str(target)).upload_tree(
        StorageUri.from_local_path(source), StorageUri.from_local_path(target)
    )

    result = json.loads((target / "G" / "val" / "zarr.json").read_text())
    assert result["shape"] == [40, 3]


def test_local_write_source_larger_shape_wins(tmp_path):
    """When source has larger shape[0] than target, source shape is kept."""
    target = tmp_path / "target.zarr"
    source = tmp_path / "source.zarr"
    _write_zarr_json(target / "G" / "val" / "zarr.json", [10, 3])
    _write_zarr_json(source / "G" / "val" / "zarr.json", [50, 3])

    _session(str(target)).upload_tree(
        StorageUri.from_local_path(source), StorageUri.from_local_path(target)
    )

    result = json.loads((target / "G" / "val" / "zarr.json").read_text())
    assert result["shape"][0] == 50


def test_s3_write_preserves_larger_target_shape(tmp_path, monkeypatch):
    remote_root = tmp_path / "remote"
    monkeypatch.setattr(
        "firecube.core.storage.session.create_filesystem",
        lambda binding: _LocalMirrorFs(remote_root, "test-bucket/product.zarr"),
    )

    with moto.mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="test-bucket")
        target_meta = json.dumps({"node_type": "array", "shape": [100, 3], "data_type": "float32"})
        (remote_root / "G" / "val").mkdir(parents=True, exist_ok=True)
        (remote_root / "G" / "val" / "zarr.json").write_text(target_meta)

        source = tmp_path / "product.zarr"
        arr = source / "G" / "val"
        arr.mkdir(parents=True)
        (arr / "zarr.json").write_text(
            json.dumps({"node_type": "array", "shape": [40, 3], "data_type": "float32"})
        )
        chunk = arr / "c"
        chunk.mkdir()
        (chunk / "0").write_bytes(b"data")

        session = _session("s3://test-bucket/product.zarr", storage_config=_s3_storage_config())
        session.upload_tree(
            StorageUri.from_local_path(source),
            StorageUri.parse("s3://test-bucket/product.zarr"),
        )

        result = json.loads((remote_root / "G" / "val" / "zarr.json").read_text())
        assert result["shape"][0] >= 100, f"zarr.json overwritten! got {result['shape']}"


def test_s3_write_fresh_target_uploads_zarr_json(tmp_path, monkeypatch):
    remote_root = tmp_path / "remote"
    monkeypatch.setattr(
        "firecube.core.storage.session.create_filesystem",
        lambda binding: _LocalMirrorFs(remote_root, "test-bucket/product.zarr"),
    )

    with moto.mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="test-bucket")
        source = tmp_path / "product.zarr"
        arr = source / "G" / "val"
        arr.mkdir(parents=True)
        (arr / "zarr.json").write_text(
            json.dumps({"node_type": "array", "shape": [40, 3], "data_type": "float32"})
        )

        _session("s3://test-bucket/product.zarr", storage_config=_s3_storage_config()).upload_tree(
            StorageUri.from_local_path(source),
            StorageUri.parse("s3://test-bucket/product.zarr"),
        )

        result = json.loads((remote_root / "G" / "val" / "zarr.json").read_text())
        assert result["shape"] == [40, 3]


def test_s3_write_non_zarr_files_always_uploaded(tmp_path, monkeypatch):
    remote_root = tmp_path / "remote"
    monkeypatch.setattr(
        "firecube.core.storage.session.create_filesystem",
        lambda binding: _LocalMirrorFs(remote_root, "test-bucket/product.zarr"),
    )

    with moto.mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="test-bucket")
        source = tmp_path / "product.zarr"
        chunk = source / "G" / "val" / "c"
        chunk.mkdir(parents=True)
        (chunk / "0").write_bytes(b"chunk_data")

        _session("s3://test-bucket/product.zarr", storage_config=_s3_storage_config()).upload_tree(
            StorageUri.from_local_path(source),
            StorageUri.parse("s3://test-bucket/product.zarr"),
        )

        assert (remote_root / "G" / "val" / "c" / "0").read_bytes() == b"chunk_data"
