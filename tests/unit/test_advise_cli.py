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

from dataclasses import dataclass
from pathlib import Path
from unittest.mock import patch

import numpy as np
import zarr
from click.testing import CliRunner, Result

from firecube.cli.advise import advise


@dataclass
class _FakeStorageConfig:
    base_uri: str
    storage_driver: str = "fsspec"
    endpoint_url: str | None = None
    access_key: str | None = None
    secret_key: str | None = None
    region: str | None = None
    path_style: bool = True
    storage_type: str = "local"
    target_path: str | None = None
    bucket: str | None = None

    @property
    def target_uri(self) -> str:
        return self.base_uri


def _make_zarr_store(
    tmp_path: Path,
    *,
    time_chunk: int = 10,
    time_size: int = 20,
    spatial: int = 8,
) -> Path:
    store_path = tmp_path / "product.zarr"
    root = zarr.open_group(str(store_path), mode="w")
    grp = root.require_group("SEVIRI_L15")
    grp.create_array(
        "temp",
        shape=(time_size, spatial, spatial),
        chunks=(time_chunk, spatial, spatial),
        dtype=np.float32,
    )
    return store_path


def _invoke(tmp_path: Path, product: str, group: str) -> Result:
    runner = CliRunner()
    cfg = _FakeStorageConfig(base_uri=str(tmp_path))
    product_arg = product if "://" in product else (tmp_path / product).as_uri()
    with patch("firecube.cli.advise.get_storage_config", return_value=cfg):
        return runner.invoke(advise, ["batch-size", "--product", product_arg, "--group", group])


def test_batch_size_recommends_chunk_aligned(tmp_path: Path) -> None:
    _make_zarr_store(tmp_path, time_chunk=10)
    result = _invoke(tmp_path, "product.zarr", "SEVIRI_L15")
    assert result.exit_code == 0
    assert "pipeline_batch_size=10" in result.output


def test_batch_size_chunk_one(tmp_path: Path) -> None:
    _make_zarr_store(tmp_path, time_chunk=1)
    result = _invoke(tmp_path, "product.zarr", "SEVIRI_L15")
    assert result.exit_code == 0
    assert "any size works" in result.output.lower() or "any batch size" in result.output.lower()


def test_batch_size_no_time_dim(tmp_path: Path) -> None:
    store_path = tmp_path / "product.zarr"
    root = zarr.open_group(str(store_path), mode="w")
    grp = root.require_group("COORDS")
    grp.create_array("lat", shape=(100,), chunks=(100,), dtype=np.float32)
    result = _invoke(tmp_path, "product.zarr", "COORDS")
    assert result.exit_code == 0
    assert "no time dimension" in result.output.lower()


def test_batch_size_nonexistent_store(tmp_path: Path) -> None:
    result = _invoke(tmp_path, "nonexistent.zarr", "FOO")
    assert result.exit_code != 0
    assert "Error opening store:" in result.output
    assert "nonexistent.zarr does not exist" in result.output
