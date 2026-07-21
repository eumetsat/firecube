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

"""Integration tests for remote archive artifacts."""

from __future__ import annotations

from pathlib import Path

import xarray as xr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.storage.uri import StorageUri
from tests.integration.test_tensogram_archive import (
    _local_env,
    _make_test_dataset,
    assert_restored_dataset_matches_source,
)

_LOCAL_STORAGE_FLAGS = ["--storage-type", "local", "--storage-driver", "fsspec"]


def _local_remote_uri(path: Path) -> str:
    return StorageUri.from_local_path(path).to_str()


def test_archive_remote_tgm_round_trip(tmp_path: Path) -> None:
    runner = CliRunner()
    env = _local_env(str(tmp_path))
    source = tmp_path / "product.zarr"
    remote_dir = tmp_path / "remote-artifacts"
    remote_dir.mkdir()
    remote_tgm = remote_dir / "product.tgm"
    restored = tmp_path / "restored.zarr"

    ds = _make_test_dataset()
    ds.to_zarr(str(source))

    create_result = runner.invoke(
        cli,
        [
            "archive",
            "create",
            "--source",
            StorageUri.from_local_path(source).to_str(),
            "--archive",
            _local_remote_uri(remote_tgm),
            *_LOCAL_STORAGE_FLAGS,
        ],
        env=env,
    )
    assert create_result.exit_code == 0, create_result.output
    assert remote_tgm.exists()
    # CLI echoes bare path for local file:// targets (zarr_to_tgm sets result["target"]
    # to str(target_path) when target is local; remote targets get the canonical URI).
    assert f"Archive created: {remote_tgm}" in create_result.output

    restore_result = runner.invoke(
        cli,
        [
            "archive",
            "restore",
            "--archive",
            _local_remote_uri(remote_tgm),
            "--target",
            StorageUri.from_local_path(restored).to_str(),
            *_LOCAL_STORAGE_FLAGS,
        ],
        env=env,
    )
    assert restore_result.exit_code == 0, restore_result.output
    assert restored.exists()

    restored_ds = xr.open_zarr(str(restored))
    try:
        assert_restored_dataset_matches_source(ds, restored_ds)
    finally:
        restored_ds.close()
