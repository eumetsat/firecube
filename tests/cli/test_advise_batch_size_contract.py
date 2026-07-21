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
from typing import Any

import zarr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.zarr.io import ZarrIO


def _invoke_batch_size(args: list[str]):
    return CliRunner().invoke(cli, ["advise", "batch-size", *args])


def test_file_uri_no_storage_flags(tmp_local_zarr: Path) -> None:
    result = _invoke_batch_size(["-p", tmp_local_zarr.as_uri(), "-g", "g1"])

    assert result.exit_code == 0
    assert "Missing option" not in result.output
    assert "Traceback" not in result.output


def test_s3_uri_infers_storage_type(monkeypatch, tmp_path: Path) -> None:
    backing_store = zarr.open_group(str(tmp_path / "remote.zarr"), mode="w")
    group = backing_store.create_group("g1")
    group.create_array("x", shape=(4, 2), chunks=(2, 2), dtype="i4")
    observed: dict[str, str] = {}

    def fake_open_group(self: ZarrIO, uri: Any, mode: str = "r") -> zarr.Group:
        assert mode == "r"
        observed["uri"] = uri.to_str()
        observed["protocol"] = self._session.product.product_uri.protocol
        observed["driver"] = self._session.driver.driver
        return backing_store

    monkeypatch.setattr(ZarrIO, "open_group", fake_open_group)

    result = _invoke_batch_size(["-p", "s3://bucket/x.zarr", "-g", "g1"])

    assert result.exit_code == 0, result.output
    assert "Recommended: --option pipeline_batch_size=2" in result.output
    assert observed == {
        "uri": "s3://bucket/x.zarr",
        "protocol": "s3",
        "driver": "fsspec",
    }
    assert "Traceback" not in result.output


def test_missing_group_clean_error(tmp_path: Path) -> None:
    store_path = tmp_path / "test.zarr"
    import zarr

    zarr.open_group(str(store_path), mode="w")

    result = _invoke_batch_size(["-p", store_path.as_uri(), "-g", "definitely-missing"])

    assert result.exit_code == 1
    assert "Error opening store:" in result.output
    assert "definitely-missing" in result.output
    assert "Traceback" not in result.output
