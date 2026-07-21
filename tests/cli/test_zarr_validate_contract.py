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

from click.testing import CliRunner

import firecube.cli.zarr as zarr_cli
from firecube.cli.main import cli
from firecube.core.storage.session import StorageSession
from firecube.core.zarr.validation import ZarrValidationReport


def _invoke_validate(args: list[str]):
    return CliRunner().invoke(cli, ["zarr", "validate", *args])


def test_file_uri_no_storage_flags(tmp_local_zarr: Path) -> None:
    result = _invoke_validate(["-p", tmp_local_zarr.as_uri(), "-g", "g1"])

    assert result.exit_code == 0
    assert "Missing option" not in result.output
    assert "Traceback" not in result.output


def test_bare_path_rejected_with_uri_required_error(tmp_local_zarr: Path) -> None:
    result = _invoke_validate(["-p", str(tmp_local_zarr), "-g", "g1"])

    assert result.exit_code == 2
    assert "URI scheme required" in result.output
    assert "Traceback" not in result.output


def test_s3_uri_infers_storage_type(monkeypatch: Any) -> None:
    observed: dict[str, str] = {}

    class FakeFS:
        pass

    def fake_fs(self: StorageSession) -> FakeFS:
        observed["session_product_uri"] = self.product.product_uri.to_str()
        observed["session_protocol"] = self.product.product_uri.protocol
        observed["session_driver"] = self.driver.driver
        return FakeFS()

    def fake_validate_group_with_fs(
        fs: FakeFS,
        store_uri: Any,
        group_path: str,
        **kwargs: Any,
    ) -> ZarrValidationReport:
        assert isinstance(fs, FakeFS)
        observed["validator_store_uri"] = store_uri.to_str()
        observed["validator_group"] = group_path
        observed["on_timeout"] = str(kwargs["on_timeout"])
        return ZarrValidationReport(
            product=store_uri.to_str(),
            group=group_path,
            shape=[4],
            chunk_shape=[4],
            expected_chunks={"dim0": 1},
            max_indices={"dim0": 0},
            extra_chunks=[],
            missing_indices={},
            chunks_processed=1,
        )

    monkeypatch.setattr(StorageSession, "fs", fake_fs)
    monkeypatch.setattr(zarr_cli, "validate_group_with_fs", fake_validate_group_with_fs)

    result = _invoke_validate(["-p", "s3://bucket/x.zarr", "-g", "g1"])

    assert result.exit_code == 0, result.output
    assert '"product": "s3://bucket/x.zarr"' in result.output
    assert '"group": "g1"' in result.output
    assert observed == {
        "session_product_uri": "s3://bucket/x.zarr",
        "session_protocol": "s3",
        "session_driver": "fsspec",
        "validator_store_uri": "s3://bucket/x.zarr",
        "validator_group": "g1",
        "on_timeout": "warn",
    }


def test_coherence_mismatch_rejected(tmp_local_zarr: Path) -> None:
    result = _invoke_validate(
        [
            "-p",
            tmp_local_zarr.as_uri(),
            "-g",
            "g1",
            "--storage-type",
            "s3",
            "--storage-driver",
            "fsspec",
        ]
    )

    assert result.exit_code == 2
    assert "incompatible" in result.output
    assert "Traceback" not in result.output


def test_missing_group_clean_error(tmp_local_zarr: Path) -> None:
    result = _invoke_validate(["-p", tmp_local_zarr.as_uri(), "-g", "definitely-not-here"])

    assert result.exit_code == 1
    assert "definitely-not-here" in result.output
    assert "Error:" in result.output
    assert "Traceback" not in result.output
