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
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.storage.session import StorageSession
from firecube.core.storage.uri import StorageUri


def _write_parquet(path: Path) -> None:
    pq.write_table(pa.table({"a": [1, 2, 3]}), path)


def _invoke_validate(args: list[str]):
    return CliRunner().invoke(cli, ["parquet", "validate", *args])


def test_file_uri_no_storage_flags(tmp_path: Path) -> None:
    parquet_path = tmp_path / "x.parquet"
    _write_parquet(parquet_path)

    result = _invoke_validate(["-p", parquet_path.as_uri()])

    assert result.exit_code == 0
    assert "Missing option" not in result.output
    assert "Traceback" not in result.output
    payload = json.loads(result.output)
    assert payload["valid"] is True
    assert payload["files_checked"] == 1


def test_s3_uri_infers_storage_type(monkeypatch: Any) -> None:
    observed: dict[str, str] = {}

    class FakeFS:
        def exists(self, uri: StorageUri) -> bool:
            observed["exists_uri"] = uri.to_str()
            return True

        def isdir(self, uri: StorageUri) -> bool:
            return False

        def find(self, uri: StorageUri) -> list[StorageUri]:
            return [uri]

        def info(self, uri: StorageUri) -> dict[str, int]:
            observed["info_uri"] = uri.to_str()
            return {"size": 8}

        def open(self, uri: StorageUri, mode: str) -> io.BytesIO:
            assert mode == "rb"
            observed["open_uri"] = uri.to_str()
            return io.BytesIO(b"PAR1PAR1")

    def fake_fs(self: StorageSession) -> FakeFS:
        observed["session_product_uri"] = self.product.product_uri.to_str()
        observed["session_protocol"] = self.product.product_uri.protocol
        observed["session_driver"] = self.driver.driver
        return FakeFS()

    monkeypatch.setattr(StorageSession, "fs", fake_fs)

    result = _invoke_validate(["-p", "s3://bucket/x.parquet"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["valid"] is True
    assert payload["files_checked"] == 1
    assert payload["resolved"]["store_uri"] == "s3://bucket/x.parquet"
    assert observed == {
        "session_product_uri": "s3://bucket/x.parquet",
        "session_protocol": "s3",
        "session_driver": "fsspec",
        "exists_uri": "s3://bucket/x.parquet",
        "info_uri": "s3://bucket/x.parquet",
        "open_uri": "s3://bucket/x.parquet",
    }


def test_coherence_mismatch_rejected(tmp_path: Path) -> None:
    parquet_path = tmp_path / "x.parquet"
    _write_parquet(parquet_path)

    result = _invoke_validate(
        [
            "-p",
            parquet_path.as_uri(),
            "--storage-type",
            "s3",
            "--storage-driver",
            "fsspec",
        ]
    )

    assert result.exit_code == 2
    assert "incompatible" in result.output
    assert "Traceback" not in result.output
