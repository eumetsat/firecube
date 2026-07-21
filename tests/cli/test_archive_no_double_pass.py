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

from firecube.cli.main import cli
from firecube.core.storage.session import StorageSession


def test_archive_create_passes_product_session_without_storage_config(
    monkeypatch,
    tmp_path: Path,
) -> None:
    from firecube.core.tensogram import converter

    captured: dict[str, Any] = {}

    def fake_zarr_to_tgm(source: str, target: str, **kwargs: Any) -> dict[str, Any]:
        session = kwargs.get("session")
        assert isinstance(session, StorageSession)
        captured["source"] = source
        captured["target"] = target
        captured["session_product_uri"] = session.product.product_uri.to_str()
        captured["session_driver"] = session.driver.driver
        captured["storage_config_passed"] = "storage_config" in kwargs
        return {
            "target": target,
            "groups": ["g1"],
            "variables": ["x"],
            "file_size_bytes": 0,
            "compression": "zstd",
        }

    monkeypatch.setattr(converter, "zarr_to_tgm", fake_zarr_to_tgm)

    source = tmp_path / "source.zarr"
    archive = tmp_path / "archive.tgm"
    result = CliRunner().invoke(
        cli,
        [
            "archive",
            "create",
            "--source",
            source.as_uri(),
            "--archive",
            archive.as_uri(),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Archive created:" in result.output
    assert captured == {
        "source": source.as_uri(),
        "target": str(archive),
        "session_product_uri": source.as_uri(),
        "session_driver": "fsspec",
        "storage_config_passed": False,
    }
