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

import logging
from pathlib import Path

import pytest
from click.testing import CliRunner

from firecube.cli.main import cli

pytestmark = pytest.mark.unit

_CONFIG_TEMPLATE = """\
[storage]
type = "local"
target_path = "{target_path}"
"""


def _write_config(tmp_path: Path, *, target_path: Path) -> Path:
    config_file = tmp_path / "firecube-test.toml"
    config_file.write_text(
        _CONFIG_TEMPLATE.format(target_path=target_path),
        encoding="utf-8",
    )
    return config_file


def _stub_list_chunks(monkeypatch) -> None:
    monkeypatch.setattr(
        "firecube.core.controlplane.manager.ChunkManager.list_chunks",
        lambda self, **kwargs: [],
    )


def test_chunks_list_no_info_before_table(
    monkeypatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    workspace = tmp_path / "workspace"
    product_path = tmp_path / "product.zarr"
    config_file = _write_config(tmp_path, target_path=product_path)

    monkeypatch.delenv("FIRECUBE_STORAGE_TYPE", raising=False)
    monkeypatch.delenv("FIRECUBE_BUCKET", raising=False)
    monkeypatch.setenv("FIRECUBE_CONFIG", str(config_file))
    _stub_list_chunks(monkeypatch)

    with caplog.at_level(logging.INFO, logger="firecube.cli"):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["chunks", "--workspace", str(workspace), "list", "--format", "json"],
        )

    assert result.exit_code == 0, result.output

    info_records = [
        r
        for r in caplog.records
        if r.name == "firecube.cli" and r.levelno == logging.INFO and "Chunks using" in r.message
    ]
    assert info_records == [], f"Unexpected INFO logs from chunks manager: {info_records}"
