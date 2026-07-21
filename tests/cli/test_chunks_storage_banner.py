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

from click.testing import CliRunner

from firecube.cli.main import cli

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


def test_chunks_banner_shown_with_config_source(monkeypatch, tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    product_path = tmp_path / "product.zarr"
    config_file = _write_config(tmp_path, target_path=product_path)

    monkeypatch.delenv("FIRECUBE_STORAGE_TYPE", raising=False)
    monkeypatch.delenv("FIRECUBE_BUCKET", raising=False)
    monkeypatch.setenv("FIRECUBE_CONFIG", str(config_file))
    _stub_list_chunks(monkeypatch)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "chunks",
            "--workspace",
            str(workspace),
            "list",
            "--format",
            "json",
        ],
    )

    assert result.exit_code == 0, result.stderr
    assert "[chunks] storage: type=local" in result.stderr
    assert "(from config)" in result.stderr


def test_chunks_banner_suppressed_with_quiet(monkeypatch, tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    product_path = tmp_path / "product.zarr"
    config_file = _write_config(tmp_path, target_path=product_path)

    monkeypatch.delenv("FIRECUBE_STORAGE_TYPE", raising=False)
    monkeypatch.delenv("FIRECUBE_BUCKET", raising=False)
    monkeypatch.setenv("FIRECUBE_CONFIG", str(config_file))
    _stub_list_chunks(monkeypatch)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "chunks",
            "--quiet",
            "--workspace",
            str(workspace),
            "list",
            "--format",
            "json",
        ],
    )

    assert result.exit_code == 0, result.stderr
    assert "[chunks] storage:" not in result.stderr


def test_chunks_banner_shown_with_env_source(monkeypatch, tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    missing_config = tmp_path / "missing.toml"

    monkeypatch.setenv("FIRECUBE_CONFIG", str(missing_config))
    monkeypatch.setenv("FIRECUBE_STORAGE_TYPE", "s3")
    monkeypatch.setenv("FIRECUBE_BUCKET", "test")
    _stub_list_chunks(monkeypatch)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "chunks",
            "--workspace",
            str(workspace),
            "list",
            "--format",
            "json",
        ],
    )

    assert result.exit_code == 0, result.stderr
    assert "[chunks] storage: type=s3" in result.stderr
    assert "(from env)" in result.stderr


def test_chunks_no_banner_when_neither_config_nor_env(monkeypatch, tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    missing_config = tmp_path / "missing.toml"

    monkeypatch.setenv("FIRECUBE_CONFIG", str(missing_config))
    monkeypatch.delenv("FIRECUBE_STORAGE_TYPE", raising=False)
    monkeypatch.delenv("FIRECUBE_BUCKET", raising=False)
    _stub_list_chunks(monkeypatch)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "chunks",
            "--workspace",
            str(workspace),
            "list",
            "--format",
            "json",
        ],
    )

    assert result.exit_code != 0
    assert (
        "Chunk operations require a full product URI or a [storage] configuration" in result.stderr
    )
    assert "[chunks] storage:" not in result.stderr
