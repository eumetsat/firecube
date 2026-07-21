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
from unittest.mock import patch

from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.ingestor.api import IngestContext, IngestResult, OutputPaths
from firecube.ingestor.config.engine import EngineConfig, config_keys


def _base_args(tmp_path: Path) -> list[str]:
    config = tmp_path / "config.toml"
    config.write_text("", encoding="utf-8")
    return [
        "--config-file",
        str(config),
        "ingest",
        "cli_test_plugin",
    ]


def _required_args(tmp_path: Path) -> list[str]:
    return [
        *_base_args(tmp_path),
        "--target",
        (tmp_path / "qa.zarr").as_uri(),
        "--product-name",
        "qa",
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
    ]


def test_unpaired_slot_start_fails(tmp_path: Path) -> None:
    result = CliRunner().invoke(cli, [*_required_args(tmp_path), "--slot-start", "0"])

    assert result.exit_code != 0, result.output
    assert "must be provided together" in result.output


def test_unpaired_slot_end_fails(tmp_path: Path) -> None:
    result = CliRunner().invoke(cli, [*_required_args(tmp_path), "--slot-end", "100"])

    assert result.exit_code != 0, result.output
    assert "must be provided together" in result.output


def test_equal_slots_fail(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        cli,
        [*_required_args(tmp_path), "--slot-start", "10", "--slot-end", "10"],
    )

    assert result.exit_code != 0, result.output
    assert "slot_start must be < slot_end" in str(result.exception)


def test_negative_slot_start_fails(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        cli,
        [*_required_args(tmp_path), "--slot-start", "-1", "--slot-end", "10"],
    )

    assert result.exit_code != 0, result.output
    assert "non-negative" in str(result.exception)


def test_valid_slot_range_accepted(tmp_path: Path) -> None:
    captured: dict[str, IngestContext] = {}

    def fake_run(self: Any, ctx: IngestContext) -> IngestResult:
        captured["ctx"] = ctx
        engine_options = {k: v for k, v in ctx.options.items() if k in config_keys(EngineConfig)}
        engine_cfg = EngineConfig.from_options(engine_options)
        assert engine_cfg.slot_start == 0
        assert engine_cfg.slot_end == 100
        assert engine_cfg.slot_size is None
        return IngestResult(
            outputs=OutputPaths(primary=str(ctx.target)),
            output_format=str(ctx.output_format),
        )

    with patch("cli_test_plugin.CliTestIngestor.run", autospec=True, side_effect=fake_run):
        result = CliRunner().invoke(
            cli,
            [*_required_args(tmp_path), "--slot-start", "0", "--slot-end", "100"],
        )

    assert result.exit_code == 0, result.output
    assert "ctx" in captured
    assert captured["ctx"].options["slot_start"] == 0
    assert captured["ctx"].options["slot_end"] == 100
