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

import pytest
from click.testing import CliRunner

from firecube.cli._slot_env import resolve_slot_range_from_env
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


def test_slot_group_default_none() -> None:
    assert EngineConfig().slot_group is None


def test_slot_group_valid_string_accepted() -> None:
    cfg = EngineConfig(slot_group="data_1km")

    assert cfg.slot_group == "data_1km"


def test_slot_group_empty_string_fails() -> None:
    with pytest.raises(ValueError, match="non-empty string"):
        EngineConfig(slot_group="")


def test_slot_group_whitespace_only_fails() -> None:
    with pytest.raises(ValueError, match="non-empty string"):
        EngineConfig(slot_group="   ")


def test_cli_passes_slot_group_to_engine_config(tmp_path: Path) -> None:
    captured: dict[str, IngestContext] = {}

    def fake_run(self: Any, ctx: IngestContext) -> IngestResult:
        captured["ctx"] = ctx
        engine_options = {k: v for k, v in ctx.options.items() if k in config_keys(EngineConfig)}
        engine_cfg = EngineConfig.from_options(engine_options)
        assert engine_cfg.slot_group == "data_1km"
        return IngestResult(
            outputs=OutputPaths(primary=str(ctx.target)),
            output_format=str(ctx.output_format),
        )

    with patch("cli_test_plugin.CliTestIngestor.run", autospec=True, side_effect=fake_run):
        result = CliRunner().invoke(
            cli,
            [*_required_args(tmp_path), "--slot-group", "data_1km"],
        )

    assert result.exit_code == 0, result.output
    assert "ctx" in captured
    assert captured["ctx"].options["slot_group"] == "data_1km"


def test_env_slot_group_used_when_cli_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FIRECUBE_SLOT_GROUP", "data_1km")

    assert resolve_slot_range_from_env(None, None, None, None) == (
        None,
        None,
        "data_1km",
    )


def test_cli_slot_group_overrides_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FIRECUBE_SLOT_GROUP", "from_env")

    assert resolve_slot_range_from_env(None, None, None, "from_cli") == (
        None,
        None,
        "from_cli",
    )
