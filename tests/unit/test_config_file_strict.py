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

import click
import pytest

from firecube.core.config import get_plugin_defaults, load_config_file


def _write_config(tmp_path: Path, content: str) -> Path:
    config_file = tmp_path / "config.toml"
    config_file.write_text(content, encoding="utf-8")
    return config_file


def test_default_product_name_in_plugin_section_is_accepted(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path,
        '[plugins.cli_test_plugin]\ndefault_product_name = "FRM"\n',
    )

    cfg = load_config_file(config_path)
    defaults = get_plugin_defaults(cfg, "cli_test_plugin")

    assert defaults.get("default_product_name") == "FRM"
    assert "default_output_name" not in defaults


def test_unknown_top_level_section_is_rejected_in_strict_mode(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path,
        '[bogus_section]\nkey = "value"\n',
    )

    with pytest.raises(click.UsageError) as exc_info:
        load_config_file(config_path, strict=True)

    assert "bogus_section" in str(exc_info.value)


def test_unknown_top_level_section_is_tolerated_in_non_strict_mode(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path,
        '[storage]\ntype = "local"\n\n[bogus_section]\nkey = "value"\n',
    )

    cfg = load_config_file(config_path)

    assert cfg["storage"]["type"] == "local"
    assert cfg["bogus_section"]["key"] == "value"


def test_known_top_level_sections_are_accepted_in_strict_mode(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path,
        "\n".join(
            [
                "[storage]",
                'type = "local"',
                "",
                "[database.duckdb]",
                "duckdb_threads = 2",
                "",
                "[plugins.cli_test_plugin]",
                "plugin_flag = true",
                "",
                "[archive]",
                'compression = "zstd"',
                "",
                "[metrics]",
                'pushgateway_url = "http://localhost:9091"',
                "",
            ]
        ),
    )

    cfg = load_config_file(config_path, strict=True)
    assert cfg["storage"]["type"] == "local"
    assert cfg["database"]["duckdb"]["duckdb_threads"] == 2
    assert cfg["plugins"]["cli_test_plugin"]["plugin_flag"] is True
    assert cfg["archive"]["compression"] == "zstd"
    assert cfg["metrics"]["pushgateway_url"] == "http://localhost:9091"


def test_missing_config_file_raises_in_strict_mode(tmp_path: Path) -> None:
    """load_config_file with strict=True MUST raise click.UsageError for missing file."""
    missing = tmp_path / "missing.toml"
    with pytest.raises(click.UsageError, match="not found"):
        load_config_file(missing, strict=True)


def test_malformed_toml_raises_in_strict_mode(tmp_path: Path) -> None:
    """load_config_file with strict=True MUST raise click.UsageError for malformed TOML."""
    bad = tmp_path / "bad.toml"
    bad.write_text("[storage\ntype = 'local'")  # Missing ] - invalid TOML
    with pytest.raises(click.UsageError, match=r"parse|TOML|Failed"):
        load_config_file(bad, strict=True)


def test_missing_config_file_returns_empty_in_permissive_mode(tmp_path: Path) -> None:
    """load_config_file with strict=False (default) MUST return {} for missing file."""
    missing = tmp_path / "missing.toml"
    result = load_config_file(missing)  # strict=False is default
    assert result == {}


def test_malformed_toml_returns_empty_in_permissive_mode(tmp_path: Path) -> None:
    """load_config_file with strict=False MUST return {} for malformed TOML (permissive)."""
    bad = tmp_path / "bad.toml"
    bad.write_text("[storage\ntype = 'local'")
    result = load_config_file(bad)  # strict=False
    assert result == {}
