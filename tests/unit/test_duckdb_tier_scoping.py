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

"""Scoping tests for the DatabaseDuckDB tier.

Asserts that ``[database.duckdb]`` no longer leaks into every plugin's
resolved defaults, while DuckDB-aware plugins (declaring ``duckdb_*``
fields in their ``PluginConfig``) still receive these settings via the
``DatabaseDuckDB`` tier in ``TierConfigurator``.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import cast

import pytest

from firecube.core.config import (
    DATABASE_DUCKDB_KEYS,
    get_plugin_defaults,
    load_config_file,
)
from firecube.ingestor.runtime.configure import TierConfigurator
from firecube.ingestor.types.config import PluginConfig
from firecube.ingestor.types.context import IngestContext


@dataclass
class _NonDbPluginConfig(PluginConfig):
    plugin_flag: bool = False


@dataclass
class _DbAwarePluginConfig(PluginConfig):
    plugin_flag: bool = False
    duckdb_memory_limit: str | None = None
    duckdb_threads: int | None = None
    duckdb_max_temp_directory_size: str | None = None


def _write_config(tmp_path: Path, body: str) -> Path:
    config_path = tmp_path / "config.toml"
    config_path.write_text(body, encoding="utf-8")
    return config_path


@pytest.mark.unit
def test_non_db_plugin_does_not_receive_database_duckdb_settings(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path,
        "\n".join(
            [
                "[database.duckdb]",
                'duckdb_max_temp_directory_size = "100GiB"',
                'duckdb_memory_limit = "8GB"',
                "duckdb_threads = 4",
                "",
                "[plugins.cli_test_plugin]",
                "test_int = 1",
                "",
            ]
        ),
    )

    cfg = load_config_file(config_path)
    options = get_plugin_defaults(cfg, "cli_test_plugin")

    for duckdb_key in DATABASE_DUCKDB_KEYS:
        assert duckdb_key not in options, (
            f"Global [database.duckdb] leaked '{duckdb_key}' into non-DB plugin defaults"
        )
    assert options.get("test_int") == 1


@pytest.mark.unit
def test_db_aware_plugin_section_overrides_reach_plugin_config(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path,
        "\n".join(
            [
                "[database.duckdb]",
                'duckdb_max_temp_directory_size = "100GiB"',
                'duckdb_memory_limit = "8GB"',
                "duckdb_threads = 4",
                "",
                "[plugins.db_aware]",
                "plugin_flag = true",
                'duckdb_max_temp_directory_size = "200GiB"',
                'duckdb_memory_limit = "16GB"',
                "duckdb_threads = 8",
                "",
            ]
        ),
    )

    cfg = load_config_file(config_path)
    options = get_plugin_defaults(cfg, "db_aware")
    ctx = IngestContext(source="input", options=options)

    configurator = TierConfigurator(
        template_config_class=None,
        plugin_config_class=_DbAwarePluginConfig,
        plugin_name="db_aware",
    )

    _, _, plugin_config = configurator.configure(ctx)

    assert plugin_config is not None
    typed = cast(_DbAwarePluginConfig, plugin_config)
    assert typed.plugin_flag is True
    assert typed.duckdb_memory_limit == "16GB"
    assert typed.duckdb_threads == 8
    assert typed.duckdb_max_temp_directory_size == "200GiB"


@pytest.mark.unit
def test_duckdb_tier_accepts_keys_without_unknown_option_error(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path,
        "\n".join(
            [
                "[plugins.db_aware]",
                "plugin_flag = true",
                'duckdb_memory_limit = "8GB"',
                "duckdb_threads = 4",
                'duckdb_max_temp_directory_size = "100GiB"',
                "",
            ]
        ),
    )

    cfg = load_config_file(config_path)
    options = get_plugin_defaults(cfg, "db_aware")
    ctx = IngestContext(source="input", options=options)

    configurator = TierConfigurator(
        template_config_class=None,
        plugin_config_class=_NonDbPluginConfig,
        plugin_name="db_aware",
    )

    _, _, plugin_config = configurator.configure(ctx)

    assert plugin_config is not None
    typed = cast(_NonDbPluginConfig, plugin_config)
    assert typed.plugin_flag is True
