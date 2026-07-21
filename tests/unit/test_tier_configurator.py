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

from dataclasses import dataclass
from typing import cast

import pytest

from firecube.core.config import get_plugin_defaults, load_config_file
from firecube.ingestor.runtime.configure import TierConfigurator
from firecube.ingestor.types.config import PluginConfig
from firecube.ingestor.types.context import IngestContext


@dataclass
class _PluginWithoutDuckDb(PluginConfig):
    plugin_flag: bool = False


@dataclass
class _PluginWithDuckDb(PluginConfig):
    plugin_flag: bool = False
    duckdb_memory_limit: str | None = None
    duckdb_threads: int | None = None
    duckdb_max_temp_directory_size: str | None = None


@pytest.mark.unit
@pytest.mark.parametrize(
    ("plugin_name", "plugin_config_class"),
    [
        ("without_duckdb", _PluginWithoutDuckDb),
        ("with_duckdb", _PluginWithDuckDb),
    ],
)
def test_database_duckdb_tier(
    tmp_path, plugin_name: str, plugin_config_class: type[PluginConfig]
) -> None:
    plugin_lines = [f"[plugins.{plugin_name}]", "plugin_flag = true"]
    if plugin_name == "with_duckdb":
        plugin_lines.extend(
            [
                'duckdb_max_temp_directory_size = "100GiB"',
                'duckdb_memory_limit = "8GB"',
                "duckdb_threads = 4",
            ]
        )

    config_path = tmp_path / "config.toml"
    config_path.write_text(
        "\n".join(
            [
                "[database.duckdb]",
                'duckdb_max_temp_directory_size = "100GiB"',
                'duckdb_memory_limit = "8GB"',
                "duckdb_threads = 4",
                "",
                *plugin_lines,
                "",
            ]
        ),
        encoding="utf-8",
    )

    cfg = load_config_file(config_path)
    options = get_plugin_defaults(cfg, plugin_name)
    ctx = IngestContext(source="input", options=options)

    configurator = TierConfigurator(
        template_config_class=None,
        plugin_config_class=plugin_config_class,
        plugin_name=plugin_name,
    )

    engine_config, template_config, plugin_config = configurator.configure(ctx)

    assert engine_config.pipeline_workers == 1
    assert template_config is None
    assert plugin_config is not None
    typed_base_plugin = cast(_PluginWithoutDuckDb | _PluginWithDuckDb, plugin_config)
    assert typed_base_plugin.plugin_flag is True

    if plugin_name == "with_duckdb":
        assert options["duckdb_memory_limit"] == "8GB"
        assert options["duckdb_threads"] == 4
        assert options["duckdb_max_temp_directory_size"] == "100GiB"
        typed_plugin = cast(_PluginWithDuckDb, plugin_config)
        assert typed_plugin.duckdb_memory_limit == "8GB"
        assert typed_plugin.duckdb_threads == 4
        assert typed_plugin.duckdb_max_temp_directory_size == "100GiB"
    else:
        assert "duckdb_memory_limit" not in options
        assert "duckdb_threads" not in options
        assert "duckdb_max_temp_directory_size" not in options
