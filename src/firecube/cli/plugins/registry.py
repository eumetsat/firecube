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

"""
Registry interface for the CLI.

This module acts as a facade between the CLI and the core ingestor registry.
It handles:
1. Discovery of available plugins (via firecube.ingestor.registry).
2. Resolution of configuration classes (Engine, Template, Plugin) for introspection.
3. Validation of plugin contracts.
"""

from __future__ import annotations

import importlib.metadata
from dataclasses import dataclass
from typing import Any

from firecube.ingestor.api import PluginConfig, TemplateConfig
from firecube.ingestor.config.engine import EngineConfig
from firecube.ingestor.registry.loader import discover_ingestors, get_ingestor


@dataclass
class PluginConfigSchemas:
    """Resolved options-tier configuration classes for a specific plugin.

    Holds the three user-facing CLI option schemas (EngineConfig, TemplateConfig,
    PluginConfig). These are the options tiers, not the storage stack
    (StorageConfig / StorageDriverConfig / StorageBinding).
    """

    engine: type[EngineConfig]
    template: type[TemplateConfig] | None
    plugin: type[PluginConfig] | None


def resolve_plugin_configs(plugin_name: str) -> PluginConfigSchemas:
    """Resolve the configuration tiers for a given plugin name.

    Args:
        plugin_name: The name of the registered ingestor/plugin.

    Returns:
        PluginConfigSchemas object containing the resolved dataclasses.

    Raises:
        TypeError: If the registered class is not a valid Ingestor.
    """

    discover_ingestors()
    ingestor_cls = get_ingestor(plugin_name)

    # Engine config is always the core EngineConfig
    engine_config = EngineConfig

    # Template config (e.g. from GenericZarrIngestor)
    # Strictly resolve from class attribute
    template_config = getattr(ingestor_cls, "template_config_class", None)

    # Plugin config
    # Strictly resolve from class attribute
    plugin_config = getattr(ingestor_cls, "plugin_config_class", None)

    return PluginConfigSchemas(
        engine=engine_config,
        template=template_config,
        plugin=plugin_config,
    )


def get_plugin_distributions(plugin_name: str) -> list[tuple[str, str]]:
    """Resolve the distribution packages providing the plugin.

    Args:
        plugin_name: The name of the plugin entry point.

    Returns:
        List of (distribution_name, version) tuples.
    """

    eps = importlib.metadata.entry_points(group="firecube.plugins")

    return [(ep.dist.name, ep.dist.version) for ep in eps if ep.name == plugin_name and ep.dist]


def get_plugin_cli_command(plugin_name: str) -> Any | None:
    """Load the CLI command group for a plugin via entry points.

    Look for an entry point in 'firecube.plugin_cli' with the given name.

    Args:
        plugin_name: The name of the plugin/command group.

    Returns:
        The Click Command/Group object, or None if not found.
    """

    eps = importlib.metadata.entry_points(group="firecube.plugin_cli")

    for ep in eps:
        if ep.name == plugin_name:
            try:
                return ep.load()
            except Exception as exc:
                raise ImportError(f"Failed to load plugin CLI '{plugin_name}': {exc}") from exc

    return None


def list_plugin_cli_names() -> list[str]:
    """List names of plugins that expose a CLI entry point.

    Returns:
        List of plugin names (sorted).
    """

    eps = importlib.metadata.entry_points(group="firecube.plugin_cli")

    return sorted(ep.name for ep in eps)
