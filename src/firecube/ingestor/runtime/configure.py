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

"""Options-tier configuration and execution-mode selection.

This module is runtime-only and is responsible for splitting the flat
`ctx.options` dict into strict options tiers (the user-facing CLI option
hierarchy). These tiers are separate from the storage stack
(StorageConfig / StorageDriverConfig / StorageBinding / StorageSession),
which handles I/O wiring independently.

Options tiers:
  - EngineConfig (always present)
  - TemplateConfig (optional, per template)
  - PluginConfig (optional, per plugin)
"""

from __future__ import annotations

import logging
import os
import uuid
from enum import Enum, auto
from typing import Any

import click

from firecube.core.config import DATABASE_DUCKDB_KEYS
from firecube.core.controlplane import ChunkManager
from firecube.ingestor.config.engine import (
    SYSTEM_KEYS,
    EngineConfig,
    config_keys,
    is_experimental_option_key,
)
from firecube.ingestor.templates.config import TemplateConfig
from firecube.ingestor.types.config import PluginConfig
from firecube.ingestor.types.context import IngestContext


class ExecutionMode(Enum):
    SEQUENTIAL = auto()
    PIPELINE = auto()


class TierConfigurator:
    """Splits a flat ``ctx.options`` dict into four strict options tiers.

    These are the user-facing CLI option tiers, not the storage stack
    (StorageConfig / StorageDriverConfig / StorageBinding). Storage I/O
    wiring is handled separately by the storage layer.

    Options tiers (in precedence order for key ownership):
        EngineConfig   — pipeline execution settings (workers, batch size,
                         write mode, workspace cleanup, …).  Always present.
        TemplateConfig — output-format settings (Zarr chunk shape, Parquet
                         partition config, …).  Present only if the ingestor
                         declares ``template_config_class``.
        DatabaseDuckDB — global ``[database.duckdb]`` defaults owned by core
                         (DuckDB resource settings shared across plugins).
        PluginConfig   — plugin-specific domain options (e.g. ``msg_region``,
                         ``forecast_horizons``).  Present only if the ingestor
                         declares ``plugin_config_class``.

    Unknown keys (not owned by any tier) raise ``ValueError`` at configure
    time, enforcing the strict schema contract.

    ``configure()`` is pure: it does not mutate ``ctx``.
    """

    def __init__(
        self,
        template_config_class: type[TemplateConfig] | None,
        plugin_config_class: type[PluginConfig] | None,
        *,
        plugin_name: str,
    ) -> None:
        self._template_config_class = template_config_class
        self._plugin_config_class = plugin_config_class
        self._plugin_name = plugin_name

    def configure(
        self, ctx: IngestContext
    ) -> tuple[EngineConfig, TemplateConfig | None, PluginConfig | None]:
        options = ctx.options or {}

        if "output_name" in options:
            raise click.UsageError(
                "Config key 'output_name' is no longer accepted. "
                "Use '--product-name' or 'default_product_name' instead.\n"
                "Edit the CLI invocation or the config file passed via --config-file."
            )

        engine_keys = config_keys(EngineConfig)
        template_keys = (
            config_keys(self._template_config_class) if self._template_config_class else set()
        )
        plugin_keys = config_keys(self._plugin_config_class) if self._plugin_config_class else set()
        duckdb_keys = set(DATABASE_DUCKDB_KEYS)

        known = engine_keys | template_keys | duckdb_keys | plugin_keys | set(SYSTEM_KEYS)
        unknown = {
            key for key in options if key not in known and not is_experimental_option_key(key)
        }
        if unknown:
            raise ValueError(
                f"Unknown configuration options for {self._plugin_name}: "
                f"{', '.join(sorted(unknown))}. Use --show-options to see valid keys. "
                f"Experimental options must use the x_ prefix (e.g. x_foo=...)."
            )

        engine_opts = {k: options[k] for k in engine_keys if k in options}
        template_opts = {k: options[k] for k in template_keys if k in options}
        plugin_opts = {k: options[k] for k in plugin_keys if k in options}

        engine_config = EngineConfig.from_options(engine_opts)
        template_config = (
            self._template_config_class.from_options(template_opts)
            if self._template_config_class
            else None
        )
        plugin_config = (
            self._plugin_config_class.from_options(plugin_opts)
            if self._plugin_config_class
            else None
        )

        return engine_config, template_config, plugin_config


def determine_execution_mode(engine_config: EngineConfig) -> ExecutionMode:
    pipeline_requested = (
        bool(engine_config.pipeline_parallel) or int(engine_config.pipeline_workers) > 1
    )
    if pipeline_requested:
        return ExecutionMode.PIPELINE
    return ExecutionMode.SEQUENTIAL


def ensure_run_id(*, ctx: IngestContext, plugin_name: str) -> str:
    """Resolve run_id from context/options or generate a new one (pure)."""
    if ctx.run_id:
        return ctx.run_id

    existing = ctx.option("run_id") or ctx.option("manifest_run_id")
    if existing:
        return str(existing)

    hostname = os.getenv("HOSTNAME", "host")
    return f"{plugin_name}-{hostname}-{uuid.uuid4().hex}"


def _validate_storage_driver(storage_driver: Any, *, storage_type: str) -> None:
    driver = getattr(storage_driver, "driver", "fsspec")
    if driver == "obstore":
        from firecube.core.filesystem._obstore_compat import require_obstore

        require_obstore()
        if storage_type not in ("local", "s3"):
            raise ValueError(
                f"Storage driver 'obstore' supports 'local' and 's3' storage types, "
                f"got '{storage_type}'. Use --storage-driver fsspec instead."
            )
        log = logging.getLogger(__name__)
        log.info("Using obstore storage driver for all I/O (experimental)")


def _storage_type_for_product_uri(product_uri: Any) -> str:
    if product_uri.is_remote():
        return "s3"
    return "local"


def ensure_chunk_manager_config(chunk_manager: ChunkManager, ctx: IngestContext) -> None:
    """Configure ChunkManager from the ingestion context."""
    if ctx.storage is not None and ctx.storage.output is not None:
        product_uri = ctx.storage.output.product.product_uri
        storage_type = _storage_type_for_product_uri(product_uri)
        _validate_storage_driver(ctx.storage.output.driver, storage_type=storage_type)

    if not getattr(chunk_manager, "base_uri", None):
        raise RuntimeError(
            "ChunkManager requires an output base URI. Provide a target or storage configuration."
        )
