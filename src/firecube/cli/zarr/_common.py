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

"""Shared helpers for the ``firecube zarr`` command package."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Any

from firecube.cli._typed_options import coerce_options_for_plugin
from firecube.ingestor.api import (
    IngestContext,
    PluginContext,
    RuntimeIngestContext,
)
from firecube.ingestor.runtime.configure import TierConfigurator

logger = logging.getLogger("firecube.cli.zarr")


def _configure_ingestor_for_cli(
    ingestor: Any,
    *,
    target: str,
    options: Sequence[tuple[str, object]] = (),
    run_id: str,
    source: str = "",
    storage: Any = None,
) -> PluginContext:
    """Build and configure a PluginContext for CLI zarr commands.

    Mirrors the tier-configuration path used by `ingest`. Coerces plugin
    options through `coerce_options_for_plugin`, builds an `IngestContext`,
    wraps into a `RuntimeIngestContext`, runs `TierConfigurator.configure`,
    and returns the `PluginContext`.
    """
    coerced_options = coerce_options_for_plugin(ingestor.name, tuple(options))
    ingest_ctx = IngestContext(
        source=source,
        target=target,
        in_memory=True,
        output_format="zarr",
        options=dict(coerced_options),
        storage=storage,
        run_id=run_id,
    )
    runtime_ctx = RuntimeIngestContext.from_ingest_context(
        ingest_ctx,
        run_id=run_id,
        temp_root=None,
        materializer=None,
    )
    configurator = TierConfigurator(
        ingestor.template_config_class,
        ingestor.plugin_config_class,
        plugin_name=ingestor.name,
    )
    ingestor.engine_config, ingestor.template_config, ingestor.plugin_config = (
        configurator.configure(runtime_ctx)
    )
    return PluginContext(runtime_ctx)
