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

import click

from firecube.cli._errors import render
from firecube.cli._typed_options import coerce_options_for_plugin
from firecube.ingestor.api import PluginContext
from firecube.ingestor.errors import RemovedConfigKeyError
from firecube.ingestor.runtime.configure import configure_ingestor

logger = logging.getLogger("firecube.cli.zarr")


def _configure_ingestor_for_cli(
    ingestor: Any,
    *,
    target: str,
    options: Sequence[tuple[str, object]] = (),
    run_id: str,
    source: str = "",
    sources: tuple[str, ...] = (),
    storage: Any = None,
    input_filters: list[str] | None = None,
    stac_search: dict[str, Any] | None = None,
) -> PluginContext:
    """Build and configure a PluginContext for CLI zarr commands.

    Mirrors the tier-configuration path used by `ingest`. Coerces plugin
    options through `coerce_options_for_plugin`, then delegates to
    `configure_ingestor`.
    """
    coerced_options = coerce_options_for_plugin(ingestor.name, tuple(options))
    try:
        return configure_ingestor(
            ingestor,
            target=target,
            options=dict(coerced_options),
            run_id=run_id,
            source=source,
            sources=sources,
            storage=storage,
            input_filters=input_filters,
            stac_search=stac_search,
        )
    except RemovedConfigKeyError as exc:
        raise click.UsageError(render(exc)) from exc
