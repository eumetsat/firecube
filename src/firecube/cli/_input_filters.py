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

"""Native input-filter flag and configuration precedence."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

import click

from firecube.core.config import get_plugin_defaults, load_config_file
from firecube.core.formats._input_filters import (
    reject_legacy_input_patterns,
    validate_input_filters,
)


def _parse_input_filters(
    ctx: click.Context, param: click.Parameter, values: tuple[str, ...]
) -> list[str] | None:
    if not values:
        return None
    if len(values) != 1:
        raise click.BadParameter(
            "Pass --input-filters once; combine filters in one JSON list.", ctx=ctx, param=param
        )
    try:
        parsed = json.loads(values[0])
        if parsed is None:
            raise ValueError("Expected a JSON list, not null; use [] to clear configured filters.")
        return validate_input_filters(parsed)
    except (ValueError, TypeError) as exc:
        raise click.BadParameter(
            f"{exc}. Use a quoted JSON list, for example: "
            '--input-filters \'["*.csv","!draft_*.csv"]\'',
            ctx=ctx,
            param=param,
        ) from exc


def input_filters_option(f: Any) -> Any:
    """Declare one JSON-list flag, rejecting repeated occurrences."""
    return click.option(
        "--input-filters",
        type=str,
        multiple=True,
        metavar="JSON",
        callback=_parse_input_filters,
        help=(
            "Input-file filters as one JSON list. Positive globs add to built-in formats; "
            "!globs exclude matches. Exclusions always win. Use [] to clear configured filters. "
            "Custom discovery hooks must apply filters themselves."
        ),
    )(f)


def resolve_input_filters(
    ctx: click.Context,
    plugin: str,
    supplied: list[str] | None,
    *,
    defaults: Mapping[str, object] | None = None,
) -> list[str] | None:
    """Resolve only input filters, keeping other command defaults untouched."""
    if defaults is None:
        config_file = (ctx.obj or {}).get("config_file")
        cfg = load_config_file(config_file, strict=config_file is not None)
        defaults = get_plugin_defaults(cfg, plugin)
    try:
        reject_legacy_input_patterns(defaults)
        configured = validate_input_filters(defaults.get("input_filters"))
        return configured if supplied is None else validate_input_filters(supplied)
    except ValueError as exc:
        raise click.UsageError(str(exc), ctx=ctx) from exc
