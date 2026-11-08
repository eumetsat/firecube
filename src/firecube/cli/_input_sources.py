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

"""The repeatable ``--input-data`` flag, ``--stac-search``, and their admission."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import Any

import click

from firecube.cli._errors import render
from firecube.cli._input_filters import load_plugin_defaults
from firecube.cli._uri_policy import admit_input_sources
from firecube.core.discovery import StacSearchError, require_stac, validate_stac_search

INPUT_DATA_HELP = (
    "Plugin input data as a URI: file:///abs/path, s3://bucket/prefix or "
    "stac://host/api-root/collections/<id> (one STAC API collection). Repeat to read "
    "several sources in one run. The plugin gets each URI as given."
)


def input_data_option(*, short: bool = True, help: str = INPUT_DATA_HELP) -> Any:
    """Declare ``--input-data`` (``-i`` when ``short``), repeatable, in the order given."""
    names = ("-i", "--input-data") if short else ("--input-data",)
    return click.option(*names, "input_data", type=str, multiple=True, help=help)


def _parse_stac_search(
    ctx: click.Context, param: click.Parameter, value: str | None
) -> dict[str, Any] | None:
    if value is None:
        return None
    try:
        return validate_stac_search(json.loads(value))
    except json.JSONDecodeError as exc:
        raise click.BadParameter(
            f"{render(StacSearchError(), ctx)} Use a quoted JSON object, for example: "
            '--stac-search \'{"datetime":"2025-06-01T00:00:00Z/2025-06-30T23:59:59Z"}\'',
            ctx=ctx,
            param=param,
        ) from exc
    except StacSearchError as exc:
        raise click.BadParameter(render(exc, ctx), ctx=ctx, param=param) from exc


def stac_search_option(f: Any) -> Any:
    """Declare ``--stac-search``: one JSON object of STAC item search keys."""
    return click.option(
        "--stac-search",
        "stac_search",
        type=str,
        default=None,
        metavar="JSON",
        callback=_parse_stac_search,
        help=(
            "STAC item search filter for every stac:// --input-data, as one JSON object "
            "with any of the keys datetime, bbox, intersects and ids. The collection "
            "comes from the stac:// URI. Replaces a stac_search table of the plugin "
            "configuration."
        ),
    )(f)


def admit_sources(
    ctx: click.Context,
    plugin: str,
    values: Sequence[str],
    stac_search: dict[str, Any] | None,
    *,
    command: str,
    defaults: Mapping[str, object] | None = None,
) -> tuple[tuple[str, ...], dict[str, Any] | None]:
    """Admit the sources of one command, before any network or store access.

    Merges ``--stac-search`` with the ``stac_search`` table of the plugin configuration; the flag
    replaces the table whole. Drops empty values and refuses a value that is not a ``file://``,
    ``s3://`` or valid ``stac://`` URI, a search without a ``stac://`` source, and a ``stac://``
    source when the ``stac`` extra is missing.

    Args:
        ctx: The Click context, which names the configuration file.
        plugin: The plugin whose configuration table applies.
        values: The ``--input-data`` values, in order.
        stac_search: The parsed ``--stac-search`` value, or ``None``.
        command: The running command, named when the extra is missing.
        defaults: The plugin's configuration table, when the caller has loaded it already.

    Returns:
        The non-empty values as given, in order, and the search to apply, or ``None``.

    Raises:
        click.BadParameter: A value breaks the source URI rule (exit 2).
        click.UsageError: The configured search is invalid, or a search was given and no source
            is ``stac://`` (exit 2).
        OptionalExtraMissingError: A source is ``stac://`` and the ``stac`` extra is missing
            (exit 4).
    """
    sources, has_stac = admit_input_sources(values, param_hint="--input-data")
    from_config = stac_search is None
    if from_config:
        if defaults is None:
            defaults = load_plugin_defaults(ctx, plugin)
        if defaults.get("stac_search") is not None:
            try:
                stac_search = validate_stac_search(defaults["stac_search"])
            except StacSearchError as exc:
                raise click.UsageError(render(exc, ctx), ctx=ctx) from exc
    if stac_search is not None and not has_stac:
        origin = "stac_search in the plugin configuration" if from_config else "--stac-search"
        raise click.UsageError(
            f"{origin} applies to stac:// sources only, and no --input-data names one."
        )
    if has_stac:
        require_stac(command)
    return sources, stac_search
