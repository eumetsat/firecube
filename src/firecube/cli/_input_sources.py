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
from collections.abc import Sequence
from typing import Any

import click

from firecube.cli._errors import render
from firecube.cli._uri_policy import admit_input_sources
from firecube.core.discovery import StacSearchError, require_stac, validate_stac_search

INPUT_DATA_HELP = (
    "Raw plugin input data: local path, file:///abs/path, s3:// prefix, or "
    "stac://host/api-root/collections/<id> (one STAC API collection). Repeat to read "
    "several sources in one run. Interpreted by the plugin."
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
            "comes from the stac:// URI."
        ),
    )(f)


def admit_sources(
    values: Sequence[str], stac_search: dict[str, Any] | None, *, command: str
) -> tuple[str, ...]:
    """Admit the sources of one command, before any network or store access.

    Refuses a bad ``stac://`` value, a ``--stac-search`` without a ``stac://`` source,
    and a ``stac://`` source when the ``stac`` extra is missing.

    Args:
        values: The ``--input-data`` values, in order.
        stac_search: The parsed ``--stac-search`` value, or ``None``.
        command: The running command, named when the extra is missing.

    Returns:
        The values, in order.

    Raises:
        click.BadParameter: A ``stac://`` value breaks the URI rule (exit 2).
        click.UsageError: ``--stac-search`` was given and no source is ``stac://`` (exit 2).
        click.ClickException: A source is ``stac://`` and the ``stac`` extra is missing.
    """
    sources, has_stac = admit_input_sources(values, param_hint="--input-data")
    if stac_search is not None and not has_stac:
        raise click.UsageError(
            "--stac-search applies to stac:// sources only, and no --input-data names one."
        )
    if has_stac:
        try:
            require_stac(command)
        except ImportError as exc:
            raise click.ClickException(str(exc)) from exc
    return sources
