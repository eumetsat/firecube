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

"""``firecube zarr multires``: build multi-resolution pyramid layers in place."""

from __future__ import annotations

import json

import click

from firecube.cli._ctx import get_storage_config
from firecube.cli._errors import render
from firecube.cli._product import recorded_identity
from firecube.cli._shared_options import (
    storage_driver_option,
    storage_type_option,
    target_uri_option,
)
from firecube.cli._uri_policy import (
    apply_smart_default,
    parse_and_validate_uri,
)
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.session import StorageSession
from firecube.core.zarr.layers import DEFAULT_MULTIRES_RESOLUTIONS
from firecube.core.zarr.multires import MultiresConfig, ZarrMultiresBuilder


@click.command(
    "multires",
    epilog="""\b
Examples:
  Default multi-resolution layers for a local Zarr product:
  firecube zarr multires --target file:///data/p.zarr

\b
  Explicit resolution levels:
  firecube zarr multires --target s3://b/p.zarr --resolutions 1.0 --resolutions 0.5

\b
  Dataset in a group, with coordinates not named lat and lon:
  firecube zarr multires --target file:///data/p.zarr --group data \\
      --lat-name latitude --lon-name longitude

\b
See also: firecube zarr validate
""",
)
@target_uri_option(tier="write")
@click.option(
    "--resolutions",
    "resolutions",
    "-r",
    multiple=True,
    type=float,
    help="Resolution levels to build (repeatable, e.g. -r 1.0 -r 0.5).",
)
@click.option(
    "-g",
    "--group",
    "group",
    default=None,
    help="Group that holds the dataset; omit, '/' or '' for the root. Levels go to <group>/multires/<res>deg.",
)
@click.option(
    "--lat-name",
    "lat_name",
    default="lat",
    show_default=True,
    help="Name of the 1-D latitude coordinate to coarsen.",
)
@click.option(
    "--lon-name",
    "lon_name",
    default="lon",
    show_default=True,
    help="Name of the 1-D longitude coordinate to coarsen.",
)
@storage_type_option(
    required=False, extra_help="Inferred from URI scheme when omitted (file://→local, s3://→s3)."
)
@storage_driver_option(required=False)
@click.pass_context
def multires(
    ctx: click.Context,
    target: str,
    resolutions: tuple[float, ...],
    group: str | None,
    lat_name: str,
    lon_name: str,
    storage_type: str | None,
    storage_driver: str | None,
    storage_anonymous: bool | None,
) -> None:
    """Build multi-resolution Zarr pyramid for an existing product."""
    target_uri = parse_and_validate_uri(target, param_hint="--target")
    storage_type = apply_smart_default(target_uri, storage_type)
    storage_config = get_storage_config(
        ctx,
        overrides={
            "storage_type": storage_type,
            "storage_driver": storage_driver,
            "anonymous": storage_anonymous,
        },
        cache=False,
    )
    driver_config = StorageDriverConfig.from_storage_config(storage_config)
    identity = recorded_identity(target_uri, format="zarr", driver=driver_config)
    session = StorageSession(
        StorageBinding(
            identity=identity,
            driver=driver_config,
        )
    )
    config = MultiresConfig(
        product=identity.product_name,
        group=(group or "").strip("/"),
        resolutions=list(resolutions) or list(DEFAULT_MULTIRES_RESOLUTIONS),
        lat_name=lat_name,
        lon_name=lon_name,
    )
    try:
        result = ZarrMultiresBuilder(
            storage_config,
            product_name=identity.product_name,
            product_uri=identity.product_uri,
            session=session,
        ).build(config)
    except ValueError as exc:
        raise click.ClickException(render(exc)) from exc

    click.echo(json.dumps(result, indent=2))
