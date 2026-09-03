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
from firecube.cli._product import resolve_product_identity
from firecube.cli._shared_options import (
    product_name_option,
    storage_driver_option,
    storage_type_option,
)
from firecube.cli._uri_policy import (
    apply_smart_default,
    parse_product_uri,
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
  firecube zarr multires --target file:///p.zarr --product-name p \\
      --storage-type local --storage-driver fsspec

\b
  Explicit resolution levels:
  firecube zarr multires --target s3://b/p.zarr --product-name p \\
      --storage-type s3 --storage-driver fsspec -r 1.0 -r 0.5

\b
See also: firecube zarr validate
""",
)
@click.option(
    "-t",
    "--target",
    "target",
    required=True,
    help="Target product URI required (file:///abs/path or s3://bucket/key); scheme must match --storage-type.",
)
@click.option(
    "--resolutions",
    "resolutions",
    "-r",
    multiple=True,
    type=float,
    help="Resolution levels to build (repeatable, e.g. -r 1.0 -r 0.5).",
)
@product_name_option(required=True)
@storage_type_option(
    required=False, extra_help="Inferred from URI scheme when omitted (file://→local, s3://→s3)."
)
@storage_driver_option(required=False)
@click.pass_context
def multires(
    ctx: click.Context,
    target: str,
    resolutions: tuple[float, ...],
    product_name: str,
    storage_type: str,
    storage_driver: str,
) -> None:
    """Build multi-resolution Zarr pyramid for an existing product."""
    parsed = parse_product_uri(target)
    storage_type = apply_smart_default(parsed, storage_type)
    storage_config = get_storage_config(
        ctx,
        overrides={"storage_type": storage_type, "storage_driver": storage_driver},
        cache=False,
    )
    driver_config = StorageDriverConfig.from_storage_config(storage_config)
    identity = resolve_product_identity(
        target,
        format="zarr",
        product_name=product_name,
        option_name="--target",
    )
    session = StorageSession(
        StorageBinding(
            identity=identity,
            driver=driver_config,
        )
    )
    config = MultiresConfig(
        product=identity.product_name,
        group="",
        resolutions=list(resolutions) or list(DEFAULT_MULTIRES_RESOLUTIONS),
    )
    try:
        result = ZarrMultiresBuilder(
            storage_config,
            product_name=identity.product_name,
            product_uri=identity.product_uri,
            session=session,
        ).build(config)
    except (ImportError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo(json.dumps(result, indent=2))
