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

import json
from pathlib import Path

import click

from firecube.cli._errors import render
from firecube.cli._shared_options import dry_run_flag
from firecube.core.errors import ManifestError

from ._manager import resolve_cli_product, resolve_manager


@click.group("compact")
def compact_group() -> None:
    """manage the chunk compact cache

    rebuild and inspect the derived compact cache of chunk records."""


@click.group("snapshots", hidden=True)
def snapshots_alias_group() -> None:
    """hidden alias of chunks compact, kept for one release"""


@compact_group.command(
    "rebuild",
    epilog="""\b
Examples:
  # preview a compact cache rebuild
  firecube chunks compact rebuild --product-name file:///data/products/MY_PRODUCT.zarr --dry-run

\b
  # rebuild the compact cache for a product
  firecube chunks compact rebuild --product-name file:///data/products/MY_PRODUCT.zarr

\b
  # rebuild and output result as JSON
  firecube chunks compact rebuild --product-name file:///data/products/MY_PRODUCT.zarr -f json

See also: firecube chunks compact status, firecube chunks list
""",
)
@click.option(
    "-n",
    "--product-name",
    "product_name",
    required=True,
    help="full product URI to rebuild into a fresh compact cache",
)
@click.option("--workspace", type=click.Path(path_type=Path), help="workspace directory override")
@click.option(
    "-f",
    "--format",
    "output_format",
    type=click.Choice(["table", "json"]),
    default="table",
    help="output format",
)
@dry_run_flag
@click.pass_context
def rebuild_cmd(
    ctx: click.Context,
    product_name: str,
    workspace: Path | None,
    output_format: str,
    dry_run: bool,
) -> None:
    """rebuild the compact cache

    rebuild the derived compact cache for a product from the chunk event log.
    the compact cache is a cached read model; use after it is missing,
    corrupt, or very stale to restore fast query performance for chunks list"""
    product_name, product_uri = resolve_cli_product(product_name)

    if dry_run:
        click.echo(f"[dry-run] Would rebuild compact cache for product '{product_name}'")
        return

    manager = resolve_manager(ctx, workspace, product_uri=product_uri, product_name=product_name)
    result = manager.rebuild_compact(product_name)

    if output_format == "json":
        click.echo(json.dumps(result, indent=2))
        return

    if result.get("locked"):
        click.echo(
            f"Compact cache rebuild skipped for {product_name}: "
            "another rebuild is already in progress."
        )
        return

    click.echo(
        f"Rebuilt compact cache for {product_name}: generation={result.get('generation')} "
        f"records={result.get('records', 0)}"
    )


@compact_group.command(
    "status",
    epilog="""\b
Examples:
  # check compact cache status for a product
  firecube chunks compact status --product-name file:///data/products/MY_PRODUCT.zarr

\b
  # output as JSON
  firecube chunks compact status --product-name file:///data/products/MY_PRODUCT.zarr -f json

See also: firecube chunks compact rebuild, firecube chunks list
""",
)
@click.option(
    "-n",
    "--product-name",
    "product_name",
    required=True,
    help="full product URI to check compact cache status for",
)
@click.option("--workspace", type=click.Path(path_type=Path), help="workspace directory override")
@click.option(
    "-f",
    "--format",
    "output_format",
    type=click.Choice(["table", "json"]),
    default="table",
    help="output format",
)
@click.pass_context
def status_cmd(
    ctx: click.Context,
    product_name: str,
    workspace: Path | None,
    output_format: str,
) -> None:
    """show compact cache age and status

    show compact cache age and status for a product's .firecube/ control plane.
    reports whether the compact cache exists, how many records it holds, and
    when it was last rebuilt. use to decide whether a rebuild is needed"""
    product_name, product_uri = resolve_cli_product(product_name)
    manager = resolve_manager(ctx, workspace, product_uri=product_uri, product_name=product_name)
    try:
        result = manager.compact_status(product_name)
    except ManifestError as exc:
        raise click.ClickException(render(exc)) from exc

    if output_format == "json":
        click.echo(json.dumps(result, indent=2))
        return

    if not result.get("exists"):
        click.echo(f"No compact cache found for {product_name}")
        return

    click.echo(f"Product:    {product_name}")
    click.echo(f"Age:        {result['age_human']}")
    click.echo(f"Cutoff:     {result.get('completed_before', 'unknown')}")
    click.echo(f"Generation: {result.get('generation', 'unknown')}")
    click.echo(f"Records:    {result.get('records', 'unknown')}")


snapshots_alias_group.add_command(rebuild_cmd)
snapshots_alias_group.add_command(status_cmd)
