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

from firecube.cli._shared_options import dry_run_flag

from ._manager import resolve_cli_product, resolve_manager


@click.group("snapshots")
def snapshots_group() -> None:
    """manage control-plane snapshots

    rebuild derived chunk snapshots."""


@snapshots_group.command(
    "rebuild",
    epilog="""\b
Examples:
  # preview a snapshot rebuild
  firecube chunks snapshots rebuild --product-name <product> --dry-run

\b
  # rebuild snapshot for a product
  firecube chunks snapshots rebuild --product-name <product>

\b
  # rebuild and output result as JSON
  firecube chunks snapshots rebuild --product-name <product> -f json

See also: firecube chunks snapshots status, firecube chunks list
""",
)
@click.option(
    "-n",
    "--product-name",
    "product_name",
    required=True,
    help="product to rebuild into a fresh chunk snapshot",
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
    """rebuild the chunk snapshot

    rebuild the derived chunk snapshot for a product from the chunk event log.
    snapshots are cached read models; use after the snapshot is missing,
    corrupt, or very stale to restore fast query performance for chunks list"""
    product_name, product_uri = resolve_cli_product(product_name)

    if dry_run:
        click.echo(f"[dry-run] Would rebuild snapshot for product '{product_name}'")
        return

    manager = resolve_manager(ctx, workspace, product_uri=product_uri, product_name=product_name)
    result = manager.rebuild_snapshot(product_name)

    if output_format == "json":
        click.echo(json.dumps(result, indent=2))
        return

    if result.get("locked"):
        click.echo(
            f"Snapshot rebuild skipped for {product_name}: another rebuild is already in progress."
        )
        return

    click.echo(
        f"Rebuilt snapshot for {product_name}: generation={result.get('generation')} "
        f"records={result.get('records', 0)}"
    )


@snapshots_group.command(
    "status",
    epilog="""\b
Examples:
  # check snapshot status for a product
  firecube chunks snapshots status --product-name <product>

\b
  # output as JSON
  firecube chunks snapshots status --product-name <product> -f json

See also: firecube chunks snapshots rebuild, firecube chunks list
""",
)
@click.option(
    "-n",
    "--product-name",
    "product_name",
    required=True,
    help="product to check snapshot status for",
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
    """show snapshot age and status

    show snapshot age and status for a product's .firecube/ control plane.
    reports whether the snapshot exists, how many events it covers, and when
    it was last rebuilt. use to decide whether a rebuild is needed"""
    product_name, product_uri = resolve_cli_product(product_name)
    manager = resolve_manager(ctx, workspace, product_uri=product_uri, product_name=product_name)
    result = manager.snapshot_status(product_name)

    if output_format == "json":
        click.echo(json.dumps(result, indent=2))
        return

    if not result.get("exists"):
        click.echo(f"No snapshot found for {product_name}")
        return

    click.echo(f"Product:    {product_name}")
    click.echo(f"Age:        {result['age_human']}")
    click.echo(f"Cutoff:     {result.get('completed_before', 'unknown')}")
    click.echo(f"Generation: {result.get('generation', 'unknown')}")
    click.echo(f"Records:    {result.get('records', 'unknown')}")
