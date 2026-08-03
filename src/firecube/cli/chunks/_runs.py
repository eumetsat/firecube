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
import sys
from pathlib import Path

import click

from firecube.cli._product import require_full_uri
from firecube.cli._shared_options import dry_run_flag, yes_flag
from firecube.core.controlplane.types import AbandonSweepResult
from firecube.core.errors import ManifestError
from firecube.core.storage.uri import StorageUri

from ._manager import resolve_cli_product, resolve_manager


@click.group("runs")
def runs_group() -> None:
    """manage ingestion run records

    inspect and manage tracked chunk runs."""


def _echo_abandon_run_result(*, product_name: str, run_id: str, result: dict[str, object]) -> None:
    if not result.get("abandoned"):
        click.echo(f"Run {run_id} already terminal with status={result.get('status')}")
        return
    click.echo(f"Abandoned run {run_id} for {product_name}")


def _echo_abandon_sweep_result(
    *,
    product_name: str,
    result: AbandonSweepResult,
    dry_run: bool,
) -> None:
    if not result.previewed:
        click.echo("No stale runs found.")
        return

    if dry_run:
        click.echo(f"[dry-run] Would abandon stale runs for {product_name}:")
        for run_id in result.previewed:
            click.echo(f"  - {run_id}")
        return

    click.echo(f"Abandoned stale runs for {product_name}:")
    if result.abandoned:
        click.echo("  Abandoned:")
        for run_id in result.abandoned:
            click.echo(f"    - {run_id}")
    if result.skipped_fresh:
        click.echo("  Skipped fresh:")
        for run_id in result.skipped_fresh:
            click.echo(f"    - {run_id}")
    if result.skipped_already_terminal:
        click.echo("  Skipped already terminal:")
        for run_id in result.skipped_already_terminal:
            click.echo(f"    - {run_id}")


@runs_group.command(
    "list",
    epilog="""\b
Examples:
  # list all runs for a product
  firecube chunks runs list --product-name file:///data/products/MY_PRODUCT.zarr

\b
  # filter by status
  firecube chunks runs list --product-name file:///data/products/MY_PRODUCT.zarr --status started

\b
  # output as JSON
  firecube chunks runs list --product-name file:///data/products/MY_PRODUCT.zarr -f json

See also: firecube chunks runs abandon, firecube chunks list
""",
)
@click.option("-n", "--product-name", "product_name", required=True, help="full product URI")
@click.option(
    "--target",
    "target",
    default=None,
    metavar="URI",
    hidden=True,
    help=(
        "Legacy product target override. Prefer passing the full product URI "
        "through --product-name."
    ),
)
@click.option(
    "--status", default=None, help="filter runs by status (started, complete, failed, abandoned)"
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
def list_cmd(
    ctx: click.Context,
    product_name: str,
    target: str | None,
    status: str | None,
    workspace: Path | None,
    output_format: str,
) -> None:
    """list tracked ingestion runs

    list tracked ingestion runs for a product from the .firecube/ control
    plane. shows run status (started, complete, failed, abandoned), timestamps,
    and span counts. use to identify stuck or failed runs before abandoning"""
    if target is not None:
        require_full_uri(target, option_name="--target")
        try:
            product_uri = StorageUri.parse(target)
        except ValueError as exc:
            raise click.UsageError(str(exc)) from exc
    else:
        product_name, product_uri = resolve_cli_product(product_name)
    manager = resolve_manager(ctx, workspace, product_uri=product_uri, product_name=product_name)
    runs = manager.list_runs(product=product_name, status=status)

    if output_format == "json":
        click.echo(
            json.dumps(
                [
                    {
                        "product": run.product,
                        "run_id": run.run_id,
                        "status": run.status,
                        "started_at": run.started_at,
                        "updated_at": run.updated_at,
                        "completed_at": run.completed_at,
                        "slot_range": (
                            list(run.slot_range) if run.slot_range is not None else None
                        ),
                        "slot_group": run.slot_group,
                        "events": run.events,
                        "parts": run.parts,
                        "stale": run.stale,
                        "error": run.error,
                    }
                    for run in runs
                ],
                indent=2,
            )
        )
        return

    if not runs:
        click.echo(f"No runs found for {product_name}.")
        return

    click.echo(f"{'Run ID':<36} {'Status':<10} {'State':<8} {'Parts':<5} {'Events':<6}")
    click.echo("-" * 80)
    for run in runs:
        state = "stale" if run.stale else "active"
        click.echo(f"{run.run_id:<36} {run.status:<10} {state:<8} {run.parts:<5} {run.events:<6}")


@runs_group.command(
    "abandon",
    epilog="""\b
Examples:
  # preview what would be abandoned
  firecube chunks runs abandon --product-name file:///data/products/MY_PRODUCT.zarr --run-id <run-id> --reason "crashed" --dry-run

\b
  # abandon a stuck run in non-interactive context
  firecube chunks runs abandon --product-name file:///data/products/MY_PRODUCT.zarr --run-id <run-id> --reason "process crashed" \\
      --yes-i-really-mean-it

\b
  # preview every stale non-terminal run (bulk mode never prompts)
  firecube chunks runs abandon --product-name file:///data/products/MY_PRODUCT.zarr --all-stale --reason "cluster crash"

\b
  # abandon every stale non-terminal run
  firecube chunks runs abandon --product-name file:///data/products/MY_PRODUCT.zarr --all-stale --reason "cluster crash" \\
      --yes-i-really-mean-it

See also: firecube chunks runs list, firecube chunks list
""",
)
@click.option("-n", "--product-name", "product_name", required=True, help="full product URI")
@click.option(
    "--target",
    "target",
    default=None,
    metavar="URI",
    hidden=True,
    help=(
        "Legacy product target override. Prefer passing the full product URI "
        "through --product-name."
    ),
)
@click.option(
    "--run-id",
    default=None,
    help="run identifier (mutually exclusive with --all-stale)",
)
@click.option(
    "--all-stale",
    is_flag=True,
    default=False,
    help=(
        "preview all stale non-terminal runs for the product; abandon them only with "
        "--yes-i-really-mean-it (mutually exclusive with --run-id)"
    ),
)
@click.option("--reason", required=True, help="operator reason for abandonment")
@click.option("--workspace", type=click.Path(path_type=Path), help="workspace directory override")
@dry_run_flag
@yes_flag
@click.pass_context
def abandon_cmd(
    ctx: click.Context,
    product_name: str,
    target: str | None,
    run_id: str | None,
    all_stale: bool,
    reason: str,
    workspace: Path | None,
    dry_run: bool,
    yes_i_really_mean_it: bool,
) -> None:
    """mark a stuck run as abandoned

    mark a non-terminal ingestion run as abandoned to unblock resume. a run
    stuck in 'started' state blocks future ingestion for that product. use this
    after confirming the original process is no longer active.

    --all-stale never prompts. without --yes-i-really-mean-it it lists stale
    runs as a dry run; with the flag it abandons them. single-run operations
    prompt in a terminal and require the flag in non-TTY contexts."""
    if target is not None:
        require_full_uri(target, option_name="--target")
        try:
            product_uri = StorageUri.parse(target)
        except ValueError as exc:
            raise click.UsageError(str(exc)) from exc
    else:
        product_name, product_uri = resolve_cli_product(product_name)

    if (run_id is not None) == all_stale:
        raise click.UsageError("Provide exactly one of --run-id or --all-stale.")

    if all_stale:
        manager = resolve_manager(
            ctx, workspace, product_uri=product_uri, product_name=product_name
        )
        effective_dry_run = dry_run or not yes_i_really_mean_it
        result = manager.abandon_stale_runs(
            product=product_name,
            reason=reason,
            dry_run=effective_dry_run,
        )
        _echo_abandon_sweep_result(
            product_name=product_name,
            result=result,
            dry_run=effective_dry_run,
        )
        return

    assert run_id is not None

    if not dry_run and not yes_i_really_mean_it:
        if not sys.stdin.isatty():
            raise click.UsageError(
                "Confirmation required: re-run with --yes-i-really-mean-it or run interactively."
            )
        click.confirm(f"Abandon run '{run_id}'?", abort=True)

    if dry_run:
        click.echo(
            f"[dry-run] Would abandon run '{run_id}' for product "
            f"'{product_name}' (reason: {reason})"
        )
        return

    manager = resolve_manager(ctx, workspace, product_uri=product_uri, product_name=product_name)
    try:
        result = manager.abandon_run(product=product_name, run_id=run_id, reason=reason)
    except ManifestError as exc:
        raise click.ClickException(str(exc)) from exc

    _echo_abandon_run_result(product_name=product_name, run_id=run_id, result=result)
