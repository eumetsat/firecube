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
from typing import Any

import click

from firecube.cli._shared_options import dry_run_flag, format_option, target_uri_option, yes_flag
from firecube.core.controlplane.types import AbandonSweepResult

from ._manager import target_product


@click.group("runs")
def runs_group() -> None:
    """manage ingestion run records

    inspect and manage tracked chunk runs."""


def _echo_claims(
    *, released: list[str], skipped: list[str], indent: str = "", dry_run: bool = False
) -> None:
    release, leave = ("Would release", "Would leave") if dry_run else ("Released", "Left")
    for domain in released:
        click.echo(f"{indent}{release} claim {domain}")
    for domain in skipped:
        click.echo(f"{indent}{leave} claim {domain}")


def _echo_abandon_run_result(*, product_name: str, run_id: str, result: dict[str, Any]) -> None:
    if not result.get("abandoned"):
        click.echo(f"Run {run_id} already terminal with status={result.get('status')}")
    else:
        click.echo(f"Abandoned run {run_id} for {product_name}")
    _echo_claims(
        released=result.get("released_claims", []),
        skipped=result.get("skipped_claims", []),
    )


def _echo_abandon_sweep_result(
    *,
    product_name: str,
    result: AbandonSweepResult,
    dry_run: bool,
) -> None:
    """Print a sweep result; claim lines print whenever the result has them."""
    if not result.previewed:
        click.echo("No stale runs found.")
        _echo_claims(
            released=result.released_claims, skipped=result.skipped_claims, dry_run=dry_run
        )
        return

    if dry_run:
        click.echo(f"[dry-run] Would abandon stale runs for {product_name}:")
        for run_id in result.previewed:
            click.echo(f"  - {run_id}")
        _echo_claims(
            released=result.released_claims,
            skipped=result.skipped_claims,
            indent="  ",
            dry_run=True,
        )
        return

    click.echo(f"Abandoned stale runs for {product_name}:")
    if result.abandoned:
        click.echo("  Abandoned:")
        for run_id in result.abandoned:
            click.echo(f"    - {run_id}")
    _echo_claims(released=result.released_claims, skipped=result.skipped_claims, indent="  ")
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
  firecube chunks runs list --target file:///data/products/MY_PRODUCT.zarr

\b
  # filter by status
  firecube chunks runs list --target file:///data/products/MY_PRODUCT.zarr --status started

\b
  # output as JSON
  firecube chunks runs list --target file:///data/products/MY_PRODUCT.zarr -f json

See also: firecube chunks runs abandon, firecube chunks list
""",
)
@target_uri_option("inspect")
@click.option(
    "--status", default=None, help="filter runs by status (started, complete, failed, abandoned)"
)
@click.option("--workspace", type=click.Path(path_type=Path), help="workspace directory override")
@format_option(("table", "json"))
@click.pass_context
def list_cmd(
    ctx: click.Context,
    target: str,
    status: str | None,
    workspace: Path | None,
    output_format: str,
) -> None:
    """list tracked ingestion runs

    list tracked ingestion runs for a product from the .firecube/ control
    plane. shows run status (started, complete, failed, abandoned), timestamps,
    and span counts. use to identify stuck or failed runs before abandoning"""
    product = target_product(ctx, workspace, target)
    product_name = product.label
    runs = product.manager.list_runs(product=product_name, status=status)

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
                        "timestamps_skipped": run.timestamps_skipped,
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

    show_skipped = any(run.timestamps_skipped > 0 for run in runs)
    header = f"{'Run ID':<36} {'Status':<10} {'State':<8} {'Parts':<5} {'Events':<6}"
    if show_skipped:
        header = f"{header} {'Skipped':<7}"
    click.echo(header)
    click.echo("-" * (87 if show_skipped else 80))

    for run in runs:
        state = "stale" if run.stale else "active"
        row = f"{run.run_id:<36} {run.status:<10} {state:<8} {run.parts:<5} {run.events:<6}"
        if show_skipped:
            row = f"{row} {run.timestamps_skipped:<7}"
        click.echo(row)


@runs_group.command(
    "abandon",
    epilog="""\b
Examples:
  # preview what would be abandoned
  firecube chunks runs abandon --target file:///data/products/MY_PRODUCT.zarr --run-id <run-id> --reason "crashed" --dry-run

\b
  # abandon a stuck run in non-interactive context
  firecube chunks runs abandon --target file:///data/products/MY_PRODUCT.zarr --run-id <run-id> --reason "process crashed" \\
      --yes-i-really-mean-it

\b
  # preview every stale non-terminal run (bulk mode never prompts)
  firecube chunks runs abandon --target file:///data/products/MY_PRODUCT.zarr --all-stale --reason "cluster crash"

\b
  # abandon every stale non-terminal run
  firecube chunks runs abandon --target file:///data/products/MY_PRODUCT.zarr --all-stale --reason "cluster crash" \\
      --yes-i-really-mean-it

See also: firecube chunks runs list, firecube chunks list
""",
)
@target_uri_option("inspect")
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
    target: str,
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

    abandoning also releases the run's write claims and prints each one; a
    claim whose owner is ambiguous stays and prints as 'Left claim'. a run that
    is already terminal keeps its status and still has its claims released.
    when the claims cannot be listed, the abandon is still recorded and the
    command exits 1; run it again to release them. a committed --all-stale
    sweep releases a claim only when it is stale, for the runs it abandons and
    runs already terminal; a fresh claim prints as 'Left claim'.

    --all-stale never prompts. without --yes-i-really-mean-it it lists stale
    runs and the claims it would release as a dry run; with the flag it
    abandons them. single-run operations
    prompt in a terminal and require the flag in non-TTY contexts."""
    if (run_id is not None) == all_stale:
        raise click.UsageError("Provide exactly one of --run-id or --all-stale.")

    if all_stale:
        product = target_product(ctx, workspace, target)
        product_name = product.label
        effective_dry_run = dry_run or not yes_i_really_mean_it
        result = product.manager.abandon_stale_runs(
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

    product = target_product(ctx, workspace, target)
    product_name = product.label
    if dry_run:
        click.echo(
            f"[dry-run] Would abandon run '{run_id}' for product "
            f"'{product_name}' (reason: {reason})"
        )
        return

    result = product.manager.abandon_run(product=product_name, run_id=run_id, reason=reason)

    _echo_abandon_run_result(product_name=product_name, run_id=run_id, result=result)
