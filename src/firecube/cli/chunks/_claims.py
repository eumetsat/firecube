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

from firecube.cli._shared_options import dry_run_flag, yes_flag
from firecube.core.controlplane.types import ClearSweepResult
from firecube.core.errors import ClaimConflictError

from ._manager import resolve_cli_product, resolve_manager


@click.group("claims")
def claims_group() -> None:
    """manage write-coordination claims

    inspect and manage chunk write claims."""


@claims_group.command(
    "list",
    epilog="""\b
Examples:
  # list all claims across all products
  firecube chunks claims list

\b
  # list claims for a specific product
  firecube chunks claims list --product-name <product>

See also: firecube chunks claims clear, firecube chunks runs list
""",
)
@click.option("-n", "--product-name", "product_name", help="filter by product name")
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
    product_name: str | None,
    workspace: Path | None,
    output_format: str,
) -> None:
    """list active write claims

    list active write-coordination claims for one or all products. claims are
    short-lived locks written to .firecube/claims/ to coordinate concurrent
    writes. stale claims from crashed processes can block ingestion -- use
    claims clear to release them"""
    product_name, product_uri = resolve_cli_product(product_name)
    manager = resolve_manager(ctx, workspace, product_uri=product_uri, product_name=product_name)
    claims = manager.list_claims(product=product_name)

    if output_format == "json":
        click.echo(
            json.dumps(
                [
                    {
                        "product": claim.product,
                        "domain": claim.domain,
                        "owner_id": claim.owner_id,
                        "claim_path": claim.claim_path,
                        "acquired_at": claim.acquired_at,
                        "last_heartbeat_at": claim.last_heartbeat_at,
                        "heartbeat_interval_s": claim.heartbeat_interval_s,
                        "stale_threshold_s": claim.stale_threshold_s,
                        "stale": claim.stale,
                    }
                    for claim in claims
                ],
                indent=2,
            )
        )
        return

    if not claims:
        click.echo("No claims found.")
        return

    click.echo(f"{'Product':<24} {'State':<8} {'Owner':<24} Domain")
    click.echo("-" * 92)
    for claim in claims:
        state = "stale" if claim.stale else "active"
        click.echo(f"{claim.product:<24} {state:<8} {claim.owner_id:<24} {claim.domain}")


@claims_group.command(
    "clear",
    epilog="""\b
Examples:
  # clear a stale claim (interactive confirmation)
  firecube chunks claims clear --product-name <product> --domain <domain>

\b
  # clear a stale claim in non-interactive context
  firecube chunks claims clear --product-name <product> --domain <domain> --yes-i-really-mean-it

\b
  # bypass the is_stale check (--force) and confirm (--yes-i-really-mean-it)
  firecube chunks claims clear --product-name <product> --domain <domain> --force --yes-i-really-mean-it

See also: firecube chunks claims list, firecube chunks runs abandon
""",
)
@click.option(
    "-n", "--product-name", "product_name", required=True, help="product owning the claim"
)
@click.option("--domain", "domain_id", help="full write-domain identifier")
@click.option(
    "--all-stale",
    "all_stale",
    is_flag=True,
    default=False,
    help="clear all stale claims for the product (mutually exclusive with --domain)",
)
@click.option("--workspace", type=click.Path(path_type=Path), help="workspace directory override")
@click.option(
    "--force",
    is_flag=True,
    help="operational bypass: clear even if the claim is still active (skips is_stale check)",
)
@dry_run_flag
@yes_flag
@click.pass_context
def clear_cmd(
    ctx: click.Context,
    product_name: str,
    domain_id: str | None,
    all_stale: bool,
    workspace: Path | None,
    force: bool,
    dry_run: bool,
    yes_i_really_mean_it: bool,
) -> None:
    """clear a blocking write claim

    clear a blocking write-coordination claim for a product and domain.
    claims are normally released when a writer exits cleanly; use this when a
    crashed process left a stale claim that blocks ingestion. verify the
    original writer is no longer running first.

    --force bypasses the is_stale check (operational bypass for active claims).
    --yes-i-really-mean-it is the confirmation flag, required in non-TTY contexts."""
    if bool(domain_id) == all_stale:
        raise click.UsageError("Provide exactly one of --domain or --all-stale.")

    if dry_run and not all_stale:
        raise click.UsageError("--dry-run is only supported with --all-stale.")

    product_name, product_uri = resolve_cli_product(product_name)
    manager = resolve_manager(ctx, workspace, product_uri=product_uri, product_name=product_name)

    if all_stale:
        dry_run = dry_run or not yes_i_really_mean_it
        result = manager.clear_stale_claims(product=product_name, dry_run=dry_run)
        _echo_clear_sweep_result(result, dry_run=dry_run)
        return

    if not yes_i_really_mean_it:
        if not sys.stdin.isatty():
            raise click.UsageError(
                "Confirmation required: re-run with --yes-i-really-mean-it or run interactively."
            )
        click.confirm("Clear claims?", abort=True)

    if domain_id is None:
        raise click.UsageError("Provide --domain when not using --all-stale.")
    try:
        cleared = manager.clear_claim(product=product_name, domain_id=domain_id, force=force)
    except ClaimConflictError as exc:
        raise click.ClickException(str(exc)) from exc

    if not cleared:
        raise click.ClickException(f"No claim found for product={product_name} domain={domain_id}")

    click.echo(f"Cleared claim for {domain_id}")


def _echo_clear_sweep_result(result: ClearSweepResult, *, dry_run: bool) -> None:
    if not result.previewed:
        click.echo("No stale claims found")
        return

    click.echo("Previewed stale claims:")
    for domain_id in result.previewed:
        click.echo(f"  - {domain_id}")

    if dry_run:
        click.echo("Dry run only; no claims cleared.")
    else:
        click.echo("Cleared stale claims:")
        if result.cleared:
            for domain_id in result.cleared:
                click.echo(f"  - {domain_id}")
        else:
            click.echo("  - none")

    click.echo(f"Skipped fresh claims: {len(result.skipped_fresh)}")
    if result.skipped_fresh:
        for domain_id in result.skipped_fresh:
            click.echo(f"  - {domain_id}")
    click.echo(f"Skipped missing claims: {len(result.skipped_missing)}")
    if result.skipped_missing:
        for domain_id in result.skipped_missing:
            click.echo(f"  - {domain_id}")
