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

"""``firecube zarr consolidate-time-coord``: densify legacy per-slot time coordinates."""

from __future__ import annotations

import click

from firecube.cli._ctx import get_storage_config
from firecube.cli._errors import echo_rendered, exit_with_error
from firecube.cli._exit_codes import ExitCode
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
from firecube.core.storage.driver_config import StorageDriverConfig


@click.command(
    "consolidate-time-coord",
    epilog="""\b
Examples:
  # Preview consolidation on an existing cube with chunked time coordinates
  firecube zarr consolidate-time-coord --target file:///data/x.zarr --dry-run

See also: firecube zarr preallocate, firecube zarr validate
""",
)
@target_uri_option(tier="write")
@storage_type_option(
    required=False, extra_help="Inferred from URI scheme when omitted (file://→local, s3://→s3)."
)
@storage_driver_option(required=False)
@click.option(
    "--dry-run",
    "dry_run",
    is_flag=True,
    default=False,
    help="Report proposed changes without mutating the store.",
)
@click.option(
    "--chunk-size",
    "chunk_size",
    default=None,
    type=int,
    help="Override the consolidated coordinate chunk size (default: computed from the axis length).",
)
@click.option(
    "--time-dim",
    "time_dim",
    default=None,
    help=(
        "Explicit time-dimension name for cubes written with a custom "
        "``time_dim_name`` (plugin ``BaseIngestor.time_dim_name`` ClassVar). "
        "When omitted, the effective name is discovered per group from the "
        "resolved-index record (``params.coordinate``) and then from the "
        "group's 1-D datetime coord arrays. Pass this when discovery cannot "
        "settle on a single candidate or must be overridden."
    ),
)
@click.pass_context
def consolidate_time_coord(
    ctx: click.Context,
    target: str,
    storage_type: str | None,
    storage_driver: str | None,
    storage_anonymous: bool | None,
    dry_run: bool,
    chunk_size: int | None,
    time_dim: str | None,
) -> None:
    """Consolidate time coordinate chunks from per-slot storage to dense storage.

    Use this command on existing cubes produced before dense time-coordinate
    materialization. After consolidation, the time coordinate is stored in a
    denser chunk layout so object storage opens the cube with less metadata work.

    Consolidation seals the cube. After it completes, the cube becomes
    read-only for further ingest. The operation is irreversible without a
    restore from backup.

    The rewrite stages the new coordinate as a time.consolidating sibling
    before replacing the original array. If a local consolidation is
    interrupted, re-run the command: it detects the partial state, repairs
    it, and preserves the coordinate values.

    Use it for existing cubes with chunks=(1,) on the time coordinate.
    Always run with --dry-run first. Exits 1 when no group has a time coordinate.
    """
    from firecube.core.zarr.coord_consolidation import consolidate_time_coords

    target_uri = parse_and_validate_uri(target, param_hint="--target")
    storage_type = apply_smart_default(target_uri, storage_type)
    if chunk_size is not None and chunk_size <= 0:
        raise click.ClickException("--chunk-size must be a positive integer.")

    if storage_type.lower() == "s3":
        raise click.ClickException(
            "remote (s3) consolidate-time-coord is not currently supported due to non-atomic crash-recovery; see follow-up plan"
        )

    storage_config = get_storage_config(
        ctx,
        overrides={
            "storage_type": storage_type,
            "storage_driver": storage_driver,
            "anonymous": storage_anonymous,
        },
        cache=False,
    )
    identity = recorded_identity(
        target_uri, format="zarr", driver=StorageDriverConfig.from_storage_config(storage_config)
    )
    summary = consolidate_time_coords(
        target=target,
        product_name=identity.product_name,
        storage_config=storage_config,
        chunk_size=chunk_size,
        time_dim=time_dim,
        dry_run=dry_run,
        report=echo_rendered,
    )

    if dry_run:
        click.echo("Summary:")
        click.echo(f"  Total groups scanned: {summary.scanned}")
        click.echo(f"  Groups eligible for consolidation: {summary.eligible}")
        click.echo(f"  Groups already sealed: {summary.already_sealed}")
        click.echo(f"  Groups blocked by drift: {summary.blocked_by_drift}")
        if summary.blocked_by_drift > 0:
            raise click.exceptions.Exit(ExitCode.CHECK_FAILED)
    if summary.eligible == 0 and summary.already_sealed == 0 and summary.blocked_by_drift == 0:
        exit_with_error(
            "no group has a time coordinate; pass --time-dim to name the time dimension.",
            ExitCode.CHECK_FAILED,
        )
