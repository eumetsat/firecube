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

"""``firecube migrate list`` and ``firecube migrate status``."""

from __future__ import annotations

import datetime

import click

from firecube.cli._shared_options import format_option
from firecube.cli.migrate._common import echo_json, migrate_options, open_target
from firecube.operations import list_migrations, load_migration_steps


def _iso(timestamp: float) -> str:
    moment = datetime.datetime.fromtimestamp(timestamp, tz=datetime.UTC)
    return moment.isoformat(timespec="seconds")


@click.command(
    "list",
    epilog="""\b
Examples:
  # list the migration steps that installed packages register
  firecube migrate list

See also: firecube migrate run
""",
)
@format_option(("table", "json"))
def list_cmd(output_format: str) -> None:
    """list the installed migration steps

    prints the id, the distribution and the version of each step. reads the
    entry points and imports no step. needs no product.
    """
    steps = load_migration_steps()
    if output_format == "json":
        echo_json(
            [{"id": s.id, "distribution": s.distribution, "version": s.version} for s in steps]
        )
        return
    if not steps:
        click.echo("no migration steps installed")
        return
    for step in steps:
        click.echo(f"{step.id}  {step.distribution or '(no distribution)'}  {step.version or '-'}")


@click.command(
    "status",
    epilog="""\b
Examples:
  # list the migration runs recorded for a product
  firecube migrate status --target file:///data/products/MY_PRODUCT.zarr

See also: firecube migrate run, firecube migrate list
""",
)
@migrate_options(tier="inspect")
@click.pass_context
def status(
    ctx: click.Context,
    target: str,
    storage_type: str | None,
    storage_driver: str | None,
    storage_anonymous: bool | None,
    output_format: str,
) -> None:
    """list the migration runs recorded for a product

    prints, oldest first, each run with its step, the step's distribution and
    version, its outcome (executed or adopted) and its status. reads the
    maintenance records and changes nothing.
    """
    product = open_target(
        ctx,
        command="migrate status",
        target=target,
        storage_type=storage_type,
        storage_driver=storage_driver,
        storage_anonymous=storage_anonymous,
        writable=False,
    )
    runs = list_migrations(product)

    if output_format == "json":
        echo_json(
            [
                {
                    "run_id": run.run_id,
                    "status": run.status,
                    "step_id": run.step_id,
                    "distribution": run.distribution,
                    "version": run.version,
                    "outcome": run.outcome,
                    "started_at": run.started_at,
                }
                for run in runs
            ]
        )
        return
    if not runs:
        click.echo("no migration runs recorded")
        return
    for run in runs:
        click.echo(
            f"{_iso(run.started_at)}  {run.run_id}  {run.step_id}  "
            f"{run.distribution} {run.version}  {run.outcome}  {run.status}"
        )
