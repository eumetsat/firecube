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

"""``firecube migrate run`` and ``firecube migrate adopt``."""

from __future__ import annotations

from typing import Any

import click

from firecube.cli._shared_options import dry_run_flag
from firecube.cli.migrate._common import (
    echo_json,
    echo_progress,
    echo_report,
    echo_warning,
    merge_step_options,
    migrate_options,
    open_session,
)
from firecube.cli.plugins.registry import load_migration_steps
from firecube.ingestor.runtime.migrate import (
    AdoptionResult,
    MigrationResult,
    adopt_product,
    migrate_product,
)


def _result_payload(result: MigrationResult) -> dict[str, Any]:
    release = result.release
    return {
        "outcome": result.outcome,
        "run_id": result.run_id,
        "resumed_from": result.resumed_from,
        "step_id": result.step_id,
        "distribution": result.distribution,
        "version": result.version,
        "before_snapshot": result.before_snapshot,
        "after_snapshot": result.after_snapshot,
        "documents": list(result.documents),
        "verdicts": [
            {
                "node_path": verdict.node_path,
                "key": verdict.key,
                "attribute_class": verdict.attribute_class,
                "allowed": verdict.allowed,
            }
            for verdict in result.verdicts
        ],
        "release": None
        if release is None
        else {"version": release.version, "snapshot": release.snapshot},
    }


def _adoption_payload(result: AdoptionResult) -> dict[str, Any]:
    return {
        "outcome": result.outcome,
        "run_id": result.run_id,
        "zarr_format": result.zarr_format,
    }


@click.command(
    "run",
    epilog="""\b
Examples:
  # show what setting one root attribute would change, and write nothing
  firecube migrate run firecube:set-attrs --target file:///data/products/MY_PRODUCT.zarr --product-name MY_PRODUCT --set title=Daily --dry-run

\b
  # set an attribute and remove another on the root group
  firecube migrate run firecube:set-attrs --target file:///data/products/MY_PRODUCT.zarr --product-name MY_PRODUCT --set title=Daily --remove comment

\b
  # run a step that a plugin installs, with one option of that step
  firecube migrate run my-plugin:fix-units --target file:///data/products/MY_PRODUCT.zarr --product-name MY_PRODUCT --option variable=temperature

See also: firecube migrate list, firecube migrate status, firecube snapshot list
""",
)
@click.argument("step_id", metavar="STEP_ID")
@migrate_options(tier="write")
@click.option(
    "--allow",
    "allow",
    multiple=True,
    type=click.Choice(["interpretation", "unknown"], case_sensitive=False),
    help=(
        "Also change attributes of this class; repeatable. Descriptive attributes need "
        "no flag, physical attributes are never changed."
    ),
)
@click.option(
    "--option",
    "step_options",
    multiple=True,
    metavar="KEY=VALUE",
    help=(
        "Option of the step; repeatable. A key given once is a string, a key given "
        "more than once is a list in the order given."
    ),
)
@click.option(
    "--set",
    "sets",
    multiple=True,
    metavar="KEY=VALUE",
    help="firecube:set-attrs only: set an attribute to a string; repeatable.",
)
@click.option(
    "--remove",
    "removes",
    multiple=True,
    metavar="KEY",
    help="firecube:set-attrs only: remove an attribute; repeatable.",
)
@click.option(
    "--node",
    "node",
    default=None,
    help="firecube:set-attrs only: node path to change; the root group when omitted.",
)
@dry_run_flag
@click.pass_context
def run(
    ctx: click.Context,
    step_id: str,
    target: str,
    product_name: str,
    storage_type: str | None,
    storage_driver: str | None,
    storage_anonymous: bool | None,
    output_format: str,
    allow: tuple[str, ...],
    step_options: tuple[str, ...],
    sets: tuple[str, ...],
    removes: tuple[str, ...],
    node: str | None,
    dry_run: bool,
) -> None:
    """run one migration step on a product

    changes Zarr metadata documents only, never a chunk. the run holds the
    exclusive maintenance claim, takes a snapshot before and after, and records
    a maintenance run. a store with no control plane is refused: run migrate
    adopt first. a step that already ran is not run again. an interrupted
    run of the step resumes. --dry-run prints the plan and the attribute classes
    and writes nothing, also on a store with no control plane. pass --allow interpretation or --allow unknown to change
    attributes of that class. with --format json the progress lines go to stderr.
    """
    options = merge_step_options(
        step_id, options=step_options, sets=sets, removes=removes, node=node
    )
    session = open_session(
        ctx,
        command="migrate run",
        target=target,
        product_name=product_name,
        storage_type=storage_type,
        storage_driver=storage_driver,
        storage_anonymous=storage_anonymous,
        writable=True,
    )
    json_out = output_format == "json"
    result = migrate_product(
        session=session,
        step_id=step_id,
        load_steps=load_migration_steps,
        step_options=options,
        allow=tuple(a.lower() for a in allow),
        dry_run=dry_run,
        report=echo_progress if json_out else echo_report,
        warn=echo_warning,
    )
    if json_out:
        echo_json(_result_payload(result))


@click.command(
    "adopt",
    epilog="""\b
Examples:
  # give a Zarr store that Firecube did not write a control plane
  firecube migrate adopt --target file:///data/products/MY_PRODUCT.zarr --product-name MY_PRODUCT

See also: firecube migrate run, firecube migrate status
""",
)
@migrate_options(tier="write")
@click.pass_context
def adopt(
    ctx: click.Context,
    target: str,
    product_name: str,
    storage_type: str | None,
    storage_driver: str | None,
    storage_anonymous: bool | None,
    output_format: str,
) -> None:
    """give an existing Zarr store a control plane

    records the store as an imported product in one adopt run. no Zarr document
    and no chunk is written, and no span history is invented. a store that is
    already a product stays as it is. adopting a Zarr format 2 store prints a
    warning, because Firecube cannot append to it. with --format json the progress lines go
    to stderr.
    """
    session = open_session(
        ctx,
        command="migrate adopt",
        target=target,
        product_name=product_name,
        storage_type=storage_type,
        storage_driver=storage_driver,
        storage_anonymous=storage_anonymous,
        writable=True,
    )
    json_out = output_format == "json"
    result = adopt_product(
        session=session,
        report=echo_progress if json_out else echo_report,
        warn=echo_warning,
    )
    if json_out:
        echo_json(_adoption_payload(result))
