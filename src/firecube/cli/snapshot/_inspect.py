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

"""``firecube snapshot list``, ``show`` and ``diff``."""

from __future__ import annotations

import click

from firecube.cli._errors import render
from firecube.cli.snapshot._common import open_manager, snapshot_options
from firecube.cli.snapshot._render import (
    diff_payload,
    echo_diff,
    echo_json,
    echo_record_line,
    echo_snapshot,
    record_payload,
    snapshot_payload,
)
from firecube.core.errors import FirecubeError


@click.command(
    "list",
    epilog="""\b
Examples:
  # list the snapshots of a product, oldest first
  firecube snapshot list --target file:///data/products/MY_PRODUCT.zarr --product-name MY_PRODUCT

\b
  # list them as JSON
  firecube snapshot list --target file:///data/products/MY_PRODUCT.zarr --product-name MY_PRODUCT -f json

See also: firecube snapshot show, firecube snapshot take
""",
)
@snapshot_options(tier="inspect")
@click.pass_context
def list_cmd(
    ctx: click.Context,
    target: str,
    product_name: str,
    storage_type: str | None,
    storage_driver: str | None,
    storage_anonymous: bool | None,
    output_format: str,
) -> None:
    """list the metadata snapshots of a product

    prints one line per snapshot, oldest first: record hash, time,
    consistency, trigger and run. a product with no snapshot prints a notice,
    or an empty list in json, and exits 0.
    """
    manager, product = open_manager(
        ctx,
        target=target,
        product_name=product_name,
        storage_type=storage_type,
        storage_driver=storage_driver,
        storage_anonymous=storage_anonymous,
        writable=False,
    )
    try:
        records = manager.list_snapshots(product)
    except FirecubeError as exc:
        raise click.ClickException(render(exc)) from exc

    if output_format == "json":
        echo_json([record_payload(record) for record in records])
        return
    if not records:
        click.echo("No metadata snapshots")
        return
    for record in records:
        echo_record_line(record)


@click.command(
    "show",
    epilog="""\b
Examples:
  # show the newest snapshot
  firecube snapshot show latest --target file:///data/products/MY_PRODUCT.zarr --product-name MY_PRODUCT

\b
  # show one snapshot by its record hash, as JSON
  firecube snapshot show <record-hash> --target file:///data/products/MY_PRODUCT.zarr --product-name MY_PRODUCT -f json

See also: firecube snapshot list, firecube snapshot diff
""",
)
@click.argument("snapshot_id", metavar="SNAPSHOT")
@snapshot_options(tier="inspect")
@click.pass_context
def show(
    ctx: click.Context,
    snapshot_id: str,
    target: str,
    product_name: str,
    storage_type: str | None,
    storage_driver: str | None,
    storage_anonymous: bool | None,
    output_format: str,
) -> None:
    """show one snapshot and the documents it holds

    SNAPSHOT is a record hash from snapshot list, or latest for the newest
    snapshot. prints the record and every captured document with its kind,
    size and digest. exits 1 when no such snapshot exists.
    """
    manager, product = open_manager(
        ctx,
        target=target,
        product_name=product_name,
        storage_type=storage_type,
        storage_driver=storage_driver,
        storage_anonymous=storage_anonymous,
        writable=False,
    )
    try:
        snapshot = manager.read_snapshot(product, snapshot_id)
    except FirecubeError as exc:
        raise click.ClickException(render(exc)) from exc

    if output_format == "json":
        echo_json(snapshot_payload(snapshot))
        return
    echo_snapshot(snapshot)


@click.command(
    "diff",
    epilog="""\b
Examples:
  # compare an older snapshot with the newest one
  firecube snapshot diff <record-hash> latest --target file:///data/products/MY_PRODUCT.zarr --product-name MY_PRODUCT

\b
  # print the differences as JSON
  firecube snapshot diff <record-hash> latest --target file:///data/products/MY_PRODUCT.zarr --product-name MY_PRODUCT -f json

See also: firecube snapshot list, firecube snapshot show
""",
)
@click.argument("before", metavar="BEFORE")
@click.argument("after", metavar="AFTER")
@snapshot_options(tier="inspect")
@click.pass_context
def diff(
    ctx: click.Context,
    before: str,
    after: str,
    target: str,
    product_name: str,
    storage_type: str | None,
    storage_driver: str | None,
    storage_anonymous: bool | None,
    output_format: str,
) -> None:
    """compare two snapshots of a product

    BEFORE and AFTER are record hashes or latest. lists the documents added,
    removed and changed, then the parsed differences of each changed node:
    attributes, dimension names, fill value, storage keys and extensions. exits
    0 whether or not the snapshots differ, and 1 when one does not exist.
    """
    manager, product = open_manager(
        ctx,
        target=target,
        product_name=product_name,
        storage_type=storage_type,
        storage_driver=storage_driver,
        storage_anonymous=storage_anonymous,
        writable=False,
    )
    try:
        result = manager.diff_snapshots(product, before, after)
    except FirecubeError as exc:
        raise click.ClickException(render(exc)) from exc

    if output_format == "json":
        echo_json(diff_payload(result))
        return
    echo_diff(result)
