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

"""``firecube snapshot take``."""

from __future__ import annotations

from typing import cast

import click

from firecube.cli._shared_options import dry_run_flag
from firecube.cli.snapshot._common import open_session, snapshot_options
from firecube.cli.snapshot._render import (
    echo_json,
    echo_plan,
    echo_record,
    plan_payload,
    record_payload,
)
from firecube.core.controlplane.metadata_snapshots import plan_snapshot
from firecube.core.controlplane.product_label import check_product_label
from firecube.core.filesystem import StorageFilesystemFull


@click.command(
    "take",
    epilog="""\b
Examples:
  # take a snapshot of the metadata of a local product
  firecube snapshot take --target file:///data/products/MY_PRODUCT.zarr

\b
  # print the new snapshot record as JSON
  firecube snapshot take --target file:///data/products/MY_PRODUCT.zarr -f json

\b
  # show what a snapshot would write, and write nothing
  firecube snapshot take --target file:///data/products/MY_PRODUCT.zarr --dry-run

See also: firecube snapshot list, firecube snapshot diff
""",
)
@snapshot_options(tier="write")
@dry_run_flag
@click.pass_context
def take(
    ctx: click.Context,
    target: str,
    storage_type: str | None,
    storage_driver: str | None,
    storage_anonymous: bool | None,
    output_format: str,
    dry_run: bool,
) -> None:
    """capture the metadata documents of a product

    stores the exact bytes of every Zarr metadata document under
    .firecube/metadata/ and prints the new snapshot record. no chunk is read.
    the snapshot is an observation: a writer running at the same time can make
    it mix states. it takes no maintenance claim. --dry-run prints the documents,
    the manifest and the parent the snapshot would record, and writes nothing.
    """
    session = open_session(
        ctx,
        target=target,
        storage_type=storage_type,
        storage_driver=storage_driver,
        storage_anonymous=storage_anonymous,
        writable=True,
    )
    manager = session.control_plane()
    ctx.call_on_close(manager.close)
    product = session.product.product_name
    if dry_run:
        fs = cast(StorageFilesystemFull, session.fs())
        plan = plan_snapshot(
            fs,
            session.product.product_uri,
            manager.enumerate_metadata_documents(product),
            records=manager.list_snapshots(product),
        )
        # A store without the stamp gets it from a real take.
        stamped = check_product_label(fs, session.product.control_root_uri, product) is not None
        if output_format == "json":
            echo_json(plan_payload(plan, label_recorded=stamped))
            return
        echo_plan(plan, label=product, label_recorded=stamped)
        return
    record = manager.take_snapshot(product, run_id=None, trigger="cli", consistency="observation")

    if output_format == "json":
        echo_json(record_payload(record))
        return
    echo_record(record)
