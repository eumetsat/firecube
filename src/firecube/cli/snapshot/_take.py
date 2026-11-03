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

import click

from firecube.cli._errors import render
from firecube.cli.snapshot._common import open_manager, snapshot_options
from firecube.cli.snapshot._render import echo_json, echo_record, record_payload
from firecube.core.errors import FirecubeError


@click.command(
    "take",
    epilog="""\b
Examples:
  # take a snapshot of the metadata of a local product
  firecube snapshot take --target file:///data/products/MY_PRODUCT.zarr --product-name MY_PRODUCT

\b
  # print the new snapshot record as JSON
  firecube snapshot take --target file:///data/products/MY_PRODUCT.zarr --product-name MY_PRODUCT -f json

See also: firecube snapshot list, firecube snapshot diff
""",
)
@snapshot_options(tier="write")
@click.pass_context
def take(
    ctx: click.Context,
    target: str,
    product_name: str,
    storage_type: str | None,
    storage_driver: str | None,
    storage_anonymous: bool | None,
    output_format: str,
) -> None:
    """capture the metadata documents of a product

    stores the exact bytes of every Zarr metadata document under
    .firecube/metadata/ and prints the new snapshot record. no chunk is read.
    the snapshot is an observation: a writer running at the same time can make
    it mix states. it takes no maintenance claim.
    """
    manager, product = open_manager(
        ctx,
        target=target,
        product_name=product_name,
        storage_type=storage_type,
        storage_driver=storage_driver,
        storage_anonymous=storage_anonymous,
        writable=True,
    )
    try:
        record = manager.take_snapshot(
            product, run_id=None, trigger="cli", consistency="observation"
        )
    except FirecubeError as exc:
        raise click.ClickException(render(exc)) from exc

    if output_format == "json":
        echo_json(record_payload(record))
        return
    echo_record(record)
