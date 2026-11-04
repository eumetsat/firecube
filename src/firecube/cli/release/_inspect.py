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

"""``firecube release list``, ``show`` and ``verify``."""

from __future__ import annotations

import click

from firecube.cli._errors import render
from firecube.cli.release._common import echo_json, open_session, release_options
from firecube.cli.release._render import (
    echo_listing,
    echo_record,
    echo_verification,
    listing_payload,
    release_payload,
    verification_payload,
)
from firecube.core.errors import FirecubeError
from firecube.ingestor.runtime.release import (
    list_releases_for_product,
    read_release_for_product,
    verify_release_for_product,
)


@click.command(
    "list",
    epilog="""\b
Examples:
  # list the releases of a product, oldest first; the current one is marked *
  firecube release list --target file:///data/products/MY_PRODUCT.zarr --product-name MY_PRODUCT

\b
  # list them as JSON
  firecube release list --target file:///data/products/MY_PRODUCT.zarr --product-name MY_PRODUCT -f json

See also: firecube release show, firecube release create
""",
)
@release_options(tier="inspect")
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
    """list the releases of a product

    prints one line per release, oldest first: version, time, snapshot and
    producer status. the current release is marked with *. a product with no
    release prints a notice, or an empty list in json, and exits 0.
    """
    session = open_session(
        ctx,
        command="release list",
        target=target,
        product_name=product_name,
        storage_type=storage_type,
        storage_driver=storage_driver,
        storage_anonymous=storage_anonymous,
        writable=False,
    )
    try:
        listing = list_releases_for_product(session=session)
    except FirecubeError as exc:
        raise click.ClickException(render(exc)) from exc
    if output_format == "json":
        echo_json(listing_payload(listing))
        return
    echo_listing(listing)


@click.command(
    "show",
    epilog="""\b
Examples:
  # show the current release
  firecube release show --target file:///data/products/MY_PRODUCT.zarr --product-name MY_PRODUCT

\b
  # show release 1.0.0 as JSON
  firecube release show 1.0.0 --target file:///data/products/MY_PRODUCT.zarr --product-name MY_PRODUCT -f json

See also: firecube release list, firecube release verify
""",
)
@click.argument("version", metavar="[VERSION]", required=False)
@release_options(tier="inspect")
@click.pass_context
def show(
    ctx: click.Context,
    version: str | None,
    target: str,
    product_name: str,
    storage_type: str | None,
    storage_driver: str | None,
    storage_anonymous: bool | None,
    output_format: str,
) -> None:
    """show one release of a product

    VERSION names the release; without it the current release is shown. prints
    the version, time, previous release, snapshot, creating run, producer, note
    and the runs recorded since the previous release. exits 1 when no such
    release exists.
    """
    session = open_session(
        ctx,
        command="release show",
        target=target,
        product_name=product_name,
        storage_type=storage_type,
        storage_driver=storage_driver,
        storage_anonymous=storage_anonymous,
        writable=False,
    )
    try:
        record = read_release_for_product(session=session, version=version)
    except FirecubeError as exc:
        raise click.ClickException(render(exc)) from exc
    if output_format == "json":
        echo_json(release_payload(record))
        return
    echo_record(record)


@click.command(
    "verify",
    epilog="""\b
Examples:
  # compare the metadata of a product with its current release
  firecube release verify --target file:///data/products/MY_PRODUCT.zarr --product-name MY_PRODUCT

\b
  # compare it with release 1.0.0 and print the result as JSON
  firecube release verify 1.0.0 --target file:///data/products/MY_PRODUCT.zarr --product-name MY_PRODUCT -f json

See also: firecube release show, firecube snapshot diff
""",
)
@click.argument("version", metavar="[VERSION]", required=False)
@release_options(tier="inspect")
@click.pass_context
def verify(
    ctx: click.Context,
    version: str | None,
    target: str,
    product_name: str,
    storage_type: str | None,
    storage_driver: str | None,
    storage_anonymous: bool | None,
    output_format: str,
) -> None:
    """compare the metadata of a product with a release

    VERSION names the release; without it the current release is used. exits 0
    when every metadata document equals the release snapshot byte for byte, and
    1 on any difference, listing the changed, added and removed paths. this
    detects metadata drift only: an edit of chunk bytes is not seen. exits 1
    also when no such release exists.
    """
    session = open_session(
        ctx,
        command="release verify",
        target=target,
        product_name=product_name,
        storage_type=storage_type,
        storage_driver=storage_driver,
        storage_anonymous=storage_anonymous,
        writable=False,
    )
    try:
        verification = verify_release_for_product(session=session, version=version)
    except FirecubeError as exc:
        raise click.ClickException(render(exc)) from exc
    if output_format == "json":
        echo_json(verification_payload(verification))
    else:
        echo_verification(verification)
    if not verification.matches:
        ctx.exit(1)
