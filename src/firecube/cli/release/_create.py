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

"""``firecube release create``."""

from __future__ import annotations

import click

from firecube.cli._errors import render
from firecube.cli._uri_policy import parse_and_validate_uri
from firecube.cli.release._common import (
    echo_json,
    echo_progress,
    echo_warning,
    open_session,
    release_options,
)
from firecube.cli.release._render import create_payload, echo_record
from firecube.core.errors import FirecubeError
from firecube.ingestor.runtime.release import create_release_for_product


@click.command(
    "create",
    epilog="""\b
Examples:
  # name the current state of a product as release 1.0.0
  firecube release create 1.0.0 --target file:///data/products/MY_PRODUCT.zarr --product-name MY_PRODUCT

\b
  # add a note and print the new release as JSON
  firecube release create 1.1.0 --target file:///data/products/MY_PRODUCT.zarr --product-name MY_PRODUCT --note "units fixed" -f json

\b
  # retain the chunk bytes in a zip archive (full the first time, a delta after)
  firecube release create 1.2.0 --target file:///data/products/MY_PRODUCT.zarr --product-name MY_PRODUCT --archive file:///data/archives/MY_PRODUCT-1.2.0.zip

See also: firecube release list, firecube release verify, firecube snapshot list
""",
)
@click.argument("version", metavar="VERSION")
@release_options(tier="write")
@click.option(
    "--note",
    "note",
    default=None,
    help="Free text kept in the release record.",
)
@click.option(
    "--archive",
    "archive",
    default=None,
    metavar="URI",
    help=(
        "Retain the product's objects in a zip archive at this file:// URI. The first "
        "release writes a full archive and each later one a delta against the previous "
        "release's archive. Nothing may exist at the path."
    ),
)
@click.option(
    "--archive-full",
    "archive_full",
    is_flag=True,
    default=False,
    help="Write a full archive even when a delta is possible. Requires --archive.",
)
@click.option(
    "--retain-versions",
    "retain_versions",
    is_flag=True,
    default=False,
    help=(
        "Record the version id of every object instead of copying bytes. Needs a "
        "versioned s3 bucket and the fsspec driver. Exclusive with --archive."
    ),
)
@click.pass_context
def create(
    ctx: click.Context,
    version: str,
    target: str,
    product_name: str,
    storage_type: str | None,
    storage_driver: str | None,
    storage_anonymous: bool | None,
    output_format: str,
    note: str | None,
    archive: str | None,
    archive_full: bool,
    retain_versions: bool,
) -> None:
    """name the current metadata of a product as a release

    VERSION is an opaque name, kept as given, and each name is used once. the
    run holds the exclusive maintenance claim, stamps the root group, takes a
    snapshot and makes the new release current. a rerun of an interrupted
    create resumes. a version whose store changed since is refused. a store
    without a control plane is refused: use migrate adopt first. with --format
    json the progress lines go to stderr.

    --archive also retains the chunk bytes of the release in a local zip
    archive; --retain-versions retains them as the object versions of a
    versioned s3 bucket. an interrupted create that wrote an archive is
    refused on rerun until the file is removed.
    """
    if archive_full and archive is None:
        raise click.UsageError("--archive-full requires --archive.")
    if archive is not None and retain_versions:
        raise click.UsageError("--archive and --retain-versions are mutually exclusive.")
    archive_uri = None
    if archive is not None:
        archive_uri = parse_and_validate_uri(archive, param_hint="--archive")
        if archive_uri.is_remote():
            raise click.UsageError(
                "Invalid value for '--archive': a release archive is a local file:// URI."
            )
    session = open_session(
        ctx,
        command="release create",
        target=target,
        product_name=product_name,
        storage_type=storage_type,
        storage_driver=storage_driver,
        storage_anonymous=storage_anonymous,
        writable=True,
    )
    json_out = output_format == "json"
    try:
        result = create_release_for_product(
            session=session,
            version=version,
            note=note,
            report=echo_progress if json_out else click.echo,
            warn=echo_warning,
            archive=archive_uri,
            archive_full=archive_full,
            retain_versions=retain_versions,
        )
    except FirecubeError as exc:
        raise click.ClickException(render(exc)) from exc
    if json_out:
        echo_json(create_payload(result))
        return
    echo_record(result.release)
