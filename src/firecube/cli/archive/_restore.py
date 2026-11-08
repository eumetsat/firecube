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

"""``firecube archive restore``."""

# pyright: reportMissingImports=false, reportCallIssue=false

from __future__ import annotations

import functools

import click

from firecube.cli._ctx import get_storage_config
from firecube.cli._errors import echo_rendered
from firecube.cli._product import resolve_product_identity
from firecube.cli._shared_options import (
    archive_uri_option,
    dry_run_flag,
    target_uri_option,
    yes_flag,
)
from firecube.cli._uri_policy import (
    apply_smart_default,
    parse_and_validate_uri,
)
from firecube.cli.archive._common import (
    _is_zip_archive,
    _local_archive,
    _require_overwrite_confirmation,
)
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.session import StorageSession


@click.command(
    "restore",
    epilog="""\b
Examples:
  # restore an archive to a local Zarr store
  firecube archive restore \\
    --archive file:///tmp/archive.tgm \\
    --target file:///path/to/restored.zarr
  # restore and overwrite an existing store
  firecube archive restore \\
    --archive file:///tmp/archive.tgm \\
    --target file:///path/to/product.zarr \\
    --overwrite
  # restore a zip archive to a local Zarr store
  firecube archive restore \\
    --archive file:///tmp/archive.zip \\
    --target file:///path/to/restored.zarr
See also: firecube archive create, firecube archive info
""",
)
@archive_uri_option(tier="write")
@target_uri_option(tier="write")
@click.option(
    "--storage-type",
    "storage_type",
    required=False,
    type=click.Choice(["local", "s3"], case_sensitive=False),
)
@click.option(
    "--storage-driver",
    "storage_driver",
    required=False,
    type=click.Choice(["fsspec", "obstore"], case_sensitive=False),
)
@click.option(
    "--storage-anonymous",
    "storage_anonymous",
    is_flag=True,
    default=None,
    help=(
        "Use anonymous access for public S3 buckets. Overrides any credentials "
        "in the environment or config file."
    ),
)
@click.option("--overwrite", is_flag=True, default=False, help="overwrite existing Zarr store")
@dry_run_flag
@yes_flag
@click.pass_context
def restore(
    ctx: click.Context,
    archive: str,
    target: str,
    storage_type: str | None,
    storage_driver: str | None,
    storage_anonymous: bool | None,
    overwrite: bool,
    dry_run: bool,
    yes_i_really_mean_it: bool,
) -> None:
    """restore a .tgm or .zip archive to a Zarr store

    decodes all encoded array groups from a .tgm archive, or copies every entry
    of a .zip archive, and writes them back to the target Zarr store. archive
    groups restore to their original paths. zip archives restore to local
    targets only.
    """
    is_zip = _is_zip_archive(archive)

    target_uri = parse_and_validate_uri(target, param_hint="--target")
    storage_type = apply_smart_default(target_uri, storage_type)
    archive_uri = parse_and_validate_uri(archive, param_hint="--archive")
    if archive_uri.is_remote():
        raise click.ClickException(
            f"Remote {'.zip' if is_zip else '.tgm'} artifacts not yet supported"
        )
    if is_zip and target_uri.is_remote():
        raise click.ClickException(
            "Restoring a .zip archive to a remote target is not yet supported"
        )
    archive_abs = _local_archive(archive) if is_zip else archive_uri.path

    _require_overwrite_confirmation(overwrite=overwrite, yes_i_really_mean_it=yes_i_really_mean_it)

    if dry_run:
        click.echo(f"[dry-run] Would restore: {archive_abs} -> {target}")
        if overwrite:
            click.echo("  Overwrite: yes")
        return

    storage_config = get_storage_config(
        ctx,
        overrides={
            "storage_type": storage_type,
            "storage_driver": storage_driver,
            "anonymous": storage_anonymous,
        },
        cache=False,
    )

    driver_config = StorageDriverConfig.from_storage_config(storage_config)
    restore_identity = resolve_product_identity(
        target_uri,
        format="zarr",
        product_name=target.rstrip("/").rsplit("/", 1)[-1],
    )
    session = StorageSession(
        StorageBinding(
            identity=restore_identity,
            driver=driver_config,
        )
    )

    from firecube.ingestor.runtime.archive import restore_archive

    restored = restore_archive(
        session=session,
        archive=archive,
        archive_path=archive_abs,
        archive_format="zip" if is_zip else "tensogram",
        target=restore_identity.product_uri.to_str(),
        overwrite=overwrite,
        report=echo_rendered,
        warn=functools.partial(echo_rendered, err=True),
    )

    result = restored.summary
    click.echo(f"Zarr restored: {result['target']}")
    if result.get("groups"):
        click.echo(f"  Groups: {', '.join(result['groups'])}")
    click.echo(f"  Variables: {', '.join(result['variables'])}")
    click.echo(f"  Coordinates: {', '.join(result['coordinates'])}")
    click.echo(
        "  Control-plane: restored"
        if result.get("controlplane_restored")
        else "  Control-plane: not present"
    )
