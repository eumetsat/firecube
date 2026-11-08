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

"""``firecube archive create``."""

# pyright: reportMissingImports=false, reportCallIssue=false

from __future__ import annotations

import functools

import click

from firecube.cli._ctx import get_config, get_storage_config
from firecube.cli._errors import echo_rendered
from firecube.cli._product import session_identity
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
from firecube.cli.archive._common import _require_overwrite_confirmation
from firecube.core.config import get_archive_defaults
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.session import StorageSession


@click.command(
    "create",
    epilog="""\b
Examples:
  # archive a local Zarr store with default compression
  firecube archive create \\
    --target file:///path/to/product.zarr \\
    --archive file:///tmp/archive.tgm
  # archive a specific time range
  firecube archive create \\
    --target file:///path/to/product.zarr \\
    --archive file:///tmp/archive.tgm \\
    --start-date 2024-01-01 \\
    --end-date 2024-02-01
  # archive a single group with explicit codec
  firecube archive create \\
    --target file:///path/to/product.zarr \\
    --archive file:///tmp/archive.tgm \\
    --group <group> \\
    --compression zstd
See also: firecube archive restore, firecube archive info, firecube archive validate
""",
)
@target_uri_option(tier="write")
@click.option(
    "--archive-format",
    "archive_format",
    required=False,
    default="tensogram",
    show_default=True,
    type=click.Choice(["zip", "tensogram"], case_sensitive=False),
    help="archive file format to write",
)
@archive_uri_option(tier="write")
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
@click.option("--start-date", default=None, help="ISO 8601 start date for time range filter")
@click.option("--end-date", default=None, help="ISO 8601 end date for time range filter")
@click.option("-g", "--group", default=None, help="product group to archive (e.g. F024, NORDLIS)")
@click.option("--variables", default=None, help="comma-separated variable names to include")
@click.option(
    "--compression",
    default=None,
    type=click.Choice(["blosc2", "szip", "zstd", "lz4", "zfp", "sz3"]),
    help="compression codec for .tgm encoding (default: zstd)",
)
@click.option("--overwrite", is_flag=True, default=False, help="overwrite existing target file")
@click.option(
    "--allow-nan/--no-allow-nan",
    default=None,
    help="allow NaN values in archive (default: true)",
)
@click.option(
    "--allow-inf/--no-allow-inf",
    default=None,
    help="allow Inf values in archive (default: true)",
)
@dry_run_flag
@yes_flag
@click.pass_context
def create(
    ctx: click.Context,
    target: str,
    archive: str,
    archive_format: str,
    storage_type: str | None,
    storage_driver: str | None,
    storage_anonymous: bool | None,
    start_date: str | None,
    end_date: str | None,
    group: str | None,
    variables: str | None,
    compression: str | None,
    overwrite: bool,
    allow_nan: bool | None,
    allow_inf: bool | None,
    dry_run: bool,
    yes_i_really_mean_it: bool,
) -> None:
    """convert a Zarr store to a .zip or .tgm archive

    reads all selected array groups, encodes them with the chosen codec
    (default: zstd), and writes a single portable .zip or .tgm file. use --start-date
    and --end-date to archive a specific time window, or --group to select one
    product group.
    """
    target_uri = parse_and_validate_uri(target, param_hint="--target")
    storage_type = apply_smart_default(target_uri, storage_type)
    archive_uri = parse_and_validate_uri(archive, param_hint="--archive")
    if archive_uri.is_remote():
        raise click.ClickException("Remote artifacts not yet supported")
    archive_abs = archive_uri.path

    storage_config = get_storage_config(
        ctx,
        overrides={
            "storage_type": storage_type,
            "storage_driver": storage_driver,
            "anonymous": storage_anonymous,
        },
        cache=False,
    )
    if storage_config.anonymous:
        raise click.UsageError(
            "archive create writes a record into the product's control plane and needs "
            "write access; anonymous storage access is not supported."
        )

    cfg = get_config(ctx)
    archive_defaults = get_archive_defaults(cfg)

    if compression is None:
        compression = str(archive_defaults.get("compression", "zstd"))
    if not overwrite:
        overwrite = bool(archive_defaults.get("overwrite", False))
    if allow_nan is None:
        allow_nan = bool(archive_defaults.get("allow_nan", True))
    if allow_inf is None:
        allow_inf = bool(archive_defaults.get("allow_inf", True))

    _require_overwrite_confirmation(overwrite=overwrite, yes_i_really_mean_it=yes_i_really_mean_it)

    if dry_run:
        click.echo(f"[dry-run] Would archive: {target} -> {archive_abs}")
        if group:
            click.echo(f"  Group: {group}")
        if start_date or end_date:
            click.echo(f"  Time range: {start_date or '*'} -> {end_date or '*'}")
        click.echo(f"  Compression: {compression or 'zstd'}")
        return

    variable_list = [v.strip() for v in variables.split(",")] if variables else None

    driver_config = StorageDriverConfig.from_storage_config(storage_config)
    session = StorageSession(
        StorageBinding(
            identity=session_identity(target_uri, format="zarr", driver=driver_config),
            driver=driver_config,
        )
    )

    if archive_format == "zip" and (group or start_date or end_date or variable_list):
        raise click.ClickException(
            "Zip archive does not support --group, --start-date, --end-date, or --variables"
        )

    from firecube.ingestor.runtime.archive import create_archive

    created = create_archive(
        session=session,
        archive=archive,
        archive_path=archive_abs,
        archive_format=archive_format,
        overwrite=overwrite,
        group=group,
        variables=variable_list,
        start_date=start_date,
        end_date=end_date,
        compression=compression,
        allow_nan=allow_nan,
        allow_inf=allow_inf,
        warn=functools.partial(echo_rendered, err=True),
    )

    click.echo(f"Archive created: {created.target}")
    tgm_result = created.tgm_summary
    if tgm_result is not None:
        if tgm_result.get("groups"):
            click.echo(f"  Groups: {', '.join(g or '(root)' for g in tgm_result['groups'])}")
        click.echo(f"  Variables: {', '.join(tgm_result['variables'])}")
        if tgm_result.get("time_range"):
            tr = tgm_result["time_range"]
            click.echo(f"  Time range: {tr.get('start')} → {tr.get('end')} ({tr.get('n')} steps)")
        elif any(tgm_result.get("time_ranges", {}).values()):
            click.echo(f"  Time ranges: {len(tgm_result['time_ranges'])} groups")
        if tgm_result.get("skipped"):
            click.echo(
                f"  Skipped: {', '.join(tgm_result['skipped'])} (unsupported dtype)", err=True
            )

    size_mb = created.file_size_bytes / (1024 * 1024)
    click.echo(f"  Size: {size_mb:.2f} MB")
