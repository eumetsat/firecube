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

"""Archive CLI commands for Tensogram (.tgm) archival.

Archive subcommands distinguish two URI categories:

* **Source/target Zarr products** are resolved via
  :class:`firecube.core.product.resolver.ProductResolver` and feed a
  :class:`firecube.core.storage.session.StorageSession` so that driver,
  endpoint, and credential resolution happen at the CLI boundary.
* **`.tgm` artifact files** are *not* products. They are external archive
  files and use raw :class:`firecube.core.storage.uri.StorageUri` only —
  product resolvers MUST NOT be used for them. ``info``/``validate``/``list``
  operate on artifacts only and never construct a session.
"""

# pyright: reportMissingImports=false, reportCallIssue=false

from __future__ import annotations

import os
import sys
import tempfile
import uuid
from pathlib import Path
from typing import Any, cast

import click

from firecube.cli._ctx import get_config, get_storage_config
from firecube.cli._product import resolve_product_identity
from firecube.cli._shared_options import (
    archive_uri_option,
    dry_run_flag,
    target_uri_option,
    yes_flag,
)
from firecube.cli._typed_options import display_format_option
from firecube.cli._uri_policy import (
    apply_smart_default,
    parse_product_uri,
)
from firecube.core import observability
from firecube.core.config import get_archive_defaults
from firecube.core.controlplane.types import MAINTENANCE_OP_ARCHIVE_RESTORE, WriteDomain
from firecube.core.errors import ClaimConflictError
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.session import StorageSession
from firecube.core.storage.transfer import copy_file as transfer_copy_file
from firecube.core.storage.transfer import session_for_uri
from firecube.core.storage.uri import StorageUri
from firecube.core.uris import is_remote_target, local_path_from_target
from firecube.ingestor.runtime.recording import SpanRecorder
from firecube.ingestor.types.context import (
    IngestResult,
    RuntimeFlags,
    RuntimeIdentity,
    RuntimeIngestContext,
)
from firecube.ingestor.types.result_metrics import OutputPaths


def _require_overwrite_confirmation(*, overwrite: bool, yes_i_really_mean_it: bool) -> None:
    """Enforce --yes-i-really-mean-it (or interactive confirm) for --overwrite."""
    if not overwrite or yes_i_really_mean_it:
        return
    if not sys.stdin.isatty():
        raise click.UsageError(
            "OverwriteWithoutConfirmation: --overwrite requires --yes-i-really-mean-it in non-TTY."
        )
    click.confirm("Overwrite existing data?", abort=True)


def _temporary_tgm_path() -> str:
    fd, path = tempfile.mkstemp(suffix=".tgm")
    os.close(fd)
    os.unlink(path)
    return path


def _maintenance_claim_message(*, product: str, operation: str, detail: str) -> str:
    return (
        f"Cannot run {operation} for product {product}: {detail}. "
        "If a prior writer is stuck, resolve it with `firecube chunks runs abandon`."
    )


def _acquire_maintenance_claim(
    *, manager, product: str, operation: str, owner_id: str
) -> WriteDomain:
    active_claims = manager.list_claims(product=product)
    if active_claims:
        details = ", ".join(f"{claim.domain} (owner={claim.owner_id})" for claim in active_claims)
        raise click.ClickException(
            _maintenance_claim_message(
                product=product,
                operation=operation,
                detail=f"active write claim(s) exist: {details}",
            )
        )

    domain = WriteDomain(product=product, category="maintenance", name=operation)
    try:
        manager.acquire_claim(product=product, domain=domain, owner_id=owner_id)
    except ClaimConflictError as exc:
        raise click.ClickException(
            _maintenance_claim_message(
                product=product,
                operation=operation,
                detail=str(exc) or "write claim acquisition failed",
            )
        ) from exc
    return domain


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.pass_context
def archive(ctx: click.Context) -> None:
    """create and manage Tensogram archives

    Archives compress all array groups from a Zarr store into a single portable
    file using configurable codecs. Use archive create to produce an archive
    and archive restore to extract it back to Zarr.
    """
    observability.init_observability("firecube-archive")
    ctx.ensure_object(dict)


@archive.command(
    "create",
    epilog="""\b
Examples:
  # archive a local Zarr store with default compression
  firecube archive create \\
    --source file:///path/to/product.zarr \\
    --archive file:///tmp/archive.tgm
  # archive a specific time range
  firecube archive create \\
    --source file:///path/to/product.zarr \\
    --archive file:///tmp/archive.tgm \\
    --start-date 2024-01-01 \\
    --end-date 2024-02-01
  # archive a single group with explicit codec
  firecube archive create \\
    --source file:///path/to/product.zarr \\
    --archive file:///tmp/archive.tgm \\
    --group <group> \\
    --compression zstd
See also: firecube archive restore, firecube archive info, firecube archive validate
""",
)
@click.option(
    "-s", "--source", required=True, help="Zarr store URI (file:///abs/path or s3://bucket/key)"
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
    source: str,
    archive: str,
    storage_type: str | None,
    storage_driver: str | None,
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
    """convert a Zarr store to a .tgm archive

    reads all selected array groups, encodes them with the chosen codec
    (default: zstd), and writes a single portable .tgm file. use --start-date
    and --end-date to archive a specific time window, or --group to select one
    product group.
    """
    from firecube.core.tensogram.converter import zarr_to_tgm

    parsed_source = parse_product_uri(source)
    storage_type = apply_smart_default(parsed_source, storage_type)
    if is_remote_target(archive):
        raise click.ClickException("Remote .tgm artifacts not yet supported")
    archive_abs = str(local_path_from_target(archive))

    storage_config = get_storage_config(
        ctx,
        overrides={"storage_type": storage_type, "storage_driver": storage_driver},
        cache=False,
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
        click.echo(f"[dry-run] Would archive: {source} -> {archive_abs}")
        if group:
            click.echo(f"  Group: {group}")
        if start_date or end_date:
            click.echo(f"  Time range: {start_date or '*'} -> {end_date or '*'}")
        click.echo(f"  Compression: {compression or 'zstd'}")
        return

    variable_list = [v.strip() for v in variables.split(",")] if variables else None

    driver_config = StorageDriverConfig.from_storage_config(storage_config)
    source_identity = resolve_product_identity(
        source,
        format="zarr",
        product_name=source.rstrip("/").rsplit("/", 1)[-1],
        option_name="--source",
    )
    session = StorageSession(
        StorageBinding(
            identity=source_identity,
            driver=driver_config,
        )
    )

    archive_uri = StorageUri.from_local_path(archive_abs)
    create_target = archive_abs
    temp_archive_path: str | None = None

    if archive_uri.is_remote():
        if session.exists(archive_uri) and not overwrite:
            raise click.ClickException(
                f"Archive file already exists: {archive}. Use overwrite=True to replace it."
            )
        temp_archive_path = _temporary_tgm_path()
        create_target = temp_archive_path

    try:
        result = zarr_to_tgm(
            source_identity.product_uri.to_str(),
            create_target,
            group=group,
            variables=variable_list,
            start_date=start_date,
            end_date=end_date,
            compression=compression,
            overwrite=overwrite,
            session=session,
            allow_nan=allow_nan,
            allow_inf=allow_inf,
        )
        if temp_archive_path is not None:
            if session.exists(archive_uri) and overwrite:
                session.delete(archive_uri)
            # Cross-endpoint: product session is bound to source zarr, not archive_uri.
            target_session = (
                session_for_uri(archive_uri, session.driver) if archive_uri.is_remote() else None
            )
            transfer_copy_file(
                StorageUri.from_local_path(temp_archive_path),
                archive_uri,
                target_session=target_session,
            )
            result["target"] = archive_uri.to_str()
    except (FileExistsError, ValueError, ImportError) as exc:
        raise click.ClickException(str(exc)) from exc
    finally:
        if temp_archive_path is not None and os.path.exists(temp_archive_path):
            os.unlink(temp_archive_path)

    click.echo(f"Archive created: {result['target']}")
    if result.get("groups"):
        click.echo(f"  Groups: {', '.join(result['groups'])}")
    click.echo(f"  Variables: {', '.join(result['variables'])}")
    if result.get("time_range"):
        tr = result["time_range"]
        click.echo(f"  Time range: {tr.get('start')} → {tr.get('end')} ({tr.get('n')} steps)")
    elif any(result.get("time_ranges", {}).values()):
        click.echo(f"  Time ranges: {len(result['time_ranges'])} groups")
    size_mb = result["file_size_bytes"] / (1024 * 1024)
    click.echo(f"  Size: {size_mb:.2f} MB | Codec: {result['compression']}")
    if result.get("skipped"):
        click.echo(f"  Skipped: {', '.join(result['skipped'])} (unsupported dtype)", err=True)


@archive.command(
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
    overwrite: bool,
    dry_run: bool,
    yes_i_really_mean_it: bool,
) -> None:
    """restore a .tgm archive to a Zarr store

    decodes all encoded array groups from the .tgm file and writes them back
    to the target Zarr store. archive groups restore to their original paths.
    """
    parsed_target = parse_product_uri(target)
    storage_type = apply_smart_default(parsed_target, storage_type)
    if is_remote_target(archive):
        raise click.ClickException("Remote .tgm artifacts not yet supported")
    archive_abs = str(local_path_from_target(archive))

    _require_overwrite_confirmation(overwrite=overwrite, yes_i_really_mean_it=yes_i_really_mean_it)

    if dry_run:
        click.echo(f"[dry-run] Would restore: {archive_abs} -> {target}")
        if overwrite:
            click.echo("  Overwrite: yes")
        return

    from firecube.core.tensogram.restore import tgm_to_zarr

    storage_config = get_storage_config(
        ctx,
        overrides={"storage_type": storage_type, "storage_driver": storage_driver},
        cache=False,
    )

    driver_config = StorageDriverConfig.from_storage_config(storage_config)
    restore_identity = resolve_product_identity(
        target,
        format="zarr",
        product_name=target.rstrip("/").rsplit("/", 1)[-1],
        option_name="--target",
    )
    session = StorageSession(
        StorageBinding(
            identity=restore_identity,
            driver=driver_config,
        )
    )

    # W3.4: Archive restore is recorded as a first-class WAL run so the audit
    # trail (`firecube chunks runs list`) shows when a product was restored
    # from an archive, what the source archive was, and whether the restore
    # succeeded or failed.  We construct the SpanRecorder lazily per phase
    # because tgm_to_zarr's `_prepare_target` may rmtree the entire target
    # directory (including any `.firecube/` we created), which would
    # invalidate a cached WAL writer.
    restore_run_id = f"archive-restore-{uuid.uuid4()}"
    product_name = restore_identity.product_name
    target_uri_str = restore_identity.product_uri.to_str()
    slice_meta: dict[str, Any] = {
        "run_kind": "archive_restore",
        "source_archive": archive,
        "target_product": target_uri_str,
    }

    def _record_failure(error: str) -> None:
        manager = session.control_plane()
        try:
            recorder = SpanRecorder(manager)
            recorder.register_run_started(
                run_id=restore_run_id,
                product=product_name,
                output_path=target_uri_str,
                output_format="zarr",
                slice_meta=slice_meta,
            )
            manager.record_maintenance_started(
                product=product_name,
                run_id=restore_run_id,
                op=MAINTENANCE_OP_ARCHIVE_RESTORE,
                scope_meta=slice_meta,
            )
            recorder.register_run_failure(
                run_id=restore_run_id,
                product=product_name,
                output_path=target_uri_str,
                output_format="zarr",
                slice_meta=slice_meta,
                error=error,
            )
            manager.record_maintenance_failed(
                product=product_name,
                run_id=restore_run_id,
                op=MAINTENANCE_OP_ARCHIVE_RESTORE,
                scope_meta=slice_meta,
                error=error,
            )
        finally:
            manager.close()

    archive_uri = StorageUri.from_local_path(archive_abs)
    restore_source = archive_abs
    temp_archive_path: str | None = None

    if archive_uri.is_remote():
        temp_archive_path = _temporary_tgm_path()
        try:
            # Cross-endpoint: product session is bound to restore target, not archive_uri.
            source_archive_session = session_for_uri(archive_uri, session.driver)
            transfer_copy_file(
                archive_uri,
                StorageUri.from_local_path(temp_archive_path),
                source_session=source_archive_session,
            )
        except Exception as exc:
            _record_failure(str(exc))
            raise
        restore_source = temp_archive_path

    claim_manager = session.control_plane()
    claim_domain: WriteDomain | None = None
    try:
        claim_domain = _acquire_maintenance_claim(
            manager=claim_manager,
            product=product_name,
            operation="archive_restore",
            owner_id=restore_run_id,
        )
        try:
            result = tgm_to_zarr(
                restore_source,
                target_uri_str,
                overwrite=overwrite,
                session=session,
                on_group_restored=lambda restored_group: click.echo(
                    f"restored group: {restored_group or '/'}"
                ),
            )
            if not result.get("variables") and not result.get("groups"):
                raise ValueError(
                    f"Archive restore produced no output: {restore_source!r} "
                    "may be empty or not a valid tensogram archive."
                )
        finally:
            if temp_archive_path is not None and os.path.exists(temp_archive_path):
                os.unlink(temp_archive_path)
    except (FileExistsError, ImportError, ValueError) as exc:
        _record_failure(str(exc))
        raise click.ClickException(str(exc)) from exc
    except Exception as exc:
        _record_failure(str(exc))
        raise
    else:
        success_manager = session.control_plane()
        try:
            success_recorder = SpanRecorder(success_manager)
            success_recorder.register_run_started(
                run_id=restore_run_id,
                product=product_name,
                output_path=target_uri_str,
                output_format="zarr",
                slice_meta=slice_meta,
            )
            success_manager.record_maintenance_started(
                product=product_name,
                run_id=restore_run_id,
                op=MAINTENANCE_OP_ARCHIVE_RESTORE,
                scope_meta=slice_meta,
            )
            synthetic_ctx = RuntimeIngestContext(
                source=archive,
                target=target_uri_str,
                output_format="zarr",
                run_id=restore_run_id,
                identity=RuntimeIdentity(run_id=restore_run_id),
                flags=RuntimeFlags(force_reingest=False),
            )
            synthetic_result = IngestResult(
                outputs=OutputPaths(primary=target_uri_str),
                output_format="zarr",
                metrics={},
            )
            success_recorder.register_run(
                ctx=synthetic_ctx,
                result=synthetic_result,
                run_id=restore_run_id,
                product=product_name,
                slice_meta=slice_meta,
            )
            success_manager.record_maintenance_completed(
                product=product_name,
                run_id=restore_run_id,
                op=MAINTENANCE_OP_ARCHIVE_RESTORE,
                scope_meta=slice_meta,
            )
        finally:
            success_manager.close()
    finally:
        try:
            if claim_domain is not None:
                claim_manager.clear_claim(
                    product=product_name,
                    domain_id=claim_domain.identifier,
                    force=True,
                )
        finally:
            claim_manager.close()

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


@archive.command(
    "info",
    epilog="""\b
Examples:
  # show metadata for an archive
  firecube archive info --archive file:///tmp/archive.tgm

\b
  # output metadata as JSON
  firecube archive info --archive file:///tmp/archive.tgm -f json

See also: firecube archive list, firecube archive validate
""",
)
@archive_uri_option(tier="inspect")
@display_format_option(default="table")
@click.pass_context
def info(ctx: click.Context, archive: str, output_format: str) -> None:
    """show metadata for a .tgm archive

    reads the archive header to display variable names, dimension sizes, codec,
    and time coverage. no data is decoded or written; suitable for quick
    inspection.
    """
    from firecube.core.tensogram._compat import require_tensogram
    from firecube.core.tensogram.schema import (
        ARCHIVE_VERSION,
        KEY_ARCHIVE_VERSION,
        ROLE_CONTROLPLANE,
    )

    parsed = parse_product_uri(archive)
    if parsed.scheme == "s3":
        raise click.ClickException("Remote .tgm artifacts not yet supported")
    local_path = parsed.normalized.removeprefix("file://")
    if not Path(local_path).exists():
        raise click.ClickException(f"Archive not found: {archive}")

    require_tensogram("firecube archive info")

    import json as _json

    import tensogram as _tensogram

    tensogram = cast(Any, _tensogram)
    as_json = output_format == "json"

    with tensogram.TensogramFile.open(local_path) as f:
        count = f.message_count()
        if count == 0:
            raise click.ClickException("Archive contains no messages.")

        first_meta = f.file_decode_metadata(0)
        first_extra = first_meta.extra or {}
        firecube_meta = first_extra.get("firecube", {})
        archive_version = str(firecube_meta.get(KEY_ARCHIVE_VERSION, ""))
        if archive_version != ARCHIVE_VERSION:
            raise click.ClickException(
                f"Unsupported Firecube archive version: {archive_version or 'missing'}. "
                f"Expected {ARCHIVE_VERSION}."
            )

        groups_info: list[dict[str, Any]] = []
        has_controlplane = False

        for i in range(count):
            meta = f.file_decode_metadata(i)
            extra = meta.extra or {}
            fc = extra.get("firecube", {})
            role = fc.get("role", "data")

            if role == ROLE_CONTROLPLANE:
                has_controlplane = True
                continue

            group_name = fc.get("group", "")
            base_entries = list(meta.base) if meta.base else []
            result = f.file_decode_descriptors(i)
            descriptors = result.get("descriptors", [])
            archived_coords = set(fc.get("coordinates", []))

            group_vars: dict[str, dict] = {}
            for j, desc in enumerate(descriptors):
                base_entry = base_entries[j] if j < len(base_entries) else {}
                obj_name = str(base_entry.get("name") or f"object_{j}")
                if obj_name not in archived_coords:
                    group_vars[obj_name] = {
                        "shape": list(desc.shape) if desc.shape else [],
                        "dtype": str(desc.dtype),
                        "dims": base_entry.get("dim_names", []),
                        "zarr_chunks": base_entry.get("zarr_chunks"),
                    }

            groups_info.append(
                {
                    "group": group_name,
                    "variables": group_vars,
                    "compression": fc.get("compression", ""),
                    "source_uri": fc.get("source_uri", ""),
                    "archived_at": fc.get("archived_at", ""),
                }
            )

        info_dict: dict[str, Any] = {
            "path": local_path,
            "format": archive_version,
            "size_bytes": os.path.getsize(local_path),
            "groups": [g["group"] for g in groups_info],
            "has_controlplane": has_controlplane,
            "group_details": groups_info,
        }

        if as_json:
            click.echo(_json.dumps(info_dict, indent=2))
        else:
            click.echo(f"Path:           {local_path}")
            click.echo(f"Format:         {archive_version} (multi-group)")
            size_mb = os.path.getsize(local_path) / (1024 * 1024)
            click.echo(f"Size:           {size_mb:.2f} MB")
            click.echo(
                "Groups:         " + (", ".join(g["group"] for g in groups_info) or "(none)")
            )
            click.echo("Control-plane:  " + ("present" if has_controlplane else "not present"))
            for g in groups_info:
                click.echo(f"\nGroup: {g['group'] or '(root)'}")
                click.echo(f"  Source:    {g['source_uri']}")
                click.echo(f"  Archived:  {g['archived_at']}")
                click.echo(f"  Codec:     {g['compression']}")
                click.echo("  Variables:")
                for vname, vmeta in g["variables"].items():
                    chunks_str = (
                        f" chunks={vmeta['zarr_chunks']}" if vmeta.get("zarr_chunks") else ""
                    )
                    click.echo(
                        f"    {vname}: shape={vmeta['shape']} dtype={vmeta['dtype']}{chunks_str}"
                    )


@archive.command(
    "validate",
    epilog="""\b
Examples:
  # validate an archive and check exit code
  firecube archive validate --archive file:///tmp/archive.tgm && echo "ok"

\b
  # quick structure-only validation
  firecube archive validate --archive file:///tmp/archive.tgm --quick

See also: firecube archive info, firecube archive create
""",
)
@archive_uri_option(tier="inspect")
@click.option(
    "--quick", is_flag=True, default=False, help="structure-only check (no hash verification)"
)
@click.pass_context
def validate(ctx: click.Context, archive: str, quick: bool) -> None:
    """check integrity of a .tgm archive

    reads the archive header and validates internal structure without fully
    decoding data. exits with code 0 if the archive is valid, code 1 if it is
    corrupted or incomplete.
    """
    from firecube.core.tensogram._compat import require_tensogram

    parsed = parse_product_uri(archive)
    if parsed.scheme == "s3":
        raise click.ClickException("Remote .tgm artifacts not yet supported")
    local_path = parsed.normalized.removeprefix("file://")
    if not Path(local_path).exists():
        raise click.ClickException(f"Archive not found: {archive}")

    require_tensogram("firecube archive validate")

    import tensogram as _tensogram

    tensogram = cast(Any, _tensogram)

    level = "quick" if quick else "default"
    report = tensogram.validate_file(local_path, level=level)

    file_issues = report.get("file_issues", [])
    messages = report.get("messages", [])

    hard_issues: list[tuple[str, dict[str, Any]]] = [("file", i) for i in file_issues]
    hard_issues.extend(
        (f"msg {i}", issue)
        for i, msg_report in enumerate(messages)
        for issue in msg_report.get("issues", [])
    )

    if not hard_issues:
        click.echo(f"VALID: {local_path} ({len(messages)} message(s), no issues)")
    else:
        click.echo(f"INVALID: {local_path} ({len(hard_issues)} issue(s))")
        for scope, issue in hard_issues:
            if scope == "file":
                click.echo(
                    f"  [file] offset={issue.get('byte_offset')}: {issue.get('description')}"
                )
            else:
                click.echo(f"  [{scope}] {issue}")
        raise SystemExit(1)


@archive.command(
    "list",
    epilog="""\b
Examples:
  # list archive contents
  firecube archive list --archive file:///tmp/archive.tgm

See also: firecube archive info, firecube archive restore
""",
)
@archive_uri_option(tier="inspect")
@click.pass_context
def list_cmd(ctx: click.Context, archive: str) -> None:
    """list contents of a .tgm archive

    displays per-variable group entries including name, dimensions, and time
    range. no data is decoded; useful for reviewing archive contents before
    restoring.
    """
    from firecube.core.tensogram._compat import require_tensogram
    from firecube.core.tensogram.schema import ROLE_CONTROLPLANE

    parsed = parse_product_uri(archive)
    if parsed.scheme == "s3":
        raise click.ClickException("Remote .tgm artifacts not yet supported")
    local_path = parsed.normalized.removeprefix("file://")
    if not Path(local_path).exists():
        raise click.ClickException(f"Archive not found: {archive}")

    require_tensogram("firecube archive list")

    import tensogram as _tensogram

    tensogram = cast(Any, _tensogram)

    with tensogram.TensogramFile.open(local_path) as f:
        count = f.message_count()
        click.echo(f"Messages: {count}")
        for i in range(count):
            meta = f.file_decode_metadata(i)
            extra = meta.extra or {}
            fc = extra.get("firecube", {})
            role = fc.get("role", "data")
            base_entries = list(meta.base) if meta.base else []

            if role == ROLE_CONTROLPLANE:
                click.echo("\n[control-plane]")
                click.echo(f"  Product: {fc.get('product', '')}")
                continue

            group_name = fc.get("group") or f"message {i}"
            click.echo(f"\nGroup: {group_name}")
            if fc:
                click.echo(f"  Source:  {fc.get('source_uri', '')}")
                click.echo(f"  Codec:   {fc.get('compression', '')}")

            result = f.file_decode_descriptors(i)
            descriptors = result.get("descriptors", [])
            click.echo(f"  Objects: {len(descriptors)}")
            for j, desc in enumerate(descriptors):
                base_entry = base_entries[j] if j < len(base_entries) else {}
                shape = list(desc.shape) if desc.shape else []
                click.echo(
                    f"    [{j}] {base_entry.get('name') or desc.params.get('name', desc.obj_type)}:"
                    f" shape={shape} dtype={desc.dtype}"
                    f" compression={desc.compression}"
                )
