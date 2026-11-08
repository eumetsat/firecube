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

"""``firecube archive info``, ``validate`` and ``list``."""

# pyright: reportMissingImports=false, reportCallIssue=false

from __future__ import annotations

import json
import zipfile
from typing import Any

import click

from firecube.cli._errors import exit_with_error
from firecube.cli._exit_codes import ExitCode
from firecube.cli._shared_options import (
    archive_uri_option,
    format_option,
)
from firecube.cli.archive._common import _is_zip_archive, _local_archive


def _echo_archive_info(info_dict: dict[str, Any], *, as_json: bool, format_label: str) -> None:
    """Print the metadata of a .tgm or .zip archive as JSON or as a table.

    Fields only one format provides are printed when present: entry count and
    per-variable codec for zip, source, archive time and group codec for .tgm.
    Groups without variables and without source metadata are skipped.
    """
    if as_json:
        click.echo(json.dumps(info_dict, indent=2))
        return
    click.echo(f"Path:           {info_dict['path']}")
    click.echo(f"Format:         {format_label}")
    click.echo(f"Size:           {info_dict['size_bytes'] / (1024 * 1024):.2f} MB")
    if "entries" in info_dict:
        click.echo(f"Entries:        {info_dict['entries']}")
    marker = info_dict.get("marker")
    if marker is not None:
        deleted_keys = marker.get("deleted_keys")
        click.echo(f"Mode:           {marker.get('mode', '-')}")
        click.echo(f"Version:        {marker.get('version', '-')}")
        click.echo(f"Previous release: {marker.get('previous_release') or '-'}")
        click.echo(f"Previous backup URI: {marker.get('previous_backup_uri') or '-'}")
        click.echo(
            f"Deleted keys:   {len(deleted_keys) if isinstance(deleted_keys, list) else '-'}"
        )
    click.echo(
        "Groups:         " + (", ".join(g or "(root)" for g in info_dict["groups"]) or "(none)")
    )
    click.echo("Control-plane:  " + ("present" if info_dict["has_controlplane"] else "not present"))
    for g in info_dict["group_details"]:
        if not g["variables"] and "source_uri" not in g:
            continue
        click.echo(f"\nGroup: {g['group'] or '(root)'}")
        if "source_uri" in g:
            click.echo(f"  Source:    {g['source_uri']}")
            click.echo(f"  Archived:  {g['archived_at']}")
            click.echo(f"  Codec:     {g['compression']}")
        click.echo("  Variables:")
        for vname, vmeta in g["variables"].items():
            chunks_str = f" chunks={vmeta['zarr_chunks']}" if vmeta.get("zarr_chunks") else ""
            codec_str = (
                f" codec={','.join(vmeta['compressors']) or 'none'}"
                if "compressors" in vmeta
                else ""
            )
            click.echo(
                f"    {vname}: shape={vmeta['shape']} dtype={vmeta['dtype']}{chunks_str}{codec_str}"
            )


def _echo_validation_json(
    path: str, issues: list[str], *, message_count: int | None = None
) -> None:
    """Print the validation verdict of one archive as JSON."""
    payload: dict[str, Any] = {"path": path, "valid": not issues, "issues": issues}
    if message_count is not None:
        payload["message_count"] = message_count
    click.echo(json.dumps(payload, indent=2))


def _tgm_listing_payload(listing: Any) -> dict[str, Any]:
    """Return the messages of an open .tgm listing as a JSON-ready mapping."""
    messages: list[dict[str, Any]] = []
    for message in listing.messages():
        entry: dict[str, Any] = {"index": message.index, "is_controlplane": message.is_controlplane}
        if message.is_controlplane:
            entry["product"] = message.product
        else:
            entry["group"] = message.group_name
            if message.has_firecube_metadata:
                entry["source_uri"] = message.source_uri
                entry["compression"] = message.compression
            entry["objects"] = [
                {
                    "index": obj.index,
                    "name": obj.name,
                    "shape": obj.shape,
                    "dtype": obj.dtype,
                    "compression": obj.compression,
                }
                for obj in message.objects()
            ]
        messages.append(entry)
    return {"format": "tgm", "message_count": listing.message_count, "messages": messages}


@click.command(
    "info",
    epilog="""\b
Examples:
  # show metadata for an archive
  firecube archive info --archive file:///tmp/archive.tgm

\b
  # output metadata as JSON
  firecube archive info --archive file:///tmp/archive.tgm -f json

\b
  # show metadata for a zip archive
  firecube archive info --archive file:///tmp/archive.zip

See also: firecube archive list, firecube archive validate
""",
)
@archive_uri_option(tier="inspect")
@format_option(("table", "json"))
@click.pass_context
def info(ctx: click.Context, archive: str, output_format: str) -> None:
    """show metadata for a .tgm or .zip archive

    reads the archive header to display variable names, dimension sizes, codec,
    and time coverage. no data is decoded or written; suitable for quick
    inspection.
    """
    local_path = _local_archive(archive)
    if _is_zip_archive(archive):
        from firecube.core.archive.zip.archive import zip_info

        try:
            info_dict = zip_info(local_path)
        except (ValueError, zipfile.BadZipFile) as exc:
            exit_with_error(str(exc), ExitCode.CHECK_FAILED)
        _echo_archive_info(
            info_dict, as_json=output_format == "json", format_label=info_dict["format"]
        )
        return

    from firecube.core.archive.tensogram.inspection import tgm_info

    info_dict = tgm_info(local_path, operation="firecube archive info")
    _echo_archive_info(
        info_dict,
        as_json=output_format == "json",
        format_label=f"{info_dict['format']} (multi-group)",
    )


@click.command(
    "validate",
    epilog="""\b
Examples:
  # validate an archive and check exit code
  firecube archive validate --archive file:///tmp/archive.tgm && echo "ok"

\b
  # quick structure-only validation
  firecube archive validate --archive file:///tmp/archive.tgm --quick

\b
  # validate a zip archive
  firecube archive validate --archive file:///tmp/archive.zip

See also: firecube archive info, firecube archive create
""",
)
@archive_uri_option(tier="inspect")
@click.option(
    "--quick", is_flag=True, default=False, help="structure-only check (no hash verification)"
)
@format_option(("table", "json"))
@click.pass_context
def validate(ctx: click.Context, archive: str, quick: bool, output_format: str) -> None:
    """check integrity of a .tgm or .zip archive

    reads the archive header and validates internal structure without fully
    decoding data. for a .zip archive, --quick skips the CRC check of every
    entry. exits with code 0 if the archive is valid, code 1 if it is
    corrupted or incomplete.
    """
    as_json = output_format == "json"
    local_path = _local_archive(archive)
    if _is_zip_archive(archive):
        from firecube.core.archive.zip.archive import validate_zip

        zip_issues = validate_zip(local_path, quick=quick)
        if as_json:
            _echo_validation_json(local_path, [str(issue) for issue in zip_issues])
        elif not zip_issues:
            click.echo(f"VALID: {local_path} (no issues)")
        else:
            click.echo(f"INVALID: {local_path} ({len(zip_issues)} issue(s))")
            for zip_issue in zip_issues:
                click.echo(f"  {zip_issue}")
        if zip_issues:
            raise click.exceptions.Exit(ExitCode.CHECK_FAILED)
        return

    from firecube.core.archive.tensogram.inspection import validate_tgm

    validation = validate_tgm(local_path, quick=quick, operation="firecube archive validate")
    hard_issues = validation.issues
    lines = [
        f"[file] offset={issue.get('byte_offset')}: {issue.get('description')}"
        if scope == "file"
        else f"[{scope}] {issue}"
        for scope, issue in hard_issues
    ]

    if as_json:
        _echo_validation_json(local_path, lines, message_count=validation.message_count)
    elif not hard_issues:
        click.echo(f"VALID: {local_path} ({validation.message_count} message(s), no issues)")
    else:
        click.echo(f"INVALID: {local_path} ({len(hard_issues)} issue(s))")
        for line in lines:
            click.echo(f"  {line}")
    if hard_issues:
        raise click.exceptions.Exit(ExitCode.CHECK_FAILED)


@click.command(
    "list",
    epilog="""\b
Examples:
  # list archive contents
  firecube archive list --archive file:///tmp/archive.tgm

\b
  # list zip archive contents
  firecube archive list --archive file:///tmp/archive.zip

See also: firecube archive info, firecube archive restore
""",
)
@archive_uri_option(tier="inspect")
@format_option(("table", "json"))
@click.pass_context
def list_cmd(ctx: click.Context, archive: str, output_format: str) -> None:
    """list contents of a .tgm or .zip archive

    displays per-variable group entries including name, dimensions, and time
    range. a .zip archive is shown as one xarray dataset per group. no data is
    decoded; useful for reviewing archive contents before restoring.
    with -f json, a .zip archive lists every variable of each group.
    """
    as_json = output_format == "json"
    local_path = _local_archive(archive)
    if _is_zip_archive(archive):
        from firecube.core.archive.zip.archive import list_zip

        listing = list_zip(local_path)
        if as_json:
            from firecube.core.archive.zip.archive import zip_info

            details = {g["group"]: g["variables"] for g in zip_info(local_path)["group_details"]}
            groups = [
                {
                    "group": name,
                    "variables": [{"name": v, **meta} for v, meta in details[name].items()],
                }
                for name, _dataset in listing
            ]
            click.echo(json.dumps({"format": "zip", "groups": groups}, indent=2))
            return
        click.echo(f"Groups: {len(listing)}")
        for group_name, dataset in listing:
            click.echo(f"\nGroup: {group_name or '(root)'}")
            click.echo(dataset)
        return

    from firecube.core.archive.tensogram.inspection import open_tgm_listing

    with open_tgm_listing(local_path, operation="firecube archive list") as listing:
        if as_json:
            click.echo(json.dumps(_tgm_listing_payload(listing), indent=2, default=str))
            return
        click.echo(f"Messages: {listing.message_count}")
        for message in listing.messages():
            if message.is_controlplane:
                click.echo("\n[control-plane]")
                click.echo(f"  Product: {message.product}")
                continue

            click.echo(f"\nGroup: {message.group_name or '(root)'}")
            if message.has_firecube_metadata:
                click.echo(f"  Source:  {message.source_uri}")
                click.echo(f"  Codec:   {message.compression}")

            objects = message.objects()
            click.echo(f"  Objects: {len(objects)}")
            for obj in objects:
                click.echo(
                    f"    [{obj.index}] {obj.name}:"
                    f" shape={obj.shape} dtype={obj.dtype}"
                    f" compression={obj.compression}"
                )
