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

"""Performance advice helpers (read-only)."""

from __future__ import annotations

import json as _json
import sys
from collections.abc import Callable

import click
import zarr

from firecube.cli._ctx import get_storage_config
from firecube.cli._errors import wrap_user_facing_errors
from firecube.cli._product import resolve_product_identity
from firecube.cli._shared_options import (
    product_uri_option,
    storage_driver_option,
    storage_type_option,
)
from firecube.cli._uri_policy import apply_smart_default, parse_product_uri
from firecube.core import observability
from firecube.core.cf import CFReport
from firecube.core.cf.validator import validate_cf18
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.session import StorageSession

_ROOT_GROUP_ALIASES = frozenset({".", "/", ""})
_COMPLIANCE_PROFILE_VALIDATORS: dict[str, Callable[..., CFReport]] = {
    "cf-18": validate_cf18,
}
_COMPLIANCE_PROFILE_LABELS = {
    "cf-18": "CF-1.8",
}


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.pass_context
def advise(ctx: click.Context) -> None:
    """performance advice helpers

    Read-only helpers for configuring ingestion. Use advise batch-size to get
    a recommended pipeline_batch_size before running large ingestion jobs.
    """
    observability.init_observability("firecube-advise")
    ctx.ensure_object(dict)


@advise.command(
    "batch-size",
    epilog="""\b
Examples:
  # get recommended batch size for a product group
  firecube advise batch-size -p <product> -g <group>

See also: firecube ingest, firecube zarr validate
""",
)
@product_uri_option(tier="inspect")
@click.option(
    "-g",
    "--group",
    "group_path",
    required=True,
    help="group path inside the product (e.g. F024/FWI)",
)
@storage_driver_option(required=False)
@storage_type_option(required=False)
@click.pass_context
@wrap_user_facing_errors
def batch_size(
    ctx: click.Context,
    product: str,
    group_path: str,
    storage_driver: str | None,
    storage_type: str | None,
) -> None:
    """recommend optimal batch size

    Recommends an optimal pipeline_batch_size based on the Zarr store's time
    chunk shape. Reads the store metadata to compute how many time steps fit
    into memory without partial chunks. Run before a long ingestion job to
    set --option pipeline_batch_size=N.
    """
    parsed_uri = parse_product_uri(product)
    storage_type = apply_smart_default(parsed_uri, storage_type)
    storage_cfg = get_storage_config(
        ctx,
        overrides={"storage_type": storage_type, "storage_driver": storage_driver},
        cache=False,
    )
    driver_config = StorageDriverConfig.from_storage_config(storage_cfg)
    identity = resolve_product_identity(
        parsed_uri.normalized, format="zarr", product_name=parsed_uri.normalized
    )
    session = StorageSession(
        StorageBinding(
            identity=identity,
            driver=driver_config,
        )
    )

    try:
        root = session.zarr.open_group(identity.product_uri, mode="r")
        grp = root[group_path]
    except Exception as exc:
        click.echo(f"Error opening store: {exc}", err=True)
        sys.exit(1)

    if not isinstance(grp, zarr.Group):
        click.echo(f"Path '{group_path}' is not a Zarr group.", err=True)
        sys.exit(1)

    time_chunk_size = _find_time_chunk_size(grp)

    if time_chunk_size is None:
        click.echo(f"No time dimension found in group '{group_path}'. Any batch size is valid.")
        return

    if time_chunk_size == 1:
        click.echo(
            "Recommended: --option pipeline_batch_size=10  (any size works; time chunk is 1)\n"
            "Rationale: Time chunk size is 1. Any batch size is safely aligned. Recommend 10 for throughput."
        )
    else:
        click.echo(
            f"Recommended: --option pipeline_batch_size={time_chunk_size}\n"
            f"Rationale: Time chunk size is {time_chunk_size}. "
            "Aligning batch size avoids partial-chunk write overhead."
        )


def _find_time_chunk_size(grp: zarr.Group) -> int | None:
    """Return the time-axis chunk size from the first data array, or *None*."""
    for _name, arr in grp.arrays():
        chunks = arr.chunks
        if not isinstance(chunks, tuple) or len(chunks) == 0:
            continue
        # In this codebase the time dimension is always axis 0 for
        # multi-dimensional data arrays (shape length > 1).
        if len(chunks) > 1:
            return chunks[0]
    return None


@advise.command(
    "compliance",
    epilog="""\b
Examples:
  # run Tier-1 CF-1.8 checks on a local cube root
  firecube advise compliance --profile cf-18 -p /path/to/cube.zarr -g .
  # check a nested group, emit JSON for downstream tooling
  firecube advise compliance --profile cf-18 -p s3://bucket/x.zarr -g F024/FWI --storage-type s3 --storage-driver fsspec --format json
  # fail (exit 2) on any warning or info finding
  firecube advise compliance --profile cf-18 -p ./cube.zarr -g . --strict

See also: firecube zarr validate
""",
)
@click.option(
    "--profile",
    required=True,
    type=click.Choice(sorted(_COMPLIANCE_PROFILE_VALIDATORS)),
    help="compliance profile to run",
)
@product_uri_option(tier="inspect")
@click.option(
    "-g",
    "--group",
    "group_path",
    required=True,
    help="group path inside the cube ('.' or '/' for root, otherwise e.g. F024/FWI)",
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["text", "json"]),
    default="text",
    show_default=True,
    help="output format for the report",
)
@click.option(
    "--strict",
    is_flag=True,
    default=False,
    help="treat any warning or info finding as a non-zero exit (code 2)",
)
@storage_driver_option(required=False)
@storage_type_option(required=False)
@click.pass_context
@wrap_user_facing_errors
def compliance(
    ctx: click.Context,
    profile: str,
    product: str,
    group_path: str,
    output_format: str,
    strict: bool,
    storage_driver: str | None,
    storage_type: str | None,
) -> None:
    """run structural compliance checks

    Opens a Zarr cube and runs the selected compliance profile. Findings are
    emitted as human-readable text (default) or as JSON. Exit codes: 0 on clean
    (or warnings/info without --strict), 1 if any error finding is reported,
    2 if warnings/info appear under --strict.
    """
    parsed_uri = parse_product_uri(product)
    storage_type = apply_smart_default(parsed_uri, storage_type)
    storage_cfg = get_storage_config(
        ctx,
        overrides={"storage_type": storage_type, "storage_driver": storage_driver},
        cache=False,
    )
    driver_config = StorageDriverConfig.from_storage_config(storage_cfg)

    identity = resolve_product_identity(
        parsed_uri.normalized, format="zarr", product_name=parsed_uri.normalized
    )
    session = StorageSession(StorageBinding(identity=identity, driver=driver_config))

    xr_group = "" if group_path in _ROOT_GROUP_ALIASES else group_path

    try:
        root = session.zarr.open_group(identity.product_uri, mode="r")
        if xr_group:
            target = root[xr_group]
            if not isinstance(target, zarr.Group):
                raise click.ClickException(
                    f"Path '{group_path}' is a Zarr array, not a group. "
                    "Specify a group path (for example '/' or 'F024/FWI')."
                )
    except KeyError as exc:
        raise click.ClickException(f"Group '{group_path}' not found in Zarr cube.") from exc

    try:
        ds = session.zarr.open_dataset(
            identity.product_uri,
            group=xr_group,
            decode_times=False,
        )
    except KeyError as exc:
        raise click.ClickException(f"Group '{group_path}' not found in Zarr cube.") from exc

    try:
        report = _COMPLIANCE_PROFILE_VALIDATORS[profile](ds, product=product, group=group_path)
    finally:
        ds.close()

    if output_format == "json":
        _render_compliance_json(report, profile=profile)
    else:
        _render_compliance_text(report, profile=profile)

    summary = report.summary
    if summary.errors > 0:
        sys.exit(1)
    if strict and (summary.warnings > 0 or summary.info > 0):
        sys.exit(2)


def _render_compliance_text(report: CFReport, *, profile: str) -> None:
    label = _COMPLIANCE_PROFILE_LABELS[profile]
    click.echo(f"{label} advisor \u2014 product={report.product} group={report.group}")

    for finding in report.findings:
        sev_tag = f"[{finding.severity.value}]"
        click.echo(f"{sev_tag:<10}{finding.id}: {finding.message}")
        if finding.suggested_fix:
            click.echo(f"          \u2192 {finding.suggested_fix}")

    summary = report.summary
    if not report.findings:
        click.echo(f"No findings \u2014 {label} structural checks passed.")
        return

    click.echo(
        f"Summary: {summary.errors} error{'s' if summary.errors != 1 else ''}, "
        f"{summary.warnings} warning{'s' if summary.warnings != 1 else ''}, "
        f"{summary.info} info."
    )


def _render_compliance_json(report: CFReport, *, profile: str) -> None:
    summary = report.summary
    payload = {
        "profile": profile,
        "product": report.product,
        "group": report.group,
        "findings": [
            {
                "id": f.id,
                "severity": f.severity.value,
                "target": f.target,
                "message": f.message,
                "suggested_fix": f.suggested_fix,
            }
            for f in report.findings
        ],
        "summary": {
            "errors": summary.errors,
            "warnings": summary.warnings,
            "info": summary.info,
        },
    }
    click.echo(_json.dumps(payload, indent=2))
