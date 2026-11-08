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

"""``firecube zarr validate`` and ``firecube zarr compare``: read-only diagnostics."""

from __future__ import annotations

import json

import click

from firecube.cli._ctx import get_storage_config
from firecube.cli._errors import render
from firecube.cli._exit_codes import ExitCode
from firecube.cli._shared_options import (
    storage_driver_option,
    storage_type_option,
    target_uri_option,
)
from firecube.cli._uri_policy import (
    apply_smart_default,
    parse_and_validate_uri,
    validate_uri_storage_coherence,
)
from firecube.core.zarr.validation import validate_group_with_fs
from firecube.operations import StorageDriverConfig, open_product
from firecube.sdk import compare_zarr_stores


@click.command(
    "validate",
    epilog="""\b
Examples:
  # validate a product group
  firecube zarr validate --target file:///data/products/MY_PRODUCT.zarr --group <group>
  # validate with a custom config file (config-file precedes subcommand)
  firecube --config-file /data/config/firecube.toml zarr validate \\
      --target file:///data/products/MY_PRODUCT.zarr --group <group>
See also: firecube chunks list, firecube parquet validate
""",
)
@target_uri_option(tier="inspect")
@click.option(
    "-g",
    "--group",
    "group_path",
    required=True,
    help="relative group path inside the product (e.g. F024/FWI)",
)
@click.option(
    "--timeout",
    "timeout_s",
    type=float,
    default=None,
    help="wall-clock timeout in seconds for chunk validation",
)
@click.option(
    "--max-chunks",
    "max_chunks",
    type=int,
    default=None,
    help="maximum number of chunks to process before stopping",
)
@click.option(
    "--on-timeout",
    "on_timeout",
    type=click.Choice(["warn", "fail"]),
    default="warn",
    show_default=True,
    help="behavior when budget is exceeded: warn returns partial report, fail raises an error",
)
@click.option(
    "--time-dim",
    "time_dim",
    default=None,
    show_default=False,
    help=(
        "time-dimension name used to classify arrays for the static-marker "
        "check; must match the cube's ``BaseIngestor.time_dim_name`` ClassVar. "
        "When omitted, the name is auto-detected from the stored "
        "``firecube_timestamp_state`` array or from a marked time coordinate. "
        "Pass this flag explicitly when "
        "the plugin uses a non-default dimension name (e.g. ``--time-dim time``)."
    ),
)
@storage_driver_option(required=False)
@storage_type_option(required=False)
@click.pass_context
def validate(
    ctx: click.Context,
    target: str,
    group_path: str,
    timeout_s: float | None,
    max_chunks: int | None,
    on_timeout: str,
    time_dim: str | None,
    storage_driver: str | None,
    storage_anonymous: bool | None,
    storage_type: str | None,
) -> None:
    """validate a Zarr array group

    Checks dimension shapes, dtypes, chunk boundaries, and required metadata
    without modifying any data. Requires --target and --group; exits with a
    structured JSON summary.
    """
    target_uri = parse_and_validate_uri(target, param_hint="--target")
    storage_type = apply_smart_default(target_uri, storage_type)
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
    product = ctx.with_resource(
        open_product(target_uri, format="zarr", driver=driver_config, require_label=False)
    )
    identity = product.identity

    fs = product.session.fs()
    try:
        report = validate_group_with_fs(
            fs,
            identity.product_uri,
            group_path,
            timeout_s=timeout_s,
            max_chunks=max_chunks,
            on_timeout=on_timeout,
            time_dim_name=time_dim,
        )
    except FileNotFoundError as exc:
        raise click.ClickException(f"Group '{group_path}' not found in Zarr product.") from exc
    except ValueError as exc:
        raise click.ClickException(render(exc)) from exc
    report_dict = report.to_dict()
    report_dict["info_notes"] = [render(note) for note in report_dict["info_notes"]]
    click.echo(json.dumps(report_dict, indent=2))
    if not report.is_valid:
        raise click.exceptions.Exit(ExitCode.CHECK_FAILED)


@click.command(
    "compare",
    context_settings={"help_option_names": ["-h", "--help"]},
    epilog="""\b
Examples:
  # compare two stores
  firecube zarr compare file:///data/a.zarr file:///data/b.zarr
""",
)
@click.argument("a_uri")
@click.argument("b_uri")
@click.option(
    "--storage-type",
    "storage_type",
    required=False,
    default=None,
    type=click.Choice(["local", "s3"], case_sensitive=False),
    help="Storage locality for both store URIs (inferred from URI scheme when omitted).",
)
@click.option(
    "--storage-driver",
    "storage_driver",
    required=False,
    default=None,
    type=click.Choice(["fsspec", "obstore"], case_sensitive=False),
    help="Storage driver for both store URIs (defaults to fsspec when omitted).",
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
def compare(
    a_uri: str,
    b_uri: str,
    storage_type: str | None,
    storage_driver: str | None,
    storage_anonymous: bool | None,
) -> None:
    """Compare two Zarr stores; exit 0 when equivalent or layout-only differences.

    Exits 1 when content mismatches are found (different values, shapes, dtypes,
    attrs, or missing arrays).  Layout-only differences (chunk shape, codecs) emit
    a WARNING to stderr and exit 0 — the stores are value-equivalent.

    Storage flags are optional: --storage-type is inferred from the URI scheme
    (file:// → local, s3:// → s3) and --storage-driver defaults to fsspec.
    """
    # A bare value of either argument is refused before any other check of A or B.
    # parse_and_validate_uri always refuses a non-empty value without "://".
    for value, param in ((a_uri, "A_URI"), (b_uri, "B_URI")):
        if value and "://" not in value:
            parse_and_validate_uri(value, param_hint=param)
    store_a = parse_and_validate_uri(a_uri, param_hint="A_URI")
    resolved_storage_type = apply_smart_default(store_a, storage_type)
    store_b = parse_and_validate_uri(b_uri, param_hint="B_URI")
    validate_uri_storage_coherence(store_b, resolved_storage_type)
    resolved_storage_driver = storage_driver.lower() if storage_driver is not None else "fsspec"
    report = compare_zarr_stores(
        a_uri,
        b_uri,
        storage_type=resolved_storage_type,
        storage_driver=resolved_storage_driver,
        anonymous=bool(storage_anonymous),
    )
    if report.equivalent:
        return
    if report.content_mismatches:
        for mismatch in report.content_mismatches:
            click.echo(mismatch, err=True)
        raise click.exceptions.Exit(ExitCode.CHECK_FAILED)
    # Layout-only differences: values are equivalent, warn and exit 0.
    layout_detail = "\n".join(report.layout_mismatches)
    click.echo(
        f"WARNING: layout differences only (chunks/codecs differ, values equivalent). "
        f"Use separate encoding options to realign.\n{layout_detail}",
        err=True,
    )
