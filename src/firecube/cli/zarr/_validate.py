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
from firecube.cli._errors import wrap_user_facing_errors
from firecube.cli._product import require_full_uri, resolve_product_identity
from firecube.cli._shared_options import (
    product_uri_option,
    storage_driver_option,
    storage_type_option,
)
from firecube.cli._uri_policy import (
    apply_smart_default,
    parse_product_uri,
    validate_uri_storage_coherence,
)
from firecube.core.api import compare_zarr_stores
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.session import StorageSession
from firecube.core.zarr.validation import validate_group_with_fs


@click.command(
    "validate",
    epilog="""\b
Examples:
  # validate a product group
  firecube zarr validate -p <product> -g <group>
  # validate with a custom config file (config-file precedes subcommand)
  firecube --config-file /path/config.toml zarr validate -p <product> -g <group>
See also: firecube chunks list, firecube parquet validate
""",
)
@product_uri_option(tier="inspect")
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
        "``firecube_timestamp_state`` array. Pass this flag explicitly when "
        "the plugin uses a non-default dimension name (e.g. ``--time-dim time``)."
    ),
)
@storage_driver_option(required=False)
@storage_type_option(required=False)
@click.pass_context
@wrap_user_facing_errors
def validate(
    ctx: click.Context,
    product: str,
    group_path: str,
    timeout_s: float | None,
    max_chunks: int | None,
    on_timeout: str,
    time_dim: str | None,
    storage_driver: str | None,
    storage_type: str | None,
) -> None:
    """validate a Zarr array group

    Checks dimension shapes, dtypes, chunk boundaries, and required metadata
    without modifying any data. Requires --product and --group; exits with a
    structured JSON summary.
    """
    parsed_uri = parse_product_uri(product)
    storage_type = apply_smart_default(parsed_uri, storage_type)
    storage_config = get_storage_config(
        ctx,
        overrides={"storage_type": storage_type, "storage_driver": storage_driver},
        cache=False,
    )
    driver_config = StorageDriverConfig.from_storage_config(storage_config)
    identity = resolve_product_identity(
        parsed_uri.normalized, format="zarr", product_name=parsed_uri.normalized
    )
    session = StorageSession(
        StorageBinding(
            identity=identity,
            driver=driver_config,
        )
    )

    fs = session.fs()
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
        raise click.ClickException(str(exc)) from exc
    click.echo(json.dumps(report.to_dict(), indent=2))
    if not report.is_valid:
        raise click.exceptions.Exit(1)


@click.command(
    "compare",
    context_settings={"help_option_names": ["-h", "--help"]},
    epilog="""\b
Examples:
  firecube zarr compare file:///data/a.zarr file:///data/b.zarr
  firecube zarr compare file:///data/a.zarr file:///data/b.zarr \\
      --storage-type local --storage-driver fsspec
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
@wrap_user_facing_errors
def compare(
    a_uri: str,
    b_uri: str,
    storage_type: str | None,
    storage_driver: str | None,
) -> None:
    """Compare two Zarr stores; exit 0 when equivalent or layout-only differences.

    Exits 1 when content mismatches are found (different values, shapes, dtypes,
    attrs, or missing arrays).  Layout-only differences (chunk shape, codecs) emit
    a WARNING to stderr and exit 0 — the stores are value-equivalent.

    Storage flags are optional: --storage-type is inferred from the URI scheme
    (file:// → local, s3:// → s3) and --storage-driver defaults to fsspec.
    """
    for uri in (a_uri, b_uri):
        require_full_uri(uri, option_name="store URI")
    parsed_a = parse_product_uri(a_uri)
    resolved_storage_type = apply_smart_default(parsed_a, storage_type)
    for uri in (a_uri, b_uri):
        validate_uri_storage_coherence(parse_product_uri(uri), resolved_storage_type)
    resolved_storage_driver = storage_driver.lower() if storage_driver is not None else "fsspec"
    report = compare_zarr_stores(
        a_uri,
        b_uri,
        storage_type=resolved_storage_type,
        storage_driver=resolved_storage_driver,
    )
    if report.equivalent:
        return
    if report.content_mismatches:
        for mismatch in report.content_mismatches:
            click.echo(mismatch, err=True)
        raise click.exceptions.Exit(1)
    # Layout-only differences: values are equivalent, warn and exit 0.
    layout_detail = "\n".join(report.layout_mismatches)
    click.echo(
        f"WARNING: layout differences only (chunks/codecs differ, values equivalent). "
        f"Use separate encoding options to realign.\n{layout_detail}",
        err=True,
    )
