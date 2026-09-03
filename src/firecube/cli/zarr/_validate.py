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
from typing import Any

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
from firecube.core.api import (
    FIRECUBE_STATIC_WRITTEN_ATTR,
    compare_zarr_stores,
)
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
        )
    except FileNotFoundError as exc:
        raise click.ClickException(f"Group '{group_path}' not found in Zarr product.") from exc
    except ValueError as exc:
        report = _validate_first_array_child(
            fs,
            identity.product_uri,
            group_path,
            timeout_s=timeout_s,
            max_chunks=max_chunks,
            on_timeout=on_timeout,
            original_error=exc,
        )
    output = report.to_dict()
    output["static_marker_failures"] = _static_marker_failures(
        fs, identity.product_uri, report.group
    )
    click.echo(json.dumps(output, indent=2))


@click.command(
    "compare",
    context_settings={"help_option_names": ["-h", "--help"]},
    epilog="""\b
Examples:
  firecube zarr compare file:///data/a.zarr file:///data/b.zarr \\
      --storage-type local --storage-driver fsspec
""",
)
@click.argument("a_uri")
@click.argument("b_uri")
@click.option(
    "--storage-type",
    "storage_type",
    required=True,
    type=click.Choice(["local", "s3"], case_sensitive=False),
    help="Storage locality for both store URIs.",
)
@click.option(
    "--storage-driver",
    "storage_driver",
    required=True,
    type=click.Choice(["fsspec", "obstore"], case_sensitive=False),
    help="Storage driver for both store URIs.",
)
@wrap_user_facing_errors
def compare(a_uri: str, b_uri: str, storage_type: str, storage_driver: str) -> None:
    """Compare two Zarr stores and exit 3 when they differ."""
    for uri in (a_uri, b_uri):
        require_full_uri(uri, option_name="store URI")
        validate_uri_storage_coherence(parse_product_uri(uri), storage_type)
    report = compare_zarr_stores(
        a_uri,
        b_uri,
        storage_type=storage_type.lower(),
        storage_driver=storage_driver.lower(),
    )
    if report.equivalent:
        return
    for mismatch in report.mismatches:
        click.echo(mismatch, err=True)
    raise click.exceptions.Exit(3)


def _validate_first_array_child(
    fs: Any,
    store_uri: Any,
    group_path: str,
    *,
    timeout_s: float | None,
    max_chunks: int | None,
    on_timeout: str,
    original_error: ValueError,
) -> Any:
    """Validate the first array below a requested container group."""
    prefix_uri = store_uri.join(group_path)
    prefix = prefix_uri.to_str().rstrip("/") + "/"
    for entry in fs.find(prefix_uri):  # pyright: ignore[reportArgumentType]
        entry_path = entry.to_str() if hasattr(entry, "to_str") else str(entry)
        if not entry_path.endswith("/zarr.json"):
            continue
        with fs.open(entry, "r") as handle:
            metadata = json.load(handle)
        if metadata.get("node_type") != "array":
            continue
        child_group = entry_path[: -len("/zarr.json")].removeprefix(prefix).strip("/")
        if not child_group:
            continue
        return validate_group_with_fs(
            fs,
            store_uri,
            f"{group_path.strip('/')}/{child_group}",
            timeout_s=timeout_s,
            max_chunks=max_chunks,
            on_timeout=on_timeout,
        )
    raise click.ClickException(str(original_error)) from original_error


def _static_marker_failures(fs: Any, store_uri: Any, array_path: str) -> list[dict[str, str]]:
    """Return static-array marker failures for the validated array, read-only."""
    meta_uri = store_uri.join(array_path).join("zarr.json")
    try:
        with fs.open(meta_uri, "r") as handle:  # pyright: ignore[reportArgumentType]
            metadata = json.load(handle)
    except (AttributeError, FileNotFoundError):
        return []

    dimension_names = metadata.get("dimension_names")
    if not dimension_names:
        return []
    first_dimension = str(dimension_names[0])
    if first_dimension in {"timestamp", "time", "firecube_timestamp_state"}:
        return []

    attrs = metadata.get("attributes") or {}
    if attrs.get(FIRECUBE_STATIC_WRITTEN_ATTR):
        return []
    return [{"array": array_path, "reason": "missing_or_false_static_marker"}]
