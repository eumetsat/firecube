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

"""``firecube zarr preallocate``: engine-owned array and coordinate materialization."""

from __future__ import annotations

import contextlib
import uuid
from typing import TYPE_CHECKING, Any, cast

import click

from firecube.cli._ctx import get_storage_config
from firecube.cli._errors import echo_rendered, render, wrap_user_facing_errors
from firecube.cli._input_filters import input_filters_option, resolve_input_filters
from firecube.cli._input_sources import admit_sources, input_data_option, stac_search_option
from firecube.cli._product import resolve_product_identity
from firecube.cli._typed_options import TypedOptionsParam
from firecube.cli._uri_policy import (
    apply_smart_default,
    parse_and_validate_uri,
)
from firecube.cli.zarr._common import _configure_ingestor_for_cli, logger
from firecube.core.api import (
    ATTR_COORD_MANAGED,
    ATTR_PREALLOCATED,
)
from firecube.core.controlplane import ChunkManager
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.session import StorageSession
from firecube.ingestor.registry.loader import discover_ingestors
from firecube.ingestor.templates.direct_zarr import DirectZarrIngestor

if TYPE_CHECKING:
    from firecube.ingestor.runtime.preallocate import PreallocateDryRunGroup


@click.command(
    "preallocate",
    epilog="""\b
Examples:
  # Pre-allocate arrays (idempotent — safe to re-run)
  firecube zarr preallocate <plugin> --product-name <name> \\
      --target file:///tmp/x.zarr --storage-type local --storage-driver fsspec --write-mode staged

  # Re-run on same target: exits 0, logs "no-op (matches nominal grid)" for each matching array
  firecube zarr preallocate <plugin> --product-name <name> \\
      --target file:///tmp/x.zarr --storage-type local --storage-driver fsspec --write-mode staged

See also: firecube zarr slots, firecube zarr validate
""",
)
@click.argument("plugin")
@click.option(
    "--target",
    required=True,
    help="Target product URI required (file:///abs/path or s3://bucket/key).",
)
@click.option("--product-name", "product_name", required=True, help="Logical product name.")
@click.option(
    "--storage-type",
    "storage_type",
    required=False,
    type=click.Choice(["local", "s3"], case_sensitive=False),
    help="Storage locality. Inferred from URI scheme when omitted (file://→local, s3://→s3).",
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
@click.option(
    "-w",
    "--write-mode",
    "write_mode",
    required=True,
    type=click.Choice(["staged", "direct"], case_sensitive=False),
)
@input_data_option()
@input_filters_option
@stac_search_option
@click.option(
    "--slot-start",
    "slot_start",
    type=int,
    default=None,
    help="Preallocate coordinate materialization window: first slot index (inclusive).",
)
@click.option(
    "--slot-end",
    "slot_end",
    type=int,
    default=None,
    help="Preallocate coordinate materialization window: last slot index (exclusive).",
)
@click.option(
    "--option",
    "option",
    multiple=True,
    type=TypedOptionsParam(),
    help="Plugin/engine option in key=value form.",
)
@click.option(
    "--dry-run/--no-dry-run",
    "dry_run",
    default=False,
    is_flag=True,
    help=(
        "Perform discovery and manifest computation only. "
        "Prints the resolved-index manifest as JSON to stdout. "
        "Makes zero filesystem mutations: no arrays are created, "
        "no index files are written, no claims are acquired."
    ),
)
@click.pass_context
@wrap_user_facing_errors
def preallocate(
    ctx: click.Context,
    plugin: str,
    target: str,
    product_name: str,
    storage_type: str,
    storage_driver: str,
    storage_anonymous: bool | None,
    write_mode: str,
    input_data: tuple[str, ...],
    input_filters: list[str] | None,
    stac_search: dict[str, Any] | None,
    slot_start: int | None,
    slot_end: int | None,
    option: tuple[tuple[str, object], ...],
    dry_run: bool,
) -> None:
    """Pre-allocate Zarr arrays for parallel ingestion.

    Idempotent: if arrays already exist with matching shape, dtype, and chunks,
    this command is a no-op. If arrays exist with mismatched schema, exits non-zero
    with a diff showing expected vs found values. Safe to re-run.
    """

    input_filters = resolve_input_filters(ctx, plugin, input_filters)

    from firecube.ingestor.api import BaseIngestor
    from firecube.ingestor.runtime.preallocate import SlotWindowError, preallocate_product

    target_uri = parse_and_validate_uri(target, param_hint="--target")
    storage_type = apply_smart_default(target_uri, storage_type)
    sources = admit_sources(input_data, stac_search, command="firecube zarr preallocate")
    if (slot_start is None) ^ (slot_end is None):
        raise click.UsageError("--slot-start and --slot-end must be provided together")
    from firecube.cli._slot_env import resolve_slot_range_from_env

    resolved_slot_start, resolved_slot_end, resolved_slot_group = resolve_slot_range_from_env(
        slot_start,
        slot_end,
        None,
        None,
    )

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
    identity = resolve_product_identity(
        target_uri,
        format="zarr",
        product_name=product_name,
    )
    binding = StorageBinding(identity=identity, driver=driver_config)
    session = StorageSession(binding)

    plugins = discover_ingestors()
    if plugin not in plugins:
        raise click.ClickException(f"Unknown plugin '{plugin}'.")
    ingestor_cls = cast(type[BaseIngestor], plugins[plugin])

    chunk_manager = ChunkManager(binding=binding)
    preallocate_run_id = f"preallocate:{uuid.uuid4().hex[:16]}"
    try:
        ingestor = ingestor_cls(chunk_manager=chunk_manager)

        if not isinstance(ingestor, DirectZarrIngestor):
            raise click.ClickException(
                f"Plugin '{plugin}' does not support slot-range parallelism. "
                "See the plugin's documentation for parallel-write support."
            )

        plugin_ctx = _configure_ingestor_for_cli(
            ingestor,
            target=target,
            sources=sources,
            options=option,
            input_filters=input_filters,
            stac_search=stac_search,
            run_id="zarr-preallocate",
        )

        try:
            # The operation records the run as terminal and releases its claim
            # before it returns or raises; the manager is closed below.
            preallocate_product(
                ingestor=ingestor,
                plugin_ctx=plugin_ctx,
                plugin_name=plugin,
                chunk_manager=chunk_manager,
                session=session,
                storage_config=storage_config,
                run_id=preallocate_run_id,
                write_mode=write_mode,
                input_data=sources or None,
                slot_start=resolved_slot_start,
                slot_end=resolved_slot_end,
                slot_group=resolved_slot_group,
                dry_run=dry_run,
                report=echo_rendered,
                warn=_echo_err,
                on_dry_run_group=_echo_dry_run_group,
                logger=logger,
            )
        except SlotWindowError as exc:
            raise click.UsageError(render(exc)) from exc
    finally:
        with contextlib.suppress(Exception):
            chunk_manager.close()


def _echo_err(line: str) -> None:
    """Print one line to stderr, rendered for the running command."""
    echo_rendered(line, err=True)


def _echo_dry_run_group(preview: PreallocateDryRunGroup) -> None:
    """Print one group's dry-run preview to stderr."""
    group_name = preview.group
    start, end = preview.window
    if preview.policy is None:
        _echo_err(
            f"group {group_name}: window [{start}, {end}); non-regular axis; no dry-run coords"
        )
    elif preview.policy == "grid":
        first, last = preview.first, preview.last
        if preview.calendar is not None and preview.units is not None and end > start:
            # The same decoder `index show --derived` uses, so the two surfaces
            # print a calendar date identically.
            from firecube.cli.index import _decoded_calendar_isoformats

            iso_first, iso_last = _decoded_calendar_isoformats(
                [first, last], units=preview.units, calendar=preview.calendar
            )
            first = f"{iso_first} ({preview.calendar})"
            last = f"{iso_last} ({preview.calendar})"
        window_suffix = (
            "" if preview.window_applied else "; window not applied to calendar coordinate"
        )
        _echo_err(
            f"group {group_name}: window [{start}, {end}); policy=grid; "
            f"items_in_window={preview.items_in_window}; first={first}; "
            f"last={last}; "
            f"would write nominal grid values and stamp {ATTR_PREALLOCATED}{window_suffix}"
        )
    elif preview.items_in_window is None:
        _echo_err(
            f"group {group_name}: window [{start}, {end}); policy=observed; "
            "no --input-data supplied; would leave coord values as NaT"
        )
    else:
        _echo_err(
            f"group {group_name}: window [{start}, {end}); policy=observed; "
            f"items_in_window={preview.items_in_window}; first={preview.first}; "
            f"last={preview.last}; "
            f"would write observed coord values at their slot indices and stamp {ATTR_COORD_MANAGED}"
        )
