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

"""``firecube zarr slots``: chunk-aligned parallel ingestion plans."""

from __future__ import annotations

import contextlib
import json
import logging
import os
from collections.abc import Generator
from typing import Any, cast

import click

from firecube.cli._ctx import get_storage_config
from firecube.cli._product import require_full_uri, resolve_product_identity
from firecube.cli._shared_options import (
    format_option,
    product_name_option,
    storage_driver_option,
    storage_type_option,
)
from firecube.cli._slot_planning import (
    PLAN_SCHEMA_VERSION,
    _chunk_aligned_remaining,
    _complement_intervals,
    _merge_intervals,
    _partition_remaining,
    _query_slots_coverage,
    _resolve_per_group_slot_sizes,
)
from firecube.cli._typed_options import TypedOptionsParam
from firecube.cli._uri_policy import (
    apply_smart_default,
    parse_product_uri,
    validate_uri_storage_coherence,
)
from firecube.cli.zarr._common import _configure_ingestor_for_cli
from firecube.core.controlplane import ChunkManager
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.ingestor.errors import ConfigurationError
from firecube.ingestor.registry import loader as ingestor_loader
from firecube.ingestor.registry.loader import discover_ingestors
from firecube.ingestor.templates.direct_zarr import DirectZarrIngestor


def _warn_stderr(message: str) -> None:
    """Write an operator warning to stderr without contaminating Click stdout capture."""
    os.write(2, f"{message}\n".encode())


class _WarnFilter(logging.Filter):
    """Block WARNING+ records for a target logger and its descendants.

    Thread-safe replacement for `logger.disabled = True`, which mutates a
    shared attribute and races between CLI threads. This filter is per-instance
    and only silences WARNING and above; DEBUG/INFO on the target logger stay
    visible if the operator has enabled them.
    """

    def __init__(self, name: str) -> None:
        super().__init__()
        self._target = name
        self._target_dot = name + "."

    def filter(self, record: logging.LogRecord) -> bool:
        if record.name != self._target and not record.name.startswith(self._target_dot):
            return True
        return record.levelno < logging.WARNING


@contextlib.contextmanager
def _suppress_logger_warnings(logger_name: str) -> Generator[None]:
    """Temporarily suppress WARNING+ records that would corrupt JSON CLI output.

    Attaches ``_WarnFilter`` to the target logger and to every handler along
    the target's ancestor chain (up to and including root) because Python's
    ``logging`` does not consult ancestor-logger filters when a descendant
    record propagates: only handler-level filters are called on the way up.
    Attaching at handlers is what actually blocks descendant WARNING records
    before they reach the stderr sink that would corrupt JSON output.
    """
    target = logging.getLogger(logger_name)
    flt = _WarnFilter(logger_name)
    target.addFilter(flt)
    handlers_touched: list[logging.Handler] = []
    current: logging.Logger | None = target
    while current is not None:
        for handler in list(current.handlers):
            handler.addFilter(flt)
            handlers_touched.append(handler)
        if not current.propagate:
            break
        current = current.parent
    try:
        yield
    finally:
        for handler in handlers_touched:
            handler.removeFilter(flt)
        target.removeFilter(flt)


def _discover_ingestors_quietly() -> dict[str, type[Any]]:
    """Discover plugins without allowing registry warnings to contaminate JSON output."""
    with _suppress_logger_warnings("firecube.registry"):
        return discover_ingestors()


@click.command(
    "slots",
    context_settings={"help_option_names": ["-h", "--help"]},
    epilog="""\b
Examples:
  # Emit JSON plan for Argo/Kubeflow (resume-aware by default)
  firecube zarr slots <plugin> --target file:///tmp/x.zarr --product-name <name> \\
      --storage-type local --storage-driver fsspec --write-mode direct

  # Human-readable table output for inspection
  firecube zarr slots <plugin> --target file:///tmp/x.zarr --product-name <name> \\
      --storage-type local --storage-driver fsspec --write-mode direct -f table

  # Override slot partition size
  firecube zarr slots <plugin> --target file:///tmp/x.zarr --product-name <name> \\
      --storage-type local --storage-driver fsspec --write-mode direct --slot-size 200
""",
)
@click.argument("plugin")
@click.option(
    "--target",
    required=True,
    help="Target product URI required (file:///abs/path or s3://bucket/key).",
)
@product_name_option(required=True)
@storage_type_option(
    required=False, extra_help="Inferred from URI scheme when omitted (file://→local, s3://→s3)."
)
@storage_driver_option(required=False)
@click.option(
    "-w",
    "--write-mode",
    "write_mode",
    required=True,
    type=click.Choice(["staged", "direct"], case_sensitive=False),
    help="Write mode.",
)
@click.option(
    "--input-data",
    "input_data",
    default=None,
    help="Source input URI or path for AUTO discovery (optional).",
)
@click.option(
    "--slot-size",
    "slot_size",
    type=int,
    default=None,
    help="Slot partition size (default: chunk_shape[0] of first array of first group).",
)
@click.option(
    "--no-resume",
    "no_resume",
    is_flag=True,
    help="Disable resume-aware narrowing; emit full [0, total_slots) ranges.",
)
@format_option(default="json")
@click.option(
    "--option",
    "option",
    multiple=True,
    type=TypedOptionsParam(),
    help="Plugin/engine option in key=value form.",
)
@click.pass_context
def slots(
    ctx: click.Context,
    plugin: str,
    target: str,
    product_name: str,
    storage_type: str,
    storage_driver: str,
    write_mode: str,
    input_data: str | None,
    slot_size: int | None,
    no_resume: bool,
    output_format: str,
    option: tuple[tuple[str, object], ...] = (),
) -> None:
    """Emit chunk-aligned slot ranges for orchestrated parallel ingestion.

    Read-only: does NOT mutate target storage or tracking state.
    JSON output is Argo withItems / Kubeflow ParallelFor compatible.
    """
    require_full_uri(target, option_name="--target")
    parsed = parse_product_uri(target)
    storage_type = apply_smart_default(parsed, storage_type)
    validate_uri_storage_coherence(parse_product_uri(target), storage_type)

    storage_config = get_storage_config(
        ctx,
        overrides={"storage_type": storage_type, "storage_driver": storage_driver},
        cache=False,
    )
    resolved_storage_driver = storage_config.storage_driver
    identity = resolve_product_identity(
        target,
        format="zarr",
        product_name=product_name,
        option_name="--target",
    )
    binding = StorageBinding(
        identity=identity,
        driver=StorageDriverConfig.from_storage_config(storage_config),
    )
    chunk_manager = ChunkManager(binding=binding)

    plugins = (
        ingestor_loader.AVAILABLE_INGESTORS
        if plugin in ingestor_loader.AVAILABLE_INGESTORS
        else _discover_ingestors_quietly()
    )
    if plugin not in plugins:
        raise click.ClickException(f"Unknown plugin '{plugin}'.")
    ingestor_cls = cast(Any, plugins[plugin])

    ingestor = (
        ingestor_cls(chunk_manager=chunk_manager) if input_data is not None else ingestor_cls()
    )

    if not isinstance(ingestor, DirectZarrIngestor):
        raise click.ClickException(
            f"Plugin '{plugin}' does not support slot-range planning. "
            "This command is available for plugins that write Zarr stores in parallel."
        )
    with _suppress_logger_warnings("firecube.registry"):
        plugin_ctx = _configure_ingestor_for_cli(
            ingestor,
            target=target,
            options=option,
            source=input_data or "",
            run_id="zarr-slots",
        )

    try:
        from firecube.core.index_spec import AUTO, IrregularTimeAxis
        from firecube.ingestor.api import BaseIngestor

        index_spec = ingestor.index_spec(plugin_ctx)
        if (
            input_data is None
            and index_spec is not None
            and any(
                isinstance(axis, IrregularTimeAxis) and axis.values is AUTO
                for axis in index_spec.groups.values()
            )
            and type(ingestor).discover_source_files is BaseIngestor.discover_source_files
        ):
            raise click.ClickException(
                f"Plugin '{plugin}' requires --input-data for AUTO discovery. "
                "Pass --input-data <source> or override discover_source_files() in the plugin."
            )
        ingestor._bind_index_at_startup(plugin_ctx)
        resolved_index = ingestor.resolved_index(plugin_ctx)
    except click.ClickException:
        raise
    except Exception as exc:
        raise click.ClickException(
            f"Plugin '{plugin}' returned no index_spec. "
            "See the plugin's documentation for parallel-write support."
        ) from exc
    current_record = chunk_manager.get_resolved_index(product=identity.product_name)
    if current_record is not None and current_record.identity_hash != resolved_index.identity_hash:
        _warn_stderr(
            "Plugin resolved index differs from persisted record; "
            "run `firecube zarr index rebuild` to reconcile."
        )
    elif (
        current_record is None
        and chunk_manager.get_slot_index_model(product=identity.product_name) is not None
    ):
        _warn_stderr(
            "Legacy slot-index record detected without a resolved-index record; "
            "run `firecube zarr index rebuild` to migrate."
        )
    global_schema = {group: resolved_index.size(group) for group in resolved_index.groups}
    for group_name, expected in global_schema.items():
        if expected <= 0:
            raise click.ClickException(
                f"Plugin '{plugin}' returned a non-positive count {expected} "
                f"for group '{group_name}'. Expected positive integers only."
            )

    schema = ingestor.zarr_schema(plugin_ctx)
    from firecube.ingestor.runtime.parallel_gate import validate_global_expected_subset_of_schema

    try:
        validate_global_expected_subset_of_schema(global_schema, schema)
    except ConfigurationError as exc:
        raise click.ClickException(str(exc)) from exc

    from firecube.ingestor.runtime.parallel_gate import warn_on_chunk_alignment

    if output_format == "json":
        with _suppress_logger_warnings("firecube.ingestor.runtime.parallel_gate"):
            warn_on_chunk_alignment(global_schema, schema)
    else:
        warn_on_chunk_alignment(global_schema, schema)

    resolved_slot_sizes = _resolve_per_group_slot_sizes(schema, slot_size)

    if no_resume:
        coverage_by_group: dict[str, list[tuple[int, int]]] = {g: [] for g in global_schema}
    else:
        try:
            coverage_by_group = _query_slots_coverage(
                ctx,
                target=target,
                product_name=product_name,
                storage_type=storage_type,
                storage_driver=resolved_storage_driver,
                groups=list(global_schema.keys()),
            )
        except Exception as exc:
            raise click.ClickException(
                f"firecube zarr slots: resume-aware coverage lookup failed ({exc}). "
                f"Either fix the underlying failure or pass --no-resume to emit "
                f"full ranges without coverage narrowing."
            ) from exc

    groups_out: list[dict[str, Any]] = []
    ranges_out: list[dict[str, Any]] = []
    for group_name in sorted(global_schema.keys()):
        total = int(global_schema[group_name])
        covered = _merge_intervals(coverage_by_group.get(group_name, []))
        remaining = _complement_intervals(covered, total)
        group_slot_size = resolved_slot_sizes.get(group_name, total)
        aligned_remaining, blocked_for_group = _chunk_aligned_remaining(
            remaining, group_slot_size, total
        )
        partitioned = _partition_remaining(aligned_remaining, group_slot_size)
        static_owner = None
        if partitioned:
            owner_start, owner_end = min(partitioned, key=lambda item: item[0])
            static_owner = {"slot_start": owner_start, "slot_end": owner_end}

        for slot_start, slot_end in partitioned:
            ranges_out.append(
                {
                    "group": group_name,
                    "slot_start": slot_start,
                    "slot_end": slot_end,
                    "env": {
                        "FIRECUBE_SLOT_START": str(slot_start),
                        "FIRECUBE_SLOT_END": str(slot_end),
                        "FIRECUBE_SLOT_GROUP": group_name,
                    },
                    "cli_args": [
                        "--slot-start",
                        str(slot_start),
                        "--slot-end",
                        str(slot_end),
                        "--slot-group",
                        group_name,
                    ],
                }
            )

        groups_out.append(
            {
                "name": group_name,
                "total_slots": total,
                "slot_size": group_slot_size,
                "covered_ranges": [[s, e] for s, e in covered],
                "remaining_ranges": [[s, e] for s, e in remaining],
                "blocked_ranges": [[s, e] for s, e in blocked_for_group],
                "static_owner": static_owner,
            }
        )

    blocked_groups = [
        (group["name"], group["blocked_ranges"]) for group in groups_out if group["blocked_ranges"]
    ]
    if blocked_groups:
        details = "; ".join(
            f"group '{name}' has blocked ranges {ranges}" for name, ranges in blocked_groups
        )
        raise click.ClickException(
            "firecube zarr slots: cannot emit executable slots — chunk-alignment coverage would silently drop work. "
            f"Blocked: {details}. "
            "Remediation: preview with "
            "`firecube chunks delete-span --product-name <product-name> --run-id <id> --force --dry-run`, "
            "then commit with `--yes-i-really-mean-it` and re-ingest using `firecube ingest ... --option force_reingest=true`. "
            "See docs/operations/parallel-zarr-writes.md#recover-a-blocked-plan."
        )

    output = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "product_name": product_name,
        "target": target,
        "storage_type": storage_type,
        "storage_driver": resolved_storage_driver,
        "write_mode": write_mode,
        "strategy": "indexed-region",
        "groups": groups_out,
        "ranges": ranges_out,
    }
    _emit_slots_output(output, output_format)


def _emit_slots_output(output: dict[str, Any], output_format: str) -> None:
    if output_format == "json":
        click.echo(json.dumps(output, indent=2))
        return
    if output_format == "table":
        click.echo("group\tslot_start\tslot_end")
        for entry in output["ranges"]:
            click.echo(f"{entry['group']}\t{entry['slot_start']}\t{entry['slot_end']}")
        return
    if output_format == "csv":
        click.echo("group,slot_start,slot_end")
        for entry in output["ranges"]:
            click.echo(f"{entry['group']},{entry['slot_start']},{entry['slot_end']}")
        return
    raise click.ClickException(f"Unsupported output format: {output_format}")
