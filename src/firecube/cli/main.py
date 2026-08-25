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

"""Unified Click-based CLI entrypoint for Firecube."""

# pyright: reportMissingImports=false, reportFunctionMemberAccess=false, reportCallIssue=false

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import click

from firecube.cli._command_schemas import IngestCommandConfig
from firecube.cli._ctx import get_storage_config
from firecube.cli._errors import MissingProductNameError, wrap_user_facing_errors
from firecube.cli._formatter import (
    COMMAND_GROUPS,
    OPTION_GROUPS,
    FirecubeGroup,
    install_option_groups_patch,
)
from firecube.cli._product import require_full_uri
from firecube.cli._rename_hints import install_rename_hints
from firecube.cli._shared_options import (
    storage_driver_option,
    storage_type_option,
    write_mode_option,
)
from firecube.cli._typed_options import TypedOptionsParam, coerce_options_for_plugin
from firecube.cli.advise import advise as advise_group
from firecube.cli.archive import archive as archive_group
from firecube.cli.catalog import catalog as catalog_group
from firecube.cli.chunks import chunks as chunks_group
from firecube.cli.completion import completion_cmd
from firecube.cli.parquet import parquet as parquet_group
from firecube.cli.plugins import plugins as plugins_group
from firecube.cli.zarr import zarr as zarr_group
from firecube.core.config import get_plugin_defaults, load_config_file
from firecube.core.observability import set_current_span_attribute, span
from firecube.core.product.resolver import ProductResolver
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.session import StorageSession
from firecube.core.uris import (
    is_remote_target,
    local_path_from_target,
)
from firecube.ingestor.api import IngestContext, StorageContext
from firecube.ingestor.registry.loader import discover_ingestors

log = logging.getLogger("firecube.cli")

install_option_groups_patch()
install_rename_hints()

CONTEXT_SETTINGS = {
    "help_option_names": ["-h", "--help"],
}

COMMAND_GROUPS["firecube"] = [
    {
        "name": "Core",
        "commands": ["ingest", "archive"],
    },
    {
        "name": "Inspect",
        "commands": ["zarr", "parquet", "chunks", "catalog"],
    },
    {
        "name": "Tools",
        "commands": ["plugins", "advise", "completion"],
    },
]

OPTION_GROUPS.update(
    {
        "firecube ingest": [
            {
                "name": "Input",
                "options": ["--input-data", "--target", "--product-name", "--output-format"],
            },
            {
                "name": "Execution",
                "options": [
                    "--write-mode",
                    "--storage-type",
                    "--storage-driver",
                    "--in-memory",
                    "--slot-start",
                    "--slot-end",
                    "--slot-size",
                ],
            },
            {
                "name": "Advanced",
                "options": ["--option", "--show-options"],
            },
        ],
        "firecube archive create": [
            {
                "name": "Required",
                "options": ["--source", "--archive"],
            },
            {
                "name": "Filtering",
                "options": ["--start-date", "--end-date", "--group", "--variables"],
            },
            {
                "name": "Encoding",
                "options": ["--compression"],
            },
            {
                "name": "Behavior",
                "options": ["--overwrite", "--allow-nan", "--allow-inf"],
            },
        ],
        "firecube archive restore": [
            {
                "name": "Required",
                "options": ["--archive", "--target"],
            },
            {
                "name": "Behavior",
                "options": ["--overwrite"],
            },
        ],
        "firecube chunks list": [
            {
                "name": "Filtering",
                "options": [
                    "--product-name",
                    "--start-date",
                    "--end-date",
                    "--time-range",
                    "--include-span",
                    "--type",
                    "--pattern",
                    "--meta",
                ],
            },
            {
                "name": "Output",
                "options": ["--format", "--limit"],
            },
            {
                "name": "Advanced",
                "options": ["--workspace"],
            },
        ],
        "firecube chunks delete": [
            {
                "name": "Scope",
                "options": ["--product-name", "--all-products"],
            },
            {
                "name": "Filters",
                "options": [
                    "--pattern",
                    "--type",
                    "--meta",
                    "--start-date",
                    "--end-date",
                    "--range",
                ],
            },
            {
                "name": "Mode",
                "options": ["--manifest-only", "--storage-only", "--include-metadata"],
            },
            {
                "name": "Safety",
                "options": ["--dry-run", "--yes-i-really-mean-it"],
            },
            {
                "name": "Context",
                "options": ["--workspace"],
            },
        ],
        "firecube chunks delete-span": [
            {
                "name": "Required",
                "options": ["--product-name"],
            },
            {
                "name": "Filtering",
                "options": ["--run-id", "--batch-id", "--group", "--meta", "--include-replaced"],
            },
            {
                "name": "Safety",
                "options": ["--dry-run", "--force", "--yes-i-really-mean-it"],
            },
            {
                "name": "Advanced",
                "options": ["--workspace"],
            },
        ],
        "firecube chunks runs list": [
            {
                "name": "Required",
                "options": ["--product-name"],
            },
            {
                "name": "Output",
                "options": ["--status", "--format", "--workspace"],
            },
        ],
        "firecube chunks runs abandon": [
            {
                "name": "Required",
                "options": ["--product-name", "--run-id", "--reason"],
            },
            {
                "name": "Safety",
                "options": ["--dry-run", "--yes-i-really-mean-it"],
            },
            {
                "name": "Advanced",
                "options": ["--workspace"],
            },
        ],
        "firecube chunks claims clear": [
            {
                "name": "Scope",
                "options": ["--domain", "--all-stale"],
            },
            {
                "name": "Required",
                "options": ["--product-name"],
            },
            {
                "name": "Safety",
                "options": ["--dry-run", "--yes-i-really-mean-it"],
            },
            {
                "name": "Advanced",
                "options": ["--workspace", "--force"],
            },
        ],
        "firecube catalog intake": [
            {
                "name": "Required",
                "options": ["--product", "--output", "--collection-id"],
            },
            {
                "name": "Optional",
                "options": ["--include-storage-options", "--no-storage-options"],
            },
        ],
        "firecube zarr validate": [
            {
                "name": "Required",
                "options": ["--product", "--group"],
            },
            {
                "name": "Options",
                "options": [
                    "--timeout",
                    "--max-chunks",
                    "--on-timeout",
                    "--storage-type",
                    "--storage-driver",
                ],
            },
        ],
        "firecube zarr slots": [
            {
                "name": "Required",
                "options": [
                    "--target",
                    "--product-name",
                    "--storage-type",
                    "--storage-driver",
                    "--write-mode",
                ],
            },
            {
                "name": "Options",
                "options": ["--slot-size", "--no-resume", "--format"],
            },
        ],
        "firecube zarr multires": [
            {
                "name": "Required",
                "options": ["--target", "--product-name", "--storage-type", "--storage-driver"],
            },
            {
                "name": "Options",
                "options": ["--resolutions"],
            },
        ],
        "firecube zarr preallocate": [
            {
                "name": "Required",
                "options": [
                    "--target",
                    "--product-name",
                    "--storage-type",
                    "--storage-driver",
                    "--write-mode",
                ],
            },
            {
                "name": "Input",
                "options": ["--input-data"],
            },
            {
                "name": "Advanced",
                "options": ["--option"],
            },
        ],
        "firecube parquet validate": [
            {
                "name": "Required",
                "options": ["--product-name"],
            },
        ],
        "firecube parquet consolidate": [
            {
                "name": "Required",
                "options": ["--product", "--output"],
            },
            {
                "name": "Options",
                "options": ["--codec", "--storage-type", "--storage-driver"],
            },
        ],
        "firecube plugins list": [
            {
                "name": "Options",
                "options": ["--format"],
            },
        ],
        "firecube plugins describe": [
            {
                "name": "Options",
                "options": ["--format"],
            },
        ],
        "firecube plugins explain": [
            {
                "name": "Options",
                "options": ["--format"],
            },
        ],
        "firecube plugins install": [
            {
                "name": "Options",
                "options": ["--editable"],
            },
        ],
        "firecube advise batch-size": [
            {
                "name": "Required",
                "options": ["--product", "--group"],
            },
        ],
        "firecube advise compliance": [
            {
                "name": "Required",
                "options": ["--profile", "--product", "--group"],
            },
            {
                "name": "Options",
                "options": [
                    "--format",
                    "--strict",
                    "--storage-type",
                    "--storage-driver",
                ],
            },
        ],
        "firecube archive info": [
            {
                "name": "Required",
                "options": ["--archive"],
            },
            {
                "name": "Options",
                "options": ["--format"],
            },
        ],
        "firecube archive validate": [
            {
                "name": "Required",
                "options": ["--archive"],
            },
            {
                "name": "Options",
                "options": ["--quick"],
            },
        ],
        "firecube archive list": [
            {
                "name": "Required",
                "options": ["--archive"],
            },
        ],
        "firecube chunks claims list": [
            {
                "name": "Filtering",
                "options": ["--product-name"],
            },
            {
                "name": "Output",
                "options": ["--format"],
            },
            {
                "name": "Advanced",
                "options": ["--workspace"],
            },
        ],
        "firecube chunks snapshots rebuild": [
            {
                "name": "Required",
                "options": ["--product-name"],
            },
            {
                "name": "Safety",
                "options": ["--dry-run"],
            },
            {
                "name": "Output",
                "options": ["--format"],
            },
            {
                "name": "Advanced",
                "options": ["--workspace"],
            },
        ],
        "firecube chunks snapshots status": [
            {
                "name": "Required",
                "options": ["--product-name"],
            },
            {
                "name": "Output",
                "options": ["--format"],
            },
            {
                "name": "Advanced",
                "options": ["--workspace"],
            },
        ],
    }
)


@click.group(cls=FirecubeGroup, context_settings=CONTEXT_SETTINGS)
@click.version_option(package_name="firecube", message="Firecube %(version)s")
@click.option(
    "--config-file",
    type=click.Path(path_type=Path),
    help="Firecube TOML config file (default: ~/.config/firecube/config.toml)",
)
@click.pass_context
def cli(ctx: click.Context, config_file: Path | None) -> None:
    """batch ingestion tool for EO datasets

    Batch ingestion and management tool for Earth Observation datasets.
    Writes Zarr and Parquet stores with built-in resume safety, idempotent
    chunk tracking, and multi-resolution support. Run firecube ingest to
    start a job, or explore subcommands for storage management and diagnostics.
    """
    ctx.ensure_object(dict)
    if config_file is not None:
        ctx.obj["config_file"] = config_file


@cli.command(
    epilog="""\b
Examples:
  # list available options for a plugin
  firecube ingest <plugin> --show-options
  # run ingestion from a local source to a local target
  firecube ingest <plugin> \\
    --input-data /path/to/input \\
    --target file:///path/to/output.zarr \\
    --product-name <name> \\
    --storage-type local \\
    --storage-driver fsspec \\
    --output-format zarr \\
    --write-mode direct
  # run with staged write mode (write locally then upload)
  firecube ingest <plugin> \\
    --input-data /data/raw \\
    --target s3://bucket/output.zarr \\
    --product-name <name> \\
    --storage-type s3 \\
    --storage-driver fsspec \\
    --output-format zarr \\
    --write-mode staged
  # pass plugin-specific options
  firecube ingest <plugin> \\
    --input-data /data/ \\
    --target file:///tmp/output.zarr \\
    --product-name <name> \\
    --storage-type local \\
    --storage-driver fsspec \\
    --output-format zarr \\
    --write-mode direct \\
    --option horizon=LST \\
    --option no_progress=true
See also: firecube plugins list, firecube chunks list, firecube advise batch-size
""",
)
@click.argument("plugin")
@click.option(
    "-i",
    "--input-data",
    "input_data",
    type=str,
    required=False,
    help="Raw plugin input data: local path, file:///abs/path, or s3:// prefix. Interpreted by the plugin.",
)
@click.option(
    "-t",
    "--target",
    type=str,
    required=False,
    help=("Target product URI required (file:///abs/path or s3://bucket/key); -t short available."),
)
@click.option(
    "-n",
    "--product-name",
    "product_name",
    default=None,
    help=(
        "Logical product name. Overrides the plugin class default and the "
        "per-plugin config default."
    ),
)
@click.option(
    "--output-format", required=False, help="output format to deliver (e.g. parquet, zarr)"
)
@storage_type_option(
    required=False,
    extra_help=(
        "Inferred from URI scheme when omitted (file://→local, s3://→s3). "
        "Explicit value overrides inference and is rejected on mismatch."
    ),
)
@storage_driver_option(
    required=False,
    extra_help=("Defaults to fsspec; overridable via FIRECUBE_STORAGE_DRIVER or [storage].driver."),
)
@write_mode_option(required=False, extra_help="Required. No local-target inference.")
@click.option(
    "--slot-start",
    "slot_start",
    type=int,
    default=None,
    help="Parallel ingestion: first slot index (inclusive).",
)
@click.option(
    "--slot-end",
    "slot_end",
    type=int,
    default=None,
    help="Parallel ingestion: last slot index (exclusive).",
)
@click.option(
    "--slot-size",
    "slot_size",
    type=int,
    default=None,
    help="Slot size for orchestrated parallel ingestion range derivation.",
)
@click.option(
    "--slot-group",
    default=None,
    help=(
        "Group name this worker owns when running multi-group parallel "
        "ingestion. Only workers with matching group are scheduled writes for "
        "that group. Defaults to None (all groups — single-group or "
        "non-parallel mode)."
    ),
)
@click.option(
    "--suppress-static-emission-for-non-owner",
    "suppress_static_emission_for_non_owner",
    is_flag=True,
    help=(
        "skip writes of static (non-time-indexed) arrays on this worker unless its "
        "--slot-start equals --static-owner-slot-start; pass to every fan-out worker "
        "so exactly one of them writes shared arrays such as latitude and longitude"
    ),
)
@click.option(
    "--static-owner-slot-start",
    "static_owner_slot_start",
    type=int,
    default=None,
    help=(
        "slot-start of the worker that writes static arrays; take the value from the "
        "static_owner field in the 'firecube zarr slots --format json' plan and pass "
        "the same value to every worker together with "
        "--suppress-static-emission-for-non-owner"
    ),
)
@click.option("--in-memory", is_flag=True, help="use in-memory DuckDB")
@click.option(
    "--option",
    "extra_options",
    multiple=True,
    type=TypedOptionsParam(),
    help="Plugin/engine option in key=value form.",
)
@click.option(
    "--show-options",
    is_flag=True,
    help="show available options for the specified plugin and exit",
)
@click.pass_context
@wrap_user_facing_errors
def ingest(
    ctx: click.Context,
    plugin: str,
    input_data: str | None,
    target: str | None,
    product_name: str | None,
    storage_type: str | None,
    storage_driver: str | None,
    output_format: str | None,
    write_mode: str | None,
    slot_start: int | None,
    slot_end: int | None,
    slot_size: int | None,
    slot_group: str | None,
    suppress_static_emission_for_non_owner: bool,
    static_owner_slot_start: int | None,
    in_memory: bool,
    extra_options,
    show_options: bool,
) -> None:
    """run an ingestion job for a plugin

    The plugin handles source reading; firecube manages write strategy (staged
    or direct), pipeline parallelism, and chunk tracking. Use --option to pass
    plugin-specific overrides, or --show-options to list available options for
    a plugin.
    """
    extra_options = coerce_options_for_plugin(plugin, extra_options)
    ingest_cfg: IngestCommandConfig | None = None
    if not show_options:
        from firecube.cli._uri_policy import apply_smart_default, parse_product_uri

        storage_type_resolved: str | None = storage_type
        if target is not None:
            parsed_target = parse_product_uri(target)
            storage_type_resolved = apply_smart_default(parsed_target, storage_type)
        # storage_driver flows through unresolved; build_storage_config handles
        # env > config > default precedence downstream.

        try:
            ingest_cfg = IngestCommandConfig(
                plugin=plugin,
                input_data=input_data,
                target=target or "",
                write_mode=write_mode,
                storage_type=storage_type_resolved,
                storage_driver=storage_driver,
                product_name=product_name,
                output_format=output_format or "zarr",
                in_memory=in_memory,
                options=dict(extra_options),
            )
        except click.UsageError:
            raise

    if not show_options:
        if target is None:
            raise click.ClickException("Missing option '--target'.")
        require_full_uri(str(target), option_name="--target")

    if (slot_start is None) ^ (slot_end is None):
        raise click.UsageError("--slot-start and --slot-end must be provided together")

    plugins = discover_ingestors()
    if plugin not in plugins:
        raise click.ClickException(f"Unknown plugin: {plugin}")

    ingestor_cls = plugins[plugin]

    if show_options:
        _print_plugin_options(plugin, ingestor_cls)
        return

    if input_data:
        input_data_str = str(input_data)
        if not is_remote_target(input_data_str):
            local_source = local_path_from_target(input_data_str)
            if not local_source.exists():
                raise click.ClickException(f"Input data not found: {input_data}")
            input_data = str(local_source)
        else:
            input_data = input_data_str

    from firecube.core import observability

    observability.init_observability(f"firecube-ingestor-{plugin}")
    try:
        with span(
            "firecube.cli.ingest",
            attributes={
                "firecube.plugin": plugin,
                "firecube.write_mode": str(write_mode or ""),
            },
        ):
            explicit_config = ctx.obj.get("config_file")
            cfg = load_config_file(explicit_config, strict=explicit_config is not None)
            assert ingest_cfg is not None
            options = get_plugin_defaults(cfg, plugin)
            from firecube.cli._slot_env import resolve_slot_range_from_env

            resolved_slot_start, resolved_slot_end, resolved_slot_group = (
                resolve_slot_range_from_env(
                    slot_start,
                    slot_end,
                    slot_size,
                    slot_group,
                )
            )
            options["slot_start"] = resolved_slot_start
            options["slot_end"] = resolved_slot_end
            options["slot_size"] = slot_size
            options["slot_group"] = resolved_slot_group
            options["suppress_static_emission_for_non_owner"] = (
                suppress_static_emission_for_non_owner
            )
            options["static_owner_slot_start"] = static_owner_slot_start
            resolved_product_name = (
                ingest_cfg.product_name
                or get_plugin_defaults(cfg, plugin).get("default_product_name")
                or getattr(ingestor_cls, "PRODUCT_NAME", None)
            )
            if not resolved_product_name:
                raise MissingProductNameError(plugin)

            target_str = ingest_cfg.target
            require_full_uri(target_str, option_name="--target")
            try:
                identity = ProductResolver.resolve(
                    target_str,
                    format=ingest_cfg.output_format,
                    product_name=str(resolved_product_name),
                )
            except ValueError as exc:
                raise click.UsageError(str(exc)) from exc
            storage_overrides: dict[str, str | None] = {
                "storage_type": str(ingest_cfg.storage_type)
                if ingest_cfg.storage_type is not None
                else None,
                "storage_driver": str(ingest_cfg.storage_driver)
                if ingest_cfg.storage_driver is not None
                else None,
            }
            storage_config = get_storage_config(ctx, overrides=storage_overrides, cache=False)
            options.setdefault("storage", {"type": storage_config.storage_type})

            # Per HIGH-3 fix: read every option from the validated typed config, not raw Click params.
            # IngestCommandConfig has applied defaults and validation; the raw params are inputs to
            # its construction only. This ensures typed defaults (e.g. output_format="zarr") flow through.
            final_write_mode = ingest_cfg.write_mode
            set_current_span_attribute("firecube.write_mode", str(final_write_mode))
            options["write_mode"] = final_write_mode

            final_output_format = ingest_cfg.output_format
            if final_output_format == "tensogram":
                from firecube.ingestor.api import is_dataset_producer

                if not is_dataset_producer(ingestor_cls):
                    raise click.ClickException(
                        f"Plugin '{plugin}' does not support --output-format tensogram. "
                        "It must implement the DatasetProducer protocol "
                        "(build_dataset + get_batch_groups)."
                    )

            # Instantiate the ingestor *after* setting the global storage config so
            # BaseIngestor can initialise ChunkManager with the correct output base.
            ingestor = ingestor_cls()

            for key, coerced_value in extra_options:
                options[key] = coerced_value

            try:
                upload_workers = int(options.get("upload_workers", 4))
            except (TypeError, ValueError) as exc:
                raise click.BadParameter("upload_workers must be an integer >= 1.") from exc
            if upload_workers < 1:
                raise click.BadParameter("upload_workers must be at least 1.")

            driver_config = StorageDriverConfig.from_storage_config(storage_config)
            session = StorageSession(
                StorageBinding(
                    identity=identity,
                    driver=driver_config,
                )
            )
            plugin_target = identity.product_uri.to_str()
            log.debug(
                "Ingest target wiring (target=%s plugin_target=%s product_name=%s resolved_product_name=%s write_mode=%s storage_type=%s)",
                target_str,
                plugin_target,
                identity.product_name,
                resolved_product_name,
                final_write_mode,
                storage_config.storage_type,
            )

            # Generate Run ID (Engine-owned)
            run_id = str(options.get("run_id") or options.get("manifest_run_id") or "")
            if not run_id:
                import uuid

                hostname = os.getenv("HOSTNAME", "host")
                run_id = f"{plugin}-{hostname}-{uuid.uuid4().hex}"

            ingest_ctx = IngestContext(
                source=input_data or "",
                target=plugin_target,
                in_memory=ingest_cfg.in_memory,
                output_format=final_output_format,
                options=options,
                storage=StorageContext(output=session),
                run_id=run_id,
            )
            result = ingestor.run(ingest_ctx)

            if result.manifest:
                click.echo(json.dumps(result.manifest, indent=2))
            else:
                click.echo(
                    json.dumps(
                        {
                            "output_path": result.outputs.primary,
                            "output_format": result.output_format,
                        },
                        indent=2,
                    )
                )
    finally:
        from firecube.core.observability import shutdown_observability

        shutdown_observability(timeout_millis=5000)


cli.add_command(archive_group, name="archive")
cli.add_command(advise_group, name="advise")
cli.add_command(chunks_group, name="chunks")
cli.add_command(plugins_group, name="plugins")
cli.add_command(zarr_group, name="zarr")
cli.add_command(catalog_group, name="catalog")
cli.add_command(parquet_group, name="parquet")
cli.add_command(completion_cmd, name="completion")


def _print_plugin_options(plugin_name: str, ingestor_cls: type) -> None:
    """Introspect and print available options for a plugin using tiered config."""
    click.echo(f"Available options for plugin '{plugin_name}':\n")

    if not hasattr(ingestor_cls, "describe_options"):
        click.echo("  (Plugin does not support option introspection)")
        return

    # Helper method on BaseIngestor returning dict[Tier, list[str]]
    tiers = ingestor_cls.describe_options()

    for tier, keys in tiers.items():
        if not keys:
            continue

        click.echo(f"[{tier}]")
        for key in keys:
            click.echo(f"  --option {key}")
        click.echo("")


if __name__ == "__main__":
    cli()
