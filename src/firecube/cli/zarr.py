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

"""Zarr-related helper commands for the Firecube CLI.

Most commands are read-only diagnostics (list, inspect, scrub).
``preallocate`` mutates the target by idempotently pre-allocating Zarr arrays
for parallel ingestion.
"""

from __future__ import annotations

import contextlib
import json
from typing import Any

import click
import numpy as np

from firecube.cli._ctx import get_storage_config
from firecube.cli._errors import wrap_user_facing_errors
from firecube.cli._product import require_full_uri, resolve_product_identity
from firecube.cli._shared_options import (
    format_option,
    product_name_option,
    product_uri_option,
    storage_driver_option,
    storage_type_option,
)
from firecube.cli._slot_planning import (
    PLAN_SCHEMA_VERSION,
    _chunk_aligned_remaining,
    _resolve_per_group_slot_sizes,
)
from firecube.cli._typed_options import TypedOptionsParam, coerce_options_for_plugin
from firecube.cli._uri_policy import (
    apply_smart_default,
    parse_product_uri,
    validate_uri_storage_coherence,
)
from firecube.core import observability
from firecube.core.controlplane import ChunkManager, WriteDomain
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.session import StorageSession
from firecube.core.zarr.layers import DEFAULT_MULTIRES_RESOLUTIONS
from firecube.core.zarr.multires import MultiresConfig, ZarrMultiresBuilder
from firecube.core.zarr.validation import validate_group_with_fs
from firecube.ingestor.api import IngestContext, PluginContext, RuntimeIngestContext
from firecube.ingestor.errors import ConfigurationError
from firecube.ingestor.registry.loader import discover_ingestors
from firecube.ingestor.runtime.configure import TierConfigurator
from firecube.ingestor.templates.direct_zarr import DirectZarrIngestor


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.pass_context
def zarr(ctx: click.Context) -> None:
    """Manage Zarr products

    Validate, build multi-resolution pyramids, pre-allocate arrays for parallel
    ingestion, and plan chunk-aligned slot ranges. Use zarr validate to check
    structural consistency, zarr multires to build downsampled layers, and
    zarr slots to emit parallel ingestion plans.
    """
    observability.init_observability("firecube-zarr")
    ctx.ensure_object(dict)


@zarr.command(
    "slots",
    context_settings={"help_option_names": ["-h", "--help"]},
    epilog="""\b
Examples:
  # Emit JSON plan for Argo/Kubeflow (resume-aware by default)
  firecube zarr slots <plugin> --target file:///tmp/x.zarr --product-name <name> \\
      --storage-type local --storage-driver fsspec --write-mode direct


  # Human-readable table output for inspection
  firecube zarr slots <plugin> --target file:///tmp/x.zarr --product-name <name> \\
      --storage-type local --storage-driver fsspec --write-mode direct -f table


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
@click.pass_context
def slots(
    ctx: click.Context,
    plugin: str,
    target: str,
    product_name: str,
    storage_type: str,
    storage_driver: str,
    write_mode: str,
    slot_size: int | None,
    no_resume: bool,
    output_format: str,
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

    plugins = discover_ingestors()
    if plugin not in plugins:
        raise click.ClickException(f"Unknown plugin '{plugin}'.")
    ingestor_cls = plugins[plugin]

    ingestor = ingestor_cls()

    if not isinstance(ingestor, DirectZarrIngestor):
        raise click.ClickException(
            f"Plugin '{plugin}' does not support slot-range planning. "
            "This command is available for plugins that write Zarr stores in parallel."
        )
    if not isinstance(ingestor, DirectZarrIngestor) or not ingestor.SUPPORTS_SLOT_RANGE_PARALLELISM:
        raise click.ClickException(
            f"Plugin '{plugin}' has not opted into slot-range parallelism. "
            "See the plugin's documentation for parallel-write support."
        )

    plugin_ctx = _build_slots_plugin_context(target=target, product_name=product_name)

    global_schema = ingestor.global_expected_time_count(plugin_ctx)
    if not global_schema:
        result_description = "None" if global_schema is None else "an empty dict"
        raise click.ClickException(
            f"Plugin '{plugin}' returned {result_description}. "
            "Must return a non-empty dict with positive values per group."
        )
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


@zarr.command(
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
    click.echo(json.dumps(report.to_dict(), indent=2))


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


@zarr.command(
    "multires",
    epilog="""\b
Examples:
  Default multi-resolution layers for a local Zarr product:
  firecube zarr multires --target file:///p.zarr --product-name p \\
      --storage-type local --storage-driver fsspec

\b
  Explicit resolution levels:
  firecube zarr multires --target s3://b/p.zarr --product-name p \\
      --storage-type s3 --storage-driver fsspec -r 1.0 -r 0.5

\b
See also: firecube zarr validate
""",
)
@click.option(
    "-t",
    "--target",
    "target",
    required=True,
    help="Target product URI required (file:///abs/path or s3://bucket/key); scheme must match --storage-type.",
)
@click.option(
    "--resolutions",
    "resolutions",
    "-r",
    multiple=True,
    type=float,
    help="Resolution levels to build (repeatable, e.g. -r 1.0 -r 0.5).",
)
@product_name_option(required=True)
@storage_type_option(
    required=False, extra_help="Inferred from URI scheme when omitted (file://→local, s3://→s3)."
)
@storage_driver_option(required=False)
@click.pass_context
def multires(
    ctx: click.Context,
    target: str,
    resolutions: tuple[float, ...],
    product_name: str,
    storage_type: str,
    storage_driver: str,
) -> None:
    """Build multi-resolution Zarr pyramid for an existing product."""
    parsed = parse_product_uri(target)
    storage_type = apply_smart_default(parsed, storage_type)
    storage_config = get_storage_config(
        ctx,
        overrides={"storage_type": storage_type, "storage_driver": storage_driver},
        cache=False,
    )
    driver_config = StorageDriverConfig.from_storage_config(storage_config)
    identity = resolve_product_identity(
        target,
        format="zarr",
        product_name=product_name,
        option_name="--target",
    )
    session = StorageSession(
        StorageBinding(
            identity=identity,
            driver=driver_config,
        )
    )
    config = MultiresConfig(
        product=identity.product_name,
        group="",
        resolutions=list(resolutions) or list(DEFAULT_MULTIRES_RESOLUTIONS),
    )
    try:
        result = ZarrMultiresBuilder(
            storage_config,
            product_name=identity.product_name,
            product_uri=identity.product_uri,
            session=session,
        ).build(config)
    except (ImportError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo(json.dumps(result, indent=2))


@zarr.command(
    "preallocate",
    epilog="""\b
Examples:
  # Pre-allocate arrays (idempotent — safe to re-run)
  firecube zarr preallocate <plugin> --product-name <name> \\
      --target file:///tmp/x.zarr --storage-type local --storage-driver fsspec --write-mode staged


  # Re-run on same target: exits 0, logs "no-op" for each matching array
  firecube zarr preallocate <plugin> --product-name <name> \\
      --target file:///tmp/x.zarr --storage-type local --storage-driver fsspec --write-mode staged


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
    "-w",
    "--write-mode",
    "write_mode",
    required=True,
    type=click.Choice(["staged", "direct"], case_sensitive=False),
)
@click.option(
    "-i",
    "--input-data",
    "input_data",
    type=str,
    required=False,
    default=None,
    help=(
        "Raw plugin input data: local path, file:///abs/path, or s3:// prefix. "
        "Interpreted by the plugin."
    ),
)
@click.option(
    "--option",
    "option",
    multiple=True,
    type=TypedOptionsParam(),
    help="Plugin/engine option in key=value form.",
)
@click.pass_context
def preallocate(
    ctx: click.Context,
    plugin: str,
    target: str,
    product_name: str,
    storage_type: str,
    storage_driver: str,
    write_mode: str,
    input_data: str | None,
    option: tuple[tuple[str, object], ...],
) -> None:
    """Pre-allocate Zarr arrays for parallel ingestion.

    Idempotent: if arrays already exist with matching shape, dtype, and chunks,
    this command is a no-op. If arrays exist with mismatched schema, exits non-zero
    with a diff showing expected vs found values. Safe to re-run.
    """
    from typing import cast

    from firecube.core.controlplane import ChunkManager
    from firecube.core.uris import storage_uri_from_target
    from firecube.core.zarr.region_writer import RegionZarrWriter
    from firecube.ingestor.api import (
        BaseIngestor,
        IndexedRegionStrategy,
        IngestContext,
        PluginContext,
        RuntimeIngestContext,
        StorageContext,
    )
    from firecube.ingestor.errors import (
        ConfigurationError,
    )
    from firecube.ingestor.registry.loader import discover_ingestors
    from firecube.ingestor.templates.direct_zarr import DirectZarrIngestor

    require_full_uri(target, option_name="--target")
    parsed = parse_product_uri(target)
    storage_type = apply_smart_default(parsed, storage_type)
    validate_uri_storage_coherence(parse_product_uri(target), storage_type)
    _ = write_mode  # accepted for parity with `firecube ingest`; not used by preallocate

    # Typed coercion of --option pairs against the plugin's config schema
    # (mirrors `firecube ingest`). Unknown keys (outside the experimental
    # ``x_*`` namespace) are rejected as UnknownOptionError BEFORE any I/O.
    coerced_options = coerce_options_for_plugin(plugin, tuple(option))

    storage_config = get_storage_config(
        ctx,
        overrides={"storage_type": storage_type, "storage_driver": storage_driver},
        cache=False,
    )
    driver_config = StorageDriverConfig.from_storage_config(storage_config)
    identity = resolve_product_identity(
        target,
        format="zarr",
        product_name=product_name,
        option_name="--target",
    )
    binding = StorageBinding(identity=identity, driver=driver_config)
    session = StorageSession(binding)

    plugins = discover_ingestors()
    if plugin not in plugins:
        raise click.ClickException(f"Unknown plugin '{plugin}'.")
    ingestor_cls = cast(type[BaseIngestor], plugins[plugin])

    chunk_manager = ChunkManager(binding=binding)
    plugin_target = identity.product_uri.to_str()
    # Run-lifecycle bookkeeping: ``ensure_slot_index_model`` creates a
    # non-terminal ``run.json`` (status=started). The command MUST drive that
    # run to a terminal state (complete/failed) before returning so the next
    # slot-range ingest is not blocked by ``ResumeGuard`` on a stuck run.
    _preallocate_run_created = False
    _preallocate_completed = False
    try:
        ingestor = ingestor_cls(chunk_manager=chunk_manager)

        if (
            not isinstance(ingestor, DirectZarrIngestor)
            or not ingestor.SUPPORTS_SLOT_RANGE_PARALLELISM
        ):
            raise click.ClickException(
                f"Plugin '{plugin}' does not support slot-range parallelism. "
                "See the plugin's documentation for parallel-write support."
            )

        ingest_ctx = IngestContext(
            source=str(input_data) if input_data is not None else "",
            target=plugin_target,
            output_format="zarr",
            options=dict(coerced_options),
            storage=StorageContext(output=session),
            run_id="preallocate",
        )
        runtime_ctx = RuntimeIngestContext.from_ingest_context(
            ingest_ctx,
            run_id="preallocate",
            temp_root=None,
            materializer=lambda src: src,
        )
        # Typed-config tier resolution. Preallocate bypasses BaseIngestor.run(), so we replicate the tier-coercion step here. See plans/IDEAS.md §22 / DONE.md for rationale.
        configurator = TierConfigurator(
            ingestor.template_config_class,
            ingestor.plugin_config_class,
            plugin_name=ingestor.name,
        )
        ingestor.engine_config, ingestor.template_config, ingestor.plugin_config = (
            configurator.configure(runtime_ctx)
        )
        plugin_ctx = PluginContext(runtime_ctx)

        # Resolve the plugin's slot-index model and persist it through the
        # control-plane BEFORE any Zarr array/group is created. A failure here
        # MUST happen before any store mutation so partially-initialised stores
        # cannot leak into the target on error.
        if getattr(type(ingestor), "SUPPORTS_SLOT_RANGE_PARALLELISM", False):
            try:
                slot_model = ingestor.slot_index_model(plugin_ctx)
            except Exception as exc:
                raise click.ClickException(
                    f"Plugin '{plugin}' failed to resolve slot_index_model: {exc}"
                ) from exc

            try:
                chunk_manager.ensure_slot_index_model(
                    product=identity.product_name,
                    model=slot_model,
                    run_id="preallocate",
                )
            except Exception as exc:
                raise click.ClickException(
                    f"Failed to record slot_index_model for plugin '{plugin}': {exc}"
                ) from exc
            _preallocate_run_created = True

        try:
            global_expected = ingestor.global_expected_time_count(plugin_ctx)
        except Exception as exc:
            raise click.ClickException(
                f"Plugin '{plugin}' failed to report expected time counts: {exc}"
            ) from exc
        if not global_expected:
            raise click.ClickException(
                f"Plugin '{plugin}' returned {'None' if global_expected is None else 'an empty dict'}. "
                "Cannot preallocate schema without expected sizes."
            )

        schema = ingestor.zarr_schema(plugin_ctx)
        all_specs = [array for group in schema for array in group.arrays]
        from firecube.ingestor.runtime.zarr.write import derive_effective_codecs_for_spec
        from firecube.ingestor.templates.config import (
            ZarrTemplateConfig,
            validate_zarr_specs_against_template,
        )

        template_config = ingestor.template_config
        if not isinstance(template_config, ZarrTemplateConfig):
            raise click.ClickException(
                f"Plugin '{plugin}' did not resolve a Zarr template configuration."
            )
        validate_zarr_specs_against_template(all_specs, template_config)

        from firecube.ingestor.runtime.parallel_gate import (
            validate_global_expected_subset_of_schema,
        )

        try:
            validate_global_expected_subset_of_schema(global_expected, schema)
        except ConfigurationError as exc:
            raise click.ClickException(str(exc)) from exc

        coord_names_by_group = {spec.group: spec.coord_names for spec in schema}

        strategy = IndexedRegionStrategy(
            store_uri=plugin_target,
            schema=schema,
            coord_names_by_group=coord_names_by_group,
            storage_config=storage_config,
            session=session,
        )

        zarr_store = None
        if getattr(strategy, "_storage_config", None):
            zarr_store = session.zarr.create_store(
                uri=storage_uri_from_target(plugin_target),
                mode="a",
            ).store

        for group_spec in schema:
            group_name = group_spec.group
            expected_time_count = global_expected.get(group_name)
            if expected_time_count is None:
                click.echo(f"group {group_name}: skipped; not present in expected time counts")
                continue
            if expected_time_count <= 0:
                raise click.ClickException(
                    f"Plugin '{plugin}' returned a non-positive count {expected_time_count} "
                    f"for group '{group_name}'. Expected positive integers only."
                )

            domain = WriteDomain(
                product=identity.product_name,
                category="zarr_schema_global",
                name=f"{group_name}:setup",
            )
            with chunk_manager.acquire_claim(
                product=identity.product_name,
                domain=domain,
                owner_id=f"preallocate:{group_name}:schema_global",
            ):
                coord_names = coord_names_by_group.get(group_name, frozenset({"y", "x", "channel"}))
                writer = RegionZarrWriter(plugin_target, store=zarr_store, coord_names=coord_names)
                root = writer._open_root()
                for arr_spec in group_spec.arrays:
                    if arr_spec.time_indexed:
                        effective_shape = (expected_time_count, *arr_spec.shape[1:])
                    else:
                        effective_shape = arr_spec.shape
                    array_path = f"{group_name}/{arr_spec.name}"
                    existing = _existing_array(root, array_path)
                    if existing is not None:
                        mismatches = _array_schema_mismatches(
                            existing=existing,
                            expected_shape=effective_shape,
                            expected_dtype=arr_spec.dtype,
                            expected_chunks=arr_spec.chunks,
                        )
                        if mismatches:
                            diff = "; ".join(mismatches)
                            raise click.ClickException(
                                f"Existing arrays mismatch the plan: array '{array_path}' has mismatches.\n"
                                f"Mismatch: {diff}\n"
                                "Either delete them or update the plan to match."
                            )
                        click.echo(f"array {array_path}: existing arrays match the plan; no-op")
                        continue

                    filters, serializer, compressors = derive_effective_codecs_for_spec(
                        arr_spec, template_config
                    )

                    writer.ensure_group(
                        array_path,
                        shape=effective_shape,
                        dtype=arr_spec.dtype,
                        fill_value=arr_spec.fill_value,
                        chunks=arr_spec.chunks,
                        attrs=arr_spec.attrs,
                        shards=arr_spec.shards,
                        dimension_names=arr_spec.dimension_names,
                        filters=filters,
                        serializer=serializer,
                        compressors=compressors,
                    )
                    click.echo(f"array {array_path}: created")

        summary = {
            "status": "ok",
            "plugin": plugin,
            "product": identity.product_name,
            "target": plugin_target,
            "groups": dict(global_expected),
        }
        click.echo(json.dumps(summary, indent=2))
        _preallocate_completed = True
    finally:
        # Terminate the preallocate run BEFORE closing the manager so the
        # terminal event reaches the WAL writer. Guarded by
        # ``_preallocate_run_created``: non-parallel plugins never call
        # ``ensure_slot_index_model``, so no run.json exists and any terminal
        # write would be spurious.
        try:
            if _preallocate_run_created:
                if _preallocate_completed:
                    with contextlib.suppress(Exception):
                        chunk_manager.record_run_terminal(
                            product=identity.product_name,
                            run_id="preallocate",
                            output_path=plugin_target,
                            output_format="zarr",
                            size=0,
                            meta={"run_id": "preallocate"},
                            status="complete",
                        )
                else:
                    with contextlib.suppress(Exception):
                        chunk_manager.record_run_failed(
                            product=identity.product_name,
                            run_id="preallocate",
                            output_path=plugin_target,
                            output_format="zarr",
                            size=0,
                            meta={"run_id": "preallocate"},
                            error="preallocate exited before completion",
                        )
        finally:
            with contextlib.suppress(Exception):
                chunk_manager.close()


def _existing_array(root: Any, array_path: str) -> Any | None:
    path_parts = [part for part in array_path.split("/") if part]
    current = root
    for part in path_parts[:-1]:
        if part not in current:
            return None
        current = current[part]
    arr_name = path_parts[-1]
    if arr_name not in current:
        return None
    return current[arr_name]


def _array_schema_mismatches(
    *,
    existing: Any,
    expected_shape: tuple[int, ...],
    expected_dtype: Any,
    expected_chunks: tuple[int, ...] | None,
) -> list[str]:
    mismatches: list[str] = []

    found_shape = tuple(existing.shape)
    if found_shape != tuple(expected_shape):
        mismatches.append(f"shape: expected {tuple(expected_shape)}, found {found_shape}")

    expected_dtype_str = str(np.dtype(expected_dtype))
    found_dtype_str = str(np.dtype(existing.dtype))
    if found_dtype_str != expected_dtype_str:
        mismatches.append(f"dtype: expected {expected_dtype_str}, found {found_dtype_str}")

    if expected_chunks is not None:
        found_chunks = tuple(existing.chunks) if existing.chunks is not None else None
        if found_chunks != tuple(expected_chunks):
            mismatches.append(f"chunks: expected {tuple(expected_chunks)}, found {found_chunks}")

    return mismatches


def _build_slots_plugin_context(*, target: str, product_name: str) -> PluginContext:
    """Build a minimal PluginContext for slot-planning hook calls."""
    ingest_ctx = IngestContext(
        source="",
        target=target,
        in_memory=True,
        output_format="zarr",
        options={"product_name": product_name},
        storage=None,
        run_id="zarr-slots",
    )
    runtime_ctx = RuntimeIngestContext.from_ingest_context(
        ingest_ctx,
        run_id="zarr-slots",
        temp_root=None,
        materializer=None,
    )
    return PluginContext(runtime_ctx)


def _query_slots_coverage(
    ctx: click.Context,
    *,
    target: str,
    product_name: str,
    storage_type: str,
    storage_driver: str,
    groups: list[str],
) -> dict[str, list[tuple[int, int]]]:
    """Read covered time ranges per group from ChunkManager."""
    coverage: dict[str, list[tuple[int, int]]] = {g: [] for g in groups}
    identity = resolve_product_identity(
        target, format="zarr", product_name=product_name, option_name="--target"
    )
    storage_config = get_storage_config(
        ctx,
        overrides={
            "storage_type": storage_type,
            "storage_driver": storage_driver,
        },
        cache=False,
    )
    driver = StorageDriverConfig.from_storage_config(storage_config)
    binding = StorageBinding(identity=identity, driver=driver)
    manager = ChunkManager(binding=binding)
    try:
        chunks = manager.list_chunks(
            product=product_name,
            chunk_type="span",
            include_replaced=False,
        )
    finally:
        manager.close()

    for chunk in chunks:
        meta = chunk.meta or {}
        group = meta.get("group")
        if group not in coverage:
            continue
        span_payload = chunk.record.get("span") if isinstance(chunk.record, dict) else None
        if not isinstance(span_payload, dict):
            continue
        time_index_ranges = span_payload.get("time_index_ranges") or []
        for entry in time_index_ranges:
            if not isinstance(entry, (list, tuple)) or len(entry) != 2:
                continue
            try:
                start = int(entry[0])
                end_inclusive = int(entry[1])
            except (TypeError, ValueError):
                continue
            coverage[group].append((start, end_inclusive + 1))

    return coverage


def _merge_intervals(intervals: list[tuple[int, int]]) -> list[tuple[int, int]]:
    if not intervals:
        return []
    sorted_iv = sorted(intervals)
    merged: list[tuple[int, int]] = [sorted_iv[0]]
    for start, end in sorted_iv[1:]:
        last_start, last_end = merged[-1]
        if start <= last_end:
            merged[-1] = (last_start, max(last_end, end))
        else:
            merged.append((start, end))
    return merged


def _complement_intervals(covered: list[tuple[int, int]], total: int) -> list[tuple[int, int]]:
    if total <= 0:
        return []
    result: list[tuple[int, int]] = []
    cursor = 0
    for start, end in covered:
        clamped_start = max(0, min(start, total))
        clamped_end = max(0, min(end, total))
        if clamped_start > cursor:
            result.append((cursor, clamped_start))
        cursor = max(cursor, clamped_end)
        if cursor >= total:
            break
    if cursor < total:
        result.append((cursor, total))
    return result


def _partition_remaining(remaining: list[tuple[int, int]], slot_size: int) -> list[tuple[int, int]]:
    """Partition each remaining interval into chunk-aligned slot_size chunks."""
    partitions: list[tuple[int, int]] = []
    for interval_start, interval_end in remaining:
        cursor = interval_start
        while cursor < interval_end:
            next_cursor = min(cursor + slot_size, interval_end)
            partitions.append((cursor, next_cursor))
            cursor = next_cursor
    return partitions


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
