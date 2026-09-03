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
import json
import uuid
from typing import Any, cast

import click

from firecube.cli._ctx import get_storage_config
from firecube.cli._errors import wrap_user_facing_errors
from firecube.cli._product import require_full_uri, resolve_product_identity
from firecube.cli._typed_options import TypedOptionsParam
from firecube.cli._uri_policy import (
    apply_smart_default,
    parse_product_uri,
    validate_uri_storage_coherence,
)
from firecube.cli.zarr._common import _configure_ingestor_for_cli, logger
from firecube.core import observability
from firecube.core.api import (
    ATTR_COORD_MANAGED,
    ATTR_PREALLOCATED,
)
from firecube.core.controlplane import ChunkManager, WriteDomain
from firecube.core.index_resolve import (
    ExtentUnknownError,
    resolve_index_spec,
)
from firecube.core.observability.metrics import TelemetryService, emit_index_ensured_full
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.session import StorageSession
from firecube.core.zarr._coord_chunks import resolve_coord_chunks
from firecube.core.zarr.coord_materialization import (
    array_schema_mismatches,
    axis_has_resolvable_extent,
    discover_regular_observed_coord_values,
    existing_array,
    materialize_irregular_coord_array,
    materialize_regular_coord_array,
)
from firecube.core.zarr.region_writer import RegionZarrWriter
from firecube.ingestor.errors import ConfigurationError
from firecube.ingestor.registry.loader import discover_ingestors
from firecube.ingestor.templates.direct_zarr import DirectZarrIngestor


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
    write_mode: str,
    input_data: str | None,
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

    from firecube.core.controlplane.manager import check_legacy_index_record
    from firecube.core.errors import LegacyIndexRecordError
    from firecube.core.index_spec import (
        AUTO,
        IrregularTimeAxis,
        RegularTimeAxis,
        effective_regular_time_policy,
    )
    from firecube.core.uris import storage_uri_from_target
    from firecube.ingestor.api import BaseIngestor, IndexedRegionStrategy

    require_full_uri(target, option_name="--target")
    parsed = parse_product_uri(target)
    storage_type = apply_smart_default(parsed, storage_type)
    validate_uri_storage_coherence(parse_product_uri(target), storage_type)
    _ = write_mode  # accepted for parity with `firecube ingest`; not used by preallocate
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
    preallocate_run_id = f"preallocate:{uuid.uuid4().hex[:16]}"
    # Run-lifecycle bookkeeping: ``ensure_resolved_index`` creates a
    # non-terminal ``run.json`` (status=started). The command MUST drive that
    # run to a terminal state (complete/failed) before returning so the next
    # slot-range ingest is not blocked by ``ResumeGuard`` on a stuck run.
    _preallocate_run_created = False
    _preallocate_run_registered = False
    _preallocate_can_record_failure = False
    _preallocate_claim_rejected = False
    _preallocate_claim_handle = None
    final_state = "failed"
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
            source=input_data or "",
            options=option,
            run_id="zarr-preallocate",
        )

        # Resolve the plugin's index and persist it through the
        # control-plane BEFORE any Zarr array/group is created. A failure here
        # MUST happen before any store mutation so partially-initialised stores
        # cannot leak into the target on error.
        try:
            ingestor._bind_index_at_startup(plugin_ctx)
            resolved_index = ingestor.resolved_index(plugin_ctx)
        except Exception as exc:
            raise click.ClickException(
                f"Plugin '{plugin}' failed to resolve index_spec: {exc}"
            ) from exc

        # Classify groups: bounded vs unbounded. No side effects here — the
        # audible warning and the hard-fail on all-unbounded live below the
        # dry-run early-return so ``--dry-run`` remains a silent preview and
        # never enforces the "at least one bounded axis" invariant (its job
        # is to preview, not to gate configuration).
        global_expected: dict[str, int] = {}
        skipped_unbounded: list[str] = []
        for group in resolved_index.groups:
            try:
                global_expected[group] = int(resolved_index.size(group))
            except ExtentUnknownError:
                skipped_unbounded.append(group)
                continue

        # Rebuild the resolved index with only the bounded groups so
        # downstream helpers that iterate every group's ``.size()``
        # (chiefly ``as_resolved_index_record`` -> ``canonical_index_payload``)
        # do not re-raise on the axes we just classified as unbounded. Guard
        # on ``global_expected`` because ``IndexSpec`` rejects empty groups —
        # all-unbounded is handled below (dry-run notice or live hard-fail).
        if skipped_unbounded and global_expected:
            _bounded_spec = resolved_index.filtered_spec(groups=global_expected)
            resolved_index = resolve_index_spec(
                _bounded_spec,
                time_dim_name=ingestor.time_dim_name,
                items=resolved_index.items,
            )

        if dry_run:
            if not global_expected:
                # All groups unbounded: nothing bounded to preview and
                # ``as_resolved_index_record`` would re-raise on the unbounded
                # groups. Emit a single-line notice and return success —
                # dry-run is a preview, not a policy gate.
                click.echo(
                    f"dry-run: no bounded groups to preview; all groups are "
                    f"unbounded ({skipped_unbounded!r})",
                    err=True,
                )
                return
            record = resolved_index.as_resolved_index_record(
                run_id="dry-run",
                recorded_at="dry-run",
            )
            click.echo(record.to_json_bytes().decode("utf-8"))
            _emit_preallocate_dry_run(
                ingestor=ingestor,
                plugin_ctx=plugin_ctx,
                resolved_index=resolved_index,
                slot_start=resolved_slot_start,
                slot_end=resolved_slot_end,
                slot_group=resolved_slot_group,
                has_input_data=input_data is not None,
            )
            return

        # Live-write path only: emit the audible warning per skipped unbounded
        # group, and hard-fail when every group is unbounded. Dry-run above
        # short-circuits before this so ``--dry-run`` stays a preview.
        for group in skipped_unbounded:
            click.echo(
                f"warning: skipping unbounded group {group!r}: "
                f"set end_date or slot_count to materialize this group's coord",
                err=True,
            )
        if not global_expected:
            raise ConfigurationError(
                "preallocate requires at least one bounded axis; "
                f"all groups are unbounded ({skipped_unbounded!r}): "
                "set end_date or slot_count on at least one group's time axis"
            )

        windows_by_group = _resolve_preallocate_windows(
            resolved_index=resolved_index,
            slot_start=resolved_slot_start,
            slot_end=resolved_slot_end,
            slot_group=resolved_slot_group,
        )

        try:
            check_legacy_index_record(
                chunk_manager,
                product=identity.product_name,
                plugin_name=plugin,
            )
        except LegacyIndexRecordError as exc:
            raise click.ClickException(str(exc)) from exc
        _preallocate_can_record_failure = True
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

        from firecube.ingestor.runtime.parallel_gate import warn_on_chunk_alignment

        warn_on_chunk_alignment(global_expected, schema)

        from firecube.core.zarr.planning import coord_chunk_sizes_by_group

        coord_chunk_sizes = coord_chunk_sizes_by_group(schema, resolved_index, windows_by_group)
        if coord_chunk_sizes:
            # Claim unconditionally whenever any coordinate array will be
            # materialized: the default full-extent invocation (no slot flags)
            # is the production driver's shape and must guard against
            # concurrent materializers exactly like a windowed one.
            try:
                _preallocate_claim_handle = chunk_manager.claim_coord_materialization_window(
                    product=identity.product_name,
                    run_id=preallocate_run_id,
                    output_path=plugin_target,
                    output_format="zarr",
                    windows_by_group=windows_by_group,
                    coord_chunk_sizes=coord_chunk_sizes,
                    slot_group=None,
                    meta={"kind": "preallocate"},
                )
            except BaseException:
                _preallocate_claim_rejected = True
                raise
            _preallocate_run_registered = True
            _preallocate_run_created = True

        if skipped_unbounded:
            # Mixed bounded/unbounded specs skip the full IndexSpec
            # record persistence. Persisting the rebuilt (bounded-only)
            # record would be a partial view of the plugin's true spec — the
            # unbounded groups belong to that spec too, and a persisted
            # bounded-only snapshot would silently claim otherwise on the
            # next resolved-index read. Per-group identity verification for
            # each bounded group still runs at ingest startup via
            # ``BaseIngestor._verify_per_group_identity_at_store``, which
            # reads the ``firecube_group_identity_hash`` attr materialized
            # below and compares it against the live plugin spec on every
            # bounded group.
            click.echo("resolved index: skipped (mixed spec has unbounded groups)")
        else:
            record = resolved_index.as_resolved_index_record(run_id=preallocate_run_id)
            _persisted_record, outcome = chunk_manager.ensure_resolved_index(
                product=identity.product_name,
                record=record,
                run_id=preallocate_run_id,
            )
            _preallocate_run_created = True
            telemetry = TelemetryService(
                observability.create_ingestion_telemetry(
                    plugin=plugin,
                    product=identity.product_name,
                    output_format="zarr",
                    write_mode=write_mode,
                    run_id=preallocate_run_id,
                ),
                plugin,
            )
            emit_index_ensured_full(
                chunk_manager,
                telemetry,
                product=identity.product_name,
                run_id=preallocate_run_id,
                record=_persisted_record,
                outcome=outcome,
                logger=logger,
            )
            click.echo(f"resolved index: {outcome}")

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
                group_axis = resolved_index.axis_for(group_name)
                for arr_spec in group_spec.arrays:
                    if arr_spec.time_indexed:
                        effective_shape = (expected_time_count, *arr_spec.shape[1:])
                    else:
                        effective_shape = arr_spec.shape
                    # For 1-D time-indexed coord specs with chunks=None, resolve
                    # dense chunks up front instead of letting Zarr auto-chunking
                    # degrade to one chunk per slot. 2-D calibration arrays keep
                    # their declared chunks (or None) unchanged.
                    if (
                        arr_spec.time_indexed
                        and arr_spec.chunks is None
                        and len(arr_spec.shape) == 1
                    ):
                        effective_chunks: tuple[int, ...] | None = resolve_coord_chunks(
                            arr_spec, expected_time_count
                        )
                    else:
                        effective_chunks = arr_spec.chunks
                    array_path = f"{group_name}/{arr_spec.name}"
                    if (
                        isinstance(group_axis, RegularTimeAxis)
                        and effective_regular_time_policy(group_axis) == "observed"
                        and arr_spec.name == group_axis.coordinate
                        and arr_spec.time_indexed
                        and len(arr_spec.shape) == 1
                    ):
                        continue
                    existing = existing_array(root, array_path)
                    if existing is not None:
                        mismatches = array_schema_mismatches(
                            existing=existing,
                            expected_shape=effective_shape,
                            expected_dtype=arr_spec.dtype,
                            expected_chunks=effective_chunks,
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
                        chunks=effective_chunks,
                        attrs=arr_spec.attrs,
                        shards=arr_spec.shards,
                        dimension_names=arr_spec.dimension_names,
                        filters=filters,
                        serializer=serializer,
                        compressors=compressors,
                    )
                    click.echo(f"array {array_path}: created")

                axis = group_axis
                if isinstance(axis, IrregularTimeAxis) and axis.values is not AUTO:
                    coord_spec = next(
                        (
                            spec
                            for spec in group_spec.arrays
                            if spec.name == axis.coordinate
                            and spec.time_indexed
                            and len(spec.shape) == 1
                        ),
                        None,
                    )
                    materialize_irregular_coord_array(
                        writer=writer,
                        root=root,
                        group_name=group_name,
                        axis=axis,
                        spec=coord_spec,
                        report=click.echo,
                    )
                elif isinstance(axis, RegularTimeAxis) and axis_has_resolvable_extent(
                    axis, resolved_index, group_name
                ):
                    coord_spec = next(
                        (
                            spec
                            for spec in group_spec.arrays
                            if spec.name == axis.coordinate
                            and spec.time_indexed
                            and len(spec.shape) == 1
                        ),
                        None,
                    )
                    materialize_regular_coord_array(
                        writer=writer,
                        root=root,
                        group_name=group_name,
                        axis=axis,
                        spec=coord_spec,
                        resolved_index=resolved_index,
                        ingestor=ingestor,
                        plugin_ctx=plugin_ctx,
                        slot_start=windows_by_group[group_name][0],
                        slot_end=windows_by_group[group_name][1],
                        has_input_data=input_data is not None,
                        input_data=input_data,
                        report=click.echo,
                    )

        summary = {
            "status": "ok",
            "plugin": plugin,
            "product": identity.product_name,
            "target": plugin_target,
            "groups": dict(global_expected),
        }
        click.echo(json.dumps(summary, indent=2))
        final_state = "complete"
    finally:
        # Terminate the preallocate run BEFORE closing the manager so the
        # terminal event reaches the WAL writer. Guarded by
        # ``_preallocate_run_created``: non-parallel plugins never call
        # ``ensure_resolved_index``, so no run.json exists and any terminal
        # write would be spurious.
        try:
            # Defer a success-path ``record_run_terminal`` failure until
            # AFTER the claim release runs. Re-raising at the failure site
            # would skip the release block below and leak a stale claim
            # file on the control-plane. The failure path
            # (``final_state != "complete"``) keeps its pre-existing
            # log-only behavior and never sets ``_terminal_exc``.
            _terminal_exc: BaseException | None = None
            try:
                if (
                    not _preallocate_run_created
                    and _preallocate_can_record_failure
                    and not _preallocate_claim_rejected
                ):
                    try:
                        chunk_manager.record_run_started(
                            product=identity.product_name,
                            run_id=preallocate_run_id,
                            output_path=plugin_target,
                            output_format="zarr",
                            size=0,
                            meta={"kind": "preallocate", "run_id": preallocate_run_id},
                        )
                        _preallocate_run_created = True
                    except Exception as exc:
                        logger.warning(
                            "could not record failure run for preallocate %s: %s",
                            preallocate_run_id,
                            exc,
                        )
                if _preallocate_run_created:
                    terminal_slot_range = (
                        (resolved_slot_start, resolved_slot_end)
                        if _preallocate_run_registered
                        and resolved_slot_start is not None
                        and resolved_slot_end is not None
                        else None
                    )
                    terminal_size = (
                        terminal_slot_range[1] - terminal_slot_range[0]
                        if terminal_slot_range is not None
                        else 0
                    )
                    try:
                        chunk_manager.record_run_terminal(
                            product=identity.product_name,
                            run_id=preallocate_run_id,
                            output_path=plugin_target,
                            output_format="zarr",
                            size=terminal_size,
                            meta={"kind": "preallocate", "run_id": preallocate_run_id},
                            status=final_state,
                            error=(
                                None
                                if final_state == "complete"
                                else "preallocate exited before completion"
                            ),
                            slot_range=terminal_slot_range,
                            # None: the run materializes every group, so the peer
                            # overlap check must weigh it against all groups
                            # (matches the record made at claim time).
                            slot_group=None,
                        )
                    except Exception as exc:
                        # A silently non-terminal run blocks all further ingest of
                        # the product until an operator abandons it. Never swallow.
                        logger.error(
                            "failed to record terminal state for preallocate run %s: %s; "
                            "the run stays non-terminal and blocks resume until "
                            "'firecube chunks runs abandon' clears it",
                            preallocate_run_id,
                            exc,
                        )
                        if final_state == "complete":
                            # Defer the re-raise so the inner finally below
                            # can release the claim first.
                            _terminal_exc = exc
            finally:
                # Release the claim regardless of terminal-record outcome.
                # This inner finally preserves claim release: without it, a deferred
                # ``_terminal_exc`` (or an in-flight exception from the try
                # body above) would skip release and leak a claim file.
                if _preallocate_claim_handle is not None:
                    try:
                        _preallocate_claim_handle.release()
                    except Exception as exc:
                        logger.error(
                            "failed to release materialization claim for run %s: %s; "
                            "a stale claim blocks future preallocates until "
                            "'firecube chunks claims clear' removes it",
                            preallocate_run_id,
                            exc,
                        )
                        # Propagate only on the success path AND when no
                        # terminal-record failure is already queued: the
                        # terminal-record exception is the root cause and
                        # must win over the release exception. The failure
                        # path keeps its pre-existing log-only behavior.
                        if final_state == "complete" and _terminal_exc is None:
                            raise
                    _preallocate_claim_handle = None
            if _terminal_exc is not None:
                raise _terminal_exc
        finally:
            with contextlib.suppress(Exception):
                chunk_manager.close()


def _resolve_preallocate_windows(
    *,
    resolved_index: Any,
    slot_start: int | None,
    slot_end: int | None,
    slot_group: str | None = None,
) -> dict[str, tuple[int, int]]:
    if slot_group is not None:
        available = tuple(resolved_index.groups)
        if slot_group not in available:
            raise click.UsageError(
                f"--slot-group {slot_group!r} is not declared by the plugin's "
                f"IndexSpec; available groups: {list(available)!r}"
            )
        axis = resolved_index.axis_for(slot_group)
        if not axis_has_resolvable_extent(axis, resolved_index, slot_group):
            raise click.UsageError(
                f"--slot-group {slot_group!r} has no resolvable extent "
                "(unbounded axis); set end_date or slot_count on the plugin's "
                "time axis to scope a preallocate window to this group"
            )

    windows: dict[str, tuple[int, int]] = {}
    for group in resolved_index.groups:
        axis = resolved_index.axis_for(group)
        if not axis_has_resolvable_extent(axis, resolved_index, group):
            # Serial-mode axes without a horizon have no window to
            # materialize; extent validation happens elsewhere.
            continue
        group_size = int(resolved_index.size(group))
        if slot_group is not None and group != slot_group:
            # Other groups get their full extent — the CLI window scopes
            # ONLY the named group so a co-scheduled parallel job for a
            # different group is not silently narrowed to a foreign window.
            windows[group] = (0, group_size)
            continue
        start = 0 if slot_start is None else slot_start
        end = group_size if slot_end is None else slot_end
        if start < 0:
            raise click.UsageError(f"--slot-start must be >= 0, got {start}")
        if end < 0:
            raise click.UsageError(f"--slot-end must be >= 0, got {end}")
        if start >= end:
            raise click.UsageError(f"--slot-start must be < --slot-end, got [{start}, {end})")
        if end > group_size:
            raise click.UsageError(
                f"--slot-end {end} exceeds resolved extent {group_size} for group {group!r}"
            )
        windows[group] = (start, end)
    return windows


def _emit_preallocate_dry_run(
    *,
    ingestor: Any,
    plugin_ctx: Any,
    resolved_index: Any,
    slot_start: int | None,
    slot_end: int | None,
    slot_group: str | None = None,
    has_input_data: bool,
) -> None:
    from firecube.core.index_spec import RegularTimeAxis, effective_regular_time_policy

    windows = _resolve_preallocate_windows(
        resolved_index=resolved_index,
        slot_start=slot_start,
        slot_end=slot_end,
        slot_group=slot_group,
    )
    for group_name, (start, end) in windows.items():
        axis = resolved_index.axis_for(group_name)
        if not isinstance(axis, RegularTimeAxis):
            click.echo(
                f"group {group_name}: window [{start}, {end}); non-regular axis; no dry-run coords",
                err=True,
            )
            continue
        policy = effective_regular_time_policy(axis)
        if policy == "grid":
            first = resolved_index.coordinate(group_name, start)
            last = resolved_index.coordinate(group_name, end - 1)
            click.echo(
                f"group {group_name}: window [{start}, {end}); policy=grid; "
                f"items_in_window={end - start}; first={first}; last={last}; "
                f"would write nominal grid values and stamp {ATTR_PREALLOCATED}",
                err=True,
            )
            continue
        if not has_input_data:
            click.echo(
                f"group {group_name}: window [{start}, {end}); policy=observed; "
                "no --input-data supplied; would leave coord values as NaT",
                err=True,
            )
            continue
        observed = discover_regular_observed_coord_values(
            ingestor=ingestor,
            plugin_ctx=plugin_ctx,
            resolved_index=resolved_index,
            group_name=group_name,
            slot_start=start,
            slot_end=end,
        )
        slots = sorted(observed)
        first = observed[slots[0]] if slots else None
        last = observed[slots[-1]] if slots else None
        click.echo(
            f"group {group_name}: window [{start}, {end}); policy=observed; "
            f"items_in_window={len(slots)}; first={first}; last={last}; "
            f"would write observed coord values at their slot indices and stamp {ATTR_COORD_MANAGED}",
            err=True,
        )
