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

"""Preallocate a product: arrays, time coordinates and engine-owned arrays.

The host is a plugin that declares an index spec and a Zarr schema (`PreallocateHost`).
`preallocate_product` creates the declared arrays at their full slot count before
slot-window pods write. Configure the plugin with `configure_ingestor` first.
Progress lines go to the caller's `report` and `warn` callables.

Slot windows are half-open `[start, end)`: `start` is the first slot, `end` the
slot after the last.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from firecube.core import observability
from firecube.core.controlplane import ChunkManager, WriteDomain
from firecube.core.errors import Message, Operation, Param, Placeholder
from firecube.core.index_resolve import (
    ExtentUnknownError,
    resolve_index_spec,
)
from firecube.core.observability.metrics import TelemetryService, emit_index_ensured_full
from firecube.core.storage.session import StorageSession
from firecube.core.time.calendar import is_gregorian_axis
from firecube.core.zarr._coord_common import (
    array_schema_mismatches,
    existing_array,
    open_existing_root_read_only,
)
from firecube.core.zarr.coord_materialization import (
    axis_has_resolvable_extent,
    discover_regular_observed_coord_values,
    materialize_irregular_coord_array,
    materialize_regular_coord_array,
)
from firecube.core.zarr.coord_rematerialization import (
    RegularCoordTarget,
    check_irregular_coord_rematerialization,
    check_regular_coord_rematerialization,
)
from firecube.core.zarr.guard._layout import resolve_effective_layout, warn_default_layout
from firecube.core.zarr.guard.engine_owned import (
    collect_engine_owned_values,
    engine_owned_adoption_error,
    engine_owned_creation_attrs,
    engine_owned_names_by_group,
    exception_text,
    is_engine_owned_reserved,
    is_engine_owned_spec,
    materialize_engine_owned_array,
)
from firecube.core.zarr.guard.metadata_authority import (
    HeldClaim,
    MetadataAuthority,
    adopt_held_claim,
    ensure_group,
    hold_authority,
)
from firecube.core.zarr.region_writer import RegionZarrWriter
from firecube.ingestor.errors import ConfigurationError, SchemaDriftError

if TYPE_CHECKING:
    from firecube.core.config import StorageConfig
    from firecube.ingestor.contracts.interfaces import PreallocateHost
    from firecube.ingestor.types.context import PluginContext

_log = logging.getLogger(__name__)

# The slot-window parameters a refusal names.
_SLOT_START = Param("slot_start")
_SLOT_END = Param("slot_end")
_SLOT_GROUP = Param("slot_group")


class SlotWindowError(ConfigurationError):
    """The requested slot window or slot group does not fit the plugin's resolved index."""


@dataclass(frozen=True)
class PreallocateDryRunGroup:
    """What a dry run would write into one bounded group's time coordinate.

    Attributes:
        group: The group name.
        window: The half-open slot window `[start, end)` the run would materialize.
        policy: `"grid"` or `"observed"` for a regular axis; `None` for any other axis.
        items_in_window: The number of slots with a coordinate value; `None` when the
            preview reads no values.
        first: The first coordinate value in the window, or `None`.
        last: The last coordinate value in the window, or `None`.
        calendar: The axis calendar when its coordinate is stored encoded (not
            Gregorian-like); `None` otherwise. `first` and `last` are then the
            encoded numbers, to be decoded with `units` and `calendar`.
        units: The CF units the encoded coordinate is stored in; `None` unless
            `calendar` is set.
        window_applied: `False` when the run writes the whole calendar coordinate
            although the window is narrower; `True` otherwise.
    """

    group: str
    window: tuple[int, int]
    policy: Literal["grid", "observed"] | None
    items_in_window: int | None = None
    first: Any = None
    last: Any = None
    calendar: str | None = None
    units: str | None = None
    window_applied: bool = True


def _ignore_line(line: str) -> None:
    """Discard a progress line."""


def preallocate_product(
    *,
    ingestor: PreallocateHost,
    plugin_ctx: PluginContext,
    plugin_name: str,
    chunk_manager: ChunkManager,
    session: StorageSession,
    storage_config: StorageConfig | None,
    run_id: str,
    write_mode: str,
    input_data: str | tuple[str, ...] | None = None,
    slot_start: int | None = None,
    slot_end: int | None = None,
    slot_group: str | None = None,
    dry_run: bool = False,
    report: Callable[[str], None] = _ignore_line,
    warn: Callable[[str], None] = _ignore_line,
    on_dry_run_group: Callable[[PreallocateDryRunGroup], None] | None = None,
    logger: logging.Logger = _log,
) -> None:
    """Create every declared array of a product at its full slot count.

    The call is idempotent: arrays that already match the plan stay as they are.
    It resolves the plugin's index and persists it in the control plane. Under a
    materialization claim it creates each bounded group's arrays, then fills its
    time coordinate and engine-owned arrays. It records the run as complete or
    failed, then releases the claim. A dry run only resolves and previews; it
    writes nothing.

    Args:
        ingestor: The plugin, a `PreallocateHost` constructed with `chunk_manager` and
            configured by `configure_ingestor`.
        plugin_ctx: The context `configure_ingestor` returned for `ingestor`.
        plugin_name: The plugin's registered name, used in messages, records and telemetry.
        chunk_manager: The control plane of the target product. The caller closes it.
        session: The storage session bound to the target product.
        storage_config: Storage settings. When given, arrays are written through
            `session`'s store; `None` lets the writer open the target URI itself.
        run_id: The run id recorded in the control plane.
        write_mode: The write mode recorded in telemetry.
        input_data: Raw plugin input, one source or several; `None` when there is none.
            Engine-owned arrays and observed coordinates need it.
        slot_start: First slot of the window. `None` starts at slot 0.
        slot_end: Slot after the last of the window. `None` ends at the group's extent.
        slot_group: The one group the window applies to. Other groups get their
            full extent. `None` applies the window to every group.
        dry_run: Preview without writing anything.
        report: Receives each progress line and the two JSON documents.
        warn: Receives each warning line.
        on_dry_run_group: Receives one preview per bounded group in a dry run.
        logger: Receives the run-record and claim failures.

    Raises:
        SlotWindowError: The slot window or slot group does not fit the resolved index.
        ConfigurationError: The plugin's index, schema or template configuration is
            unusable, or engine-owned arrays have no input data.
        SchemaDriftError: An existing array disagrees with the plan or its ownership
            marker, or the target holds a legacy slot-index record without a
            resolved-index record.
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
    from firecube.ingestor.api import IndexedRegionStrategy

    identity = session.product
    plugin_target = identity.product_uri.to_str()
    # Run-lifecycle bookkeeping: ``ensure_resolved_index`` creates a
    # non-terminal ``run.json`` (status=started). The operation MUST drive that
    # run to a terminal state (complete/failed) before returning so the next
    # slot-range ingest is not blocked by ``ResumeGuard`` on a stuck run.
    _preallocate_run_created = False
    _preallocate_run_registered = False
    _preallocate_can_record_failure = False
    _preallocate_claim_rejected = False
    _preallocate_claim_handle = None
    final_state = "failed"
    try:
        # Resolve the plugin's index and persist it through the
        # control-plane BEFORE any Zarr array/group is created. A failure here
        # MUST happen before any store mutation so partially-initialised stores
        # cannot leak into the target on error.
        try:
            ingestor.bind_index_at_startup(plugin_ctx)
            resolved_index = ingestor.resolved_index(plugin_ctx)
        except Exception as exc:
            raise ConfigurationError(
                Message(
                    "Plugin '{plugin}' failed to resolve index_spec: {error}",
                    plugin=plugin_name,
                    error=exception_text(exc),
                    product=identity.product_name,
                    product_uri=plugin_target,
                    run_id=run_id,
                )
            ) from exc

        # Classify groups: bounded vs unbounded. No side effects here — the
        # audible warning and the hard-fail on all-unbounded live below the
        # dry-run early-return so a dry run remains a silent preview and
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
                warn(
                    f"dry-run: no bounded groups to preview; all groups are "
                    f"unbounded ({skipped_unbounded!r})"
                )
                return
            record = resolved_index.as_resolved_index_record(
                run_id="dry-run",
                recorded_at="dry-run",
            )
            report(record.to_json_bytes().decode("utf-8"))
            _preview_dry_run(
                ingestor=ingestor,
                plugin_ctx=plugin_ctx,
                resolved_index=resolved_index,
                slot_start=slot_start,
                slot_end=slot_end,
                slot_group=slot_group,
                has_input_data=input_data is not None,
                on_group=on_dry_run_group,
            )
            return

        # Live-write path only: emit the audible warning per skipped unbounded
        # group, and hard-fail when every group is unbounded. Dry-run above
        # short-circuits before this so a dry run stays a preview.
        for group in skipped_unbounded:
            warn(
                f"warning: skipping unbounded group {group!r}: "
                f"set end_date or slot_count to materialize this group's coord"
            )
        if not global_expected:
            raise ConfigurationError(
                "preallocate requires at least one bounded axis; "
                f"all groups are unbounded ({skipped_unbounded!r}): "
                "set end_date or slot_count on at least one group's time axis"
            )

        windows_by_group = _resolve_preallocate_windows(
            resolved_index=resolved_index,
            slot_start=slot_start,
            slot_end=slot_end,
            slot_group=slot_group,
        )

        try:
            check_legacy_index_record(
                chunk_manager,
                product=identity.product_name,
                plugin_name=plugin_name,
            )
        except LegacyIndexRecordError as exc:
            raise SchemaDriftError(str(exc) if exc.message is None else exc.message) from exc
        _preallocate_can_record_failure = True
        schema = ingestor.zarr_schema(plugin_ctx)
        all_specs = [array for group in schema for array in group.arrays]
        from firecube.ingestor.templates.config import (
            ZarrTemplateConfig,
            validate_zarr_specs_against_template,
        )

        template_config = ingestor.template_config
        if not isinstance(template_config, ZarrTemplateConfig):
            raise ConfigurationError(
                f"Plugin '{plugin_name}' did not resolve a Zarr template configuration."
            )
        validate_zarr_specs_against_template(all_specs, template_config)

        from firecube.ingestor.runtime.parallel_gate import (
            validate_global_expected_subset_of_schema,
        )

        validate_global_expected_subset_of_schema(global_expected, schema)

        from firecube.ingestor.runtime.parallel_gate import warn_on_chunk_alignment

        warn_on_chunk_alignment(global_expected, schema)

        engine_owned_values = _discover_engine_owned_values(
            ingestor=ingestor,
            plugin_ctx=plugin_ctx,
            resolved_index=resolved_index,
            schema=schema,
            global_expected=global_expected,
            input_data=input_data,
        )

        from firecube.core.zarr.planning import (
            coord_chunk_sizes_by_group,
            engine_owned_extents_by_group,
        )

        coord_chunk_sizes = coord_chunk_sizes_by_group(schema, resolved_index, windows_by_group)
        whole_extent_groups = engine_owned_extents_by_group(schema, global_expected)
        if coord_chunk_sizes or whole_extent_groups:
            # Claim unconditionally whenever any coordinate array will be
            # materialized: the default full-extent invocation (no slot window)
            # is the production driver's shape and must guard against
            # concurrent materializers exactly like a windowed one.
            try:
                _preallocate_claim_handle = chunk_manager.claim_coord_materialization_window(
                    product=identity.product_name,
                    run_id=run_id,
                    output_path=plugin_target,
                    output_format="zarr",
                    windows_by_group=windows_by_group,
                    coord_chunk_sizes=coord_chunk_sizes,
                    slot_group=None,
                    meta={"kind": "preallocate"},
                    whole_extent_groups=whole_extent_groups,
                )
            except BaseException:
                _preallocate_claim_rejected = True
                raise
            _preallocate_run_registered = True
            _preallocate_run_created = True

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

        store_writer = RegionZarrWriter(plugin_target, store=zarr_store)
        # Refuse a coordinate relabel in any group before the first store write,
        # and before ensure_resolved_index publishes the incoming index (its
        # current.json and the root mirror), so a refusal leaves the store as
        # it was and a retry with the original axis still succeeds.
        regular_coord_targets = _preflight_coordinates(
            schema=schema,
            resolved_index=resolved_index,
            global_expected=global_expected,
            root=open_existing_root_read_only(store_writer),
        )

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
            report("resolved index: skipped (mixed spec has unbounded groups)")
        else:
            record = resolved_index.as_resolved_index_record(run_id=run_id)
            _persisted_record, outcome = chunk_manager.ensure_resolved_index(
                product=identity.product_name,
                record=record,
                run_id=run_id,
            )
            _preallocate_run_created = True
            telemetry = TelemetryService(
                observability.create_ingestion_telemetry(
                    plugin=plugin_name,
                    product=identity.product_name,
                    output_format="zarr",
                    write_mode=write_mode,
                    run_id=run_id,
                ),
                plugin_name,
            )
            emit_index_ensured_full(
                chunk_manager,
                telemetry,
                product=identity.product_name,
                run_id=run_id,
                record=_persisted_record,
                outcome=outcome,
                logger=logger,
            )
            report(f"resolved index: {outcome}")

        _ensure_ancestors(store_writer, _preallocate_claim_handle, global_expected)
        for group_spec in schema:
            group_name = group_spec.group
            expected_time_count = global_expected.get(group_name)
            if expected_time_count is None:
                report(f"group {group_name}: skipped; not present in expected time counts")
                continue
            if expected_time_count <= 0:
                raise ConfigurationError(
                    f"Plugin '{plugin_name}' returned a non-positive count {expected_time_count} "
                    f"for group '{group_name}'. Expected positive integers only."
                )

            domain = WriteDomain(
                product=identity.product_name,
                category="zarr_schema_global",
                name=f"{group_name}:setup",
            )
            owner = f"{run_id}:{group_name}:schema_global"
            # The group's metadata (arrays, coordinates, markers) changes only under the
            # setup claim's authority; it covers the group, not the root (made above).
            with hold_authority(
                store_writer.metadata_scope(group_name),
                claim=chunk_manager.acquire_claim(
                    product=identity.product_name, domain=domain, owner_id=owner
                ),
                claim_domain=domain.identifier,
                owner_id=owner,
            ) as authority:
                coord_names = coord_names_by_group.get(group_name, frozenset({"y", "x", "channel"}))
                writer = RegionZarrWriter(
                    plugin_target,
                    store=authority.scope.backing,
                    coord_names=coord_names,
                    authority=authority,
                )
                root = writer._open_root()
                group_axis = resolved_index.axis_for(group_name)
                for arr_spec in group_spec.arrays:
                    if arr_spec.time_indexed:
                        effective_shape = (expected_time_count, *arr_spec.shape[1:])
                    else:
                        effective_shape = arr_spec.shape
                    engine_owned = is_engine_owned_spec(arr_spec)
                    layout = resolve_effective_layout(
                        arr_spec,
                        coord_name=ingestor.time_dim_name,
                        time_count=expected_time_count,
                        engine_owned=engine_owned,
                    )
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
                    _refuse_ownership_change(existing, engine_owned, array_path)
                    if existing is not None:
                        mismatches = array_schema_mismatches(
                            existing=existing,
                            expected_shape=effective_shape,
                            expected_dtype=arr_spec.dtype,
                            expected_chunks=_stored_chunks_to_match(arr_spec, layout, engine_owned),
                        )
                        if mismatches:
                            diff = "; ".join(mismatches)
                            raise SchemaDriftError(
                                f"Existing arrays mismatch the plan: array '{array_path}' has mismatches.\n"
                                f"Mismatch: {diff}\n"
                                "Either delete them or update the plan to match."
                            )
                        report(f"array {array_path}: existing arrays match the plan; no-op")
                        continue

                    _create_array(
                        writer,
                        arr_spec,
                        array_path,
                        effective_shape,
                        layout,
                        engine_owned,
                        template_config,
                        report,
                    )

                axis = group_axis
                if isinstance(axis, IrregularTimeAxis) and axis.values is not AUTO:
                    materialize_irregular_coord_array(
                        writer=writer,
                        root=root,
                        group_name=group_name,
                        axis=axis,
                        spec=_coord_spec(group_spec, axis),
                        report=report,
                    )
                elif isinstance(axis, RegularTimeAxis) and axis_has_resolvable_extent(
                    axis, resolved_index, group_name
                ):
                    materialize_regular_coord_array(
                        writer=writer,
                        root=root,
                        group_name=group_name,
                        axis=axis,
                        spec=_coord_spec(group_spec, axis),
                        resolved_index=resolved_index,
                        ingestor=ingestor,
                        plugin_ctx=plugin_ctx,
                        slot_start=windows_by_group[group_name][0],
                        slot_end=windows_by_group[group_name][1],
                        has_input_data=input_data is not None,
                        input_data=(
                            ", ".join(input_data) if isinstance(input_data, tuple) else input_data
                        ),
                        report=report,
                        target=regular_coord_targets[group_name],
                    )

                _materialize_engine_owned_arrays(
                    root, authority, group_name, engine_owned_values, report
                )

        summary = {
            "status": "ok",
            "plugin": plugin_name,
            "product": identity.product_name,
            "target": plugin_target,
            "groups": dict(global_expected),
        }
        report(json.dumps(summary, indent=2))
        final_state = "complete"
    finally:
        # Terminate the preallocate run so the terminal event reaches the WAL
        # writer before the caller closes the manager. Guarded by
        # ``_preallocate_run_created``: non-parallel plugins never call
        # ``ensure_resolved_index``, so no run.json exists and any terminal
        # write would be spurious.
        #
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
                        run_id=run_id,
                        output_path=plugin_target,
                        output_format="zarr",
                        size=0,
                        meta={"kind": "preallocate", "run_id": run_id},
                    )
                    _preallocate_run_created = True
                except Exception as exc:
                    logger.warning(
                        "could not record failure run for preallocate %s: %s",
                        run_id,
                        exc,
                    )
            if _preallocate_run_created:
                terminal_slot_range = (
                    (slot_start, slot_end)
                    if _preallocate_run_registered
                    and slot_start is not None
                    and slot_end is not None
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
                        run_id=run_id,
                        output_path=plugin_target,
                        output_format="zarr",
                        size=terminal_size,
                        meta={"kind": "preallocate", "run_id": run_id},
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
                    # The log names the library operation; `warn` gets the same
                    # message, which the CLI prints with its command.
                    product_uri = chunk_manager.get_product_root(identity.product_name)
                    terminal_failure = Message(
                        "failed to record terminal state for preallocate run {run_id}: {error}; "
                        "the run stays non-terminal and blocks resume until it is "
                        "abandoned:\n  {abandon}",
                        run_id=run_id,
                        error=exception_text(exc),
                        abandon=Operation(
                            "abandon_run",
                            product_name=product_uri,
                            run_id=run_id,
                            reason=Placeholder('"<reason>"'),
                        ),
                        product=identity.product_name,
                        product_uri=product_uri,
                    )
                    logger.error(terminal_failure)
                    warn(terminal_failure)
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
                    product_uri = chunk_manager.get_product_root(identity.product_name)
                    claim_domain = _preallocate_claim_handle.info.domain
                    release_failure = Message(
                        "failed to release materialization claim for run {run_id}: {error}; "
                        "a stale claim blocks future preallocates until it is "
                        "cleared:\n  {clear}",
                        run_id=run_id,
                        error=exception_text(exc),
                        clear=Operation(
                            "clear_claim",
                            product_name=product_uri,
                            domain_id=claim_domain,
                            force=True,  # the claim is fresh until its heartbeat times out
                        ),
                        product=identity.product_name,
                        product_uri=product_uri,
                        claim_domain=claim_domain,
                    )
                    logger.error(release_failure)
                    warn(release_failure)
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


def _refuse_ownership_change(existing: Any, engine_owned: bool, array_path: str) -> None:
    """Refuse a declaration that disagrees with the stored array's ownership marker."""
    if existing is not None and not engine_owned and is_engine_owned_reserved(existing):
        raise SchemaDriftError(
            f"array {array_path!r} is engine-owned in the target but no "
            "longer declared engine_owned=True; ownership changes are "
            "refused. Restore the declaration, or rebuild the array."
        )
    if existing is not None and engine_owned and not is_engine_owned_reserved(existing):
        # Ownership is never inferred from contents: an unmarked
        # array is pod-written even when it holds only fill values.
        raise engine_owned_adoption_error(array_path)


def _stored_chunks_to_match(arr_spec: Any, layout: Any, engine_owned: bool) -> Any:
    """Chunks an existing array must have to match the plan, or ``None`` for any."""
    # A stored layout is never judged against the creation
    # default: an existing undeclared array keeps its chunks.
    # Nor is an undeclared engine-owned one: only preallocation
    # writes it, under the materialization claim, so any
    # stored layout is safe.
    undeclared_owned = engine_owned and arr_spec.chunks is None
    # Likewise a pod-written array declaring shards and no
    # chunks: its inner chunks are the creation rule's, not a
    # contract, and earlier releases chose others.
    shards_only = layout.source == "declared" and arr_spec.time_indexed and arr_spec.chunks is None
    return (
        None
        if layout.source in ("default", "zarr") or undeclared_owned or shards_only
        else layout.chunks
    )


def _create_array(
    writer: Any,
    arr_spec: Any,
    array_path: str,
    effective_shape: tuple[int, ...],
    layout: Any,
    engine_owned: bool,
    template_config: Any,
    report: Callable[[str], None],
) -> None:
    """Create one missing array with its effective layout, codecs and ownership marker."""
    from firecube.ingestor.runtime.zarr.write import derive_effective_codecs_for_spec

    filters, serializer, compressors = derive_effective_codecs_for_spec(arr_spec, template_config)

    writer.ensure_group(
        array_path,
        shape=effective_shape,
        dtype=arr_spec.dtype,
        fill_value=arr_spec.fill_value,
        chunks=layout.chunks,
        attrs=arr_spec.attrs,
        shards=layout.shards,
        dimension_names=arr_spec.dimension_names,
        filters=filters,
        serializer=serializer,
        compressors=compressors,
        # Marked in the write that creates it: no crash leaves
        # an unmarked engine-owned array behind.
        engine_attrs=engine_owned_creation_attrs() if engine_owned else None,
    )
    if layout.is_default:
        warn_default_layout(array_path, layout)
    report(f"array {array_path}: created")


def _ensure_ancestors(writer: RegionZarrWriter, claim: Any | None, groups: Iterable[str]) -> None:
    """Create the root and every missing ancestor of *groups* under a store-wide authority.

    A group's setup authority covers that group only and cannot create the
    root or a parent group (``parent`` of ``parent/data``). The store-wide one
    names the materialization claim this run holds (``claim``, a held claim
    handle); without one it excludes this process's threads only, as the
    writer's default authority does.
    """
    scope = writer.metadata_scope()
    authority_cm = (
        adopt_held_claim(
            scope, HeldClaim(domain_id=claim.info.domain, owner_id=claim.info.owner_id)
        )
        if claim is not None
        else hold_authority(scope, claim=None)
    )
    with authority_cm as authority:
        assert scope.backing is not None  # metadata_scope binds the writer's store
        ensure_group(authority, scope.backing, "")
        for group in groups:  # ensure_group creates each missing level, root first
            ensure_group(authority, scope.backing, group.strip("/").rpartition("/")[0])


def _coord_spec(group_spec: Any, axis: Any) -> Any | None:
    """The spec of the group's rank-1 time-indexed array named after the axis coordinate."""
    return next(
        (
            spec
            for spec in group_spec.arrays
            if spec.name == axis.coordinate and spec.time_indexed and len(spec.shape) == 1
        ),
        None,
    )


def _preflight_coordinates(
    *,
    schema: Any,
    resolved_index: Any,
    global_expected: dict[str, int],
    root: Any | None,
) -> dict[str, RegularCoordTarget]:
    """Run each group's coordinate refusals before any group is written.

    Visits the groups the materialization loop materializes, with the same axis
    dispatch, on a read-only live *root* (`None` when the store does not exist).
    It reads only 1-D time coordinate arrays and their attrs and writes nothing.

    Returns:
        Each regular group's `RegularCoordTarget`, for the loop to reuse.
    """
    from firecube.core.index_spec import AUTO, IrregularTimeAxis, RegularTimeAxis

    targets: dict[str, RegularCoordTarget] = {}
    for group_spec in schema:
        group_name = group_spec.group
        if global_expected.get(group_name, 0) <= 0:
            continue  # the loop skips or refuses it
        axis = resolved_index.axis_for(group_name)
        if isinstance(axis, IrregularTimeAxis) and axis.values is not AUTO:
            check_irregular_coord_rematerialization(
                root=root,
                group_name=group_name,
                axis=axis,
                spec=_coord_spec(group_spec, axis),
            )
        elif isinstance(axis, RegularTimeAxis) and axis_has_resolvable_extent(
            axis, resolved_index, group_name
        ):
            targets[group_name] = check_regular_coord_rematerialization(
                root=root,
                group_name=group_name,
                axis=axis,
                spec=_coord_spec(group_spec, axis),
                resolved_index=resolved_index,
            )
    return targets


def _materialize_engine_owned_arrays(
    root: Any,
    authority: MetadataAuthority,
    group_name: str,
    engine_owned_values: dict[str, dict[str, dict[int, Any]]],
    report: Callable[[str], None],
) -> None:
    """Write each engine-owned array of *group_name* whole, its markers under *authority*."""
    for array_name, values_by_slot in engine_owned_values.get(group_name, {}).items():
        array_path = f"{group_name}/{array_name}"
        materialize_engine_owned_array(
            arr=root[array_path],
            root=root,
            authority=authority,
            array_path=array_path,
            values_by_slot=values_by_slot,
            report=report,
        )


def _discover_engine_owned_values(
    *,
    ingestor: Any,
    plugin_ctx: Any,
    resolved_index: Any,
    schema: Any,
    global_expected: dict[str, int],
    input_data: str | tuple[str, ...] | None,
) -> dict[str, dict[str, dict[int, Any]]]:
    """Validate the engine-owned declarations and collect their values from every item.

    Runs BEFORE the claim and any array creation, so bad plugin input leaves the
    store untouched. Returns ``{}`` when the schema declares no engine-owned array. Every axis
    kind is inspected: the values come only from ``inspect_item``, never from
    the axis, so input data is required.
    """
    owned = engine_owned_names_by_group(schema)
    if not owned:
        return {}
    paths = [f"{group}/{name}" for group, names in owned.items() for name in names]
    for group, names in owned.items():
        if group not in global_expected:
            raise ConfigurationError(
                f"engine-owned arrays {[f'{group}/{n}' for n in names]} need a bounded "
                f"time axis for group {group!r}: set end_date or slot_count in index_spec"
            )
        coord_names = {ingestor.time_dim_name}
        axis_coord = getattr(resolved_index.axis_for(group), "coordinate", None)
        if isinstance(axis_coord, str):
            coord_names.add(axis_coord)
        clashing = sorted(set(names) & coord_names)
        if clashing:
            raise ConfigurationError(
                f"group {group!r}: the time coordinate {clashing} cannot be engine_owned; "
                "the engine already materializes coordinates"
            )
    if input_data is None:
        raise ConfigurationError(
            Message(
                "engine-owned arrays {paths} take their values from inspect_item on every "
                "source item; pass {input_data} so preallocate can inspect them",
                paths=paths,
                input_data=Param("input_data"),
            )
        )
    dtypes = {
        group_spec.group: {
            arr_spec.name: arr_spec.dtype
            for arr_spec in group_spec.arrays
            if arr_spec.name in owned.get(group_spec.group, ())
        }
        for group_spec in schema
        if group_spec.group in owned
    }
    return collect_engine_owned_values(
        ingestor=ingestor,
        plugin_ctx=plugin_ctx,
        resolved_index=resolved_index,
        dtypes=dtypes,
        extents={group: global_expected[group] for group in dtypes},
    )


def _resolve_preallocate_windows(
    *,
    resolved_index: Any,
    slot_start: int | None,
    slot_end: int | None,
    slot_group: str | None = None,
) -> dict[str, tuple[int, int]]:
    """Return the half-open slot window of every bounded group, or raise `SlotWindowError`."""
    from firecube.core.index_spec import RegularTimeAxis

    if slot_group is not None:
        available = tuple(resolved_index.groups)
        if slot_group not in available:
            raise SlotWindowError(
                Message(
                    "{slot_group} {group!r} is not declared by the plugin's "
                    "IndexSpec; available groups: {available!r}",
                    slot_group=_SLOT_GROUP,
                    group=slot_group,
                    available=list(available),
                )
            )
        axis = resolved_index.axis_for(slot_group)
        if not axis_has_resolvable_extent(axis, resolved_index, slot_group):
            raise SlotWindowError(
                Message(
                    "{slot_group} {group!r} has no resolvable extent "
                    "(unbounded axis); set end_date or slot_count on the plugin's "
                    "time axis to scope a preallocate window to this group",
                    slot_group=_SLOT_GROUP,
                    group=slot_group,
                )
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
            # Other groups get their full extent — the window scopes
            # ONLY the named group so a co-scheduled parallel job for a
            # different group is not silently narrowed to a foreign window.
            windows[group] = (0, group_size)
            continue
        start = 0 if slot_start is None else slot_start
        end = group_size if slot_end is None else slot_end
        if start < 0:
            raise SlotWindowError(
                Message(
                    "{slot_start} must be >= 0, got {start}",
                    slot_start=_SLOT_START,
                    start=start,
                    group=group,
                    slot_window=(start, end),
                )
            )
        if end < 0:
            raise SlotWindowError(
                Message(
                    "{slot_end} must be >= 0, got {end}",
                    slot_end=_SLOT_END,
                    end=end,
                    group=group,
                    slot_window=(start, end),
                )
            )
        # A calendar coordinate is always written whole, so an empty data window
        # is a valid "materialize the calendar, no data slots" call there. Every
        # other axis needs start < end: there is nothing to preallocate.
        is_calendar_axis = isinstance(axis, RegularTimeAxis) and not is_gregorian_axis(axis)
        if start > end or (start == end and not is_calendar_axis):
            raise SlotWindowError(
                Message(
                    "{slot_start} must be < {slot_end}, got [{slot_window[0]}, {slot_window[1]})",
                    slot_start=_SLOT_START,
                    slot_end=_SLOT_END,
                    slot_window=(start, end),
                    group=group,
                )
            )
        if end > group_size:
            raise SlotWindowError(
                Message(
                    "{slot_end} {end} exceeds resolved extent {extent} for group {group!r}",
                    slot_end=_SLOT_END,
                    end=end,
                    extent=group_size,
                    group=group,
                    slot_window=(start, end),
                )
            )
        windows[group] = (start, end)
    return windows


def _preview_dry_run(
    *,
    ingestor: Any,
    plugin_ctx: Any,
    resolved_index: Any,
    slot_start: int | None,
    slot_end: int | None,
    slot_group: str | None = None,
    has_input_data: bool,
    on_group: Callable[[PreallocateDryRunGroup], None] | None,
) -> None:
    """Hand *on_group* one preview per bounded group, in window order."""
    from firecube.core.index_spec import RegularTimeAxis, effective_regular_time_policy

    windows = _resolve_preallocate_windows(
        resolved_index=resolved_index,
        slot_start=slot_start,
        slot_end=slot_end,
        slot_group=slot_group,
    )
    emit = on_group if on_group is not None else _ignore_preview
    for group_name, (start, end) in windows.items():
        axis = resolved_index.axis_for(group_name)
        if not isinstance(axis, RegularTimeAxis):
            emit(PreallocateDryRunGroup(group=group_name, window=(start, end), policy=None))
            continue
        policy = effective_regular_time_policy(axis)
        if policy == "grid":
            is_calendar_axis = not is_gregorian_axis(axis)
            # An empty window (calendar axes only) has no first or last value.
            first = resolved_index.coordinate(group_name, start) if end > start else None
            last = resolved_index.coordinate(group_name, end - 1) if end > start else None
            emit(
                PreallocateDryRunGroup(
                    group=group_name,
                    window=(start, end),
                    policy="grid",
                    items_in_window=end - start,
                    first=first,
                    last=last,
                    calendar=axis.calendar if is_calendar_axis else None,
                    units=axis.encoded_units if is_calendar_axis else None,
                    # A calendar coordinate is written whole, whatever the window.
                    window_applied=not (
                        is_calendar_axis
                        and (start, end) != (0, int(resolved_index.size(group_name)))
                    ),
                )
            )
            continue
        if not has_input_data:
            emit(PreallocateDryRunGroup(group=group_name, window=(start, end), policy="observed"))
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
        emit(
            PreallocateDryRunGroup(
                group=group_name,
                window=(start, end),
                policy="observed",
                items_in_window=len(slots),
                first=first,
                last=last,
            )
        )


def _ignore_preview(preview: PreallocateDryRunGroup) -> None:
    """Discard a dry-run preview."""
