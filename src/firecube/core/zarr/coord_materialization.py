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

"""Engine-owned coordinate materialization for direct-Zarr time axes.

This module is the write half of the single-writer time-coordinate
mechanism: it creates coordinate arrays, fills grid or observed values,
stamps the sealing/ownership markers, and reconciles re-runs per slot.
Callers hold the global coord-materialization claim for the duration of a
materialization run. Pods only verify a coordinate that carries a preallocate
marker (via ``RegionZarrWriter``); an unmarked coordinate is written by pods.

Every payload write goes through `guarded_write` under this process's
whole-array lock of the coordinate (`hold_array`), keyed as the pods key
theirs. Every attribute and marker is written through the writer's
``update_array_attrs``, under the writer's metadata authority, from the node
read from the store, never through an older handle.

Progress reporting is caller-injected: functions accept a
``report`` callable (the CLI passes ``click.echo``) so the module stays
free of any CLI dependency. Failures raise ``SchemaDriftError`` for
store-state conflicts and ``ConfigurationError`` for caller-input
problems.
"""

from __future__ import annotations

import contextlib
import logging
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import AbstractContextManager
from typing import Any

import numpy as np
from zarr.storage import LocalStore

from firecube.core.errors import (
    CommandName,
    ConfigurationError,
    Message,
    Param,
    SchemaDriftError,
)
from firecube.core.index_resolve import (
    ExtentUnknownError,
)
from firecube.core.index_resolve import (
    _compute_group_identity_hash as compute_group_identity_hash,
)
from firecube.core.index_spec import RegularTimeAxis
from firecube.core.time.convert import coord_to_datetime64
from firecube.core.time.encoding import CoordinateEncoding, coordinate_encoding_for
from firecube.core.zarr._coord_chunks import resolve_coord_chunks
from firecube.core.zarr._coord_common import build_irregular_coord_attrs, existing_array
from firecube.core.zarr._reserved_attrs import FIRECUBE_GROUP_IDENTITY_HASH_ATTR
from firecube.core.zarr._sealing_markers import ATTR_COORD_MANAGED, ATTR_PREALLOCATED
from firecube.core.zarr.coord_rematerialization import (
    RegularCoordTarget,
    _check_existing_irregular_coord,
    _check_existing_regular_coord,
    _plan_encoded_grid_fill,
    _resolve_regular_coord_target,
)
from firecube.core.zarr.guard._unit_locks import _store_identity
from firecube.core.zarr.guard.engine_owned import exception_text
from firecube.core.zarr.guard.guarded_write import (
    ArrayHold,
    guarded_write,
    hold_array,
    selection_box,
)
from firecube.core.zarr.guard.unit_ownership import WriteFootprint
from firecube.core.zarr.region_writer import RegionZarrWriter

log = logging.getLogger("firecube.core.zarr.coord_materialization")

NOTICE_LEVEL = 25
logging.addLevelName(NOTICE_LEVEL, "NOTICE")


def _noop_report(message: str) -> None:
    """Default progress sink: discard the message."""
    del message


def _hold_coord(writer: Any, coord_path: str) -> AbstractContextManager[ArrayHold]:
    """Hold the whole-array lock of *coord_path* in *writer*'s store (`hold_array`).

    Keyed by the store identity of the writer's metadata authority, which is
    spelled as the stored-unit locks spell it.
    """
    return hold_array(writer.authority.scope.store, coord_path)


@contextlib.contextmanager
def _slot_writes(arr: Any, coord_path: str, hold: ArrayHold | None) -> Iterator[ArrayHold]:
    """Yield *hold*, or the whole-array lock of *coord_path* taken here when it is ``None``.

    Without the caller's hold only a local store can be keyed (its resolved
    path is its identity); any other store is refused, so the lock is never
    keyed by a spelling the other writers of the array do not use.
    """
    if hold is not None:
        yield hold
        return
    if not isinstance(arr.store, LocalStore):
        raise ValueError(
            f"writing {coord_path!r} in a {type(arr.store).__name__} needs the caller's "
            "hold_array hold of it"
        )
    with hold_array(_store_identity("", arr.store), coord_path) as taken:
        yield taken


def _write_coord(arr: Any, hold: ArrayHold, selection: Any, payload: Any) -> None:
    """``arr[selection] = payload`` through `guarded_write` under the whole-array *hold*.

    The footprint is the box *selection* covers (an integer, a slice or ``...``).
    """
    shape = tuple(int(n) for n in arr.shape)
    box = selection_box(selection, shape) or tuple((0, n) for n in shape)
    guarded_write(
        arr,
        WriteFootprint(hold.array_path, box),
        selection,
        payload,
        store=hold.store,
        write_empty_chunks=None,
        hold=hold,
    )


def materialize_irregular_coord_array(
    *,
    writer: Any,
    root: Any,
    group_name: str,
    axis: Any,
    spec: Any | None = None,
    report: Callable[[str], None] = _noop_report,
) -> None:
    """Write ``axis.values`` densely at ``{group_name}/{axis.coordinate}``.

    Storage follows the axis's `CoordinateEncoding` (see
    `coordinate_encoding_for`). A Gregorian axis (``axis.calendar`` unset)
    stores ``datetime64``, ``datetime64[ns]`` unless *spec* declares a dtype.
    A calendar axis stores its canonical encoded numbers as ``int64`` or
    ``float64``, with ``units`` and ``calendar`` attrs. Shape is
    ``(len(axis.values),)`` either way.

    Behavior on existing array:

    * Values match the resolved axis → idempotent no-op; the
      ``ATTR_PREALLOCATED`` marker is stamped if not already present so
      subsequent ``write_timestamp`` calls take the marker-aware path.
    * Values differ and the existing array is entirely fill (NaT for
      Gregorian, the encoding's fill sentinel otherwise: a spec-loop
      pre-allocated shell) → fill values, merge attrs, stamp marker.
    * Values differ and the existing array holds non-fill content → drift
      error via ``SchemaDriftError`` so the operator sees the conflict
      before any downstream write.
    * Either side is encoded, the existing array is established (sealed,
      coord-managed, identity-stamped, or holding a non-fill value), and its
      ``units``, ``calendar``, storage kind or identity hash differ →
      `CoordinateRelabelError`, nothing written. Equal stored numbers do not
      excuse it: the same number under another epoch or calendar is another instant.

    A calendar coordinate gets its ``units`` and ``calendar`` attrs before its
    values, and its markers last. Attrs mirror ``build_regular_coord_attrs``:
    minimal defaults ``{"standard_name": "time", "axis": "T"}`` merged with
    ``spec.attrs`` (with reserved firecube keys and CF-encoding-owned
    ``units``/``calendar`` keys stripped). ``spec.dimension_names`` is honored
    when provided; the default is ``(axis.coordinate,)``.
    """
    values = axis.values
    slot_count = len(values)
    coord_path = f"{group_name}/{axis.coordinate}"
    encoding = coordinate_encoding_for(axis, spec, coord_path=coord_path)
    encoded = bool(encoding.extra_attrs)
    target_dtype = encoding.dtype
    coord_data = encoding.encode_values(values)
    attrs = build_irregular_coord_attrs(spec, axis)
    group_identity_hash = compute_group_identity_hash(axis, int(slot_count), target_dtype)
    dim_names = (
        tuple(spec.dimension_names)
        if spec is not None and getattr(spec, "dimension_names", None) is not None
        else (axis.coordinate,)
    )

    existing = existing_array(root, coord_path)
    if existing is not None:
        stamps = {ATTR_PREALLOCATED: True, FIRECUBE_GROUP_IDENTITY_HASH_ATTR: group_identity_hash}
        with _hold_coord(writer, coord_path) as hold:
            values_match = _check_existing_irregular_coord(
                coord_path,
                existing,
                encoding=encoding,
                coord_data=coord_data,
                group_identity_hash=group_identity_hash,
            )
            if values_match:
                writer.update_array_attrs(coord_path, stamps)
                report(f"array {coord_path}: existing irregular coord array matches; no-op")
                return
            if encoded:
                existing = writer.update_array_attrs(coord_path, attrs)
            _write_coord(existing, hold, ..., coord_data)
        writer.update_array_attrs(coord_path, stamps if encoded else {**attrs, **stamps})
        report(f"array {coord_path}: filled existing irregular coord array")
        return

    coord_arr = writer.ensure_group(
        coord_path,
        shape=(slot_count,),
        dtype=target_dtype,
        fill_value=encoding.fill_value,
        chunks=resolve_coord_chunks(spec, slot_count),
        attrs=attrs,
        dimension_names=dim_names,
    )
    with _hold_coord(writer, coord_path) as hold:
        _write_coord(coord_arr, hold, ..., coord_data)
    writer.update_array_attrs(
        coord_path,
        {ATTR_PREALLOCATED: True, FIRECUBE_GROUP_IDENTITY_HASH_ATTR: group_identity_hash},
    )
    report(f"array {coord_path}: created (irregular coord materialization)")


def axis_has_resolvable_extent(axis: Any, resolved_index: Any, group: str) -> bool:
    if not isinstance(axis, RegularTimeAxis):
        return False
    try:
        resolved_index.size(group)
    except ExtentUnknownError:
        return False
    return True


def discover_regular_observed_coord_values(
    *,
    ingestor: Any,
    plugin_ctx: Any,
    resolved_index: Any,
    group_name: str,
    slot_start: int,
    slot_end: int,
    items: Iterable[Any] | None = None,
) -> dict[int, Any]:
    """Return the observed coordinate of every slot in ``[slot_start, slot_end)`` of a group.

    *items* are the source items the command already discovered; ``None``
    calls ``ingestor.discover_source_files``. Each kept item is inspected once.

    Raises:
        ConfigurationError: ``inspect_item`` raises, or its coordinate maps to no slot.
        SchemaDriftError: Two items put different values on one slot.
    """
    observed: dict[int, Any] = {}
    source_items: Iterable[Any] = (
        ingestor.discover_source_files(plugin_ctx) if items is None else items
    )
    for item in source_items:
        if not ingestor.filter_item(item, plugin_ctx):
            continue
        try:
            info = ingestor.inspect_item(item, plugin_ctx)
        except Exception as exc:
            raise ConfigurationError(
                Message(
                    "inspect_item raised an error for item {item!r}: {cause}",
                    item=item,
                    group=group_name,
                    cause=exception_text(exc),
                )
            ) from exc
        if info is None:
            continue
        coordinate = info.coordinate if hasattr(info, "coordinate") else info
        try:
            slot_index = int(resolved_index.position(group_name, coordinate))
        except (TypeError, ValueError) as exc:
            raise ConfigurationError(
                Message(
                    "inspect_item returned coordinate {coordinate!r} for item {item!r} "
                    "that could not be mapped to a slot index: {cause}",
                    coordinate=coordinate,
                    item=item,
                    group=group_name,
                    cause=exception_text(exc),
                )
            ) from exc
        if not (slot_start <= slot_index < slot_end):
            continue
        previous = observed.get(slot_index)
        if previous is not None and coord_to_datetime64(previous) != coord_to_datetime64(
            coordinate
        ):
            raise SchemaDriftError(
                f"observed coordinate drift for group {group_name!r} slot {slot_index}: "
                f"existing discovery value {previous!r}, new discovery value {coordinate!r}"
            )
        observed[slot_index] = coordinate
    return observed


def write_observed_regular_coord_values(
    *,
    arr: Any,
    coord_path: str,
    values_by_slot: dict[int, Any],
    target_dtype: np.dtype[Any],
    hold: ArrayHold | None = None,
) -> tuple[int, int]:
    """Fill NaT slots of a fresh observed coordinate; refuse a slot holding another value.

    Runs under *hold*, the caller's whole-array lock of *coord_path*; without
    one the lock is taken here (a local store only, see `_slot_writes`).
    """
    written = 0
    matched = 0
    with _slot_writes(arr, coord_path, hold) as held:
        for slot_index, coordinate in sorted(values_by_slot.items()):
            desired = RegionZarrWriter._normalize_for_coord_compare(
                coord_to_datetime64(coordinate), target_dtype
            )
            current_norm = RegionZarrWriter._normalize_for_coord_compare(
                arr[slot_index], target_dtype
            )
            if np.isnat(current_norm):
                _write_coord(arr, held, slot_index, desired)
                written += 1
                continue
            if current_norm == desired:
                matched += 1
                continue
            raise SchemaDriftError(
                f"coordinate array {coord_path} slot {slot_index} drift: "
                f"stored value {current_norm!r}, discovered value {desired!r}"
            )
    return written, matched


def reconcile_observed_regular_coord_values(
    *,
    arr: Any,
    coord_path: str,
    values_by_slot: dict[int, Any],
    target_dtype: np.dtype[Any],
    hold: ArrayHold | None = None,
) -> tuple[int, int]:
    """Fill NaT observed slots, no-op matching slots, and refuse drift.

    Runs under *hold*, the caller's whole-array lock of *coord_path*; without
    one the lock is taken here (a local store only, see `_slot_writes`).
    """
    written = 0
    matched = 0
    with _slot_writes(arr, coord_path, hold) as held:
        for slot_index, coordinate in sorted(values_by_slot.items()):
            desired = RegionZarrWriter._normalize_for_coord_compare(
                coord_to_datetime64(coordinate), target_dtype
            )
            stored = arr[slot_index]
            stored_norm = RegionZarrWriter._normalize_for_coord_compare(stored, target_dtype)
            if np.isnat(stored_norm):
                _write_coord(arr, held, slot_index, desired)
                written += 1
                continue
            if stored_norm == desired:
                matched += 1
                continue
            raise SchemaDriftError(
                f"coordinate array {coord_path} slot {slot_index} drift: "
                f"stored value {stored_norm!r}, incoming value {desired!r}"
            )
    if written:
        log.log(
            NOTICE_LEVEL,
            "reconciled %s NaT observed coord slot(s) in %s",
            written,
            coord_path,
        )
    return written, matched


def stamp_coord_managed_marker(writer: Any, coord_path: str) -> Any:
    """Stamp ``ATTR_COORD_MANAGED`` through *writer*'s authority; return the node as written.

    Raises:
        SchemaDriftError: The marker could not be written; names the cleanup.
    """
    try:
        return writer.update_array_attrs(coord_path, {ATTR_COORD_MANAGED: True})
    except Exception as exc:
        raise SchemaDriftError(
            Message(
                "failed to stamp {marker} on coordinate array {array_path}; "
                "the observed shell is a legacy/unmarked state and must be cleaned up "
                "with existing `{tooling}` tooling before retrying",
                marker=ATTR_COORD_MANAGED,
                array_path=coord_path,
                tooling=_CHUNKS_TOOLING,
            )
        ) from exc


_CHUNKS_TOOLING = CommandName(("chunks",), "ChunkManager")


def _legacy_shell_refusal(
    coord_path: str, group: str, slot_window: tuple[int, int]
) -> SchemaDriftError:
    """Build the refusal for an observed-regime coordinate shell without the managed marker."""
    return SchemaDriftError(
        Message(
            "coordinate array {array_path} lacks {marker}; marker "
            "absent means legacy classification, so observed-regime "
            "materialization refuses to run on this shell. Clean up the "
            "legacy coordinate shell with existing `{tooling}` tooling "
            "before retrying.",
            array_path=coord_path,
            group=group,
            slot_window=slot_window,
            marker=ATTR_COORD_MANAGED,
            tooling=_CHUNKS_TOOLING,
        )
    )


def _fill_existing_regular_grid_encoded(
    *,
    writer: Any,
    coord_path: str,
    existing: Any,
    encoding: CoordinateEncoding,
    values: np.ndarray,
    slot_count: int,
    window_suffix: str,
    attrs: Mapping[str, Any],
    group_identity_hash: str,
    report: Callable[[str], None],
) -> None:
    """Fill the fill-sentinel slots of an existing encoded grid coordinate, the whole array.

    Every refusal in `_plan_encoded_grid_fill` runs before the first write.
    Then *attrs* (``units`` and ``calendar`` among them) are written, then the
    values through `guarded_write`, then the markers.
    """
    with _hold_coord(writer, coord_path) as hold:
        stored_full, filled = _plan_encoded_grid_fill(
            coord_path=coord_path,
            existing=existing,
            encoding=encoding,
            values=values,
            group_identity_hash=group_identity_hash,
        )
        existing = writer.update_array_attrs(coord_path, attrs)
        if filled:
            _write_coord(existing, hold, ..., stored_full)
    writer.update_array_attrs(
        coord_path,
        {ATTR_PREALLOCATED: True, FIRECUBE_GROUP_IDENTITY_HASH_ATTR: group_identity_hash},
    )
    if filled == slot_count:
        report(f"array {coord_path}: filled existing regular coord array{window_suffix}")
    elif filled:
        log.log(NOTICE_LEVEL, "reconciled %s fill grid coord slot(s) in %s", filled, coord_path)
        report(
            f"array {coord_path}: filled {filled} fill slot(s) with nominal grid "
            f"values{window_suffix}"
        )
    else:
        report(f"array {coord_path}: no-op (matches nominal grid){window_suffix}")


def materialize_regular_coord_array(
    *,
    writer: Any,
    root: Any,
    group_name: str,
    axis: Any,
    spec: Any | None,
    resolved_index: Any | None = None,
    ingestor: Any | None = None,
    plugin_ctx: Any | None = None,
    slot_start: int = 0,
    slot_end: int | None = None,
    has_input_data: bool = False,
    input_data: str | None = None,
    report: Callable[[str], None] = _noop_report,
    target: RegularCoordTarget | None = None,
    items: Iterable[Any] | None = None,
) -> None:
    """Materialize a dense regular time coordinate at ``{group_name}/{axis.coordinate}``.

    The branch is decided by the axis's stored-values policy:
    ``"grid"`` means the nominal grid IS
    the coordinate, so values are prefilled and the array is sealed with
    ``firecube_preallocated`` and ingest writes become verify-only no-ops;
    ``"observed"`` means the stored values are real observation times
    unknowable before ingest, so the array is created at the dense chunk
    shape but left NaT and unsealed.

    With ``axis.calendar`` set, the coordinate stores encoded ``int64`` or
    ``float64`` numbers (slot ``n`` holds ``n * cadence_s`` in the axis's
    derived ``units``), not ``datetime64``. A calendar axis resolves only to
    the ``"grid"`` policy. Its coordinate is always written whole, whatever
    ``slot_start`` and ``slot_end`` say: an unwritten slot would hold the fill
    sentinel, which xarray cannot decode. The window still labels the report.
    A calendar coordinate gets ``units`` and ``calendar`` before its values,
    and ``firecube_preallocated`` and the identity hash last.

    *target*, when given, is the `RegularCoordTarget` that
    `check_regular_coord_rematerialization` returned for the same group, axis
    and spec; it replaces resolving the encoding and nominal values again.

    *items*, when given, are the source items the command already discovered.
    An observed coordinate reads them instead of calling
    ``ingestor.discover_source_files``.

    Raises:
        ValueError: If ``axis.slot_count`` is ``None`` and *resolved_index*
            is also ``None``, or an existing array's shape does not match.
        ConfigurationError: If a calendar axis resolves to a policy other
            than ``"grid"``, or *spec* and *axis* disagree on dtype, ``units``
            or ``calendar``.
        SchemaDriftError: If an existing coordinate array's markers or stored
            values conflict with this materialization.
        CoordinateRelabelError: If either side is encoded and an established
            existing array differs from the axis in storage kind, ``units``,
            ``calendar`` or identity hash. Raised before any write.
    """
    if target is None:
        target = _resolve_regular_coord_target(
            group_name=group_name, axis=axis, spec=spec, resolved_index=resolved_index
        )
    coord_path = target.coord_path
    slot_count = target.slot_count
    is_calendar_axis = target.is_calendar_axis
    coordinate_policy = target.policy
    encoding = target.encoding
    target_dtype = encoding.dtype
    values = target.values
    attrs = target.attrs
    group_identity_hash = target.group_identity_hash
    effective_slot_end = slot_count if slot_end is None else slot_end

    # Only grid-valued coordinates are computable without source inspection.
    # Observed-values floor axes are materialized from inspect_item() below when
    # the operator supplies --input-data; otherwise they keep the legacy NaT shell.
    prefill = coordinate_policy == "grid"

    observed_regime = coordinate_policy == "observed"
    manage_observed = observed_regime and has_input_data
    observed_values: dict[int, Any] = {}
    if manage_observed:
        if resolved_index is None or ingestor is None or plugin_ctx is None:
            raise ValueError(
                "observed coordinate materialization requires resolved index and plugin context"
            )
        observed_values = discover_regular_observed_coord_values(
            ingestor=ingestor,
            plugin_ctx=plugin_ctx,
            resolved_index=resolved_index,
            group_name=group_name,
            slot_start=slot_start,
            slot_end=effective_slot_end,
            items=items,
        )
        # Stamping firecube_coord_managed on a NaT-shell coord (zero
        # writes) traps every subsequent ingest in the NaT-under-marker
        # check in ``ensure_timestamp_slot``; refuse loudly so the store
        # stays recoverable.
        if not observed_values:
            raise ConfigurationError(
                Message(
                    "no items discovered from {input_data_param}={input_data!r} for {array_path}; "
                    "refusing to stamp firecube_coord_managed on an empty array "
                    "(would block further ingest). Check {input_data_param} path, plugin's "
                    "discover_source_files, and {slot_start}/{slot_end} window.",
                    input_data_param=Param("input_data"),
                    input_data=input_data,
                    array_path=coord_path,
                    group=group_name,
                    slot_window=(slot_start, effective_slot_end),
                    slot_start=Param("slot_start"),
                    slot_end=Param("slot_end"),
                )
            )

    is_full_grid = slot_start == 0 and effective_slot_end == slot_count
    window_label = "" if is_full_grid else f" in window [{slot_start}, {effective_slot_end})"
    window_suffix = "" if is_full_grid else "; window not applied to calendar coordinate"

    existing = existing_array(root, coord_path)
    if existing is not None:
        _check_existing_regular_coord(existing, target)
        if prefill and is_calendar_axis:
            _fill_existing_regular_grid_encoded(
                writer=writer,
                coord_path=coord_path,
                existing=existing,
                encoding=encoding,
                values=values,
                slot_count=slot_count,
                window_suffix=window_suffix,
                attrs=attrs,
                group_identity_hash=group_identity_hash,
                report=report,
            )
            return
        existing = writer.update_array_attrs(coord_path, attrs)
        window_slice = slice(slot_start, effective_slot_end)
        if prefill:
            if bool(existing.attrs.get(ATTR_COORD_MANAGED, False)):
                raise SchemaDriftError(
                    f"coordinate array {coord_path} carries {ATTR_COORD_MANAGED}: its "
                    "values are engine-materialized observed times. Refusing to "
                    "overwrite them with the nominal grid."
                )
            # Grid values are deterministic and this run holds the global
            # materialization claim, so filling a stored NaT with the nominal
            # value is always safe: windowed and full prefills converge to the
            # same array. Only a stored non-NaT value that differs from the
            # nominal grid is drift.
            with _hold_coord(writer, coord_path) as hold:
                stored_window = np.asarray(existing[window_slice])
                expected_window = np.asarray(values[window_slice])
                filled = 0
                for offset, (stored_value, incoming_value) in enumerate(
                    zip(stored_window.flat, expected_window.flat, strict=True)
                ):
                    stored_norm = RegionZarrWriter._normalize_for_coord_compare(
                        stored_value, target_dtype
                    )
                    if np.isnat(stored_norm):
                        stored_window[offset] = incoming_value
                        filled += 1
                        continue
                    incoming_norm = RegionZarrWriter._normalize_for_coord_compare(
                        incoming_value, target_dtype
                    )
                    if stored_norm != incoming_norm:
                        slot_index = slot_start + offset
                        raise SchemaDriftError(
                            f"coordinate array {coord_path} diverged from nominal grid at slot "
                            f"{slot_index}: stored {stored_value!r}, incoming {incoming_value!r}"
                        )
                if filled:
                    _write_coord(existing, hold, window_slice, stored_window)
            writer.update_array_attrs(
                coord_path,
                {ATTR_PREALLOCATED: True, FIRECUBE_GROUP_IDENTITY_HASH_ATTR: group_identity_hash},
            )
            if filled == len(stored_window):
                report(f"array {coord_path}: filled existing regular coord array{window_label}")
            elif filled:
                log.log(
                    NOTICE_LEVEL,
                    "reconciled %s NaT grid coord slot(s) in %s",
                    filled,
                    coord_path,
                )
                report(
                    f"array {coord_path}: filled {filled} NaT slot(s) with nominal grid "
                    f"values{window_label}"
                )
            else:
                report(f"array {coord_path}: no-op (matches nominal grid){window_label}")
            return
        elif manage_observed:
            if bool(existing.attrs.get(ATTR_PREALLOCATED, False)):
                raise SchemaDriftError(
                    f"coordinate array {coord_path} already has {ATTR_PREALLOCATED}; "
                    f"cannot stamp {ATTR_COORD_MANAGED}"
                )
            if not bool(existing.attrs.get(ATTR_COORD_MANAGED, False)):
                raise _legacy_shell_refusal(
                    coord_path, group_name, (slot_start, effective_slot_end)
                )
            with _hold_coord(writer, coord_path) as hold:
                written, matched = reconcile_observed_regular_coord_values(
                    arr=existing,
                    coord_path=coord_path,
                    values_by_slot=observed_values,
                    target_dtype=target_dtype,
                    hold=hold,
                )
            writer.update_array_attrs(
                coord_path, {FIRECUBE_GROUP_IDENTITY_HASH_ATTR: group_identity_hash}
            )
            if observed_values and written == 0 and matched == len(observed_values):
                report(
                    f"array {coord_path}: observed coord values already match in window "
                    f"[{slot_start}, {effective_slot_end}); no-op"
                )
                return
            report(
                f"array {coord_path}: materialized {len(observed_values)} observed coord values "
                f"in window [{slot_start}, {effective_slot_end})"
            )
        elif observed_regime:
            if not bool(existing.attrs.get(ATTR_COORD_MANAGED, False)):
                raise _legacy_shell_refusal(
                    coord_path, group_name, (slot_start, effective_slot_end)
                )
            report(
                f"array {coord_path}: existing regular coord array kept coord-managed "
                "(observed coordinate values); values written at ingest"
            )
        return

    arr = writer.ensure_group(
        coord_path,
        shape=(slot_count,),
        dtype=target_dtype,
        fill_value=encoding.fill_value,
        chunks=resolve_coord_chunks(spec, slot_count),
        attrs=attrs,
        dimension_names=(axis.coordinate,),
    )
    if prefill:
        with _hold_coord(writer, coord_path) as hold:
            if is_calendar_axis or is_full_grid:
                _write_coord(arr, hold, ..., values)
            else:
                window = slice(slot_start, effective_slot_end)
                _write_coord(arr, hold, window, values[slot_start:effective_slot_end])
        writer.update_array_attrs(
            coord_path,
            {ATTR_PREALLOCATED: True, FIRECUBE_GROUP_IDENTITY_HASH_ATTR: group_identity_hash},
        )
        label = window_suffix if is_calendar_axis else window_label
        report(f"array {coord_path}: created (regular coord materialization){label}")
    elif manage_observed:
        arr = stamp_coord_managed_marker(writer, coord_path)
        with _hold_coord(writer, coord_path) as hold:
            write_observed_regular_coord_values(
                arr=arr,
                coord_path=coord_path,
                values_by_slot=observed_values,
                target_dtype=target_dtype,
                hold=hold,
            )
        writer.update_array_attrs(
            coord_path, {FIRECUBE_GROUP_IDENTITY_HASH_ATTR: group_identity_hash}
        )
        report(
            f"array {coord_path}: created and materialized {len(observed_values)} "
            f"observed coord values in window [{slot_start}, {effective_slot_end})"
        )
    elif observed_regime:
        stamp_coord_managed_marker(writer, coord_path)
        writer.update_array_attrs(
            coord_path, {FIRECUBE_GROUP_IDENTITY_HASH_ATTR: group_identity_hash}
        )
        report(
            f"array {coord_path}: created (dense chunking, coord-managed; values "
            "written at ingest under observed-value reconciliation)"
        )
