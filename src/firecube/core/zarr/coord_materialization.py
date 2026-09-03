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
materialization run; pods never write coordinate arrays (they verify via
``RegionZarrWriter``).

Progress reporting is caller-injected: functions accept a
``report`` callable (the CLI passes ``click.echo``) so the module stays
free of any CLI dependency. Failures raise ``SchemaDriftError`` for
store-state conflicts and ``ConfigurationError`` for caller-input
problems.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

import numpy as np

from firecube.core.errors import ConfigurationError, SchemaDriftError
from firecube.core.index_resolve import (
    ExtentUnknownError,
)
from firecube.core.index_resolve import (
    _compute_group_identity_hash as compute_group_identity_hash,
)
from firecube.core.index_spec import (
    RegularTimeAxis,
    effective_regular_time_policy,
)
from firecube.core.index_spec import (
    _canonical_coordinate_value as canonical_coordinate_value,
)
from firecube.core.zarr._coord_chunks import resolve_coord_chunks
from firecube.core.zarr._coord_lifecycle import assert_coord_markers_consistent
from firecube.core.zarr._reserved_attrs import (
    FIRECUBE_GROUP_IDENTITY_HASH_ATTR,
    RESERVED_ARRAY_ATTRS,
)
from firecube.core.zarr._sealing_markers import ATTR_COORD_MANAGED, ATTR_PREALLOCATED
from firecube.core.zarr.region_writer import RegionZarrWriter

log = logging.getLogger("firecube.core.zarr.coord_materialization")

NOTICE_LEVEL = 25
logging.addLevelName(NOTICE_LEVEL, "NOTICE")


def _noop_report(message: str) -> None:
    """Default progress sink: discard the message."""
    del message


def existing_array(root: Any, array_path: str) -> Any | None:
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


def array_schema_mismatches(
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

    Dtype defaults to ``datetime64[ns]``; when *spec* is provided and carries
    a dtype, that dtype is honored. Shape ``(len(axis.values),)``.

    Behavior on existing array:

    * Values match the resolved axis → idempotent no-op; the
      ``ATTR_PREALLOCATED`` marker is stamped if not already present so
      subsequent ``write_timestamp`` calls take the marker-aware path.
    * Values differ and the existing array is entirely NaT (spec-loop
      pre-allocated shell) → fill values, merge attrs, stamp marker.
    * Values differ and the existing array holds non-NaT content → drift
      error via ``SchemaDriftError`` so the operator sees the conflict
      before any downstream write.

    Attrs mirror ``build_regular_coord_attrs``: minimal defaults
    ``{"standard_name": "time", "axis": "T"}`` merged with ``spec.attrs``
    (with reserved firecube keys and CF-encoding-owned ``units``/``calendar``
    keys stripped). ``spec.dimension_names`` is honored when provided; the
    default is ``(axis.coordinate,)``.
    """
    values = axis.values
    slot_count = len(values)
    target_dtype = (
        np.dtype(spec.dtype)
        if spec is not None and getattr(spec, "dtype", None) is not None
        else np.dtype("datetime64[ns]")
    )
    coord_data = np.array(
        [coord_to_datetime64(value) for value in values], dtype="datetime64[ns]"
    ).astype(target_dtype)
    coord_path = f"{group_name}/{axis.coordinate}"
    attrs = build_irregular_coord_attrs(spec, axis)
    group_identity_hash = compute_group_identity_hash(axis, int(slot_count), target_dtype)
    dim_names = (
        tuple(spec.dimension_names)
        if spec is not None and getattr(spec, "dimension_names", None) is not None
        else (axis.coordinate,)
    )

    existing = existing_array(root, coord_path)
    if existing is not None:
        mismatches = array_schema_mismatches(
            existing=existing,
            expected_shape=(slot_count,),
            expected_dtype=target_dtype,
            expected_chunks=None,
        )
        if mismatches:
            diff = "; ".join(mismatches)
            raise SchemaDriftError(
                f"Existing coord array mismatch: '{coord_path}' has mismatches.\n"
                f"Mismatch: {diff}\n"
                "Either delete it or update the plugin's IrregularTimeAxis to match."
            )
        assert_coord_markers_consistent(dict(existing.attrs), coord_path)
        if bool(existing.attrs.get(ATTR_COORD_MANAGED, False)):
            raise SchemaDriftError(
                f"Existing coord array '{coord_path}' carries {ATTR_COORD_MANAGED}; "
                "refusing to seal engine-managed observed coordinates as an irregular axis."
            )
        existing_values = np.asarray(existing[:])
        if np.array_equal(existing_values, coord_data):
            if not bool(existing.attrs.get(ATTR_PREALLOCATED, False)):
                existing.attrs[ATTR_PREALLOCATED] = True
            existing.attrs[FIRECUBE_GROUP_IDENTITY_HASH_ATTR] = group_identity_hash
            report(f"array {coord_path}: existing irregular coord array matches; no-op")
            return
        if not values_all_nat(existing_values):
            raise SchemaDriftError(
                f"Existing coord array '{coord_path}' has values that differ from the "
                "resolved IrregularTimeAxis. Delete it or align the plugin's axis values."
            )
        existing[...] = coord_data
        merged_attrs = dict(existing.attrs)
        merged_attrs.update(attrs)
        existing.attrs.update(merged_attrs)
        existing.attrs[ATTR_PREALLOCATED] = True
        existing.attrs[FIRECUBE_GROUP_IDENTITY_HASH_ATTR] = group_identity_hash
        report(f"array {coord_path}: filled existing irregular coord array")
        return

    coord_arr = writer.ensure_group(
        coord_path,
        shape=(slot_count,),
        dtype=target_dtype,
        fill_value=np.array(np.datetime64("NaT", "ns"), dtype=target_dtype)[()],
        chunks=resolve_coord_chunks(spec, slot_count),
        attrs=attrs,
        dimension_names=dim_names,
    )
    coord_arr[...] = coord_data
    coord_arr.attrs[ATTR_PREALLOCATED] = True
    coord_arr.attrs[FIRECUBE_GROUP_IDENTITY_HASH_ATTR] = group_identity_hash
    report(f"array {coord_path}: created (irregular coord materialization)")


def build_irregular_coord_attrs(spec: Any | None, axis: Any) -> dict[str, Any]:
    """Build irregular coord attrs, mirroring ``build_regular_coord_attrs``.

    Minimal defaults (``standard_name="time"``, ``axis="T"``) merged with
    ``spec.attrs`` (when provided) after stripping reserved firecube keys and
    the xarray-CF-encoding-owned keys ``units`` and ``calendar``.
    """
    del axis  # unused; symmetry with `build_regular_coord_attrs`
    minimal: dict[str, Any] = {"standard_name": "time", "axis": "T"}
    if spec is None or getattr(spec, "attrs", None) is None:
        return minimal
    merged = dict(minimal)
    merged.update({k: v for k, v in spec.attrs.items() if k not in RESERVED_ARRAY_ATTRS})
    merged.pop("units", None)
    merged.pop("calendar", None)
    return merged


def values_all_nat(values: np.ndarray) -> bool:
    """True when *values* is a datetime64 array with every element NaT."""
    if values.dtype.kind != "M":
        return False
    return bool(np.all(np.isnat(values)))


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
) -> dict[int, Any]:
    observed: dict[int, Any] = {}
    for item in ingestor.discover_source_files(plugin_ctx):
        if not ingestor.filter_item(item, plugin_ctx):
            continue
        try:
            info = ingestor.inspect_item(item, plugin_ctx)
        except Exception as exc:
            raise ConfigurationError(
                f"inspect_item raised an error for item {item!r}: {exc}"
            ) from exc
        if info is None:
            continue
        coordinate = info.coordinate if hasattr(info, "coordinate") else info
        try:
            slot_index = int(resolved_index.position(group_name, coordinate))
        except (TypeError, ValueError) as exc:
            raise ConfigurationError(
                f"inspect_item returned coordinate {coordinate!r} for item {item!r} "
                f"that could not be mapped to a slot index: {exc}"
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
) -> tuple[int, int]:
    written = 0
    matched = 0
    for slot_index, coordinate in sorted(values_by_slot.items()):
        desired = RegionZarrWriter._normalize_for_coord_compare(
            coord_to_datetime64(coordinate), target_dtype
        )
        current_norm = RegionZarrWriter._normalize_for_coord_compare(arr[slot_index], target_dtype)
        if np.isnat(current_norm):
            arr[slot_index] = desired
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
) -> tuple[int, int]:
    """Fill NaT observed slots, no-op matching slots, and refuse drift."""
    written = 0
    matched = 0
    for slot_index, coordinate in sorted(values_by_slot.items()):
        desired = RegionZarrWriter._normalize_for_coord_compare(
            coord_to_datetime64(coordinate), target_dtype
        )
        stored = arr[slot_index]
        stored_norm = RegionZarrWriter._normalize_for_coord_compare(stored, target_dtype)
        if np.isnat(stored_norm):
            arr[slot_index] = desired
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


def stamp_coord_managed_marker(arr: Any, coord_path: str) -> None:
    try:
        arr.attrs[ATTR_COORD_MANAGED] = True
    except Exception as exc:
        raise SchemaDriftError(
            f"failed to stamp {ATTR_COORD_MANAGED} on coordinate array {coord_path}; "
            "the observed shell is a legacy/unmarked state and must be cleaned up "
            "with existing `firecube chunks` tooling before retrying"
        ) from exc


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
) -> None:
    """Materialize a dense regular time coordinate at ``{group_name}/{axis.coordinate}``.

    The branch is decided by the axis's stored-values policy:
    ``"grid"`` means the nominal grid IS
    the coordinate, so values are prefilled and the array is sealed with
    ``firecube_preallocated`` and ingest writes become verify-only no-ops;
    ``"observed"`` means the stored values are real observation times
    unknowable before ingest, so the array is created at the dense chunk
    shape but left NaT and unsealed.
    """
    slot_count = axis.slot_count
    if slot_count is None:
        if resolved_index is None:
            raise ValueError(
                f"Cannot materialize RegularTimeAxis '{axis.coordinate}': slot_count is None. "
                "Declare either slot_count or end_date in the plugin's index_spec."
            )
        slot_count = int(resolved_index.size(group_name))
    effective_slot_end = int(slot_count) if slot_end is None else slot_end

    epoch = coord_to_datetime64(axis.epoch)
    cadence = np.timedelta64(int(axis.cadence_s * 1e9), "ns")
    values = epoch + np.arange(slot_count, dtype=np.int64) * cadence
    target_dtype = (
        np.dtype(spec.dtype)
        if spec is not None and getattr(spec, "dtype", None) is not None
        else np.dtype("datetime64[ns]")
    )
    values = values.astype(target_dtype)
    coord_path = f"{group_name}/{axis.coordinate}"
    attrs = build_regular_coord_attrs(spec, axis)
    group_identity_hash = compute_group_identity_hash(axis, int(slot_count), target_dtype)

    coordinate_policy = effective_regular_time_policy(axis)
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
        )
        # Stamping firecube_coord_managed on a NaT-shell coord (zero
        # writes) traps every subsequent ingest in the NaT-under-marker
        # check in ``ensure_timestamp_slot``; refuse loudly so the store
        # stays recoverable.
        if not observed_values:
            raise ConfigurationError(
                f"no items discovered from --input-data={input_data!r} for {coord_path}; "
                f"refusing to stamp firecube_coord_managed on an empty array "
                f"(would block further ingest). Check --input-data path, plugin's "
                f"discover_source_files, and --slot-start/--slot-end window."
            )

    existing = existing_array(root, coord_path)
    if existing is not None:
        expected_shape = (slot_count,)
        if tuple(existing.shape) != expected_shape:
            raise ValueError(
                f"Preallocated coord array {coord_path!r} has shape {existing.shape}, "
                f"expected {expected_shape}. Refuse to resize silently."
            )
        assert_coord_markers_consistent(dict(existing.attrs), coord_path)
        existing_attrs = dict(existing.attrs)
        existing_attrs.update(attrs)
        existing.attrs.update(existing_attrs)
        window_slice = slice(slot_start, effective_slot_end)
        window_label = (
            ""
            if slot_start == 0 and effective_slot_end == slot_count
            else (f" in window [{slot_start}, {effective_slot_end})")
        )
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
                existing[window_slice] = stored_window
            if not bool(existing.attrs.get(ATTR_PREALLOCATED, False)):
                existing.attrs[ATTR_PREALLOCATED] = True
            existing.attrs[FIRECUBE_GROUP_IDENTITY_HASH_ATTR] = group_identity_hash
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
                raise SchemaDriftError(
                    f"coordinate array {coord_path} lacks {ATTR_COORD_MANAGED}; marker "
                    "absent means legacy classification, so observed-regime "
                    "materialization refuses to run on this shell. Clean up the "
                    "legacy coordinate shell with existing `firecube chunks` tooling "
                    "before retrying."
                )
            written, matched = reconcile_observed_regular_coord_values(
                arr=existing,
                coord_path=coord_path,
                values_by_slot=observed_values,
                target_dtype=target_dtype,
            )
            existing.attrs[FIRECUBE_GROUP_IDENTITY_HASH_ATTR] = group_identity_hash
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
                raise SchemaDriftError(
                    f"coordinate array {coord_path} lacks {ATTR_COORD_MANAGED}; marker "
                    "absent means legacy classification, so observed-regime "
                    "materialization refuses to run on this shell. Clean up the "
                    "legacy coordinate shell with existing `firecube chunks` tooling "
                    "before retrying."
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
        fill_value=np.array(np.datetime64("NaT", "ns"), dtype=target_dtype)[()],
        chunks=resolve_coord_chunks(spec, slot_count),
        attrs=attrs,
        dimension_names=(axis.coordinate,),
    )
    if prefill:
        if slot_start == 0 and effective_slot_end == slot_count:
            arr[...] = values
        else:
            arr[slice(slot_start, effective_slot_end)] = values[slot_start:effective_slot_end]
        arr.attrs[ATTR_PREALLOCATED] = True
        arr.attrs[FIRECUBE_GROUP_IDENTITY_HASH_ATTR] = group_identity_hash
        if slot_start == 0 and effective_slot_end == slot_count:
            report(f"array {coord_path}: created (regular coord materialization)")
        else:
            report(
                f"array {coord_path}: created (regular coord materialization) in window "
                f"[{slot_start}, {effective_slot_end})"
            )
    elif manage_observed:
        stamp_coord_managed_marker(arr, coord_path)
        write_observed_regular_coord_values(
            arr=arr,
            coord_path=coord_path,
            values_by_slot=observed_values,
            target_dtype=target_dtype,
        )
        arr.attrs[FIRECUBE_GROUP_IDENTITY_HASH_ATTR] = group_identity_hash
        report(
            f"array {coord_path}: created and materialized {len(observed_values)} "
            f"observed coord values in window [{slot_start}, {effective_slot_end})"
        )
    elif observed_regime:
        stamp_coord_managed_marker(arr, coord_path)
        arr.attrs[FIRECUBE_GROUP_IDENTITY_HASH_ATTR] = group_identity_hash
        report(
            f"array {coord_path}: created (dense chunking, coord-managed; values "
            "written at ingest under observed-value reconciliation)"
        )


def build_regular_coord_attrs(spec: Any | None, axis: Any) -> dict[str, Any]:
    """Build coord attrs without injecting xarray-owned CF encoding attrs."""
    minimal: dict[str, Any] = {"standard_name": "time", "axis": "T"}
    if spec is None or getattr(spec, "attrs", None) is None:
        return minimal
    merged = dict(minimal)
    merged.update({k: v for k, v in spec.attrs.items() if k not in RESERVED_ARRAY_ATTRS})
    merged.pop("units", None)
    merged.pop("calendar", None)
    return merged


def coord_to_datetime64(value: Any) -> np.datetime64:
    canonical = canonical_coordinate_value(value)
    if isinstance(canonical, str):
        canonical = canonical.removesuffix("Z")
        canonical = canonical.removesuffix("+00:00")
    return np.datetime64(canonical, "ns")
