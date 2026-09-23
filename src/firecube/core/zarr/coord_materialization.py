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
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import numpy as np

from firecube.core.encoded_time import encode_coordinate, is_gregorian_axis
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
from firecube.core.zarr._calendar_guard import reject_non_gregorian_calendar_value
from firecube.core.zarr._coord_chunks import resolve_coord_chunks
from firecube.core.zarr._coord_lifecycle import assert_coord_markers_consistent
from firecube.core.zarr._reserved_attrs import (
    FIRECUBE_GROUP_IDENTITY_HASH_ATTR,
    RESERVED_ARRAY_ATTRS,
)
from firecube.core.zarr._sealing_markers import ATTR_COORD_MANAGED, ATTR_PREALLOCATED
from firecube.core.zarr.region_writer import RegionZarrWriter, _array_is_all_fill, _fill_mask

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


def _validate_calendar_spec_attrs(
    spec: Any | None, *, axis_units: str, axis_calendar: str, coord_path: str
) -> None:
    """Raise when a plugin's ``ZarrArraySpec.attrs`` contradicts its calendar axis.

    Equal ``units``/``calendar`` values in ``spec.attrs`` are fine (they are
    stripped and reinstated from the axis either way, by
    `build_regular_coord_attrs`/`build_irregular_coord_attrs`). A *different*
    value is a caller-input error: the plugin declared a coordinate array
    that disagrees with its own time axis's CF encoding.

    Raises:
        ConfigurationError: If ``spec.attrs`` declares ``units`` or
            ``calendar`` different from *axis_units*/*axis_calendar*.
    """
    if spec is None or getattr(spec, "attrs", None) is None:
        return
    spec_calendar = spec.attrs.get("calendar")
    if spec_calendar is not None and spec_calendar != axis_calendar:
        raise ConfigurationError(
            f"array spec for {coord_path!r} declares attrs['calendar']={spec_calendar!r}, "
            f"which contradicts its time axis's calendar={axis_calendar!r}. Remove the "
            "attr, or make it match the axis."
        )
    spec_units = spec.attrs.get("units")
    if spec_units is not None and spec_units != axis_units:
        raise ConfigurationError(
            f"array spec for {coord_path!r} declares attrs['units']={spec_units!r}, "
            f"which contradicts its time axis's derived units={axis_units!r}. Remove the "
            "attr, or make it match the axis."
        )


def _resolve_calendar_target_dtype(
    *,
    spec: Any | None,
    calendar: str,
    values_are_integral: bool,
    coord_path: str,
) -> np.dtype[Any]:
    """Resolve and validate the on-disk dtype for an encoded calendar coordinate.

    Defaults to ``int64`` when every value is integral, else ``float64``. A
    plugin ``ZarrArraySpec`` may declare ``int64`` or ``float64`` explicitly
    (honored); any other dtype -- including ``datetime64``, which contradicts
    the axis's calendar declaration -- is a caller-input error.

    Raises:
        ConfigurationError: If ``spec.dtype`` is ``datetime64``, is neither
            ``int64`` nor ``float64``, or is ``int64`` while the resolved
            values are not all integral.
    """
    default_dtype = np.dtype("int64") if values_are_integral else np.dtype("float64")
    spec_dtype_raw = (
        spec.dtype if spec is not None and getattr(spec, "dtype", None) is not None else None
    )
    if spec_dtype_raw is None:
        return default_dtype
    spec_dtype = np.dtype(spec_dtype_raw)
    if spec_dtype.kind == "M":
        raise ConfigurationError(
            f"array spec for {coord_path!r} declares datetime64 dtype {spec_dtype!s}, but its "
            f"time axis declares calendar={calendar!r}; a calendar coordinate is stored as an "
            "encoded number, not datetime64. Declare int64 or float64 in ZarrArraySpec.dtype, "
            "or omit dtype to use the default."
        )
    if spec_dtype not in (np.dtype("int64"), np.dtype("float64")):
        raise ConfigurationError(
            f"array spec for {coord_path!r} declares dtype {spec_dtype!s}, which is not "
            f"supported for a calendar={calendar!r} coordinate; use int64 or float64."
        )
    if spec_dtype == np.dtype("int64") and not values_are_integral:
        raise ConfigurationError(
            f"array spec for {coord_path!r} declares dtype int64, but the resolved axis "
            f"values for calendar={calendar!r} are not all integral; use float64 instead."
        )
    return spec_dtype


def _encoded_fill_value(dtype: np.dtype[Any]) -> Any:
    """Return the fill sentinel for an encoded calendar coordinate's dtype.

    ``NaN`` for a float dtype, else the ``int64`` minimum -- deliberately the
    same choice `RegionZarrWriter`/``firecube.core.controlplane.deletion``
    already use for a "clearly not real data" integer sentinel.
    """
    if dtype.kind == "f":
        return float("nan")
    return int(np.iinfo(np.int64).min)


def _scalar_is_fill(value: Any, fill_value: Any) -> bool:
    """Return whether a single stored value equals its dtype's fill sentinel."""
    return _array_is_all_fill(np.atleast_1d(np.asarray(value)), fill_value)


@dataclass(frozen=True)
class CoordinateEncoding:
    """The on-disk storage encoding for a time coordinate array.

    Firecube stores a time coordinate one of two ways: Gregorian time as
    ``datetime64``, or a non-Gregorian CF calendar as an encoded
    ``int64``/``float64`` number with ``units``/``calendar`` attrs. Every
    technical choice that distinction implies -- dtype, fill sentinel, extra
    CF attrs, and how a plugin value becomes a stored one -- is captured
    once here instead of being re-derived at each call site from
    ``axis.calendar is None`` or ``dtype.kind == "M"``.

    Built by `coordinate_encoding_for` (from a time axis, at materialization
    time) or `coordinate_encoding_from_array` (from an opened array's
    dtype/attrs, at write time); both dispatch on the same rule and produce
    an equivalent instance for the same coordinate.

    Attributes:
        dtype: The array's on-disk dtype.
        fill_value: The unfilled-slot sentinel, in *dtype*.
        extra_attrs: CF attrs this encoding requires beyond a coordinate's
            usual minimal attrs -- ``{"units": ..., "calendar": ...}`` for
            an encoded axis, ``{}`` for Gregorian.
        encode_values: Convert a sequence of plugin coordinate values to a
            dense array in *dtype*.
        encode_scalar: Convert one plugin coordinate value to a stored
            scalar in *dtype*.
        is_fill: Vectorised fill-sentinel mask over a stored array.
        scalar_is_fill: Whether one stored scalar is the fill sentinel.
        values_equal: Whether two stored scalars are equal, fill-aware
            (NaT counts as equal to NaT for Gregorian; an encoded axis's
            fill is a real, directly comparable number).
    """

    dtype: np.dtype[Any]
    fill_value: Any
    extra_attrs: dict[str, Any]
    encode_values: Callable[[Sequence[Any] | np.ndarray], np.ndarray]
    encode_scalar: Callable[[Any], Any]
    is_fill: Callable[[np.ndarray], np.ndarray]
    scalar_is_fill: Callable[[Any], bool]
    values_equal: Callable[[Any, Any], bool]


def _gregorian_encoding(dtype: np.dtype[Any]) -> CoordinateEncoding:
    """Build the Gregorian (``datetime64``) `CoordinateEncoding` for *dtype*."""
    fill_value = np.array(np.datetime64("NaT", "ns"), dtype=dtype)[()]
    return CoordinateEncoding(
        dtype=dtype,
        fill_value=fill_value,
        extra_attrs={},
        encode_values=lambda values: np.array(
            [coord_to_datetime64(value) for value in values], dtype="datetime64[ns]"
        ).astype(dtype),
        encode_scalar=lambda value: np.array(coord_to_datetime64(value), dtype=dtype)[()],
        is_fill=lambda arr: np.isnat(arr),
        scalar_is_fill=lambda value: bool(np.isnat(np.asarray(value))),
        values_equal=lambda a, b: (bool(np.isnat(a)) and bool(np.isnat(b))) or bool(a == b),
    )


def _encoded_encoding(dtype: np.dtype[Any], *, units: str, calendar: str) -> CoordinateEncoding:
    """Build the encoded (calendar) `CoordinateEncoding` for *dtype*/*units*/*calendar*."""
    fill_value = _encoded_fill_value(dtype)
    return CoordinateEncoding(
        dtype=dtype,
        fill_value=fill_value,
        extra_attrs={"units": units, "calendar": calendar},
        encode_values=lambda values: np.asarray(values, dtype=dtype),
        encode_scalar=lambda value: encode_coordinate(value, units=units, calendar=calendar),
        is_fill=lambda arr: _fill_mask(arr, fill_value),
        scalar_is_fill=lambda value: _scalar_is_fill(value, fill_value),
        values_equal=lambda a, b: bool(a == b),
    )


def _axis_values_are_integral(axis: Any) -> bool:
    """Return whether *axis*'s encoded values are all integral.

    An `IrregularTimeAxis` with ``calendar`` set stores each value
    canonicalised to a Python ``int`` or ``float`` at construction (see
    ``IrregularTimeAxis.__post_init__``); its concrete ``values`` are
    checked directly. A `RegularTimeAxis` has no ``values`` sequence -- its
    encoded values are ``n * cadence_s`` for a positive integer
    ``cadence_s``, always integral.
    """
    values = getattr(axis, "values", None)
    if values is None:
        return True
    return all(isinstance(value, int) for value in values)


def coordinate_encoding_for(
    axis: Any, spec: Any | None, *, coord_path: str | None = None
) -> CoordinateEncoding:
    """Build the storage encoding a time axis materializes to.

    Dispatches once, on `firecube.core.encoded_time.is_gregorian_axis`: a
    Gregorian axis (``axis.calendar`` unset, or normalising to a
    Gregorian-like name) stores ``datetime64``; any other calendar stores
    an encoded ``int64``/``float64`` number in the axis's CF ``units``.

    Args:
        axis: A ``RegularTimeAxis`` or ``IrregularTimeAxis``.
        spec: The plugin's ``ZarrArraySpec`` for this coordinate, or
            ``None``. A declared ``dtype`` is honored (Gregorian) or
            validated against the axis's calendar (encoded) -- see
            `_resolve_calendar_target_dtype`.
        coord_path: Store path used only to name this coordinate in
            `ConfigurationError` messages raised by dtype/attrs validation.
            Defaults to ``axis.coordinate``.

    Returns:
        The `CoordinateEncoding` this axis materializes to.

    Raises:
        ConfigurationError: See `_validate_calendar_spec_attrs` and
            `_resolve_calendar_target_dtype`.
    """
    path = coord_path if coord_path is not None else axis.coordinate

    if is_gregorian_axis(axis):
        target_dtype = (
            np.dtype(spec.dtype)
            if spec is not None and getattr(spec, "dtype", None) is not None
            else np.dtype("datetime64[ns]")
        )
        return _gregorian_encoding(target_dtype)

    calendar = axis.calendar
    units = getattr(axis, "encoded_units", None)
    if units is None:
        units = axis.units
    _validate_calendar_spec_attrs(spec, axis_units=units, axis_calendar=calendar, coord_path=path)
    target_dtype = _resolve_calendar_target_dtype(
        spec=spec,
        calendar=calendar,
        values_are_integral=_axis_values_are_integral(axis),
        coord_path=path,
    )
    return _encoded_encoding(target_dtype, units=units, calendar=calendar)


def coordinate_encoding_from_array(
    dtype: np.dtype[Any] | str, attrs: Mapping[str, Any]
) -> CoordinateEncoding:
    """Build the storage encoding an existing coordinate array already uses.

    Mirrors `coordinate_encoding_for`'s branch rule from what an opened
    array already carries, for a caller -- the region writer -- that only
    has an array, not the axis that created it: a non-``datetime64`` dtype
    whose attrs carry both ``units`` and ``calendar`` is an encoded
    (calendar) coordinate; any other combination is Gregorian.

    Args:
        dtype: The coordinate array's on-disk dtype.
        attrs: The coordinate array's attrs.

    Returns:
        The `CoordinateEncoding` matching *dtype*/*attrs*.
    """
    resolved_dtype = np.dtype(dtype)
    if resolved_dtype.kind != "M" and "units" in attrs and "calendar" in attrs:
        return _encoded_encoding(
            resolved_dtype, units=str(attrs["units"]), calendar=str(attrs["calendar"])
        )
    return _gregorian_encoding(resolved_dtype)


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
    `coordinate_encoding_for`): a Gregorian axis (``axis.calendar`` unset)
    stores ``datetime64``, defaulting to ``datetime64[ns]`` unless *spec*
    declares a dtype; a calendar axis stores an encoded ``int64``/``float64``
    number, since ``axis.values`` are already canonical encoded numbers (see
    `IrregularTimeAxis`) rather than raw datetimes. Shape is
    ``(len(axis.values),)`` either way.

    Behavior on existing array:

    * Values match the resolved axis → idempotent no-op; the
      ``ATTR_PREALLOCATED`` marker is stamped if not already present so
      subsequent ``write_timestamp`` calls take the marker-aware path.
    * Values differ and the existing array is entirely fill (NaT for
      Gregorian, the encoding's fill sentinel otherwise -- a spec-loop
      pre-allocated shell) → fill values, merge attrs, stamp marker.
    * Values differ and the existing array holds non-fill content → drift
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
    coord_path = f"{group_name}/{axis.coordinate}"
    encoding = coordinate_encoding_for(axis, spec, coord_path=coord_path)
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
        if not bool(np.all(encoding.is_fill(existing_values))):
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
        fill_value=encoding.fill_value,
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
    the xarray-CF-encoding-owned keys ``units`` and ``calendar``. For a
    non-Gregorian axis (see
    `firecube.core.encoded_time.is_gregorian_axis`), ``units`` (from
    ``axis.units``) and ``calendar`` (from ``axis.calendar``) are then
    reinstated from the axis -- the plugin-declared values are stripped
    either way, since CF encoding is axis-owned, not plugin-owned.
    """
    minimal: dict[str, Any] = {"standard_name": "time", "axis": "T"}
    merged = dict(minimal)
    if spec is not None and getattr(spec, "attrs", None) is not None:
        merged.update({k: v for k, v in spec.attrs.items() if k not in RESERVED_ARRAY_ATTRS})
        merged.pop("units", None)
        merged.pop("calendar", None)
    if not is_gregorian_axis(axis):
        merged["units"] = axis.units
        merged["calendar"] = axis.calendar
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


def _fill_existing_regular_grid_gregorian(
    *,
    coord_path: str,
    existing: Any,
    target_dtype: np.dtype[Any],
    values: np.ndarray,
    slot_start: int,
    effective_slot_end: int,
    window_label: str,
    group_identity_hash: str,
    report: Callable[[str], None],
) -> None:
    """Fill NaT slots of an existing Gregorian grid coordinate, within its window.

    Grid values are deterministic and the caller holds the global
    materialization claim, so filling a stored NaT with the nominal value is
    always safe: windowed and full prefills converge to the same array. Only
    a stored non-NaT value that differs from the nominal grid is drift.
    """
    if bool(existing.attrs.get(ATTR_COORD_MANAGED, False)):
        raise SchemaDriftError(
            f"coordinate array {coord_path} carries {ATTR_COORD_MANAGED}: its "
            "values are engine-materialized observed times. Refusing to "
            "overwrite them with the nominal grid."
        )
    window_slice = slice(slot_start, effective_slot_end)
    stored_window = np.asarray(existing[window_slice])
    expected_window = np.asarray(values[window_slice])
    filled = 0
    for offset, (stored_value, incoming_value) in enumerate(
        zip(stored_window.flat, expected_window.flat, strict=True)
    ):
        stored_norm = RegionZarrWriter._normalize_for_coord_compare(stored_value, target_dtype)
        if np.isnat(stored_norm):
            stored_window[offset] = incoming_value
            filled += 1
            continue
        incoming_norm = RegionZarrWriter._normalize_for_coord_compare(incoming_value, target_dtype)
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
        log.log(NOTICE_LEVEL, "reconciled %s NaT grid coord slot(s) in %s", filled, coord_path)
        report(
            f"array {coord_path}: filled {filled} NaT slot(s) with nominal grid "
            f"values{window_label}"
        )
    else:
        report(f"array {coord_path}: no-op (matches nominal grid){window_label}")


def _fill_existing_regular_grid_encoded(
    *,
    coord_path: str,
    existing: Any,
    encoding: CoordinateEncoding,
    values: np.ndarray,
    slot_count: int,
    window_suffix: str,
    group_identity_hash: str,
    report: Callable[[str], None],
) -> None:
    """Fill fill-sentinel slots of an existing encoded grid coordinate.

    A regular calendar coord array is engine-owned and is always fully
    materialized regardless of ``slot_start``/``slot_end`` (see
    `materialize_regular_coord_array`): an unwritten slot holds the
    encoding's fill sentinel, which xarray cannot decode as a CF time value.
    """
    if bool(existing.attrs.get(ATTR_COORD_MANAGED, False)):
        raise SchemaDriftError(
            f"coordinate array {coord_path} carries {ATTR_COORD_MANAGED}: calendar axes "
            "only support the grid policy, so an engine-managed observed shell here "
            "means the store was edited out of band or the schema changed underneath it."
        )
    stored_full = np.asarray(existing[:])
    filled = 0
    for slot_index, (stored_value, incoming_value) in enumerate(
        zip(stored_full.flat, values.flat, strict=True)
    ):
        if encoding.scalar_is_fill(stored_value):
            stored_full[slot_index] = incoming_value
            filled += 1
            continue
        if not encoding.values_equal(stored_value, incoming_value):
            raise SchemaDriftError(
                f"coordinate array {coord_path} diverged from nominal grid at slot "
                f"{slot_index}: stored {stored_value!r}, incoming {incoming_value!r}"
            )
    if filled:
        existing[:] = stored_full
    if not bool(existing.attrs.get(ATTR_PREALLOCATED, False)):
        existing.attrs[ATTR_PREALLOCATED] = True
    existing.attrs[FIRECUBE_GROUP_IDENTITY_HASH_ATTR] = group_identity_hash
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
) -> None:
    """Materialize a dense regular time coordinate at ``{group_name}/{axis.coordinate}``.

    The branch is decided by the axis's stored-values policy:
    ``"grid"`` means the nominal grid IS
    the coordinate, so values are prefilled and the array is sealed with
    ``firecube_preallocated`` and ingest writes become verify-only no-ops;
    ``"observed"`` means the stored values are real observation times
    unknowable before ingest, so the array is created at the dense chunk
    shape but left NaT and unsealed.

    Storage follows the axis's `CoordinateEncoding` (see
    `coordinate_encoding_for`): with ``axis.calendar`` set, the coordinate is
    an encoded ``int64``/``float64`` number (``slot n == n * cadence_s`` in
    the axis's derived ``units``), not ``datetime64``. A calendar axis only
    ever resolves to the ``"grid"`` policy (`RegularTimeAxis` construction
    rejects ``mode="floor"`` with ``calendar`` set), so the observed-value
    machinery described above never runs for a calendar axis -- asserted
    below rather than left to fall through into it. A calendar coordinate is
    also always fully materialized regardless of ``slot_start``/``slot_end``:
    an unwritten slot would hold the encoding's fill sentinel, which xarray
    cannot decode as a CF time value; the window still labels the report (and
    downstream data-array preallocation), but never gates which coord slots
    are written.

    Raises:
        ValueError: If ``axis.slot_count`` is ``None`` and *resolved_index*
            is also ``None``, or an existing array's shape does not match.
        ConfigurationError: If a calendar axis's resolved policy is not
            ``"grid"`` (should be unreachable), or if ``spec``/``axis``
            disagree on dtype or ``units``/``calendar`` attrs (see
            `_resolve_calendar_target_dtype`, `_validate_calendar_spec_attrs`).
        SchemaDriftError: If an existing coordinate array's markers or stored
            values conflict with this materialization.
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

    coord_path = f"{group_name}/{axis.coordinate}"
    is_calendar_axis = not is_gregorian_axis(axis)
    coordinate_policy = effective_regular_time_policy(axis)

    # Checked before resolving the encoding (which touches axis.units/
    # axis.encoded_units): RegularTimeAxis construction already rejects
    # mode='floor' together with calendar, but a hand-built axis-like object
    # could still reach here without those attributes, and this guard must
    # fire before anything else looks for them.
    if is_calendar_axis and coordinate_policy != "grid":
        calendar = axis.calendar
        raise ConfigurationError(
            f"calendar={calendar!r} regular axis for group {group_name!r} resolved to "
            f"policy={coordinate_policy!r}; calendar axes only support mode='exact' "
            "(policy='grid'). This should be unreachable because RegularTimeAxis "
            "construction rejects mode='floor' together with calendar; report this as a "
            "defect if you see it."
        )

    encoding = coordinate_encoding_for(axis, spec, coord_path=coord_path)
    target_dtype = encoding.dtype

    if is_calendar_axis:
        values_int = np.arange(slot_count, dtype=np.int64) * int(axis.cadence_s)
        values = encoding.encode_values(values_int)
    else:
        epoch = coord_to_datetime64(axis.epoch)
        cadence = np.timedelta64(int(axis.cadence_s * 1e9), "ns")
        values = (epoch + np.arange(slot_count, dtype=np.int64) * cadence).astype(target_dtype)

    attrs = build_regular_coord_attrs(spec, axis)
    group_identity_hash = compute_group_identity_hash(axis, int(slot_count), target_dtype)

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

    is_full_grid = slot_start == 0 and effective_slot_end == slot_count
    window_label = "" if is_full_grid else f" in window [{slot_start}, {effective_slot_end})"
    window_suffix = "" if is_full_grid else "; window not applied to calendar coordinate"

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

        if prefill:
            if is_calendar_axis:
                _fill_existing_regular_grid_encoded(
                    coord_path=coord_path,
                    existing=existing,
                    encoding=encoding,
                    values=values,
                    slot_count=slot_count,
                    window_suffix=window_suffix,
                    group_identity_hash=group_identity_hash,
                    report=report,
                )
            else:
                _fill_existing_regular_grid_gregorian(
                    coord_path=coord_path,
                    existing=existing,
                    target_dtype=target_dtype,
                    values=values,
                    slot_start=slot_start,
                    effective_slot_end=effective_slot_end,
                    window_label=window_label,
                    group_identity_hash=group_identity_hash,
                    report=report,
                )
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
        fill_value=encoding.fill_value,
        chunks=resolve_coord_chunks(spec, slot_count),
        attrs=attrs,
        dimension_names=(axis.coordinate,),
    )
    if prefill:
        if is_calendar_axis:
            arr[...] = values
            arr.attrs[ATTR_PREALLOCATED] = True
            arr.attrs[FIRECUBE_GROUP_IDENTITY_HASH_ATTR] = group_identity_hash
            report(f"array {coord_path}: created (regular coord materialization){window_suffix}")
        else:
            if slot_start == 0 and effective_slot_end == slot_count:
                arr[...] = values
            else:
                arr[slice(slot_start, effective_slot_end)] = values[slot_start:effective_slot_end]
            arr.attrs[ATTR_PREALLOCATED] = True
            arr.attrs[FIRECUBE_GROUP_IDENTITY_HASH_ATTR] = group_identity_hash
            report(f"array {coord_path}: created (regular coord materialization){window_label}")
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
    """Build coord attrs without injecting xarray-owned CF encoding attrs.

    For a non-Gregorian axis (see
    `firecube.core.encoded_time.is_gregorian_axis`), ``units`` (derived from
    ``axis.epoch``, see `RegularTimeAxis.encoded_units`) and ``calendar`` are
    reinstated from the axis after the strip below -- CF encoding is
    axis-owned, not plugin-owned, so a plugin-declared ``units``/``calendar``
    in ``spec.attrs`` is stripped either way.
    """
    minimal: dict[str, Any] = {"standard_name": "time", "axis": "T"}
    if spec is None or getattr(spec, "attrs", None) is None:
        merged = dict(minimal)
    else:
        merged = dict(minimal)
        merged.update({k: v for k, v in spec.attrs.items() if k not in RESERVED_ARRAY_ATTRS})
        merged.pop("units", None)
        merged.pop("calendar", None)
    if not is_gregorian_axis(axis):
        merged["units"] = axis.encoded_units
        merged["calendar"] = axis.calendar
    return merged


def coord_to_datetime64(value: Any) -> np.datetime64:
    """Convert a plugin-supplied coordinate value to ``datetime64[ns]``.

    Raises:
        ValueError: If *value* is calendar-valued (see
            `firecube.core.encoded_time.is_calendar_valued`) on a
            non-Gregorian calendar; see
            `firecube.core.zarr._calendar_guard.reject_non_gregorian_calendar_value`.
            Gregorian-like calendar values keep converting as before.
    """
    reject_non_gregorian_calendar_value(value)
    canonical = canonical_coordinate_value(value)
    if isinstance(canonical, str):
        canonical = canonical.removesuffix("Z")
        canonical = canonical.removesuffix("+00:00")
    return np.datetime64(canonical, "ns")
