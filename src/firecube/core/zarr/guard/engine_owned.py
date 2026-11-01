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

"""Engine-owned per-slot arrays: one writer, ``firecube zarr preallocate``.

An array declared ``ZarrArraySpec(engine_owned=True)`` holds one scalar per
slot. ``inspect_item`` supplies it as ``ItemInfo(values={name: scalar})``;
preallocate inspects every source item, checks the values, writes each array
whole under the global coord-materialization claim, creating it with
``ATTR_ENGINE_OWNED`` in its creation metadata and stamping
``ATTR_ENGINE_OWNED_COMPLETE`` last. An existing array without the marker is
pod-written, whatever it holds, and is never taken over.
Ingest pods never write these arrays: the pod side
(``verify_engine_owned_writes`` and
``require_engine_owned_materialized``) verifies write intents
against the stored values and refuses a target whose arrays are not
materialized. Slot-parallel writers therefore never share
a chunk of them, whatever the chunk layout.

Failures raise ``ConfigurationError`` for inconsistent plugin input (missing,
unknown or conflicting values) and ``SchemaDriftError`` when the store holds a
different value than the sources.
"""

from __future__ import annotations

import bisect
import decimal
import fractions
import numbers
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any

import numpy as np

from firecube.core.errors import (
    ConfigurationError,
    Message,
    Operation,
    Placeholder,
    SchemaDriftError,
)
from firecube.core.zarr._sealing_markers import (
    ATTR_ENGINE_OWNED,
    ATTR_ENGINE_OWNED_COMPLETE,
    ATTR_ENGINE_OWNED_SLOTS,
)
from firecube.core.zarr.guard.metadata_authority import MetadataAuthority, set_attributes


def exception_text(exc: BaseException) -> str:
    """Return the exception's `Message` when it was raised with one, else `str(exc)`.

    Use it to nest a caught refusal in a wrapper's message without losing its parts.
    """
    first = exc.args[0] if exc.args else None
    return first if isinstance(first, Message) else str(exc)


def preallocate_operation(**extra: object) -> Operation:
    """Return the preallocate recovery operation every unmaterialized-value refusal names.

    The CLI prints the command with placeholders for its required arguments; a library caller
    reads the function path. Keyword arguments add or replace arguments, such as a slot window.
    """
    arguments: dict[str, object] = {
        "plugin": Placeholder("<plugin>"),
        "target": Placeholder("<uri>"),
        "product_name": Placeholder("<name>"),
        "write_mode": Placeholder("<mode>"),
        "input_data": Placeholder("<path>"),
    }
    arguments.update(extra)
    return Operation("preallocate", **arguments)


def is_engine_owned_spec(arr_spec: Any) -> bool:
    """Return True when *arr_spec* is declared ``engine_owned=True``.

    The one test every site uses. ``is True`` (not truthiness) so a spec-like
    object without the field, or a stray non-bool, is never read as owned.
    """
    return getattr(arr_spec, "engine_owned", False) is True


def is_pod_written_slot_array(arr_spec: Any, *, time_coord_name: str) -> bool:
    """Return True when pods write slots of the array *arr_spec* declares.

    A time-indexed array that is neither engine-owned nor the time coordinate.
    Pods only verify engine-owned arrays, and a time coordinate that carries a
    preallocate marker; an unmarked coordinate, which pods do write, is left
    out as well. Static arrays have no slots.
    """
    return (
        bool(getattr(arr_spec, "time_indexed", True))
        and not is_engine_owned_spec(arr_spec)
        and arr_spec.name != time_coord_name
    )


def engine_owned_names_by_group(schema: Iterable[Any]) -> dict[str, tuple[str, ...]]:
    """Return ``{group: (array name, ...)}`` for arrays declared ``engine_owned=True``."""
    owned: dict[str, tuple[str, ...]] = {}
    for group_spec in schema:
        names = tuple(
            arr_spec.name for arr_spec in group_spec.arrays if is_engine_owned_spec(arr_spec)
        )
        if names:
            owned[str(group_spec.group)] = names
    return owned


def _stored_as_error(
    value: Any, dtype: np.dtype, *, array_path: str, origin: str, why: str
) -> ConfigurationError:
    return ConfigurationError(
        f"engine-owned array {array_path!r}: {origin} supplied {value!r}, which "
        f"cannot be stored as {dtype}: {why}"
    )


def _decimal_of(item: Any) -> decimal.Decimal:
    """Parse a ``str``/``bytes``/``Decimal`` exactly, or raise ``ValueError``."""
    try:
        if isinstance(item, bytes):
            item = item.decode("ascii")
        return item if isinstance(item, decimal.Decimal) else decimal.Decimal(item)
    except (decimal.InvalidOperation, UnicodeDecodeError) as exc:
        raise ValueError(f"not a number: {item!r}") from exc


def _exact_integer(item: Any, dtype: np.dtype) -> int:
    """Return *item* as a Python ``int`` within *dtype*'s range, or raise ``ValueError``.

    Non-finite values, fractions and out-of-range values are refused, judged on
    the value in its own representation (no narrowing through ``float``).
    """
    info = np.iinfo(dtype)
    if isinstance(item, (bool, numbers.Integral)):
        number = int(item)
    elif isinstance(item, (str, bytes, decimal.Decimal)):
        exact = _decimal_of(item)
        if not exact.is_finite():
            raise ValueError("not a finite number")
        if exact != exact.to_integral_value():
            raise ValueError("not a whole number")
        if not info.min <= exact <= info.max:
            raise ValueError(f"outside the range {info.min}..{info.max}")
        number = int(exact)
    elif isinstance(item, fractions.Fraction):
        if item.denominator != 1:
            raise ValueError("not a whole number")
        number = item.numerator
    elif isinstance(item, (float, np.floating)):
        if not np.isfinite(item):
            raise ValueError("not a finite number")
        if item != np.floor(item):
            raise ValueError("not a whole number")
        number = int(item)
    else:
        raise ValueError(f"not a number: {type(item).__name__}")
    if not info.min <= number <= info.max:
        raise ValueError(f"outside the range {info.min}..{info.max}")
    return number


def coerce_engine_owned_value(value: Any, dtype: Any, *, array_path: str, origin: str) -> Any:
    """Return *value* as a 0-d array of *dtype*, or raise ``ConfigurationError``.

    ``origin`` names where the value came from (an item, a write intent) for
    the error message.
    """
    payload = np.asarray(value)
    if payload.size != 1:
        raise ConfigurationError(
            f"engine-owned array {array_path!r} holds one scalar per slot; {origin} "
            f"supplied shape {payload.shape}"
        )
    target = np.dtype(dtype)
    scalar = payload.reshape(())
    try:
        if target.kind in "iu" and scalar.dtype.kind in "biufUSO":
            return np.asarray(_exact_integer(scalar.item(), target), dtype=target)
        if target.kind == "b" and scalar.dtype.kind != "b":
            raise ValueError("only bool and numpy.bool_ are stored in a bool array")
        converted = _cast_quietly(scalar, target)
        if target.kind == "f" and np.isinf(converted) and _is_finite_number(scalar.item()):
            raise ValueError("finite, but larger than the dtype can hold (it would become inf)")
        return converted
    except (TypeError, ValueError, ArithmeticError) as exc:
        raise _stored_as_error(
            value, target, array_path=array_path, origin=origin, why=str(exc)
        ) from exc


def _cast_quietly(scalar: np.ndarray, target: np.dtype) -> np.ndarray:
    """Cast with NumPy's overflow warning silenced; the caller inspects the result."""
    with np.errstate(over="ignore"):
        return np.asarray(scalar, dtype=target)


def _is_finite_number(item: Any) -> bool:
    """Return whether *item* is a finite real number in its own representation."""
    if isinstance(item, (bool, numbers.Integral, fractions.Fraction)):
        return True
    if isinstance(item, (str, bytes, decimal.Decimal)):
        try:
            return _decimal_of(item).is_finite()
        except ValueError:
            return False
    if isinstance(item, (float, np.floating)):
        return bool(np.isfinite(item))
    return False


def engine_owned_values_equal(left: Any, right: Any) -> bool:
    """Compare two scalars; NaN equals NaN and NaT equals NaT."""
    a = np.asarray(left)
    b = np.asarray(right)
    if a.dtype.kind in "fcmM" and b.dtype.kind in "fcmM":
        a_missing = bool(np.isnat(a)) if a.dtype.kind in "mM" else bool(np.isnan(a))
        b_missing = bool(np.isnat(b)) if b.dtype.kind in "mM" else bool(np.isnan(b))
        if a_missing or b_missing:
            return a_missing and b_missing
    return bool(a == b)


def is_engine_owned_reserved(arr: Any) -> bool:
    """Return whether *arr* carries ``ATTR_ENGINE_OWNED``: preallocate owns it.

    Decides who writes the array, complete or not.
    """
    return bool(arr.attrs.get(ATTR_ENGINE_OWNED, False))


def is_engine_owned_materialized(arr: Any) -> bool:
    """Return whether preallocate finished writing *arr* (``ATTR_ENGINE_OWNED_COMPLETE``)."""
    return is_engine_owned_reserved(arr) and arr.attrs.get(ATTR_ENGINE_OWNED_COMPLETE) is True


def engine_owned_creation_attrs() -> dict[str, Any]:
    """Return the attributes preallocate writes into an engine-owned array's creation metadata.

    ``ATTR_ENGINE_OWNED`` with an empty ``ATTR_ENGINE_OWNED_SLOTS``: the array
    is engine-owned from the write that creates it and no slot holds a value
    yet, so no crash point leaves an unmarked array behind.
    """
    return {ATTR_ENGINE_OWNED: True, ATTR_ENGINE_OWNED_SLOTS: []}


def engine_owned_adoption_error(array_path: str) -> SchemaDriftError:
    """Return the refusal for an existing unmarked array now declared ``engine_owned=True``."""
    return SchemaDriftError(
        f"array {array_path!r} is pod-written in the target (no {ATTR_ENGINE_OWNED} "
        "marker, whatever it holds) but now declared engine_owned=True; ownership changes "
        "are refused. Restore the declaration, or delete the array and re-run preallocate."
    )


def collect_engine_owned_values(
    *,
    ingestor: Any,
    plugin_ctx: Any,
    resolved_index: Any,
    dtypes: Mapping[str, Mapping[str, Any]],
    extents: Mapping[str, int],
    items: Iterable[Any] | None = None,
) -> dict[str, dict[str, dict[int, Any]]]:
    """Inspect every source item and return the engine-owned values per slot.

    Args:
        ingestor: The plugin instance (``discover_source_files``,
            ``filter_item``, ``inspect_item``).
        plugin_ctx: Context passed to the plugin hooks.
        resolved_index: Maps an item coordinate to its slot in each group.
        dtypes: ``{group: {array name: dtype}}`` of the engine-owned arrays.
        extents: Slot count of each group in *dtypes*; items positioned
            outside it are not part of the cube and are skipped.
        items: The items the command already discovered. ``None`` calls
            ``discover_source_files``.

    Returns:
        ``{group: {array name: {slot: 0-d value}}}``.

    Raises:
        ConfigurationError: An item's coordinate cannot be placed, an item
            lacks a value for an engine-owned array of its group, names an
            array that is not engine-owned, or two items on one slot disagree.
    """
    known = {name for names in dtypes.values() for name in names}
    collected: dict[str, dict[str, dict[int, Any]]] = {
        group: {name: {} for name in names} for group, names in dtypes.items()
    }
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
                    cause=exception_text(exc),
                )
            ) from exc
        if info is None:
            continue
        coordinate = info.coordinate if hasattr(info, "coordinate") else info
        values = getattr(info, "values", None) or {}
        unknown = sorted(set(values) - known)
        if unknown:
            raise ConfigurationError(
                f"inspect_item returned values for {unknown} for item {item!r}, which name "
                f"no engine-owned array; engine-owned arrays: {sorted(known)}"
            )
        for group, names in dtypes.items():
            try:
                slot = int(resolved_index.position(group, coordinate))
            except (KeyError, IndexError, TypeError, ValueError) as exc:
                raise ConfigurationError(
                    Message(
                        "inspect_item returned coordinate {coordinate!r} for item {item!r} "
                        "that could not be mapped to a slot of group {group!r}: {cause}",
                        coordinate=coordinate,
                        item=item,
                        group=group,
                        cause=exception_text(exc),
                    )
                ) from exc
            if not 0 <= slot < extents[group]:
                continue
            for name, dtype in names.items():
                array_path = f"{group}/{name}"
                if name not in values:
                    raise ConfigurationError(
                        f"inspect_item returned no value for engine-owned array "
                        f"{array_path!r} for item {item!r}; return "
                        f"ItemInfo(coordinate=..., values={{{name!r}: <scalar>}})"
                    )
                value = coerce_engine_owned_value(
                    values[name],
                    dtype,
                    array_path=array_path,
                    origin=f"slot {slot}: item {item!r}",
                )
                previous = collected[group][name].get(slot)
                if previous is not None and not engine_owned_values_equal(previous, value):
                    raise ConfigurationError(
                        f"engine-owned array {array_path!r} slot {slot}: two items disagree "
                        f"({previous} and {value}, the second from item {item!r})"
                    )
                collected[group][name][slot] = value
    return collected


class _SlotRanges:
    """Sorted, merged ``[start, end)`` slot ranges with binary-search membership."""

    __slots__ = ("_ends", "_starts")

    def __init__(self, pairs: Iterable[Sequence[int]]) -> None:
        spans = sorted((int(start), int(end)) for start, end in pairs)
        merged: list[list[int]] = []
        for start, end in spans:
            if end <= start:
                continue
            if merged and start <= merged[-1][1]:
                merged[-1][1] = max(merged[-1][1], end)
            else:
                merged.append([start, end])
        self._starts = [start for start, _ in merged]
        self._ends = [end for _, end in merged]

    def __contains__(self, slot: object) -> bool:
        index = bisect.bisect_right(self._starts, slot) - 1  # type: ignore[arg-type]
        return index >= 0 and slot < self._ends[index]  # type: ignore[operator]

    def with_slots(self, slots: Iterable[int]) -> list[list[int]]:
        """Return the ranges covering these ranges and *slots*, sorted and merged."""
        runs: list[list[int]] = []
        for slot in sorted(slots):
            if runs and runs[-1][1] == slot:
                runs[-1][1] = slot + 1
            else:
                runs.append([slot, slot + 1])
        return _SlotRanges([*zip(self._starts, self._ends, strict=True), *runs]).as_lists()

    def as_lists(self) -> list[list[int]]:
        return [[start, end] for start, end in zip(self._starts, self._ends, strict=True)]


def _written_slots(arr: Any) -> _SlotRanges:
    """Slots listed in ``ATTR_ENGINE_OWNED_SLOTS``; every slot when the list is absent."""
    ranges = arr.attrs.get(ATTR_ENGINE_OWNED_SLOTS)
    if ranges is None:
        return _SlotRanges([(0, int(arr.shape[0]))])
    return _SlotRanges(ranges)


def materialize_engine_owned_array(
    *,
    arr: Any,
    root: Any,
    authority: MetadataAuthority,
    array_path: str,
    values_by_slot: Mapping[int, Any],
    report: Callable[[str], None],
) -> None:
    """Write the discovered values into *arr* and record which slots now hold one.

    Ownership is read from the store, never inferred from contents:

    * an array without ``ATTR_ENGINE_OWNED`` is pod-written, whatever it holds
      (all fill values included), and that ownership change raises
      ``SchemaDriftError`` before anything is written. Preallocate creates
      engine-owned arrays with `engine_owned_creation_attrs` in their creation
      metadata, so every array it created carries the marker;
    * a slot listed in ``ATTR_ENGINE_OWNED_SLOTS`` must replay the stored value
      exactly (NaN, NaT and other fill values included), else
      ``SchemaDriftError`` before anything is written;
    * an unlisted slot (no source item seen before) takes the discovered value.

    The slots that need no write are recorded before the values are written,
    the remaining slots after, so a crash at any point leaves no stored value
    that a rerun could silently replace. ``ATTR_ENGINE_OWNED_COMPLETE`` is
    cleared before anything changes and stamped last, so ingest never takes
    an interrupted run for a finished one. The
    caller holds the global coord-materialization claim, so no other writer
    process touches the array.

    The read, merge and write run under this process's whole-array lock of
    *array_path* (`hold_array`), the payload write through `guarded_write`.
    *arr* is used for that payload only: every marker is written under
    *authority* through `metadata_authority.set_attributes`, which reads the
    node below *root* from the store first, never through *arr*.
    """
    # Function-scope import: guarded_write imports unit_ownership, which imports this module.
    from firecube.core.zarr.guard.guarded_write import WriteFootprint, guarded_write, hold_array

    def stamp(attrs: Mapping[str, Any]) -> None:
        set_attributes(authority, root, array_path, attrs)

    if not is_engine_owned_reserved(arr):
        raise engine_owned_adoption_error(array_path)
    with hold_array(authority.scope.store, array_path) as hold:
        stored = np.asarray(arr[...])
        fill_value = arr.fill_value
        written_slots = _written_slots(arr)
        merged = stored.copy()
        added: set[int] = set()
        changed = False
        for slot, desired in sorted(values_by_slot.items()):
            current = stored[slot]
            if slot in written_slots:
                if not engine_owned_values_equal(current, desired):
                    raise SchemaDriftError(
                        f"engine-owned array {array_path!r} slot {slot} drift: stored {current}, "
                        f"sources give {desired}. Delete the array or fix the plugin's values."
                    )
                continue
            if not (
                engine_owned_values_equal(current, desired)
                or engine_owned_values_equal(current, fill_value)
            ):
                raise SchemaDriftError(
                    f"engine-owned array {array_path!r} slot {slot} drift: stored {current} "
                    f"outside the written slots, sources give {desired}"
                )
            changed = changed or not engine_owned_values_equal(current, desired)
            merged[slot] = desired
            added.add(slot)
        # Slots already holding their final value (a fill-valued value such as NaN
        # included) are listed BEFORE the payload write, so no crash leaves them
        # looking like holes. Slots that still need a write are listed after it:
        # interrupted before, they hold the fill value and stay fillable; after, their
        # non-fill value is guarded by the "stored outside the written slots" rule.
        settled = {slot for slot in added if engine_owned_values_equal(stored[slot], merged[slot])}
        complete = is_engine_owned_materialized(arr)
        if complete and added:
            stamp({ATTR_ENGINE_OWNED_COMPLETE: False})
            complete = False
        # The marker and an empty list were written with the array's creation
        # metadata, so an array preallocate created is never marked without a list
        # (which counts as every slot written); only the list grows here.
        if settled:
            stamp({ATTR_ENGINE_OWNED_SLOTS: written_slots.with_slots(settled)})
        if changed:
            whole = WriteFootprint(hold.array_path, tuple((0, int(n)) for n in arr.shape))
            guarded_write(
                arr, whole, ..., merged, store=hold.store, write_empty_chunks=None, hold=hold
            )
        if added - settled:
            stamp({ATTR_ENGINE_OWNED_SLOTS: written_slots.with_slots(added)})
        if not complete:
            stamp({ATTR_ENGINE_OWNED_COMPLETE: True})
    report(
        f"array {array_path}: engine-owned values written for {len(added)} slot(s), "
        f"{len(values_by_slot) - len(added)} already stored"
    )


def require_engine_owned_materialized(root: Any | None, array_paths: Sequence[str]) -> None:
    """Refuse ingest when an engine-owned array is absent or not materialized.

    Ingest pods never write engine-owned arrays (``ZarrArraySpec(engine_owned=True)``);
    ``firecube zarr preallocate --input-data ...`` writes them whole and stamps
    ``firecube_engine_owned_complete`` once it finished; an array preallocate
    reserved (``firecube_engine_owned``) but never completed is refused too.
    Call this before any payload write.

    Args:
        root: The final target's root group opened read-only, or ``None`` when
            the target does not exist yet.
        array_paths: ``group/array`` paths of the engine-owned arrays.

    Raises:
        ConfigurationError: Naming every unmaterialized array and the
            preallocate command that writes them.

    Examples:
        >>> require_engine_owned_materialized(None, [])  # no engine-owned arrays
        >>> require_engine_owned_materialized(None, ["data/platform_lon"])  # doctest: +ELLIPSIS
        Traceback (most recent call last):
        ...
        firecube.core.errors.ConfigurationError: engine-owned arrays ['data/platform_lon'] ...
    """
    missing: list[str] = []
    for path in array_paths:
        arr = _array_or_none(root, path)
        if arr is None or not is_engine_owned_materialized(arr):
            missing.append(path)
    if missing:
        raise ConfigurationError(
            Message(
                "engine-owned arrays {missing} are not materialized in the target (absent, or "
                "a preallocate did not finish); pods only verify them. Write them first "
                "with: {preallocate}",
                missing=missing,
                array_path=tuple(missing),
                preallocate=preallocate_operation(),
            )
        )


def require_engine_owned_ownership(root: Any | None, declared: Sequence[tuple[str, bool]]) -> None:
    """Refuse ingest when declarations disagree with the ownership stored in the target.

    Who writes an array is decided by its ``firecube_engine_owned`` marker in
    the final target as much as by the current ``ZarrArraySpec(engine_owned=...)``:
    dropping the flag from a materialized array would let pods overwrite it,
    and adding it to a pod-written array would orphan the pods' data. Either
    change is refused before any payload write. Engine-owned arrays that do not
    exist yet are then checked by ``require_engine_owned_materialized``.

    Args:
        root: The final target's root group opened read-only, or ``None`` when
            the target does not exist yet.
        declared: ``(group/array, engine_owned)`` for every declared array.

    Raises:
        SchemaDriftError: An existing array's stored ownership differs from
            its declaration.
        ConfigurationError: A declared engine-owned array is not materialized.

    Examples:
        >>> require_engine_owned_ownership(None, [("data/radiance", False)])
    """
    changed: list[str] = []
    for path, engine_owned in declared:
        arr = _array_or_none(root, path)
        if arr is None:
            continue
        stored = is_engine_owned_reserved(arr)
        if stored != engine_owned:
            changed.append(
                f"{path} (stored {'engine-owned' if stored else 'pod-written'}, declared "
                f"{'engine-owned' if engine_owned else 'pod-written'})"
            )
    if changed:
        raise SchemaDriftError(
            f"array ownership changed for {changed}; ownership changes are refused. "
            "Restore the engine_owned declaration, or rebuild the array."
        )
    require_engine_owned_materialized(root, [path for path, owned in declared if owned])


def _slot_holds_no_value(array_path: str, slot: int) -> SchemaDriftError:
    return SchemaDriftError(
        Message(
            "engine-owned array {array_path!r} slot {slot} holds no value: "
            "preallocate saw no source item for it. Re-run {preallocate} "
            "with inputs that include this slot.",
            array_path=array_path,
            slot=slot,
            preallocate=preallocate_operation(),
        )
    )


def require_engine_owned_slots(root: Any | None, slots: Sequence[tuple[str, int]]) -> None:
    """Refuse pod writes to slots preallocate has not materialized.

    Each entry is ``(group/array, slot)``: an engine-owned array of the group
    a pod write addresses, and the slot it addresses. The slot must be listed
    in the array's ``firecube_engine_owned_slots``; an array stored without
    that list counts every slot as materialized. Checked whether or not the
    pod's item supplies a value for the array, so a payload never lands in a
    slot whose engine-owned values are missing. Nothing is written.

    Args:
        root: The final target's root group opened read-only (in staged mode
            the target, not the workspace), or ``None`` if it does not exist.
        slots: The engine-owned arrays and slots the batch addresses.

    Raises:
        ConfigurationError: An array is not materialized.
        SchemaDriftError: A slot is not materialized; the message names the
            array, the slot and the preallocate command that fills it.
    """
    by_array: dict[str, list[int]] = {}
    for array_path, slot in slots:
        by_array.setdefault(array_path, []).append(int(slot))
    for array_path, wanted in by_array.items():
        require_engine_owned_materialized(root, [array_path])
        arr = _array_or_none(root, array_path)
        assert arr is not None
        materialized = _written_slots(arr)
        for slot in sorted(wanted):
            if slot not in materialized:
                raise _slot_holds_no_value(array_path, slot)


def verify_engine_owned_writes(
    root: Any | None, writes: Sequence[tuple[str, str, int, Any]]
) -> None:
    """Verify pod writes aimed at engine-owned arrays instead of performing them.

    Each entry is ``(group, array, slot, value)``; ``value`` is a scalar, a
    one-element array, or a zero-arg callable returning one. The stored value
    must equal it (NaN equals NaN, NaT equals NaT) after conversion to the
    array's dtype. Nothing is written. Mirrors the verify-or-error handling of
    a preallocated time coordinate in ``RegionZarrWriter.write_timestamp``.
    Whether the slot is materialized at all is `require_engine_owned_slots`.

    Args:
        root: The final target's root group opened read-only (in staged mode
            the target, not the workspace), or ``None`` if it does not exist.
        writes: The intercepted writes.

    Raises:
        ConfigurationError: The array is not materialized, or a value is not a
            scalar of a compatible type.
        SchemaDriftError: A stored value differs, or a slot is not listed in
            ``firecube_engine_owned_slots`` (preallocate saw no source item for it).
    """
    for group, array_name, ts_index, value in writes:
        array_path = f"{group}/{array_name}"
        require_engine_owned_materialized(root, [array_path])
        arr = _array_or_none(root, array_path)
        assert arr is not None
        desired = coerce_engine_owned_value(
            value() if callable(value) else value,
            arr.dtype,
            array_path=array_path,
            origin=f"a write intent for slot {ts_index}",
        )
        current = np.asarray(arr[ts_index])
        if engine_owned_values_equal(current, desired):
            continue
        if ts_index not in _written_slots(arr):
            raise _slot_holds_no_value(array_path, ts_index)
        raise SchemaDriftError(
            f"engine-owned array {array_path!r} slot {ts_index} diverged: stored "
            f"{current}, incoming {desired}. Pods never overwrite engine-owned values."
        )


def _array_or_none(root: Any | None, path: str) -> Any | None:
    if root is None:
        return None
    try:
        return root[path]
    except KeyError:
        return None
