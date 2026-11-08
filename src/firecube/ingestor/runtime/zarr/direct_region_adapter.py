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

"""DirectZarr write intents judged against one run's write session.

`DirectRegionAdapter` holds the checks `DirectZarrIngestor` runs around its
writes: the engine-owned split and verification, the declared-target guard,
window ownership of stored units, the staged-window refusal and the staged
ledger records. It reads the run's `ZarrWriteSession` and the declared schema;
it never interprets source items or plugin config. Template types appear only
under ``TYPE_CHECKING`` (the template imports this module).
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator, Sequence
from dataclasses import replace
from typing import TYPE_CHECKING, Any

from firecube.core.errors import ConfigurationError
from firecube.core.index_resolve import ExtentUnknownError
from firecube.core.zarr.guard._layout import resolve_effective_layout
from firecube.core.zarr.guard.engine_owned import (
    is_engine_owned_spec,
    is_pod_written_slot_array,
    require_engine_owned_ownership,
    require_engine_owned_slots,
    verify_engine_owned_writes,
)
from firecube.core.zarr.guard.unit_ownership import (
    StagedSlotWrite,
    require_planned_slots_fill_units,
    require_window_owns_stored_units,
    stored_write_unit,
)
from firecube.ingestor.runtime.zarr.strategies.indexed_region import (
    require_intents_in_slot_window,
)
from firecube.ingestor.runtime.zarr.write_session import ZarrWriteSession

if TYPE_CHECKING:
    from firecube.ingestor.templates.direct_zarr import WriteIntent, ZarrArraySpec, ZarrGroupSpec


def open_zarr_array(root: Any, path: str) -> Any | None:
    """``root[path]``, or ``None`` when the array is absent."""
    try:
        return root[path]
    except (KeyError, FileNotFoundError):
        return None


class DirectRegionAdapter:
    """One batch's view of a DirectZarr schema and intents as guarded-write checks."""

    def __init__(
        self,
        session: ZarrWriteSession,
        schema: Sequence[ZarrGroupSpec],
        *,
        time_coord_name: str,
        index_binding: Any | None = None,
    ) -> None:
        self._session = session
        self._schema = schema
        self._coord_name = time_coord_name
        self._binding = index_binding

    # --- declared targets and engine-owned arrays -----------------------------------

    @property
    def slot_scope(self) -> str | None:
        """The one group a slot window writes (``None``: every group, or no window).

        A slot window with a slot_group writes that group only, so the strategy
        skips the other groups' intents: so do the engine-owned checks.
        """
        return self._session.slot_group if self._session.window is not None else None

    def require_declared_targets(self, intents: Sequence[WriteIntent]) -> None:
        """Refuse write intents aimed at an array ``zarr_schema()`` does not declare.

        The ownership checks, the staged ledger and schema setup judge declared
        arrays only, so an undeclared target would be written unchecked. A
        ``timestamp`` intent always writes its group's time coordinate, whatever
        its ``array`` field holds, and is exempt.
        """
        declared: dict[str, set[str]] = {}
        for group_spec in self._schema:
            declared.setdefault(group_spec.group, set()).update(a.name for a in group_spec.arrays)
        undeclared = sorted(
            {
                (intent.group, intent.array, intent.kind)
                for intent in intents
                if intent.kind != "timestamp" and intent.array not in declared.get(intent.group, ())
            }
        )
        if not undeclared:
            return
        group, array, kind = undeclared[0]
        arrays = declared.get(group)
        group_note = (
            f"group {group!r} is not declared"
            if arrays is None
            else f"group {group!r} declares {sorted(arrays)}"
        )
        raise ConfigurationError(
            f"WriteIntent.{kind} targets '{group}/{array}', which zarr_schema() does not "
            f"declare ({group_note}); declare the array in zarr_schema() or stop emitting "
            "write intents for it"
        )

    def require_engine_owned_materialized(self, ctx: Any) -> None:
        """Refuse the run on an ownership change or an unmaterialized engine-owned array.

        Compares every declared array with the ownership marker stored in the
        final target, so a plugin that drops ``engine_owned=True`` cannot write
        over a materialized array. One check per run: preallocate cannot
        rewrite them while this run is live (its claim conflicts with every
        live ranged run).
        """
        if self._session.engine_owned_checked():
            return
        declared = [
            (f"{group_spec.group}/{arr_spec.name}", is_engine_owned_spec(arr_spec))
            for group_spec in self._schema
            for arr_spec in group_spec.arrays
        ]
        require_engine_owned_ownership(self._session.final_root(ctx), declared)
        self._session.mark_engine_owned_checked()

    def split_engine_owned(
        self, ctx: Any, intents: Sequence[WriteIntent]
    ) -> tuple[list[WriteIntent], list[WriteIntent]]:
        """``(owned, to_write)``: pods never write engine-owned arrays.

        Owned intents get the assigned-window / slot_group assertion the
        strategy gives pod-written ones (the strategy never sees them),
        whether or not the batch also holds pod-written intents. Those of the
        slot scope are then verified against the final target before any
        payload write; ``owned`` holds only those, and none is written.
        """
        owned_paths = {
            (group_spec.group, arr_spec.name)
            for group_spec in self._schema
            for arr_spec in group_spec.arrays
            if is_engine_owned_spec(arr_spec)
        }
        owned: list[WriteIntent] = []
        to_write: list[WriteIntent] = []
        for intent in intents:
            if (intent.group, intent.array) not in owned_paths:
                to_write.append(intent)
            elif intent.kind == "static":
                raise ConfigurationError(
                    f"WriteIntent.static targets engine-owned array "
                    f"'{intent.group}/{intent.array}'; preallocate writes it from "
                    "ItemInfo.values and pods only verify it per slot"
                )
            else:
                owned.append(intent)
        window = self._session.window
        if window is not None and owned:
            owned_by_group: dict[str, list[WriteIntent]] = {}
            for intent in owned:
                owned_by_group.setdefault(intent.group, []).append(intent)
            require_intents_in_slot_window(owned_by_group, window, self._session.slot_group)
        scope = self.slot_scope
        scoped = [i for i in owned if scope is None or i.group == scope]
        if scoped:
            verify_engine_owned_writes(
                self._session.final_root(ctx),
                [(i.group, i.array, i.ts_index, i.data) for i in scoped],
            )
        return scoped, to_write

    def require_engine_owned_slots(self, ctx: Any, intents: Sequence[WriteIntent]) -> None:
        """Refuse writes to a slot the engine-owned arrays of its group do not hold.

        Every slot-addressed intent of the slot scope counts, whether or not
        its item also emits an intent for the engine-owned array: a payload
        must not land in a slot preallocate never materialized. Read from the
        final target.
        """
        scope = self.slot_scope
        owned_by_group: dict[str, list[str]] = {}
        for group_spec in self._schema:
            for arr_spec in group_spec.arrays:
                if is_engine_owned_spec(arr_spec):
                    owned_by_group.setdefault(group_spec.group, []).append(arr_spec.name)
        slots = sorted(
            {
                (f"{intent.group}/{name}", int(intent.ts_index))
                for intent in intents
                if scope is None or intent.group == scope
                if intent.kind in ("region", "1d", "timestamp") and intent.ts_index is not None
                for name in owned_by_group.get(intent.group, ())
            }
        )
        if slots:
            require_engine_owned_slots(self._session.final_root(ctx), slots)

    def admit(
        self, ctx: Any, owned: Sequence[WriteIntent], to_write: Sequence[WriteIntent]
    ) -> None:
        """Before the staged ledger and any write: declared arrays, materialized slots."""
        self.require_declared_targets(to_write)
        self.require_engine_owned_slots(ctx, [*owned, *to_write])

    # --- stored-unit ownership -------------------------------------------------------

    def pod_written_specs(
        self, slot_group: str | None
    ) -> list[tuple[str, ZarrGroupSpec, ZarrArraySpec]]:
        """Time-indexed arrays this run writes slots of, in the groups its window covers.

        Engine-owned arrays and the time coordinate are left out: pods only
        verify engine-owned arrays, and a time coordinate that carries a
        preallocate marker. An unmarked coordinate, which pods do write, is
        left out as well. Static arrays have no slots.
        """
        return [
            (f"{group_spec.group}/{arr_spec.name}", group_spec, arr_spec)
            for group_spec in self._schema
            if slot_group is None or group_spec.group == slot_group
            for arr_spec in group_spec.arrays
            if is_pod_written_slot_array(arr_spec, time_coord_name=self._coord_name)
        ]

    def require_window_owns_stored_units(self, ctx: Any) -> None:
        """Refuse a slot window that holds part of a stored chunk or shard of the target.

        Runs at pod startup before schema setup, so a refused run leaves every
        file outside ``.firecube/`` as it was. Judged on the layout actually
        stored (declared or not); arrays the target lacks are created later
        with the engine's layout, which the capability gate has aligned.
        """
        window = self._session.window
        if window is None:
            return
        paths = [path for path, _, _ in self.pod_written_specs(self._session.slot_group)]
        require_window_owns_stored_units(self._session.final_root(ctx), paths, window)

    def refuse_sparse_staged_window(
        self,
        ctx: Any,
        planned_slots: frozenset[int],
        *,
        window: tuple[int, int],
        slot_group: str | None,
    ) -> None:
        """Refuse, before any batch, a staged window that fills only part of a unit.

        Promotion replaces whole objects, so every stored unit those slots
        touch must have all its slots in the run (target layout when the array
        exists there, else the layout the array will be created with).
        """
        specs = self.pod_written_specs(slot_group)
        if not planned_slots or not specs:
            return
        root = self._session.final_root(ctx)
        binding = self._binding
        units: list[tuple[str, tuple[int, ...], str, int]] = []
        for path, group_spec, arr_spec in specs:
            arr = open_zarr_array(root, path) if root is not None else None
            if arr is not None:
                if arr.ndim == 0:
                    continue
                unit_shape, unit_kind = stored_write_unit(arr)
                units.append((path, unit_shape, unit_kind, int(arr.shape[0])))
                continue
            try:
                time_len = (
                    int(binding.resolved.size(group_spec.group))
                    if binding is not None
                    else int(arr_spec.shape[0])
                )
            except ExtentUnknownError:
                continue
            layout = resolve_effective_layout(
                arr_spec, coord_name=self._coord_name, time_count=time_len
            )
            unit = layout.shards if layout.shards is not None else layout.chunks
            if unit is not None:
                kind = "shard" if layout.shards is not None else "chunk"
                units.append((path, tuple(int(n) for n in unit), kind, time_len))
        require_planned_slots_fill_units(units, planned_slots, window)

    # --- staged footprints -----------------------------------------------------------

    def absolute_slots(self, store_uri: str, intents: Sequence[WriteIntent]) -> list[WriteIntent]:
        """Return a staged batch's *intents* with every negative ``ts_index`` made absolute.

        The slot counts from the end of the workspace array the intent writes,
        read once here. The ledger records and the writer receives that slot,
        so a later batch that grows the array cannot change it. A
        ``timestamp`` intent counts on its group's time coordinate. A slot
        window refuses negative slots instead, as the strategy does; a run
        that is not staged gets *intents* unchanged.

        Raises:
            ConfigurationError: A negative slot addresses an array the
                workspace does not hold yet, or lies before its first slot.
            WriteIntentRangeError: In a slot window, a slot lies outside the window.
        """
        session = self._session
        slotted = ("region", "1d", "timestamp")
        if not session.staged or not any(
            intent.kind in slotted and intent.ts_index is not None and int(intent.ts_index) < 0
            for intent in intents
        ):
            return list(intents)
        window = session.window
        if window is not None:
            owned: dict[str, list[WriteIntent]] = {}
            for intent in intents:
                if session.slot_group is None or intent.group == session.slot_group:
                    owned.setdefault(intent.group, []).append(intent)
            require_intents_in_slot_window(owned, window, session.slot_group)
            return list(intents)
        root = session.staged_root(store_uri)
        lengths: dict[str, int | None] = {}
        out: list[WriteIntent] = []
        for intent in intents:
            if intent.kind not in slotted or intent.ts_index is None or int(intent.ts_index) >= 0:
                out.append(intent)
                continue
            name = self._coord_name if intent.kind == "timestamp" else intent.array
            path = f"{intent.group}/{name}"
            if path not in lengths:
                arr = open_zarr_array(root, path) if root is not None else None
                lengths[path] = int(arr.shape[0]) if arr is not None and arr.ndim > 0 else None
            length = lengths[path]
            slot = int(intent.ts_index) + (length or 0)
            if length is None or slot < 0:
                held = "does not hold it yet" if length is None else f"holds {length} slot(s)"
                raise ConfigurationError(
                    f"WriteIntent.{intent.kind} for '{path}' has ts_index={intent.ts_index}, "
                    f"which counts from the end of the array, but the staged workspace {held}; "
                    "emit an absolute slot instead"
                )
            out.append(replace(intent, ts_index=slot))
        return out

    def staged_footprints(
        self, intents: Sequence[WriteIntent]
    ) -> tuple[list[StagedSlotWrite], list[StagedSlotWrite]]:
        """Slot writes of a staged batch: ``(touched, written on success)``.

        A strided row selection is touched but never counts as written, so a
        unit it lands in is refused rather than promoted half-filled. Pass
        *intents* through :meth:`absolute_slots` first: a negative slot raises
        ``ValueError``.
        """
        window = self._session.window
        slot_group = self._session.slot_group
        # Same groups the strategy writes: slot_group narrows them only in a slot window.
        pod_written = {
            (group_spec.group, arr_spec.name)
            for _, group_spec, arr_spec in self.pod_written_specs(
                slot_group if window is not None else None
            )
        }
        touched: list[StagedSlotWrite] = []
        written: list[StagedSlotWrite] = []
        for intent in intents:
            if intent.kind not in ("region", "1d") or intent.ts_index is None:
                continue
            if window is not None and slot_group is not None and intent.group != slot_group:
                continue  # the strategy skips other groups' intents in a slot window
            if (intent.group, intent.array) not in pod_written:
                continue
            rows: tuple[int | None, int | None] | None = None
            contiguous = True
            if intent.kind == "region" and isinstance(intent.y_slice, slice):
                rows = (intent.y_slice.start, intent.y_slice.stop)
                contiguous = intent.y_slice.step in (None, 1)
            channel = intent.channel_index if intent.kind == "region" else None
            write = StagedSlotWrite(
                array_path=f"{intent.group}/{intent.array}",
                ts_index=int(intent.ts_index),
                rows=rows,
                channel=None if channel is None else int(channel),
            )
            touched.append(write)
            if contiguous:
                written.append(write)
        return touched, written

    @contextlib.contextmanager
    def writing(self, store_uri: str, intents: Sequence[WriteIntent]) -> Iterator[None]:
        """Run the batch's writes as one admitted batch of the session.

        In a staged run the touched units are recorded before the writes and
        the written ones only when the body returns normally. Every
        pod-written array of the schema is recorded as a ledger array, whose
        uncovered data objects promotion refuses.
        """
        with self._session.admit_write():
            if not self._session.staged:
                yield
                return
            touched, written = self.staged_footprints(intents)
            arrays = [path for path, _, _ in self.pod_written_specs(None)]
            self._session.record_touched(store_uri, touched, arrays=arrays)
            yield
            if written:
                self._session.record_written(store_uri, written)
