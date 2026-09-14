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

"""Failed-batch repair for the xarray-append Zarr path.

A batch on the append path is atomic per ``(batch, group)``. When a group's
write raises part-way, the store may hold a half-written region overwrite, a
partially extended tail, or a half-created fresh group. This module undoes
what can be undone and marks what cannot:

- fresh groups created by the failed batch are deleted;
- every append-dimension array extended past the pre-write cursor is resized
  back to it (zarr 3 ``resize`` drops the chunks outside the new shape);
- region slots the batch overwrote are marked ``failed_batch`` (state 3) so
  the next ``resume_existing`` run refills them in place.

The repair is best-effort: its errors are captured in :class:`RepairOutcome`
and the caller still records the failed span with its ranges, because the WAL,
not the array, is what says which slots are untrusted.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, cast

FAILED_BATCH_STATE = 3
"""State value written over the region slots of a failed batch."""

if TYPE_CHECKING:
    from firecube.core.filesystem.store_factory import ZarrStoreHandle


@dataclass
class TouchedSlots:
    """Slots one group's batch may have touched before it failed.

    Attributes:
        group: Zarr group path the batch wrote into.
        batch_start_cursor: Length of the append dimension before this batch
            wrote anything; tails are truncated back to it.
        region_slices: Region slices handed to region writes, in write order.
            Recorded before each write, so a write that raised half-way is
            still covered.
        region_coords: Append-coordinate-only datasets matching
            ``region_slices``; they give the failed span its time bounds.
        fresh_group: ``True`` when the batch created the group; the group is
            deleted on failure.
    """

    group: str
    batch_start_cursor: int
    region_slices: list[slice] = field(default_factory=list)
    region_coords: list[Any] = field(default_factory=list)
    fresh_group: bool = False

    def record_region(self, region: slice, ds: Any, append_dim: str) -> None:
        """Remember a region write about to happen.

        Args:
            region: Store slice along the append dimension being overwritten.
            ds: Dataset about to be written; only its append coordinate is
                kept so the batch's payload is not retained.
            append_dim: Name of the append dimension.
        """
        self.region_slices.append(region)
        self.region_coords.append(ds[[append_dim]] if append_dim in ds.coords else ds[[]])

    def region_ranges(self) -> list[list[int]]:
        """Return the region slices as inclusive ``[start, end]`` pairs."""
        ranges: list[list[int]] = []
        for item in self.region_slices:
            start = int(item.start or 0)
            stop = int(item.stop if item.stop is not None else start)
            if stop > start:
                ranges.append([start, stop - 1])
        return ranges


@dataclass
class RepairOutcome:
    """What the repair changed in the store, and what it could not.

    Attributes:
        state_marked_ranges: Inclusive index ranges now carrying state 3.
        truncated_to: Append-dimension length the group's arrays were resized
            to, or ``None`` when no array had grown past the cursor.
        group_removed: ``True`` when a half-created fresh group was deleted.
        error: Repair failure text, or ``None`` when every step succeeded.
    """

    state_marked_ranges: list[list[int]] = field(default_factory=list)
    truncated_to: int | None = None
    group_removed: bool = False
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Render the outcome for batch metrics and the span record."""
        return {
            "state_marked_ranges": [list(pair) for pair in self.state_marked_ranges],
            "truncated_to": self.truncated_to,
            "group_removed": self.group_removed,
            "error": self.error,
        }


@dataclass
class AppendBatchOutcome:
    """Everything the host needs to record one failed append batch truthfully.

    Attributes:
        committed: Coverage entries of the groups whose writes completed
            before the failure; they become active spans.
        failed_group: Group whose write raised.
        failed_entry: Coverage entry over the region slots the failed group
            touched (``write_strategy="append_failed"``), or ``None`` when
            nothing was written.
        repair: Result of :func:`repair_failed_group` for the failed group.
        not_attempted_groups: Groups after the failed one, never written.
        counters: The batch counters accumulated up to the failure, in the
            same shape ``append_time_groups`` reports on success.
    """

    committed: list[dict[str, Any]]
    failed_group: str
    failed_entry: dict[str, Any] | None
    repair: RepairOutcome
    not_attempted_groups: list[str]
    counters: dict[str, Any]


class AppendBatchFailed(RuntimeError):
    """One group's append raised; the batch outcome describes the store state.

    The original exception is chained as ``__cause__``; the message names
    the failed group and repeats the cause so operators see both.

    Args:
        outcome: Committed, failed and not-attempted groups of the batch.
        cause: The exception the group's write raised.
    """

    def __init__(self, outcome: AppendBatchOutcome, cause: BaseException) -> None:
        self.outcome = outcome
        detail = str(cause) or type(cause).__name__
        super().__init__(f"append failed in group {outcome.failed_group!r}: {detail}")


def _append_axis(array: Any, append_dim: str) -> int | None:
    dim_names = getattr(getattr(array, "metadata", None), "dimension_names", None)
    if not dim_names:
        return None
    names = [str(name) for name in dim_names]
    return names.index(append_dim) if append_dim in names else None


def _truncate_group(group: Any, *, append_dim: str, cursor: int) -> int | None:
    truncated_to: int | None = None
    for name in sorted(group.array_keys()):
        array = cast(Any, group[name])
        axis = _append_axis(array, append_dim)
        if axis is None or int(array.shape[axis]) <= cursor:
            continue
        new_shape = [int(size) for size in array.shape]
        new_shape[axis] = cursor
        array.resize(tuple(new_shape))
        truncated_to = cursor
    return truncated_to


def repair_failed_group(
    *,
    zarr_store: ZarrStoreHandle,
    group: str,
    append_dim: str,
    state_var_name: str,
    touched: TouchedSlots,
    logger: logging.Logger,
) -> RepairOutcome:
    """Undo the tail and mark the regions of a group whose batch write failed.

    Opens the store without consolidated metadata so the repair sees the
    arrays as they are on disk. A fresh group is deleted outright; an
    existing group has every append-dimension array resized back to
    ``touched.batch_start_cursor`` and its region slots set to state 3.

    Args:
        zarr_store: Handle of the store the batch wrote into.
        group: Zarr group path of the failed write.
        append_dim: Name of the append dimension.
        state_var_name: Name of the per-timestamp state array in the group.
        touched: Slots recorded while the batch was writing.
        logger: Receives one warning per repair step that failed.

    Returns:
        A :class:`RepairOutcome`; ``error`` is set when a step failed and the
        store may still hold the partial write.
    """
    import zarr

    outcome = RepairOutcome()
    errors: list[str] = []

    try:
        root = zarr.open_group(
            **zarr_store.zarr_kwargs(), mode="r+", zarr_format=3, use_consolidated=False
        )
    except Exception as exc:
        outcome.error = f"open failed: {type(exc).__name__}: {exc}"
        logger.warning("Failed-batch repair could not open group %r: %s", group, outcome.error)
        return outcome

    if touched.fresh_group:
        try:
            if group in root:
                del root[group]
                outcome.group_removed = True
        except Exception as exc:
            outcome.error = f"delete failed: {type(exc).__name__}: {exc}"
            logger.warning("Failed-batch repair could not delete group %r: %s", group, exc)
        return outcome

    try:
        zarr_group = cast(Any, root[group])
    except (KeyError, FileNotFoundError):
        return outcome

    try:
        outcome.truncated_to = _truncate_group(
            zarr_group, append_dim=append_dim, cursor=touched.batch_start_cursor
        )
    except Exception as exc:
        errors.append(f"truncate failed: {type(exc).__name__}: {exc}")
        logger.warning("Failed-batch repair could not truncate group %r: %s", group, exc)

    ranges = touched.region_ranges()
    if ranges:
        from firecube.core.zarr.state import update_timestamp_state

        try:
            update_timestamp_state(
                zarr_store=zarr_store,
                array_path=f"{group}/{state_var_name}",
                time_index_ranges=ranges,
                value=FAILED_BATCH_STATE,
            )
            outcome.state_marked_ranges = ranges
        except Exception as exc:
            errors.append(f"state update failed: {type(exc).__name__}: {exc}")
            logger.warning(
                "Failed-batch repair could not mark state %d over %r in group %r: %s",
                FAILED_BATCH_STATE,
                ranges,
                group,
                exc,
            )

    if errors:
        outcome.error = "; ".join(errors)
    return outcome
