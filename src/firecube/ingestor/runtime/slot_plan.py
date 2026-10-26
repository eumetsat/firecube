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

"""Plan the slot windows of a product for parallel ingestion.

The host is a plugin that declares an index spec and a Zarr schema (`PreallocateHost`).
`plan_slots` returns one `GroupSlotPlan` per group of the plugin's resolved index.
Configure the plugin with `configure_ingestor` first. The plan reads the target and
its control plane and writes neither.

Slot windows are half-open `[start, end)`: `start` is the first slot, `end` the
slot after the last. The control plane stores span coverage inclusive; the
coverage lookup converts it.
"""

from __future__ import annotations

import contextlib
import logging
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from firecube.core.controlplane import ChunkManager
from firecube.core.errors import (
    CommandName,
    FirecubeError,
    Message,
    Operation,
    Param,
    Placeholder,
)
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.ingestor.errors import ConfigurationError

if TYPE_CHECKING:
    from firecube.core.config import StorageConfig
    from firecube.core.product.identity import ProductIdentity
    from firecube.ingestor.contracts.interfaces import PreallocateHost
    from firecube.ingestor.templates.direct_zarr import ZarrGroupSpec
    from firecube.ingestor.types.context import PluginContext

#: Ranges in the whole plan, all groups, above which the plan is refused unless confirmed.
PLAN_RANGE_CEILING = 1_000_000


class SlotPlanError(ConfigurationError):
    """The slot plan cannot be built from the plugin, the target or its coverage."""


class SlotPlanBlockedError(SlotPlanError):
    """Chunk alignment leaves remaining slots that no executable window covers.

    Attributes:
        blocked: The blocked half-open intervals of each group that has any, in group order.
    """

    def __init__(self, blocked: Mapping[str, Sequence[tuple[int, int]]]) -> None:
        self.blocked = {group: list(intervals) for group, intervals in blocked.items()}
        details = "; ".join(f"group '{g}': {iv}" for g, iv in self.blocked.items())
        super().__init__(f"chunk alignment would drop remaining slots: {details}")


class SlotPlanCeilingError(SlotPlanError):
    """The plan has more ranges than the ceiling allows.

    Attributes:
        plans: The plan of each group, in group order.
        range_count: The number of ranges in the whole plan.
        ceiling: The ceiling the plan exceeds.
        suggested_slot_size: The smallest slot size whose plan is executable and within
            `ceiling`, or `None` when there is none.
    """

    def __init__(
        self,
        plans: Sequence[GroupSlotPlan],
        range_count: int,
        ceiling: int,
        suggested_slot_size: int | None,
    ) -> None:
        self.plans = list(plans)
        self.range_count = range_count
        self.ceiling = ceiling
        self.suggested_slot_size = suggested_slot_size
        super().__init__(f"the plan has {range_count} ranges, more than the limit of {ceiling}")


@dataclass(frozen=True)
class GroupSlotPlan:
    """One group's slot plan as half-open intervals; its ranges are generated on demand.

    Attributes:
        name: The group name.
        total: The group's slot count.
        slot_size: The length of every range except an interval's last.
        covered: The merged intervals the control plane records as written.
        remaining: The intervals of `[0, total)` that `covered` leaves out.
        aligned: The parts of `remaining` that start on a multiple of `slot_size`.
        blocked: The parts of `remaining` that no aligned range covers.
    """

    name: str
    total: int
    slot_size: int
    covered: list[tuple[int, int]]
    remaining: list[tuple[int, int]]
    aligned: list[tuple[int, int]]
    blocked: list[tuple[int, int]]

    @property
    def range_count(self) -> int:
        """Return the number of ranges `ranges()` yields, without generating them."""
        from firecube.core.zarr.planning import partition_count

        return partition_count(self.aligned, self.slot_size)

    @property
    def static_owner(self) -> tuple[int, int] | None:
        """Return the group's first range, or `None` when the group has no range.

        It is the first slot_size piece of the first aligned interval, the lowest
        range since aligned intervals are sorted.
        """
        if not self.aligned:
            return None
        owner_start, interval_end = self.aligned[0]
        return owner_start, min(owner_start + self.slot_size, interval_end)

    def ranges(self) -> Iterator[tuple[int, int]]:
        """Yield the group's ranges in slot order, one at a time."""
        from firecube.core.zarr.planning import iter_partitions

        return iter_partitions(self.aligned, self.slot_size)


_SLOTS_COMMAND = CommandName(("zarr", "slots"), library="plan_slots")
"""Label the planner in a message: `firecube zarr slots` in the CLI, `plan_slots` in Python."""


def _cause_text(exc: BaseException) -> str:
    """Return the refusal `Message` of `exc` when it has one, else `str(exc)`, so parts survive."""
    if isinstance(exc, FirecubeError) and exc.message is not None:
        return exc.message
    return str(exc)


def _ignore_line(line: str) -> None:
    """Discard a warning line."""


def plan_slots(
    *,
    ingestor: PreallocateHost,
    plugin_ctx: PluginContext,
    plugin_name: str,
    chunk_manager: ChunkManager,
    identity: ProductIdentity,
    target: str,
    storage_config: StorageConfig,
    coverage_storage_config: Callable[[], StorageConfig],
    input_data: str | tuple[str, ...] | None = None,
    slot_size: int | None = None,
    resume: bool = True,
    allow_above_ceiling: bool = False,
    alignment_warnings: bool = True,
    warn: Callable[[str], None] = _ignore_line,
) -> list[GroupSlotPlan]:
    """Return the slot plan of every group of a product, in group order.

    The plan resolves the plugin's index and schema, reads the stored layout of
    existing arrays and, when resuming, the coverage the control plane records.
    It writes nothing. Ranges are generated on demand, never stored.

    Args:
        ingestor: The plugin, a `PreallocateHost` configured by `configure_ingestor`.
        plugin_ctx: The context `configure_ingestor` returned for `ingestor`.
        plugin_name: The plugin's registered name, used in messages.
        chunk_manager: The control plane of the target product, read for its index records.
        identity: The identity of the target product.
        target: The target product URI.
        storage_config: Storage settings the stored array layout is read with.
        coverage_storage_config: Builds the storage settings the coverage lookup reads
            with; called only when resuming.
        input_data: Raw plugin input, one source or several; `None` when there is none.
            A plugin with an AUTO axis needs it unless it overrides
            `discover_source_files`.
        slot_size: The slot size of every group. It must be a multiple of each group's
            time unit. `None` gives each group its time unit.
        resume: Leave out the slots the control plane records as covered.
        allow_above_ceiling: Return a plan with more ranges than `PLAN_RANGE_CEILING`.
        alignment_warnings: Log the capability gate's chunk-alignment warnings.
        warn: Receives each warning that the persisted index records disagree with the plugin.

    Returns:
        One plan per group of the resolved index, sorted by group name.

    Raises:
        SlotPlanBlockedError: Chunk alignment leaves remaining slots no window covers.
        SlotPlanCeilingError: The plan exceeds `PLAN_RANGE_CEILING` and `allow_above_ceiling`
            is false.
        SlotPlanError: The plugin's index, schema or slot size is unusable, or the stored
            layout or coverage cannot be read.
    """
    from firecube.core.index_spec import AUTO, IrregularTimeAxis
    from firecube.core.zarr.planning import (
        chunk_aligned_remaining,
        complement_intervals,
        merge_intervals,
        resolve_per_group_slot_sizes,
        smallest_executable_slot_size,
    )
    from firecube.ingestor.runtime import parallel_gate
    from firecube.ingestor.runtime.base import BaseIngestor
    from firecube.ingestor.runtime.parallel_gate import (
        validate_global_expected_subset_of_schema,
        warn_on_chunk_alignment,
    )

    try:
        index_spec = ingestor.index_spec(plugin_ctx)
        if (
            input_data is None
            and index_spec is not None
            and any(
                isinstance(axis, IrregularTimeAxis) and axis.values is AUTO
                for axis in index_spec.groups.values()
            )
            and type(ingestor).discover_source_files is BaseIngestor.discover_source_files
        ):
            raise SlotPlanError(
                Message(
                    "Plugin '{plugin}' requires {input_data} for AUTO discovery. "
                    "Pass {input_data_source} or override discover_source_files() in the plugin.",
                    plugin=plugin_name,
                    input_data=Param("input_data"),
                    input_data_source=Param("input_data", Placeholder("<source>")),
                    product=identity.product_name,
                    product_uri=target,
                )
            )
        ingestor.bind_index_at_startup(plugin_ctx)
        resolved_index = ingestor.resolved_index(plugin_ctx)
    except SlotPlanError:
        raise
    except Exception as exc:
        raise SlotPlanError(
            f"Plugin '{plugin_name}' returned no index_spec. "
            "See the plugin's documentation for parallel-write support."
        ) from exc
    current_record = chunk_manager.get_resolved_index(product=identity.product_name)
    if current_record is not None and current_record.identity_hash != resolved_index.identity_hash:
        warn(
            "Plugin resolved index differs from persisted record. "
            "Use the plugin that wrote the product, or a new target."
        )
    elif (
        current_record is None
        and chunk_manager.get_slot_index_model(product=identity.product_name) is not None
    ):
        warn(
            Message(
                "Legacy slot-index record detected without a resolved-index record; "
                "run `{rebuild}` to migrate.",
                rebuild=Operation(
                    "rebuild_index",
                    target=target,
                    plugin=plugin_name,
                    product_name=identity.product_name,
                ),
                product=identity.product_name,
                product_uri=target,
                plugin=plugin_name,
            )
        )
    global_schema = {group: resolved_index.size(group) for group in resolved_index.groups}
    for group_name, expected in global_schema.items():
        if expected <= 0:
            raise SlotPlanError(
                f"Plugin '{plugin_name}' returned a non-positive count {expected} "
                f"for group '{group_name}'. Expected positive integers only."
            )

    schema = ingestor.zarr_schema(plugin_ctx)
    try:
        validate_global_expected_subset_of_schema(global_schema, schema)
    except ConfigurationError as exc:
        raise SlotPlanError(_cause_text(exc)) from exc

    if alignment_warnings:
        warn_on_chunk_alignment(global_schema, schema)
    else:
        with _warnings_dropped(parallel_gate.log):
            warn_on_chunk_alignment(global_schema, schema)

    stored_units = _stored_time_units(
        target,
        storage_config,
        schema,
        time_coord_name=ingestor.time_dim_name,
    )
    try:
        resolved_slot_sizes = resolve_per_group_slot_sizes(
            schema, slot_size, stored_units=stored_units
        )
    except ConfigurationError as exc:
        raise SlotPlanError(_cause_text(exc)) from exc

    if not resume:
        coverage_by_group: dict[str, list[tuple[int, int]]] = {g: [] for g in global_schema}
    else:
        try:
            coverage_by_group = _query_slots_coverage(
                identity=identity,
                storage_config=coverage_storage_config,
                groups=list(global_schema.keys()),
            )
        except Exception as exc:
            raise SlotPlanError(
                Message(
                    "{command}: resume-aware coverage lookup failed ({cause}). "
                    "Either fix the underlying failure or pass {no_resume} to emit "
                    "full ranges without coverage narrowing.",
                    command=_SLOTS_COMMAND,
                    cause=_cause_text(exc),
                    no_resume=Param("no_resume", True),
                    plugin=plugin_name,
                    product=identity.product_name,
                    product_uri=target,
                )
            ) from exc

    plans: list[GroupSlotPlan] = []
    for group_name in sorted(global_schema.keys()):
        total = int(global_schema[group_name])
        covered = merge_intervals(coverage_by_group.get(group_name, []))
        remaining = complement_intervals(covered, total)
        group_slot_size = resolved_slot_sizes.get(group_name, total)
        aligned_remaining, blocked_for_group = chunk_aligned_remaining(
            remaining, group_slot_size, total
        )
        plans.append(
            GroupSlotPlan(
                name=group_name,
                total=total,
                slot_size=group_slot_size,
                covered=covered,
                remaining=remaining,
                aligned=aligned_remaining,
                blocked=blocked_for_group,
            )
        )

    blocked = {plan.name: plan.blocked for plan in plans if plan.blocked}
    if blocked:
        raise SlotPlanBlockedError(blocked)

    range_count = sum(plan.range_count for plan in plans)
    ceiling = PLAN_RANGE_CEILING
    if range_count > ceiling and not allow_above_ceiling:
        try:
            units = resolve_per_group_slot_sizes(schema, None, stored_units=stored_units)
        except ConfigurationError as exc:
            raise SlotPlanError(_cause_text(exc)) from exc
        suggested = smallest_executable_slot_size(
            {plan.name: plan.remaining for plan in plans},
            {plan.name: plan.total for plan in plans},
            units,
            ceiling,
        )
        raise SlotPlanCeilingError(plans, range_count, ceiling, suggested)
    return plans


@contextlib.contextmanager
def _warnings_dropped(logger: logging.Logger) -> Iterator[None]:
    """Drop the WARNING and higher records `logger` itself emits while the block runs.

    Records of descendant loggers pass. Other threads logging on `logger` meanwhile
    lose their warnings too.
    """

    def below_warning(record: logging.LogRecord) -> bool:
        return record.levelno < logging.WARNING

    logger.addFilter(below_warning)
    try:
        yield
    finally:
        logger.removeFilter(below_warning)


def _stored_time_units(
    target: str,
    storage_config: Any,
    schema: Sequence[ZarrGroupSpec],
    *,
    time_coord_name: str,
) -> dict[str, list[int]]:
    """Return the stored chunk or shard time extent of each pod-written array the target has.

    Existing pod-written arrays constrain the slot size by their stored chunk or shard,
    the unit pod startup judges windows by. The target opens read-only and nothing is
    created in it; a target without a root group, or without an array, adds no unit.

    Raises:
        SlotPlanError: Any failure reading the target, its type and message in the text.
    """
    import zarr

    from firecube.core.filesystem.ops import create_filesystem_for_uri
    from firecube.core.filesystem.store_factory import create_zarr_store
    from firecube.core.zarr.guard.engine_owned import is_pod_written_slot_array
    from firecube.core.zarr.guard.unit_ownership import stored_write_unit

    units: dict[str, list[int]] = {}
    try:
        # Existence goes through the storage driver before any zarr store is built:
        # building one can create the target, and only an absent root means no units.
        fs, target_uri = create_filesystem_for_uri(target, storage_config, format="zarr")
        if not fs.exists(target_uri.join("zarr.json")):
            return units
        handle = create_zarr_store(uri=target, storage_config=storage_config, mode="r")
        root = cast(
            Any,
            zarr.open_group(
                **handle.zarr_kwargs(), mode="r", zarr_format=3, use_consolidated=False
            ),
        )
        for group_spec in schema:
            for arr_spec in group_spec.arrays:
                if not is_pod_written_slot_array(arr_spec, time_coord_name=time_coord_name):
                    continue
                path = f"{group_spec.group}/{arr_spec.name}"
                # An absent array adds nothing; one that exists must parse, or the plan fails.
                if not fs.exists(target_uri.join(*path.strip("/").split("/"), "zarr.json")):
                    continue
                arr = root[path]
                if getattr(arr, "ndim", 0) == 0:
                    continue
                units.setdefault(group_spec.group, []).append(int(stored_write_unit(arr)[0][0]))
    except Exception as exc:
        raise SlotPlanError(
            Message(
                "{command}: cannot read the layout of existing arrays in {product_uri} "
                "({exc_type}: {cause}).",
                command=_SLOTS_COMMAND,
                product_uri=target,
                exc_type=type(exc).__name__,
                cause=_cause_text(exc),
            )
        ) from exc
    return units


def _query_slots_coverage(
    *,
    identity: ProductIdentity,
    storage_config: Callable[[], StorageConfig],
    groups: list[str],
) -> dict[str, list[tuple[int, int]]]:
    """Return the half-open slot intervals the control plane records as covered, per group.

    Stored span ranges are inclusive; each becomes `[start, end + 1)`. Malformed
    entries and groups outside `groups` are skipped.

    Args:
        identity: The identity of the target product.
        storage_config: Builds the storage settings the control plane is read with.
        groups: The groups to report; each gets a list, empty when nothing is covered.
    """
    coverage: dict[str, list[tuple[int, int]]] = {g: [] for g in groups}
    driver = StorageDriverConfig.from_storage_config(storage_config())
    binding = StorageBinding(identity=identity, driver=driver)
    manager = ChunkManager(binding=binding)
    try:
        chunks = manager.list_chunks(
            product=identity.product_name,
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
