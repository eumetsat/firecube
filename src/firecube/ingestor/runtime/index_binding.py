"""Engine glue: resolve IndexSpec and filter items by slot range.

This module bridges the pure core resolvers (``firecube.core.index_resolve``)
with the ingestor runtime. It is the only place that knows about both
``IndexSpec`` and the ingestor's ``PluginContext``.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from firecube.core.index_resolve import ResolvedIndex, resolve_index_spec
from firecube.core.index_spec import IndexSpec
from firecube.ingestor.errors import ConfigurationError

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class IndexBinding:
    """Resolved index bound to a specific plugin context.

    Holds the spec, the resolved index, and the context identity so the
    ``DirectZarrIngestor`` can cache it per ``(id(ctx._ctx), spec)``.

    Args:
        spec: The ``IndexSpec`` that was resolved.
        resolved: The resolved index ready for slot-index computation.
        ctx_id: ``id(ctx._ctx)`` at resolution time; used as the cache key.
    """

    spec: IndexSpec
    """The index specification that was resolved."""

    resolved: ResolvedIndex
    """The resolved index ready for slot-index computation."""

    ctx_id: int
    """Identity of the plugin context at resolution time (cache key)."""


def resolve_index_spec_for_ingestor(ingestor: Any, ctx: Any) -> IndexBinding | None:
    """Resolve the ingestor's ``index_spec`` into an ``IndexBinding``.

    Calls ``ingestor.index_spec(ctx)``; returns ``None`` if the spec is
    ``None`` (serial-mode plugin). Otherwise resolves via
    ``core/index_resolve.resolve_index_spec`` using the ingestor's
    time-dimension name.

    Args:
        ingestor: A ``DirectZarrIngestor`` instance (typed as ``Any`` to
            avoid a circular import with ``templates/direct_zarr.py``).
        ctx: The ``PluginContext`` for this run.

    Returns:
        An ``IndexBinding`` if the plugin provides an ``IndexSpec``, or
        ``None`` for serial-mode plugins.

    Raises:
        ConfigurationError: If the spec is invalid (e.g. axis coordinate
            mismatch).
    """
    spec: IndexSpec | None = ingestor.index_spec(ctx)
    if spec is None:
        return None

    time_dim_name: str = ingestor._resolve_time_dim_name()
    resolved = resolve_index_spec(spec, time_dim_name=time_dim_name)
    return IndexBinding(spec=spec, resolved=resolved, ctx_id=id(ctx._ctx))


def filter_items_by_index(
    items: Sequence[Any],
    resolved: ResolvedIndex,
    slot_start: int,
    slot_end: int,
    slot_group: str | None,
    inspect_item: Callable[[Any, Any], Any | None],
    ctx: Any,
) -> list[Any]:
    """Filter source items to those whose slot index falls in [slot_start, slot_end).

    Calls ``inspect_item(item, ctx)`` for each item. Items for which
    ``inspect_item`` returns ``None`` are dropped. Items whose slot index
    falls outside ``[slot_start, slot_end)`` are also dropped.

    Args:
        items: The full list of source items for this batch.
        resolved: The resolved index for slot-index computation.
        slot_start: Inclusive start of the slot range.
        slot_end: Exclusive end of the slot range.
        slot_group: The group to use for slot-index computation. When
            ``None`` and all groups share the same axis, any group is used.
            When ``None`` and groups differ, raises ``ConfigurationError``.
        inspect_item: Callable ``(item, ctx) -> ItemInfo | None``. Must
            accept ``ctx`` as its second argument.
        ctx: The ``PluginContext`` for this run; threaded to ``inspect_item``.

    Returns:
        Filtered list of items whose slot index falls in the range.

    Raises:
        ConfigurationError: If ``slot_group`` is ``None`` and groups have
            different axes.
    """
    groups = resolved.groups
    if slot_group is not None:
        if slot_group not in groups:
            raise ConfigurationError(
                f"slot_group={slot_group!r} is not in the resolved index; "
                f"available groups: {list(groups)}"
            )
        active_group = slot_group
    elif len(groups) == 1:
        active_group = groups[0]
    else:
        # Multiple groups: allow slot_group=None when all groups share the same
        # axis object (identity check per plan task 1.3). This covers the common
        # case where a plugin declares multiple groups with the same RegularTimeAxis
        # instance (e.g. FCI data_1km + data_2km sharing one axis).
        axes = [resolved._spec.groups[g] for g in groups]
        if all(a is axes[0] for a in axes[1:]):
            active_group = groups[0]
        else:
            raise ConfigurationError(
                "Multiple groups in the resolved index with different axes; "
                "set slot_group to specify which group to use for slot-range filtering. "
                f"Available groups: {list(groups)}"
            )

    kept: list[Any] = []
    dropped_none = 0
    dropped_range = 0

    for item in items:
        info = inspect_item(item, ctx)
        if info is None:
            dropped_none += 1
            continue

        coordinate = info.coordinate if hasattr(info, "coordinate") else info
        try:
            ts_index = resolved.position(active_group, coordinate)
        except (ValueError, TypeError) as exc:
            raise ConfigurationError(
                f"inspect_item returned coordinate {coordinate!r} for item {item!r} "
                f"that could not be mapped to a slot index: {exc}"
            ) from exc

        if slot_start <= ts_index < slot_end:
            kept.append(item)
        else:
            dropped_range += 1

    logger.debug(
        "pre_batch_filter: kept=%d dropped_none=%d dropped_range=%d slot=[%d,%d) group=%r",
        len(kept),
        dropped_none,
        dropped_range,
        slot_start,
        slot_end,
        active_group,
    )
    return kept
