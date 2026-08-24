"""Engine glue: resolve IndexSpec and filter items by slot range.

This module bridges the pure core resolvers (``firecube.core.index_resolve``)
with the ingestor runtime. It is the only place that knows about both
``IndexSpec`` and the ingestor's ``PluginContext``.
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from itertools import pairwise
from pathlib import Path
from typing import Any

from firecube.core.controlplane.types import (
    ItemManifestEntry,
    SourceRefKind,
    validate_manifest_entries,
)
from firecube.core.errors import (
    DuplicateIrregularCoordinateError,
    MissingIrregularCoordinateError,
    NoDiscoveredItemsError,
)
from firecube.core.index_resolve import ResolvedIndex, resolve_index_spec
from firecube.core.index_spec import AUTO, IndexSpec, IrregularTimeAxis, _canonical_coordinate_value
from firecube.ingestor.errors import ConfigurationError

logger = logging.getLogger(__name__)

_HASH_PREFIX_BYTES = 64 * 1024


@dataclass(frozen=True)
class IndexBinding:
    """Resolved index bound to a specific plugin context.

    Holds the spec, the resolved index, and the context identity so the
    ``DirectZarrIngestor`` can cache it per ``(id(ctx._ctx), spec)``.

    Args:
        spec: The ``IndexSpec`` that was resolved.
        resolved: The resolved index ready for slot-index computation.
    """

    spec: IndexSpec
    """The index specification that was resolved."""

    resolved: ResolvedIndex
    """The resolved index ready for slot-index computation."""


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
    resolved_spec = spec
    manifest: tuple[ItemManifestEntry, ...] | None = None
    auto_axis = _first_auto_irregular_axis(spec)
    if auto_axis is not None:
        discovered_axis, manifest = _discover_auto_irregular_axis(auto_axis, ingestor, ctx)
        resolved_spec = _replace_auto_irregular_axes(spec, discovered_axis)

    resolved = resolve_index_spec(resolved_spec, time_dim_name=time_dim_name, items=manifest)
    return IndexBinding(spec=resolved_spec, resolved=resolved)


def _first_auto_irregular_axis(spec: IndexSpec) -> IrregularTimeAxis | None:
    for axis in spec.groups.values():
        if isinstance(axis, IrregularTimeAxis) and axis.values is AUTO:
            return axis
    return None


def _replace_auto_irregular_axes(spec: IndexSpec, discovered_axis: IrregularTimeAxis) -> IndexSpec:
    groups = {
        group: discovered_axis
        if isinstance(axis, IrregularTimeAxis) and axis.values is AUTO
        else axis
        for group, axis in spec.groups.items()
    }
    return IndexSpec(name=spec.name, groups=groups, time_unit=spec.time_unit)


def _discover_auto_irregular_axis(
    axis: IrregularTimeAxis, ingestor: Any, ctx: Any
) -> tuple[IrregularTimeAxis, tuple[ItemManifestEntry, ...]]:
    """Discover concrete values and a manifest for ``IrregularTimeAxis(values=AUTO)``."""

    discovered: list[tuple[Any, str, SourceRefKind, str]] = []
    for item in _iter_runtime_items(ingestor, ctx):
        source_ref, source_ref_kind = _source_ref(item)
        try:
            info = ingestor.inspect_item(item, ctx)
        except Exception as exc:
            raise MissingIrregularCoordinateError(axis.coordinate, source_ref) from exc
        coordinate = getattr(info, "coordinate", info) if info is not None else None
        if coordinate is None:
            raise MissingIrregularCoordinateError(axis.coordinate, source_ref)
        discovered.append((coordinate, source_ref, source_ref_kind, _item_identity_hash(item)))

    if not discovered:
        raise NoDiscoveredItemsError(axis.coordinate, _source_description(ctx))

    discovered.sort(key=lambda entry: entry[0])
    for current, next_ in pairwise(discovered):
        if current[0] == next_[0]:
            raise DuplicateIrregularCoordinateError(
                axis.coordinate,
                current[0],
                current[1],
                next_[1],
            )

    manifest = tuple(
        ItemManifestEntry(
            identity_hash=identity_hash,
            coordinate_value=_canonical_coordinate_value(coordinate),
            source_ref=source_ref,
            source_ref_kind=source_ref_kind,
        )
        for coordinate, source_ref, source_ref_kind, identity_hash in discovered
    )
    validate_manifest_entries(list(manifest))
    return IrregularTimeAxis(
        coordinate=axis.coordinate, values=tuple(row[0] for row in discovered)
    ), manifest


def _iter_runtime_items(ingestor: Any, ctx: Any) -> Iterable[Any]:
    for item in ingestor.discover_source_files(ctx):
        if ingestor.filter_item(item, ctx):
            yield item


def _source_description(ctx: Any) -> str | None:
    source = getattr(ctx, "source", None)
    return str(source) if source is not None else None


def _source_ref(item: Any) -> tuple[str, SourceRefKind]:
    uri = getattr(item, "uri", None)
    if isinstance(uri, str) and uri:
        return uri, "uri"
    local_path = getattr(item, "local_path", None)
    if callable(local_path):
        path = local_path()
        if path is not None:
            return str(Path(str(path)).resolve()), "path"
    text = str(item)
    if "://" in text:
        return text, "uri"
    try:
        path = Path(text).resolve()
    except TypeError:
        return text, "identifier"
    if path.exists():
        return str(path), "path"
    return text, "identifier"


def _item_identity_hash(item: Any) -> str:
    source_ref, source_ref_kind = _source_ref(item)
    if source_ref_kind != "path":
        return hashlib.sha256(f"{source_ref_kind}\0{source_ref}".encode()).hexdigest()
    return _path_identity_hash(source_ref)


def _path_identity_hash(path: str) -> str:
    local = Path(path)
    stat = local.stat()
    digest = hashlib.sha256()
    digest.update(str(stat.st_size).encode("ascii"))
    digest.update(b"\0")
    digest.update(str(stat.st_mtime_ns).encode("ascii"))
    digest.update(b"\0")
    with local.open("rb") as handle:
        digest.update(hashlib.sha256(handle.read(_HASH_PREFIX_BYTES)).hexdigest().encode("ascii"))
    return digest.hexdigest()


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
        # axis object (Python is-check on axis identity). This covers the common
        # case where a plugin declares multiple groups with the same RegularTimeAxis
        # instance (e.g. FCI data_1km + data_2km sharing one axis).
        axes = [resolved._spec.groups[g] for g in groups]
        if all(a is axes[0] for a in axes[1:]):
            active_group = groups[0]
        else:
            raise ConfigurationError(
                "Multiple groups in the resolved index reference distinct axis instances "
                "(Python `is` identity check across groups). When a plugin uses multiple "
                "index-spec groups without an explicit `slot_group`, all groups must "
                "point to the SAME axis object (not merely value-equal axes). Either "
                "share one axis instance across the groups in your `index_spec()`, or "
                "pass `slot_group=...` to name the group to use for slot-range filtering. "
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
