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

"""Direct-Zarr ingestor template for streaming region-based writes.

Provides ``DirectZarrIngestor`` — an abstract base for plugins that bypass
xarray and write directly to Zarr regions using ``RegionZarrWriter``.  The
plugin implements two hooks plus optional slot-range parallelism:

- ``zarr_schema(ctx)`` — declares groups, arrays, and shapes.
- ``build_write_intents(batch, ctx)`` — converts a batch into a list of
  ``WriteIntent`` specs describing region writes to execute.
- ``index_spec(ctx)`` — (optional) declares the time-axis index for parallel ingestion.
- ``inspect_item(item, ctx)`` — (optional) maps a source item to its slot coordinate.

The template handles store opening, claim coordination, coverage tracking, and metrics.
"""

from __future__ import annotations

import contextlib
import functools
import hashlib
import json
import logging
import random
import time
from abc import abstractmethod
from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import numpy as np

from firecube.core.api import (
    ATTR_PREALLOCATED,
    FIRECUBE_GROUP_IDENTITY_HASH_ATTR,
    AxisSpec,
    ExtentUnknownError,
    IndexSpec,
    IntegerAxis,
    IrregularTimeAxis,
    ItemInfo,
    RegularTimeAxis,
    ResolvedIndex,
    compute_group_identity_hash,
)
from firecube.core.errors import (
    ClaimConflictError,
    FirecubeError,
    IndexedWriteCompilationError,
    Message,
    Operation,
    Placeholder,
)
from firecube.core.indexed_write import IndexedWrite
from firecube.core.time.calendar import is_gregorian_axis
from firecube.core.zarr.guard._layout import resolve_effective_layout, warn_default_layout
from firecube.core.zarr.guard.engine_owned import is_engine_owned_spec, preallocate_operation
from firecube.core.zarr.guard.metadata_authority import MetadataAuthority, hold_authority
from firecube.ingestor.api import (
    BaseIngestor,
    ConfigurationError,
    OutputPaths,
    PipelineBatch,
    PipelineResult,
    PipelineRunState,
    PluginContext,
    RuntimeIngestContext,
    SchemaDriftError,
    SchemaSizeMismatchError,
    UnboundedAxisError,
    WriteDomain,
    ZarrTemplateConfig,
    merge_batch_metrics,
)
from firecube.ingestor.runtime.zarr.direct_region_adapter import (
    DirectRegionAdapter,
    open_zarr_array,
)
from firecube.ingestor.runtime.zarr.strategies.indexed_region import (
    RegionZarrWriter,
    ensure_group_ancestors,
)
from firecube.ingestor.runtime.zarr.write import derive_effective_codecs_for_spec
from firecube.ingestor.runtime.zarr.write_session import ZarrWriteSession
from firecube.ingestor.types import write_mode_policy

if TYPE_CHECKING:
    from firecube.core.storage.completion import StagedPromotionUnits

log = logging.getLogger(__name__)


def _ctx_product_name(ctx: PluginContext, default: str) -> str:
    storage = ctx.storage
    if storage is not None and storage.output is not None:
        return str(storage.output.product.product_name)
    return default


def _normalized_fill_value(fill_value: Any) -> Any:
    if fill_value is None:
        return None
    try:
        if bool(np.isnan(fill_value)):
            return "nan"
    except (TypeError, ValueError):
        pass
    return repr(fill_value)


def _compute_schema_hash(schema: Sequence[Any], global_expected: dict[str, int]) -> str:
    """Return a deterministic short hash for the declared Zarr schema shape contract.

    Audit-only fingerprint: emitted into run metadata for post-hoc diffing across
    runs. Never consulted for drift enforcement or write gating — codec drift is
    detected at write time by ``compare_pipelines`` against on-disk codecs.

    Codec fields (``filters``/``serializer``/``compressors``) are folded into the
    per-array record ONLY when at least one is declared, so schemas that omit
    them hash identically to pre-codec-parity baselines (backward-compatible,
    same pattern as group-level ``attrs`` below).
    """
    from firecube.core.zarr.codec_pipeline import normalize_codec_dict

    records: list[dict[str, Any]] = []
    expected_groups = set(global_expected)
    for group_spec in sorted(schema, key=lambda spec: str(spec.group)):
        if expected_groups and group_spec.group not in expected_groups:
            continue
        for arr_spec in sorted(group_spec.arrays, key=lambda arr: str(arr.name)):
            record: dict[str, Any] = {
                "group": group_spec.group,
                "array": arr_spec.name,
                "dtype": str(np.dtype(arr_spec.dtype)),
                "rank": len(arr_spec.shape),
                "shape_non_time": list(arr_spec.shape[1:]),
                "chunks": list(arr_spec.chunks) if arr_spec.chunks is not None else None,
                "fill_value": _normalized_fill_value(arr_spec.fill_value),
                "shards": list(arr_spec.shards) if arr_spec.shards is not None else None,
                "attrs": dict(sorted(arr_spec.attrs.items()))
                if arr_spec.attrs is not None
                else None,
                "dimension_names": list(arr_spec.dimension_names)
                if arr_spec.dimension_names is not None
                else None,
                "time_indexed": bool(arr_spec.time_indexed),
            }
            # Codec fields — only included when at least one is declared.
            # Omitting when all are None preserves byte-identical hashes for
            # legacy schemas (same pattern as group-level attrs below).
            spec_filters = getattr(arr_spec, "filters", None)
            spec_serializer = getattr(arr_spec, "serializer", None)
            spec_compressors = getattr(arr_spec, "compressors", None)
            if (
                spec_filters is not None
                or spec_serializer is not None
                or spec_compressors is not None
            ):
                record["filters"] = (
                    [normalize_codec_dict(f) for f in spec_filters]
                    if spec_filters is not None
                    else None
                )
                record["serializer"] = (
                    normalize_codec_dict(spec_serializer) if spec_serializer is not None else None
                )
                record["compressors"] = (
                    [normalize_codec_dict(c) for c in spec_compressors]
                    if spec_compressors is not None
                    else None
                )
            records.append(record)
        # Fold group-level attrs into the identity ONLY when present, so schemas
        # that declare none hash identically to before (backward-compatible).
        group_attrs = getattr(group_spec, "attrs", None)
        if group_attrs:
            records.append(
                {
                    "group": group_spec.group,
                    "group_attrs": dict(sorted(group_attrs.items())),
                }
            )
    payload = json.dumps(records, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _schema_dimension_names(arr_spec: Any, time_coord_name: str) -> tuple[str, ...] | None:
    explicit = getattr(arr_spec, "dimension_names", None)
    if explicit is not None:
        return explicit
    shape = tuple(getattr(arr_spec, "shape", ()))
    if getattr(arr_spec, "name", None) == time_coord_name and len(shape) == 1:
        return (time_coord_name,)
    return None


def _hold_setup_authority(
    writer: RegionZarrWriter,
    group_name: str,
    *,
    chunk_manager: Any,
    product: str,
    domain: WriteDomain,
    owner_id: str,
    describe_conflict: bool,
) -> contextlib.AbstractContextManager[MetadataAuthority]:
    """Take the setup claim of *group_name* and the authority over its subtree it entitles.

    The authority is released before the claim. Without a control plane
    (``chunk_manager`` is ``None``) it is explicitly ``PROCESS_LOCAL``: it
    excludes the threads of this process only. *describe_conflict* is set on the last attempt.
    """
    claim = (
        chunk_manager.acquire_claim(
            product=product,
            domain=domain,
            owner_id=owner_id,
            describe_conflict=describe_conflict,
        )
        if chunk_manager is not None
        else None
    )
    return hold_authority(
        writer.metadata_scope(group_name),
        claim=claim,
        claim_domain=domain.identifier,
        owner_id=owner_id,
    )


def _setup_global_zarr_schema(
    *,
    strategy: Any,
    schema: Sequence[Any],
    global_expected: dict[str, int],
    product: str,
    run_id: str,
    chunk_manager: Any,
    time_coord_name: str = "timestamp",
    template_config: Any = None,
) -> None:
    """Ensure direct-Zarr arrays are sized for the full parallel run.

    When ``template_config`` is provided, per-array codec declarations are
    validated against the template compression setting BEFORE any array is
    created. Invalid combinations raise ``ValueError``, leaving the target
    store untouched.
    """
    # Validate per-array codec declarations against template BEFORE creating any arrays.
    # Lazy import avoids a circular import between direct_zarr and templates.config.
    if template_config is not None:
        from firecube.ingestor.templates.config import validate_zarr_specs_against_template

        all_specs = [arr for group_spec in schema for arr in group_spec.arrays]
        validate_zarr_specs_against_template(all_specs, template_config)

    from firecube.core.uris import storage_uri_from_target
    from firecube.core.zarr.region_writer import group_schema_satisfied
    from firecube.ingestor.runtime.zarr.strategies.indexed_region import (
        _session_for_store,
    )

    schema_has_time_indexed = any(
        arr.time_indexed for group_spec in schema for arr in group_spec.arrays
    )
    if not global_expected and schema_has_time_indexed:
        raise ConfigurationError("Parallel global schema is empty; expected group sizes.")

    for group_name, expected_time_count in global_expected.items():
        if expected_time_count <= 0:
            raise ConfigurationError(
                f"Parallel global schema has non-positive count for group {group_name!r}: "
                f"{expected_time_count}"
            )

    def _make_writer(group_name: str) -> RegionZarrWriter:
        zarr_store = None
        storage_config = getattr(strategy, "_storage_config", None)
        if storage_config:
            session = getattr(strategy, "_session", None) or _session_for_store(
                strategy._store_uri, storage_config
            )
            zarr_store = session.zarr.create_store(
                uri=storage_uri_from_target(strategy._store_uri),
                mode="a",
            ).store
        coord_names = getattr(strategy, "_coord_names_by_group", {}).get(
            group_name, frozenset({"y", "x", "channel"})
        )
        return RegionZarrWriter(
            strategy._store_uri,
            store=zarr_store,
            coord_names=coord_names,
            time_coord_name=time_coord_name,
        )

    schema_by_group = {spec.group: spec for spec in schema}
    for group_name, group_spec in schema_by_group.items():
        expected_time_count = global_expected.get(group_name)
        group_has_time_indexed = any(arr.time_indexed for arr in group_spec.arrays)
        if expected_time_count is None:
            if group_has_time_indexed:
                log.warning(
                    "Group '%s' has time-indexed arrays in zarr_schema() but is missing from "
                    "the resolved index; skipping global schema pre-allocation. "
                    "If this group receives parallel writes, add it to index_spec(ctx).",
                    group_name,
                )
                continue
            # Static-only group: process without a time count. The sentinel value is
            # never consumed because verify_array_spec skips shape[0] checks for
            # time_indexed=False specs and the per-array effective_shape below
            # uses arr_spec.shape directly for static arrays.
            expected_time_count = 0

        # Read-only fast path: setup is idempotent, so if the arrays already exist
        # and match (the `firecube zarr preallocate` workflow, or a peer pod that
        # already ran setup) there is nothing to mutate. Skip the exclusive claim
        # entirely so simultaneous slot-range pods do not race on it. The claim is
        # only taken below when a real mutation (array creation) is required.
        if group_schema_satisfied(
            _make_writer(group_name), group_name, group_spec.arrays, expected_time_count
        ):
            log.warning(
                "Parallel evidence: stage=schema_verify group=%s expected_shape=%s status=%s",
                group_name,
                global_expected.get(group_name, "static-only"),
                "verified",
            )
            continue

        domain = WriteDomain(
            product=product,
            category="zarr_schema_global",
            name=f"{group_name}:setup",
        )
        # Bounded retry for the exclusive schema-setup claim.
        # Pattern mirrors the control-plane claim convergence helpers.
        # On ClaimConflictError, re-check convergence before sleeping/retrying:
        # another pod may have finished schema setup while we were waiting.
        _schema_max_retries = 5
        _schema_backoff = 0.1
        for _schema_attempt in range(_schema_max_retries + 1):
            try:
                writer = _make_writer(group_name)
                # Every metadata change below is made under the authority this
                # claim entitles, over the group's subtree.
                with _hold_setup_authority(
                    writer,
                    group_name,
                    chunk_manager=chunk_manager,
                    product=product,
                    domain=domain,
                    owner_id=f"{run_id}:{group_name}:schema_global",
                    describe_conflict=_schema_attempt == _schema_max_retries,
                ) as authority:
                    # Re-check INSIDE the claim: another pod may have converged
                    # between the outer satisfied-check and claim acquisition.
                    if group_schema_satisfied(
                        _make_writer(group_name),
                        group_name,
                        group_spec.arrays,
                        expected_time_count,
                    ):
                        log.warning(
                            "Parallel evidence: stage=schema_verify group=%s expected_shape=%s status=%s",
                            group_name,
                            global_expected.get(group_name, "static-only"),
                            "converged_under_claim",
                        )
                        break
                    # The group's authority may not create nodes above it.
                    ensure_group_ancestors(writer, group_name)
                    existing_shape: tuple[int, ...] | None = None
                    newly_created = False
                    for arr_spec in group_spec.arrays:
                        if arr_spec.time_indexed:
                            effective_shape = (expected_time_count, *arr_spec.shape[1:])
                        else:
                            effective_shape = arr_spec.shape
                        group_path = f"{group_name}/{arr_spec.name}"
                        root = writer._open_root()
                        path_parts = [p for p in group_path.split("/") if p]
                        current = root
                        for part in path_parts[:-1]:
                            if part not in current:
                                current = None
                                break
                            current = current[part]
                        arr_name = path_parts[-1]
                        array_exists = current is not None and arr_name in current
                        _eff_filters, _eff_serializer, _eff_compressors = (
                            derive_effective_codecs_for_spec(arr_spec, template_config)
                        )
                        layout = resolve_effective_layout(
                            arr_spec,
                            coord_name=time_coord_name,
                            time_count=expected_time_count,
                            engine_owned=is_engine_owned_spec(arr_spec),
                        )
                        arr = writer.ensure_group(
                            group_path,
                            shape=effective_shape,
                            dtype=arr_spec.dtype,
                            fill_value=arr_spec.fill_value,
                            chunks=layout.chunks,
                            shards=layout.shards,
                            attrs=arr_spec.attrs,
                            dimension_names=_schema_dimension_names(arr_spec, time_coord_name),
                            filters=_eff_filters,
                            serializer=_eff_serializer,
                            compressors=_eff_compressors,
                            authority=authority,
                        )
                        if not array_exists:
                            newly_created = True
                            if layout.is_default:
                                warn_default_layout(group_path, layout)
                        if existing_shape is None:
                            existing_shape = getattr(arr, "shape", None) if array_exists else None
                        if array_exists:
                            try:
                                writer.verify_array_spec(group_path, arr_spec, expected_time_count)
                            except SchemaDriftError as exc:
                                if (
                                    arr_spec.time_indexed
                                    and arr.ndim > 0
                                    and arr.shape[0] < expected_time_count
                                ):
                                    raise SchemaSizeMismatchError(
                                        Message(
                                            "Schema drift for group {group}: existing array "
                                            "shape[0]={existing} < global_expected={expected}. "
                                            "Run `{preallocate}` first.",
                                            group=group_name,
                                            existing=arr.shape[0],
                                            expected=expected_time_count,
                                            preallocate=Operation(
                                                "preallocate",
                                                plugin=Placeholder("<plugin>"),
                                                target=Placeholder("<uri>"),
                                                product_name=Placeholder("<name>"),
                                                write_mode=Placeholder("<mode>"),
                                            ),
                                            product=product,
                                            run_id=run_id,
                                            array_path=group_path,
                                        )
                                    ) from exc
                                raise
                    writer.set_group_attrs(
                        group_name, getattr(group_spec, "attrs", None), authority=authority
                    )
                    log.warning(
                        "Parallel evidence: stage=schema_verify group=%s existing_shape=%s expected_shape=%s status=%s",
                        group_name,
                        existing_shape,
                        global_expected.get(group_name, "static-only"),
                        "created" if newly_created else "verified",
                    )
                break  # claim released normally -> success
            except ClaimConflictError:
                # Another pod holds the claim. Re-check WITHOUT the claim:
                # they may have finished while we waited.
                if group_schema_satisfied(
                    _make_writer(group_name),
                    group_name,
                    group_spec.arrays,
                    expected_time_count,
                ):
                    log.warning(
                        "Parallel evidence: stage=schema_verify group=%s expected_shape=%s status=%s",
                        group_name,
                        global_expected.get(group_name, "static-only"),
                        "converged_after_conflict",
                    )
                    break  # schema is done; move to next group
                if _schema_attempt == _schema_max_retries:
                    raise  # propagate; pod must fail
                time.sleep(_schema_backoff + random.random() * _schema_backoff)
                _schema_backoff *= 2


@dataclass(frozen=True)
class ZarrArraySpec:
    """Specification for a single Zarr array within a group."""

    name: str
    """Name of the Zarr array within its group."""
    shape: tuple[int, ...]
    """Full shape of the array, with time as the first axis when indexed."""
    dtype: np.dtype[Any] | type[np.generic] | type[Any] | str
    """NumPy dtype or dtype-like value used when creating the array."""
    chunks: tuple[int, ...] | None = None
    """Chunk shape for the array, or ``None`` for the default layout.

    With ``None`` and no ``shards``, a time-indexed array that pods write is
    created with one slot per chunk. If zarr's own chunks for the whole array
    already have a time chunk of 1, they are kept unchanged and nothing is
    logged. Otherwise the array gets time chunk 1, with the other dimensions
    chosen by zarr for a single slot, unsharded; Firecube logs one warning
    naming the chunks when it changes zarr's own choice this way.

    With ``None`` and ``shards`` declared, the declared shards are kept. A
    rank-1 time-indexed array (one that pods write, an engine-owned array, or
    the time coordinate) gets as its chunk the largest divisor of its shard
    that is at most ``min(256, slots)``, where ``slots`` is the array's time
    extent at creation. For a time-indexed array of higher rank, zarr chooses
    the chunks.

    The time coordinate and engine-owned arrays that declare neither ``chunks``
    nor ``shards`` get dense chunks of ``min(256, slots)``. One exception: an
    observed time coordinate (``RegularTimeAxis(mode="floor")``) that
    ``firecube zarr preallocate`` creates is always unsharded and its declared
    ``shards`` are not used. With ``chunks`` undeclared it gets dense chunks of
    ``min(256, slots)``; declared ``chunks`` are used as given.

    A static array's chunks are chosen by zarr. The default never rechunks an
    existing array. ``firecube zarr preallocate`` refuses an existing array
    whose stored chunks differ from the chunks its spec resolves to: the
    declared ``chunks``, or, for a time coordinate that declares none, the
    chunks the default would create. It does not compare the chunks of an
    array that declares no ``chunks`` and that pods write (with or without
    ``shards``), of an engine-owned or static array that declares no
    ``chunks``, or of an observed time coordinate.
    """
    fill_value: Any = None
    """Fill value written into unused cells when the array is preallocated."""
    expected_time_count: int | None = None
    """Expected number of time slots for time-indexed arrays, if known."""
    shards: tuple[int, ...] | None = None
    """Optional Zarr sharding shape for the array, or ``None`` to disable sharding."""
    attrs: Mapping[str, Any] | None = None
    """Array-level attributes stamped into ``zarr.json`` verbatim."""
    dimension_names: tuple[str, ...] | None = None
    """Dimension names for the array, ordered to match ``shape``."""
    time_indexed: bool = True
    """Whether the array participates in time-axis preallocation and slot writes."""
    filters: tuple[dict, ...] | None = None
    """Per-array codec filters.

    Each entry is a Zarr v3 ArrayArrayCodec config dict. ``None`` inherits the
    template default; an explicit tuple overrides the template filter pipeline
    for this array only.
    """
    serializer: dict | None = None
    """Per-array serializer codec.

    This is a Zarr v3 ArrayBytesCodec config dict. ``None`` inherits the
    template default; an explicit value overrides the template serializer for
    this array only.
    """
    compressors: tuple[dict, ...] | None = None
    """Per-array compressor codecs.

    Each entry is a Zarr v3 BytesBytesCodec config dict. ``None`` inherits the
    template default. An empty tuple means explicitly uncompressed for this
    array.
    """
    engine_owned: bool = False
    """Whether the engine, not ingest pods, writes this per-slot array.

    For a rank-1, time-indexed array with one scalar per slot (a platform
    position, a per-slot calibration constant). ``inspect_item`` supplies the
    slot's value as ``ItemInfo(coordinate=..., values={name: scalar})``;
    ``firecube zarr preallocate --input-data ...`` inspects every source item
    and writes the array whole, once, then marks it materialized. Ingest pods
    never write it: a write intent that targets it is checked against the
    stored value instead, and a mismatch fails the batch with
    ``SchemaDriftError``. Ingest into a target where the array is not
    materialized is refused. Slot-parallel pods therefore never share a chunk
    of it, whatever its chunks; with ``chunks=None`` it is created with the
    dense chunks of the time coordinate.

    Raises:
        ValueError: At construction, when ``engine_owned=True`` is combined
            with ``time_indexed=False`` or with a shape that is not rank 1.
        ConfigurationError: From ``firecube zarr preallocate``, before it
            acquires its write claim or writes any array, when the array is
            the group's time coordinate (the plugin's time dimension name or
            the coordinate of the group's index axis), because the engine
            already materializes coordinates. The same command raises it when
            the group has no bounded time axis or ``--input-data`` is missing.

    Examples:
        >>> spec = ZarrArraySpec(
        ...     name="platform_lon",
        ...     shape=(144,),
        ...     dtype="float64",
        ...     fill_value=float("nan"),
        ...     engine_owned=True,
        ... )
        >>> spec.engine_owned
        True
    """

    def __post_init__(self) -> None:
        if self.engine_owned and (not self.time_indexed or len(self.shape) != 1):
            raise ValueError(
                f"ZarrArraySpec {self.name!r}: engine_owned=True needs a rank-1 time-indexed "
                f"array (one scalar per slot); got shape={tuple(self.shape)!r}, "
                f"time_indexed={self.time_indexed!r}"
            )
        if (
            isinstance(self.fill_value, str)
            and self.fill_value == "NaT"
            and self.dtype is not None
            and "datetime" in str(self.dtype)
        ):
            raise ValueError(
                f"fill_value='NaT' is a string but dtype={self.dtype!r} is a datetime type. "
                f"Use np.datetime64('NaT', 'ns') explicitly to avoid schema-verify mismatch "
                f"in parallel-mode ingest."
            )
        if self.expected_time_count is not None and self.expected_time_count < 0:
            raise ValueError(
                f"expected_time_count must be non-negative, got {self.expected_time_count}"
            )
        if self.expected_time_count is not None and not self.time_indexed:
            raise ValueError(
                "ZarrArraySpec.expected_time_count is only valid when time_indexed=True; "
                "set expected_time_count=None for static arrays "
                f"(got expected_time_count={self.expected_time_count!r}, time_indexed={self.time_indexed!r})."
            )
        if self.shards is not None and len(self.shards) != len(self.shape):
            raise ValueError(
                f"shards length {len(self.shards)} must match shape rank {len(self.shape)}"
            )
        if self.dimension_names is not None and len(self.dimension_names) != len(self.shape):
            raise ValueError(
                f"dimension_names length {len(self.dimension_names)} must match shape rank {len(self.shape)}"
            )
        if self.attrs is not None and not isinstance(self.attrs, Mapping):
            raise ValueError(f"attrs must be a Mapping, got {type(self.attrs).__name__}")
        if self.filters is not None:
            if not isinstance(self.filters, tuple):
                raise ValueError(
                    f"ZarrArraySpec.filters must be a tuple of dicts, got {type(self.filters).__name__}"
                )
            for i, f in enumerate(self.filters):
                if not isinstance(f, dict):
                    raise ValueError(
                        f"ZarrArraySpec.filters[{i}] must be a dict, got {type(f).__name__}"
                    )
        if self.serializer is not None:
            if not isinstance(self.serializer, dict):
                raise ValueError(
                    f"ZarrArraySpec.serializer must be a dict, got {type(self.serializer).__name__}"
                )
            if "name" not in self.serializer:
                raise ValueError("ZarrArraySpec.serializer must have a 'name' key")
        if self.compressors is not None:
            if not isinstance(self.compressors, tuple):
                raise ValueError(
                    "ZarrArraySpec.compressors must be a tuple of dicts (or empty tuple for uncompressed), "
                    f"got {type(self.compressors).__name__}"
                )
            for i, c in enumerate(self.compressors):
                if not isinstance(c, dict):
                    raise ValueError(
                        f"ZarrArraySpec.compressors[{i}] must be a dict, got {type(c).__name__}"
                    )


@dataclass(frozen=True)
class ZarrGroupSpec:
    """Specification for a Zarr group and its arrays.

    Returned by ``DirectZarrIngestor.zarr_schema()`` to describe the full layout.

    ``attrs`` is optional, convention-agnostic group-level metadata stamped onto
    the group's ``zarr.json`` at schema setup — e.g. dataset-level attributes a
    plugin chooses to publish. Firecube writes the mapping verbatim and does not
    interpret it (no convention is assumed); reserved firecube-internal attribute
    names are rejected at write time.
    """

    group: str
    arrays: list[ZarrArraySpec] = field(default_factory=list)
    coord_names: frozenset[str] = frozenset({"y", "x", "channel"})
    attrs: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        if self.attrs is not None and not isinstance(self.attrs, Mapping):
            raise ValueError(f"attrs must be a Mapping, got {type(self.attrs).__name__}")


@dataclass(frozen=True)
class WriteIntent:
    """A single write operation to execute against the Zarr store.

    Produced by ``DirectZarrIngestor.build_write_intents()`` to describe
    what data should be written, where, and at which timestamp index.

    The ``kind`` field selects the write method on ``RegionZarrWriter``.
    Here, "timestamp" means the conceptual time/index axis, not the on-disk
    dimension name; the latter is configured separately via
    ``IndexedRegionStrategy.time_coord_name``.

    - ``"region"`` → ``write_region(group, array, ts_index, y_slice, data, channel_index=...)``
    - ``"1d"`` → ``write_1d(group, array, ts_index, data)``
    - ``"timestamp"`` → ``write_timestamp(group, ts_index, timestamp_val)``
    - ``"static"`` → ``write_static(group, array_name, data)`` (non-time-indexed; ``ts_index`` is ignored)

    ``data`` may be an eager ``numpy.ndarray`` or a zero-arg callable that
    returns one. It is resolved exactly once at dispatch time, in dispatch
    order. The callable must close over stable inputs (paths, configuration),
    not open file handles or per-batch scratch objects. Callable data is
    supported for ``kind="region"``, ``kind="static"`` and ``kind="1d"``. Passing a
    callable for other kinds raises ``TypeError`` at construction. Any
    callable exception propagates with the same error surface as an eager
    payload rejection.
    """

    group: str
    array: str
    ts_index: int
    data: np.ndarray | Callable[[], np.ndarray] | Any
    kind: str = "region"
    y_slice: slice | None = None
    channel_index: int | None = None
    timestamp_val: Any = None
    """Conceptual time/index axis value for ``kind="timestamp"`` writes.

    "timestamp" is a stable plugin-contract token, not the on-disk dim name;
    the actual dim/coord name comes from ``IndexedRegionStrategy.time_coord_name``.
    """

    def __post_init__(self) -> None:
        valid_kinds = {"region", "1d", "timestamp", "static"}
        if self.kind not in valid_kinds:
            raise ValueError(
                f"WriteIntent.kind must be one of {sorted(valid_kinds)!r}; got {self.kind!r}"
            )
        callable_kinds = {"region", "static", "1d"}
        if callable(self.data) and self.kind not in callable_kinds:
            supported_kinds = "', '".join(sorted(callable_kinds))
            raise TypeError(
                f"callable data is not supported for kind={self.kind!r}; "
                f"supported kinds: '{supported_kinds}'"
            )
        if self.kind == "timestamp" and self.timestamp_val is None:
            raise ValueError("WriteIntent with kind='timestamp' requires timestamp_val to be set")

    @classmethod
    def slot(cls, *, group: str, array: str, index: int, data: Any) -> WriteIntent:
        """Write a 1-D array slice at a single time slot.

        Use this for 1-D arrays that grow along the time axis — per-slot
        scalars, per-slot vectors, or any array where each slot contributes
        one row. The array must be declared with ``time_indexed=True`` in
        `ZarrArraySpec`.

        ``data`` may be an eager ``np.ndarray`` or a zero-arg callable that
        returns one; a callable is resolved once, at dispatch time, so a large
        whole-slot payload is only read when it is written.

        Args:
            group: Zarr group name matching a `ZarrGroupSpec` in the schema.
            array: Array name within the group.
            index: Time-slot index for this write.
            data: Array data to write at this slot.

        Returns:
            A ``WriteIntent`` with ``kind="1d"`` and ``ts_index=index``.

        Examples:
            >>> import numpy as np
            >>> intent = WriteIntent.slot(group="data", array="counts", index=7, data=np.zeros((4,)))
            >>> intent.kind
            '1d'
            >>> intent.ts_index
            7
        """
        return cls(group=group, array=array, ts_index=index, data=data, kind="1d")

    @classmethod
    def region(
        cls,
        *,
        group: str,
        array: str,
        index: int,
        data: Any,
        y_slice: slice,
        channel_index: int | None = None,
    ) -> WriteIntent:
        """Write a 2-D spatial region at a single time slot.

        Use this for the main image arrays — counts, radiances, quality flags,
        pixel times — where each slot contributes a spatial tile. The array
        must be declared with ``time_indexed=True`` in `ZarrArraySpec`.

        ``data`` may be an eager ``np.ndarray`` or a zero-arg callable
        ``Callable[[], np.ndarray]``; the callable is resolved at dispatch time.

        Args:
            group: Zarr group name matching a `ZarrGroupSpec` in the schema.
            array: Array name within the group.
            index: Time-slot index for this write.
            data: 2-D array data, or a callable that returns it.
            y_slice: Row slice within the array.
            channel_index: Channel dimension index, or ``None`` for non-channel arrays.

        Returns:
            A ``WriteIntent`` with ``kind="region"``.

        Examples:
            >>> import numpy as np
            >>> intent = WriteIntent.region(
            ...     group="data_1km", array="counts", index=3,
            ...     data=np.zeros((100, 2048)), y_slice=slice(0, 100),
            ... )
            >>> intent.kind
            'region'
            >>> intent.y_slice
            slice(0, 100, None)
        """
        return cls(
            group=group,
            array=array,
            ts_index=index,
            data=data,
            kind="region",
            y_slice=y_slice,
            channel_index=channel_index,
        )

    @classmethod
    def coordinate(cls, *, group: str, index: int, value: Any) -> WriteIntent:
        """Write the time-axis coordinate value for a single slot.

        Use this to record the actual timestamp (or integer index) that
        corresponds to ``ts_index``. The engine writes it into the time
        coordinate array so the output cube is self-describing.

        Args:
            group: Zarr group name matching a `ZarrGroupSpec` in the schema.
            index: Time-slot index for this coordinate.
            value: The coordinate value for this slot — typically a
                ``datetime``, ``numpy.datetime64``, or integer. Must not be
                ``None``.

        Returns:
            A ``WriteIntent`` with ``kind="timestamp"``.

        Raises:
            ValueError: If ``value`` is ``None``.

        Examples:
            >>> from datetime import datetime, timezone
            >>> intent = WriteIntent.coordinate(
            ...     group="data", index=5,
            ...     value=datetime(2024, 1, 1, tzinfo=timezone.utc),
            ... )
            >>> intent.kind
            'timestamp'
            >>> intent.ts_index
            5
        """
        return cls(
            group=group,
            array="__timestamp__",
            ts_index=index,
            data=None,
            kind="timestamp",
            timestamp_val=value,
        )

    @classmethod
    def static(cls, *, group: str, array: str, data: Any) -> WriteIntent:
        """Write a static (non-time-indexed) array — coordinate grids, lookup tables, masks.

        Use this for arrays that are the same across every time slot: latitude/
        longitude grids, channel names, calibration tables, spatial references.
        The array must be declared with ``time_indexed=False`` in
        `ZarrArraySpec`; the engine pre-creates it at its declared shape
        during schema setup.

        **Write-once contract**: the engine writes the array on the first ingest
        run and stamps a marker attribute. On any subsequent run (resume or
        re-ingest) the incoming data must be byte-identical to what was already
        written, or the ingest fails with `SchemaDriftError`. There is no
        partial-update path for static arrays.

        ``data`` may be an eager ``np.ndarray`` or a zero-arg callable
        ``Callable[[], np.ndarray]``; the callable is resolved at dispatch time.

        Args:
            group: Zarr group name matching a `ZarrGroupSpec` in the schema.
            array: Array name declared with ``time_indexed=False`` in that group.
            data: Data to write, or a callable that returns it.

        Returns:
            A ``WriteIntent`` with ``kind="static"``.

        Raises:
            SchemaDriftError: On resume, if ``data`` does not match the
                already-committed array byte-for-byte (NaN-aware).
            TypeError: If ``data`` is callable and ``kind`` is not ``"static"``
                (cannot happen via this factory; raised by ``__post_init__``
                only when constructing ``WriteIntent`` directly with a
                mismatched kind).
        """
        return cls(group=group, array=array, ts_index=0, data=data, kind="static")


def _compile_indexed_write(iw: IndexedWrite, resolved_index: ResolvedIndex) -> list[WriteIntent]:
    """Compile an `IndexedWrite` into a `WriteIntent` by resolving its slot.

    Pure function. No I/O, no logging, no state mutation, no caching side
    effects — the same ``(iw, resolved_index)`` inputs always produce equal
    outputs. The ``iw.data`` payload passes through unchanged: callable
    payloads are not invoked, arrays are not copied.

    Args:
        iw: The coordinate-keyed indexed write to compile.
        resolved_index: The resolved index used to look up ``iw.coordinate``
            within ``iw.group``.

    Returns:
        A single-element ``list[WriteIntent]``. Always a list — never a bare
        intent, never a generator — so callers can concatenate without
        special-casing.

    Raises:
        IndexedWriteCompilationError: If ``iw.coordinate`` cannot be resolved
            to a slot in ``iw.group`` (unknown group, coordinate not present,
            out-of-range integer, misaligned timestamp, wrong coordinate
            type). The original resolver error is chained via ``__cause__``,
            and ``reason`` ends with its type and message.
    """
    try:
        slot = resolved_index.position(iw.group, iw.coordinate)
    except (KeyError, ValueError, IndexError, TypeError) as exc:
        raise IndexedWriteCompilationError(
            coordinate=iw.coordinate,
            reason=(
                f"coordinate not in resolved index for group '{iw.group}': "
                f"{type(exc).__name__}: {exc}"
            ),
            iw_repr=repr(iw)[:200],
        ) from exc

    if iw._kind == "region":
        # ``IndexedWrite.region()`` builds always populate ``y_slice``; the
        # leading-underscore field signals the raw constructor is private.
        assert iw.y_slice is not None, (
            "IndexedWrite._kind='region' invariant violated: y_slice is None. "
            "Use IndexedWrite.region() rather than the raw constructor."
        )
        return [
            WriteIntent.region(
                group=iw.group,
                array=iw.array,
                index=slot,
                data=iw.data,
                y_slice=iw.y_slice,
                channel_index=iw.channel_index,
            )
        ]
    # iw._kind == "slot"
    return [
        WriteIntent.slot(
            group=iw.group,
            array=iw.array,
            index=slot,
            data=iw.data,
        )
    ]


def _axis_coordinate_name(axis: Any) -> str | None:
    if isinstance(axis, (RegularTimeAxis, IrregularTimeAxis)):
        return str(axis.coordinate)
    if isinstance(axis, IntegerAxis):
        return None
    return None


def _open_zarr_root_for_read(store_uri: str, storage_config: Any) -> Any | None:
    """Open the root group read-only; ``None`` only when the store is confirmed absent.

    A missing store raises ``FileNotFoundError`` (local path) or zarr's
    ``GroupNotFoundError``, a ``FileNotFoundError`` subclass (no root
    ``zarr.json``: local, fsspec and obstore alike). Any other failure, such as a
    transient read error, propagates: reading it as "absent" would skip the
    ownership and identity checks that rely on this.
    """
    from firecube.core.filesystem.store_factory import create_zarr_store
    from firecube.core.uris import is_remote_target
    from firecube.core.zarr.region_writer import open_root_read_only

    if not is_remote_target(store_uri):
        return open_root_read_only(store_uri)
    if storage_config is None:
        return None
    handle = create_zarr_store(uri=store_uri, storage_config=storage_config, mode="r")
    return open_root_read_only(store_uri, **handle.zarr_kwargs())


def _open_zarr_store_for_write(store_uri: str, storage_config: Any) -> Any:
    """A writable zarr Store at *store_uri* through the configured driver; no I/O.

    The fsspec driver hands back a URL plus options for a remote target; the
    metadata authority needs the configured Store object itself.

    Raises:
        ConfigurationError: A remote target without a storage configuration.
    """
    from zarr.storage import FsspecStore, LocalStore

    from firecube.core.filesystem.store_factory import create_zarr_store
    from firecube.core.uris import is_remote_target, local_path_from_target

    if storage_config is None:
        if is_remote_target(store_uri):
            raise ConfigurationError(f"no storage configuration to open {store_uri} for writing")
        return LocalStore(str(local_path_from_target(store_uri)))
    handle = create_zarr_store(uri=store_uri, storage_config=storage_config, mode="a")
    if isinstance(handle.store, str):
        return FsspecStore.from_url(
            handle.store, storage_options=handle.storage_options, read_only=False
        )
    return handle.store


class DirectZarrIngestor(BaseIngestor):
    """Abstract template for direct-Zarr region-based ingestors.

    Plugins that write directly to Zarr (bypassing xarray) should subclass
    this template and implement:

    - `zarr_schema` — declare groups and arrays.
    - `build_write_intents` — convert a batch into write operations.

    The template orchestrates store setup, write execution via a region write
    strategy, coverage tracking, and metrics aggregation.
    """

    template_config_class = ZarrTemplateConfig
    _zarr_write_session: ZarrWriteSession | None = None

    def index_spec(self, ctx: PluginContext) -> IndexSpec | None:
        """Override to enable slot-range parallel ingestion.

        Return an ``IndexSpec`` describing the product's index shape, or
        ``None`` for serial-only plugins (no ``--slot-start``/``--slot-end``).

        **``index_spec`` MUST be resolvable from typed config alone.** The
        implementation may read ``self.plugin_config`` and
        ``self.template_config``; it MUST NOT depend on ``ctx.source``
        contents (source listing, file peek, or ``--input-data``). Reason:
        ``firecube zarr slots`` and ``firecube zarr preallocate`` call
        ``index_spec`` without any ``--input-data``; if your product's epoch
        or size derives from source, expose an explicit config override (e.g.
        ``MyConfig.time_epoch``) and raise ``ConfigurationError`` naming the
        missing config field when the override is absent.

        Default returns ``None`` (serial-only plugin).
        """
        return None

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        """Override to enable slot-range parallel ingestion.

        Called by the engine for each source item to determine its slot index.
        Return an ``ItemInfo`` with the item's time coordinate, or ``None``
        to drop the item from this worker's slot range.

        Default raises ``NotImplementedError`` — override this method to
        enable parallel ingestion.

        Args:
            item: A source item from the batch.
            ctx: The plugin context for this run.

        Returns:
            An ``ItemInfo`` with the item's coordinate, or ``None`` to drop.

        Raises:
            NotImplementedError: If not overridden.
        """
        raise NotImplementedError(
            f"{type(self).__name__} did not override inspect_item(). "
            "Override this method to enable parallel ingestion; "
            "return None to drop items your plugin cannot map to a slot."
        )

    def resolved_index(self, ctx: PluginContext) -> ResolvedIndex:
        """Return the resolved index for this run, cached per context.

        Raises ``ConfigurationError`` if ``index_spec(ctx)`` returns ``None``
        (serial-only plugin).

        Args:
            ctx: The plugin context for this run.

        Returns:
            The ``ResolvedIndex`` for slot-index computation.

        Raises:
            ConfigurationError: If ``index_spec(ctx)`` returns ``None``.
        """
        if not hasattr(self, "_index_binding"):
            self._bind_index_at_startup(ctx)

        cache = getattr(self, "_resolved_index_cache", None)
        if cache is None:
            self._resolved_index_cache: dict[int, ResolvedIndex] = {}
            cache = self._resolved_index_cache
        key = id(getattr(ctx, "_ctx", ctx))
        if key not in cache:
            binding = self._index_binding
            if binding is None:
                raise ConfigurationError(
                    f"{type(self).__name__}.resolved_index(ctx) requires index_spec(ctx) "
                    "to return a non-None IndexSpec. "
                    "Override index_spec() to enable parallel ingestion."
                )
            cache[key] = binding.resolved
        return cache[key]

    @abstractmethod
    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        """Declare the Zarr store layout for this ingestor.

        Returns a list of group specifications describing every group and
        array that the ingestor may write to.  Called once per batch to
        ensure groups and arrays exist before writes begin.

        The declared groups are also the ingestor's write groups, so the
        schema must cover every group any ``WriteIntent`` targets.

        Examples:
            Declare one group with a time-indexed array and its time axis:

                def zarr_schema(self, ctx):
                    n_times = self.resolved_index(ctx).size("FWI")
                    # Or use a literal for serial mode.
                    return [
                        ZarrGroupSpec(
                            group="FWI",
                            arrays=[
                                ZarrArraySpec(
                                    name="fire_risk",
                                    shape=(n_times, 550, 475),
                                    dtype="float32",
                                    chunks=(1, 550, 475),
                                    dimension_names=("timestamp", "y", "x"),
                                ),
                                ZarrArraySpec(
                                    name="timestamp",
                                    shape=(n_times,),
                                    dtype="int64",
                                    dimension_names=("timestamp",),
                                ),
                            ],
                        )
                    ]
        """

    def _cached_zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        cache = getattr(self, "_zarr_schema_cache_by_ctx", None)
        if cache is None:
            self._zarr_schema_cache_by_ctx = cache = {}
        key = id(getattr(ctx, "_ctx", ctx))
        if key not in cache:
            cache[key] = self.zarr_schema(ctx)
        return cache[key]

    def get_batch_groups(self, items: Sequence[Any], ctx: PluginContext) -> list[str]:
        """Return the sorted set of groups declared by ``zarr_schema(ctx)``.

        On this template the group set is schema-declared, not item-derived:
        writes are routed by ``WriteIntent.group``. Plugins should NOT
        override this method — a hand-rolled group list can disagree with
        the declared schema and break group/schema agreement.
        """
        _ = items  # group set is schema-declared, not item-derived
        specs = self._cached_zarr_schema(ctx)
        return sorted({spec.group for spec in specs})

    def build_write_intents(
        self, batch: PipelineBatch, ctx: PluginContext
    ) -> Sequence[WriteIntent | IndexedWrite]:
        """Convert a batch into a list of write operations.

        Return one flat list that may freely mix two element types:

        - **``IndexedWrite``** — a coordinate-keyed write. Build it with
          ``coordinate=<timestamp or integer key>`` and the engine resolves
          the slot index for you; an unmappable coordinate raises
          ``IndexedWriteCompilationError`` before any write.
        - **``WriteIntent``** — a fully resolved write. Use it when you have
          computed the index yourself, and for writes that carry no slot
          coordinate (``WriteIntent.coordinate`` and ``WriteIntent.static``).

        Skip an item by not appending anything for it; emit several writes
        for one item by appending several elements. Return an empty list to
        skip the whole batch. Every element's ``group`` must exist in
        ``zarr_schema(ctx)``, and resolved indexes must fall inside the
        worker's slot range when slot-range parallelism is enabled.
        Compilation of ``IndexedWrite`` elements runs at the call site,
        outside this hook, so no override can bypass it.

        For every compiled ``IndexedWrite``, the engine also emits the slot's
        time-coordinate verify-write automatically (one per resolved slot,
        skipped when the list already carries an explicit
        ``WriteIntent.coordinate`` for that slot), so plugins on this path
        never resolve or emit coordinate writes themselves.

        Examples:
            One coordinate-keyed write per item plus one static array:

                def build_write_intents(self, batch, ctx):
                    out = []
                    for item in batch.items:
                        timestamp, values = read_product(ctx.materialize(item))
                        out.append(IndexedWrite.slot(
                            group="data", array="value",
                            coordinate=timestamp, data=values,
                        ))
                    out.append(WriteIntent.static(
                        group="grid", array="lat", data=self._lat_grid,
                    ))
                    return out

        Args:
            batch: The pipeline batch to convert.
            ctx: The plugin context for this run.

        Returns:
            A list mixing ``WriteIntent`` and ``IndexedWrite`` elements.

        Raises:
            NotImplementedError: If the plugin does not override this hook.
        """
        _ = batch, ctx
        raise NotImplementedError(
            f"{type(self).__name__} did not override build_write_intents(). "
            "Override it to emit WriteIntent and IndexedWrite elements."
        )

    def _compile_write_intents(
        self, raw: Sequence[WriteIntent | IndexedWrite], ctx: PluginContext
    ) -> list[WriteIntent]:
        """Resolve ``IndexedWrite`` elements and emit their coordinate writes.

        Runs after ``build_write_intents`` returns, so plugins cannot bypass
        compilation by overriding a hook. ``resolved_index(ctx)`` is consulted
        only when the list actually contains an ``IndexedWrite``, keeping
        serial plugins (``index_spec() -> None``) free to emit plain intents.

        For each unique ``(group, slot)`` produced by compilation, one
        ``WriteIntent.coordinate`` verify-write is appended automatically
        unless the plugin already emitted an explicit coordinate intent for
        that slot. The stored value is verified (never overwritten) by the
        marker-aware timestamp write path.
        """
        if not any(isinstance(element, IndexedWrite) for element in raw):
            return [element for element in raw if isinstance(element, WriteIntent)]
        resolved = self.resolved_index(ctx)
        intents: list[WriteIntent] = []
        explicit_coordinate_slots: set[tuple[str, int]] = set()
        compiled_slots: dict[tuple[str, int], Any] = {}
        for element in raw:
            if isinstance(element, IndexedWrite):
                compiled = _compile_indexed_write(element, resolved)
                intents.extend(compiled)
                for intent in compiled:
                    if intent.ts_index is not None:
                        compiled_slots.setdefault(
                            (element.group, intent.ts_index), element.coordinate
                        )
            else:
                intents.append(element)
                if element.kind == "timestamp" and element.ts_index is not None:
                    explicit_coordinate_slots.add((element.group, element.ts_index))
        for (group, slot), coordinate in compiled_slots.items():
            if (group, slot) in explicit_coordinate_slots:
                continue
            intents.append(WriteIntent.coordinate(group=group, index=slot, value=coordinate))
        return intents

    def _bind_index_at_startup(self, ctx: PluginContext) -> None:
        """Override: bind IndexSpec once at pod startup via base helper.

        Uses BaseIngestor._resolve_index_binding_at_startup so templates do not
        import runtime binding internals directly.
        """
        self._resolved_index_cache = {}
        self._index_binding = self._resolve_index_binding_at_startup(ctx)

    def _ensure_index_record_at_startup(self, ctx: PluginContext) -> None:
        """Override base hook: delegate to _ensure_index_identity_at_startup."""
        self._ensure_index_identity_at_startup(ctx)

    def _ensure_index_identity_at_startup(self, ctx: PluginContext) -> None:
        """Writes engine-owned `.firecube/index/current.json`; called once at pod startup."""
        if not hasattr(self, "_index_binding"):
            self._bind_index_at_startup(ctx)

        binding = self._index_binding
        if binding is None:
            return  # serial-mode plugin; no index identity to stamp
        if getattr(self, "_resolved_index_stamped", False):
            return

        self._verify_calendar_axes_preallocated_at_startup(ctx, binding.resolved)

        product = _ctx_product_name(ctx, self.name)
        self._check_legacy_index_record_at_startup(product=product, plugin_name=self.name)
        run_id = str(ctx.run_id or ctx.option("run_id", "unknown"))
        try:
            record = binding.resolved.as_resolved_index_record(run_id=run_id)
        except ExtentUnknownError:
            # Mixed spec (some groups bounded, some unbounded): the full
            # canonical record cannot be built because unbounded axes have
            # no fixed extent. Skip full-record persistence and verify each bounded group independently
            # against its stamped ``firecube_group_identity_hash`` coord attr.
            self._verify_per_group_identity_at_startup(ctx, binding.resolved)
            self._resolved_index_stamped = True
            return
        stored_record, outcome = self._chunk_manager.ensure_resolved_index(
            product=product,
            record=record,
            run_id=run_id,
        )
        self._emit_index_ensured_event(
            ctx=ctx,
            product=product,
            run_id=run_id,
            record=stored_record,
            outcome=outcome,
        )
        self._resolved_index_stamped = True

    def _verify_calendar_axes_preallocated_at_startup(
        self, ctx: PluginContext, resolved: ResolvedIndex
    ) -> None:
        """Refuse ingest into a calendar group whose time coordinate preallocate has not marked.

        Pods never create or fill a calendar coordinate; without this check the legacy
        timestamp write would create it as Gregorian ``datetime64``. Runs before any batch,
        for every run that binds an index.

        The check reads the final target in every write mode, because preallocate writes
        there and a staged workspace is not seeded yet. It reads each coordinate by its
        full path from a live root.

        Raises:
            ConfigurationError: A calendar group's coordinate is absent or lacks
                ``firecube_preallocated``.
        """
        calendar_groups: list[tuple[str, AxisSpec, str]] = []
        for group_name in resolved.groups:
            axis = resolved.axis_for(group_name)
            if axis is not None and not is_gregorian_axis(axis):
                calendar = getattr(axis, "calendar", None)
                assert calendar is not None  # is_gregorian_axis is True without one
                calendar_groups.append((group_name, axis, calendar))
        if not calendar_groups:
            return

        try:
            store_uri = self.resolve_output_uri(ctx, write_mode="direct")
        except ConfigurationError:
            # No resolvable output yet (e.g. a fresh in-memory context): nothing to
            # verify. Any other exception propagates; swallowing it would disable the check.
            return

        root = _open_zarr_root_for_read(store_uri, self._chunk_manager.storage_config)
        for group_name, axis, calendar in calendar_groups:
            coord_name = _axis_coordinate_name(axis)
            if coord_name is None:
                continue
            coord_array_path = f"{group_name}/{coord_name}"
            coord_arr = None if root is None else open_zarr_array(root, coord_array_path)
            if coord_arr is not None and bool(coord_arr.attrs.get(ATTR_PREALLOCATED, False)):
                continue
            raise ConfigurationError(
                Message(
                    "group {group!r} declares calendar={calendar!r} on its time axis; "
                    "its coordinate array {array_path!r} at target {product_uri} must be "
                    "preallocated before ingest. Calendar coordinates are single-writer: "
                    "the engine will not create or fill them on first ingest write. "
                    "Run: {preallocate}",
                    group=group_name,
                    calendar=calendar,
                    array_path=coord_array_path,
                    product_uri=store_uri,
                    preallocate=preallocate_operation(),
                )
            )

    def _verify_per_group_identity_at_startup(
        self, ctx: PluginContext, resolved: ResolvedIndex
    ) -> None:
        """Verify each bounded group's identity hash against the coord array attr.

        For mixed-spec cubes (some bounded, some unbounded groups), the full
        resolved-index record cannot be persisted because unbounded axes have
        no fixed extent. This helper implements the per-group defense-in-depth
        verification path: for each bounded group in the resolved spec, it
        computes the canonical ``firecube_group_identity_hash`` and compares
        it against the stamp on the coord array. On divergence it raises
        ``SchemaDriftError`` naming the group and both hashes.

        Unbounded groups are skipped (no hash to compare). Missing stamps are
        treated as skip (backward-compat with pre-hash stores and fresh
        ingests that have not yet run ``firecube zarr preallocate``).
        """
        write_mode = self.engine_config.write_mode
        try:
            store_uri = self.resolve_output_uri(ctx, write_mode=write_mode)
        except ConfigurationError:
            # No resolvable output yet (e.g. fresh in-memory context): nothing
            # to verify. Any other exception must propagate; swallowing it
            # would silently disable per-group identity verification.
            return

        self._verify_per_group_identity_at_store(store_uri, resolved)

    def _verify_per_group_identity_at_store(self, store_uri: str, resolved: ResolvedIndex) -> None:
        root = _open_zarr_root_for_read(store_uri, self._chunk_manager.storage_config)
        if root is None:
            return

        for group_name in resolved.groups:
            axis = resolved.axis_for(group_name)
            if axis is None:
                continue
            try:
                resolved_size = int(resolved.size(group_name))
            except ExtentUnknownError:
                continue  # unbounded group: no per-group hash to verify
            coord_name = _axis_coordinate_name(axis)
            if coord_name is None:
                continue
            coord_array_path = f"{group_name}/{coord_name}"
            coord_arr = open_zarr_array(root, coord_array_path)
            if coord_arr is None:
                continue  # coord array not yet created — skip verification
            stamped = coord_arr.attrs.get(FIRECUBE_GROUP_IDENTITY_HASH_ATTR)
            if stamped is None:
                continue  # legacy/fresh store: no stamp to compare against
            dtype = coord_arr.dtype
            expected = compute_group_identity_hash(axis, resolved_size, dtype)
            if str(stamped) != expected:
                raise SchemaDriftError(
                    f"per-group identity drift for group {group_name!r} at "
                    f"{coord_array_path}: stored={stamped!s}, declared={expected}"
                )

    def _verify_schema_at_pod_startup(self, ctx: PluginContext) -> None:
        """Verify global direct-Zarr schema once per pod process before batches run."""
        from firecube.ingestor.api import IndexedRegionStrategy

        # Called once per run, on the main thread before any batch: every run
        # kind gets its own session here (empty ledger, ownership unchecked),
        # serial runs included.
        session = self._begin_write_session(ctx)

        if self._parallel_execution_state is None:
            return

        write_mode = self.engine_config.write_mode
        store_uri = self.resolve_output_uri(ctx, write_mode=write_mode)
        product = session.product
        run_id = session.run_id

        schema = self._cached_zarr_schema(ctx)
        adapter = self._write_adapter(schema)
        # Before any array is created: pods never create or write engine-owned arrays.
        adapter.require_engine_owned_materialized(ctx)
        # Before schema setup touches the store: the window must own every
        # stored unit it writes into.
        adapter.require_window_owns_stored_units(ctx)
        coord_names_by_group = {spec.group: spec.coord_names for spec in schema}
        strategy = IndexedRegionStrategy(
            store_uri=store_uri,
            schema=schema,
            coord_names_by_group=coord_names_by_group,
            time_coord_name=self._resolve_time_dim_name(),
            storage_config=self._chunk_manager.storage_config,
        )

        global_expected = self._parallel_execution_state.global_expected
        for group_name, expected_time_count in global_expected.items():
            if self._parallel_execution_state.schema_verified.get(group_name, False):
                continue
            filtered_group_specs = [spec for spec in schema if spec.group == group_name]
            if not filtered_group_specs:
                raise ConfigurationError(
                    f"_verify_schema_at_pod_startup: group '{group_name}' from global_expected "
                    f"is not declared in zarr_schema(). This should have been caught by "
                    f"validate_global_expected_subset_of_schema in the capability gate; "
                    f"this hard-fail is defense-in-depth."
                )
            _setup_global_zarr_schema(
                strategy=strategy,
                schema=filtered_group_specs,
                global_expected={group_name: expected_time_count},
                product=product,
                run_id=run_id,
                chunk_manager=self._chunk_manager,
                time_coord_name=self._resolve_time_dim_name(),
                template_config=getattr(self, "template_config", None),
            )
            self._parallel_execution_state.schema_verified[group_name] = True
            schema_hash = _compute_schema_hash(
                filtered_group_specs,
                {group_name: expected_time_count},
            )
            try:
                self._chunk_manager.record_schema_verification(
                    product=product,
                    run_id=run_id,
                    group=group_name,
                    plugin=self.name,
                    schema_hash=schema_hash,
                    verified_at=datetime.now(UTC).isoformat(),
                    expected_time_count=expected_time_count,
                    meta={"write_mode": str(write_mode)},
                )
            except Exception as exc:
                self._log.warning(
                    "Failed to record schema verification audit event for group %s: %s",
                    group_name,
                    exc,
                )

    # --- Write session --------------------------------------------------------------------

    def _begin_write_session(self, ctx: PluginContext) -> ZarrWriteSession:
        """Build this run's write session, closing any earlier run's.

        Called on the main thread before any batch or worker. Window, slot
        group and write mode come from ``engine_config``; a staged session
        also gets the workspace URI, for its seeding authority, and the final
        target, opened at promotion for the authority promoted metadata is
        published under. An earlier session whose close still fails (its
        writes in flight) refuses this run and stays in place.
        """
        previous = self._zarr_write_session
        if previous is not None:
            previous.close()
            self._zarr_write_session = None
        engine_config = self.engine_config
        start = getattr(engine_config, "slot_start", None)
        end = getattr(engine_config, "slot_end", None)
        write_mode = str(engine_config.write_mode)
        session = ZarrWriteSession(
            product=_ctx_product_name(ctx, self.name),
            run_id=str(ctx.run_id or ctx.option("run_id", "unknown")),
            write_mode=write_mode,
            window=(start, end) if isinstance(start, int) and isinstance(end, int) else None,
            slot_group=getattr(engine_config, "slot_group", None),
            acquire_claim=lambda **kwargs: self._chunk_manager.acquire_claim(**kwargs),
            open_root=lambda uri: _open_zarr_root_for_read(uri, self._chunk_manager.storage_config),
            # In staged mode the workspace never holds engine-owned chunks:
            # their values, and the stored layout, live in the final target.
            open_final_root=lambda c: _open_zarr_root_for_read(
                self.resolve_output_uri(c, write_mode="direct"),
                self._chunk_manager.storage_config,
            ),
            workspace_uri=self.resolve_output_uri(ctx, write_mode=write_mode)
            if write_mode == "staged"
            else None,
            open_final_store=lambda: self._final_store(ctx),
        )
        self._zarr_write_session = session
        return session

    def _final_store(self, ctx: PluginContext) -> tuple[str, Any]:
        """The final target's URI and a writable zarr Store over it; no I/O."""
        final_uri = self.resolve_output_uri(ctx, write_mode="direct")
        return final_uri, _open_zarr_store_for_write(final_uri, self._chunk_manager.storage_config)

    def _write_session(self) -> ZarrWriteSession:
        """The run's write session; a run that never built one is refused."""
        session = self._zarr_write_session
        if session is None:
            raise ConfigurationError(
                f"{type(self).__name__} holds no write session: _verify_schema_at_pod_startup "
                "builds it before any batch, and a subclass overriding that hook must call super()"
            )
        return session

    def _write_adapter(self, schema: Sequence[ZarrGroupSpec]) -> DirectRegionAdapter:
        return DirectRegionAdapter(
            self._write_session(),
            schema,
            time_coord_name=self._resolve_time_dim_name(),
            index_binding=getattr(self, "_index_binding", None),
        )

    def _close_run_state(self) -> None:
        """End of run, whatever its outcome: the session turns terminal and is dropped.

        It is dropped only once `ZarrWriteSession.close` succeeded: a close
        that raises (writes still in flight) leaves it in place, so a retry
        and `_write_session` see the same, terminal session.
        """
        session = self._zarr_write_session
        if session is not None:
            session.close()
            self._zarr_write_session = None

    def _refuse_sparse_staged_window(
        self,
        ctx: PluginContext,
        planned_slots: frozenset[int],
        *,
        window: tuple[int, int],
        slot_group: str | None,
    ) -> None:
        """Refuse, before any batch, a staged window that fills only part of a unit.

        Called by the engine, for staged runs only, with the slot of every
        source item the window keeps.
        """
        self._write_adapter(self._cached_zarr_schema(ctx)).refuse_sparse_staged_window(
            ctx, planned_slots, window=window, slot_group=slot_group
        )

    def _staged_seeding_authority(self) -> MetadataAuthority | None:
        """The authority the engine seeds this run's staged workspace metadata under.

        Called by the engine before each batch of a Zarr output. The session's
        ``PROCESS_LOCAL`` authority over the whole workspace store in a staged
        run; ``None`` otherwise.
        """
        return self._write_session().staged_seeding_authority()

    def _note_seeded_coordinates(self, workspace: str, paths: Collection[str]) -> None:
        """Record the coordinate arrays whose chunks seeding left in *workspace*.

        Called by the engine after each batch's seeding. Promotion classifies
        these coordinates by the target's lifecycle marker.
        """
        self._write_session().note_seeded_coordinates(workspace, paths)

    def _refuse_incomplete_staged_units(self) -> None:
        """Refuse promotion of a workspace unit this run did not write in every cell.

        Called by the engine just before a staged run is promoted. The target
        has not been touched yet, so a refusal leaves it as it was. Catches
        what the window check cannot see before writing: serial runs, and rows
        or channels a slot's writes leave out.
        """
        self._write_session().verify_promotion()

    def _staged_promotion_units(self) -> StagedPromotionUnits | None:
        """The units this staged run proved written, for an object-delta promotion.

        Called by the engine after `_refuse_incomplete_staged_units` passed,
        for staged runs only. Promotion then puts the units the run wrote,
        deletes from the target the ones it left all fill (zarr stores no
        object for them) and publishes the ``zarr.json`` documents under the
        session's authority over the target. It leaves alone the seeded chunks
        of a time coordinate the target marks managed.
        """
        return self._write_session().staged_promotion_units(
            time_coord_name=self._resolve_time_dim_name()
        )

    def _aggregate_metrics(
        self, ctx: RuntimeIngestContext, state: PipelineRunState
    ) -> dict[str, Any]:
        """Aggregate run metrics using the default batch-merge policy."""
        return merge_batch_metrics(ctx, state)

    def _process_batch(self, batch: PipelineBatch, ctx: PluginContext) -> PipelineResult:
        """Execute direct-Zarr writes for one batch.

        Orchestrates: ``batch_setup`` → ``prepare_batch_data`` → schema setup →
        intent generation → strategy execution → coverage and metrics assembly.
        ``cleanup_batch_data`` and ``batch_teardown`` fire in ``finally`` on
        success, empty-intents, and failure paths — mirroring
        `GenericZarrIngestor._process_batch` verbatim so plugin authors
        get the same lifecycle contract across both templates.
        """
        from firecube.ingestor.api import IndexedRegionStrategy

        self.batch_setup(ctx)

        try:
            session = self._write_session()
            prep_metrics = self.prepare_batch_data(batch, ctx) or {}

            write_mode = self.engine_config.write_mode
            store_uri = self.resolve_output_uri(ctx, write_mode=write_mode)

            schema = self._cached_zarr_schema(ctx)

            # Validate per-array codec declarations against template before any write.
            # Lazy import avoids a circular import between direct_zarr and templates.config.
            template_cfg = getattr(self, "template_config", None)
            if template_cfg is not None:
                from firecube.ingestor.templates.config import (
                    validate_zarr_specs_against_template,
                )

                all_specs = [arr for group_spec in schema for arr in group_spec.arrays]
                validate_zarr_specs_against_template(all_specs, template_cfg)

            coord_names_by_group = {spec.group: spec.coord_names for spec in schema}
            adapter = self._write_adapter(schema)
            adapter.require_engine_owned_materialized(ctx)

            intents = self._compile_write_intents(self.build_write_intents(batch, ctx), ctx)
            if not intents:
                return PipelineResult(
                    batch=batch,
                    outputs=OutputPaths(primary=str(store_uri)),
                    metrics={**prep_metrics, "count": 0},
                    success=True,
                )

            # Pods never write engine-owned arrays: the adapter verifies those
            # intents against the target and drops them from the writes.
            owned_intents, intents_to_write = adapter.split_engine_owned(ctx, intents)
            if not intents_to_write:
                adapter.require_engine_owned_slots(ctx, owned_intents)
                return PipelineResult(
                    batch=batch,
                    outputs=OutputPaths(primary=str(store_uri)),
                    metrics={**prep_metrics, "count": len(intents)},
                    success=True,
                )
            # Staged: the ledger and the writer must name one slot, whatever
            # another batch later does to the array's extent.
            intents_to_write = adapter.absolute_slots(str(store_uri), intents_to_write)

            strategy = IndexedRegionStrategy(
                store_uri=store_uri,
                schema=schema,
                coord_names_by_group=coord_names_by_group,
                time_coord_name=self._resolve_time_dim_name(),
                storage_config=self._chunk_manager.storage_config,
            )
            codec_pipelines_by_array = {
                (group_spec.group, arr_spec.name): derive_effective_codecs_for_spec(
                    arr_spec,
                    getattr(self, "template_config", None),
                )
                for group_spec in schema
                for arr_spec in group_spec.arrays
            }

            group_to_intents: dict[str, list[WriteIntent]] = {}
            for intent in intents_to_write:
                group_to_intents.setdefault(intent.group, []).append(intent)

            if not hasattr(self, "_index_binding"):
                self._bind_index_at_startup(ctx)

            binding = self._index_binding
            if session.window is not None and binding is not None:
                # Defense in depth: serial-mode plugins should already be gated upstream,
                # but this keeps the failure a ConfigurationError if the gate is bypassed.
                global_expected: dict[str, int] = {}
                for group in binding.resolved.groups:
                    try:
                        global_expected[group] = binding.resolved.size(group)
                    except ExtentUnknownError as exc:
                        raise UnboundedAxisError(group) from exc

                # Strict coverage: every group receiving WriteIntents MUST be declared
                # in index_spec(ctx). Without this, a group could be written to by
                # parallel pods but schema pre-allocation never happened, causing deadlock or corruption.
                # Extra groups in global_expected are ALLOWED (debug log only): they are sidecars
                # (e.g., lat/lon groups that the plugin manages separately without slot-range intents).
                groups_with_intents: set[str] = set(group_to_intents.keys())
                groups_in_global: set[str] = set(global_expected.keys())
                missing_from_global = groups_with_intents - groups_in_global
                if missing_from_global:
                    raise ConfigurationError(
                        f"Parallel mode: groups {sorted(missing_from_global)} received WriteIntents "
                        f"but are not declared in index_spec(ctx) (declared: "
                        f"{sorted(groups_in_global)}). "
                        "Every group that receives writes must be declared in "
                        "index_spec(ctx) for safe parallel-pod schema pre-allocation."
                    )
                # Defense in depth: intent groups must also be in zarr_schema()
                groups_in_schema: set[str] = {spec.group for spec in schema}
                missing_from_schema = groups_with_intents - groups_in_schema
                if missing_from_schema:
                    raise ConfigurationError(
                        f"WriteIntents reference groups {sorted(missing_from_schema)} that are "
                        f"not declared in zarr_schema() ({sorted(groups_in_schema)}). "
                        "build_write_intents() must only emit intents for groups declared in "
                        "zarr_schema()."
                    )
                # extras_in_global cannot happen: validate_global_expected_subset_of_schema
                # in the capability gate ensures global_expected.keys() ⊆ zarr_schema() groups at startup.

            # Before the staged ledger and any write: pods write declared arrays
            # only, and only slots whose engine-owned values preallocate wrote.
            adapter.admit(ctx, owned_intents, intents_to_write)

            ctx_config = getattr(ctx, "config", None)
            concurrency_template_config = getattr(
                ctx_config,
                "template_config",
                getattr(self, "template_config", None),
            )
            region_write_concurrency = getattr(
                concurrency_template_config,
                "zarr_region_write_concurrency",
                1,
            )
            zarr_write_empty_chunks = getattr(
                concurrency_template_config,
                "zarr_write_empty_chunks",
                False,
            )
            with adapter.writing(str(store_uri), intents_to_write):
                metrics = strategy.write_groups(
                    group_to_intents=group_to_intents,
                    schema=schema,
                    claim_for_group=session.schema_claim,
                    claim_for_slot=session.slot_claim,
                    slot_range=session.window,
                    slot_group=session.slot_group,
                    codec_pipelines_by_array=codec_pipelines_by_array,
                    region_write_concurrency=region_write_concurrency,
                    suppress_static_emission_for_non_owner=getattr(
                        self.engine_config,
                        "suppress_static_emission_for_non_owner",
                        False,
                    ),
                    static_owner_slot_start=getattr(
                        self.engine_config,
                        "static_owner_slot_start",
                        None,
                    ),
                    zarr_write_empty_chunks=zarr_write_empty_chunks,
                    on_write=(
                        functools.partial(session.record_write_result, store_uri=str(store_uri))
                        if session.staged
                        else None
                    ),
                    track_units=session.staged,
                )

            # prep_metrics unpacked first so template-owned keys win on collision;
            # reversing would let plugin prep silently clobber reported metrics.
            final_metrics = {
                **prep_metrics,
                "zarr": metrics,
                "coverage": metrics.get("coverage", []),
                "count": len(intents),
                "storage_handled": write_mode_policy(write_mode).storage_handled_by_engine,
            }

            return PipelineResult(
                batch=batch,
                outputs=OutputPaths(primary=str(store_uri), zarr=str(store_uri)),
                metrics=final_metrics,
                success=True,
            )

        except ConfigurationError:
            raise
        except Exception as exc:
            self._log.exception("Direct Zarr batch processing failed")
            return PipelineResult(
                batch=batch,
                outputs=OutputPaths(primary=""),
                success=False,
                error=str(exc),
                failure=exc if isinstance(exc, FirecubeError) else None,
            )

        finally:
            try:
                self.cleanup_batch_data(batch, ctx)
            except Exception as exc:
                self._log.warning("Batch cleanup failed: %s", exc)
            self.batch_teardown(ctx)


__all__ = [
    "DirectZarrIngestor",
    "WriteIntent",
    "ZarrArraySpec",
    "ZarrGroupSpec",
    "_compute_schema_hash",
    "_setup_global_zarr_schema",
]
