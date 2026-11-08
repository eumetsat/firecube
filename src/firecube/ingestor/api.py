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

"""Deprecated import path for Firecube plugin names. It will be removed in v0.2.0.

Import from `firecube.sdk` instead. The first name read through this module raises one
`DeprecationWarning` per process.
"""

from __future__ import annotations

import warnings
from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from firecube.core.controlplane import SpanCoverage, WriteDomain
    from firecube.core.controlplane.types import (
        ItemManifestEntry,
        ResolvedIndexRecord,
        validate_manifest_entries,
    )
    from firecube.core.errors import (
        DuplicateIrregularCoordinateError,
        IndexedWriteCompilationError,
        MissingIrregularCoordinateError,
        NoDiscoveredItemsError,
    )
    from firecube.core.index_resolve import ExtentUnknownError, ResolvedIndex, resolve_index_spec
    from firecube.core.index_spec import (
        AUTO,
        AxisSpec,
        IndexSpec,
        IntegerAxis,
        IrregularTimeAxis,
        ItemInfo,
        RegularTimeAxis,
        TimeAxis,
    )
    from firecube.core.index_spec import (
        _canonical_coordinate_value as canonical_coordinate_value,  # noqa: F401
    )
    from firecube.core.indexed_write import IndexedWrite
    from firecube.core.intake import CatalogGroupInfo
    from firecube.core.zarr._reserved_attrs import (
        FIRECUBE_STATIC_WRITTEN_ATTR,
        RESERVED_ARRAY_ATTRS,
        assert_attrs_safe,
    )
    from firecube.core.zarr.migration import (
        ABSENT,
        AttributePatch,
        MetadataPlan,
        MigrationStep,
        NodeView,
        ReleaseRecord,
        StoreView,
    )
    from firecube.ingestor.config.engine import EngineConfig, config_keys
    from firecube.ingestor.contracts.interfaces import (
        DatasetProducer,
        Ingestor,
        PipelineHost,
        SourceFile,
        is_dataset_producer,
    )
    from firecube.ingestor.errors import (
        ConfigurationError,
        IngestorError,
        ManifestError,
        RangeOverlapError,
        ResumeConflictError,
        SchemaDriftError,
        SchemaSizeMismatchError,
        StorageError,
        UnboundedAxisError,
        WriteIntentRangeError,
    )
    from firecube.ingestor.registry.loader import discover_ingestors, register_ingestor
    from firecube.ingestor.runtime.aggregation import merge_batch_metrics
    from firecube.ingestor.runtime.base import BaseIngestor, LocalSourceFile
    from firecube.ingestor.runtime.tensogram.strategy import TensogramWriteStrategy
    from firecube.ingestor.runtime.zarr.contracts import AppendWriteStrategy, RegionWriteStrategy
    from firecube.ingestor.runtime.zarr.existing_cube_check import verify_dim_compatibility
    from firecube.ingestor.runtime.zarr.strategies.append import AppendStrategy
    from firecube.ingestor.runtime.zarr.strategies.indexed_region import IndexedRegionStrategy
    from firecube.ingestor.templates.config import (
        ParquetTemplateConfig,
        TemplateConfig,
        TensogramTemplateConfig,
        ZarrTemplateConfig,
    )
    from firecube.ingestor.templates.direct_zarr import (
        DirectZarrIngestor,
        WriteIntent,
        ZarrArraySpec,
        ZarrGroupSpec,
    )
    from firecube.ingestor.templates.generic import GenericParquetIngestor, GenericZarrIngestor
    from firecube.ingestor.templates.generic_tensogram import GenericTensogramIngestor
    from firecube.ingestor.types.config import PluginConfig
    from firecube.ingestor.types.context import (
        ExecutionParams,
        IngestContext,
        IngestResult,
        OutputPaths,
        PipelineBatch,
        PipelineResult,
        PipelineRunState,
        PluginContext,
        ResultMetrics,
        RuntimeIngestContext,
        StorageContext,
    )
    from firecube.ingestor.types.manifest import IngestManifest
    from firecube.ingestor.types.planned_range import (
        PlannedRange,
        SlotRange,
        chunk_align_ranges,
        compute_covered_ranges,
        validate_chunk_alignment,
        validate_slot_range,
        warn_if_misaligned,
    )
    from firecube.ingestor.types.result_metrics import PipelineMetrics, StorageMetrics

__all__ = [
    "ABSENT",
    "AUTO",
    "FIRECUBE_STATIC_WRITTEN_ATTR",
    "RESERVED_ARRAY_ATTRS",
    "AppendStrategy",
    "AppendWriteStrategy",
    "AttributePatch",
    "AxisSpec",
    "BaseIngestor",
    "CatalogGroupInfo",
    "ConfigurationError",
    "DatasetProducer",
    "DirectZarrIngestor",
    "DuplicateIrregularCoordinateError",
    "EngineConfig",
    "ExecutionParams",
    "ExtentUnknownError",
    "GenericParquetIngestor",
    "GenericTensogramIngestor",
    "GenericZarrIngestor",
    "IndexSpec",
    "IndexedRegionStrategy",
    "IndexedWrite",
    "IndexedWriteCompilationError",
    "IngestContext",
    "IngestManifest",
    "IngestResult",
    "Ingestor",
    "IngestorError",
    "IntegerAxis",
    "IrregularTimeAxis",
    "ItemInfo",
    "ItemManifestEntry",
    "LocalSourceFile",
    "ManifestError",
    "MetadataPlan",
    "MigrationStep",
    "MissingIrregularCoordinateError",
    "NoDiscoveredItemsError",
    "NodeView",
    "OutputPaths",
    "ParquetTemplateConfig",
    "PipelineBatch",
    "PipelineHost",
    "PipelineMetrics",
    "PipelineResult",
    "PipelineRunState",
    "PlannedRange",
    "PluginConfig",
    "PluginContext",
    "RangeOverlapError",
    "RegionWriteStrategy",
    "RegularTimeAxis",
    "ReleaseRecord",
    "ResolvedIndex",
    "ResolvedIndexRecord",
    "ResultMetrics",
    "ResumeConflictError",
    "RuntimeIngestContext",
    "SchemaDriftError",
    "SchemaSizeMismatchError",
    "SlotRange",
    "SourceFile",
    "SpanCoverage",
    "StorageContext",
    "StorageError",
    "StorageMetrics",
    "StoreView",
    "TemplateConfig",
    "TensogramTemplateConfig",
    "TensogramWriteStrategy",
    "TimeAxis",
    "UnboundedAxisError",
    "WriteDomain",
    "WriteIntent",
    "WriteIntentRangeError",
    "ZarrArraySpec",
    "ZarrGroupSpec",
    "ZarrTemplateConfig",
    "assert_attrs_safe",
    "chunk_align_ranges",
    "compute_covered_ranges",
    "config_keys",
    "discover_ingestors",
    "is_dataset_producer",
    "merge_batch_metrics",
    "register_ingestor",
    "resolve_index_spec",
    "validate_chunk_alignment",
    "validate_manifest_entries",
    "validate_slot_range",
    "verify_dim_compatibility",
    "warn_if_misaligned",
]

# Source module of every name. A name never resolves through another deprecated module.
_SOURCES: dict[str, tuple[str, ...]] = {
    "firecube.core.controlplane": ("SpanCoverage", "WriteDomain"),
    "firecube.core.controlplane.types": (
        "ItemManifestEntry",
        "ResolvedIndexRecord",
        "validate_manifest_entries",
    ),
    "firecube.core.errors": (
        "DuplicateIrregularCoordinateError",
        "IndexedWriteCompilationError",
        "MissingIrregularCoordinateError",
        "NoDiscoveredItemsError",
    ),
    "firecube.core.index_resolve": ("ExtentUnknownError", "ResolvedIndex", "resolve_index_spec"),
    "firecube.core.index_spec": (
        "AUTO",
        "AxisSpec",
        "IndexSpec",
        "IntegerAxis",
        "IrregularTimeAxis",
        "ItemInfo",
        "RegularTimeAxis",
        "TimeAxis",
        "canonical_coordinate_value",
    ),
    "firecube.core.indexed_write": ("IndexedWrite",),
    "firecube.core.intake": ("CatalogGroupInfo",),
    "firecube.core.zarr._reserved_attrs": (
        "FIRECUBE_STATIC_WRITTEN_ATTR",
        "RESERVED_ARRAY_ATTRS",
        "assert_attrs_safe",
    ),
    "firecube.ingestor.config.engine": ("EngineConfig", "config_keys"),
    "firecube.ingestor.contracts.interfaces": (
        "DatasetProducer",
        "Ingestor",
        "PipelineHost",
        "SourceFile",
        "is_dataset_producer",
    ),
    "firecube.core.zarr.migration": (
        "ABSENT",
        "AttributePatch",
        "MetadataPlan",
        "MigrationStep",
        "NodeView",
        "ReleaseRecord",
        "StoreView",
    ),
    "firecube.ingestor.errors": (
        "ConfigurationError",
        "IngestorError",
        "ManifestError",
        "RangeOverlapError",
        "ResumeConflictError",
        "SchemaDriftError",
        "SchemaSizeMismatchError",
        "StorageError",
        "UnboundedAxisError",
        "WriteIntentRangeError",
    ),
    "firecube.ingestor.registry.loader": ("discover_ingestors", "register_ingestor"),
    "firecube.ingestor.runtime.aggregation": ("merge_batch_metrics",),
    "firecube.ingestor.runtime.base": ("BaseIngestor", "LocalSourceFile"),
    "firecube.ingestor.runtime.tensogram.strategy": ("TensogramWriteStrategy",),
    "firecube.ingestor.runtime.zarr.contracts": ("AppendWriteStrategy", "RegionWriteStrategy"),
    "firecube.ingestor.runtime.zarr.existing_cube_check": ("verify_dim_compatibility",),
    "firecube.ingestor.runtime.zarr.strategies.append": ("AppendStrategy",),
    "firecube.ingestor.runtime.zarr.strategies.indexed_region": ("IndexedRegionStrategy",),
    "firecube.ingestor.templates.config": (
        "ParquetTemplateConfig",
        "TemplateConfig",
        "TensogramTemplateConfig",
        "ZarrTemplateConfig",
    ),
    "firecube.ingestor.templates.direct_zarr": (
        "DirectZarrIngestor",
        "WriteIntent",
        "ZarrArraySpec",
        "ZarrGroupSpec",
    ),
    "firecube.ingestor.templates.generic": ("GenericParquetIngestor", "GenericZarrIngestor"),
    "firecube.ingestor.templates.generic_tensogram": ("GenericTensogramIngestor",),
    "firecube.ingestor.types.config": ("PluginConfig",),
    "firecube.ingestor.types.context": (
        "ExecutionParams",
        "IngestContext",
        "IngestResult",
        "OutputPaths",
        "PipelineBatch",
        "PipelineResult",
        "PipelineRunState",
        "PluginContext",
        "ResultMetrics",
        "RuntimeIngestContext",
        "StorageContext",
    ),
    "firecube.ingestor.types.manifest": ("IngestManifest",),
    "firecube.ingestor.types.planned_range": (
        "PlannedRange",
        "SlotRange",
        "chunk_align_ranges",
        "compute_covered_ranges",
        "validate_chunk_alignment",
        "validate_slot_range",
        "warn_if_misaligned",
    ),
    "firecube.ingestor.types.result_metrics": ("PipelineMetrics", "StorageMetrics"),
}
# Names whose definition carries another name.
_RENAMED = {"canonical_coordinate_value": "_canonical_coordinate_value"}
# Names firecube.sdk does not carry, listed as in firecube.core.api so the warning imports nothing.
_NOT_IN_SDK = frozenset(
    {
        "AppendStrategy",
        "AppendWriteStrategy",
        "DatasetProducer",
        "IndexedRegionStrategy",
        "IngestManifest",
        "LocalSourceFile",
        "PipelineHost",
        "PlannedRange",
        "RegionWriteStrategy",
        "SlotRange",
        "SpanCoverage",
        "TensogramWriteStrategy",
        "WriteDomain",
        "canonical_coordinate_value",
        "chunk_align_ranges",
        "compute_covered_ranges",
        "config_keys",
        "is_dataset_producer",
        "merge_batch_metrics",
        "validate_chunk_alignment",
        "validate_manifest_entries",
        "validate_slot_range",
        "warn_if_misaligned",
    }
)
_MODULE_OF = {name: module for module, names in _SOURCES.items() for name in names}
_warned = False


def _message(name: str) -> str:
    text = (
        f"{__name__} is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead."
    )
    if name in _NOT_IN_SDK:
        text += f" firecube.sdk does not carry {name}; open an issue if your plugin needs it."
    return text


def __getattr__(name: str) -> Any:
    """Resolve a deprecated name from its source module; warn on the first name only."""
    global _warned
    module = _MODULE_OF.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    # The flag, not the warnings registry, keeps this to one warning per process: the
    # registry deduplicates per caller location. The resolved name is not cached in the
    # module namespace, so that no name is ever bound here outside TYPE_CHECKING.
    if not _warned:
        _warned = True
        warnings.warn(_message(name), DeprecationWarning, stacklevel=2)
    return getattr(import_module(module), _RENAMED.get(name, name))


def __dir__() -> list[str]:
    return list(__all__)
