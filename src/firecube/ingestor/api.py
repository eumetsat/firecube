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

"""Public API for Firecube Ingestor Plugins.

Plugins should import from this module rather than accessing internals directly.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

from firecube.core.api import (
    AUTO,
    AxisSpec,
    DuplicateIrregularCoordinateError,
    IndexedWrite,
    IndexedWriteCompilationError,
    IndexSpec,
    IntegerAxis,
    IrregularTimeAxis,
    ItemInfo,
    ItemManifestEntry,
    MissingIrregularCoordinateError,
    NoDiscoveredItemsError,
    RegularTimeAxis,
    ResolvedIndex,
    ResolvedIndexRecord,
    validate_manifest_entries,
)
from firecube.core.controlplane import SpanCoverage, WriteDomain
from firecube.core.index_spec import (
    _canonical_coordinate_value as canonical_coordinate_value,  # noqa: F401
)
from firecube.core.intake import CatalogGroupInfo
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
    WriteIntentRangeError,
)
from firecube.ingestor.registry.loader import (
    discover_ingestors,
    register_ingestor,
)
from firecube.ingestor.runtime.aggregation import merge_batch_metrics
from firecube.ingestor.runtime.base import (
    BaseIngestor,
    LocalSourceFile,
)
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
from firecube.ingestor.types.config import PluginConfig
from firecube.ingestor.types.context import (
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

if TYPE_CHECKING:
    from firecube.ingestor.templates.direct_zarr import (
        DirectZarrIngestor,
        WriteIntent,
        ZarrArraySpec,
        ZarrGroupSpec,
    )
    from firecube.ingestor.templates.generic import GenericParquetIngestor, GenericZarrIngestor
    from firecube.ingestor.templates.generic_tensogram import GenericTensogramIngestor
__all__ = [
    "AUTO",
    "AppendStrategy",
    "AppendWriteStrategy",
    "AxisSpec",
    "BaseIngestor",
    "CatalogGroupInfo",
    "ConfigurationError",
    "DatasetProducer",
    "DirectZarrIngestor",
    "DuplicateIrregularCoordinateError",
    "EngineConfig",
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
    "MissingIrregularCoordinateError",
    "NoDiscoveredItemsError",
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
    "TemplateConfig",
    "TensogramTemplateConfig",
    "TensogramWriteStrategy",
    "WriteDomain",
    "WriteIntent",
    "WriteIntentRangeError",
    "ZarrArraySpec",
    "ZarrGroupSpec",
    "ZarrTemplateConfig",
    "chunk_align_ranges",
    "compute_covered_ranges",
    "config_keys",
    "discover_ingestors",
    "is_dataset_producer",
    "merge_batch_metrics",
    "register_ingestor",
    "validate_chunk_alignment",
    "validate_manifest_entries",
    "validate_slot_range",
    "verify_dim_compatibility",
    "warn_if_misaligned",
]


def __getattr__(name: str) -> Any:
    if name in {"GenericParquetIngestor", "GenericTensogramIngestor", "GenericZarrIngestor"}:
        from firecube.ingestor.templates.generic import (
            GenericParquetIngestor,
            GenericZarrIngestor,
        )

        GenericTensogramIngestor = import_module(
            "firecube.ingestor.templates.generic_tensogram"
        ).GenericTensogramIngestor

        return {
            "GenericParquetIngestor": GenericParquetIngestor,
            "GenericTensogramIngestor": GenericTensogramIngestor,
            "GenericZarrIngestor": GenericZarrIngestor,
        }[name]

    if name in {"DirectZarrIngestor", "WriteIntent", "ZarrArraySpec", "ZarrGroupSpec"}:
        from firecube.ingestor.templates.direct_zarr import (
            DirectZarrIngestor,
            WriteIntent,
            ZarrArraySpec,
            ZarrGroupSpec,
        )

        return {
            "DirectZarrIngestor": DirectZarrIngestor,
            "WriteIntent": WriteIntent,
            "ZarrArraySpec": ZarrArraySpec,
            "ZarrGroupSpec": ZarrGroupSpec,
        }[name]

    raise AttributeError(name)


def __dir__() -> list[str]:
    return sorted(
        [
            *globals().keys(),
            "DirectZarrIngestor",
            "GenericParquetIngestor",
            "GenericTensogramIngestor",
            "GenericZarrIngestor",
            "WriteIntent",
            "ZarrArraySpec",
            "ZarrGroupSpec",
        ]
    )
