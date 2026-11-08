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

"""Declare the names of the deprecated `firecube.ingestor.api` module for type checkers."""

from firecube.core.controlplane import SpanCoverage as SpanCoverage
from firecube.core.controlplane import WriteDomain as WriteDomain
from firecube.core.controlplane.types import ItemManifestEntry as ItemManifestEntry
from firecube.core.controlplane.types import ResolvedIndexRecord as ResolvedIndexRecord
from firecube.core.errors import (
    DuplicateIrregularCoordinateError as DuplicateIrregularCoordinateError,
)
from firecube.core.errors import IndexedWriteCompilationError as IndexedWriteCompilationError
from firecube.core.errors import MissingIrregularCoordinateError as MissingIrregularCoordinateError
from firecube.core.errors import NoDiscoveredItemsError as NoDiscoveredItemsError
from firecube.core.index_resolve import ExtentUnknownError as ExtentUnknownError
from firecube.core.index_resolve import ResolvedIndex as ResolvedIndex
from firecube.core.index_spec import AUTO as AUTO
from firecube.core.index_spec import AxisSpec as AxisSpec
from firecube.core.index_spec import IndexSpec as IndexSpec
from firecube.core.index_spec import IntegerAxis as IntegerAxis
from firecube.core.index_spec import IrregularTimeAxis as IrregularTimeAxis
from firecube.core.index_spec import ItemInfo as ItemInfo
from firecube.core.index_spec import RegularTimeAxis as RegularTimeAxis
from firecube.core.index_spec import TimeAxis as TimeAxis
from firecube.core.indexed_write import IndexedWrite as IndexedWrite
from firecube.core.intake import CatalogGroupInfo as CatalogGroupInfo
from firecube.core.zarr._reserved_attrs import (
    FIRECUBE_STATIC_WRITTEN_ATTR as FIRECUBE_STATIC_WRITTEN_ATTR,
)
from firecube.core.zarr._reserved_attrs import RESERVED_ARRAY_ATTRS as RESERVED_ARRAY_ATTRS
from firecube.core.zarr.migration import ABSENT as ABSENT
from firecube.core.zarr.migration import AttributePatch as AttributePatch
from firecube.core.zarr.migration import MetadataPlan as MetadataPlan
from firecube.core.zarr.migration import MigrationStep as MigrationStep
from firecube.core.zarr.migration import NodeView as NodeView
from firecube.core.zarr.migration import ReleaseRecord as ReleaseRecord
from firecube.core.zarr.migration import StoreView as StoreView
from firecube.ingestor._api_deprecated import assert_attrs_safe as assert_attrs_safe
from firecube.ingestor._api_deprecated import (
    canonical_coordinate_value as canonical_coordinate_value,
)
from firecube.ingestor._api_deprecated import chunk_align_ranges as chunk_align_ranges
from firecube.ingestor._api_deprecated import compute_covered_ranges as compute_covered_ranges
from firecube.ingestor._api_deprecated import config_keys as config_keys
from firecube.ingestor._api_deprecated import discover_ingestors as discover_ingestors
from firecube.ingestor._api_deprecated import is_dataset_producer as is_dataset_producer
from firecube.ingestor._api_deprecated import merge_batch_metrics as merge_batch_metrics
from firecube.ingestor._api_deprecated import register_ingestor as register_ingestor
from firecube.ingestor._api_deprecated import resolve_index_spec as resolve_index_spec
from firecube.ingestor._api_deprecated import validate_chunk_alignment as validate_chunk_alignment
from firecube.ingestor._api_deprecated import validate_manifest_entries as validate_manifest_entries
from firecube.ingestor._api_deprecated import validate_slot_range as validate_slot_range
from firecube.ingestor._api_deprecated import verify_dim_compatibility as verify_dim_compatibility
from firecube.ingestor._api_deprecated import warn_if_misaligned as warn_if_misaligned
from firecube.ingestor.config.engine import EngineConfig as EngineConfig
from firecube.ingestor.contracts.interfaces import DatasetProducer as DatasetProducer
from firecube.ingestor.contracts.interfaces import Ingestor as Ingestor
from firecube.ingestor.contracts.interfaces import PipelineHost as PipelineHost
from firecube.ingestor.contracts.interfaces import SourceFile as SourceFile
from firecube.ingestor.errors import ConfigurationError as ConfigurationError
from firecube.ingestor.errors import IngestorError as IngestorError
from firecube.ingestor.errors import ManifestError as ManifestError
from firecube.ingestor.errors import RangeOverlapError as RangeOverlapError
from firecube.ingestor.errors import ResumeConflictError as ResumeConflictError
from firecube.ingestor.errors import SchemaDriftError as SchemaDriftError
from firecube.ingestor.errors import SchemaSizeMismatchError as SchemaSizeMismatchError
from firecube.ingestor.errors import StorageError as StorageError
from firecube.ingestor.errors import UnboundedAxisError as UnboundedAxisError
from firecube.ingestor.errors import WriteIntentRangeError as WriteIntentRangeError
from firecube.ingestor.runtime.base import BaseIngestor as BaseIngestor
from firecube.ingestor.runtime.base import LocalSourceFile as LocalSourceFile
from firecube.ingestor.runtime.tensogram.strategy import (
    TensogramWriteStrategy as TensogramWriteStrategy,
)
from firecube.ingestor.runtime.zarr.contracts import AppendWriteStrategy as AppendWriteStrategy
from firecube.ingestor.runtime.zarr.contracts import RegionWriteStrategy as RegionWriteStrategy
from firecube.ingestor.runtime.zarr.strategies.append import AppendStrategy as AppendStrategy
from firecube.ingestor.runtime.zarr.strategies.indexed_region import (
    IndexedRegionStrategy as IndexedRegionStrategy,
)
from firecube.ingestor.templates.config import ParquetTemplateConfig as ParquetTemplateConfig
from firecube.ingestor.templates.config import TemplateConfig as TemplateConfig
from firecube.ingestor.templates.config import TensogramTemplateConfig as TensogramTemplateConfig
from firecube.ingestor.templates.config import ZarrTemplateConfig as ZarrTemplateConfig
from firecube.ingestor.templates.direct_zarr import DirectZarrIngestor as DirectZarrIngestor
from firecube.ingestor.templates.direct_zarr import WriteIntent as WriteIntent
from firecube.ingestor.templates.direct_zarr import ZarrArraySpec as ZarrArraySpec
from firecube.ingestor.templates.direct_zarr import ZarrGroupSpec as ZarrGroupSpec
from firecube.ingestor.templates.generic import GenericParquetIngestor as GenericParquetIngestor
from firecube.ingestor.templates.generic import GenericZarrIngestor as GenericZarrIngestor
from firecube.ingestor.templates.generic_tensogram import (
    GenericTensogramIngestor as GenericTensogramIngestor,
)
from firecube.ingestor.types.config import PluginConfig as PluginConfig
from firecube.ingestor.types.context import ExecutionParams as ExecutionParams
from firecube.ingestor.types.context import IngestContext as IngestContext
from firecube.ingestor.types.context import IngestResult as IngestResult
from firecube.ingestor.types.context import OutputPaths as OutputPaths
from firecube.ingestor.types.context import PipelineBatch as PipelineBatch
from firecube.ingestor.types.context import PipelineResult as PipelineResult
from firecube.ingestor.types.context import PipelineRunState as PipelineRunState
from firecube.ingestor.types.context import PluginContext as PluginContext
from firecube.ingestor.types.context import ResultMetrics as ResultMetrics
from firecube.ingestor.types.context import RuntimeIngestContext as RuntimeIngestContext
from firecube.ingestor.types.context import StorageContext as StorageContext
from firecube.ingestor.types.manifest import IngestManifest as IngestManifest
from firecube.ingestor.types.planned_range import PlannedRange as PlannedRange
from firecube.ingestor.types.planned_range import SlotRange as SlotRange
from firecube.ingestor.types.result_metrics import PipelineMetrics as PipelineMetrics
from firecube.ingestor.types.result_metrics import StorageMetrics as StorageMetrics

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
