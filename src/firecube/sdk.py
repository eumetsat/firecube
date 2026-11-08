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

"""Import door for Firecube plugin authors.

Import everything a plugin needs from this module. `firecube.operations` is the door for tooling.
Each name keeps the name and docstring of its defining module. The template classes load on
first access.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

from firecube.core.batch_registry import BatchResourceRegistry
from firecube.core.controlplane.types import ItemManifestEntry, ResolvedIndexRecord
from firecube.core.discovery import DiscoveryRequest, SourceItem, discover, discover_input_files
from firecube.core.errors import (
    ConfigurationError,
    DuplicateIrregularCoordinateError,
    IndexedWriteCompilationError,
    ManifestError,
    MissingIrregularCoordinateError,
    NoDiscoveredItemsError,
    SchemaDriftError,
    StorageError,
)
from firecube.core.filesystem.ops import (
    create_filesystem_for_uri,
    delete_path,
    ensure_directory,
    path_stats,
)
from firecube.core.formats.hdf5 import materialize_hdf5_path, read_hdf5_array
from firecube.core.formats.netcdf import (
    clean_netcdf_encoding,
    normalize_string_vars,
    prepare_netcdf_for_zarr,
    rename_time_dim,
)
from firecube.core.formats.zip import extract_all_from_zips
from firecube.core.index_resolve import (
    ExtentUnknownError,
    ResolvedIndex,
    coerce_to_epoch_s,
    resolve_index_spec,
)
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
from firecube.core.indexed_write import IndexedWrite
from firecube.core.intake import CatalogGroupInfo
from firecube.core.slot_index import epoch_s_to_iso, iso_to_epoch_s, normalize_epoch_iso
from firecube.core.uris import (
    infer_target_protocol,
    is_remote_target,
    local_path_from_target,
    parse_uri,
)
from firecube.core.zarr._reserved_attrs import (
    FIRECUBE_STATIC_WRITTEN_ATTR,
    RESERVED_ARRAY_ATTRS,
    assert_attrs_safe,
)
from firecube.core.zarr.guard.chunk_geometry import (
    axis_selection_is_chunk_aligned,
    chunk_axis_range,
    physical_chunk_keys_for_region,
)
from firecube.core.zarr.metadata_documents import ABSENT, NodeView
from firecube.core.zarr.time_decode import decode_time_array
from firecube.core.zarr.validation import (
    ZarrCompareReport,
    compare_zarr_stores,
    read_chunk_grid_with_shards,
)
from firecube.ingestor.config.engine import EngineConfig
from firecube.ingestor.contracts.interfaces import Ingestor, SourceFile
from firecube.ingestor.contracts.migration import (
    AttributePatch,
    MetadataPlan,
    MigrationStep,
    ReleaseRecord,
    StoreView,
)
from firecube.ingestor.errors import (
    IngestorError,
    RangeOverlapError,
    ResumeConflictError,
    SchemaSizeMismatchError,
    UnboundedAxisError,
    WriteIntentRangeError,
)
from firecube.ingestor.registry.loader import discover_ingestors, register_ingestor
from firecube.ingestor.runtime.base import BaseIngestor
from firecube.ingestor.runtime.zarr.existing_cube_check import verify_dim_compatibility
from firecube.ingestor.templates.config import (
    ParquetTemplateConfig,
    TemplateConfig,
    TensogramTemplateConfig,
    ZarrTemplateConfig,
)
from firecube.ingestor.types.config import PluginConfig
from firecube.ingestor.types.context import (
    ExecutionParams,
    IngestContext,
    IngestResult,
    PipelineBatch,
    PipelineResult,
    PipelineRunState,
    PluginContext,
    RuntimeIngestContext,
    StorageContext,
)
from firecube.ingestor.types.result_metrics import (
    OutputPaths,
    PipelineMetrics,
    ResultMetrics,
    StorageMetrics,
)

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
    "ABSENT",
    "AUTO",
    "FIRECUBE_STATIC_WRITTEN_ATTR",
    "RESERVED_ARRAY_ATTRS",
    "AttributePatch",
    "AxisSpec",
    "BaseIngestor",
    "BatchResourceRegistry",
    "CatalogGroupInfo",
    "ConfigurationError",
    "DirectZarrIngestor",
    "DiscoveryRequest",
    "DuplicateIrregularCoordinateError",
    "EngineConfig",
    "ExecutionParams",
    "ExtentUnknownError",
    "GenericParquetIngestor",
    "GenericTensogramIngestor",
    "GenericZarrIngestor",
    "IndexSpec",
    "IndexedWrite",
    "IndexedWriteCompilationError",
    "IngestContext",
    "IngestResult",
    "Ingestor",
    "IngestorError",
    "IntegerAxis",
    "IrregularTimeAxis",
    "ItemInfo",
    "ItemManifestEntry",
    "ManifestError",
    "MetadataPlan",
    "MigrationStep",
    "MissingIrregularCoordinateError",
    "NoDiscoveredItemsError",
    "NodeView",
    "OutputPaths",
    "ParquetTemplateConfig",
    "PipelineBatch",
    "PipelineMetrics",
    "PipelineResult",
    "PipelineRunState",
    "PluginConfig",
    "PluginContext",
    "RangeOverlapError",
    "RegularTimeAxis",
    "ReleaseRecord",
    "ResolvedIndex",
    "ResolvedIndexRecord",
    "ResultMetrics",
    "ResumeConflictError",
    "RuntimeIngestContext",
    "SchemaDriftError",
    "SchemaSizeMismatchError",
    "SourceFile",
    "SourceItem",
    "StorageContext",
    "StorageError",
    "StorageMetrics",
    "StoreView",
    "TemplateConfig",
    "TensogramTemplateConfig",
    "TimeAxis",
    "UnboundedAxisError",
    "WriteIntent",
    "WriteIntentRangeError",
    "ZarrArraySpec",
    "ZarrCompareReport",
    "ZarrGroupSpec",
    "ZarrTemplateConfig",
    "assert_attrs_safe",
    "axis_selection_is_chunk_aligned",
    "chunk_axis_range",
    "clean_netcdf_encoding",
    "coerce_to_epoch_s",
    "compare_zarr_stores",
    "create_filesystem_for_uri",
    "decode_time_array",
    "delete_path",
    "discover",
    "discover_ingestors",
    "discover_input_files",
    "ensure_directory",
    "epoch_s_to_iso",
    "extract_all_from_zips",
    "infer_target_protocol",
    "is_remote_target",
    "iso_to_epoch_s",
    "local_path_from_target",
    "materialize_hdf5_path",
    "normalize_epoch_iso",
    "normalize_string_vars",
    "parse_uri",
    "path_stats",
    "physical_chunk_keys_for_region",
    "prepare_netcdf_for_zarr",
    "read_chunk_grid_with_shards",
    "read_hdf5_array",
    "register_ingestor",
    "rename_time_dim",
    "resolve_index_spec",
    "verify_dim_compatibility",
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
