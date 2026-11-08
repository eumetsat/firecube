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

"""Contract guard for the public ``firecube.core.api`` / ``firecube.ingestor.api`` surface.

Removed (must NOT be re-exported):

* ``firecube.core.api``: ``open_fsspec_url`` → ``firecube.core.filesystem.ops._open_fsspec_url``
  (adapter boundary), ``fs_kwargs_for_uri`` → ``firecube.core.filesystem``,
  ``RegionZarrWriter`` → ``firecube.core.zarr.region_writer``.
* ``firecube.ingestor.api``: ``CoverageTracker`` → ``firecube.ingestor.runtime.coverage``,
  ``ScratchManager`` → module deleted (was at ``firecube.ingestor.runtime.scratch``; see DONE.md for this plan's rationale).
"""

from __future__ import annotations

import contextlib
import importlib
import re
import warnings
from collections.abc import Iterator
from types import ModuleType

import pytest

pytestmark = pytest.mark.contract

_REMOVED_CORE_API: tuple[str, ...] = (
    "open_fsspec_url",
    "fs_kwargs_for_uri",
    "RegionZarrWriter",
)

_REMOVED_INGESTOR_API: tuple[str, ...] = (
    "CoverageTracker",
    "ScratchManager",
)


@pytest.mark.parametrize("name", _REMOVED_CORE_API)
def test_removed_core_api_name_raises_import_error(name: str) -> None:
    with _old_door_warning_ignored("firecube.core.api"):
        import firecube.core.api as core_api

        assert not hasattr(core_api, name), (
            f"`firecube.core.api.{name}` must not be exposed; the alias was removed."
        )
        assert name not in core_api.__all__, f"`firecube.core.api.__all__` must not list {name!r}."
        with pytest.raises(ImportError):
            exec(f"from firecube.core.api import {name}")


@pytest.mark.parametrize("name", _REMOVED_INGESTOR_API)
def test_removed_ingestor_api_name_raises_import_error(name: str) -> None:
    with _old_door_warning_ignored("firecube.ingestor.api"):
        import firecube.ingestor.api as ingestor_api

        assert not hasattr(ingestor_api, name), (
            f"`firecube.ingestor.api.{name}` must not be exposed; the alias was removed."
        )
        assert name not in ingestor_api.__all__, (
            f"`firecube.ingestor.api.__all__` must not list {name!r}."
        )
        with pytest.raises(ImportError):
            exec(f"from firecube.ingestor.api import {name}")


# Frozen on the tree before the deprecation shim: (name, defining module). Written out
# literally, never read from ``__all__`` at run time, so that a later trim of ``__all__``
# or a name dropped from the shim fails here. ``_ALIASED_DEFINITIONS`` holds the names
# whose definition carries another name.
def _pairs(table: str) -> tuple[tuple[str, str], ...]:
    """Expand lines of ``<defining module> <name> <name> ...`` into ``(name, module)`` pairs."""
    return tuple(
        (name, module)
        for module, *names in (line.split() for line in table.splitlines() if line.strip())
        for name in names
    )


_INGESTOR_API_NAMES = _pairs(
    """
firecube.core.controlplane SpanCoverage WriteDomain
firecube.core.controlplane.types ItemManifestEntry ResolvedIndexRecord validate_manifest_entries
firecube.core.errors DuplicateIrregularCoordinateError IndexedWriteCompilationError
firecube.core.errors MissingIrregularCoordinateError NoDiscoveredItemsError
firecube.core.index_resolve ExtentUnknownError ResolvedIndex resolve_index_spec
firecube.core.index_spec AUTO AxisSpec IndexSpec IntegerAxis IrregularTimeAxis ItemInfo
firecube.core.index_spec RegularTimeAxis TimeAxis
firecube.core.indexed_write IndexedWrite
firecube.core.intake CatalogGroupInfo
firecube.core.zarr._reserved_attrs FIRECUBE_STATIC_WRITTEN_ATTR RESERVED_ARRAY_ATTRS
firecube.core.zarr._reserved_attrs assert_attrs_safe
firecube.ingestor.config.engine EngineConfig config_keys
firecube.ingestor.contracts.interfaces DatasetProducer Ingestor PipelineHost SourceFile
firecube.ingestor.contracts.interfaces is_dataset_producer
firecube.ingestor.contracts.migration ABSENT AttributePatch MetadataPlan MigrationStep NodeView
firecube.ingestor.contracts.migration ReleaseRecord StoreView
firecube.ingestor.errors ConfigurationError IngestorError ManifestError RangeOverlapError
firecube.ingestor.errors ResumeConflictError SchemaDriftError SchemaSizeMismatchError StorageError
firecube.ingestor.errors UnboundedAxisError WriteIntentRangeError
firecube.ingestor.registry.loader discover_ingestors register_ingestor
firecube.ingestor.runtime.aggregation merge_batch_metrics
firecube.ingestor.runtime.base BaseIngestor LocalSourceFile
firecube.ingestor.runtime.tensogram.strategy TensogramWriteStrategy
firecube.ingestor.runtime.zarr.contracts AppendWriteStrategy RegionWriteStrategy
firecube.ingestor.runtime.zarr.existing_cube_check verify_dim_compatibility
firecube.ingestor.runtime.zarr.strategies.append AppendStrategy
firecube.ingestor.runtime.zarr.strategies.indexed_region IndexedRegionStrategy
firecube.ingestor.templates.config ParquetTemplateConfig TemplateConfig TensogramTemplateConfig
firecube.ingestor.templates.config ZarrTemplateConfig
firecube.ingestor.templates.direct_zarr DirectZarrIngestor WriteIntent ZarrArraySpec ZarrGroupSpec
firecube.ingestor.templates.generic GenericParquetIngestor GenericZarrIngestor
firecube.ingestor.templates.generic_tensogram GenericTensogramIngestor
firecube.ingestor.types.config PluginConfig
firecube.ingestor.types.context ExecutionParams IngestContext IngestResult OutputPaths
firecube.ingestor.types.context PipelineBatch PipelineResult PipelineRunState PluginContext
firecube.ingestor.types.context ResultMetrics RuntimeIngestContext StorageContext
firecube.ingestor.types.manifest IngestManifest
firecube.ingestor.types.planned_range PlannedRange SlotRange chunk_align_ranges
firecube.ingestor.types.planned_range compute_covered_ranges validate_chunk_alignment
firecube.ingestor.types.planned_range validate_slot_range warn_if_misaligned
firecube.ingestor.types.result_metrics PipelineMetrics StorageMetrics
    """
)

# Importable from ``firecube.ingestor.api`` but not listed in its ``__all__``.
_INGESTOR_API_UNLISTED_NAMES = (("canonical_coordinate_value", "firecube.core.index_spec"),)

_CORE_API_NAMES = _pairs(
    """
firecube.core.archive.tensogram._compat require_tensogram
firecube.core.batch_registry BatchResourceRegistry
firecube.core.config StorageConfig
firecube.core.controlplane RunInfo describe_control_plane
firecube.core.controlplane.types ItemManifestEntry ResolvedIndexRecord validate_manifest_entries
firecube.core.discovery DiscoveryRequest SourceItem discover discover_input_files
firecube.core.errors DuplicateIrregularCoordinateError IndexedWriteCompilationError
firecube.core.errors MissingIrregularCoordinateError NoDiscoveredItemsError
firecube.core.filesystem create_filesystem_for_uri delete_path ensure_directory path_stats
firecube.core.formats clean_netcdf_encoding extract_all_from_zips materialize_hdf5_path
firecube.core.formats normalize_string_vars prepare_netcdf_for_zarr read_hdf5_array rename_time_dim
firecube.core.index_resolve ExtentUnknownError ResolvedIndex _compute_group_identity_hash
firecube.core.index_resolve coerce_to_epoch_s compute_group_identity_hash resolve_index_spec
firecube.core.index_spec AUTO AxisSpec IndexSpec IntegerAxis IrregularTimeAxis ItemInfo
firecube.core.index_spec RegularTimeAxis TimeAxis
firecube.core.indexed_write IndexedWrite
firecube.core.intake CatalogGroupInfo
firecube.core.product ensure_product_uri resolve_dataset_target
firecube.core.slot_index SlotAxis SlotIndexModel epoch_s_to_iso iso_to_epoch_s normalize_epoch_iso
firecube.core.uris infer_target_protocol is_remote_target local_path_from_target parse_uri
firecube.core.zarr._coord_lifecycle assert_coord_markers_consistent
firecube.core.zarr._reserved_attrs FIRECUBE_GROUP_IDENTITY_HASH_ATTR FIRECUBE_STATIC_WRITTEN_ATTR
firecube.core.zarr._reserved_attrs RESERVED_ARRAY_ATTRS assert_attrs_safe
firecube.core.zarr._sealing_markers ATTR_CONSOLIDATED_AT ATTR_COORD_MANAGED ATTR_PREALLOCATED
firecube.core.zarr.guard.chunk_geometry axis_selection_is_chunk_aligned chunk_axis_range
firecube.core.zarr.guard.chunk_geometry physical_chunk_keys_for_region
firecube.core.zarr.region_writer RegionZarrWriterProtocol
firecube.core.zarr.time_decode decode_time_array encode_time_array
firecube.core.zarr.validation ZarrCompareReport compare_zarr_stores read_chunk_grid_with_shards
    """
)

# Names whose definition in the defining module carries a different name.
_ALIASED_DEFINITIONS: dict[tuple[str, str], str] = {
    ("firecube.ingestor.api", "canonical_coordinate_value"): "_canonical_coordinate_value",
    ("firecube.core.api", "compute_group_identity_hash"): "_compute_group_identity_hash",
}


@contextlib.contextmanager
def _old_door_warning_ignored(door: str) -> Iterator[None]:
    """Ignore only the deprecation message of ``door``; any other warning still surfaces."""
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=(
                re.escape(
                    f"{door} is deprecated and will be removed in v0.2.0. "
                    "Import from firecube.sdk instead."
                )
                + "(?:"
                + re.escape(" firecube.sdk does not carry ")
                + r"\S+"
                + re.escape("; open an issue if your plugin needs it.")
                + ")?$"
            ),
            category=DeprecationWarning,
        )
        yield


def _import_from_old_door(door: str, name: str) -> tuple[ModuleType, object]:
    """Return ``(module, imported object)`` for ``from <door> import <name>``."""
    with _old_door_warning_ignored(door):
        module = __import__(door, fromlist=[name])
        return module, getattr(module, name)


@pytest.mark.parametrize(
    ("name", "defining_module"),
    _INGESTOR_API_NAMES + _INGESTOR_API_UNLISTED_NAMES,
    ids=[n for n, _ in _INGESTOR_API_NAMES + _INGESTOR_API_UNLISTED_NAMES],
)
def test_ingestor_api_name_stays_importable_as_its_definition(
    name: str, defining_module: str
) -> None:
    door = "firecube.ingestor.api"
    module, imported = _import_from_old_door(door, name)
    attr = _ALIASED_DEFINITIONS.get((door, name), name)

    assert imported is getattr(importlib.import_module(defining_module), attr), (
        f"`from {door} import {name}` must return the object defined in {defining_module}."
    )
    if (name, defining_module) not in _INGESTOR_API_UNLISTED_NAMES:
        assert name in module.__all__, f"`{door}.__all__` must list {name!r}."


@pytest.mark.parametrize(
    ("name", "defining_module"), _CORE_API_NAMES, ids=[n for n, _ in _CORE_API_NAMES]
)
def test_core_api_name_stays_importable_as_its_definition(name: str, defining_module: str) -> None:
    door = "firecube.core.api"
    module, imported = _import_from_old_door(door, name)
    attr = _ALIASED_DEFINITIONS.get((door, name), name)

    assert imported is getattr(importlib.import_module(defining_module), attr), (
        f"`from {door} import {name}` must return the object defined in {defining_module}."
    )
    assert name in module.__all__, f"`{door}.__all__` must list {name!r}."
