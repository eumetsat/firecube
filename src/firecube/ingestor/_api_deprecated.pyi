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

"""Declare the deprecated functions of `firecube.ingestor.api` for type checkers; no runtime module exists."""

import logging
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from typing_extensions import deprecated

from firecube.core.controlplane import SpanCoverage
from firecube.core.controlplane.types import ItemManifestEntry
from firecube.core.index_resolve import ResolvedIndex
from firecube.core.index_spec import IndexSpec
from firecube.ingestor.contracts.interfaces import Ingestor
from firecube.ingestor.registry.loader import _IngestorT
from firecube.ingestor.types.context import PipelineRunState, RuntimeIngestContext
from firecube.ingestor.types.planned_range import PlannedRange

@deprecated(
    "firecube.ingestor.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead."
)
def assert_attrs_safe(attrs: Mapping[str, Any]) -> None: ...
@deprecated(
    "firecube.ingestor.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead. firecube.sdk does not carry chunk_align_ranges; open an issue if your plugin needs it."
)
def chunk_align_ranges(ranges: list[PlannedRange], chunk_size: int) -> list[PlannedRange]: ...
@deprecated(
    "firecube.ingestor.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead. firecube.sdk does not carry compute_covered_ranges; open an issue if your plugin needs it."
)
def compute_covered_ranges(coverage: list[SpanCoverage]) -> list[PlannedRange]: ...
@deprecated(
    "firecube.ingestor.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead. firecube.sdk does not carry config_keys; open an issue if your plugin needs it."
)
def config_keys(cls: type) -> set[str]: ...
@deprecated(
    "firecube.ingestor.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead."
)
def discover_ingestors() -> dict[str, type[Ingestor]]: ...
@deprecated(
    "firecube.ingestor.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead. firecube.sdk does not carry is_dataset_producer; open an issue if your plugin needs it."
)
def is_dataset_producer(cls: type) -> bool: ...
@deprecated(
    "firecube.ingestor.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead. firecube.sdk does not carry merge_batch_metrics; open an issue if your plugin needs it."
)
def merge_batch_metrics(ctx: RuntimeIngestContext, state: PipelineRunState) -> dict[str, Any]: ...
@deprecated(
    "firecube.ingestor.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead."
)
def register_ingestor(name: str) -> Callable[[type[_IngestorT]], type[_IngestorT]]: ...
@deprecated(
    "firecube.ingestor.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead."
)
def resolve_index_spec(
    spec: IndexSpec, *, time_dim_name: str, items: Sequence[ItemManifestEntry] | None = None
) -> ResolvedIndex: ...
@deprecated(
    "firecube.ingestor.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead. firecube.sdk does not carry validate_chunk_alignment; open an issue if your plugin needs it."
)
def validate_chunk_alignment(
    slot_start: int,
    slot_end: int,
    chunk_shapes_per_group: dict[str, list[tuple[int, ...]]],
    global_expected: dict[str, int] | None = None,
    shards_per_group: dict[str, list[tuple[int, ...]]] | None = None,
) -> None: ...
@deprecated(
    "firecube.ingestor.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead. firecube.sdk does not carry validate_manifest_entries; open an issue if your plugin needs it."
)
def validate_manifest_entries(entries: list[ItemManifestEntry]) -> None: ...
@deprecated(
    "firecube.ingestor.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead. firecube.sdk does not carry validate_slot_range; open an issue if your plugin needs it."
)
def validate_slot_range(slot_start: int, slot_end: int) -> None: ...
@deprecated(
    "firecube.ingestor.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead."
)
def verify_dim_compatibility(
    target_uri: str, declared_dim: str, group_paths: Sequence[str], storage_config: Any
) -> None: ...
@deprecated(
    "firecube.ingestor.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead. firecube.sdk does not carry warn_if_misaligned; open an issue if your plugin needs it."
)
def warn_if_misaligned(
    slot_start: int,
    slot_end: int,
    chunk_shapes_per_group: dict[str, list[tuple[int, ...]]],
    logger: logging.Logger,
    global_expected: dict[str, int] | None = None,
    shards_per_group: dict[str, list[tuple[int, ...]]] | None = None,
) -> None: ...
@deprecated(
    "firecube.ingestor.api is deprecated and will be removed in v0.2.0. Import from firecube.sdk instead. firecube.sdk does not carry canonical_coordinate_value; open an issue if your plugin needs it."
)
def canonical_coordinate_value(value: Any) -> Any: ...
