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

"""Template-specific configuration for Firecube ingestors.

These configurations control the output format logic (Zarr chunks, Parquet partitions)
and are owned by the `Generic[Type]Ingestor` templates.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TypeVar, get_type_hints

from firecube.ingestor.config.engine import config_keys

T = TypeVar("T", bound="TemplateConfig")

ZARR_MULTI_RES_REMOVED_MESSAGE = (
    "zarr_multi_res during ingest has been removed; "
    "run 'firecube zarr multires <target>' after ingestion instead."
)


@dataclass
class TemplateConfig:
    """Base for template configurations."""

    @classmethod
    def from_options(cls: type[T], options: dict[str, Any]) -> T:
        from firecube.ingestor.config.coercion import coerce_cli_value

        if cls is ZarrTemplateConfig and "zarr_multi_res" in options:
            raise ValueError(ZARR_MULTI_RES_REMOVED_MESSAGE)

        known = config_keys(cls)
        unknown = set(options.keys()) - known
        if unknown:
            raise ValueError(
                f"Unknown Template options: {', '.join(sorted(unknown))}. "
                f"Valid keys: {', '.join(sorted(known))}"
            )

        type_hints = get_type_hints(cls)
        init_kwargs = {}
        for key, value in options.items():
            target_type = type_hints.get(key)
            init_kwargs[key] = coerce_cli_value(value, target_type, key)
        return cls(**init_kwargs)


@dataclass
class ZarrTemplateConfig(TemplateConfig):
    """Configuration for GenericZarrIngestor.

    Attributes:
        zarr_chunk_shape: Optional per-dimension inner chunk sizes.
        zarr_sharding: Enable Zarr v3 sharding.
        zarr_shard_shape: Optional per-dimension shard sizes.
        zarr_compression: Compression setting. ``False`` disables compression,
            ``True`` selects the default codec, and a string selects a codec name.
        zarr_consolidate: Consolidate Zarr metadata after writes.
        zarr_time_encoding: Optional time encoding override.
        zarr_async_concurrency: Async write concurrency used by Zarr.
        dask_scheduler: Optional Dask scheduler override.
        dask_write_threads: Optional write-thread count for Dask-backed writes.
    """

    zarr_chunk_shape: dict[str, int] | None = None
    zarr_sharding: bool = False
    zarr_shard_shape: dict[str, int] | None = None
    zarr_compression: bool | str = False
    zarr_consolidate: bool = False
    zarr_time_encoding: str | None = None
    zarr_async_concurrency: int = 10
    dask_scheduler: str | None = None
    dask_write_threads: int = 0


@dataclass
class ParquetTemplateConfig(TemplateConfig):
    """Configuration for GenericParquetIngestor.

    Attributes:
        parquet_partition_by: Optional Hive-style partition columns.
        parquet_row_group_size: Optional rows per Parquet row group.
    """

    parquet_partition_by: list[str] | None = None
    parquet_row_group_size: int | None = None


@dataclass
class TensogramTemplateConfig(TemplateConfig):
    """Configuration for GenericTensogramIngestor.

    Attributes:
        tensogram_compression: Compression codec for Tensogram output.
        tensogram_message_granularity: Message grouping strategy.
        tensogram_allow_nan: Permit NaN values in archive output.
        tensogram_allow_inf: Permit infinite values in archive output.
    """

    tensogram_compression: str = "zstd"
    tensogram_message_granularity: str = "per_variable"
    tensogram_allow_nan: bool = True
    tensogram_allow_inf: bool = True
