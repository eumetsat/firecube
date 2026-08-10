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

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, TypeVar, cast, get_type_hints

from firecube.ingestor.config.engine import config_keys

T = TypeVar("T", bound="TemplateConfig")

ZARR_MULTI_RES_REMOVED_MESSAGE = (
    "zarr_multi_res during ingest has been removed; "
    "run 'firecube zarr multires <target>' after ingestion instead."
)


def _validate_zarr_codecs(codecs: list[dict] | None) -> None:
    if codecs is None:
        return
    if not isinstance(codecs, list):
        raise ValueError("zarr_codecs must be a list of codec entries")
    if len(codecs) == 0:
        raise ValueError("zarr_codecs must contain at least one codec entry")

    allowed_keys = {"name", "configuration"}
    for index, entry in enumerate(codecs):
        if not isinstance(entry, dict):
            raise ValueError(
                f"zarr_codecs[{index}] must be an object with keys {{'name', 'configuration'}}"
            )
        extra = set(entry.keys()) - allowed_keys
        if extra:
            raise ValueError(
                f"zarr_codecs[{index}] has unexpected keys: {extra!r}; allowed keys: {allowed_keys!r}"
            )
        if "name" not in entry:
            raise ValueError(f"zarr_codecs[{index}].name is required")
        if not isinstance(entry["name"], str):
            raise ValueError(
                f"zarr_codecs[{index}].name must be a string, got {type(entry['name']).__name__}"
            )
        if "configuration" in entry and not isinstance(entry["configuration"], dict):
            raise ValueError(f"zarr_codecs[{index}].configuration must be an object when present")

    from firecube.core.zarr.codec_pipeline import split_zarr_codecs

    normalized_codecs = [
        {**entry, "configuration": entry.get("configuration", {})} for entry in codecs
    ]
    split_zarr_codecs(normalized_codecs)

    if len(codecs) == 1:
        return

    from zarr.core.codec_pipeline import codecs_from_list
    from zarr.registry import get_codec_class

    resolved_codecs = []
    for entry in normalized_codecs:
        codec_class = get_codec_class(cast(str, entry["name"]))
        resolved_codecs.append(codec_class.from_dict(entry))

    try:
        codecs_from_list(resolved_codecs)
    except Exception as exc:
        raise ValueError(
            "zarr_codecs entries must form a valid Zarr codec pipeline in order "
            f"(serializer before compressors, filters before serializer): {exc}"
        ) from exc


def validate_zarr_specs_against_template(
    specs: Sequence[Any],
    template: ZarrTemplateConfig,
) -> None:
    """Validate per-array codec declarations against the template compression setting.

    ``specs`` is typed ``Sequence[Any]`` (not ``Sequence[ZarrArraySpec]``) to
    avoid a circular import with ``direct_zarr``; codec fields are read via
    ``getattr`` with ``None`` defaults.

    Raises
    ------
    ValueError
        If any spec declares codec fields while ``template.zarr_compression`` is False.
    """
    if template.zarr_compression:
        return
    for spec in specs:
        declared_fields = []
        if getattr(spec, "filters", None) is not None:
            declared_fields.append("filters")
        if getattr(spec, "serializer", None) is not None:
            declared_fields.append("serializer")
        if getattr(spec, "compressors", None) is not None:
            declared_fields.append("compressors")
        if declared_fields:
            raise ValueError(
                f"ZarrArraySpec {spec.name!r} declares codec fields "
                f"({', '.join(declared_fields)}) but ZarrTemplateConfig.zarr_compression=False. "
                "Either set zarr_compression=True and declare per-array codecs, "
                "or set zarr_compression=False and remove per-array codec fields."
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
        return cast(T, cls(**init_kwargs))


@dataclass
class ZarrTemplateConfig(TemplateConfig):
    """Configuration for GenericZarrIngestor.

    Attributes:
        zarr_chunk_shape: Optional per-dimension inner chunk sizes.
        zarr_sharding: Enable Zarr v3 sharding.
        zarr_shard_shape: Optional per-dimension shard sizes.
        zarr_compression: ``True`` (default) delegates to zarr's default codec
            pipeline (``ZstdCodec(level=0)`` in zarr-python v3, matching the
            upstream default and pre-PR-25 firecube effective behavior).
            ``False`` explicitly disables compression. String values are
            rejected at construction time; only bool values are accepted.
        zarr_codecs: Optional flat list of codec entries in Zarr v3 metadata
            format ``[{"name": str, "configuration": dict}]``. Requires
            ``zarr_compression=True``. When set, the declared pipeline is used
            instead of zarr's default. Structural validation happens here;
            codec-specific resolution happens later.
        zarr_consolidate: Consolidate Zarr metadata after writes.
        zarr_time_encoding: Optional time encoding override.
        zarr_async_concurrency: Async write concurrency used by Zarr.
        dask_scheduler: Optional Dask scheduler override.
        dask_write_threads: Optional write-thread count for Dask-backed writes.
    """

    zarr_chunk_shape: dict[str, int] | None = None
    zarr_sharding: bool = False
    zarr_shard_shape: dict[str, int] | None = None
    zarr_compression: bool = True
    zarr_codecs: list[dict] | None = None
    zarr_consolidate: bool = False
    zarr_time_encoding: str | None = None
    zarr_async_concurrency: int = 10
    dask_scheduler: str | None = None
    dask_write_threads: int = 0

    def __post_init__(self) -> None:
        if type(self.zarr_compression) is not bool:
            raise ValueError(
                "zarr_compression must be bool, got "
                f"{type(self.zarr_compression).__name__}: {self.zarr_compression!r}"
            )

        if not self.zarr_compression and self.zarr_codecs is not None:
            raise ValueError(
                f"zarr_compression=False conflicts with zarr_codecs={self.zarr_codecs!r}: "
                "specifying a codec requires compression to be enabled.\n"
                "Either enable compression and keep the codec:\n"
                "  zarr_compression = true\n"
                '  zarr_codecs = [{"name": "...", "configuration": {...}}]\n'
                "Or remove zarr_codecs for uncompressed output."
            )

        _validate_zarr_codecs(self.zarr_codecs)


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
