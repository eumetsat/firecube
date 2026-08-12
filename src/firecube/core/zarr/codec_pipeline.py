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

"""Shared normalization and classification helpers for Zarr codec pipelines."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

from zarr.abc.codec import ArrayArrayCodec, ArrayBytesCodec, BytesBytesCodec
from zarr.registry import get_codec_class

__all__ = [
    "CodecPipeline",
    "compare_pipelines",
    "normalize_codec_dict",
    "normalize_pipeline",
    "split_zarr_codecs",
]


@dataclass(frozen=True)
class CodecPipeline:
    """Split Zarr codec pipeline declaration."""

    filters: tuple[dict, ...] | None
    serializer: dict | None
    compressors: tuple[dict, ...] | None


def normalize_codec_dict(entry: dict) -> dict:
    """Return a canonical copy of a Zarr codec dictionary.

    Args:
        entry: Codec dictionary with at least a ``name`` key and optionally a
            ``configuration`` mapping.

    Returns:
        A non-mutating copy with top-level keys sorted, a present
        ``configuration`` key, and sorted configuration keys.
    """
    normalized: dict[str, Any] = dict(entry)
    configuration = normalized.get("configuration", {}) or {}
    normalized["configuration"] = dict(sorted(cast(dict[str, Any], configuration).items()))
    return dict(sorted(normalized.items()))


def normalize_pipeline(pipeline: CodecPipeline) -> CodecPipeline:
    """Normalize every declared codec dictionary in *pipeline*."""
    return CodecPipeline(
        filters=(
            tuple(normalize_codec_dict(entry) for entry in pipeline.filters)
            if pipeline.filters is not None
            else None
        ),
        serializer=(
            normalize_codec_dict(pipeline.serializer) if pipeline.serializer is not None else None
        ),
        compressors=(
            tuple(normalize_codec_dict(entry) for entry in pipeline.compressors)
            if pipeline.compressors is not None
            else None
        ),
    )


def compare_pipelines(
    declared: CodecPipeline,
    on_disk_codecs: tuple,
) -> list[tuple[str, dict | None, dict | None]]:
    """Compare a declared codec pipeline with raw on-disk Zarr codecs.

    Args:
        declared: Split codec declaration from the plugin/schema layer.
        on_disk_codecs: Raw tuple of codec instances from ``arr.metadata.codecs``.

    Returns:
        Field-level mismatches as ``(field, declared_value, on_disk_value)``.
        A fully undeclared pipeline never reports drift.
    """
    if declared == CodecPipeline(None, None, None):
        return []

    normalized_declared = normalize_pipeline(declared)
    on_disk = _pipeline_from_zarr_codecs(on_disk_codecs)
    on_disk_for_compare = _project_on_disk_to_declared(normalized_declared, on_disk)
    mismatches: list[tuple[str, dict | None, dict | None]] = []

    if normalized_declared.filters != on_disk_for_compare.filters:
        mismatches.append(
            (
                "filters",
                cast(dict | None, normalized_declared.filters),
                cast(dict | None, on_disk_for_compare.filters),
            )
        )

    if (
        normalized_declared.serializer is not None
        and normalized_declared.serializer != on_disk_for_compare.serializer
    ):
        mismatches.append(
            ("serializer", normalized_declared.serializer, on_disk_for_compare.serializer)
        )

    if normalized_declared.compressors != on_disk_for_compare.compressors:
        mismatches.append(
            (
                "compressors",
                cast(dict | None, normalized_declared.compressors),
                cast(dict | None, on_disk_for_compare.compressors),
            )
        )

    return mismatches


def split_zarr_codecs(
    codecs: list[dict] | None,
) -> tuple[list[dict] | None, dict | None, list[dict] | None]:
    """Split a flat Zarr codec list into filters, serializer, and compressors.

    Args:
        codecs: Flat codec dictionaries as accepted by zarr-python.

    Returns:
        Original dictionaries grouped by codec ABC classification.

    Raises:
        ValueError: If a codec name is unknown or more than one serializer is
            present.
    """
    if codecs is None:
        return None, None, None

    filters: list[dict] = []
    serializer: dict | None = None
    compressors: list[dict] = []

    for entry in codecs:
        name = cast(str, entry["name"])
        try:
            codec_class = get_codec_class(name)
        except KeyError as orig:
            raise ValueError(
                f"Codec {name!r} is not registered in zarr's codec registry."
            ) from orig

        codec = codec_class.from_dict(entry)
        if isinstance(codec, ArrayArrayCodec):
            filters.append(entry)
        elif isinstance(codec, ArrayBytesCodec):
            if serializer is not None:
                raise ValueError("Multiple ArrayBytesCodec serializers are not supported.")
            serializer = entry
        elif isinstance(codec, BytesBytesCodec):
            compressors.append(entry)

    return filters or None, serializer, compressors or None


def _pipeline_from_zarr_codecs(on_disk_codecs: tuple) -> CodecPipeline:
    filters: list[dict] = []
    serializer: dict | None = None
    compressors: list[dict] = []

    for codec in on_disk_codecs:
        codec_dict = normalize_codec_dict(cast(dict, codec.to_dict()))
        if isinstance(codec, ArrayArrayCodec):
            filters.append(codec_dict)
        elif isinstance(codec, ArrayBytesCodec):
            serializer = codec_dict
        elif isinstance(codec, BytesBytesCodec):
            compressors.append(codec_dict)

    return CodecPipeline(
        filters=tuple(filters) or None,
        serializer=serializer,
        compressors=tuple(compressors) or None,
    )


def _project_on_disk_to_declared(
    declared: CodecPipeline,
    on_disk: CodecPipeline,
) -> CodecPipeline:
    return CodecPipeline(
        filters=_project_codec_tuple(declared.filters, on_disk.filters),
        serializer=_project_codec_dict(declared.serializer, on_disk.serializer),
        compressors=_project_codec_tuple(declared.compressors, on_disk.compressors),
    )


def _project_codec_tuple(
    declared: tuple[dict, ...] | None,
    on_disk: tuple[dict, ...] | None,
) -> tuple[dict, ...] | None:
    if declared is None or on_disk is None or len(declared) != len(on_disk):
        return on_disk
    return tuple(
        _project_codec_dict(declared_entry, on_disk_entry) or on_disk_entry
        for declared_entry, on_disk_entry in zip(declared, on_disk, strict=True)
    )


def _project_codec_dict(declared: dict | None, on_disk: dict | None) -> dict | None:
    if declared is None or on_disk is None or declared.get("name") != on_disk.get("name"):
        return on_disk

    declared_config = cast(dict[str, Any], declared.get("configuration", {}))
    on_disk_config = cast(dict[str, Any], on_disk.get("configuration", {}))
    projected = {key: on_disk[key] for key in declared if key in on_disk and key != "configuration"}
    projected["configuration"] = {
        key: on_disk_config[key] for key in declared_config if key in on_disk_config
    }
    return normalize_codec_dict(projected)
