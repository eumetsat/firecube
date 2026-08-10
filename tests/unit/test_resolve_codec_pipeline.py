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

"""Tests for resolve_codec_pipeline() — the split codec-registry resolver in write.py."""

from __future__ import annotations

from typing import Any, cast

import pytest
from zarr.abc.codec import ArrayBytesCodec, BytesBytesCodec
from zarr.codecs import BloscCodec, BytesCodec

from firecube.ingestor.runtime.zarr.write import resolve_codec_pipeline

pytestmark = pytest.mark.unit


def _configuration(codec: BytesBytesCodec | ArrayBytesCodec) -> dict[str, Any]:
    return cast(dict[str, Any], codec.to_dict().get("configuration", {}))


def test_all_none_returns_triple_none() -> None:
    assert resolve_codec_pipeline(None, None, None) == (None, None, None)


def test_all_defaults_returns_triple_none() -> None:
    assert resolve_codec_pipeline() == (None, None, None)


def test_compressors_blosc_zstd_clevel5() -> None:
    filters, serializer, compressors = resolve_codec_pipeline(
        compressors=[{"name": "blosc", "configuration": {"cname": "zstd", "clevel": 5}}]
    )

    assert filters is None
    assert serializer is None
    assert compressors is not None
    assert len(compressors) == 1
    codec = compressors[0]
    assert isinstance(codec, BytesBytesCodec)
    assert isinstance(codec, BloscCodec)
    config = _configuration(codec)
    assert config.get("cname") == "zstd"
    assert config.get("clevel") == 5


def test_compressors_multiple_entries_preserve_order() -> None:
    filters, serializer, compressors = resolve_codec_pipeline(
        compressors=[
            {"name": "blosc", "configuration": {"cname": "zstd", "clevel": 3}},
            {"name": "zstd", "configuration": {"level": 7}},
        ]
    )

    assert filters is None
    assert serializer is None
    assert compressors is not None
    assert [type(c).__name__.lower() for c in compressors] == ["bloscodec", "zstdcodec"] or [
        c.to_dict().get("name") for c in compressors
    ] == ["blosc", "zstd"]


def test_serializer_bytes_codec() -> None:
    filters, serializer, compressors = resolve_codec_pipeline(
        serializer={"name": "bytes", "configuration": {}}
    )

    assert filters is None
    assert compressors is None
    assert isinstance(serializer, ArrayBytesCodec)
    assert isinstance(serializer, BytesCodec)


def test_filters_bytes_rejected_as_wrong_abc() -> None:
    with pytest.raises(TypeError, match="filters") as exc_info:
        resolve_codec_pipeline(filters=[{"name": "bytes", "configuration": {}}])

    assert "ArrayArrayCodec" in str(exc_info.value)
    assert "bytes" in str(exc_info.value)


def test_serializer_blosc_rejected_as_wrong_abc() -> None:
    with pytest.raises(TypeError, match="serializer"):
        resolve_codec_pipeline(
            serializer={"name": "blosc", "configuration": {"cname": "zstd", "clevel": 5}}
        )


def test_compressors_bytes_rejected_as_wrong_abc() -> None:
    with pytest.raises(TypeError, match="compressors"):
        resolve_codec_pipeline(compressors=[{"name": "bytes", "configuration": {}}])


def test_unknown_codec_raises_value_error_naming_the_codec() -> None:
    with pytest.raises(ValueError, match="not a registered zarr codec") as exc_info:
        resolve_codec_pipeline(compressors=[{"name": "__no_such__", "configuration": {}}])

    assert "__no_such__" in str(exc_info.value)


def test_unknown_serializer_raises_value_error() -> None:
    with pytest.raises(ValueError, match="not a registered zarr codec"):
        resolve_codec_pipeline(serializer={"name": "__no_such__", "configuration": {}})


def test_malformed_configuration_raises_value_error() -> None:
    with pytest.raises(ValueError, match="failed codec-specific validation"):
        resolve_codec_pipeline(
            compressors=[{"name": "blosc", "configuration": {"cname": "zstd", "clevel": "fast"}}]
        )


def test_empty_compressors_list_returns_none_not_empty_list() -> None:
    filters, _serializer, compressors = resolve_codec_pipeline(filters=[], compressors=[])

    assert filters is None
    assert compressors is None


def test_missing_configuration_key_is_accepted() -> None:
    _, _, compressors = resolve_codec_pipeline(compressors=[{"name": "blosc"}])

    assert compressors is not None
    assert isinstance(compressors[0], BytesBytesCodec)
