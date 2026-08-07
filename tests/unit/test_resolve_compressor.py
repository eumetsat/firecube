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

"""Tests for resolve_compressor() — the codec-registry resolver in write.py."""

from __future__ import annotations

from typing import Any, cast

import pytest
from zarr.abc.codec import BytesBytesCodec
from zarr.codecs import BloscCodec

from firecube.ingestor.runtime.zarr.write import resolve_compressor

pytestmark = pytest.mark.unit


def _configuration(codec: BytesBytesCodec) -> dict[str, Any]:
    return cast(dict[str, Any], codec.to_dict().get("configuration", {}))


def test_none_no_default_returns_none() -> None:
    assert resolve_compressor(None, use_default=False) is None


def test_none_use_default_returns_blosc_zstd_5() -> None:
    codec = resolve_compressor(None, use_default=True)

    assert isinstance(codec, BytesBytesCodec)
    assert isinstance(codec, BloscCodec)
    d = codec.to_dict()
    assert d["name"] == "blosc"
    config = _configuration(codec)
    assert config.get("cname") == "zstd"
    assert config.get("clevel") == 5


def test_explicit_blosc_custom_knobs() -> None:
    codec = resolve_compressor(
        {"name": "blosc", "configuration": {"cname": "zstd", "clevel": 9, "typesize": 8}},
        use_default=False,
    )

    assert isinstance(codec, BytesBytesCodec)
    assert isinstance(codec, BloscCodec)
    config = _configuration(codec)
    assert config.get("clevel") == 9
    assert config.get("typesize") == 8


def test_explicit_zstd_codec() -> None:
    codec = resolve_compressor(
        {"name": "zstd", "configuration": {"level": 7}},
        use_default=False,
    )

    assert isinstance(codec, BytesBytesCodec)
    d = codec.to_dict()
    assert d["name"] == "zstd"


def test_unknown_codec_raises_value_error() -> None:
    with pytest.raises(ValueError, match="zarr_codecs\\[0\\]\\.name"):
        resolve_compressor({"name": "__no_such__", "configuration": {}}, use_default=False)


def test_unknown_codec_error_message_names_the_codec() -> None:
    with pytest.raises(ValueError, match="not a registered zarr codec"):
        resolve_compressor({"name": "__no_such__", "configuration": {}}, use_default=False)


def test_bytes_codec_rejected_as_non_compressor() -> None:
    with pytest.raises(TypeError, match="not a BytesBytesCodec"):
        resolve_compressor(
            {"name": "bytes", "configuration": {"endian": "little"}}, use_default=False
        )


def test_bytes_codec_error_names_config_path() -> None:
    with pytest.raises(TypeError, match="zarr_codecs\\[0\\]\\.name"):
        resolve_compressor(
            {"name": "bytes", "configuration": {"endian": "little"}}, use_default=False
        )


def test_malformed_config_raises_codec_specific_validation_error() -> None:
    with pytest.raises(ValueError, match="codec-specific validation") as exc_info:
        resolve_compressor(
            {"name": "blosc", "configuration": {"cname": "zstd", "clevel": "fast"}},
            use_default=False,
        )

    assert "zarr_codecs[0].configuration" in str(exc_info.value)


def test_no_configuration_key_delegates_to_zarr() -> None:
    # Missing 'configuration' key in the entry — T5 allows this; T6 must handle it.
    codec = resolve_compressor({"name": "blosc"}, use_default=False)

    assert isinstance(codec, BytesBytesCodec)
