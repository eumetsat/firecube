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

"""Unit coverage for canonical Zarr codec pipeline helpers."""

from __future__ import annotations

from typing import Any, cast

import numpy as np
import pytest
import zarr
from zarr.codecs import BloscCodec
from zarr.storage import MemoryStore

from firecube.core.zarr.codec_pipeline import (
    CodecPipeline,
    compare_pipelines,
    normalize_codec_dict,
    split_zarr_codecs,
)

pytestmark = pytest.mark.unit


def _array_codecs(*, compressor: BloscCodec) -> tuple:
    store = MemoryStore()
    arr = zarr.create_array(
        store=store,
        shape=(2,),
        chunks=(2,),
        dtype=np.dtype("int32"),
        compressors=[compressor],
        zarr_format=3,
    )
    return cast(Any, arr.metadata).codecs


def test_normalize_codec_dict_equivalent_regardless_of_key_order() -> None:
    a = normalize_codec_dict(
        {"name": "blosc", "configuration": {"shuffle": "shuffle", "clevel": 5, "cname": "zstd"}}
    )
    b = normalize_codec_dict(
        {"configuration": {"cname": "zstd", "clevel": 5, "shuffle": "shuffle"}, "name": "blosc"}
    )

    assert a == b


def test_normalize_codec_dict_fills_missing_configuration() -> None:
    assert normalize_codec_dict({"name": "zstd"}) == {"configuration": {}, "name": "zstd"}


def test_normalize_codec_dict_canonicalizes_equivalent_key_order() -> None:
    left = {"name": "blosc", "configuration": {"cname": "zstd", "clevel": 5}}
    right = {"configuration": {"clevel": 5, "cname": "zstd"}, "name": "blosc"}

    assert normalize_codec_dict(left) == normalize_codec_dict(right)


def test_compare_pipelines_returns_empty_list_when_declared_matches_on_disk() -> None:
    codecs = _array_codecs(compressor=BloscCodec(cname="zstd", clevel=5))
    declared = CodecPipeline(
        None,
        None,
        ({"name": "blosc", "configuration": {"cname": "zstd", "clevel": 5}},),
    )

    assert compare_pipelines(declared, codecs) == []


def test_compare_pipelines_reports_compressor_level_mismatch() -> None:
    codecs = _array_codecs(compressor=BloscCodec(cname="zstd", clevel=5))
    declared = CodecPipeline(
        None,
        None,
        ({"name": "blosc", "configuration": {"cname": "zstd", "clevel": 9}},),
    )

    mismatches = compare_pipelines(declared, codecs)

    assert [field for field, _, _ in mismatches] == ["compressors"]


def test_compare_pipelines_returns_empty_list_when_nothing_declared() -> None:
    codecs = _array_codecs(compressor=BloscCodec(cname="zstd", clevel=5))

    assert compare_pipelines(CodecPipeline(None, None, None), codecs) == []


def test_split_zarr_codecs_none_returns_empty_split() -> None:
    assert split_zarr_codecs(None) == (None, None, None)


def test_split_zarr_codecs_classifies_compressor() -> None:
    codec = {"name": "zstd", "configuration": {}}

    assert split_zarr_codecs([codec]) == (None, None, [codec])


def test_split_zarr_codecs_classifies_serializer_and_compressor() -> None:
    serializer = {"name": "bytes", "configuration": {}}
    compressor = {"name": "zstd", "configuration": {}}

    assert split_zarr_codecs([serializer, compressor]) == (None, serializer, [compressor])
