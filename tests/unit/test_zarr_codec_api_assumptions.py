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

"""Assumption-lock tests for zarr-python 3.x public API.

These tests lock the external API contracts that Phase 1 (issue #25) depends on.
If a zarr-python upgrade breaks any of these, it signals that
resolve_codec_pipeline() or _build_zarr_encoding() need review.
"""

from __future__ import annotations

from typing import Any, cast

import numpy as np
import pytest
import xarray as xr
import zarr
from zarr.abc.codec import BytesBytesCodec
from zarr.codecs import BloscCodec, BytesCodec
from zarr.registry import get_codec_class
from zarr.storage import MemoryStore

pytestmark = [pytest.mark.unit, pytest.mark.contract]


def _write_dataset_and_get_array(encoding: dict[str, object]) -> zarr.Array:
    ds = xr.Dataset({"var": (("x",), np.array([1, 2], dtype=np.int32))})
    store = MemoryStore()
    ds.to_zarr(store, encoding={"var": encoding})
    return cast(zarr.Array, zarr.open_array(store, path="var", mode="r"))


def test_get_codec_class_returns_class_for_known_codecs() -> None:
    """resolve_codec_pipeline() uses get_codec_class() for registry lookup;
    these names must work."""
    for name in ("blosc", "zstd", "gzip"):
        codec_class = get_codec_class(name)
        assert isinstance(codec_class, type)


def test_get_codec_class_raises_for_unknown_codec() -> None:
    """resolve_codec_pipeline() catches this exception and wraps it with
    config-path context."""
    with pytest.raises(KeyError, match="__no_such_codec_xyz__"):
        get_codec_class("__no_such_codec_xyz__")


def test_blosc_zstd_gzip_are_bytes_bytes_codecs() -> None:
    """resolve_codec_pipeline() type-gates output via isinstance(codec,
    BytesBytesCodec)."""
    for name in ("blosc", "zstd", "gzip"):
        codec = get_codec_class(name).from_dict({"name": name, "configuration": {}})
        assert isinstance(codec, BytesBytesCodec)


def test_bytes_codec_is_not_a_bytes_bytes_codec() -> None:
    """resolve_codec_pipeline() must reject 'bytes' codec at the compressors
    position with TypeError because it's an ArrayBytesCodec (serializer)."""
    assert not isinstance(BytesCodec(), BytesBytesCodec)


def test_disable_compression_encoding_shape() -> None:
    """Lock all three encoding shapes that affect compressor presence.

    Shape A: {} (LOSER) -> zarr injects a default BytesBytesCodec compressor.
    Shape B: {"compressors": None} (winner variant) -> no BytesBytesCodec.
    Shape C: {"compressors": []} (WINNER) -> no BytesBytesCodec.

    T7's _build_zarr_encoding uses {"compressors": []} for the
    disabled-compression branch because it is explicit and deterministic
    (Shape B also works but is equivalent; Shape A is excluded because it
    silently adds a default compressor).
    """
    # Shape A: empty dict {} — zarr injects its default compressor (LOSER)
    arr_a = _write_dataset_and_get_array({})
    codecs_a = cast(Any, arr_a.metadata).codecs
    assert any(isinstance(c, BytesBytesCodec) for c in codecs_a), (
        "Shape A {} must produce a default compressor (zarr injects one)"
    )

    # Shape B: {"compressors": None} — no compressor (winner variant)
    arr_b = _write_dataset_and_get_array({"compressors": None})
    codecs_b = cast(Any, arr_b.metadata).codecs
    assert not any(isinstance(c, BytesBytesCodec) for c in codecs_b), (
        'Shape B {"compressors": None} must produce no compressor'
    )

    # Shape C: {"compressors": []} — no compressor (canonical WINNER)
    arr_c = _write_dataset_and_get_array({"compressors": []})
    codecs_c = cast(Any, arr_c.metadata).codecs
    assert not any(isinstance(c, BytesBytesCodec) for c in codecs_c), (
        'Shape C {"compressors": []} must produce no compressor'
    )
    assert [type(c).__name__ for c in codecs_c] == ["BytesCodec"]


def test_compressor_encoding_with_codec_instance() -> None:
    """write_dataset_to_zarr passes codec instances in compressors; xarray must
    accept this."""
    arr = _write_dataset_and_get_array({"compressors": [BloscCodec(cname="zstd", clevel=5)]})
    codecs = cast(Any, arr.metadata).codecs
    assert any(type(codec).__name__.lower().startswith("blosc") for codec in codecs)
