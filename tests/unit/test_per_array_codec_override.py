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

"""Per-array codec-override contract tests for DirectZarr.

Locks the per-array override matrix that governs how ``ZarrArraySpec`` codec
fields (``filters`` / ``serializer`` / ``compressors``) interact with the
``ZarrTemplateConfig.zarr_compression`` setting. The seven cases split into:

Positive (template ``True``, override lands on the array):
  * per-array ``compressors=None``    → inherit template default (zarr default)
  * per-array ``compressors=()``      → explicitly uncompressed for THIS array
                                        (``compress-except-X`` pattern)
  * per-array ``compressors=(blosc,)`` → declared blosc lands on the array

Negative (template ``False``, ANY per-array codec is rejected by the shared
validator ``validate_zarr_specs_against_template``):
  * per-array ``compressors=(blosc,)`` → ``ValueError``
  * per-array ``compressors=()``       → ``ValueError`` (empty tuple is still a declaration)
  * per-array ``filters=(bitround,)``  → ``ValueError``
  * per-array ``serializer={bytes}``   → ``ValueError``

Positive cases exercise the full derivation → writer path with real zarr stores;
negative cases only need the validator call.
"""

from __future__ import annotations

from typing import Any, cast

import numpy as np
import pytest
import zarr
from zarr.abc.codec import BytesBytesCodec

from firecube.core.zarr.region_writer import RegionZarrWriter
from firecube.ingestor.runtime.zarr.write import derive_effective_codecs_for_spec
from firecube.ingestor.templates.config import (
    ZarrTemplateConfig,
    validate_zarr_specs_against_template,
)
from firecube.ingestor.templates.direct_zarr import ZarrArraySpec

pytestmark = pytest.mark.unit


BLOSC_ENTRY: dict[str, Any] = {
    "name": "blosc",
    "configuration": {"cname": "zstd", "clevel": 5},
}


def _codecs_of(store_path: str, array_path: str) -> list[Any]:
    arr = zarr.open_array(store_path, path=array_path, mode="r")
    return list(cast(Any, arr.metadata).codecs)


def _codec_names(codecs: list[Any]) -> list[str]:
    names: list[str] = []
    for codec in codecs:
        to_dict = getattr(codec, "to_dict", None)
        if callable(to_dict):
            dumped = cast(dict[str, Any], to_dict())
            name = dumped.get("name")
            if isinstance(name, str):
                names.append(name)
                continue
        names.append(type(codec).__name__)
    return names


def _apply(spec: ZarrArraySpec, template: ZarrTemplateConfig, store_path: str) -> str:
    filters, serializer, compressors = derive_effective_codecs_for_spec(spec, template)
    writer = RegionZarrWriter(store_path)
    writer.ensure_group(
        f"G/{spec.name}",
        shape=spec.shape,
        dtype=np.dtype(spec.dtype),
        filters=filters,
        serializer=serializer,
        compressors=compressors,
    )
    return store_path


# ---------------------------------------------------------------------------
# Positive matrix — template True, per-array override lands on the array
# ---------------------------------------------------------------------------


def test_per_array_none_inherits_template_default(tmp_path) -> None:
    """None per-array + True template + no template codecs → zarr default codec."""
    template = ZarrTemplateConfig(zarr_compression=True, zarr_codecs=None)
    spec = ZarrArraySpec(name="var", shape=(8,), dtype="f4")

    store = _apply(spec, template, str(tmp_path / "inherit.zarr"))
    codecs = _codecs_of(store, "G/var")
    assert any(isinstance(c, BytesBytesCodec) for c in codecs), (
        f"None per-array must inherit zarr default (a BytesBytesCodec), got {_codec_names(codecs)!r}"
    )


def test_per_array_empty_compressors_uncompressed(tmp_path) -> None:
    """``compressors=()`` per-array + True template → array is explicitly uncompressed."""
    template = ZarrTemplateConfig(zarr_compression=True, zarr_codecs=None)
    spec = ZarrArraySpec(name="mask", shape=(8,), dtype="u1", compressors=())

    store = _apply(spec, template, str(tmp_path / "compress-except.zarr"))
    codecs = _codecs_of(store, "G/mask")
    assert not any(isinstance(c, BytesBytesCodec) for c in codecs), (
        f"per-array compressors=() must yield no BytesBytesCodec, got {_codec_names(codecs)!r}"
    )


def test_per_array_specific_compressor(tmp_path) -> None:
    """``compressors=(blosc,)`` per-array + True template → declared blosc lands on the array."""
    template = ZarrTemplateConfig(zarr_compression=True, zarr_codecs=None)
    spec = ZarrArraySpec(
        name="var",
        shape=(8,),
        dtype="f4",
        compressors=(BLOSC_ENTRY,),
    )

    store = _apply(spec, template, str(tmp_path / "per-array-blosc.zarr"))
    names = _codec_names(_codecs_of(store, "G/var"))
    assert "blosc" in names, f"per-array declared blosc must land on the array, got {names!r}"


# ---------------------------------------------------------------------------
# Negative matrix — template False rejects any per-array codec declaration
# ---------------------------------------------------------------------------


def test_per_array_false_template_with_compressors_raises() -> None:
    """Template False + per-array declared blosc compressor → validator rejects."""
    template = ZarrTemplateConfig(zarr_compression=False)
    spec = ZarrArraySpec(
        name="var",
        shape=(8,),
        dtype="f4",
        compressors=(BLOSC_ENTRY,),
    )
    with pytest.raises(ValueError) as excinfo:
        validate_zarr_specs_against_template([spec], template)
    msg = str(excinfo.value)
    assert "'var'" in msg
    assert "compressors" in msg
    assert "zarr_compression=False" in msg


def test_per_array_false_template_with_empty_compressors_raises() -> None:
    """Template False + per-array ``compressors=()`` → validator rejects (empty is still a declaration)."""
    template = ZarrTemplateConfig(zarr_compression=False)
    spec = ZarrArraySpec(name="var", shape=(8,), dtype="f4", compressors=())
    with pytest.raises(ValueError) as excinfo:
        validate_zarr_specs_against_template([spec], template)
    msg = str(excinfo.value)
    assert "'var'" in msg
    assert "compressors" in msg
    assert "zarr_compression=False" in msg


def test_per_array_false_template_with_filters_raises() -> None:
    """Template False + per-array declared filter → validator rejects."""
    template = ZarrTemplateConfig(zarr_compression=False)
    spec = ZarrArraySpec(
        name="var",
        shape=(8,),
        dtype="f4",
        filters=({"name": "bitround", "configuration": {"keepbits": 8}},),
    )
    with pytest.raises(ValueError) as excinfo:
        validate_zarr_specs_against_template([spec], template)
    msg = str(excinfo.value)
    assert "'var'" in msg
    assert "filters" in msg
    assert "zarr_compression=False" in msg


def test_per_array_false_template_with_serializer_raises() -> None:
    """Template False + per-array declared serializer → validator rejects."""
    template = ZarrTemplateConfig(zarr_compression=False)
    spec = ZarrArraySpec(
        name="var",
        shape=(8,),
        dtype="f4",
        serializer={"name": "bytes"},
    )
    with pytest.raises(ValueError) as excinfo:
        validate_zarr_specs_against_template([spec], template)
    msg = str(excinfo.value)
    assert "'var'" in msg
    assert "serializer" in msg
    assert "zarr_compression=False" in msg
