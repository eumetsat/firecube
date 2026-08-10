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

"""Per-array codec override preservation tests against template default flip.

Locks direct return values from ``derive_effective_codecs_for_spec`` when the
template setting ``ZarrTemplateConfig.zarr_compression`` is ``True`` (the new
default). Protects against a regression in which flipping the template default
silently overrides per-array ``ZarrArraySpec`` codec fields.

Complements ``test_per_array_codec_override.py``: those tests exercise the full
derivation → writer path against real zarr stores (end-to-end). These tests pin
the direct return tuple of the derivation function so that:

* Test 1: a specific per-array compressor configuration (``zstd`` level=3)
  survives the template default and is resolved into a real codec instance
  with the declared configuration — NOT rewritten to the zarr default.
* Test 2: ``compressors=()`` (the ``compress-except-X`` marker) yields an
  explicit ``[]`` — not ``None`` and not the zarr default.
* Test 3: an all-None spec with ``zarr_compression=True`` and no template
  ``zarr_codecs`` falls through to ``(None, None, None)`` so zarr applies its
  own default at write time.
"""

from __future__ import annotations

from typing import Any, cast

import pytest
from zarr.abc.codec import BytesBytesCodec

from firecube.ingestor.runtime.zarr.write import derive_effective_codecs_for_spec
from firecube.ingestor.templates.config import ZarrTemplateConfig
from firecube.ingestor.templates.direct_zarr import ZarrArraySpec

pytestmark = pytest.mark.unit


def test_per_array_zstd_level_survives_template_true_default() -> None:
    """Per-array ``zstd(level=3)`` preserves level=3 despite the ``zarr_compression=True`` template default.

    Guards the regression in which flipping ``ZarrTemplateConfig.zarr_compression``
    to ``True`` by default silently coerces per-array compressors to the zarr
    default (which would collapse level=3 into level=0).
    """
    template = ZarrTemplateConfig(zarr_compression=True)
    spec = ZarrArraySpec(
        name="data",
        shape=(10, 1),
        dtype="float32",
        compressors=({"name": "zstd", "configuration": {"level": 3}},),
    )

    filters, serializer, compressors = derive_effective_codecs_for_spec(spec, template)

    assert filters is None, f"filters must be None when only compressors declared, got {filters!r}"
    assert serializer is None, (
        f"serializer must be None when only compressors declared, got {serializer!r}"
    )
    assert compressors is not None, "compressors must be a resolved list, not None"
    assert len(compressors) == 1, (
        f"per-array compressors length must be 1 for a single zstd entry, got {len(compressors)}"
    )
    codec = compressors[0]
    assert isinstance(codec, BytesBytesCodec), (
        f"resolved compressor must be a BytesBytesCodec, got {type(codec).__name__}"
    )
    dumped = cast(dict[str, Any], codec.to_dict())
    assert dumped.get("name") == "zstd", (
        f"per-array zstd must not be rewritten to another codec, got name={dumped.get('name')!r}"
    )
    configuration = cast(dict[str, Any], dumped.get("configuration", {}))
    assert configuration.get("level") == 3, (
        "per-array zstd level=3 must survive; template default flip must not coerce it "
        f"(got configuration={configuration!r})"
    )


def test_per_array_empty_compressors_survives_template_true_default() -> None:
    """Per-array ``compressors=()`` yields ``[]`` despite the ``zarr_compression=True`` template default.

    The ``compress-except-X`` pattern relies on ``()`` meaning "this specific
    array is uncompressed even though the template compresses by default".
    Guards the regression in which flipping the template default silently
    reinstates compression on arrays that explicitly opted out.
    """
    template = ZarrTemplateConfig(zarr_compression=True)
    spec = ZarrArraySpec(
        name="data",
        shape=(10, 1),
        dtype="float32",
        compressors=(),
    )

    _filters, _serializer, compressors = derive_effective_codecs_for_spec(spec, template)

    assert compressors == [], (
        "per-array compressors=() must yield an explicit empty list (uncompressed for this "
        f"array); template default flip must not reinstate compression (got {compressors!r})"
    )


def test_per_array_all_none_falls_through_to_zarr_default() -> None:
    """All-None per-array + ``zarr_compression=True`` + no ``zarr_codecs`` → ``(None, None, None)``.

    The ``(None, None, None)`` tuple signals "let zarr apply its own default at
    write time" — the intended template-default path when the plugin declares
    nothing. Guards against the template default flip accidentally injecting
    codec instances that would preempt zarr's own default resolution.
    """
    template = ZarrTemplateConfig(zarr_compression=True)
    spec = ZarrArraySpec(name="data", shape=(10, 1), dtype="float32")

    result = derive_effective_codecs_for_spec(spec, template)

    assert result == (None, None, None), (
        "no per-array declaration + zarr_compression=True + zarr_codecs=None must fall "
        f"through to (None, None, None) so zarr applies its own default; got {result!r}"
    )
