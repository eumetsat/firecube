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

from __future__ import annotations

import pytest

from firecube.ingestor.templates.config import (
    ZarrTemplateConfig,
    validate_zarr_specs_against_template,
)
from firecube.ingestor.templates.direct_zarr import ZarrArraySpec


@pytest.mark.unit
def test_compression_false_with_per_array_compressors_raises() -> None:
    spec = ZarrArraySpec(
        name="counts",
        shape=(10, 10),
        dtype="i4",
        compressors=({"name": "blosc"},),
    )
    template = ZarrTemplateConfig(zarr_compression=False)
    with pytest.raises(ValueError) as excinfo:
        validate_zarr_specs_against_template([spec], template)
    message = str(excinfo.value)
    assert "'counts'" in message
    assert "compressors" in message
    assert "zarr_compression=False" in message


@pytest.mark.unit
def test_compression_false_with_per_array_filters_raises() -> None:
    spec = ZarrArraySpec(
        name="counts",
        shape=(10, 10),
        dtype="i4",
        filters=({"name": "bitround", "configuration": {"keepbits": 8}},),
    )
    template = ZarrTemplateConfig(zarr_compression=False)
    with pytest.raises(ValueError) as excinfo:
        validate_zarr_specs_against_template([spec], template)
    message = str(excinfo.value)
    assert "'counts'" in message
    assert "filters" in message
    assert "zarr_compression=False" in message


@pytest.mark.unit
def test_compression_false_with_per_array_serializer_raises() -> None:
    spec = ZarrArraySpec(
        name="counts",
        shape=(10, 10),
        dtype="i4",
        serializer={"name": "bytes"},
    )
    template = ZarrTemplateConfig(zarr_compression=False)
    with pytest.raises(ValueError) as excinfo:
        validate_zarr_specs_against_template([spec], template)
    message = str(excinfo.value)
    assert "'counts'" in message
    assert "serializer" in message
    assert "zarr_compression=False" in message


@pytest.mark.unit
def test_compression_false_without_any_codec_fields_succeeds() -> None:
    spec = ZarrArraySpec(
        name="counts",
        shape=(10, 10),
        dtype="i4",
    )
    template = ZarrTemplateConfig(zarr_compression=False)
    validate_zarr_specs_against_template([spec], template)


@pytest.mark.unit
def test_compression_true_with_empty_compressors_succeeds() -> None:
    spec = ZarrArraySpec(
        name="mask",
        shape=(10, 10),
        dtype="u1",
        compressors=(),
    )
    template = ZarrTemplateConfig(zarr_compression=True)
    validate_zarr_specs_against_template([spec], template)


@pytest.mark.unit
def test_compression_true_with_per_array_compressors_succeeds() -> None:
    spec = ZarrArraySpec(
        name="counts",
        shape=(10, 10),
        dtype="i4",
        compressors=({"name": "blosc"},),
    )
    template = ZarrTemplateConfig(zarr_compression=True)
    validate_zarr_specs_against_template([spec], template)


@pytest.mark.unit
def test_compression_true_without_any_codec_fields_succeeds() -> None:
    spec = ZarrArraySpec(
        name="counts",
        shape=(10, 10),
        dtype="i4",
    )
    template = ZarrTemplateConfig(zarr_compression=True)
    validate_zarr_specs_against_template([spec], template)


@pytest.mark.unit
def test_multiple_specs_only_offender_name_in_error() -> None:
    clean_spec = ZarrArraySpec(name="latitude", shape=(10,), dtype="f4")
    offender = ZarrArraySpec(
        name="radiance",
        shape=(10, 10),
        dtype="f4",
        compressors=({"name": "blosc"},),
    )
    other_clean = ZarrArraySpec(name="longitude", shape=(10,), dtype="f4")
    template = ZarrTemplateConfig(zarr_compression=False)
    with pytest.raises(ValueError) as excinfo:
        validate_zarr_specs_against_template(
            [clean_spec, offender, other_clean],
            template,
        )
    message = str(excinfo.value)
    assert "'radiance'" in message
    assert "'latitude'" not in message
    assert "'longitude'" not in message


@pytest.mark.unit
def test_error_message_mentions_zarr_compression_false() -> None:
    spec = ZarrArraySpec(
        name="counts",
        shape=(10, 10),
        dtype="i4",
        compressors=({"name": "blosc"},),
    )
    template = ZarrTemplateConfig(zarr_compression=False)
    with pytest.raises(ValueError, match="zarr_compression=False"):
        validate_zarr_specs_against_template([spec], template)
