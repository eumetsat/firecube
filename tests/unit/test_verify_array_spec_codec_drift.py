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
from zarr.codecs import BloscCodec, BytesCodec

from firecube.core.errors import SchemaDriftError
from firecube.core.zarr.region_writer import RegionZarrWriter
from firecube.ingestor.api import ZarrArraySpec

pytestmark = pytest.mark.unit


@pytest.fixture()
def writer(tmp_path) -> RegionZarrWriter:
    return RegionZarrWriter(str(tmp_path / "codec-drift.zarr"))


def test_declared_compressor_level_drift_raises(writer: RegionZarrWriter) -> None:
    writer.ensure_group(
        "grp/data",
        shape=(10,),
        dtype="f4",
        compressors=[BloscCodec(cname="zstd", clevel=5)],
    )
    spec = ZarrArraySpec(
        name="data",
        shape=(10,),
        dtype="f4",
        compressors=({"name": "blosc", "configuration": {"cname": "zstd", "clevel": 9}},),
    )

    with pytest.raises(SchemaDriftError, match="field='compressors'"):
        writer.verify_array_spec("grp/data", spec, expected_time_count=10)


def test_undeclared_codecs_do_not_drift_against_zarr_defaults(writer: RegionZarrWriter) -> None:
    writer.ensure_group("grp/data", shape=(10,), dtype="f4")
    spec = ZarrArraySpec(name="data", shape=(10,), dtype="f4")

    writer.verify_array_spec("grp/data", spec, expected_time_count=10)


def test_static_array_declared_compressor_drift_raises(writer: RegionZarrWriter) -> None:
    writer.ensure_group(
        "grp/data",
        shape=(10,),
        dtype="f4",
        compressors=[BloscCodec(cname="zstd", clevel=5)],
    )
    spec = ZarrArraySpec(
        name="data",
        shape=(10,),
        dtype="f4",
        time_indexed=False,
        compressors=({"name": "blosc", "configuration": {"cname": "zstd", "clevel": 9}},),
    )

    with pytest.raises(SchemaDriftError, match="field='compressors'"):
        writer.verify_array_spec("grp/data", spec, expected_time_count=10)


def test_declared_compressor_key_order_does_not_drift(writer: RegionZarrWriter) -> None:
    writer.ensure_group(
        "grp/data",
        shape=(10,),
        dtype="f4",
        compressors=[BloscCodec(cname="zstd", clevel=5)],
    )
    spec = ZarrArraySpec(
        name="data",
        shape=(10,),
        dtype="f4",
        compressors=({"configuration": {"clevel": 5, "cname": "zstd"}, "name": "blosc"},),
    )

    writer.verify_array_spec("grp/data", spec, expected_time_count=10)


def test_declared_serializer_endian_drift_raises(writer: RegionZarrWriter) -> None:
    writer.ensure_group(
        "grp/data",
        shape=(10,),
        dtype="i4",
        serializer=BytesCodec(endian="little"),
    )
    spec = ZarrArraySpec(
        name="data",
        shape=(10,),
        dtype="i4",
        serializer={"name": "bytes", "configuration": {"endian": "big"}},
    )

    with pytest.raises(SchemaDriftError, match="field='serializer'"):
        writer.verify_array_spec("grp/data", spec, expected_time_count=10)
