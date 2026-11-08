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

from copy import deepcopy
from typing import Any

import pytest
from zarr.codecs import BloscCodec, BytesCodec

from firecube.core.errors import SchemaDriftError
from firecube.core.zarr.region_writer import RegionZarrWriter
from firecube.sdk import ZarrArraySpec

pytestmark = pytest.mark.unit


_BLOSC_5 = [BloscCodec(cname="zstd", clevel=5)]
_BLOSC_9 = ({"name": "blosc", "configuration": {"cname": "zstd", "clevel": 9}},)
_CODEC_DRIFT_ROWS: tuple[tuple[str, dict[str, Any], dict[str, Any], str | None], ...] = (
    # (row id, stored-array kwargs, declared-spec kwargs, expected drifted field or None)
    (
        "declared_compressor_level_drift_raises",
        {"compressors": _BLOSC_5},
        {"compressors": _BLOSC_9},
        "compressors",
    ),
    (
        "static_array_declared_compressor_drift_raises",
        {"compressors": _BLOSC_5},
        {"time_indexed": False, "compressors": _BLOSC_9},
        "compressors",
    ),
    (
        "declared_serializer_endian_drift_raises",
        {"dtype": "i4", "serializer": BytesCodec(endian="little")},
        {"dtype": "i4", "serializer": {"name": "bytes", "configuration": {"endian": "big"}}},
        "serializer",
    ),
    ("undeclared_codecs_do_not_drift_against_zarr_defaults", {}, {}, None),
    (
        "declared_compressor_key_order_does_not_drift",
        {"compressors": _BLOSC_5},
        {"compressors": ({"configuration": {"clevel": 5, "cname": "zstd"}, "name": "blosc"},)},
        None,
    ),
)


def test_verify_array_spec_codec_drift(subtests, tmp_path) -> None:
    for row_id, stored, declared, drifted in _CODEC_DRIFT_ROWS:
        with subtests.test(msg=f"test_{row_id}"):
            writer = RegionZarrWriter(str(tmp_path / f"{row_id}.zarr"))
            writer.ensure_group("grp/data", **{"shape": (10,), "dtype": "f4", **deepcopy(stored)})
            spec = ZarrArraySpec(
                name="data", **{"shape": (10,), "dtype": "f4", **deepcopy(declared)}
            )
            if drifted is None:
                writer.verify_array_spec("grp/data", spec, expected_time_count=10)
            else:
                with pytest.raises(SchemaDriftError, match=f"field='{drifted}'"):
                    writer.verify_array_spec("grp/data", spec, expected_time_count=10)
