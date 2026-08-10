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

from typing import Any

import pytest


@pytest.mark.unit
def test_compressors_entry_must_be_dict():
    from firecube.ingestor.api import ZarrArraySpec

    compressors: Any = ([1, 2, 3],)
    with pytest.raises(ValueError, match=r"compressors\[0\].*dict"):
        ZarrArraySpec(name="x", shape=(10,), dtype="f4", compressors=compressors)


@pytest.mark.unit
def test_serializer_must_be_dict():
    from firecube.ingestor.api import ZarrArraySpec

    serializer: Any = "bytes"
    with pytest.raises(ValueError, match=r"serializer.*dict"):
        ZarrArraySpec(name="x", shape=(10,), dtype="f4", serializer=serializer)


@pytest.mark.unit
def test_compressors_must_be_tuple():
    from firecube.ingestor.api import ZarrArraySpec

    compressors: Any = {"name": "blosc"}
    with pytest.raises(ValueError, match=r"compressors.*tuple"):
        ZarrArraySpec(name="x", shape=(10,), dtype="f4", compressors=compressors)


@pytest.mark.unit
def test_full_construction_with_all_codec_fields():
    from firecube.ingestor.api import ZarrArraySpec

    spec = ZarrArraySpec(
        name="x",
        shape=(10, 4, 5),
        dtype="f4",
        shards=(1, 2, 3),
        attrs={"units": "K"},
        dimension_names=("time", "y", "x"),
        time_indexed=False,
        filters=({"name": "bitround", "configuration": {"keepbits": 8}},),
        serializer={"name": "bytes", "configuration": {}},
        compressors=({"name": "blosc", "configuration": {"cname": "zstd"}},),
    )

    assert spec.shards == (1, 2, 3)
    assert spec.attrs == {"units": "K"}
    assert spec.dimension_names == ("time", "y", "x")
    assert spec.time_indexed is False
    assert spec.filters == ({"name": "bitround", "configuration": {"keepbits": 8}},)
    assert spec.serializer == {"name": "bytes", "configuration": {}}
    assert spec.compressors == ({"name": "blosc", "configuration": {"cname": "zstd"}},)
