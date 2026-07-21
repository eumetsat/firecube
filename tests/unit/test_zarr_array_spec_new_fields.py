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
def test_shards_default_is_none():
    from firecube.ingestor.api import ZarrArraySpec

    spec = ZarrArraySpec(name="x", shape=(10, 4, 5), dtype="float32")

    assert spec.shards is None


@pytest.mark.unit
def test_attrs_default_is_none():
    from firecube.ingestor.api import ZarrArraySpec

    spec = ZarrArraySpec(name="x", shape=(10, 4, 5), dtype="float32")

    assert spec.attrs is None


@pytest.mark.unit
def test_dimension_names_default_is_none():
    from firecube.ingestor.api import ZarrArraySpec

    spec = ZarrArraySpec(name="x", shape=(10, 4, 5), dtype="float32")

    assert spec.dimension_names is None


@pytest.mark.unit
def test_time_indexed_default_is_true():
    from firecube.ingestor.api import ZarrArraySpec

    spec = ZarrArraySpec(name="x", shape=(10, 4, 5), dtype="float32")

    assert spec.time_indexed is True


@pytest.mark.unit
def test_shards_length_mismatch_raises():
    from firecube.ingestor.api import ZarrArraySpec

    with pytest.raises(ValueError, match=r"shards.*length|rank"):
        ZarrArraySpec(name="x", shape=(10, 4, 5), dtype="float32", shards=(1, 2))


@pytest.mark.unit
def test_dimension_names_length_mismatch_raises():
    from firecube.ingestor.api import ZarrArraySpec

    with pytest.raises(ValueError, match="dimension_names"):
        ZarrArraySpec(
            name="x",
            shape=(10, 4, 5),
            dtype="float32",
            dimension_names=("time",),
        )


@pytest.mark.unit
def test_attrs_invalid_type_raises():
    from firecube.ingestor.api import ZarrArraySpec

    attrs: Any = [1, 2, 3]
    with pytest.raises(ValueError, match="attrs"):
        ZarrArraySpec(name="x", shape=(10, 4, 5), dtype="float32", attrs=attrs)


@pytest.mark.unit
def test_all_new_fields_constructible():
    from firecube.ingestor.api import ZarrArraySpec

    attrs = {"units": "K"}
    spec = ZarrArraySpec(
        name="x",
        shape=(10, 4, 5),
        dtype="float32",
        shards=(1, 2, 3),
        attrs=attrs,
        dimension_names=("time", "y", "x"),
        time_indexed=False,
    )

    assert spec.shards == (1, 2, 3)
    assert spec.attrs == attrs
    assert spec.dimension_names == ("time", "y", "x")
    assert spec.time_indexed is False


@pytest.mark.unit
def test_expected_time_count_with_time_indexed_false_raises() -> None:
    from firecube.ingestor.api import ZarrArraySpec

    with pytest.raises(
        ValueError,
        match=r"(?i)expected_time_count.*time_indexed|time_indexed.*expected_time_count",
    ):
        ZarrArraySpec(
            name="lat",
            shape=(4,),
            dtype="float64",
            time_indexed=False,
            expected_time_count=10,
        )


@pytest.mark.unit
def test_static_spec_without_expected_time_count_constructs() -> None:
    from firecube.ingestor.api import ZarrArraySpec

    spec = ZarrArraySpec(name="lat", shape=(4,), dtype="float64", time_indexed=False)

    assert spec.expected_time_count is None
    assert spec.time_indexed is False


@pytest.mark.unit
def test_time_indexed_spec_with_expected_time_count_constructs() -> None:
    from firecube.ingestor.api import ZarrArraySpec

    spec = ZarrArraySpec(name="data", shape=(10, 4), dtype="float32", expected_time_count=10)

    assert spec.expected_time_count == 10
    assert spec.time_indexed is True


@pytest.mark.unit
def test_time_indexed_spec_with_zero_expected_time_count_constructs() -> None:
    from firecube.ingestor.api import ZarrArraySpec

    spec = ZarrArraySpec(name="data", shape=(0, 4), dtype="float32", expected_time_count=0)

    assert spec.expected_time_count == 0
    assert spec.time_indexed is True
