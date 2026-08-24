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

import logging
from typing import Any

import numpy as np
import pytest

from firecube.core.errors import SchemaDriftError
from firecube.core.zarr.region_writer import RegionZarrWriter
from firecube.ingestor.api import ZarrArraySpec


@pytest.fixture()
def writer(tmp_path):
    store = tmp_path / "sample.zarr"
    return RegionZarrWriter(str(store))


def _make_spec(**kwargs) -> ZarrArraySpec:
    base = {
        "name": "data",
        "shape": (4, 3, 2),
        "dtype": np.float32,
        "chunks": (2, 3, 2),
        "fill_value": 0.0,
    }
    base.update(kwargs)
    return ZarrArraySpec(**base)


def _make_array(
    writer: RegionZarrWriter,
    *,
    shape=(4, 3, 2),
    dtype: Any = np.float32,
    chunks=(2, 3, 2),
    shards: Any = None,
    fill_value: Any = 0.0,
):
    kwargs = {
        "shape": shape,
        "dtype": dtype,
        "chunks": chunks,
        "fill_value": fill_value,
    }
    if shards is not None:
        kwargs["shards"] = shards
    return writer.ensure_group("grp/data", **kwargs)


def test_matching_passes(writer):
    _make_array(writer)
    writer.verify_array_spec("grp/data", _make_spec(), expected_time_count=4)


def test_dtype_mismatch(writer):
    _make_array(writer, dtype=np.float32)

    with pytest.raises(SchemaDriftError, match="field='dtype'"):
        writer.verify_array_spec("grp/data", _make_spec(dtype=np.float64), expected_time_count=4)


def test_chunks_mismatch(writer):
    _make_array(writer, chunks=(2, 3, 2))

    with pytest.raises(SchemaDriftError, match="field='chunks'"):
        writer.verify_array_spec("grp/data", _make_spec(chunks=(1, 3, 2)), expected_time_count=4)


def test_shape_non_time_mismatch(writer):
    _make_array(writer, shape=(4, 5, 2))

    with pytest.raises(SchemaDriftError, match=r"shape\[1:\]"):
        writer.verify_array_spec("grp/data", _make_spec(shape=(4, 6, 2)), expected_time_count=4)


def test_rank_mismatch(writer):
    _make_array(writer, shape=(4, 3), chunks=(2, 3))

    with pytest.raises(SchemaDriftError, match="field='rank'"):
        writer.verify_array_spec("grp/data", _make_spec(shape=(4, 3, 1)), expected_time_count=4)


def test_fill_value_mismatch(writer):
    _make_array(writer, fill_value=0.0)

    with pytest.raises(SchemaDriftError, match="field='fill_value'"):
        writer.verify_array_spec("grp/data", _make_spec(fill_value=1.0), expected_time_count=4)


def test_fill_value_both_nan_passes(writer):
    _make_array(writer, fill_value=np.nan)
    writer.verify_array_spec("grp/data", _make_spec(fill_value=np.nan), expected_time_count=4)


def test_fill_value_both_nat_passes(writer):
    # datetime64 NaT is never == itself; two NaT fills must not read as drift
    # (otherwise slot-range verify fails for any timestamp/coordinate array).
    nat = np.datetime64("NaT", "s")
    _make_array(writer, dtype="datetime64[s]", fill_value=nat)
    writer.verify_array_spec(
        "grp/data",
        _make_spec(dtype="datetime64[s]", fill_value=nat),
        expected_time_count=4,
    )


def test_fill_value_nat_vs_real_datetime_still_drifts(writer):
    # A NaT fill and a concrete datetime fill are genuinely different.
    _make_array(writer, dtype="datetime64[s]", fill_value=np.datetime64("NaT", "s"))
    with pytest.raises(SchemaDriftError, match="field='fill_value'"):
        writer.verify_array_spec(
            "grp/data",
            _make_spec(dtype="datetime64[s]", fill_value=np.datetime64("2024-01-01", "s")),
            expected_time_count=4,
        )


def test_shape_time_larger_warns(writer, caplog):
    _make_array(writer, shape=(6, 3, 2))
    caplog.set_level(logging.WARNING, logger="firecube.core.zarr.region_writer")

    writer.verify_array_spec("grp/data", _make_spec(shape=(4, 3, 2)), expected_time_count=4)

    assert "over-allocation is benign" in caplog.text


def test_shape_time_smaller_fails(writer):
    _make_array(writer, shape=(3, 3, 2))

    with pytest.raises(SchemaDriftError, match=r"field='shape\[0\]'"):
        writer.verify_array_spec("grp/data", _make_spec(shape=(4, 3, 2)), expected_time_count=4)


def test_spec_chunks_none_skips_check(writer):
    _make_array(writer, chunks=(1, 3, 2))

    writer.verify_array_spec("grp/data", _make_spec(chunks=None), expected_time_count=4)


def test_verify_array_spec_raises_on_sharded_disk_unsharded_spec(writer):
    _make_array(writer, shards=(4, 3, 2))

    with pytest.raises(SchemaDriftError, match="sharded"):
        writer.verify_array_spec("grp/data", _make_spec(shards=None), expected_time_count=4)


def test_verify_array_spec_passes_when_both_unsharded(writer):
    _make_array(writer)

    writer.verify_array_spec("grp/data", _make_spec(shards=None), expected_time_count=4)


def test_verify_array_spec_passes_when_both_sharded(writer):
    _make_array(writer, shards=(4, 3, 2))

    writer.verify_array_spec(
        "grp/data",
        _make_spec(shards=(4, 3, 2)),
        expected_time_count=4,
    )


def test_mismatching_shards_raises(writer):
    writer.ensure_group(
        "grp/data",
        shape=(4, 3, 2),
        dtype=np.float32,
        chunks=(2, 3, 2),
        shards=(4, 3, 2),
        fill_value=0.0,
    )
    with pytest.raises(SchemaDriftError, match="field='shards'"):
        writer.verify_array_spec(
            "grp/data",
            _make_spec(shards=(2, 3, 2)),
            expected_time_count=4,
        )


def test_matching_attrs_subset_passes(writer):
    writer.ensure_group(
        "grp/data",
        shape=(4, 3, 2),
        dtype=np.float32,
        chunks=(2, 3, 2),
        fill_value=0.0,
        attrs={"units": "K", "long_name": "temperature"},
    )
    writer.verify_array_spec(
        "grp/data",
        _make_spec(attrs={"units": "K"}),
        expected_time_count=4,
    )


def test_conflicting_attrs_raises(writer):
    writer.ensure_group(
        "grp/data",
        shape=(4, 3, 2),
        dtype=np.float32,
        chunks=(2, 3, 2),
        fill_value=0.0,
        attrs={"units": "K"},
    )
    with pytest.raises(SchemaDriftError, match=r"field=\"attrs\['units'\]\""):
        writer.verify_array_spec(
            "grp/data",
            _make_spec(attrs={"units": "Pa"}),
            expected_time_count=4,
        )


def test_reserved_attr_in_spec_raises(writer):
    _make_array(writer)
    with pytest.raises(ValueError, match="Reserved attr"):
        writer.verify_array_spec(
            "grp/data",
            _make_spec(attrs={"_ARRAY_DIMENSIONS": ["time", "y", "x"]}),
            expected_time_count=4,
        )


def test_matching_dimension_names_passes(writer):
    writer.ensure_group(
        "grp/data",
        shape=(4, 3, 2),
        dtype=np.float32,
        chunks=(2, 3, 2),
        fill_value=0.0,
        dimension_names=("time", "y", "x"),
    )
    writer.verify_array_spec(
        "grp/data",
        _make_spec(dimension_names=("time", "y", "x")),
        expected_time_count=4,
    )


def test_dimension_names_conflict_raises(writer):
    writer.ensure_group(
        "grp/data",
        shape=(4, 3, 2),
        dtype=np.float32,
        chunks=(2, 3, 2),
        fill_value=0.0,
        dimension_names=("time", "y", "x"),
    )
    with pytest.raises(SchemaDriftError, match="field='dimension_names'"):
        writer.verify_array_spec(
            "grp/data",
            _make_spec(dimension_names=("t", "lat", "lon")),
            expected_time_count=4,
        )


def test_time_indexed_false_skips_time_axis(writer):
    writer.ensure_group(
        "grp/data",
        shape=(4, 5),
        dtype=np.float32,
        chunks=(4, 5),
        fill_value=0.0,
    )
    writer.verify_array_spec(
        "grp/data",
        _make_spec(shape=(4, 5), chunks=(4, 5), time_indexed=False),
        expected_time_count=1000,
    )
