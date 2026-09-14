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

"""Readback regressions for auxiliary coordinates and strict append ordering."""

from typing import Any, cast

import numpy as np
import pytest
import xarray as xr
import zarr

from firecube.core.config import StorageConfig
from firecube.core.zarr.validation import validate_group_with_fs
from firecube.ingestor.api import GenericZarrIngestor, PluginContext
from firecube.ingestor.errors import AppendOverwriteRefused, SchemaDriftReingestError
from firecube.ingestor.runtime.engine import PipelineFailedBatchesError
from firecube.ingestor.runtime.zarr.append_failure import AppendBatchFailed
from firecube.ingestor.runtime.zarr.strategies.append import AppendStrategy
from tests.helpers.storage import make_local_session, make_test_context

pytestmark = pytest.mark.integration


def _dataset(offsets, *, value=1, auxiliary=True):
    times = np.datetime64("2024-01-01", "ns") + np.asarray(offsets).astype("timedelta64[ms]")
    coords = {"time": times, "x": [0, 1]}
    if auxiliary:
        coords["coverage_end"] = ("time", times + np.timedelta64(1, "ms"))
        coords["bounds"] = (
            ("time", "edge"),
            np.column_stack([times, times + np.timedelta64(1, "ms")]),
        )
    ds = xr.Dataset(
        {
            "value": (("time", "x"), np.full((len(times), 2), value, dtype=float)),
            "static": ("x", [10.0, 20.0]),
        },
        coords=coords,
    )
    for name in ["time", "coverage_end", "bounds"]:
        if name in ds:
            ds[name].encoding.update(dtype="int64", units="nanoseconds since 1970-01-01")
    return ds


def _strategy(path, **options):
    return AppendStrategy(
        store=object(),
        store_uri=str(path),
        session=make_local_session(str(path)),
        storage_config=StorageConfig(storage_type="local", storage_driver="fsspec"),
        append_dim="time",
        chunk_shape={"time": 1, "x": 2},
        **options,
    )


def _write(path, offsets, *, value=1, auxiliary=True, **options):
    return _strategy(path, **options).write_groups(
        group_to_timestamps={"G": offsets},
        dataset_for_batch=lambda g, t: _dataset(t, value=value, auxiliary=auxiliary),
        batch_size=max(len(offsets), 1),
    )


@pytest.mark.parametrize("state", [1, 2, 3])
@pytest.mark.parametrize("tail", [False, True])
def test_coordinates_overwrite_and_refill(tmp_path, state, tail):
    path = tmp_path / "data.zarr"
    _write(path, [0, 1])
    group = cast(zarr.Group, zarr.open_group(str(path), mode="r+")["G"])
    cast(Any, group["firecube_timestamp_state"])[:] = state
    offsets = [0, 1, 2] if tail else [0, 1]
    _write(path, offsets, value=7, force_reingest=state == 1, resume_existing=state != 1)
    with xr.open_zarr(str(path), group="G", consolidated=False) as actual:
        expected = _dataset(offsets, value=7)
        for name in expected.variables:
            np.testing.assert_array_equal(actual[name].values, expected[name].values)
    np.testing.assert_array_equal(
        cast(Any, group["firecube_timestamp_state"])[:], np.ones(len(offsets))
    )


@pytest.mark.parametrize("operation", ["append", "overwrite"])
@pytest.mark.parametrize("change", ["missing", "extra"])
def test_schema_refusal_preserves_existing_arrays(tmp_path, operation, change):
    path = tmp_path / "data.zarr"
    _write(path, [0, 1], auxiliary=change == "missing")
    before = {str(p.relative_to(path)): p.read_bytes() for p in path.rglob("*") if p.is_file()}
    offsets = [2] if operation == "append" else [0, 1]
    with pytest.raises(SchemaDriftReingestError):
        _write(path, offsets, auxiliary=change == "extra", force_reingest=operation == "overwrite")
    after = {str(p.relative_to(path)): p.read_bytes() for p in path.rglob("*") if p.is_file()}
    assert before == after


@pytest.mark.parametrize(
    "offsets,reason", [([1, 1], "duplicates_incoming"), ([2, 1], "unsorted_incoming")]
)
def test_fresh_invalid_axis_refused_before_arrays_created(tmp_path, offsets, reason):
    path = tmp_path / "data.zarr"
    with pytest.raises(AppendOverwriteRefused) as exc:
        _write(path, offsets)
    assert exc.value.reason == reason
    assert not (path / "G").exists()


@pytest.mark.parametrize("offset", [0, 1, 2])
def test_plain_append_must_follow_stored_maximum(tmp_path, offset):
    path = tmp_path / "data.zarr"
    _write(path, [1, 2])
    with pytest.raises(AppendOverwriteRefused):
        _write(path, [offset])
    with xr.open_zarr(str(path), group="G", consolidated=False) as actual:
        np.testing.assert_array_equal(actual.time.values, _dataset([1, 2]).time.values)


def test_duplicates_not_hidden_by_resume_skip(tmp_path):
    path = tmp_path / "data.zarr"
    _write(path, [1, 2])
    with pytest.raises(AppendOverwriteRefused) as exc:
        _write(path, [1, 1], resume_existing=True)
    assert exc.value.reason == "duplicates_incoming"


class _Zarr(GenericZarrIngestor):
    PRODUCT_NAME = "coordinate_safety"
    name = "coordinate_safety"
    time_dim_name = "time"

    def discover_source_files(self, ctx: PluginContext):
        return ctx.option("x_times")

    def get_batch_groups(self, items, ctx: PluginContext):
        return ["G"]

    def build_dataset(self, group, items, ctx: PluginContext):
        return _dataset(items, value=ctx.option("x_value", 1))


def _engine_run(root, mode, times, workers=1, **options):
    host = _Zarr()
    ctx = make_test_context(
        root,
        product="coordinate_safety.zarr",
        options={
            "write_mode": mode,
            "x_times": times,
            "pipeline_workers": workers,
            "pipeline_batch_size": 1,
            "no_progress": True,
            "cleanup_workspace": True,
            "zarr_chunk_shape": {"time": 1, "x": 2},
            **options,
        },
    )
    host.run(ctx)
    return host


@pytest.mark.parametrize("mode", ["direct", "staged"])
@pytest.mark.parametrize("workers", [1, 2])
@pytest.mark.parametrize("existing", [False, True])
def test_engine_rejects_older_later_batch(tmp_path, mode, workers, existing):
    target = tmp_path / "coordinate_safety.zarr"
    if existing:
        _engine_run(tmp_path, mode, [0])
    with pytest.raises(PipelineFailedBatchesError, match="insert"):
        _engine_run(tmp_path, mode, [2, 1], workers, resume_existing=existing)
    if mode == "direct" or existing:
        with xr.open_zarr(str(target), group="G", consolidated=False) as actual:
            offsets = ([0, 2] if existing else [2]) if mode == "direct" else [0]
            np.testing.assert_array_equal(actual.time.values, _dataset(offsets).time.values)
            np.testing.assert_array_equal(actual.value.values, np.ones((len(offsets), 2)))
    else:
        assert not (target / "G").exists()


@pytest.mark.parametrize("mode", ["direct", "staged"])
def test_engine_resume_and_force_preserve_coordinates(tmp_path, mode):
    _engine_run(tmp_path, mode, [0, 1])
    _engine_run(tmp_path, mode, [0, 1, 2], resume_existing=True)
    _engine_run(tmp_path, mode, [1, 2], force_reingest=True, x_value=7)
    with xr.open_zarr(
        str(tmp_path / "coordinate_safety.zarr"), group="G", consolidated=False
    ) as actual:
        np.testing.assert_array_equal(actual.time.values, _dataset([0, 1, 2]).time.values)
        np.testing.assert_array_equal(
            actual.coverage_end.values, _dataset([0, 1, 2]).coverage_end.values
        )
        np.testing.assert_array_equal(actual.bounds.values, _dataset([0, 1, 2]).bounds.values)
        np.testing.assert_array_equal(actual.value.values, [[1, 1], [7, 7], [7, 7]])


def test_one_call_with_older_subbatch_rolls_back_group(tmp_path):
    path = tmp_path / "data.zarr"
    with pytest.raises(AppendBatchFailed, match="insert"):
        _strategy(path).write_groups(
            group_to_timestamps={"G": [2, 1]},
            dataset_for_batch=lambda g, t: _dataset(t),
            batch_size=1,
        )
    assert not (path / "G").exists()


def test_cached_boundary_rewinds_after_failed_append_rolls_back(tmp_path):
    path = tmp_path / "data.zarr"
    _write(path, [0])
    strategy = _strategy(path)
    with pytest.raises(AppendBatchFailed, match="insert"):
        strategy.write_groups(
            group_to_timestamps={"G": [2, 1]},
            dataset_for_batch=lambda g, t: _dataset(t),
            batch_size=1,
        )
    with xr.open_zarr(str(path), group="G", consolidated=False) as actual:
        np.testing.assert_array_equal(actual.time.values, _dataset([0]).time.values)
    strategy.write_groups(
        group_to_timestamps={"G": [1]},
        dataset_for_batch=lambda g, t: _dataset(t),
        batch_size=1,
    )
    with xr.open_zarr(str(path), group="G", consolidated=False) as actual:
        np.testing.assert_array_equal(actual.time.values, _dataset([0, 1]).time.values)


def test_validator_reports_short_auxiliary_array(tmp_path):
    path = tmp_path / "data.zarr"
    _write(path, [0, 1])
    cast(zarr.Array, zarr.open_group(str(path), mode="r+")["G/coverage_end"]).resize((1,))
    session = make_local_session(str(path))
    report = validate_group_with_fs(session.fs(), session.product.product_uri, "G")
    assert any("coverage_end" in issue and "length 1" in issue for issue in report.validity_issues)


def test_nat_refused_and_empty_batch_creates_no_group(tmp_path):
    path = tmp_path / "data.zarr"
    assert _write(path, [])["batch_processing"]["timestamps_written"] == 0
    assert not (path / "G").exists()
    ds = _dataset([0]).assign_coords(time=[np.datetime64("NaT", "ns")])
    with pytest.raises(AppendOverwriteRefused) as exc:
        _strategy(path).write_groups(
            group_to_timestamps={"G": [0]}, dataset_for_batch=lambda g, t: ds, batch_size=1
        )
    assert exc.value.reason == "nat_incoming"
    assert not (path / "G").exists()


def test_append_refuses_dates_not_representable_in_stored_units(tmp_path):
    path = tmp_path / "data.zarr"
    initial = _dataset([0])
    for variable in initial.variables.values():
        variable.encoding.clear()
    _strategy(path).write_groups(
        group_to_timestamps={"G": [0]}, dataset_for_batch=lambda g, t: initial, batch_size=1
    )
    before = {str(p.relative_to(path)): p.read_bytes() for p in path.rglob("*") if p.is_file()}
    with pytest.raises(ValueError, match="stored time encoding"):
        _write(path, [1])
    assert {
        str(p.relative_to(path)): p.read_bytes() for p in path.rglob("*") if p.is_file()
    } == before
