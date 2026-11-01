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
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
import xarray as xr
import zarr

from firecube.core.storage.session import storage_config_from_binding
from firecube.ingestor.api import ConfigurationError
from firecube.ingestor.errors import SchemaDriftError
from firecube.ingestor.runtime.zarr.strategies.append import AppendStrategy
from firecube.ingestor.runtime.zarr.write import write_dataset_to_zarr
from tests.helpers.storage import local_zarr_handle, make_local_session

pytestmark = pytest.mark.integration

_GROUP = "G"


def _dataset(
    start: str,
    *,
    static: np.ndarray | None = None,
    extra_static: bool = False,
) -> xr.Dataset:
    timestamps = pd.date_range(start, periods=2, freq="h")
    data_vars: dict[str, Any] = {
        "dynamic": (("timestamp", "x"), np.arange(6, dtype=np.float32).reshape(2, 3)),
    }
    if static is not None:
        data_vars["static"] = (("x",), static)
    if extra_static:
        data_vars["new_static"] = (("x",), np.array([5, 6, 7], dtype=np.int16))
    return xr.Dataset(
        data_vars,
        coords={"timestamp": timestamps, "x": np.arange(3)},
    )


def _write_first(store: Path, ds: xr.Dataset) -> None:
    write_dataset_to_zarr(
        ds,
        zarr_store=local_zarr_handle(store, mode="w"),
        group=_GROUP,
        mode="w",
        time_dim="timestamp",
        compression=False,
    )


def _append(store: Path, ds: xr.Dataset, *, force_reingest: bool = False) -> None:
    write_dataset_to_zarr(
        ds,
        zarr_store=local_zarr_handle(store, mode="a"),
        preflight_compare_zarr_store=local_zarr_handle(store, mode="r"),
        group=_GROUP,
        mode="a",
        time_dim="timestamp",
        compression=False,
        force_reingest=force_reingest,
    )


def test_fresh_first_write_stores_static_var(tmp_path: Path) -> None:
    store = tmp_path / "fresh.zarr"

    _write_first(store, _dataset("2024-01-01", static=np.array([1, 2, 3], dtype=np.float32)))

    opened = xr.open_zarr(store, group=_GROUP, consolidated=False)
    assert "static" in opened.data_vars
    np.testing.assert_array_equal(opened["static"].values, np.array([1, 2, 3], dtype=np.float32))


def test_same_static_value_across_runs_does_not_raise(tmp_path: Path) -> None:
    store = tmp_path / "same.zarr"
    _write_first(store, _dataset("2024-01-01", static=np.array([1, 2, 3], dtype=np.float32)))

    _append(store, _dataset("2024-01-01T02:00", static=np.array([1, 2, 3], dtype=np.float32)))

    opened = xr.open_zarr(store, group=_GROUP, consolidated=False)
    assert opened.sizes["timestamp"] == 4


def test_different_static_value_raises_schema_drift(tmp_path: Path) -> None:
    store = tmp_path / "drift.zarr"
    _write_first(store, _dataset("2024-01-01", static=np.array([1, 2, 3], dtype=np.float32)))

    with pytest.raises(SchemaDriftError, match="static"):
        _append(store, _dataset("2024-01-01T02:00", static=np.array([1, 9, 3], dtype=np.float32)))


def test_new_static_var_on_append_raises_schema_drift(tmp_path: Path) -> None:
    store = tmp_path / "new-static.zarr"
    _write_first(store, _dataset("2024-01-01", static=np.array([1, 2, 3], dtype=np.float32)))

    with pytest.raises(SchemaDriftError, match="new_static"):
        _append(
            store,
            _dataset(
                "2024-01-01T02:00",
                static=np.array([1, 2, 3], dtype=np.float32),
                extra_static=True,
            ),
        )


def test_same_static_value_float32_float64_does_not_raise(tmp_path: Path) -> None:
    store = tmp_path / "dtype-tolerant.zarr"
    _write_first(store, _dataset("2024-01-01", static=np.array([1, 2, 3], dtype=np.float32)))

    _append(store, _dataset("2024-01-01T02:00", static=np.array([1, 2, 3], dtype=np.float64)))

    opened = xr.open_zarr(store, group=_GROUP, consolidated=False)
    assert opened.sizes["timestamp"] == 4


def test_force_reingest_static_drift_raises_new_store_guidance(tmp_path: Path) -> None:
    store = tmp_path / "force.zarr"
    _write_first(store, _dataset("2024-01-01", static=np.array([1, 2, 3], dtype=np.float32)))

    with pytest.raises(SchemaDriftError, match="Create a new store"):
        _append(
            store,
            _dataset("2024-01-01T02:00", static=np.array([1, 9, 3], dtype=np.float32)),
            force_reingest=True,
        )


def test_staged_preflight_compare_opens_final_target_not_workspace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = tmp_path / "workspace" / "product.zarr"
    final = tmp_path / "final" / "product.zarr"
    final.parent.mkdir(parents=True)
    workspace.parent.mkdir(parents=True)
    captured: list[str] = []

    from firecube.ingestor.runtime.zarr import append as append_module

    original_write = append_module.write_dataset_to_zarr

    def capturing_write(*args: Any, **kwargs: Any) -> None:
        handle = kwargs.get("preflight_compare_zarr_store")
        if handle is not None:
            captured.append(str(handle.target_uri))
        original_write(*args, **kwargs)

    monkeypatch.setattr(append_module, "write_dataset_to_zarr", capturing_write)
    session = make_local_session(str(workspace))
    storage_config = storage_config_from_binding(session._binding)
    strategy = AppendStrategy(
        store=str(workspace),
        store_uri=str(workspace),
        resume_target_uri=None,
        preflight_compare_target_uri=str(final),
        chunk_shape={"timestamp": 2},
        compression=False,
        append_dim="timestamp",
        logger=logging.getLogger("test.staged-preflight"),
        storage_config=storage_config,
        session=session,
        pipeline_write_mode="staged",
        final_target_uri=str(final),
    )

    strategy.write_groups(
        group_to_timestamps={_GROUP: list(pd.date_range("2024-01-01", periods=2, freq="h"))},
        dataset_for_batch=lambda _group, _ts: _dataset(
            "2024-01-01", static=np.array([1, 2, 3], dtype=np.float32)
        ),
        batch_size=2,
    )

    assert captured == [f"file://{final}"]
    assert captured[0] != f"file://{workspace}"


_P = {"scale_factor": np.float32(0.01), "add_offset": np.float32(-5.0), "_FillValue": 65535}
_ILL = {"scale_factor": 1.0, "add_offset": 1e20, "_FillValue": 65535}


def _every(dtype: str) -> np.ndarray:
    info = np.iinfo(dtype)
    return np.arange(info.min, info.max + 1, dtype=np.int64).astype(dtype)


def _packed(start: str, raw: np.ndarray, attrs: dict[str, Any], backing: str) -> xr.Dataset:
    ds = xr.Dataset(
        {"radiance": (("timestamp", "y", "x"), raw.reshape(2, 1, -1), dict(attrs))},
        coords={"timestamp": pd.date_range(start, periods=2, freq="h")},
    )
    return ds.chunk() if backing == "dask" else ds


def _f(scale: float, offset: float | None, fill: int) -> dict[str, Any]:
    out = {"scale_factor": np.float32(scale), "_FillValue": fill}
    return out | ({} if offset is None else {"add_offset": np.float32(offset)})


_NEAR = {**_ILL, "scale_factor": 0.01, "add_offset": 1e13}
_I32 = np.array([16777217, 2147483647, -2147483647, 123456789, 0, -2147483648], dtype=np.int32)
_F32 = np.array([-999.0, 1.5, 2.25, -3.5, 100.0, 0.125], dtype=np.float32)
_RAW_CASES = {  # case: (attrs, appended raw values, one raw value in them, its decoded value)
    "uint16": (_P, _every("uint16"), 1000, 5.0),
    "int16": (_f(0.02, 10.0, -32768), _every("int16"), 100, 12.0),
    "int32": (_f(0.5, None, -2147483648), _I32, 16777217, 8388608.5),
    "uint8": (_f(0.5, 1.0, 255), _every("uint8"), 10, 6.0),
    "int8_unsigned": (
        {"_Unsigned": "true", "_FillValue": np.int8(-1)},
        _every("int8"),
        -128,
        128.0,
    ),
    "float32": ({"_FillValue": np.float32(-999.0)}, _F32, 1.5, 1.5),
    "uint16_near_bound": (_NEAR, _every("uint16"), 1000, 1e13 + 10),
}


@pytest.mark.parametrize("backing", ["numpy", "dask"])
@pytest.mark.parametrize("case", list(_RAW_CASES))
def test_append_of_cf_packed_variable_keeps_raw_dtype_values_attrs_and_decodes(
    tmp_path: Path, case: str, backing: str
) -> None:
    attrs, second, probe, want = _RAW_CASES[case]
    first = second[::-1].copy()
    store = tmp_path / "packed.zarr"
    _write_first(store, _packed("2024-01-01", first, attrs, backing))
    _append(store, _packed("2024-01-01T02:00", second, attrs, backing))
    stored = zarr.open_array(str(store), path=f"{_GROUP}/radiance", mode="r")
    both = np.concatenate([first, second])
    assert stored.dtype == second.dtype
    assert np.array_equal(np.asarray(stored[:]).ravel(), both)
    meta = {k: v for k, v in attrs.items() if k != "_FillValue"}  # fill is stored encoded
    assert {k: stored.attrs.get(k) for k in meta} == meta
    decoded = xr.open_zarr(store, group=_GROUP, consolidated=False)["radiance"].values.ravel()
    assert np.array_equal(np.isnan(decoded), both == attrs["_FillValue"])
    k = len(second) + int(np.flatnonzero(second == probe)[0])
    assert decoded[k] == pytest.approx(want, abs=1e-6)


_S, _C, _U = SchemaDriftError, ConfigurationError, np.uint16


def _mv(value: Any) -> dict[str, Any]:
    return {**_P, "missing_value": value}


_DRIFT_CASES = {  # case: (error, key named, dtype, batch 1 attrs, batch 2 attrs or None, dtype 2)
    "scale_factor": (_S, "scale_factor", _U, _P, {**_P, "scale_factor": 0.02}, None),
    "add_offset": (_S, "add_offset", _U, _P, {**_P, "add_offset": -4.0}, None),
    "fill_value": (_S, "_FillValue", _U, _P, {**_P, "_FillValue": 0}, None),
    "missing_value": (_S, "missing_value", _U, _mv(65535), _mv(0), None),
    "key_added": (_S, "scale_factor", _U, {}, _P, None),
    "key_removed": (_S, "_FillValue", _U, _P, dict(list(_P.items())[:2]), None),
    "dtype": (_S, "dtype", _U, _P, None, np.int16),
    "int64": (_C, None, np.int64, {"scale_factor": 1.0, "add_offset": 0.0}, None, None),
    "ill_conditioned": (_C, None, _U, _ILL, None, None),
    "overflow": (_C, None, _U, {**_ILL, "scale_factor": 1e308, "add_offset": 0.0}, None, None),
    "fill_missing_differ": (_C, None, _U, _mv(65534), None, None),
    "missing_vector": (_C, None, _U, _mv([65534, 65535]), None, None),
    "zero_scale": (_C, None, _U, {**_P, "scale_factor": 0.0}, None, None),
    "packed_float": (_C, None, np.float32, _P, None, None),
}


@pytest.mark.parametrize("backing", ["numpy", "dask"])
@pytest.mark.parametrize("case", list(_DRIFT_CASES))
def test_append_with_drifted_cf_packing_is_refused_and_store_stays_byte_identical(
    tmp_path: Path, case: str, backing: str
) -> None:
    exc, key, dtype1, attrs1, attrs2, dtype2 = _DRIFT_CASES[case]
    attrs2, dtype2 = attrs2 or attrs1, dtype2 or dtype1
    store = tmp_path / "refused.zarr"
    _write_first(store, _packed("2024-01-01", np.arange(6, dtype=dtype1), attrs1, backing))
    before = {p: p.read_bytes() for p in store.rglob("*") if p.is_file()}

    with pytest.raises(exc) as info:
        _append(store, _packed("2024-01-01T02:00", np.arange(6, dtype=dtype2), attrs2, backing))

    assert "radiance" in str(info.value)
    assert key is None or key in str(info.value)
    assert {p: p.read_bytes() for p in store.rglob("*") if p.is_file()} == before
