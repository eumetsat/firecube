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

from pathlib import Path

import numpy as np
import pytest
import xarray as xr

from firecube.core.config import StorageConfig
from firecube.core.filesystem.store_factory import ZarrStoreHandle, create_zarr_store
from firecube.core.storage.uri import StorageUri
from tests.helpers.storage import make_test_session


def _make_sample_dataset() -> xr.Dataset:
    rng = np.random.default_rng(42)
    return xr.Dataset(
        {
            "temperature": (
                ["timestamp", "lat", "lon"],
                rng.standard_normal((5, 8, 8)).astype(np.float32),
            ),
            "humidity": (
                ["timestamp", "lat", "lon"],
                rng.standard_normal((5, 8, 8)).astype(np.float32),
            ),
        },
        coords={
            "timestamp": np.arange(5),
            "lat": np.linspace(-4.0, 4.0, 8),
            "lon": np.linspace(-4.0, 4.0, 8),
        },
    )


def _write_zarr_with_driver(
    ds: xr.Dataset, store_path: str, driver: str, group: str | None = None
) -> None:
    sc = StorageConfig(storage_type="local", storage_driver=driver)
    handle = create_zarr_store(uri=store_path, storage_config=sc, mode="w")
    ds.to_zarr(**handle.zarr_kwargs(), group=group, mode="w", zarr_format=3)


def _open_zarr_with_driver(store_path: str, driver: str, group: str | None = None) -> xr.Dataset:
    sc = StorageConfig(storage_type="local", storage_driver=driver)
    handle = create_zarr_store(uri=store_path, storage_config=sc, mode="r")
    return xr.open_zarr(**handle.zarr_kwargs(), group=group, zarr_format=3)


@pytest.mark.integration
class TestFsspecDriver:
    def test_write_and_read_back(self, tmp_path: Path) -> None:
        store_path = str(tmp_path / "fsspec_output.zarr")
        ds = _make_sample_dataset()

        _write_zarr_with_driver(ds, store_path, "fsspec")

        assert Path(store_path).exists()
        assert (Path(store_path) / "zarr.json").exists()

        ds_read = _open_zarr_with_driver(store_path, "fsspec")

        assert set(ds_read.dims) == {"timestamp", "lat", "lon"}
        assert set(ds_read.data_vars) == {"temperature", "humidity"}
        assert ds_read.sizes["timestamp"] == 5
        assert ds_read.sizes["lat"] == 8
        assert ds_read.sizes["lon"] == 8
        xr.testing.assert_equal(ds, ds_read)

    def test_write_with_group(self, tmp_path: Path) -> None:
        store_path = str(tmp_path / "fsspec_grouped.zarr")
        ds = _make_sample_dataset()

        _write_zarr_with_driver(ds, store_path, "fsspec", group="Euro")

        ds_read = _open_zarr_with_driver(store_path, "fsspec", group="Euro")
        xr.testing.assert_equal(ds, ds_read)


@pytest.mark.integration
class TestObstoreDriver:
    def test_write_and_read_back(self, tmp_path: Path) -> None:
        store_path = str(tmp_path / "obstore_output.zarr")
        ds = _make_sample_dataset()

        _write_zarr_with_driver(ds, store_path, "obstore")

        assert Path(store_path).exists()

        ds_read = _open_zarr_with_driver(store_path, "obstore")

        assert set(ds_read.dims) == {"timestamp", "lat", "lon"}
        assert set(ds_read.data_vars) == {"temperature", "humidity"}
        assert ds_read.sizes["timestamp"] == 5
        assert ds_read.sizes["lat"] == 8
        assert ds_read.sizes["lon"] == 8
        xr.testing.assert_equal(ds, ds_read)

    def test_write_with_group(self, tmp_path: Path) -> None:
        store_path = str(tmp_path / "obstore_grouped.zarr")
        ds = _make_sample_dataset()

        _write_zarr_with_driver(ds, store_path, "obstore", group="Euro")

        ds_read = _open_zarr_with_driver(store_path, "obstore", group="Euro")
        xr.testing.assert_equal(ds, ds_read)


@pytest.mark.integration
class TestCrossDriverParity:
    def test_structures_match(self, tmp_path: Path) -> None:
        fsspec_path = str(tmp_path / "parity_fsspec.zarr")
        obstore_path = str(tmp_path / "parity_obstore.zarr")
        ds = _make_sample_dataset()

        _write_zarr_with_driver(ds, fsspec_path, "fsspec")
        _write_zarr_with_driver(ds, obstore_path, "obstore")

        ds_fsspec = _open_zarr_with_driver(fsspec_path, "fsspec")
        ds_obstore = _open_zarr_with_driver(obstore_path, "obstore")

        assert dict(ds_fsspec.sizes) == dict(ds_obstore.sizes)
        assert set(ds_fsspec.data_vars) == set(ds_obstore.data_vars)
        for coord in ds_fsspec.coords:
            xr.testing.assert_equal(ds_fsspec.coords[coord], ds_obstore.coords[coord])
        xr.testing.assert_equal(ds_fsspec, ds_obstore)

    def test_grouped_structures_match(self, tmp_path: Path) -> None:
        fsspec_path = str(tmp_path / "group_fsspec.zarr")
        obstore_path = str(tmp_path / "group_obstore.zarr")
        ds = _make_sample_dataset()

        _write_zarr_with_driver(ds, fsspec_path, "fsspec", group="G1")
        _write_zarr_with_driver(ds, obstore_path, "obstore", group="G1")

        ds_fsspec = _open_zarr_with_driver(fsspec_path, "fsspec", group="G1")
        ds_obstore = _open_zarr_with_driver(obstore_path, "obstore", group="G1")

        assert dict(ds_fsspec.sizes) == dict(ds_obstore.sizes)
        xr.testing.assert_equal(ds_fsspec, ds_obstore)

    def test_cross_driver_readback(self, tmp_path: Path) -> None:
        fsspec_path = str(tmp_path / "cross_fsspec.zarr")
        obstore_path = str(tmp_path / "cross_obstore.zarr")
        ds = _make_sample_dataset()

        _write_zarr_with_driver(ds, fsspec_path, "fsspec")
        ds_cross1 = _open_zarr_with_driver(fsspec_path, "obstore")
        xr.testing.assert_equal(ds, ds_cross1)

        _write_zarr_with_driver(ds, obstore_path, "obstore")
        ds_cross2 = _open_zarr_with_driver(obstore_path, "fsspec")
        xr.testing.assert_equal(ds, ds_cross2)


@pytest.mark.integration
class TestCreateZarrStoreContract:
    def test_obstore_returns_handle_with_object_store(self, tmp_path: Path) -> None:
        from zarr.storage import ObjectStore

        store_path = str(tmp_path / "contract_obstore.zarr")
        sc = StorageConfig(storage_type="local", storage_driver="obstore")
        result = create_zarr_store(uri=store_path, storage_config=sc, mode="w")
        assert isinstance(result, ZarrStoreHandle)
        assert isinstance(result.store, ObjectStore)
        assert result.storage_options is None

    def test_obstore_read_mode_is_read_only(self, tmp_path: Path) -> None:
        store_path = str(tmp_path / "contract_ro.zarr")
        sc = StorageConfig(storage_type="local", storage_driver="obstore")
        write_handle = create_zarr_store(uri=store_path, storage_config=sc, mode="w")
        root = xr.Dataset({"existing": (("x",), np.array([1], dtype=np.int16))})
        root.to_zarr(**write_handle.zarr_kwargs(), mode="w", zarr_format=3)

        read_handle = create_zarr_store(uri=store_path, storage_config=sc, mode="r")
        readback = xr.open_zarr(**read_handle.zarr_kwargs(), zarr_format=3)
        try:
            xr.testing.assert_equal(root, readback)
        finally:
            readback.close()

        with pytest.raises(Exception, match=r"read|write|mode|readonly|read-only"):
            xr.Dataset({"blocked": (("x",), np.array([2], dtype=np.int16))}).to_zarr(
                **read_handle.zarr_kwargs(),
                mode="a",
                zarr_format=3,
            )


@pytest.mark.integration
class TestPipelineWithBothDrivers:
    @pytest.mark.parametrize("driver", ["fsspec", "obstore"])
    def test_pipeline_ingest_writes_dataset_with_driver(self, tmp_path: Path, driver: str) -> None:
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        (source_dir / "input.nc").touch()

        target_dir = (tmp_path / f"pipeline_{driver}.zarr").resolve()

        from firecube.ingestor.api import (
            GenericZarrIngestor,
            IngestContext,
            StorageContext,
            register_ingestor,
        )

        @register_ingestor(f"_test_driver_{driver}")
        class _DriverTestIngestor(GenericZarrIngestor):
            PRODUCT_NAME = f"_test_driver_{driver}"
            name = f"_test_driver_{driver}"

            def build_dataset(self, group, items, ctx):
                _ = (group, items, ctx)
                return xr.Dataset(
                    {
                        "val": (
                            ["timestamp", "x"],
                            np.arange(12, dtype=np.float32).reshape(3, 4),
                        )
                    },
                    coords={"timestamp": np.arange(3), "x": np.arange(4)},
                )

        ctx = IngestContext(
            source=str(source_dir),
            target=str(target_dir),
            output_format="zarr",
            storage=StorageContext(
                output=make_test_session(
                    target_dir.parent,
                    product=target_dir.name,
                    driver=driver,  # type: ignore[arg-type]
                )
            ),
            options={
                "pipeline_parallel": False,
                "pipeline_workers": 1,
                "pipeline_batch_size": 1,
                "include_patterns": ["*.nc"],
                "write_mode": "direct",
            },
        )

        ingestor = _DriverTestIngestor()
        result = ingestor.run(ctx)
        assert result.output_path == StorageUri.from_local_path(target_dir).to_str()

        readback = _open_zarr_with_driver(str(target_dir), driver, group="default")
        expected = xr.Dataset(
            {
                "val": (
                    ["timestamp", "x"],
                    np.arange(12, dtype=np.float32).reshape(3, 4),
                )
            },
            coords={"timestamp": np.arange(3), "x": np.arange(4)},
        )
        try:
            assert set(readback.data_vars) == {"val", "firecube_timestamp_state"}
            xr.testing.assert_equal(expected["val"], readback["val"])
            xr.testing.assert_equal(expected["timestamp"], readback["timestamp"])
            xr.testing.assert_equal(expected["x"], readback["x"])
        finally:
            readback.close()
