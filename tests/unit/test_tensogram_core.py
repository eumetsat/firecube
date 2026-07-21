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

"""Unit tests for firecube.core.tensogram — metadata, converter, restore, compat."""

from __future__ import annotations

import pytest


class TestDatasetToGlobalMeta:
    """dataset_to_global_meta produces correct Tensogram global metadata."""

    def test_includes_version_and_source(self):
        """dataset_to_global_meta returns dict with version=3 and source_uri."""
        import numpy as np
        import xarray as xr

        from firecube.core.tensogram.metadata import dataset_to_global_meta

        ds = xr.Dataset({"FWI": (["t"], np.array([1.0, 2.0]))})
        meta = dataset_to_global_meta(ds, source_uri="s3://test/product.zarr", compression="blosc2")
        # tensogram >=0.18 metadata is free-form; the legacy top-level
        # "version" key (removed GlobalMetadata.version) must not be written.
        assert "version" not in meta
        assert meta["firecube"]["source_uri"] == "s3://test/product.zarr"
        assert meta["firecube"]["compression"] == "blosc2"


class TestVariableToDescriptor:
    """variable_to_descriptor produces correct Tensogram data-object descriptors."""

    def test_returns_valid_tensogram_desc(self):
        """variable_to_descriptor returns correct shape/dtype/compression."""
        import numpy as np
        import xarray as xr

        from firecube.core.tensogram.metadata import variable_to_descriptor

        var = xr.Variable(["time", "lat"], np.ones((3, 4), dtype="float32"))
        desc = variable_to_descriptor("FWI", var, compression="blosc2")
        assert desc["type"] == "ntensor"
        assert desc["shape"] == [3, 4]
        assert desc["dtype"] == "float32"
        assert desc["compression"] == "blosc2"

    def test_datetime64_converts_to_float64(self):
        """datetime64 variables are encoded as float64 in the descriptor."""
        import numpy as np
        import xarray as xr

        from firecube.core.tensogram.metadata import variable_to_descriptor

        var = xr.Variable(["time"], np.array(["2024-01-01", "2024-01-02"], dtype="datetime64"))
        desc = variable_to_descriptor("timestamp", var, compression="none")
        assert desc["dtype"] == "float64"
        assert desc["shape"] == [2]


class TestVariableToBaseEntry:
    """variable_to_base_entry produces per-object metadata for meta['base']."""

    def test_returns_name_and_dim_names(self):
        import numpy as np
        import xarray as xr

        from firecube.core.tensogram.metadata import variable_to_base_entry

        var = xr.Variable(["time", "lat"], np.ones((3, 4), dtype="float32"))
        entry = variable_to_base_entry("FWI", var)
        assert entry == {"name": "FWI", "dim_names": ["time", "lat"]}

    def test_accepts_coordinate_variables(self):
        import numpy as np
        import xarray as xr

        from firecube.core.tensogram.metadata import variable_to_base_entry

        coord = xr.Variable(["lat"], np.linspace(-90, 90, 8, dtype="float32"))
        entry = variable_to_base_entry("lat", coord)
        assert entry == {"name": "lat", "dim_names": ["lat"]}


class TestZarrToTgm:
    """zarr_to_tgm converts Zarr stores to Tensogram archives."""

    def test_basic_conversion(self, tmp_path):
        """zarr_to_tgm converts a simple Zarr to a .tgm file."""
        import numpy as np
        import xarray as xr

        from firecube.core.tensogram.converter import zarr_to_tgm

        src = str(tmp_path / "test.zarr")
        tgt = str(tmp_path / "test.tgm")
        ds = xr.Dataset(
            {"FWI": (["t", "y"], np.ones((3, 4), dtype="float32"))},
            coords={"t": [0, 1, 2], "y": [1, 2, 3, 4]},
        )
        ds.to_zarr(src)
        result = zarr_to_tgm(src, tgt)
        assert (tmp_path / "test.tgm").exists()
        assert result["variables"] == ["FWI"]
        assert result["file_size_bytes"] > 0
        assert result["compression"] == "zstd"

    def test_produces_xarray_openable_file(self, tmp_path):
        """The produced .tgm is openable with xr.open_dataset(engine='tensogram')."""
        import numpy as np
        import xarray as xr

        from firecube.core.tensogram.converter import zarr_to_tgm

        src = str(tmp_path / "test.zarr")
        tgt = str(tmp_path / "test.tgm")
        ds = xr.Dataset({"T": (["x"], np.array([1.0, 2.0, 3.0], dtype="float32"))})
        ds.to_zarr(src)
        zarr_to_tgm(src, tgt)
        loaded = xr.open_dataset(tgt, engine="tensogram")
        assert "T" in loaded.data_vars

    def test_raises_file_exists_error(self, tmp_path):
        """zarr_to_tgm raises FileExistsError if target exists and overwrite=False."""
        import numpy as np
        import xarray as xr

        from firecube.core.tensogram.converter import zarr_to_tgm

        src = str(tmp_path / "test.zarr")
        tgt = str(tmp_path / "test.tgm")
        ds = xr.Dataset({"X": (["t"], np.ones(3))})
        ds.to_zarr(src)
        zarr_to_tgm(src, tgt)
        with pytest.raises(FileExistsError):
            zarr_to_tgm(src, tgt)

    def test_raises_value_error_for_time_filter_on_no_time_dim(self, tmp_path):
        """zarr_to_tgm raises ValueError when time filter given but no time dim."""
        import numpy as np
        import xarray as xr

        from firecube.core.tensogram.converter import zarr_to_tgm

        src = str(tmp_path / "notime.zarr")
        tgt = str(tmp_path / "notime.tgm")
        ds = xr.Dataset({"X": (["y", "x"], np.ones((3, 4)))})
        ds.to_zarr(src)
        with pytest.raises(ValueError, match="time dimension"):
            zarr_to_tgm(src, tgt, start_date="2024-01-01")

    def test_time_subsetting_reduces_timestamps(self, tmp_path):
        """zarr_to_tgm with start/end date returns only the specified time range."""
        import numpy as np
        import xarray as xr

        from firecube.core.tensogram.converter import zarr_to_tgm

        src = str(tmp_path / "time.zarr")
        tgt = str(tmp_path / "time.tgm")
        values = np.arange(15, dtype="float32").reshape(5, 3)
        ds = xr.Dataset(
            {"FWI": (["timestamp", "y"], values)},
            coords={"timestamp": [1, 2, 3, 4, 5]},
        )
        ds.to_zarr(src)
        zarr_to_tgm(src, tgt, start_date="2", end_date="4")
        loaded = xr.open_dataset(tgt, engine="tensogram")
        np.testing.assert_array_equal(loaded["timestamp"].values, np.array([2, 3, 4]))
        np.testing.assert_array_equal(loaded["FWI"].values, values[1:4])

    def test_variable_names_preserved_in_v3(self, tmp_path):
        import numpy as np
        import xarray as xr

        from firecube.core.tensogram.converter import zarr_to_tgm

        src = str(tmp_path / "test.zarr")
        tgt = str(tmp_path / "test.tgm")
        ds = xr.Dataset({"FWI": (["t"], np.ones(3, dtype="float32"))})
        ds.to_zarr(src)
        zarr_to_tgm(src, tgt)
        loaded = xr.open_dataset(tgt, engine="tensogram")
        assert "FWI" in loaded.data_vars

    def test_allow_nan_true_accepts_nan_data(self, tmp_path):
        import numpy as np
        import xarray as xr

        from firecube.core.tensogram.converter import zarr_to_tgm

        src = str(tmp_path / "nan.zarr")
        tgt = str(tmp_path / "nan.tgm")
        ds = xr.Dataset({"X": (["t"], np.array([1.0, float("nan"), 3.0], dtype="float32"))})
        ds.to_zarr(src)
        result = zarr_to_tgm(src, tgt, allow_nan=True)
        assert result["allow_nan"] is True
        assert (tmp_path / "nan.tgm").exists()

    def test_allow_nan_false_rejects_nan_data(self, tmp_path):
        import numpy as np
        import xarray as xr

        from firecube.core.tensogram.converter import zarr_to_tgm

        src = str(tmp_path / "nan.zarr")
        tgt = str(tmp_path / "nan.tgm")
        ds = xr.Dataset({"X": (["t"], np.array([1.0, float("nan"), 3.0], dtype="float32"))})
        ds.to_zarr(src)
        with pytest.raises(ValueError, match="strict-NaN check"):
            zarr_to_tgm(src, tgt, allow_nan=False)

    def test_default_compression_is_zstd(self, tmp_path):
        """zarr_to_tgm default compression is zstd."""
        import numpy as np
        import xarray as xr

        from firecube.core.tensogram.converter import zarr_to_tgm

        src = str(tmp_path / "test.zarr")
        tgt = str(tmp_path / "test.tgm")
        ds = xr.Dataset({"X": (["t"], np.ones(3, dtype="float32"))})
        ds.to_zarr(src)
        result = zarr_to_tgm(src, tgt)
        assert result["compression"] == "zstd"

    def test_skipped_string_variables(self, tmp_path):
        """zarr_to_tgm skips string variables and reports them."""
        import numpy as np
        import xarray as xr

        from firecube.core.tensogram.converter import zarr_to_tgm

        src = str(tmp_path / "mixed.zarr")
        tgt = str(tmp_path / "mixed.tgm")
        ds = xr.Dataset(
            {
                "temp": (["t"], np.array([1.0, 2.0], dtype="float32")),
                "name": (["t"], np.array(["a", "b"], dtype="<U1")),
            }
        )
        ds.to_zarr(src)
        result = zarr_to_tgm(src, tgt)
        assert "temp" in result["variables"]
        assert "name" not in result["variables"]
        assert "name" in result["skipped"]

    def test_variables_excludes_skipped(self, tmp_path):
        """zarr_to_tgm return dict 'variables' only includes actually archived vars."""
        import numpy as np
        import xarray as xr

        from firecube.core.tensogram.converter import zarr_to_tgm

        src = str(tmp_path / "skip.zarr")
        tgt = str(tmp_path / "skip.tgm")
        ds = xr.Dataset(
            {
                "val": (["t"], np.ones(3, dtype="float32")),
                "label": (["t"], np.array(["x", "y", "z"])),
            }
        )
        ds.to_zarr(src)
        result = zarr_to_tgm(src, tgt)
        assert result["variables"] == ["val"]
        assert result["skipped"] == ["label"]


class TestTgmToZarr:
    """tgm_to_zarr restores Tensogram archives to Zarr stores."""

    def test_restores_data_variables(self, tmp_path):
        """tgm_to_zarr restores variables and produces a readable Zarr store."""
        import numpy as np
        import xarray as xr

        from firecube.core.tensogram.converter import zarr_to_tgm
        from firecube.core.tensogram.restore import tgm_to_zarr

        src = str(tmp_path / "orig.zarr")
        tgm = str(tmp_path / "arch.tgm")
        rest = str(tmp_path / "rest.zarr")
        ds = xr.Dataset(
            {"FWI": (["t", "y"], np.ones((3, 4), dtype="float32"))},
            coords={"t": [0, 1, 2], "y": [1.0, 2.0, 3.0, 4.0]},
        )
        ds.to_zarr(src)
        zarr_to_tgm(src, tgm)
        tgm_to_zarr(tgm, rest)
        restored = xr.open_zarr(rest)
        assert list(ds.data_vars) == list(restored.data_vars)
        np.testing.assert_array_equal(restored["t"].values, ds["t"].values)
        np.testing.assert_array_equal(restored["y"].values, ds["y"].values)
        np.testing.assert_array_equal(restored["FWI"].values, ds["FWI"].values)

    def test_raises_file_exists_error(self, tmp_path):
        """tgm_to_zarr raises FileExistsError if target exists and overwrite=False."""
        import numpy as np
        import xarray as xr

        from firecube.core.tensogram.converter import zarr_to_tgm
        from firecube.core.tensogram.restore import tgm_to_zarr

        src = str(tmp_path / "orig.zarr")
        tgm = str(tmp_path / "arch.tgm")
        rest = str(tmp_path / "rest.zarr")
        ds = xr.Dataset({"X": (["t"], np.ones(3))})
        ds.to_zarr(src)
        zarr_to_tgm(src, tgm)
        tgm_to_zarr(tgm, rest)
        with pytest.raises(FileExistsError):
            tgm_to_zarr(tgm, rest)


class TestGetArchiveDefaults:
    """get_archive_defaults reads [archive] config section."""

    def test_returns_empty_when_no_section(self):
        from firecube.core.config import get_archive_defaults

        assert get_archive_defaults({}) == {}

    def test_returns_archive_section(self):
        from firecube.core.config import get_archive_defaults

        cfg = {"archive": {"compression": "lz4", "overwrite": True}}
        result = get_archive_defaults(cfg)
        assert result["compression"] == "lz4"
        assert result["overwrite"] is True


class TestRequireTensogram:
    """Import guard raises helpful errors when tensogram is absent."""

    def test_raises_when_not_installed(self, monkeypatch):
        """require_tensogram raises ImportError with install guidance when tensogram absent."""
        from firecube.core.tensogram import _compat

        monkeypatch.setattr(_compat, "HAS_TENSOGRAM", False)
        with pytest.raises(ImportError, match="pip install 'firecube\\[tensogram\\]'"):
            _compat.require_tensogram("test operation")


class TestSchemaHelpers:
    """Tests for archive schema constants and helpers."""

    def test_archive_version_is_v1(self):
        from firecube.core.tensogram.schema import ARCHIVE_VERSION

        assert ARCHIVE_VERSION == "v1"

    def test_make_data_meta_has_required_keys(self):
        from firecube.core.tensogram.schema import make_data_meta

        meta = make_data_meta(
            "F024",
            [{"name": "x", "dim_names": ["x"]}],
            source_uri="s3://bucket/product.zarr",
            compression="zstd",
        )
        assert "version" not in meta
        assert meta["firecube"]["archive_version"] == "v1"
        assert meta["firecube"]["role"] == "data"
        assert meta["firecube"]["group"] == "F024"
        assert meta["firecube"]["compression"] == "zstd"

    def test_make_controlplane_meta_has_required_keys(self):
        from firecube.core.tensogram.schema import make_controlplane_meta

        meta = make_controlplane_meta("my_product")
        assert meta["firecube"]["role"] == "controlplane"
        assert meta["firecube"]["product"] == "my_product"
        assert meta["firecube"]["archive_version"] == "v1"


class TestExtractZarrArrayMetadata:
    """Tests for extract_zarr_array_metadata."""

    def test_extracts_chunk_shapes(self, tmp_path):
        import zarr

        from firecube.core.config import StorageConfig
        from firecube.core.tensogram.metadata import extract_zarr_array_metadata

        store = zarr.open_group(str(tmp_path / "test.zarr"), mode="w")
        g = store.create_group("G")
        g.create_array("var1", shape=(100, 50), chunks=(10, 25), dtype="float32")

        config = StorageConfig(storage_type="local")
        config.target_path = str(tmp_path)  # type: ignore[attr-defined]

        meta = extract_zarr_array_metadata(str(tmp_path / "test.zarr"), "G", storage_config=config)
        assert "var1" in meta
        assert meta["var1"]["chunks"] == (10, 25)
        assert meta["var1"]["dtype"] == "float32"

    def test_handles_no_compressor(self, tmp_path):
        import zarr

        from firecube.core.config import StorageConfig
        from firecube.core.tensogram.metadata import extract_zarr_array_metadata

        store = zarr.open_group(str(tmp_path / "nocomp.zarr"), mode="w")
        store.create_array("raw", shape=(10,), chunks=(5,), dtype="float32", compressor=None)

        config = StorageConfig(storage_type="local")
        config.target_path = str(tmp_path)  # type: ignore[attr-defined]

        meta = extract_zarr_array_metadata(
            str(tmp_path / "nocomp.zarr"), None, storage_config=config
        )
        assert meta["raw"]["compressor"] is None

    def test_variable_to_base_entry_with_zarr_meta(self):
        import numpy as np
        import xarray as xr

        from firecube.core.tensogram.metadata import variable_to_base_entry

        var = xr.Variable(["x", "y"], np.zeros((5, 3)))
        zarr_m = {"chunks": (5, 3), "compressor": {"id": "zstd", "level": 3}, "fill_value": 0.0}
        entry = variable_to_base_entry("temp", var, zarr_meta=zarr_m)
        assert entry["zarr_chunks"] == [5, 3]
        assert entry["zarr_compressor"] == {"id": "zstd", "level": 3}
        assert entry["zarr_fill_value"] == 0.0


class TestControlplaneCodec:
    """Tests for serialize/deserialize_controlplane."""

    def test_deserialize_roundtrip(self):
        import json

        import numpy as np

        from firecube.core.tensogram.controlplane_codec import deserialize_controlplane

        state = {
            "schema_version": "v1",
            "product": "test_product",
            "group_filter": None,
            "spans": [{"key": "span_1", "meta": {"group": "F024"}}],
            "runs": [],
            "claims": [],
        }
        data_bytes = json.dumps(state).encode("utf-8")
        arr = np.frombuffer(data_bytes, dtype=np.uint8).copy()
        result = deserialize_controlplane(arr)

        assert result["product"] == "test_product"
        assert result["schema_version"] == "v1"
        assert len(result["spans"]) == 1
        assert result["spans"][0]["meta"]["group"] == "F024"

    def test_deserialize_empty_state(self):
        import json

        import numpy as np

        from firecube.core.tensogram.controlplane_codec import deserialize_controlplane

        state = {"schema_version": "v1", "product": "P", "spans": [], "runs": [], "claims": []}
        arr = np.frombuffer(json.dumps(state).encode(), dtype=np.uint8).copy()
        result = deserialize_controlplane(arr)
        assert result["product"] == "P"
        assert result["spans"] == []


class TestReconstructCompressor:
    """Tests for _reconstruct_compressor in restore.py."""

    def test_none_config_returns_none(self):
        from firecube.core.tensogram.restore import _reconstruct_compressor

        assert _reconstruct_compressor(None) is None
