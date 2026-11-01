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

import json

import numpy as np
import pytest
import zarr

from firecube.core.config import StorageConfig

_LOCAL = StorageConfig(storage_type="local")


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

    @pytest.mark.parametrize("snapshot", ["root", "G"])
    def test_archives_live_variable_size_under_a_stale_snapshot(self, tmp_path, snapshot):
        import xarray as xr

        from firecube.core.tensogram.converter import zarr_to_tgm

        path = _store_with_stale_snapshot(tmp_path, snapshot)
        zarr_to_tgm(str(path), str(tmp_path / "o.tgm"), group="G", storage_config=_LOCAL)
        assert xr.open_dataset(str(tmp_path / "o.tgm"), engine="tensogram")["v"].size == 6

    def test_archives_a_group_created_after_the_root_snapshot(self, tmp_path):
        from firecube.core.tensogram.converter import zarr_to_tgm

        path = str(tmp_path / "s.zarr")
        zarr.open_group(path, mode="w", zarr_format=3).create_group("old")
        zarr.consolidate_metadata(path)
        live = zarr.open_group(path, mode="r+", use_consolidated=False)
        live.create_group("new").create_array("v", data=np.zeros(2), dimension_names=["v"])
        assert "new" in zarr_to_tgm(path, str(tmp_path / "o.tgm"), storage_config=_LOCAL)["groups"]

    def test_archive_lists_groups_variables_and_coordinates_in_name_order(self, tmp_path, subtests):
        import xarray as xr

        from firecube.core.tensogram.converter import zarr_to_tgm
        from firecube.core.tensogram.inspection import open_tgm_listing, tgm_info

        src, tgt = str(tmp_path / "s.zarr"), str(tmp_path / "o.tgm")
        names = ["temp", "alpha", "wind", "cloud", "rain"]
        for g in ["Zeta", "Alpha", "Mid", "Beta"]:
            data = {v: (("x", "y", "t"), np.zeros((2, 2, 2))) for v in names}
            coords = {"x": [0, 1], "y": [0, 1], "t": [0, 1]}
            xr.Dataset(data, coords=coords).to_zarr(src, group=g, mode="a")
        zarr_to_tgm(src, tgt, storage_config=_LOCAL)
        with open_tgm_listing(tgt, operation="test") as listing:
            seen = {m.group_name: [o.name for o in m.objects()] for m in listing.messages()}
        details = tgm_info(tgt, operation="test")["group_details"]
        info = {d["group"]: list(d["variables"]) for d in details}
        with subtests.test(msg="group_order"):
            assert list(seen) == ["Alpha", "Beta", "Mid", "Zeta"]
        for group in ["Alpha", "Beta", "Mid", "Zeta"]:
            with subtests.test(msg=f"{group}_objects"):
                assert seen[group] == [*sorted(names), "t", "x", "y"]
            with subtests.test(msg=f"{group}_variables"):
                assert info[group] == sorted(names)

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

    def test_refuses_a_360_day_time_coordinate_instead_of_dropping_it(self, tmp_path):
        import xarray as xr

        from firecube.core.tensogram.converter import CalendarArchiveError, zarr_to_tgm

        src = str(tmp_path / "cal.zarr")
        times = xr.date_range(
            "2020-01-01", periods=5, freq="D", calendar="360_day", use_cftime=True
        )
        xr.Dataset({"X": (["time"], np.arange(5.0))}, coords={"time": times}).to_zarr(src)

        with pytest.raises(CalendarArchiveError) as excinfo:
            zarr_to_tgm(src, str(tmp_path / "cal.tgm"))

        assert (excinfo.value.coordinate, excinfo.value.calendar) == ("time", "360_day")

    @pytest.mark.integration
    def test_repro_tgm_nat_fill(self, tmp_path):
        import tensogram

        from firecube.core.tensogram.converter import zarr_to_tgm

        src, tgt = str(tmp_path / "nat.zarr"), str(tmp_path / "nat.tgm")
        group = zarr.open_group(src, mode="w", zarr_format=3)
        time = group.create_array(
            "time", shape=(3,), dtype="datetime64[s]", dimension_names=["time"]
        )
        time[:] = np.array(["2024-01-01", "NaT", "2024-01-03"], dtype="datetime64[s]")
        group.create_array("X", data=np.ones(3, "float32"), dimension_names=["time"])

        zarr_to_tgm(src, tgt)

        with tensogram.TensogramFile.open(tgt) as f:  # pyright: ignore[reportAttributeAccessIssue]
            entries = f.file_decode_metadata(0).base
            payloads = f.decode_message(0).objects
        index = [e["name"] for e in entries].index("time")
        assert entries[index]["zarr_fill_value"] == payloads[index][1][1]

    @pytest.mark.integration
    def test_repro_tgm_no_partial_file(self, tmp_path):
        import xarray as xr

        from firecube.core.tensogram.converter import zarr_to_tgm

        src, tgt = str(tmp_path / "notime.zarr"), tmp_path / "notime.tgm"
        xr.Dataset({"X": (["y", "x"], np.ones((3, 4)))}).to_zarr(src)

        with pytest.raises(ValueError, match="time dimension"):
            zarr_to_tgm(src, str(tgt), start_date="2024-01-01")

        assert not tgt.exists()


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


def _archive(tmp_path, *, controlplane=False):
    import numpy as np
    import xarray as xr

    from firecube.core.tensogram.converter import zarr_to_tgm

    src = tmp_path / "src.zarr"
    xr.Dataset({"FWI": (["t"], np.ones(3, dtype="float32"))}).to_zarr(str(src))
    if controlplane:
        (src / ".firecube").mkdir()
    zarr_to_tgm(str(src), str(tmp_path / "a.tgm"))
    return str(tmp_path / "a.tgm")


class TestTgmInspection:
    @pytest.mark.integration
    @pytest.mark.parametrize("controlplane", [True, False])
    def test_info_reports_variables_and_controlplane_presence(self, tmp_path, controlplane):
        from firecube.core.tensogram.inspection import tgm_info

        path = _archive(tmp_path, controlplane=controlplane)
        info = tgm_info(path, operation="firecube archive info")
        size = (tmp_path / "a.tgm").stat().st_size
        keys = ["path", "format", "size_bytes", "groups", "has_controlplane", "group_details"]
        assert list(info) == keys
        assert [info[k] for k in keys[:5]] == [path, "v1", size, [""], controlplane]
        assert list(info["group_details"][0]["variables"]) == ["FWI"]

    @pytest.mark.integration
    def test_info_refuses_unsupported_version_naming_the_path(self, tmp_path):
        import tensogram

        from firecube.core.tensogram.inspection import TensogramArchiveError, tgm_info

        path = str(tmp_path / "old.tgm")
        with tensogram.TensogramFile.create(path) as f:  # pyright: ignore[reportAttributeAccessIssue]
            f.append({"firecube": {"role": "data", "archive_version": "v0"}}, [])
        with pytest.raises(TensogramArchiveError, match="Unsupported") as exc:
            tgm_info(path, operation="firecube archive info")
        assert exc.value.path == path

    @pytest.mark.integration
    def test_listing_yields_controlplane_and_data_messages_with_objects(self, tmp_path):
        from firecube.core.tensogram.inspection import open_tgm_listing

        path = _archive(tmp_path, controlplane=True)
        with open_tgm_listing(path, operation="firecube archive list") as listing:
            messages = list(listing.messages())
            assert listing.message_count == len(messages) == 2
            data, controlplane = messages
            assert [o.name for o in data.objects()] == ["FWI"]
            assert (data.is_controlplane, controlplane.is_controlplane) == (False, True)
            assert controlplane.objects() == ()

    @pytest.mark.integration
    def test_validate_flags_a_truncated_archive_but_not_an_intact_one(self, tmp_path):
        from firecube.core.tensogram.inspection import validate_tgm

        path = _archive(tmp_path, controlplane=True)
        intact = validate_tgm(path, operation="firecube archive validate")
        assert (intact.message_count, intact.issues) == (2, ())
        (tmp_path / "cut.tgm").write_bytes((tmp_path / "a.tgm").read_bytes()[:200])
        cut = validate_tgm(str(tmp_path / "cut.tgm"), operation="firecube archive validate")
        assert cut.issues

    @pytest.mark.integration
    def test_repro_tgm_validate_no_messages(self, tmp_path):
        import tensogram

        from firecube.core.tensogram.inspection import validate_tgm

        path = str(tmp_path / "empty.tgm")
        with tensogram.TensogramFile.create(path):  # pyright: ignore[reportAttributeAccessIssue]
            pass

        result = validate_tgm(path, operation="firecube archive validate")

        assert result.message_count == 0
        assert result.issues

    @pytest.mark.unit
    @pytest.mark.parametrize("name", ["tgm_info", "open_tgm_listing", "validate_tgm"])
    def test_missing_extras_names_the_operation(self, monkeypatch, name):
        from firecube.core.tensogram import _compat, inspection

        monkeypatch.setattr(_compat, "HAS_TENSOGRAM", False)
        call = getattr(inspection, name)
        with pytest.raises(ImportError) as exc:
            result = call("missing.tgm", operation="firecube archive probe")
            if name == "open_tgm_listing":
                result.__enter__()
        assert str(exc.value).startswith("firecube archive probe")


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


def _store_with_stale_snapshot(tmp_path, snapshot, *, group=True, reencode=False):
    """A store whose snapshot (at ``snapshot``: "root" or "G") predates v growing 4 to 6."""
    path = tmp_path / "s.zarr"
    root = zarr.open_group(str(path), mode="w", zarr_format=3)
    parent = root.create_group("G") if group else root
    parent.create_array("v", data=np.arange(4, dtype="float32"), chunks=(2,), dimension_names=["v"])
    zarr.consolidate_metadata(str(path))
    if snapshot == "G":
        zarr.consolidate_metadata(str(path / "G"))
    name = "G/v" if group else "v"
    live = zarr.open_array(str(path / name), mode="r+")
    live.resize((6,))
    live[4:] = [4, 5]
    if reencode:
        doc = path / name / "zarr.json"
        meta = json.loads(doc.read_text())
        meta["fill_value"] = 7.0
        meta["chunk_grid"]["configuration"]["chunk_shape"] = [3]
        doc.write_text(json.dumps(meta))
    return path


class TestExtractZarrArrayMetadata:
    """Tests for extract_zarr_array_metadata."""

    @pytest.mark.parametrize(("snap", "grp"), [("root", None), ("G", "G")])
    def test_reads_live_chunks_and_fill_value_under_a_stale_snapshot(self, tmp_path, snap, grp):
        from firecube.core.tensogram.metadata import extract_zarr_array_metadata

        path = _store_with_stale_snapshot(tmp_path, snap, group=grp is not None, reencode=True)
        meta = extract_zarr_array_metadata(str(path), grp, storage_config=_LOCAL)
        assert (meta["v"]["chunks"], meta["v"]["fill_value"]) == ((3,), 7.0)

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
