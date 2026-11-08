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

"""Integration tests for the Tensogram archive/restore cycle.

Tests the complete workflow: Zarr → .tgm → validate → restore → verify.
All tests use synthetic data (tmp_path fixture, no S3).
"""

from __future__ import annotations

import json
import logging
from typing import Any, cast
from unittest import mock

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.archive.tensogram.converter import zarr_to_tgm


def _local_env(tmp_path: str) -> dict[str, str]:
    return {"FIRECUBE_STORAGE_TYPE": "local", "FIRECUBE_TARGET_PATH": str(tmp_path)}


_LOCAL_STORAGE_FLAGS = ["--storage-type", "local", "--storage-driver", "fsspec"]
_RESTORE_FLAGS = [*_LOCAL_STORAGE_FLAGS, "--product-name", "restored.zarr"]  # label-less archives


def assert_restored_dataset_matches_source(source: xr.Dataset, restored: xr.Dataset) -> None:
    assert set(restored.data_vars) == set(source.data_vars)
    np.testing.assert_array_equal(restored["timestamp"].values, source["timestamp"].values)
    np.testing.assert_allclose(restored["latitude"].values, source["lat"].values)
    np.testing.assert_allclose(restored["longitude"].values, source["lon"].values)
    for name in source.data_vars:
        assert restored[name].dims == ("timestamp", "latitude", "longitude")
        np.testing.assert_allclose(restored[name].values, source[name].values)
        assert restored[name].attrs["attrs"] == source[name].attrs


def _make_test_dataset(n_time: int = 10, n_lat: int = 8, n_lon: int = 6) -> xr.Dataset:
    """Create a realistic multi-variable Zarr-like dataset with CF attrs."""
    times = np.arange(n_time)
    lats = np.linspace(-90, 90, n_lat).astype("float32")
    lons = np.linspace(-180, 180, n_lon).astype("float32")
    rng = np.random.default_rng(42)
    fwi = rng.random((n_time, n_lat, n_lon)).astype("float32")
    dsr = rng.random((n_time, n_lat, n_lon)).astype("float32")

    return xr.Dataset(
        {
            "FWI": (
                ["timestamp", "lat", "lon"],
                fwi,
                {"units": "1", "standard_name": "fire_weather_index"},
            ),
            "DSR": (["timestamp", "lat", "lon"], dsr, {"units": "1"}),
        },
        coords={"timestamp": times, "lat": lats, "lon": lons},
        attrs={"Conventions": "CF-1.8", "title": "Test Fire Weather Index"},
    )


def _make_datetime_dataset(n_time: int = 10, n_lat: int = 8, n_lon: int = 6) -> xr.Dataset:
    """Dataset with datetime64 timestamps for time-range subsetting tests."""
    times = pd.date_range("2024-01-01", periods=n_time, freq="D")
    lats = np.linspace(-90, 90, n_lat).astype("float32")
    lons = np.linspace(-180, 180, n_lon).astype("float32")
    rng = np.random.default_rng(42)
    fwi = rng.random((n_time, n_lat, n_lon)).astype("float32")

    return xr.Dataset(
        {
            "FWI": (
                ["timestamp", "lat", "lon"],
                fwi,
                {"units": "1", "standard_name": "fire_weather_index"},
            ),
        },
        coords={"timestamp": times, "lat": lats, "lon": lons},
        attrs={"Conventions": "CF-1.8", "title": "Test FWI datetime"},
    )


@pytest.mark.integration
def test_full_archive_restore_cycle(tmp_path):
    runner = CliRunner()
    env = _local_env(tmp_path)
    src = str(tmp_path / "product.zarr")
    src_uri = f"file://{src}"
    tgm = str(tmp_path / "product.tgm")
    rest = str(tmp_path / "restored.zarr")
    rest_uri = f"file://{rest}"

    ds = _make_test_dataset()
    ds.to_zarr(src)

    tgm_uri = f"file://{tgm}"
    result = runner.invoke(
        cli,
        ["archive", "create", "--target", src_uri, "--archive", tgm_uri, *_LOCAL_STORAGE_FLAGS],
        env=env,
    )
    assert result.exit_code == 0, f"archive create failed: {result.output}"
    assert (tmp_path / "product.tgm").exists()
    assert "Archive created" in result.output
    assert "FWI" in result.output

    result = runner.invoke(cli, ["archive", "validate", "--archive", tgm_uri], env=env)
    assert result.exit_code == 0, f"validate failed: {result.output}"
    assert "VALID" in result.output

    result = runner.invoke(cli, ["archive", "info", "--archive", tgm_uri], env=env)
    assert result.exit_code == 0
    assert "FWI" in result.output
    assert "DSR" in result.output

    result = runner.invoke(cli, ["archive", "list", "--archive", tgm_uri], env=env)
    assert result.exit_code == 0
    assert "FWI" in result.output
    assert "DSR" in result.output

    result = runner.invoke(
        cli,
        ["archive", "restore", "--archive", tgm_uri, "--target", rest_uri, *_RESTORE_FLAGS],
        env=env,
    )
    assert result.exit_code == 0, f"restore failed: {result.output}"
    assert (tmp_path / "restored.zarr").exists()

    restored = xr.open_zarr(rest)
    try:
        assert_restored_dataset_matches_source(ds, restored)
    finally:
        restored.close()


@pytest.mark.integration
def test_archive_with_product_group(tmp_path):
    """Archive a specific product group from a multi-group Zarr."""
    src = str(tmp_path / "product.zarr")
    tgm = str(tmp_path / "group_archive.tgm")

    # Create Zarr with group "F024"
    ds = _make_test_dataset(n_time=5)
    ds.to_zarr(src, group="F024")

    # Archive just the F024 group
    result = zarr_to_tgm(src, tgm, group="F024")
    assert result["group"] == "F024"
    assert "FWI" in result["variables"]

    # Verify .tgm is xarray-openable
    loaded = xr.open_dataset(tgm, engine="tensogram")
    assert "FWI" in loaded.data_vars
    loaded.close()


@pytest.mark.integration
def test_archive_time_range_subsetting(tmp_path):
    runner = CliRunner()
    env = _local_env(tmp_path)
    src = str(tmp_path / "product.zarr")
    src_uri = f"file://{src}"
    tgm = str(tmp_path / "subset.tgm")

    ds = _make_datetime_dataset(n_time=10)
    ds.to_zarr(src)

    result = runner.invoke(
        cli,
        [
            "archive",
            "create",
            "--target",
            src_uri,
            "--archive",
            f"file://{tgm}",
            *_LOCAL_STORAGE_FLAGS,
            "--start-date",
            "2024-01-04",
            "--end-date",
            "2024-01-07",
        ],
        env=env,
    )
    assert result.exit_code == 0, result.output

    loaded = xr.open_dataset(tgm, engine="tensogram")
    try:
        assert loaded.sizes["timestamp"] == 4
        expected_times = pd.date_range("2024-01-04", "2024-01-07", freq="D")
        actual_times = pd.to_datetime(loaded["timestamp"].values.astype("int64"))
        np.testing.assert_array_equal(actual_times.values, expected_times.values)
        np.testing.assert_allclose(
            loaded["FWI"].values,
            ds.sel(timestamp=expected_times)["FWI"].values,
        )
    finally:
        loaded.close()


@pytest.mark.integration
def test_archive_variable_selection(tmp_path):
    runner = CliRunner()
    env = _local_env(tmp_path)
    src = str(tmp_path / "product.zarr")
    src_uri = f"file://{src}"
    tgm = str(tmp_path / "var_subset.tgm")

    ds = _make_test_dataset()
    ds.to_zarr(src)

    result = runner.invoke(
        cli,
        [
            "archive",
            "create",
            "--target",
            src_uri,
            "--archive",
            f"file://{tgm}",
            *_LOCAL_STORAGE_FLAGS,
            "--variables",
            "FWI",
        ],
        env=env,
    )
    assert result.exit_code == 0, result.output

    loaded = xr.open_dataset(tgm, engine="tensogram")
    assert "FWI" in loaded.data_vars
    assert "DSR" not in loaded.data_vars
    loaded.close()


@pytest.mark.integration
def test_multigroup_archive_restore_cycle(tmp_path):
    import zarr

    src = str(tmp_path / "multigroup.zarr")
    src_uri = f"file://{src}"
    store = zarr.open_group(src, mode="w")
    g1 = store.create_group("GroupA")
    g1.create_array("temp", shape=(20, 10), chunks=(7, 5), dtype="float32")
    g2 = store.create_group("GroupB")
    g2.create_array("wind", shape=(15, 8), chunks=(5, 4), dtype="float64")

    tgm = str(tmp_path / "multi.tgm")
    rest = str(tmp_path / "restored.zarr")
    rest_uri = f"file://{rest}"

    runner = CliRunner()
    env = _local_env(tmp_path)
    tgm_uri = f"file://{tgm}"
    result = runner.invoke(
        cli,
        ["archive", "create", "--target", src_uri, "--archive", tgm_uri, *_LOCAL_STORAGE_FLAGS],
        env=env,
    )
    assert result.exit_code == 0, result.output
    assert "GroupA" in result.output
    assert "GroupB" in result.output

    result = runner.invoke(
        cli,
        ["archive", "restore", "--archive", tgm_uri, "--target", rest_uri, *_RESTORE_FLAGS],
        env=env,
    )
    assert result.exit_code == 0, result.output

    restored_store = zarr.open_group(rest, mode="r")
    restored_members = cast(dict[str, Any], dict(restored_store.members()))
    assert "GroupA" in restored_members
    assert "GroupB" in restored_members
    restored_group_a = zarr.open_group(rest, mode="r", path="GroupA")
    restored_group_b = zarr.open_group(rest, mode="r", path="GroupB")
    assert cast(Any, restored_group_a["temp"]).chunks == (7, 5)
    assert cast(Any, restored_group_b["wind"]).chunks == (5, 4)


@pytest.mark.integration
def test_group_filter_archives_single_group(tmp_path):
    import tensogram
    import zarr

    src = str(tmp_path / "multigroup.zarr")
    src_uri = f"file://{src}"
    store = zarr.open_group(src, mode="w")
    g1 = store.create_group("GroupA")
    g1.create_array("temp", shape=(10, 5), chunks=(5, 5), dtype="float32")
    g2 = store.create_group("GroupB")
    g2.create_array("wind", shape=(8, 4), chunks=(4, 2), dtype="float64")

    tgm = str(tmp_path / "single.tgm")
    runner = CliRunner()
    env = _local_env(tmp_path)
    result = runner.invoke(
        cli,
        [
            "archive",
            "create",
            "--target",
            src_uri,
            "--archive",
            f"file://{tgm}",
            *_LOCAL_STORAGE_FLAGS,
            "--group",
            "GroupA",
        ],
        env=env,
    )
    assert result.exit_code == 0, result.output

    with cast(Any, tensogram).TensogramFile.open(tgm) as f:
        groups = []
        for i in range(f.message_count()):
            meta = f.file_decode_metadata(i)
            extra = (meta.extra or {}) if hasattr(meta, "extra") else {}
            fc = extra.get("firecube", {})
            if fc.get("group"):
                groups.append(fc["group"])

    assert "GroupA" in groups
    assert "GroupB" not in groups


@pytest.mark.integration
def test_chunk_fidelity_preserved_on_restore(tmp_path):
    import zarr

    src = str(tmp_path / "chunked.zarr")
    src_uri = f"file://{src}"
    ds = xr.Dataset(
        {
            "var1": (("t", "y", "x"), np.zeros((100, 50, 20), dtype="float32")),
            "var2": (("t", "y"), np.zeros((100, 50), dtype="float64")),
        }
    )
    ds.to_zarr(
        src,
        group="Data",
        encoding={"var1": {"chunks": (13, 7, 5)}, "var2": {"chunks": (11, 9)}},
    )

    tgm = str(tmp_path / "chunked.tgm")
    rest = str(tmp_path / "restored.zarr")
    rest_uri = f"file://{rest}"

    runner = CliRunner()
    env = _local_env(tmp_path)
    tgm_uri = f"file://{tgm}"
    create_result = runner.invoke(
        cli,
        ["archive", "create", "--target", src_uri, "--archive", tgm_uri, *_LOCAL_STORAGE_FLAGS],
        env=env,
    )
    assert create_result.exit_code == 0, create_result.output

    restore_result = runner.invoke(
        cli,
        ["archive", "restore", "--archive", tgm_uri, "--target", rest_uri, *_RESTORE_FLAGS],
        env=env,
    )
    assert restore_result.exit_code == 0, restore_result.output

    restored = zarr.open_group(rest, mode="r", path="Data")
    assert cast(Any, restored["var1"]).chunks == (13, 7, 5)
    assert cast(Any, restored["var2"]).chunks == (11, 9)


def _archive_cli(tmp_path, *args):
    flags = _RESTORE_FLAGS if args[0] == "restore" else _LOCAL_STORAGE_FLAGS
    result = CliRunner().invoke(cli, ["archive", *args, *flags], env=_local_env(tmp_path))
    assert result.exit_code == 0, result.output


@pytest.mark.integration
def test_restore_creates_no_root_snapshot(tmp_path):
    src, rest = tmp_path / "src.zarr", tmp_path / "rest.zarr"
    _make_test_dataset().to_zarr(src, consolidated=False)
    tgm = (tmp_path / "a.tgm").as_uri()
    _archive_cli(tmp_path, "create", "--target", src.as_uri(), "--archive", tgm)
    _archive_cli(tmp_path, "restore", "--archive", tgm, "--target", rest.as_uri())

    assert json.loads((rest / "zarr.json").read_text()).get("consolidated_metadata") is None


@pytest.mark.integration
def test_remote_grouped_restore_refreshes_existing_root_snapshot(tmp_path):
    """Remote branch simulated: is_remote_target is made true over a real local store."""
    ds = _make_test_dataset()
    src, rest = tmp_path / "src.zarr", tmp_path / "rest.zarr"
    ds.to_zarr(src, group="g1", mode="a", consolidated=False)
    ds.rename(FWI="OLD").to_zarr(rest, group="g1", mode="a")  # root snapshot lists g1/OLD
    tgm = (tmp_path / "a.tgm").as_uri()
    _archive_cli(tmp_path, "create", "--target", src.as_uri(), "--archive", tgm, "--group", "g1")
    with mock.patch(
        "firecube.core.archive.tensogram.restore.is_remote_target", lambda target: True
    ):
        _archive_cli(tmp_path, "restore", "--archive", tgm, "--target", rest.as_uri())

    listed = json.loads((rest / "zarr.json").read_text())["consolidated_metadata"]["metadata"]
    assert "g1/FWI" in listed
    assert "g1/OLD" not in listed
    restored = xr.open_zarr(rest, group="g1", consolidated=False)
    np.testing.assert_allclose(restored["FWI"].values, ds["FWI"].values)
    restored.close()


@pytest.mark.integration
def test_archive_without_controlplane_warns_and_succeeds(
    tmp_path,
    caplog: pytest.LogCaptureFixture,
):
    import zarr

    caplog.set_level(logging.WARNING, logger="firecube.core.archive.tensogram.converter")
    src = str(tmp_path / "nocp.zarr")
    src_uri = f"file://{src}"
    store = zarr.open_group(src, mode="w")
    g = store.create_group("Data")
    g.create_array("x", shape=(5,), chunks=(5,), dtype="float32")

    tgm = str(tmp_path / "nocp.tgm")
    runner = CliRunner()
    env = _local_env(tmp_path)
    result = runner.invoke(
        cli,
        [
            "archive",
            "create",
            "--target",
            src_uri,
            "--archive",
            f"file://{tgm}",
            *_LOCAL_STORAGE_FLAGS,
        ],
        env=env,
    )

    assert result.exit_code == 0, result.output
    assert (tmp_path / "nocp.tgm").exists()
    assert any(
        "No .firecube/ found" in record.getMessage()
        and "archiving data only" in record.getMessage()
        for record in caplog.records
    )

    info = runner.invoke(cli, ["archive", "info", "--archive", f"file://{tgm}"], env=env)
    assert info.exit_code == 0, info.output
    assert "Control-plane:  not present" in info.output


@pytest.mark.integration
def test_archive_info_shows_groups(tmp_path):
    src = str(tmp_path / "mg.zarr")
    src_uri = f"file://{src}"
    xr.Dataset({"alpha_data": (("t", "x"), np.zeros((5, 3), dtype="float32"))}).to_zarr(
        src,
        group="Alpha",
    )
    xr.Dataset({"beta_data": (("t", "x"), np.zeros((4, 2), dtype="float64"))}).to_zarr(
        src,
        group="Beta",
        mode="a",
    )

    tgm = str(tmp_path / "mg.tgm")
    runner = CliRunner()
    env = _local_env(tmp_path)
    tgm_uri = f"file://{tgm}"
    create_result = runner.invoke(
        cli,
        ["archive", "create", "--target", src_uri, "--archive", tgm_uri, *_LOCAL_STORAGE_FLAGS],
        env=env,
    )
    assert create_result.exit_code == 0, create_result.output

    result = runner.invoke(cli, ["archive", "info", "--archive", tgm_uri], env=env)
    assert result.exit_code == 0, result.output
    assert "Alpha" in result.output
    assert "Beta" in result.output


@pytest.mark.integration
def test_repro_tgm_restore_keeps_compressor(tmp_path):
    import zarr
    from zarr.codecs import GzipCodec

    src, rest = tmp_path / "src.zarr", tmp_path / "rest.zarr"
    data = zarr.open_group(src, mode="w").create_group("Data")
    data.create_array(
        "v",
        data=np.arange(40, dtype="float32").reshape(8, 5),
        dimension_names=("t", "x"),
        compressors=GzipCodec(level=3),
    )
    tgm = (tmp_path / "c.tgm").as_uri()
    _archive_cli(tmp_path, "create", "--target", src.as_uri(), "--archive", tgm)
    _archive_cli(tmp_path, "restore", "--archive", tgm, "--target", rest.as_uri())

    source_codecs = cast(Any, zarr.open_group(src, mode="r", path="Data")["v"]).compressors
    restored_codecs = cast(Any, zarr.open_group(rest, mode="r", path="Data")["v"]).compressors
    assert [c.to_dict() for c in source_codecs] == [{"name": "gzip", "configuration": {"level": 3}}]
    assert [c.to_dict() for c in restored_codecs] == [c.to_dict() for c in source_codecs]


@pytest.mark.integration
def test_repro_tgm_root_label(tmp_path):
    src = tmp_path / "root.zarr"
    _make_test_dataset().to_zarr(src)
    tgm = (tmp_path / "root.tgm").as_uri()
    runner = CliRunner()
    env = _local_env(tmp_path)

    created = runner.invoke(
        cli,
        ["archive", "create", "--target", src.as_uri(), "--archive", tgm, *_LOCAL_STORAGE_FLAGS],
        env=env,
    )
    assert created.exit_code == 0, created.output
    groups_line = next(ln for ln in created.output.splitlines() if ln.strip().startswith("Groups:"))
    assert "(root)" in groups_line

    listed = runner.invoke(cli, ["archive", "list", "--archive", tgm], env=env)
    assert listed.exit_code == 0, listed.output
    assert "(root)" in listed.output
    assert "Group: message 0" not in listed.output


@pytest.mark.integration
def test_repro_tgm_explicit_root_restore_keeps_control_plane(tmp_path):
    from firecube.core.archive.tensogram.restore import tgm_to_zarr

    src, tgm, rest = tmp_path / "src.zarr", tmp_path / "root.tgm", tmp_path / "rest.zarr"
    _make_test_dataset().to_zarr(src)
    zarr_to_tgm(str(src), str(tgm), group="/")
    claim = rest / ".firecube" / "claims" / "held.json"
    claim.parent.mkdir(parents=True)
    claim.write_text("{}")

    tgm_to_zarr(str(tgm), str(rest))

    assert claim.exists()
    assert (rest / "zarr.json").exists()
