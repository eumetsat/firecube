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

import json
from dataclasses import replace
from pathlib import Path

import numpy as np
import pytest
import xarray as xr
from click.testing import CliRunner

from firecube.cli.main import cli


def _invoke_multires(args: list[str]):
    return CliRunner().invoke(cli, ["zarr", "multires", *args])


def _store_bytes(root: Path) -> dict[str, bytes]:
    return {str(p.relative_to(root)): p.read_bytes() for p in root.rglob("*") if p.is_file()}


def test_coherence_mismatch_rejected() -> None:
    result = _invoke_multires(
        [
            "--target",
            "file:///tmp/x.zarr",
            "--product-name",
            "pn",
            "--storage-type",
            "s3",
            "--storage-driver",
            "fsspec",
            "--resolutions",
            "2",
        ]
    )

    assert result.exit_code == 2
    assert "incompatible" in result.output
    assert "Traceback" not in result.output


@pytest.mark.parametrize("had_snapshot", [False, True], ids=["creates_none", "refreshes_existing"])
def test_multires_snapshot_never_created_and_refreshed_when_present(
    tmp_path: Path, had_snapshot: bool
) -> None:
    store = tmp_path / "x.zarr"
    ds = xr.Dataset(
        {"FWI": (["lat", "lon"], np.ones((2, 2), dtype="f4"))},
        coords={"lat": [0.0, 1.0], "lon": [0.0, 1.0]},
    )
    ds.to_zarr(store, zarr_format=3, consolidated=had_snapshot)

    flags = ["--storage-type", "local", "--storage-driver", "fsspec", "--resolutions", "45"]
    result = _invoke_multires(["--target", store.as_uri(), "--product-name", "pn", *flags])

    assert result.exit_code == 0, result.output
    snapshot = json.loads((store / "zarr.json").read_text()).get("consolidated_metadata")
    if had_snapshot:
        assert {"multires/45.0deg", "multires/45.0deg/FWI"} <= set(snapshot["metadata"])
    else:
        assert snapshot is None


@pytest.mark.contract
def test_repro_multires_named_group(tmp_path: Path) -> None:
    store = tmp_path / "x.zarr"
    axis = [0.0, 1.0, 2.0, 3.0]
    ds = xr.Dataset(
        {"FWI": (["lat", "lon"], np.ones((4, 4), dtype="f4"))},
        coords={"lat": axis, "lon": axis},
    )
    ds.to_zarr(store, group="g", zarr_format=3, consolidated=False)

    flags = ["--storage-type", "local", "--storage-driver", "fsspec", "--resolutions", "2"]
    result = _invoke_multires(
        ["--target", store.as_uri(), "--product-name", "pn", "-g", "g", *flags]
    )

    assert result.exit_code == 0, result.output
    level = store / "g" / "multires" / "2.0deg"
    assert json.loads((level / "FWI" / "zarr.json").read_text())["shape"] == [2, 2]
    assert not (store / "multires").exists()


@pytest.mark.contract
def test_repro_multires_coordinate_names(tmp_path: Path) -> None:
    store = tmp_path / "x.zarr"
    axis = [0.0, 1.0, 2.0, 3.0]
    ds = xr.Dataset(
        {"FWI": (["latitude", "longitude"], np.ones((4, 4), dtype="f4"))},
        coords={"latitude": axis, "longitude": axis},
    )
    ds.to_zarr(store, zarr_format=3, consolidated=False)

    flags = ["--storage-type", "local", "--storage-driver", "fsspec", "--resolutions", "2"]
    result = _invoke_multires(
        [
            "--target",
            store.as_uri(),
            "--product-name",
            "pn",
            "--lat-name",
            "latitude",
            "--lon-name",
            "longitude",
            *flags,
        ]
    )

    assert result.exit_code == 0, result.output
    level = store / "multires" / "2.0deg"
    assert json.loads((level / "FWI" / "zarr.json").read_text())["shape"] == [2, 2]


@pytest.mark.contract
def test_repro_multires_missing_coordinate(tmp_path: Path) -> None:
    store = tmp_path / "x.zarr"
    ds = xr.Dataset(
        {"FWI": (["latitude", "longitude"], np.ones((2, 2), dtype="f4"))},
        coords={"latitude": [0.0, 1.0], "longitude": [0.0, 1.0]},
    )
    ds.to_zarr(store, zarr_format=3, consolidated=False)

    before = _store_bytes(store)

    flags = ["--storage-type", "local", "--storage-driver", "fsspec", "--resolutions", "45"]
    result = _invoke_multires(["--target", store.as_uri(), "--product-name", "pn", *flags])

    assert result.exit_code == 2, result.output
    assert isinstance(result.exception, SystemExit), repr(result.exception)
    assert "Traceback" not in result.output
    lines = [line for line in result.output.splitlines() if line.strip()]
    assert len(lines) == 1, result.output
    assert "lat" in lines[0]
    assert _store_bytes(store) == before


@pytest.mark.contract
def test_repro_multires_2d_coordinates(tmp_path: Path) -> None:
    store = tmp_path / "x.zarr"
    lat2d = np.array([[0.0, 0.0], [1.0, 1.0]])
    lon2d = np.array([[0.0, 1.0], [0.0, 1.0]])
    ds = xr.Dataset(
        {"FWI": (["y", "x"], np.ones((2, 2), dtype="f4"))},
        coords={"latitude": (["y", "x"], lat2d), "longitude": (["y", "x"], lon2d)},
    )
    ds.to_zarr(store, group="g", zarr_format=3, consolidated=False)
    before = _store_bytes(store)

    flags = ["--storage-type", "local", "--storage-driver", "fsspec", "--resolutions", "45"]
    result = _invoke_multires(
        [
            "--target",
            store.as_uri(),
            "--product-name",
            "pn",
            "-g",
            "g",
            "--lat-name",
            "latitude",
            "--lon-name",
            "longitude",
            *flags,
        ]
    )

    assert result.exit_code == 2, result.output
    assert isinstance(result.exception, SystemExit), repr(result.exception)
    assert "Traceback" not in result.output
    lines = [line for line in result.output.splitlines() if line.strip()]
    assert len(lines) == 1, result.output
    assert "latitude" in lines[0] or "longitude" in lines[0]
    assert "dimension" in lines[0].lower()
    assert _store_bytes(store) == before


def test_case_insensitive_flags_accepted(tmp_path: Path) -> None:
    result = _invoke_multires(
        [
            "--target",
            (tmp_path / "x.zarr").as_uri(),
            "--product-name",
            "pn",
            "--storage-type",
            "LOCAL",
            "--storage-driver",
            "FSSPEC",
            "--resolutions",
            "2",
        ]
    )

    assert "Invalid value" not in result.output


@pytest.mark.contract
def test_multires_refused_under_exclusive_maintenance(tmp_path: Path) -> None:
    from firecube.core.controlplane import ChunkManager
    from tests.helpers.storage import make_test_binding

    store = tmp_path / "x.zarr"
    ds = xr.Dataset(
        {"FWI": (["lat", "lon"], np.ones((4, 4), dtype="f4"))},
        coords={"lat": [0.0, 1.0, 2.0, 3.0], "lon": [0.0, 1.0, 2.0, 3.0]},
    )
    ds.to_zarr(store, zarr_format=3, consolidated=False)
    (tmp_path / "ws").mkdir()
    manager = ChunkManager(binding=make_test_binding(tmp_path, product="x.zarr"), workspace=tmp_path / "ws")  # fmt: skip
    claim = manager.acquire_exclusive_maintenance(product="x.zarr", run_id="other-maintenance-run")
    before = {k: v for k, v in _store_bytes(store).items() if not k.startswith(".firecube")}
    try:
        flags = ["--storage-type", "local", "--storage-driver", "fsspec", "--resolutions", "2"]
        result = _invoke_multires(["--target", store.as_uri(), "--product-name", "x.zarr", *flags])
    finally:
        claim.release()
        manager.close()

    after = {k: v for k, v in _store_bytes(store).items() if not k.startswith(".firecube")}
    assert result.exit_code == 1, result.output
    assert "maintenance" in result.output.lower()
    assert after == before


@pytest.mark.parametrize(("claimed", "other"), [("logical_name", None), ("b.zarr", "a.zarr")])
def test_library_multires_checks_written_store_when_name_differs(
    tmp_path: Path, claimed: str, other: str | None
) -> None:
    from firecube.core.config import StorageConfig
    from firecube.core.controlplane import ChunkManager
    from firecube.core.errors import ProductUnderMaintenanceError
    from firecube.core.storage.uri import StorageUri
    from firecube.core.zarr.multires import MultiresConfig, ZarrMultiresBuilder
    from tests.helpers.storage import make_test_binding, make_test_session

    store = tmp_path / (claimed if other else "physical.zarr")
    xr.Dataset({"v": (["lat", "lon"], np.ones((4, 4)))}, coords={"lat": range(4), "lon": range(4)}).to_zarr(store, zarr_format=3, consolidated=False)  # fmt: skip
    held = make_test_binding(tmp_path, product=store.name)
    held = replace(held, identity=replace(held.identity, product_name=claimed))
    holder = ChunkManager(binding=held, workspace=tmp_path)
    builder = ZarrMultiresBuilder(
        StorageConfig(storage_type="local", storage_driver="fsspec"),
        product_name=other or claimed,
        product_uri=StorageUri.from_local_path(tmp_path / (other or store.name)),
        session=make_test_session(tmp_path, product=other) if other else None,
    )
    claim = holder.acquire_exclusive_maintenance(product=claimed, run_id="other-run")
    with claim, pytest.raises(ProductUnderMaintenanceError):
        builder.build(MultiresConfig(product=claimed, group="", resolutions=[2.0]))
    assert not (store / "multires").exists()
