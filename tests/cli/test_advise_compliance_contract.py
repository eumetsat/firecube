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
from pathlib import Path
from typing import Any

import xarray as xr
import zarr
from click.testing import CliRunner

import firecube.cli.advise as advise_module
from firecube.cli.main import cli
from firecube.core.cf import CFReport
from firecube.core.zarr.io import ZarrIO


def _invoke_compliance(args: list[str]):
    return CliRunner().invoke(cli, ["advise", "compliance", *args])


def test_bare_path_rejected_with_uri_required_error(tmp_local_zarr: Path) -> None:
    result = _invoke_compliance(["--profile", "cf-18", "-p", str(tmp_local_zarr), "-g", "g1"])

    assert result.exit_code == 2
    assert "URI scheme required" in result.output
    assert "Traceback" not in result.output


def test_file_uri_works_without_storage_flags(tmp_local_zarr: Path) -> None:
    result = _invoke_compliance(["--profile", "cf-18", "-p", tmp_local_zarr.as_uri(), "-g", "g1"])

    assert "Missing option" not in result.output
    assert "Traceback" not in result.output
    assert result.exit_code != 2


def test_s3_uri_infers_storage_type(monkeypatch, tmp_path: Path) -> None:
    backing_store = zarr.open_group(str(tmp_path / "remote.zarr"), mode="w")
    backing_store.create_group("g")
    observed: dict[str, str] = {}

    def fake_open_group(self: ZarrIO, uri: Any, mode: str = "r") -> zarr.Group:
        assert mode == "r"
        observed["open_group_uri"] = uri.to_str()
        observed["protocol"] = self._session.product.product_uri.protocol
        observed["driver"] = self._session.driver.driver
        return backing_store

    def fake_open_dataset(self: ZarrIO, uri: Any, group: str = "", **kwargs: Any) -> xr.Dataset:
        observed["open_dataset_uri"] = uri.to_str()
        observed["dataset_group"] = group
        observed["decode_times"] = str(kwargs.get("decode_times"))
        return xr.Dataset()

    def fake_validator(ds: xr.Dataset, *, product: str, group: str) -> CFReport:
        assert isinstance(ds, xr.Dataset)
        observed["validator_product"] = product
        observed["validator_group"] = group
        return CFReport(product=product, group=group)

    monkeypatch.setattr(ZarrIO, "open_group", fake_open_group)
    monkeypatch.setattr(ZarrIO, "open_dataset", fake_open_dataset)
    monkeypatch.setitem(advise_module._COMPLIANCE_PROFILE_VALIDATORS, "cf-18", fake_validator)

    result = _invoke_compliance(
        ["--profile", "cf-18", "-p", "s3://bucket/x.zarr", "-g", "g", "--format", "json"]
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["summary"] == {"errors": 0, "warnings": 0, "info": 0}
    assert observed == {
        "open_group_uri": "s3://bucket/x.zarr",
        "protocol": "s3",
        "driver": "fsspec",
        "open_dataset_uri": "s3://bucket/x.zarr",
        "dataset_group": "g",
        "decode_times": "False",
        "validator_product": "s3://bucket/x.zarr",
        "validator_group": "g",
    }
    assert "Traceback" not in result.output


def test_file_uri_rejects_s3_storage_type(tmp_local_zarr: Path) -> None:
    result = _invoke_compliance(
        [
            "--profile",
            "cf-18",
            "-p",
            tmp_local_zarr.as_uri(),
            "-g",
            "g1",
            "--storage-type",
            "s3",
        ]
    )

    assert result.exit_code == 2
    assert "--storage-type 's3' is incompatible with URI scheme 'file'" in result.output
    assert "Traceback" not in result.output


def test_missing_group_is_clean_click_error(tmp_local_zarr: Path) -> None:
    result = _invoke_compliance(
        ["--profile", "cf-18", "-p", tmp_local_zarr.as_uri(), "-g", "definitely-not-here"]
    )

    assert result.exit_code == 1
    assert "definitely-not-here" in result.output
    assert "Error:" in result.output
    assert "Traceback" not in result.output


def test_missing_local_store_is_clean_click_error(tmp_path: Path) -> None:
    missing_store = tmp_path / "missing.zarr"

    result = CliRunner().invoke(
        cli,
        [
            "advise",
            "compliance",
            "--profile",
            "cf-18",
            "-p",
            missing_store.as_uri(),
            "-g",
            "NORDLIS",
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 1
    assert "Error:" in result.output
    assert "does not exist" in result.output
    assert "Traceback" not in result.output
