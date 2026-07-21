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

import json
import subprocess

import pytest
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli

_LOCAL_STORAGE_FLAGS = ["--storage-type", "local", "--storage-driver", "fsspec"]


def _make_array_repro(tmp_path):
    product_dir = tmp_path / "cf_array_repro.zarr"
    root = zarr.open_group(product_dir, mode="w")
    root.create_array("raw_data", shape=(10,), dtype="float32")
    return product_dir.as_uri()


def _run_advise_compliance(product_uri: str, group_path: str):
    return subprocess.run(
        [
            "uv",
            "run",
            "firecube",
            "advise",
            "compliance",
            "--profile",
            "cf-18",
            "--product",
            product_uri,
            "--group",
            group_path,
            *_LOCAL_STORAGE_FLAGS,
            "--format",
            "json",
        ],
        capture_output=True,
        text=True,
    )


@pytest.fixture
def clean_cube(tmp_path):
    from tests.fixtures.cf_dataset_fixtures import make_cf_compliant_dataset

    ds = make_cf_compliant_dataset(time_dim="time")
    p = tmp_path / "cf_clean.zarr"
    ds.to_zarr(p, mode="w", zarr_format=3, consolidated=False)
    return p.as_uri()


@pytest.fixture
def broken_cube(tmp_path):
    from tests.fixtures.cf_dataset_fixtures import make_broken_dataset

    ds = make_broken_dataset("conventions")
    p = tmp_path / "cf_broken.zarr"
    ds.to_zarr(p, mode="w", zarr_format=3, consolidated=False)
    return p.as_uri()


@pytest.mark.integration
def test_advise_compliance_cf18_clean_zero_errors(clean_cube):
    r = subprocess.run(
        [
            "uv",
            "run",
            "firecube",
            "advise",
            "compliance",
            "--profile",
            "cf-18",
            "--product",
            clean_cube,
            "--group",
            ".",
            *_LOCAL_STORAGE_FLAGS,
            "--format",
            "json",
        ],
        capture_output=True,
        text=True,
    )
    assert r.returncode == 0, r.stderr
    out = json.loads(r.stdout)
    assert out["profile"] == "cf-18"
    assert out["summary"]["errors"] == 0


@pytest.mark.integration
def test_advise_compliance_cf18_broken_reports_cf001(broken_cube):
    r = subprocess.run(
        [
            "uv",
            "run",
            "firecube",
            "advise",
            "compliance",
            "--profile",
            "cf-18",
            "--product",
            broken_cube,
            "--group",
            ".",
            *_LOCAL_STORAGE_FLAGS,
            "--format",
            "json",
        ],
        capture_output=True,
        text=True,
    )
    assert r.returncode == 1, r.stderr
    out = json.loads(r.stdout)
    assert out["profile"] == "cf-18"
    assert any(f["id"] == "CF001" for f in out["findings"])


@pytest.mark.integration
def test_advise_compliance_cf18_text_format(broken_cube):
    r = subprocess.run(
        [
            "uv",
            "run",
            "firecube",
            "advise",
            "compliance",
            "--profile",
            "cf-18",
            "--product",
            broken_cube,
            "--group",
            ".",
            *_LOCAL_STORAGE_FLAGS,
            "--format",
            "text",
        ],
        capture_output=True,
        text=True,
    )
    assert r.returncode == 1
    assert "CF001" in r.stdout
    assert "error" in r.stdout


@pytest.mark.integration
def test_removed_advise_cf_endpoint_reports_command_error():
    result = CliRunner().invoke(cli, ["advise", "cf"])

    assert result.exit_code == 2, result.output
    assert "Usage: cli advise" in result.output
    assert "No such command 'cf'" in result.output


@pytest.mark.integration
def test_advise_compliance_rejects_unknown_profile(clean_cube):
    r = subprocess.run(
        [
            "uv",
            "run",
            "firecube",
            "advise",
            "compliance",
            "--profile",
            "not-real",
            "--product",
            clean_cube,
            "--group",
            ".",
            *_LOCAL_STORAGE_FLAGS,
        ],
        capture_output=True,
        text=True,
    )
    assert r.returncode != 0
    assert "Invalid value for '--profile'" in r.stderr


@pytest.mark.integration
def test_advise_compliance_cf18_rejects_array_path_with_clean_error(tmp_path):
    product_uri = _make_array_repro(tmp_path)
    r = _run_advise_compliance(product_uri, "/raw_data")
    combined = r.stdout + r.stderr
    assert r.returncode != 0, combined
    assert "array" in combined.lower()
    assert "group" in combined.lower()
    assert "Traceback" not in combined
    assert "AttributeError" not in combined


@pytest.mark.integration
def test_advise_compliance_cf18_rejects_nonexistent_group_with_clean_error(tmp_path):
    product_uri = _make_array_repro(tmp_path)
    r = _run_advise_compliance(product_uri, "/nonexistent")
    combined = r.stdout + r.stderr
    assert r.returncode != 0, combined
    assert "not found" in combined.lower()
    assert "Traceback" not in combined


@pytest.mark.integration
def test_advise_compliance_cf18_root_group_does_not_hit_array_check(tmp_path):
    product_uri = _make_array_repro(tmp_path)
    r = _run_advise_compliance(product_uri, "/")
    combined = r.stdout + r.stderr
    assert r.returncode in {0, 1}, combined
    assert "array, not a group" not in combined
    assert "Traceback" not in combined
