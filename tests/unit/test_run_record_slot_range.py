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

from firecube.core.controlplane.events import RunEventWriter
from firecube.core.controlplane.repo import (
    ManifestRepository,
    _deserialize_slot_range,
)
from firecube.core.controlplane.types import SCHEMA_VERSION, RunInfo
from firecube.core.filesystem import create_filesystem
from firecube.core.product.identity import ProductIdentity
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.uri import StorageUri


def _make_repo(temp_workspace) -> tuple[ManifestRepository, str]:
    product_uri = StorageUri.from_local_path(temp_workspace / "product")
    control_uri = product_uri.join(".firecube")
    binding = StorageBinding(
        identity=ProductIdentity(
            product_uri=product_uri,
            product_name="product",
            format="zarr",
            control_root_uri=control_uri,
        ),
        driver=StorageDriverConfig(driver="fsspec"),
    )
    repo = ManifestRepository(binding=binding, workspace=temp_workspace)
    return repo, "product"


def _make_writer_with_slot_range(temp_workspace, slot_range):
    product_uri = StorageUri.from_local_path(temp_workspace / "product")
    control_uri = product_uri.join(".firecube")
    binding = StorageBinding(
        identity=ProductIdentity(
            product_uri=product_uri,
            product_name="product",
            format="zarr",
            control_root_uri=control_uri,
        ),
        driver=StorageDriverConfig(driver="fsspec"),
    )
    fs = create_filesystem(binding)
    return (
        RunEventWriter(
            fs=fs,
            control_uri=control_uri,
            product="product",
            run_id="run-001",
            slot_range=slot_range,
        ),
        temp_workspace / "product" / ".firecube" / "runs" / "run-001" / "run.json",
    )


def test_run_info_with_slot_range_round_trips(temp_workspace):
    repo, product = _make_repo(temp_workspace)
    payload = {
        "run_id": "run-abc",
        "status": "started",
        "run_dir": "runs/run-abc",
        "run_uri": "file:///product/.firecube/runs/run-abc",
        "started_at": 1.0,
        "updated_at": 2.0,
        "completed_at": None,
        "events": 5,
        "parts": 1,
        "slot_range": [100, 200],
    }
    info = repo._run_info_from_entry(product, payload)
    assert isinstance(info, RunInfo)
    assert info.slot_range == (100, 200)


def test_run_info_missing_slot_range_defaults_to_none(temp_workspace):
    repo, product = _make_repo(temp_workspace)
    payload = {
        "run_id": "run-abc",
        "status": "started",
        "started_at": 1.0,
        "updated_at": 2.0,
        "completed_at": None,
        "events": 0,
        "parts": 0,
    }
    info = repo._run_info_from_entry(product, payload)
    assert info.slot_range is None


def test_run_info_null_slot_range_is_none(temp_workspace):
    repo, product = _make_repo(temp_workspace)
    payload = {
        "run_id": "run-abc",
        "status": "started",
        "started_at": 1.0,
        "updated_at": 2.0,
        "completed_at": None,
        "events": 0,
        "parts": 0,
        "slot_range": None,
    }
    info = repo._run_info_from_entry(product, payload)
    assert info.slot_range is None


def test_record_includes_slot_range_when_set(temp_workspace):
    repo, _ = _make_repo(temp_workspace)
    payload = repo._build_run_record(
        run_id="run-xyz",
        output_path="s3://bucket/product.zarr",
        output_format="zarr",
        status="started",
        size=0,
        meta={"plugin": "test"},
        slot_range=(0, 100),
    )
    assert payload["slot_range"] == [0, 100]


def test_record_omits_slot_range_when_none(temp_workspace):
    repo, _ = _make_repo(temp_workspace)
    payload = repo._build_run_record(
        run_id="run-xyz",
        output_path="s3://bucket/product.zarr",
        output_format="zarr",
        status="started",
        size=0,
        meta={"plugin": "test"},
        slot_range=None,
    )
    assert "slot_range" not in payload


def test_schema_version_constant_unchanged():
    assert SCHEMA_VERSION == "v2"


def test_deserialize_slot_range_helper_handles_tuple_and_list():
    assert _deserialize_slot_range([10, 20]) == (10, 20)
    assert _deserialize_slot_range((10, 20)) == (10, 20)
    assert _deserialize_slot_range(None) is None
    assert _deserialize_slot_range("bad") is None
    assert _deserialize_slot_range([1, 2, 3]) is None


def test_run_meta_json_round_trips_slot_range(temp_workspace):
    writer, run_meta_path = _make_writer_with_slot_range(temp_workspace, slot_range=(0, 50))
    writer.append("run_started", {"key": "run_run-001"}, flush=True)
    written = json.loads(run_meta_path.read_text(encoding="utf-8"))
    assert written["slot_range"] == [0, 50]


def test_run_meta_json_omits_slot_range_for_single_pod(temp_workspace):
    writer, run_meta_path = _make_writer_with_slot_range(temp_workspace, slot_range=None)
    writer.append("run_started", {"key": "run_run-001"}, flush=True)
    written = json.loads(run_meta_path.read_text(encoding="utf-8"))
    assert "slot_range" not in written
