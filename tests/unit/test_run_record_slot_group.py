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
from typing import cast

from firecube.core.controlplane.events import RunEventWriter
from firecube.core.controlplane.repo import ManifestRepository, _deserialize_slot_group
from firecube.core.controlplane.types import RunInfo
from firecube.core.filesystem import StorageFilesystemFull, create_filesystem
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


def _make_writer_with_slot_group(temp_workspace, slot_group):
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
    fs = cast(StorageFilesystemFull, create_filesystem(binding))
    return (
        RunEventWriter(
            fs=fs,
            control_uri=control_uri,
            product="product",
            run_id="run-001",
            slot_group=slot_group,
        ),
        temp_workspace / "product" / ".firecube" / "runs" / "run-001" / "run.json",
    )


def test_slot_group_persisted_in_run_info(temp_workspace):
    repo, product = _make_repo(temp_workspace)
    payload = {
        "run_id": "run-abc",
        "status": "started",
        "started_at": 1.0,
        "updated_at": 2.0,
        "completed_at": None,
        "events": 5,
        "parts": 1,
        "slot_group": "group_a",
    }
    info = repo._run_info_from_entry(product, payload)
    assert isinstance(info, RunInfo)
    assert info.slot_group == "group_a"


def test_slot_group_omitted_defaults_to_none(temp_workspace):
    repo, product = _make_repo(temp_workspace)
    info = repo._run_info_from_entry(
        product,
        {
            "run_id": "run-abc",
            "status": "started",
            "started_at": 1.0,
            "updated_at": 2.0,
            "completed_at": None,
            "events": 0,
            "parts": 0,
        },
    )
    assert info.slot_group is None


def test_slot_group_roundtrip(temp_workspace):
    repo, product = _make_repo(temp_workspace)
    payload = repo._build_run_record(
        run_id="run-xyz",
        output_path="s3://bucket/product.zarr",
        output_format="zarr",
        status="started",
        size=0,
        meta={"plugin": "test"},
        slot_group="group_b",
    )
    info = repo._run_info_from_entry(
        product, payload | {"run_id": "run-xyz", "events": 0, "parts": 0}
    )
    assert info.slot_group == "group_b"


def test_slot_group_none_not_in_payload(temp_workspace):
    repo, _ = _make_repo(temp_workspace)
    payload = repo._build_run_record(
        run_id="run-xyz",
        output_path="s3://bucket/product.zarr",
        output_format="zarr",
        status="started",
        size=0,
        meta={"plugin": "test"},
        slot_group=None,
    )
    assert "slot_group" not in payload


def test_deserialize_slot_group_helper():
    assert _deserialize_slot_group("group_a") == "group_a"
    assert _deserialize_slot_group(None) is None
    assert _deserialize_slot_group(["group_a"]) is None


def test_run_meta_json_round_trips_slot_group(temp_workspace):
    writer, run_meta_path = _make_writer_with_slot_group(temp_workspace, slot_group="group_a")
    writer.append("run_started", {"key": "run_run-001"}, flush=True)
    written = json.loads(run_meta_path.read_text(encoding="utf-8"))
    assert written["slot_group"] == "group_a"


def test_run_meta_json_omits_slot_group_for_single_pod(temp_workspace):
    writer, run_meta_path = _make_writer_with_slot_group(temp_workspace, slot_group=None)
    writer.append("run_started", {"key": "run_run-001"}, flush=True)
    written = json.loads(run_meta_path.read_text(encoding="utf-8"))
    assert "slot_group" not in written
