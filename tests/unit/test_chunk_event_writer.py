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
import time

from firecube.core.controlplane.events import RunEventWriter
from firecube.core.filesystem import create_filesystem
from firecube.core.product.identity import ProductIdentity
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.uri import StorageUri


def _make_writer(temp_workspace, *, heartbeat_threshold_s: float, segment_size: int = 25):
    control_root = temp_workspace / "product" / ".firecube"
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
    writer = RunEventWriter(
        fs=fs,
        control_uri=control_uri,
        product="product",
        run_id="run-001",
        segment_size=segment_size,
        heartbeat_threshold_s=heartbeat_threshold_s,
    )
    return writer, control_root


def test_buffered_span_appends_do_not_rewrite_run_meta_per_event(temp_workspace):
    writer, control_root = _make_writer(
        temp_workspace, heartbeat_threshold_s=3600.0, segment_size=10
    )
    run_meta_path = control_root / "runs" / "run-001" / "run.json"

    initial_meta = json.loads(run_meta_path.read_text(encoding="utf-8"))

    writer.append("span_upsert", {"key": "k1"}, flush=False)
    writer.append("span_upsert", {"key": "k2"}, flush=False)

    current_meta = json.loads(run_meta_path.read_text(encoding="utf-8"))
    event_files = sorted((control_root / "runs" / "run-001").glob("events-*.jsonl"))

    assert current_meta == initial_meta
    assert len(writer._buffer) == 2
    assert event_files == []


def test_heartbeat_refreshes_run_meta_without_flushing_buffer(temp_workspace):
    writer, control_root = _make_writer(temp_workspace, heartbeat_threshold_s=30.0, segment_size=10)
    run_meta_path = control_root / "runs" / "run-001" / "run.json"

    writer._last_meta_write = time.time() - 100.0
    writer.append("span_upsert", {"key": "k1"}, flush=False)

    current_meta = json.loads(run_meta_path.read_text(encoding="utf-8"))
    event_files = sorted((control_root / "runs" / "run-001").glob("events-*.jsonl"))

    assert current_meta["events"] == 1
    assert current_meta["parts"] == 0
    assert len(writer._buffer) == 1
    assert event_files == []
