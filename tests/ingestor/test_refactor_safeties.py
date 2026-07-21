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

from tests.helpers.storage import make_test_binding

from firecube.core.controlplane import ChunkManager
from firecube.ingestor.runtime.recording import SpanRecorder
from firecube.ingestor.types.context import IngestResult, OutputPaths, RuntimeIngestContext


def test_span_recorder_skips_when_already_registered(tmp_path) -> None:
    manager = ChunkManager(binding=make_test_binding(tmp_path), workspace=tmp_path)
    try:
        recorder = SpanRecorder(manager)
        run_id = "already-registered-run"
        product = "product.zarr"
        manager.record_run_started(
            product=product,
            run_id=run_id,
            output_path="file:///tmp/product.zarr",
            output_format="zarr",
            size=0,
            meta={"plugin": "test_recorder"},
        )
        before = manager.list_runs(product=product)

        result = IngestResult(outputs=OutputPaths(primary="out"), output_format="test")
        result.registered = True

        recorder.register_run(
            ctx=RuntimeIngestContext(source=str(tmp_path)),
            result=result,
            run_id=run_id,
            product=product,
            slice_meta={"plugin": "test_recorder"},
        )

        after = manager.list_runs(product=product)
        assert [(run.run_id, run.status) for run in after] == [
            (run.run_id, run.status) for run in before
        ]
        assert after[0].status == "started"
    finally:
        manager.close()
