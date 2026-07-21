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

from pathlib import Path

from firecube.ingestor.api import IngestContext, IngestManifest, StorageContext
from firecube.ingestor.types.context import RuntimeFlags, RuntimeIngestContext


def test_ingest_context_optimization():
    """Verify that IngestContext slots logic works."""
    ctx = IngestContext(source=".")
    assert ctx.options == {}

    # Should accept typed storage context.
    storage = StorageContext(output=None)
    ctx.storage = storage
    assert ctx.storage is storage


def test_manifest_serialization():
    """Verify manifest correctness."""
    m = IngestManifest(
        plugin="test",
        output_format="zarr",
        stored_at="/tmp/foo",
        files=10,
        bytes=1000,
        duration_s=1.5,
        metrics={"rows": 50},
        run_id="run-123",
    )
    d = m.to_dict()
    assert d["plugin"] == "test"
    assert d["run_id"] == "run-123"
    assert "product" not in d  # Optional field not set


def test_context_run_id_injection():
    """Verify run_id is carried."""
    ctx = IngestContext(source=".", run_id="exclusive-run")
    assert ctx.run_id == "exclusive-run"


def test_runtime_context_clone_isolated_from_input():
    """Runtime context must not mutate caller-owned options."""
    input_ctx = IngestContext(source=".", options={"foo": "bar"})
    runtime_ctx = RuntimeIngestContext.from_ingest_context(
        input_ctx,
        run_id="run-123",
        temp_root=Path("/tmp"),
        materializer=lambda source: Path(source),
    )

    assert runtime_ctx.run_id == "run-123"
    assert runtime_ctx.options["run_id"] == "run-123"
    assert "run_id" not in input_ctx.options

    runtime_ctx.options["foo"] = "changed"
    assert input_ctx.options["foo"] == "bar"


def test_runtime_context_groups_identity_flags_services():
    input_ctx = IngestContext(source=".", options={"foo": "bar"})
    runtime_ctx = RuntimeIngestContext.from_ingest_context(
        input_ctx,
        run_id="run-999",
        temp_root=Path("/tmp"),
        materializer=lambda source: Path(source),
    )

    assert runtime_ctx.identity.run_id == "run-999"
    assert runtime_ctx.temp_root == Path("/tmp")

    runtime_ctx.flags = RuntimeFlags(force_reingest=True, incremental=True, dry_run=False)
    assert runtime_ctx.force_reingest is True
    assert runtime_ctx.incremental is True
    assert runtime_ctx.dry_run is False

    assert callable(runtime_ctx._materializer)
