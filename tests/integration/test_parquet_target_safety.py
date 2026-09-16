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

"""Parquet containment must preserve existing rows and their run records."""

from typing import Literal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from firecube.core.storage.completion import StorageCompleter
from firecube.ingestor.api import GenericParquetIngestor, PluginContext
from firecube.ingestor.errors import ResumeConflictError
from firecube.ingestor.runtime.engine import PipelineFailedBatchesError
from tests.helpers.storage import make_test_context

pytestmark = pytest.mark.integration


class _Parquet(GenericParquetIngestor):
    PRODUCT_NAME = "parquet_safety"
    name = "parquet_safety"

    def discover_source_files(self, ctx: PluginContext):
        return ["first", "second"]

    def build_dataset(self, group, batch, ctx: PluginContext):
        return pa.table({"item": batch.items, "value": [ctx.option("x_value", 1)]})

    def output_relpath(self, group, batch, ctx: PluginContext):
        return ctx.option("x_path") or super().output_relpath(group, batch, ctx)


def _context(root, mode, *, driver: Literal["fsspec", "obstore"] = "fsspec", **options):
    return make_test_context(
        root,
        product="parquet_safety",
        format="parquet",
        driver=driver,
        options={
            "write_mode": mode,
            "pipeline_workers": 1,
            "pipeline_batch_size": 1,
            "no_progress": True,
            "cleanup_workspace": True,
            **options,
        },
    )


@pytest.mark.parametrize("mode", ["direct", "staged"])
@pytest.mark.parametrize("flag", ["resume_existing", "force_reingest"])
@pytest.mark.parametrize("driver", ["fsspec", "obstore"])
def test_reuse_refused_without_changing_prior_rows_or_runs(tmp_path, mode, flag, driver):
    host = _Parquet()
    host.run(_context(tmp_path, mode, driver=driver))
    target = tmp_path / "parquet_safety"
    before = {p.name: p.read_bytes() for p in target.glob("*.parquet")}
    assert len(before) == 2
    with pytest.raises(ResumeConflictError, match=r"fresh.*target"):
        _Parquet().run(_context(tmp_path, mode, driver=driver, **{flag: True, "x_value": 2}))
    assert {p.name: p.read_bytes() for p in target.glob("*.parquet")} == before
    runs = host._chunk_manager.list_runs(product="parquet_safety")
    assert len(runs) == 1
    assert runs[0].status == "complete"


@pytest.mark.parametrize("mode", ["direct", "staged"])
def test_untracked_existing_target_refused(tmp_path, mode):
    target = tmp_path / "parquet_safety"
    target.mkdir()
    old = target / "part-batch_0000.parquet"
    pq.write_table(pa.table({"value": [99]}), old)
    before = old.read_bytes()
    with pytest.raises(ResumeConflictError, match=r"fresh.*target"):
        _Parquet().run(_context(tmp_path, mode, force_reingest=True))
    assert old.read_bytes() == before


@pytest.mark.parametrize("mode", ["direct", "staged"])
def test_duplicate_plugin_output_path_never_overwrites_first_batch(tmp_path, mode):
    host = _Parquet()
    with pytest.raises(PipelineFailedBatchesError, match="already exists"):
        host.run(_context(tmp_path, mode, x_path="same.parquet"))
    runs = host._chunk_manager.list_runs(product="parquet_safety")
    assert [r.status for r in runs] == ["failed"]
    if mode == "direct":
        assert pq.read_table(tmp_path / "parquet_safety" / "same.parquet").to_pydict() == {
            "item": ["first"],
            "value": [1],
        }
    else:
        assert not list((tmp_path / "parquet_safety").glob("*.parquet"))


@pytest.mark.parametrize(
    "path", ["../outside.parquet", "/tmp/outside.parquet", ".firecube/data.parquet"]
)
def test_plugin_output_stays_inside_product_data(tmp_path, path):
    with pytest.raises(PipelineFailedBatchesError, match=r"relative.*data path"):
        _Parquet().run(_context(tmp_path, "direct", x_path=path))


def test_staged_destination_conflict_is_refused_before_promotion(tmp_path):
    target = tmp_path / "parquet_safety"

    class LateConflict(_Parquet):
        def build_dataset(self, group, batch, ctx: PluginContext):
            target.mkdir(exist_ok=True)
            pq.write_table(pa.table({"value": [99]}), target / "external.parquet")
            return super().build_dataset(group, batch, ctx)

    host = LateConflict()
    with pytest.raises(ResumeConflictError, match=r"fresh.*target"):
        host.run(_context(tmp_path, "staged"))
    assert [p.name for p in target.glob("*.parquet")] == ["external.parquet"]
    assert pq.read_table(target / "external.parquet").to_pydict() == {"value": [99]}
    assert [r.status for r in host._chunk_manager.list_runs(product="parquet_safety")] == ["failed"]


def test_claim_held_through_failed_promotion_and_released(tmp_path, monkeypatch):
    host = _Parquet()

    def fail_promotion(self, result, ctx):
        claims = host._chunk_manager.list_claims(product="parquet_safety")
        assert any(":fresh_output:" in c.domain for c in claims)
        raise OSError("promotion interrupted")

    monkeypatch.setattr(StorageCompleter, "complete_output", fail_promotion)
    with pytest.raises(OSError, match="promotion interrupted"):
        host.run(_context(tmp_path, "staged"))
    assert [r.status for r in host._chunk_manager.list_runs(product="parquet_safety")] == ["failed"]
    assert host._chunk_manager.list_claims(product="parquet_safety") == []
    assert not list((tmp_path / "parquet_safety").glob("*.parquet"))


def test_discovery_failure_does_not_create_control_plane(tmp_path):
    class BadDiscovery(_Parquet):
        def discover_source_files(self, ctx: PluginContext):
            raise ValueError("invalid source")

    with pytest.raises(ValueError, match="invalid source"):
        BadDiscovery().run(_context(tmp_path, "direct"))
    assert not (tmp_path / "parquet_safety" / ".firecube").exists()


@pytest.fixture(scope="module")
def parquet_s3():
    import boto3
    from moto.server import ThreadedMotoServer

    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0)
    server.start()
    host, port = server.get_host_and_port()
    endpoint = f"http://{host}:{port}"
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        region_name="us-east-1",
    )
    client.create_bucket(Bucket="parquet-safety")
    try:
        yield endpoint, client
    finally:
        server.stop()


@pytest.mark.s3
@pytest.mark.parametrize("driver", ["fsspec", "obstore"])
@pytest.mark.parametrize("mode", ["direct", "staged"])
@pytest.mark.parametrize("collision", [False, True])
def test_remote_lifecycle_preserves_parts(tmp_path, parquet_s3, driver, mode, collision):
    from firecube.core.credentials import Credentials
    from firecube.core.product.identity import ProductIdentity
    from firecube.core.storage.binding import StorageBinding
    from firecube.core.storage.driver_config import StorageDriverConfig
    from firecube.core.storage.session import StorageSession
    from firecube.core.storage.uri import StorageUri
    from firecube.ingestor.types.context import IngestContext, StorageContext

    endpoint, client = parquet_s3
    prefix = f"{driver}-{mode}-{collision}"
    target = f"s3://parquet-safety/{prefix}"
    session = StorageSession(
        StorageBinding(
            identity=ProductIdentity.from_uri(
                StorageUri.parse(target), "parquet", product_name="parquet_safety"
            ),
            driver=StorageDriverConfig(
                driver=driver,
                endpoint_url=endpoint,
                credentials=Credentials(access_key="testing", secret_key="testing"),
                region="us-east-1",
            ),
        )
    )

    def context(**extra):
        return IngestContext(
            source=str(tmp_path),
            target=target,
            output_format="parquet",
            storage=StorageContext(output=session),
            options={
                "write_mode": mode,
                "pipeline_workers": 1 if collision else 2,
                "pipeline_batch_size": 1,
                "no_progress": True,
                "x_path": "same.parquet" if collision else None,
                **extra,
            },
        )

    host = _Parquet()
    if collision:
        with pytest.raises(PipelineFailedBatchesError, match="already exists"):
            host.run(context())
    else:
        host.run(context())
    objects = client.list_objects_v2(Bucket="parquet-safety", Prefix=prefix + "/").get(
        "Contents", []
    )
    parts = {
        obj["Key"]: client.get_object(Bucket="parquet-safety", Key=obj["Key"])["Body"].read()
        for obj in objects
        if obj["Key"].endswith(".parquet")
    }
    expected_items = ["first", "second"]
    if collision:
        expected_items = ["first"] if mode == "direct" else []
    assert len(parts) == len(expected_items)
    rows = [
        item
        for data in parts.values()
        for item in pq.read_table(pa.BufferReader(data)).to_pydict()["item"]
    ]
    assert sorted(rows) == expected_items
    assert [r.status for r in host._chunk_manager.list_runs(product="parquet_safety")] == [
        "failed" if collision else "complete"
    ]
    with pytest.raises(ResumeConflictError):
        _Parquet().run(context(resume_existing=True))
    for key, data in parts.items():
        assert client.get_object(Bucket="parquet-safety", Key=key)["Body"].read() == data
