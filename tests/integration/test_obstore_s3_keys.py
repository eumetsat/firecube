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

"""Regression: obstore S3 writes must not produce doubled object keys.

The bug: ``ObstoreFilesystem`` historically initialised ``S3Store`` so that the
store had a non-empty prefix derived from the product URI's path component, and
``_resolve_path`` returned the *full* path (including that prefix). Calling
``store.put(rel_path)`` then doubled the prefix, producing keys like
``data/product.zarr/data/product.zarr/chunk.0`` instead of the intended
``data/product.zarr/chunk.0``.

These tests use a real ``moto[server]`` HTTP endpoint because ``obstore`` is a
Rust extension and bypasses Python-level mocks (``moto.mock_aws()``), so the
only reliable way to verify the actual S3 keys is to talk to a real S3-shaped
server.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import boto3
import pytest
from moto.server import ThreadedMotoServer

from firecube.core.credentials import Credentials
from firecube.core.filesystem.ops import create_filesystem
from firecube.core.filesystem.store_factory import create_obstore_store
from firecube.core.product.identity import ProductIdentity
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.uri import StorageUri

pytestmark = pytest.mark.s3

_BUCKET = "test-bucket"
_ACCESS_KEY = "testing"
_SECRET_KEY = "testing"
_REGION = "us-east-1"


@pytest.fixture(scope="module")
def moto_s3_endpoint() -> Iterator[str]:
    server = ThreadedMotoServer(port=0)
    server.start()
    host, port = server.get_host_and_port()
    endpoint = f"http://{host}:{port}"
    try:
        yield endpoint
    finally:
        server.stop()


@pytest.fixture
def s3_bucket(moto_s3_endpoint: str) -> Iterator[Any]:
    client = boto3.client(
        "s3",
        endpoint_url=moto_s3_endpoint,
        aws_access_key_id=_ACCESS_KEY,
        aws_secret_access_key=_SECRET_KEY,
        region_name=_REGION,
    )
    client.create_bucket(Bucket=_BUCKET)
    try:
        yield client
    finally:
        objects = client.list_objects_v2(Bucket=_BUCKET).get("Contents", [])
        if objects:
            client.delete_objects(
                Bucket=_BUCKET,
                Delete={"Objects": [{"Key": o["Key"]} for o in objects]},
            )
        client.delete_bucket(Bucket=_BUCKET)


def _binding_for(product_path: str, endpoint: str) -> StorageBinding:
    uri = StorageUri(protocol="s3", authority=_BUCKET, path=product_path)
    return StorageBinding(
        identity=ProductIdentity.from_uri(uri, "zarr", product_name="test_product"),
        driver=StorageDriverConfig(
            driver="obstore",
            endpoint_url=endpoint,
            credentials=Credentials(access_key=_ACCESS_KEY, secret_key=_SECRET_KEY),
            region=_REGION,
            path_style=True,
        ),
    )


def _list_keys(client: Any) -> list[str]:
    resp = client.list_objects_v2(Bucket=_BUCKET)
    return sorted(obj["Key"] for obj in resp.get("Contents", []))


@pytest.mark.integration
class TestObstoreS3KeysNoDoubling:
    def test_filesystem_open_write_no_doubled_prefix(
        self,
        moto_s3_endpoint: str,
        s3_bucket: Any,
    ) -> None:
        binding = _binding_for("/data/product.zarr", moto_s3_endpoint)
        fs = create_filesystem(binding)

        target = StorageUri.parse(f"s3://{_BUCKET}/data/product.zarr/chunk.0")
        with fs.open(target, "wb") as fh:
            fh.write(b"hello-world")

        keys = _list_keys(s3_bucket)
        assert keys == ["data/product.zarr/chunk.0"], (
            f"Expected single-prefix key, got: {keys}. "
            "Doubled prefix indicates store prefix and _resolve_path are inconsistent."
        )
        for key in keys:
            assert "data/product.zarr/data/product.zarr" not in key, (
                f"Doubled prefix detected in key: {key}"
            )

    def test_filesystem_round_trip_under_nested_prefix(
        self,
        moto_s3_endpoint: str,
        s3_bucket: Any,
    ) -> None:
        binding = _binding_for("/very/deep/2026/05/product.zarr", moto_s3_endpoint)
        fs = create_filesystem(binding)

        payload = b"round-trip-payload"
        target = StorageUri.parse(f"s3://{_BUCKET}/very/deep/2026/05/product.zarr/sub/chunk.0")
        with fs.open(target, "wb") as fh:
            fh.write(payload)

        with fs.open(target, "rb") as fh:
            assert fh.read() == payload

        keys = _list_keys(s3_bucket)
        assert keys == ["very/deep/2026/05/product.zarr/sub/chunk.0"], (
            f"Expected single-prefix nested key, got: {keys}"
        )

    def test_filesystem_put_no_doubled_prefix(
        self,
        moto_s3_endpoint: str,
        s3_bucket: Any,
        tmp_path: Path,
    ) -> None:
        local_src = tmp_path / "src.bin"
        local_src.write_bytes(b"put-payload")

        binding = _binding_for("/data/product.zarr", moto_s3_endpoint)
        fs = create_filesystem(binding)

        dst = StorageUri.parse(f"s3://{_BUCKET}/data/product.zarr/asset.bin")
        fs.put(StorageUri.from_local_path(local_src), dst)

        keys = _list_keys(s3_bucket)
        assert keys == ["data/product.zarr/asset.bin"], f"Expected single-prefix key, got: {keys}"

    def test_filesystem_find_returns_single_prefix_uris(
        self,
        moto_s3_endpoint: str,
        s3_bucket: Any,
    ) -> None:
        binding = _binding_for("/data/product.zarr", moto_s3_endpoint)
        fs = create_filesystem(binding)

        for name in ("a.bin", "b.bin", "sub/c.bin"):
            target = StorageUri.parse(f"s3://{_BUCKET}/data/product.zarr/{name}")
            with fs.open(target, "wb") as fh:
                fh.write(b"x")

        root = StorageUri.parse(f"s3://{_BUCKET}/data/product.zarr")
        found = sorted(uri.to_str() for uri in fs.find(root))

        assert found == [
            f"s3://{_BUCKET}/data/product.zarr/a.bin",
            f"s3://{_BUCKET}/data/product.zarr/b.bin",
            f"s3://{_BUCKET}/data/product.zarr/sub/c.bin",
        ], f"find() returned unexpected URIs (likely doubled): {found}"

    def test_create_obstore_store_writes_single_prefix(
        self,
        moto_s3_endpoint: str,
        s3_bucket: Any,
    ) -> None:
        from firecube.core.config import StorageConfig

        sc = StorageConfig(
            storage_type="s3",
            storage_driver="obstore",
            endpoint_url=moto_s3_endpoint,
            access_key=_ACCESS_KEY,
            secret_key=_SECRET_KEY,
            region=_REGION,
            path_style=True,
        )

        target = f"s3://{_BUCKET}/data/product.zarr"
        raw = create_obstore_store(target, sc)

        import obstore

        obstore.put(raw, "zarr.json", b'{"node_type":"group"}')

        keys = _list_keys(s3_bucket)
        assert keys == ["data/product.zarr/zarr.json"], (
            f"create_obstore_store wrote unexpected keys (likely doubled or missing): {keys}"
        )
