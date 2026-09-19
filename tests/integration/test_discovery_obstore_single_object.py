# Copyright 2025-2026 EUMETSAT
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Single-object S3 discovery regression tests for supported storage drivers."""

from __future__ import annotations

from collections.abc import Iterator

import boto3
import pytest
from moto.server import ThreadedMotoServer

from firecube.core.config import StorageConfig
from firecube.core.formats.discovery import discover_input_files


@pytest.fixture(scope="module")
def moto_single_object() -> Iterator[str]:
    """Start moto ThreadedMotoServer and seed a bucket with one object."""
    server = ThreadedMotoServer(port=0)
    server.start()
    host, port = server.get_host_and_port()
    endpoint = f"http://{host}:{port}"

    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )
    client.create_bucket(Bucket="test-single-bucket")
    client.put_object(
        Bucket="test-single-bucket",
        Key="data/one.nc",
        Body=b"FAKE_NETCDF_CONTENT",
    )

    try:
        yield endpoint
    finally:
        server.stop()


@pytest.mark.integration
@pytest.mark.s3
@pytest.mark.parametrize("driver", ["fsspec", "obstore"])
def test_single_object_source_returns_object(moto_single_object: str, driver: str) -> None:
    """discover_input_files on a single-object S3 URI returns that object."""
    cfg = StorageConfig(
        storage_type="s3",
        storage_driver=driver,
        endpoint_url=moto_single_object,
        path_style=True,
        access_key="testing",
        secret_key="testing",
    )
    results = discover_input_files(
        "s3://test-single-bucket/data/one.nc",
        storage_config=cfg,
    )

    assert results == ["s3://test-single-bucket/data/one.nc"]
