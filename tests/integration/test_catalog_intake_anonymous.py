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
import os
from collections.abc import Iterator
from pathlib import Path

import boto3
import numpy as np
import pytest
import xarray as xr
import yaml
from click.testing import CliRunner
from moto.server import ThreadedMotoServer

from firecube.cli.main import cli


@pytest.fixture(scope="module")
def moto_zarr_product(tmp_path_factory: pytest.TempPathFactory) -> Iterator[tuple[str, str]]:
    """Serve a public-read S3 bucket containing a minimal catalogable Zarr product."""
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
    bucket = "catalog-test-bucket"
    client.create_bucket(Bucket=bucket)
    client.put_bucket_policy(
        Bucket=bucket,
        Policy=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": "*",
                        "Action": ["s3:GetObject", "s3:ListBucket"],
                        "Resource": [
                            f"arn:aws:s3:::{bucket}",
                            f"arn:aws:s3:::{bucket}/*",
                        ],
                    }
                ],
            }
        ),
    )

    local_store = tmp_path_factory.mktemp("catalog-product") / "product.zarr"
    xr.Dataset(
        {"temperature": (("time",), np.array([1.0, 2.0], dtype=np.float32))},
        coords={"time": np.array([0, 1], dtype=np.int64)},
    ).to_zarr(local_store, group="g1", mode="w", zarr_format=3, consolidated=False)

    for path in local_store.rglob("*"):
        if path.is_file():
            key = f"product.zarr/{path.relative_to(local_store).as_posix()}"
            client.upload_file(str(path), bucket, key, ExtraArgs={"ACL": "public-read"})

    try:
        yield endpoint, f"s3://{bucket}/product.zarr"
    finally:
        server.stop()


def _catalog_env(endpoint: str) -> dict[str, str]:
    env = {
        key: value
        for key, value in os.environ.items()
        if key
        not in {
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
            "AWS_SECURITY_TOKEN",
            "AWS_DEFAULT_REGION",
            "FIRECUBE_ACCESS_KEY",
            "FIRECUBE_SECRET_KEY",
            "FIRECUBE_S3_ANONYMOUS",
        }
    }
    env["AWS_SHARED_CREDENTIALS_FILE"] = "/dev/null"
    env["AWS_CONFIG_FILE"] = "/dev/null"
    env["FIRECUBE_ENDPOINT_URL"] = endpoint
    env["FIRECUBE_PATH_STYLE"] = "true"
    env["FIRECUBE_S3_ANONYMOUS"] = "false"
    return env


def _storage_options(catalog_path: Path) -> dict[str, object]:
    catalog = yaml.safe_load(catalog_path.read_text(encoding="utf-8"))
    source = catalog["sources"]["cli_test_plugin_g1"]
    return source["args"]["storage_options"]


@pytest.mark.integration
@pytest.mark.parametrize(
    ("anonymous", "expected_anon"),
    [(True, True), (False, False)],
)
def test_catalog_intake_storage_anonymous_controls_yaml_storage_options(
    moto_zarr_product: tuple[str, str],
    tmp_path: Path,
    anonymous: bool,
    expected_anon: bool,
) -> None:
    endpoint, product_uri = moto_zarr_product
    output_path = tmp_path / f"catalog-anon-{anonymous}.yaml"
    env = _catalog_env(endpoint)
    if not anonymous:
        env["FIRECUBE_ACCESS_KEY"] = "testing"
        env["FIRECUBE_SECRET_KEY"] = "testing"

    args = [
        "catalog",
        "intake",
        "cli_test_plugin",
        "--product",
        product_uri,
        "--output",
        output_path.as_uri(),
        "--collection-id",
        "test-coll",
        "--storage-driver",
        "fsspec",
    ]
    if anonymous:
        args.append("--storage-anonymous")

    result = CliRunner().invoke(cli, args, env=env, catch_exceptions=True)

    assert result.exit_code == 0, result.output
    storage_options = _storage_options(output_path)
    assert storage_options["anon"] is expected_anon
    if anonymous:
        assert "key" not in storage_options
        assert "secret" not in storage_options
    else:
        assert storage_options["key"] == "${FIRECUBE_ACCESS_KEY}"
        assert storage_options["secret"] == "${FIRECUBE_SECRET_KEY}"
