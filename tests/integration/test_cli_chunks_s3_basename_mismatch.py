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

"""Regression: ``chunks runs list`` must honor explicit S3 targets.

This covers the basename-mismatch case where the logical product name differs
from the S3 object basename at the target root. The command must still find the
control-plane run under the exact target URI, including a trailing-slash form.
"""

from __future__ import annotations

import json
from collections.abc import Iterator

import boto3
import pytest
from click.testing import CliRunner
from moto import mock_aws
from moto.server import ThreadedMotoServer

from firecube.cli.main import cli

pytestmark = [pytest.mark.integration, pytest.mark.s3]

_BUCKET = "test-bucket"
_REGION = "us-east-1"
_RUN_META = {
    "schema_version": "v2",
    "status": "started",
    "parts": 0,
    "events": 0,
    "started_at": 1.0,
    "updated_at": 1.0,
    "run_uri": "file:///tmp/placeholder",
    "run_stale_threshold_s": 3600,
}


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


def _put_run_json(endpoint: str, target_root: str, *, product: str, run_id: str) -> None:
    client = boto3.client("s3", endpoint_url=endpoint, region_name=_REGION)
    client.create_bucket(Bucket=_BUCKET)
    payload = {
        **_RUN_META,
        "product": product,
        "run_id": run_id,
    }
    client.put_object(
        Bucket=_BUCKET,
        Key=f"{target_root}/.firecube/runs/{run_id}/run.json",
        Body=json.dumps(payload, separators=(",", ":"), sort_keys=True).encode(),
    )


def _invoke_runs_list(target: str, endpoint: str) -> tuple[int, str]:
    result = CliRunner().invoke(
        cli,
        [
            "chunks",
            "runs",
            "list",
            "--product-name",
            "logical_product",
            "--target",
            target,
            "--format",
            "json",
        ],
        env={
            "AWS_ACCESS_KEY_ID": "testing",
            "AWS_SECRET_ACCESS_KEY": "testing",
            "AWS_DEFAULT_REGION": _REGION,
            "AWS_EC2_METADATA_DISABLED": "true",
            "AWS_S3_ADDRESSING_STYLE": "path",
            "FIRECUBE_STORAGE_TYPE": "s3",
            "FIRECUBE_STORAGE_DRIVER": "fsspec",
            "FIRECUBE_ENDPOINT_URL": endpoint,
            "FIRECUBE_ACCESS_KEY": "testing",
            "FIRECUBE_SECRET_KEY": "testing",
            "FIRECUBE_REGION": _REGION,
            "FIRECUBE_PATH_STYLE": "true",
        },
    )
    return result.exit_code, result.output


def _assert_run_found(output: str) -> None:
    output_lines = output.splitlines()
    if any(line.strip() == "[]" for line in output_lines):
        runs = []
    else:
        json_start = next(i for i, line in enumerate(output_lines) if line.strip() == "[")
        runs = json.loads("\n".join(output_lines[json_start:]))
    assert len(runs) == 1, runs
    assert runs[0]["run_id"] == "run-001"
    assert runs[0]["status"] == "started"


@mock_aws
def test_s3_runs_list_basename_mismatch(moto_s3_endpoint: str) -> None:
    _put_run_json(moto_s3_endpoint, "path/output.zarr", product="logical_product", run_id="run-001")

    exit_code, output = _invoke_runs_list("s3://test-bucket/path/output.zarr", moto_s3_endpoint)

    assert exit_code == 0, output
    _assert_run_found(output)


@mock_aws
def test_s3_runs_list_basename_mismatch_trailing_slash(moto_s3_endpoint: str) -> None:
    _put_run_json(moto_s3_endpoint, "path/output.zarr", product="logical_product", run_id="run-001")

    exit_code, output = _invoke_runs_list("s3://test-bucket/path/output.zarr/", moto_s3_endpoint)

    assert exit_code == 0, output
    _assert_run_found(output)
