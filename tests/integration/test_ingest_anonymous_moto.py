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

"""
Regression guard for --storage-anonymous source listing.

Verifies that --storage-anonymous enables reading from anonymous-read S3 buckets
through the real ingest pipeline (cli_test_plugin, default discover_source_files
at base.py:417 which uses chunk_manager.storage_config directly).

Note: Findings 1/2a/2b affect the write domain and binding round-trips,
not source discovery (which uses the original StorageConfig.anonymous directly).
These tests are regression guards. The authoritative round-trip unit tests
are in test_storage_config_binding_roundtrip.py.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from pathlib import Path

import boto3
import pytest
from click.testing import CliRunner
from moto.server import ThreadedMotoServer

from firecube.cli.main import cli


@pytest.fixture(scope="module")
def moto_s3() -> Iterator[str]:
    """Start moto ThreadedMotoServer, seed a public-read bucket with prefix source."""
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
    client.create_bucket(Bucket="test-bucket")

    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": "*",
                "Action": ["s3:GetObject", "s3:ListBucket"],
                "Resource": [
                    "arn:aws:s3:::test-bucket",
                    "arn:aws:s3:::test-bucket/*",
                ],
            }
        ],
    }
    client.put_bucket_policy(Bucket="test-bucket", Policy=json.dumps(policy))

    for name in ["item_001.nc", "item_002.nc"]:
        client.put_object(
            Bucket="test-bucket",
            Key=f"data/{name}",
            Body=b"FAKE_NETCDF_CONTENT_" + name.encode(),
        )

    try:
        yield endpoint
    finally:
        server.stop()


def _scrubbed_env(endpoint: str) -> dict[str, str]:
    """Return env dict with all AWS credentials removed + moto endpoint set."""
    env = {
        k: v
        for k, v in os.environ.items()
        if k
        not in {
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
            "AWS_SECURITY_TOKEN",
            "AWS_DEFAULT_REGION",
            "FIRECUBE_S3_ANONYMOUS",
        }
    }
    env["AWS_SHARED_CREDENTIALS_FILE"] = "/dev/null"
    env["AWS_CONFIG_FILE"] = "/dev/null"
    env["FIRECUBE_ENDPOINT_URL"] = endpoint
    env["FIRECUBE_PATH_STYLE"] = "true"
    env["FIRECUBE_S3_ANONYMOUS"] = "false"
    return env


CRED_ERROR_MARKERS = [
    "NoCredentialsError",
    "Unable to locate credentials",
    "Cannot list source location",
    "AccessDenied",
]

KNOWN_POST_DISCOVERY_MARKERS = [
    "Malformed URI",
]


def _has_cred_error(output: str, exc: BaseException | None) -> bool:
    combined = output + (str(exc) if exc else "")
    return any(marker in combined for marker in CRED_ERROR_MARKERS)


def _has_expected_no_flag_failure(output: str, exc: BaseException | None) -> bool:
    combined = output + (str(exc) if exc else "")
    return _has_cred_error(output, exc) or any(
        marker in combined for marker in KNOWN_POST_DISCOVERY_MARKERS
    )


@pytest.mark.integration
@pytest.mark.s3
@pytest.mark.parametrize("driver", ["fsspec", "obstore"])
def test_ingest_with_storage_anonymous(
    moto_s3: str,
    tmp_path: Path,
    driver: str,
) -> None:
    """
    Regression guard: --storage-anonymous enables source listing from an
    anonymous-read S3 bucket. Source listing must succeed (files found).

    The ingest exits non-zero due to a pre-existing cli_test_plugin stub
    bug (returns Path(ctx.target) causing 'file:/' mangling) — this is
    out of scope for this plan. The meaningful assertion is that source
    listing succeeded and no credentials error appeared.
    """
    env = _scrubbed_env(moto_s3)
    result = CliRunner().invoke(
        cli,
        [
            "ingest",
            "cli_test_plugin",
            "--input-data",
            "s3://test-bucket/data/",
            "--target",
            f"file://{tmp_path}/out.zarr",
            "--product-name",
            "moto_ingest_smoke",
            "--storage-type",
            "local",
            "--storage-driver",
            driver,
            "--output-format",
            "zarr",
            "--write-mode",
            "staged",
            "--storage-anonymous",
            "--option",
            "no_progress=true",
        ],
        env=env,
        catch_exceptions=True,
    )
    combined = (result.output or "") + (str(result.exception) if result.exception else "")

    # Source listing must succeed — no credentials error appeared
    assert not _has_cred_error(result.output, result.exception), (
        f"Got unexpected credentials error with --storage-anonymous ({driver}).\n"
        f"This means the anonymous flag did not reach the s3 filesystem.\n"
        f"Output: {combined[:1000]}"
    )
    # Source listing must have found the seeded objects
    assert any(
        marker in combined
        for marker in [
            "Found 2 files",
            "Found 2 source",
            "Finalizing pipeline",
            "1 batches",
            "Malformed URI",
        ]
    ), (
        f"Source listing did not reach pipeline phase with --storage-anonymous ({driver}).\n"
        f"Output: {combined[:1000]}"
    )


@pytest.mark.integration
@pytest.mark.s3
@pytest.mark.parametrize("driver", ["fsspec", "obstore"])
def test_ingest_no_flag_negative_control(
    moto_s3: str,
    tmp_path: Path,
    driver: str,
) -> None:
    """
    NEGATIVE CONTROL: without --storage-anonymous, creds are scrubbed => auth error.
    Must PASS both before and after fixes. Proves the bucket policy requires auth
    without the flag (moto is not freely readable by default unless policy is correct).
    """
    env = _scrubbed_env(moto_s3) | {
        "AWS_ACCESS_KEY_ID": "invalid",
        "AWS_SECRET_ACCESS_KEY": "invalid",
    }
    result = CliRunner().invoke(
        cli,
        [
            "ingest",
            "cli_test_plugin",
            "--input-data",
            "s3://test-bucket/data/",
            "--target",
            f"file://{tmp_path}/out.zarr",
            "--product-name",
            "moto_ingest_no_flag",
            "--storage-type",
            "local",
            "--storage-driver",
            driver,
            "--output-format",
            "zarr",
            "--write-mode",
            "staged",
            "--option",
            "no_progress=true",
            "--option",
            "allow_empty_source=true",
        ],
        env=env,
        catch_exceptions=True,
    )
    combined = (result.output or "") + (str(result.exception) if result.exception else "")
    assert result.exit_code != 0, (
        f"Without --storage-anonymous, expected auth failure, got exit 0. Output: {combined[:500]}"
    )
    assert _has_expected_no_flag_failure(result.output, result.exception), (
        "Without --storage-anonymous, expected credentials or known post-discovery "
        f"failure ({driver}), got:\n{combined[:1000]}"
    )
