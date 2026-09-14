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

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import boto3
import numpy as np
import pytest
import xarray as xr
from moto.server import ThreadedMotoServer

from firecube.core.config import StorageConfig
from firecube.core.filesystem.store_factory import create_zarr_store

pytestmark = pytest.mark.integration

_REPO_ROOT = Path(__file__).resolve().parents[2]
_FIXTURE_PLUGINS = _REPO_ROOT / "tests" / "fixtures" / "firecube_test_plugins"
_SYNTHETIC_PRECIP = _REPO_ROOT / "tests" / "fixtures" / "gen_synthetic_precip_daily.py"

_BUCKET = "m5-bucket"
_REGION = "us-east-1"
_ACCESS_KEY = "testing"
_SECRET_KEY = "testing"
_EXPECTED_TIMESTAMPS = np.datetime64("2024-01-01", "ns") + np.arange(30) * np.timedelta64(1, "D")


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


def _generate_days(out_dir: Path, start_day: int, end_day: int) -> None:
    result = subprocess.run(
        [sys.executable, str(_SYNTHETIC_PRECIP), str(out_dir), str(start_day), str(end_day)],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def _firecube_command(*args: str) -> list[str]:
    return [
        "uv",
        "run",
        "--with-editable",
        str(_REPO_ROOT),
        "--with-editable",
        str(_FIXTURE_PLUGINS),
        "firecube",
        *args,
    ]


def _last_manifest(text: str) -> dict[str, Any]:
    manifest_start = text.rfind('{\n  "plugin"')
    assert manifest_start >= 0, text
    payload = json.loads(text[manifest_start:])
    assert isinstance(payload, dict), payload
    return payload


def _s3_env(endpoint: str) -> dict[str, str]:
    return {
        **os.environ,
        "AWS_ACCESS_KEY_ID": _ACCESS_KEY,
        "AWS_SECRET_ACCESS_KEY": _SECRET_KEY,
        "AWS_DEFAULT_REGION": _REGION,
        "AWS_EC2_METADATA_DISABLED": "true",
        "AWS_S3_ADDRESSING_STYLE": "path",
        "FIRECUBE_ENDPOINT_URL": endpoint,
        "FIRECUBE_ACCESS_KEY": _ACCESS_KEY,
        "FIRECUBE_SECRET_KEY": _SECRET_KEY,
        "FIRECUBE_REGION": _REGION,
        "FIRECUBE_PATH_STYLE": "true",
    }


def _ingest_staged(
    source: Path,
    target_uri: str,
    *,
    storage_type: str,
    batch_size: int,
    env: dict[str, str] | None = None,
) -> dict[str, Any]:
    result = subprocess.run(
        _firecube_command(
            "ingest",
            "precip_daily",
            "--input-data",
            str(source),
            "--target",
            target_uri,
            "--product-name",
            "m5",
            "--storage-type",
            storage_type,
            "--storage-driver",
            "fsspec",
            "--output-format",
            "zarr",
            "--write-mode",
            "staged",
            "--option",
            "no_progress=true",
            "--option",
            f"pipeline_batch_size={batch_size}",
        ),
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
        env=env,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return _last_manifest(result.stdout)


def _open_default_group(target_uri: str, storage_config: StorageConfig) -> xr.Dataset:
    handle = create_zarr_store(uri=target_uri, storage_config=storage_config, mode="r")
    return xr.open_zarr(**handle.zarr_kwargs(), group="default", zarr_format=3, consolidated=False)


def _assert_thirty_daily_timestamps(dataset: xr.Dataset, manifest: dict[str, Any]) -> None:
    timestamps = np.asarray(dataset["time"].values).astype("datetime64[ns]")
    assert timestamps.shape == (30,)
    assert len(np.unique(timestamps)) == 30
    assert np.all(np.diff(timestamps) > np.timedelta64(0, "ns"))
    assert np.array_equal(timestamps, _EXPECTED_TIMESTAMPS)
    assert dataset["precipitation"].shape[0] == 30
    assert manifest["metrics"]["count"] == 30
    assert manifest["metrics"]["pipeline"]["files_processed"] == 30


def test_staged_write_keeps_all_pipeline_batches(tmp_path: Path) -> None:
    """staged mode accumulates each pipeline batch before final upload."""
    source = tmp_path / "input"
    target = tmp_path / "m5-staged.zarr"
    _generate_days(source, 1, 30)

    manifest = _ingest_staged(source, target.as_uri(), storage_type="local", batch_size=10)

    dataset = _open_default_group(
        target.as_uri(), StorageConfig(storage_type="local", storage_driver="fsspec")
    )
    _assert_thirty_daily_timestamps(dataset, manifest)


@pytest.mark.s3
def test_staged_write_keeps_all_pipeline_batches_on_s3(
    tmp_path: Path, moto_s3_endpoint: str
) -> None:
    """staged mode uploads every pipeline batch to the S3 target, not only the last."""
    source = tmp_path / "input"
    target_uri = f"s3://{_BUCKET}/m5.zarr"
    _generate_days(source, 1, 30)
    boto3.client(
        "s3",
        endpoint_url=moto_s3_endpoint,
        aws_access_key_id=_ACCESS_KEY,
        aws_secret_access_key=_SECRET_KEY,
        region_name=_REGION,
    ).create_bucket(Bucket=_BUCKET)

    manifest = _ingest_staged(
        source,
        target_uri,
        storage_type="s3",
        batch_size=10,
        env=_s3_env(moto_s3_endpoint),
    )

    dataset = _open_default_group(
        target_uri,
        StorageConfig(
            storage_type="s3",
            endpoint_url=moto_s3_endpoint,
            access_key=_ACCESS_KEY,
            secret_key=_SECRET_KEY,
            region=_REGION,
            path_style=True,
            storage_driver="fsspec",
        ),
    )
    _assert_thirty_daily_timestamps(dataset, manifest)
