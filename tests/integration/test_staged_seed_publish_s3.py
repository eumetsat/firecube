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

"""S3-marked seed-then-publish integration test (staged workflow).

Verifies the full staged workflow against a real S3-shaped HTTP endpoint:

    Run 1: fresh staged ingest of 10 timestamps to ``s3://bucket/product.zarr``.
    Run 2: staged ingest of 5 new timestamps [10-14] with ``resume_existing=true``.
        - workspace metadata seeded from S3 target,
        - touched chunks seeded from S3 target,
        - new batch appended into workspace,
        - workspace promoted back to S3 target.

Assertions after Run 2:

* the S3 target's cumulative time shape is >= 15,
* group attributes from the first write survive the append,
* static coordinates (``lat``, ``lon``) are byte-identical to Run 1,
* the pre-existing precipitation slices [0:10] are preserved (seeding worked
  — a lost seed would have overwritten them with fill values on promotion).

Uses ``ThreadedMotoServer`` (not ``moto.mock_aws()``) because the staged
promotion path can traverse obstore, which is a Rust extension that bypasses
Python-level mocks and needs a real HTTP endpoint.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import boto3
import numpy as np
import pytest
import xarray as xr
from moto.server import ThreadedMotoServer

from firecube.core.config import StorageConfig
from firecube.core.credentials import Credentials
from firecube.core.filesystem.store_factory import create_zarr_store
from firecube.core.product.identity import ProductIdentity
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.session import StorageSession
from firecube.core.storage.uri import StorageUri
from tests.integration._staged_helpers import (
    generate_days,
    run_staged_ingest,
    s3_env,
)

pytestmark = pytest.mark.s3

_BUCKET = "seed-publish-bucket"
_PRODUCT_KEY = "seed-publish.zarr"
_REGION = "us-east-1"
_ACCESS_KEY = "testing"
_SECRET_KEY = "testing"


@pytest.fixture(scope="module")
def moto_s3_endpoint() -> Iterator[str]:
    """Real HTTP S3 endpoint served by moto (obstore-compatible)."""
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
    """Bucket lifetime scoped to each test; fixture yields the boto3 client."""
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


@pytest.fixture
def s3_session(moto_s3_endpoint: str, s3_bucket: Any) -> StorageSession:
    """StorageSession bound to the S3 target under test."""
    del s3_bucket
    uri = StorageUri(protocol="s3", authority=_BUCKET, path=f"/{_PRODUCT_KEY}")
    binding = StorageBinding(
        identity=ProductIdentity.from_uri(uri, "zarr", product_name="seed_publish"),
        driver=StorageDriverConfig(
            driver="fsspec",
            endpoint_url=moto_s3_endpoint,
            credentials=Credentials(access_key=_ACCESS_KEY, secret_key=_SECRET_KEY),
            region=_REGION,
            path_style=True,
        ),
    )
    return StorageSession(binding)


def _s3_env(endpoint: str) -> dict[str, str]:
    return s3_env(
        endpoint,
        access_key=_ACCESS_KEY,
        secret_key=_SECRET_KEY,
        region=_REGION,
    )


def _s3_storage_config(endpoint: str) -> StorageConfig:
    return StorageConfig(
        storage_type="s3",
        endpoint_url=endpoint,
        access_key=_ACCESS_KEY,
        secret_key=_SECRET_KEY,
        region=_REGION,
        path_style=True,
        storage_driver="fsspec",
    )


def _open_default_group(target_uri: str, endpoint: str) -> xr.Dataset:
    handle = create_zarr_store(
        uri=target_uri,
        storage_config=_s3_storage_config(endpoint),
        mode="r",
    )
    return xr.open_zarr(
        **handle.zarr_kwargs(),
        group="default",
        zarr_format=3,
        consolidated=False,
    )


def test_staged_seed_publish_preserves_s3_target(
    tmp_path: Path,
    moto_s3_endpoint: str,
    s3_bucket: Any,
    s3_session: StorageSession,
) -> None:
    del s3_bucket
    """Full seed-then-publish flow: 10 timestamps then +5 via staged mode.

    Guarantees a lost seed would fail the assertion — Run 2 writes into the
    same chunk that already holds Run 1's slots (``timeseries`` layout chunks
    time by 365), so if the workspace opened empty the promotion would replace
    slots [0:10] with fill values.
    """
    source_first = tmp_path / "input-first"
    source_second = tmp_path / "input-second"
    target_uri = f"s3://{_BUCKET}/{_PRODUCT_KEY}"

    # Run 1 — fresh staged ingest of 10 timestamps [days 1-10].
    generate_days(source_first, 1, 10)
    manifest_first = run_staged_ingest(
        "precip_daily",
        source_first,
        target_uri,
        product_name="seed_publish",
        storage_type="s3",
        env=_s3_env(moto_s3_endpoint),
        resume=False,
    )
    assert manifest_first["metrics"]["count"] == 10

    ds_after_first = _open_default_group(target_uri, moto_s3_endpoint)
    assert ds_after_first["precipitation"].shape[0] == 10
    first_attrs = dict(ds_after_first.attrs)
    first_lat = np.asarray(ds_after_first["lat"].values)
    first_lon = np.asarray(ds_after_first["lon"].values)
    first_precip_prefix = np.asarray(ds_after_first["precipitation"].values[:10]).copy()

    # Run 2 — 5 new timestamps [days 11-15] via staged resume.
    generate_days(source_second, 11, 15)
    manifest_second = run_staged_ingest(
        "precip_daily",
        source_second,
        target_uri,
        product_name="seed_publish",
        storage_type="s3",
        env=_s3_env(moto_s3_endpoint),
        resume=True,
    )
    assert manifest_second["metrics"]["count"] == 5

    ds_final = _open_default_group(target_uri, moto_s3_endpoint)

    # Cumulative time shape holds Run 1 + Run 2.
    assert ds_final["precipitation"].shape[0] >= 15, (
        f"Expected cumulative time shape >= 15, got {ds_final['precipitation'].shape[0]}. "
        "S3 target metadata was clobbered by workspace metadata during promotion."
    )

    # Group attrs from the first write survive the append.
    assert dict(ds_final.attrs) == first_attrs

    # Static coordinates untouched.
    assert np.array_equal(np.asarray(ds_final["lat"].values), first_lat)
    assert np.array_equal(np.asarray(ds_final["lon"].values), first_lon)

    # Existing precipitation slots preserved — the seed-touched-chunks guarantee.
    final_precip_prefix = np.asarray(ds_final["precipitation"].values[:10])
    assert np.array_equal(final_precip_prefix, first_precip_prefix), (
        "Existing precipitation values were overwritten by staged promotion. "
        "seed_touched_data_chunks did not seed chunk 0 from the S3 target."
    )

    # StorageSession sees the promoted target and no leftover ``.firecube/staged``
    # tree from the second run.
    root_uri = StorageUri(protocol="s3", authority=_BUCKET, path=f"/{_PRODUCT_KEY}")
    found = [uri.to_str() for uri in s3_session.fs().find(root_uri)]
    assert any("/precipitation/" in key for key in found), (
        f"No precipitation chunks found under {root_uri.to_str()}: {found[:5]}"
    )
    assert not any(".firecube/staged/" in key for key in found), (
        f"Staged workspace residue leaked into promoted S3 target: "
        f"{[k for k in found if '.firecube/staged/' in k][:5]}"
    )
