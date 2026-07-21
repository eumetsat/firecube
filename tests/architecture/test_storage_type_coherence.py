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

# pyright: reportMissingImports=false
"""Cross-layer coherence for ``storage_type``.

A user-supplied ``--storage-type`` flows through four layers:

1. ``IngestCommandConfig._VALID_STORAGE_TYPES`` (CLI Click choice surface)
2. ``StorageConfig.validate()`` (runtime config validator)
3. ``StorageUri.SUPPORTED_PROTOCOLS`` (URI scheme parser)
4. ``create_filesystem(binding)`` (driver-aware filesystem factory)

If any of these four disagree, a user can pass a flag the CLI accepts but no
downstream code can honour. This module asserts the four layers stay in sync.

The CLI surface is the current public contract. URI protocols may include
additional parser support, but every CLI-advertised storage type must map to a
supported URI protocol and a filesystem implementation.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import boto3
import pytest
from moto.server import ThreadedMotoServer

from firecube.cli._command_schemas import IngestCommandConfig
from firecube.core.config import StorageConfig
from firecube.core.credentials import Credentials
from firecube.core.filesystem.ops import create_filesystem
from firecube.core.product.identity import ProductIdentity
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.uri import SUPPORTED_PROTOCOLS, StorageUri

# Mapping from the CLI's ``storage_type`` to the URI scheme used by
# ``StorageUri``. ``local`` is a CLI-level label (file:// or memory://);
# we test against ``file`` because that is what the filesystem factory uses
# for on-disk targets. ``az`` deliberately maps to ``"az"`` to demonstrate the
# cross-layer incoherence pre-T22 ("az" is NOT in SUPPORTED_PROTOCOLS).
_STORAGE_TYPE_TO_URI_PROTOCOL: dict[str, str] = {
    "local": "file",
    "s3": "s3",
    "gs": "gs",
    "az": "az",
}

_MOTO_BUCKET = "firecube-coherence-bucket"
_MOTO_ACCESS_KEY = "testing"
_MOTO_SECRET_KEY = "testing"
_MOTO_REGION = "us-east-1"


@pytest.fixture(scope="module")
def moto_s3_endpoint() -> Iterator[str]:
    """Run an in-process moto S3 endpoint for the s3 storage_type case."""
    server = ThreadedMotoServer(port=0)
    server.start()
    host, port = server.get_host_and_port()
    endpoint = f"http://{host}:{port}"
    try:
        yield endpoint
    finally:
        server.stop()


@pytest.fixture
def moto_s3_bucket(moto_s3_endpoint: str) -> Iterator[Any]:
    """Create-and-clean a single bucket per test against the moto endpoint."""
    client = boto3.client(
        "s3",
        endpoint_url=moto_s3_endpoint,
        aws_access_key_id=_MOTO_ACCESS_KEY,
        aws_secret_access_key=_MOTO_SECRET_KEY,
        region_name=_MOTO_REGION,
    )
    client.create_bucket(Bucket=_MOTO_BUCKET)
    try:
        yield client
    finally:
        objects = client.list_objects_v2(Bucket=_MOTO_BUCKET).get("Contents", [])
        if objects:
            client.delete_objects(
                Bucket=_MOTO_BUCKET,
                Delete={"Objects": [{"Key": o["Key"]} for o in objects]},
            )
        client.delete_bucket(Bucket=_MOTO_BUCKET)


def test_cli_choices_equal_storage_config_validate_accepted_types() -> None:
    """CLI ``_VALID_STORAGE_TYPES`` MUST match the set ``StorageConfig.validate()`` accepts.

    Before T22 this fails: the CLI advertises ``{"local", "s3", "gs", "az"}``
    while ``StorageConfig.validate()`` only accepts ``{"local", "s3"}``. After
    T22 the CLI surface narrows to ``{"local", "s3"}`` and the two layers agree.
    """
    cli_choices = set(IngestCommandConfig._VALID_STORAGE_TYPES)

    # Derive the StorageConfig-accepted types by probing validate() over the
    # current CLI surface plus the historically-considered protocols. Using
    # validate() as the single source of truth avoids hardcoding the expected
    # set on the right-hand side.
    candidates = cli_choices | {"local", "s3", "gs", "az"}
    accepted_by_storage_config: set[str] = set()
    for candidate in candidates:
        try:
            StorageConfig(storage_type=candidate).validate()
        except ValueError:
            continue
        accepted_by_storage_config.add(candidate)

    assert cli_choices == accepted_by_storage_config, (
        "CLI _VALID_STORAGE_TYPES diverges from StorageConfig.validate()-accepted set: "
        f"CLI={sorted(cli_choices)}, StorageConfig={sorted(accepted_by_storage_config)}. "
        "Either narrow the CLI choices (T22) or extend StorageConfig.validate() to match."
    )


def test_storage_uri_protocols_superset_of_cli_choices() -> None:
    """``StorageUri.SUPPORTED_PROTOCOLS`` must cover every CLI-advertised storage_type.

    A user must not be able to pass a CLI storage type for which the URI parser
    rejects every matching URI.
    """
    unmapped = [
        storage_type
        for storage_type in IngestCommandConfig._VALID_STORAGE_TYPES
        if _STORAGE_TYPE_TO_URI_PROTOCOL[storage_type] not in SUPPORTED_PROTOCOLS
    ]
    assert not unmapped, (
        f"CLI storage_type(s) {sorted(unmapped)!r} map to URI protocols outside "
        f"StorageUri.SUPPORTED_PROTOCOLS={sorted(SUPPORTED_PROTOCOLS)}. "
        "Either add the protocol or remove the CLI choice (T22 removes 'az')."
    )


@pytest.mark.parametrize(
    "storage_type",
    sorted(IngestCommandConfig._VALID_STORAGE_TYPES),
)
def test_create_filesystem_supports_all_cli_choices(
    storage_type: str,
    tmp_path: Path,
    moto_s3_endpoint: str,
    moto_s3_bucket: Any,
) -> None:
    """``create_filesystem()`` must succeed for every CLI-advertised storage_type.

    Parametrization tracks ``_VALID_STORAGE_TYPES`` so the test surface narrows
    automatically when T22 lands. Pre-T22, ``gs``/``az`` raise because they
    have no concrete backend / URI parser entry; post-T22 only ``local``/``s3``
    remain and both succeed end-to-end (local via tmp_path, s3 via moto).
    """
    if storage_type == "local":
        uri = StorageUri.from_local_path(tmp_path / "product.zarr")
        driver_config = StorageDriverConfig(driver="fsspec")
    elif storage_type == "s3":
        uri = StorageUri(protocol="s3", authority=_MOTO_BUCKET, path="/data/product.zarr")
        driver_config = StorageDriverConfig(
            driver="fsspec",
            endpoint_url=moto_s3_endpoint,
            credentials=Credentials(
                access_key=_MOTO_ACCESS_KEY,
                secret_key=_MOTO_SECRET_KEY,
            ),
            region=_MOTO_REGION,
            path_style=True,
        )
    else:
        # gs / az: no implementation. Construct a URI with the advertised
        # protocol and let the factory raise loudly. Pre-T22 this is expected
        # to fail; post-T22 these cases vanish from the parametrize matrix.
        protocol = _STORAGE_TYPE_TO_URI_PROTOCOL[storage_type]
        uri = StorageUri(protocol=protocol, authority="example-bucket", path="/data/product.zarr")
        driver_config = StorageDriverConfig(driver="fsspec")

    binding = StorageBinding(
        identity=ProductIdentity.from_uri(uri, "zarr", product_name="test_product"),
        driver=driver_config,
    )

    fs = create_filesystem(binding)
    assert fs is not None
