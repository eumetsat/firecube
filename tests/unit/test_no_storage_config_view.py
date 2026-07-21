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

from firecube.core.config import StorageConfig
from firecube.core.credentials import Credentials
from firecube.core.product.identity import ProductIdentity
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.session import StorageSession, storage_config_from_binding
from firecube.core.storage.uri import StorageUri


def _binding() -> StorageBinding:
    return StorageBinding(
        identity=ProductIdentity.from_uri(
            StorageUri.parse("s3://bucket/products/demo.zarr"),
            "zarr",
            product_name="demo",
        ),
        driver=StorageDriverConfig(
            driver="fsspec",
            endpoint_url="https://s3.example.com",
            credentials=Credentials(access_key="AK", secret_key="SK"),
            region="eu-central-1",
            path_style=True,
        ),
    )


def test_storage_config_from_binding_returns_plain_driver_config_without_location_fields() -> None:
    config = storage_config_from_binding(_binding())

    assert type(config) is StorageConfig
    assert config.storage_type == "s3"
    assert config.storage_driver == "fsspec"
    assert config.endpoint_url == "https://s3.example.com"
    assert config.access_key == "AK"
    assert config.secret_key == "SK"
    assert config.region == "eu-central-1"
    assert config.path_style is True
    assert not hasattr(config, "target_path")
    assert not hasattr(config, "bucket")
    assert not hasattr(config, "target_uri")


def test_storage_session_exposes_identity_and_driver_as_separate_contracts() -> None:
    binding = _binding()
    session = StorageSession(binding)

    assert session.product.product_uri.to_str() == "s3://bucket/products/demo.zarr"
    assert session.product.control_root_uri.to_str() == "s3://bucket/products/demo.zarr/.firecube"
    assert session.driver.driver == "fsspec"
    assert session.driver.endpoint_url == "https://s3.example.com"
