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

import importlib
from pathlib import Path

import pytest

from firecube.core.credentials import Credentials

StorageDriverConfig = importlib.import_module(
    "firecube.core.storage.driver_config"
).StorageDriverConfig
StorageUri = importlib.import_module("firecube.core.storage.uri").StorageUri
_product_target = importlib.import_module("firecube.core.product.target")
ProductTarget = _product_target.ProductTarget
ResolvedProduct = _product_target.ResolvedProduct


@pytest.mark.unit
def test_resolve_remote_prefix_returns_storage_uri() -> None:
    resolved = ProductTarget.resolve(
        "s3://bucket/data/2026/x.zarr",
        StorageDriverConfig(driver="fsspec"),
        product_name="x",
        plugin_default_format="zarr",
    )

    assert isinstance(resolved.product_uri, StorageUri)
    assert isinstance(resolved.output_base_uri, StorageUri)
    assert isinstance(resolved.control_root_uri, StorageUri)
    assert resolved.product_uri.to_str() == "s3://bucket/data/2026/x.zarr"
    assert resolved.output_base_uri.to_str() == "s3://bucket/data/2026"
    assert resolved.control_root_uri.to_str() == "s3://bucket/data/2026/x.zarr/.firecube"


@pytest.mark.unit
def test_resolve_local_absolute(tmp_path: Path) -> None:
    target = (tmp_path / "foo" / "x.zarr").resolve()

    resolved = ProductTarget.resolve(
        str(target),
        StorageDriverConfig(driver="fsspec"),
        product_name="x",
        plugin_default_format="zarr",
    )

    assert resolved.product_uri.protocol == "file"
    assert resolved.output_base_uri.protocol == "file"
    assert resolved.control_root_uri.protocol == "file"
    assert resolved.product_uri.to_str() == target.as_uri()
    assert resolved.output_base_uri.to_str() == target.parent.as_uri()
    assert resolved.control_root_uri.to_str() == f"{target.as_uri()}/.firecube"


@pytest.mark.unit
def test_resolve_file_localhost() -> None:
    target = Path("/tmp/x.zarr")

    resolved = ProductTarget.resolve(
        "file://localhost/tmp/x.zarr",
        StorageDriverConfig(driver="fsspec"),
        product_name="x",
        plugin_default_format="zarr",
    )

    assert resolved.product_uri.protocol == "file"
    assert resolved.product_uri.to_str() == target.as_uri()
    assert resolved.output_base_uri.to_str() == target.parent.as_uri()
    assert resolved.control_root_uri.to_str() == f"{target.as_uri()}/.firecube"


@pytest.mark.unit
def test_resolve_bare_name_with_default_base() -> None:
    resolved = ProductTarget.resolve(
        "x.zarr",
        StorageDriverConfig(driver="fsspec"),
        product_name="x",
        plugin_default_format="zarr",
        default_base_uri=StorageUri.parse("s3://bucket"),
    )

    assert resolved.product_name == "x"
    assert resolved.product_uri.to_str() == "s3://bucket/x.zarr"
    assert resolved.output_base_uri.to_str() == "s3://bucket"
    assert resolved.control_root_uri.to_str() == "s3://bucket/x.zarr/.firecube"


@pytest.mark.unit
def test_resolve_bare_name_without_default_raises() -> None:
    with pytest.raises(ValueError, match="default_base_uri"):
        ProductTarget.resolve(
            "x.zarr",
            StorageDriverConfig(driver="fsspec"),
            product_name="x",
            plugin_default_format="zarr",
        )


@pytest.mark.unit
def test_resolve_obstore_unsupported_protocol_raises() -> None:
    with pytest.raises(ValueError, match="https") as exc_info:
        ProductTarget.resolve(
            "https://example.com/x.zarr",
            StorageDriverConfig(driver="obstore"),
            product_name="x",
            plugin_default_format="zarr",
        )

    assert "unsupported protocol" in str(exc_info.value)


@pytest.mark.unit
def test_resolve_obstore_supported_protocol_ok() -> None:
    resolved = ProductTarget.resolve(
        "s3://bucket/x.zarr",
        StorageDriverConfig(driver="obstore"),
        product_name="x",
        plugin_default_format="zarr",
    )

    assert resolved.product_uri.to_str() == "s3://bucket/x.zarr"
    assert resolved.output_base_uri.to_str() == "s3://bucket"


@pytest.mark.unit
def test_resolved_product_no_credentials() -> None:
    resolved = ProductTarget.resolve(
        "s3://bucket/data/x.zarr",
        StorageDriverConfig(
            driver="fsspec",
            credentials=Credentials(
                access_key="SENTINEL_ACCESS_KEY_DO_NOT_USE",
                secret_key="SENTINEL_SECRET_KEY_DO_NOT_USE",
            ),
        ),
        product_name="x",
        plugin_default_format="zarr",
    )

    assert isinstance(resolved, ResolvedProduct)
    assert resolved.product_uri.to_str() == "s3://bucket/data/x.zarr"
    assert resolved.output_base_uri.to_str() == "s3://bucket/data"
    assert not hasattr(resolved, "credentials")
    assert not hasattr(resolved, "access_key")
    assert "SENTINEL_ACCESS_KEY_DO_NOT_USE" not in repr(resolved)
    assert "SENTINEL_SECRET_KEY_DO_NOT_USE" not in repr(resolved)
