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

import pytest

from firecube.core.storage.uri import StorageUri

ProductIdentity = importlib.import_module("firecube.core.product.identity").ProductIdentity


@pytest.mark.unit
def test_from_uri_uses_explicit_product_name_and_control_root() -> None:
    identity = ProductIdentity.from_uri(
        StorageUri.parse("s3://bucket/data/2026/TEST_PRODUCT.zarr"),
        "zarr",
        product_name="TEST_PRODUCT",
    )

    assert identity.product_name == "TEST_PRODUCT"
    assert identity.product_uri.to_str() == "s3://bucket/data/2026/TEST_PRODUCT.zarr"
    assert identity.control_root_uri.to_str() == "s3://bucket/data/2026/TEST_PRODUCT.zarr/.firecube"
    assert identity.format == "zarr"


@pytest.mark.unit
def test_from_uri_is_frozen_hashable_and_equal() -> None:
    left = ProductIdentity.from_uri(
        StorageUri.parse("s3://bucket/data/x.zarr"), "zarr", product_name="x"
    )
    right = ProductIdentity.from_uri(
        StorageUri.parse("s3://bucket/data/x.zarr"), "zarr", product_name="x"
    )

    assert left == right
    assert hash(left) == hash(right)
    assert left in {right}


@pytest.mark.unit
def test_invalid_format_raises_value_error() -> None:
    with pytest.raises(ValueError, match="Invalid format"):
        ProductIdentity.from_uri(
            StorageUri.parse("s3://bucket/data/x.zarr"), "csv", product_name="x"
        )


@pytest.mark.unit
def test_no_output_base_uri_field() -> None:
    identity = ProductIdentity.from_uri(
        StorageUri.parse("s3://bucket/x.zarr"),
        "zarr",
        product_name="x",
    )

    assert identity.product_uri.to_str() == "s3://bucket/x.zarr"
    assert identity.control_root_uri.to_str() == "s3://bucket/x.zarr/.firecube"
    assert not hasattr(identity, "output_base_uri")


@pytest.mark.unit
def test_rejects_uris_without_product_name() -> None:
    with pytest.raises(ValueError, match="product_name is required"):
        ProductIdentity.from_uri(StorageUri.parse("s3://bucket/x.zarr"), "zarr", product_name="")


@pytest.mark.unit
def test_from_uri_requires_product_name_argument() -> None:
    with pytest.raises(TypeError):
        ProductIdentity.from_uri(StorageUri.parse("s3://bucket/x.zarr"), "zarr")  # type: ignore[call-arg]
