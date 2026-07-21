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

ProductResolver = importlib.import_module("firecube.core.product.resolver").ProductResolver


def test_resolve_s3_uri_returns_product_identity() -> None:
    identity = ProductResolver.resolve(
        "s3://bucket/2026/TEST_PRODUCT.zarr", "zarr", product_name="TEST_PRODUCT"
    )

    assert identity.product_name == "TEST_PRODUCT"
    assert identity.product_uri.to_str() == "s3://bucket/2026/TEST_PRODUCT.zarr"
    assert identity.control_root_uri.to_str() == "s3://bucket/2026/TEST_PRODUCT.zarr/.firecube"


def test_resolve_file_uri_returns_product_identity() -> None:
    identity = ProductResolver.resolve("file:///abs/path/cube.zarr", "zarr", product_name="cube")

    assert identity.product_name == "cube"
    assert identity.product_uri.to_str() == "file:///abs/path/cube.zarr"
    assert identity.product_uri.authority is None


def test_resolve_bare_name_rejected_with_full_uri_hint() -> None:
    with pytest.raises(ValueError, match="full URI"):
        ProductResolver.resolve("TEST_PRODUCT.zarr", "zarr", product_name="TEST_PRODUCT")


def test_resolve_absolute_path_mentions_file_uri() -> None:
    with pytest.raises(ValueError, match="file://"):
        ProductResolver.resolve("/abs/path", "zarr", product_name="abs")


def test_resolve_relative_path_mentions_rejected() -> None:
    with pytest.raises(ValueError, match="rejected"):
        ProductResolver.resolve("./relative", "zarr", product_name="relative")


def test_resolve_invalid_uri_propagates_parse_error() -> None:
    with pytest.raises(ValueError, match="unsupported"):
        ProductResolver.resolve("ftp://host/path", "zarr", product_name="path")
