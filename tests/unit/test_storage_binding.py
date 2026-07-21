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
import pickle

from firecube.core.credentials import Credentials  # pyright: ignore[reportMissingImports]
from firecube.core.storage.uri import StorageUri

ProductIdentity = importlib.import_module("firecube.core.product.identity").ProductIdentity
StorageBinding = importlib.import_module("firecube.core.storage.binding").StorageBinding
StorageCacheKey = importlib.import_module("firecube.core.storage.cache_key").StorageCacheKey


def _binding():
    identity = ProductIdentity.from_uri(
        StorageUri.parse("s3://bucket/data/x.zarr"), "zarr", product_name="x"
    )
    driver = importlib.import_module("firecube.core.storage.driver_config").StorageDriverConfig(
        driver="obstore",
        endpoint_url="https://s3.example.com",
        credentials=Credentials(access_key="ak", secret_key="sk"),
        region="eu-west-1",
    )
    return StorageBinding(identity=identity, driver=driver)


def test_bindings_with_same_fields_compare_equal() -> None:
    left = _binding()
    right = _binding()

    assert left == right


def test_binding_is_picklable() -> None:
    binding = _binding()

    data = pickle.dumps(binding)

    assert pickle.loads(data) == binding


def test_binding_is_hashable() -> None:
    binding = _binding()

    assert binding in {binding}
    assert hash(binding)


def test_binding_cache_key_returns_storage_cache_key() -> None:
    binding = _binding()

    cache_key = binding.cache_key()

    assert isinstance(cache_key, StorageCacheKey)
