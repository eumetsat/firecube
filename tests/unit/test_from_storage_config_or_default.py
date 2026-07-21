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

from firecube.core.config import StorageConfig

StorageDriverConfig = importlib.import_module(
    "firecube.core.storage.driver_config"
).StorageDriverConfig


def test_factory_with_none_returns_default_fsspec() -> None:
    config = StorageDriverConfig.from_storage_config_or_default(None)

    assert config.driver == "fsspec"


def test_factory_with_storage_config_matches_from_storage_config() -> None:
    storage_config = StorageConfig(
        storage_type="s3",
        endpoint_url="https://x",
        access_key="ak",
        secret_key="sk",
        region="r",
        path_style=False,
        storage_driver="obstore",
    )

    assert StorageDriverConfig.from_storage_config_or_default(
        storage_config
    ) == StorageDriverConfig.from_storage_config(storage_config)


def test_factory_default_has_no_credentials() -> None:
    config = StorageDriverConfig.from_storage_config_or_default(None)

    assert config.credentials is None


def test_factory_default_has_no_endpoint_url() -> None:
    config = StorageDriverConfig.from_storage_config_or_default(None)

    assert config.endpoint_url is None
