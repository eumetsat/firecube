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
from dataclasses import FrozenInstanceError, fields

import pytest

from firecube.core.config import StorageConfig
from firecube.core.credentials import Credentials  # pyright: ignore[reportMissingImports]

StorageDriverConfig = importlib.import_module(
    "firecube.core.storage.driver_config"
).StorageDriverConfig


def test_equal_configs_with_same_fields_compare_equal() -> None:
    left = StorageDriverConfig(
        driver="fsspec",
        endpoint_url="https://s3.example.com",
        region="eu-west-1",
        credentials=Credentials(access_key="ak", secret_key="sk"),
        path_style=False,
    )
    right = StorageDriverConfig(
        driver="fsspec",
        endpoint_url="https://s3.example.com",
        region="eu-west-1",
        credentials=Credentials(access_key="ak", secret_key="sk"),
        path_style=False,
    )

    assert left == right


def test_different_credentials_make_configs_unequal() -> None:
    left = StorageDriverConfig(
        driver="fsspec",
        credentials=Credentials(access_key="ak-1", secret_key="sk"),
    )
    right = StorageDriverConfig(
        driver="fsspec",
        credentials=Credentials(access_key="ak-2", secret_key="sk"),
    )

    assert left != right


def test_default_path_style_and_credentials_none() -> None:
    config = StorageDriverConfig(driver="fsspec")

    assert config.driver == "fsspec"
    assert config.endpoint_url is None
    assert config.credentials is None
    assert config.region is None
    assert config.path_style is True


def test_invalid_driver_raises() -> None:
    with pytest.raises(ValueError):
        StorageDriverConfig(driver="obstoore")


def test_invalid_endpoint_raises() -> None:
    with pytest.raises(ValueError):
        StorageDriverConfig(driver="fsspec", endpoint_url="not-a-url")


def test_repr_does_not_leak_credentials() -> None:
    config = StorageDriverConfig(
        driver="fsspec",
        credentials=Credentials(
            access_key="SENTINEL_ACCESS_KEY_DO_NOT_USE", secret_key="SENTINEL_SECRET_KEY_DO_NOT_USE"
        ),
    )

    rendered = repr(config)

    assert "SENTINEL_ACCESS_KEY_DO_NOT_USE" not in rendered
    assert "SENTINEL_SECRET_KEY_DO_NOT_USE" not in rendered


def test_from_storage_config_uses_credentials_boundary() -> None:
    storage_config = StorageConfig(
        storage_type="s3",
        endpoint_url="https://x",
        access_key="ak",
        secret_key="sk",
        region="r",
        path_style=False,
        storage_driver="obstore",
    )

    driver_config = StorageDriverConfig.from_storage_config(storage_config)
    field_names = {field.name for field in fields(StorageDriverConfig)}

    assert driver_config.driver == "obstore"
    assert driver_config.endpoint_url == "https://x"
    assert driver_config.credentials == Credentials(access_key="ak", secret_key="sk")
    assert driver_config.region == "r"
    assert driver_config.path_style is False
    assert "storage_type" not in field_names
    assert "bucket" not in field_names
    assert "target_path" not in field_names
    assert "output_base_uri" not in field_names


def test_frozen_immutable() -> None:
    config = StorageDriverConfig(driver="fsspec")
    driver_attr = "driver"

    with pytest.raises((FrozenInstanceError, AttributeError)):
        setattr(config, driver_attr, "obstore")
