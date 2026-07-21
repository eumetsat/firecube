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

import pytest

from firecube.core.config import StorageConfig
from firecube.core.storage.driver_config import StorageDriverConfig


def test_direct_construction_outside_tests_rejected() -> None:
    namespace = {
        "StorageDriverConfig": StorageDriverConfig,
        "__file__": "src/firecube/_lockdown_probe.py",
    }
    code = compile(
        "def construct():\n    return StorageDriverConfig(driver='fsspec')\n",
        "src/firecube/_lockdown_probe.py",
        "exec",
    )
    exec(code, namespace)

    with pytest.raises(RuntimeError, match="StorageDriverConfig must be constructed"):
        namespace["construct"]()


def test_factory_from_storage_config_works() -> None:
    storage_config = StorageConfig(storage_type="local")

    config = StorageDriverConfig.from_storage_config(storage_config)

    assert config.driver == "fsspec"


def test_factory_from_storage_config_or_default_none_works() -> None:
    config = StorageDriverConfig.from_storage_config_or_default(None)

    assert config.driver == "fsspec"
