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

import os

import pytest

from firecube.core.config import StorageConfig, build_storage_config


def test_storage_config_repr_omits_credentials() -> None:
    cfg = StorageConfig(
        storage_type="s3",
        access_key="SENTINEL_ACCESS_KEY_DO_NOT_USE",
        secret_key="SENTINEL_SECRET_KEY_DO_NOT_USE",
    )
    rendered = repr(cfg)
    assert "SENTINEL_ACCESS_KEY_DO_NOT_USE" not in rendered
    assert "SENTINEL_SECRET_KEY_DO_NOT_USE" not in rendered


@pytest.mark.parametrize(
    "name", ["FIRECUBE_ACCESS_KEY", "FIRECUBE_SECRET_KEY", "access_key", "secret_key"]
)
def test_half_credential_pair_names_env_vars_and_keys(
    name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FIRECUBE_STORAGE_TYPE", "s3")
    monkeypatch.setenv("FIRECUBE_ACCESS_KEY", "only-the-access-key")
    monkeypatch.delenv("FIRECUBE_SECRET_KEY", raising=False)
    cfg = build_storage_config({}, os.environ, {})

    with pytest.raises(ValueError) as excinfo:
        cfg.validate()

    assert name in str(excinfo.value)
