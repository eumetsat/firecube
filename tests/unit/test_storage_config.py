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

from firecube.core.config import StorageConfig


def test_storage_config_repr_omits_credentials() -> None:
    cfg = StorageConfig(
        storage_type="s3",
        access_key="SENTINEL_ACCESS_KEY_DO_NOT_USE",
        secret_key="SENTINEL_SECRET_KEY_DO_NOT_USE",
    )
    rendered = repr(cfg)
    assert "SENTINEL_ACCESS_KEY_DO_NOT_USE" not in rendered
    assert "SENTINEL_SECRET_KEY_DO_NOT_USE" not in rendered
