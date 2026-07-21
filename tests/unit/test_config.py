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

import os

from firecube.core.config import build_storage_config


def test_storage_type_precedence_env_over_config(monkeypatch) -> None:
    monkeypatch.setenv("FIRECUBE_STORAGE_TYPE", "s3")

    cfg = {"storage": {"type": "local"}}

    storage_config = build_storage_config(cfg, os.environ, {})

    assert storage_config.storage_type == "s3"


def test_storage_type_precedence_cli_over_env_over_config(monkeypatch) -> None:
    monkeypatch.setenv("FIRECUBE_STORAGE_TYPE", "s3")

    cfg = {"storage": {"type": "local"}}
    overrides = {"storage_type": "local"}

    storage_config = build_storage_config(cfg, os.environ, overrides)

    assert storage_config.storage_type == "local"


def test_storage_type_falls_back_to_config_when_no_env_or_cli() -> None:
    cfg = {"storage": {"type": "local"}}

    storage_config = build_storage_config(cfg, {}, {})

    assert storage_config.storage_type == "local"
