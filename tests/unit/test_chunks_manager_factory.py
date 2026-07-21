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

import click

from firecube.core.config import StorageConfig
from firecube.core.storage.uri import StorageUri

manager_module = importlib.import_module("firecube.cli.chunks._manager")
StorageDriverConfig = importlib.import_module(
    "firecube.core.storage.driver_config"
).StorageDriverConfig


class _FakeChunkManager:
    def __init__(self, *, binding, workspace) -> None:
        self.binding = binding
        self.workspace = workspace
        self.base_uri = binding.identity.product_uri

    def close(self) -> None:
        pass


def _make_ctx(workspace):
    return click.Context(click.Command("chunks"), obj={"workspace": workspace})


def test_resolve_manager_uses_default_driver_when_storage_config_missing(
    monkeypatch, temp_workspace
) -> None:
    calls: list[object | None] = []
    original = StorageDriverConfig.from_storage_config_or_default.__func__

    def _recording_factory(cls, config):
        calls.append(config)
        return original(cls, config)

    monkeypatch.setattr(
        StorageDriverConfig,
        "from_storage_config_or_default",
        classmethod(_recording_factory),
    )
    monkeypatch.setattr(manager_module, "ChunkManager", _FakeChunkManager)
    monkeypatch.setattr(manager_module, "get_config", lambda ctx: {})
    monkeypatch.setattr(manager_module, "get_storage_config", lambda *args, **kwargs: None)

    manager = manager_module.resolve_manager(
        _make_ctx(temp_workspace),
        temp_workspace,
        StorageUri.parse("file:///tmp/TEST_PRODUCT.zarr"),
    )

    assert calls == [None]
    assert manager.binding.driver == StorageDriverConfig.from_storage_config_or_default(None)


def test_resolve_manager_uses_storage_config_driver_when_present(
    monkeypatch, temp_workspace
) -> None:
    storage_config = StorageConfig(
        storage_type="s3",
        endpoint_url="https://example.com",
        access_key="ak",
        secret_key="sk",
        region="eu-west-1",
        path_style=False,
        storage_driver="obstore",
    )
    calls: list[object | None] = []
    original = StorageDriverConfig.from_storage_config_or_default.__func__

    def _recording_factory(cls, config):
        calls.append(config)
        return original(cls, config)

    monkeypatch.setattr(
        StorageDriverConfig,
        "from_storage_config_or_default",
        classmethod(_recording_factory),
    )
    monkeypatch.setattr(manager_module, "ChunkManager", _FakeChunkManager)
    monkeypatch.setattr(manager_module, "get_config", lambda ctx: {"storage": True})
    monkeypatch.setattr(
        manager_module,
        "get_storage_config",
        lambda *args, **kwargs: storage_config,
    )

    manager = manager_module.resolve_manager(
        _make_ctx(temp_workspace),
        temp_workspace,
        StorageUri.parse("s3://bucket/TEST_PRODUCT.zarr"),
    )

    assert calls == [storage_config]
    assert manager.binding.driver == StorageDriverConfig.from_storage_config(storage_config)
