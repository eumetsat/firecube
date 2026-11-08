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

"""Migration tests for CLI bridge consumers (T14 + T16)."""

from __future__ import annotations

from pathlib import Path

import click
import pytest

from firecube.core.config import StorageConfig


class TestBaseUriFromStorageConfig:
    """Behavior preservation: ``_base_uri_from_storage_config`` produces equivalent
    URIs to the pre-T14 getattr-based implementation, via the identity seam."""

    def test_s3_with_bucket(self) -> None:
        from firecube.cli.chunks._manager import _base_uri_from_storage_config

        cfg = StorageConfig(storage_type="s3")
        cfg.bucket = "my-bucket"  # type: ignore[attr-defined]
        uri = _base_uri_from_storage_config(cfg)
        assert uri is not None
        assert uri.protocol == "s3"
        assert uri.authority == "my-bucket"

    def test_local_with_target_path(self) -> None:
        from firecube.cli.chunks._manager import _base_uri_from_storage_config

        cfg = StorageConfig(storage_type="local")
        cfg.target_path = "/data/products"  # type: ignore[attr-defined]
        uri = _base_uri_from_storage_config(cfg)
        assert uri is not None
        assert uri.protocol == "file"
        assert uri.path == "/data/products"

    def test_local_missing_target_path_returns_none(self) -> None:
        from firecube.cli.chunks._manager import _base_uri_from_storage_config

        cfg = StorageConfig(storage_type="local")
        assert _base_uri_from_storage_config(cfg) is None

    def test_s3_missing_bucket_returns_none(self) -> None:
        from firecube.cli.chunks._manager import _base_uri_from_storage_config

        cfg = StorageConfig(storage_type="s3")
        assert _base_uri_from_storage_config(cfg) is None


class TestCtxDebugLogResolvesWithoutBridgeReads:
    """Smoke test: ``get_storage_config`` resolves and caches without raising
    AttributeError when the resolved ``StorageConfig`` carries the runtime
    bridge attrs (target_path/bucket) attached via ``build_storage_config``."""

    def test_resolve_s3_storage_config_does_not_crash(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from firecube.cli import _ctx

        for k in (
            "FIRECUBE_STORAGE_TYPE",
            "FIRECUBE_BUCKET",
            "FIRECUBE_ENDPOINT_URL",
            "FIRECUBE_ACCESS_KEY",
            "FIRECUBE_SECRET_KEY",
        ):
            monkeypatch.delenv(k, raising=False)

        monkeypatch.setenv("FIRECUBE_STORAGE_TYPE", "s3")
        monkeypatch.setenv("FIRECUBE_BUCKET", "smoke-bucket")
        monkeypatch.setenv("FIRECUBE_ACCESS_KEY", "AK")
        monkeypatch.setenv("FIRECUBE_SECRET_KEY", "SK")

        ctx = click.Context(click.Command("smoke"), obj={})
        cfg = _ctx.get_storage_config(ctx, set_global=False)

        assert cfg.storage_type == "s3"
        assert ctx.obj["storage_config"] is cfg

    def test_resolve_local_storage_config_does_not_crash(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        from firecube.cli import _ctx

        for k in (
            "FIRECUBE_STORAGE_TYPE",
            "FIRECUBE_BUCKET",
            "FIRECUBE_TARGET_PATH",
        ):
            monkeypatch.delenv(k, raising=False)

        target = tmp_path / "data"
        target.mkdir()
        monkeypatch.setenv("FIRECUBE_STORAGE_TYPE", "local")
        monkeypatch.setenv("FIRECUBE_TARGET_PATH", str(target))

        ctx = click.Context(click.Command("smoke"), obj={})
        cfg = _ctx.get_storage_config(ctx, set_global=False)

        assert cfg.storage_type == "local"
