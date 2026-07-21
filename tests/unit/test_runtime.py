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

"""Characterization tests for runtime.py env-export functions."""

from __future__ import annotations

from pathlib import Path

import pytest

from firecube.core.config import StorageConfig
from firecube.core.product.identity import ProductIdentity
from firecube.core.runtime import (
    _target_uri_from_config,
    export_storage_config_to_env,
    resolve_storage_config,
)
from firecube.core.storage.uri import StorageUri


def _identity(uri: str, fmt: str = "zarr") -> ProductIdentity:
    return ProductIdentity.from_uri(StorageUri.parse(uri), format=fmt, product_name="test_product")


class TestTargetUriFromConfig:
    def test_s3_identity_returns_uri_with_scheme(self) -> None:
        identity = _identity("s3://my-bucket/data/test.zarr")
        assert _target_uri_from_config(identity) == "s3://my-bucket/data/test.zarr"

    def test_s3_identity_with_bucket_only_returns_bucket_uri(self) -> None:
        # Bucket-only base URI (no path beyond /) preserves legacy
        # `s3://bucket` (no trailing slash) when parsed from that bare form.
        identity = ProductIdentity(
            product_name="",
            product_uri=StorageUri.parse("s3://my-bucket"),
            control_root_uri=StorageUri.parse("s3://my-bucket/.firecube"),
            format="zarr",
        )
        assert _target_uri_from_config(identity) == "s3://my-bucket"

    def test_local_identity_returns_bare_path(self) -> None:
        # Legacy FIRECUBE_TARGET_URI was a bare path for local storage
        # (no `file://` prefix). Preserve that to avoid breaking child
        # processes that consume the variable.
        identity = _identity("file:///data/products")
        assert _target_uri_from_config(identity) == "/data/products"


class TestExportStorageConfigToEnv:
    def test_s3_full_uri_exports_bucket_and_storage_type(self) -> None:
        identity = _identity("s3://my-bucket/data/test.zarr")
        cfg = StorageConfig(storage_type="s3")
        env: dict[str, str] = {}

        export_storage_config_to_env(cfg, identity, env=env)

        assert env["FIRECUBE_BUCKET"] == "my-bucket"
        assert env["FIRECUBE_STORAGE_TYPE"] == "s3"

    def test_s3_with_credentials_exports_aws_aliases(self) -> None:
        identity = _identity("s3://my-bucket/data/test.zarr")
        cfg = StorageConfig(
            storage_type="s3",
            endpoint_url="https://s3.example.com",
            access_key="AK",
            secret_key="SK",
            region="eu-central-1",
            path_style=True,
        )
        env: dict[str, str] = {}

        export_storage_config_to_env(cfg, identity, env=env)

        assert env["FIRECUBE_ACCESS_KEY"] == "AK"
        assert env["FIRECUBE_SECRET_KEY"] == "SK"
        assert env["FIRECUBE_ENDPOINT_URL"] == "https://s3.example.com"
        assert env["FIRECUBE_REGION"] == "eu-central-1"
        assert env["FIRECUBE_PATH_STYLE"] == "true"
        assert env["AWS_ACCESS_KEY_ID"] == "AK"
        assert env["AWS_SECRET_ACCESS_KEY"] == "SK"
        assert env["AWS_DEFAULT_REGION"] == "eu-central-1"
        assert env["AWS_ENDPOINT_URL"] == "https://s3.example.com"
        assert env["AWS_S3_ADDRESSING_STYLE"] == "path"

    def test_local_exports_target_path_bare(self) -> None:
        identity = _identity("file:///data/products")
        cfg = StorageConfig(storage_type="local")
        env: dict[str, str] = {}

        export_storage_config_to_env(cfg, identity, env=env)

        assert env["FIRECUBE_STORAGE_TYPE"] == "local"
        assert env["FIRECUBE_TARGET_PATH"] == "/data/products"
        # FIRECUBE_TARGET_URI keeps the legacy bare-path form for local.
        assert env["FIRECUBE_TARGET_URI"] == "/data/products"
        assert "FIRECUBE_BUCKET" not in env

    def test_identity_none_skips_location_fields(self) -> None:
        cfg = StorageConfig(storage_type="local")
        env: dict[str, str] = {}

        export_storage_config_to_env(cfg, identity=None, env=env)

        assert env["FIRECUBE_STORAGE_TYPE"] == "local"
        assert "FIRECUBE_BUCKET" not in env
        assert "FIRECUBE_TARGET_PATH" not in env
        assert "FIRECUBE_TARGET_URI" not in env

    def test_path_style_false_emits_virtual_addressing(self) -> None:
        identity = _identity("s3://my-bucket/x.zarr")
        cfg = StorageConfig(storage_type="s3", path_style=False)
        env: dict[str, str] = {}

        export_storage_config_to_env(cfg, identity, env=env)

        assert env["FIRECUBE_PATH_STYLE"] == "false"
        assert env["AWS_S3_ADDRESSING_STYLE"] == "virtual"

    def test_omitted_env_writes_to_os_environ(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("FIRECUBE_BUCKET", raising=False)
        identity = _identity("s3://target-bucket/path/x.zarr")
        cfg = StorageConfig(storage_type="s3")

        export_storage_config_to_env(cfg, identity)

        import os

        assert os.environ.get("FIRECUBE_BUCKET") == "target-bucket"


class TestResolveStorageConfigEnvExport:
    def test_s3_resolve_exports_firecube_and_aws_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        for k in (
            "FIRECUBE_BUCKET",
            "FIRECUBE_TARGET_URI",
            "FIRECUBE_STORAGE_TYPE",
            "AWS_ACCESS_KEY_ID",
        ):
            monkeypatch.delenv(k, raising=False)

        import os

        cfg = resolve_storage_config(
            env={
                "FIRECUBE_STORAGE_TYPE": "s3",
                "FIRECUBE_BUCKET": "my-bucket",
                "FIRECUBE_ACCESS_KEY": "AK",
                "FIRECUBE_SECRET_KEY": "SK",
            },
            export_env=True,
            set_global=False,
        )

        assert cfg.storage_type == "s3"
        assert os.environ.get("FIRECUBE_BUCKET") == "my-bucket"
        assert os.environ.get("FIRECUBE_STORAGE_TYPE") == "s3"
        assert os.environ.get("FIRECUBE_TARGET_URI") == "s3://my-bucket"
        assert os.environ.get("AWS_ACCESS_KEY_ID") == "AK"

    def test_local_resolve_exports_target_path(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        for k in ("FIRECUBE_TARGET_PATH", "FIRECUBE_TARGET_URI"):
            monkeypatch.delenv(k, raising=False)

        import os

        target = tmp_path / "data"
        target.mkdir()
        cfg = resolve_storage_config(
            env={
                "FIRECUBE_STORAGE_TYPE": "local",
                "FIRECUBE_TARGET_PATH": str(target),
            },
            export_env=True,
            set_global=False,
        )

        assert cfg.storage_type == "local"
        assert os.environ.get("FIRECUBE_TARGET_PATH") == str(target)
        # Legacy bare-path semantics for local FIRECUBE_TARGET_URI.
        assert os.environ.get("FIRECUBE_TARGET_URI") == str(target)
