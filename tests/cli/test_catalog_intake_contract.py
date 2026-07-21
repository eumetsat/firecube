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

from pathlib import Path
from typing import Any

from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.intake import CatalogSourceSpec
from firecube.core.storage.session import StorageSession


def _invoke_intake(args: list[str]):
    return CliRunner().invoke(cli, ["catalog", "intake", *args])


def test_file_uri_no_storage_flags(tmp_local_zarr: Path) -> None:
    catalog_path = tmp_local_zarr.parent / "catalog.yaml"
    result = _invoke_intake(
        [
            "cli_test_plugin",
            "-p",
            tmp_local_zarr.as_uri(),
            "-o",
            catalog_path.as_uri(),
            "--collection-id",
            "test-coll",
        ]
    )

    assert "Missing option" not in result.output
    assert result.exit_code == 0, result.output
    assert catalog_path.exists()


def test_s3_uri_infers_storage_type(monkeypatch: Any, tmp_path: Path) -> None:
    from firecube.cli import catalog as catalog_module

    class DummyIngestor:
        pass

    captured: dict[str, str] = {}

    def fake_discover_ingestors() -> dict[str, type[DummyIngestor]]:
        return {"cli_test_plugin": DummyIngestor}

    def fake_discover_catalog_groups(
        store_uri: str,
        *,
        storage_session: StorageSession | None = None,
        storage_config: Any | None = None,
    ) -> list[str]:
        assert store_uri == "s3://bucket/x.zarr"
        assert isinstance(storage_session, StorageSession)
        assert storage_config is None
        captured["session_product_uri"] = storage_session.product.product_uri.to_str()
        captured["session_protocol"] = storage_session.product.product_uri.protocol
        captured["session_driver"] = storage_session.driver.driver
        return ["g1"]

    def fake_build_catalog_source_specs(
        *,
        plugin_name: str,
        product: str,
        store_uri: str,
        groups: list[str],
        group_info_resolver: Any | None = None,
        storage_session: StorageSession | None = None,
        storage_config: Any | None = None,
    ) -> list[CatalogSourceSpec]:
        assert plugin_name == "cli_test_plugin"
        assert product == "s3://bucket/x.zarr"
        assert store_uri == "s3://bucket/x.zarr"
        assert groups == ["g1"]
        assert group_info_resolver is None
        assert isinstance(storage_session, StorageSession)
        assert storage_config is None
        return [CatalogSourceSpec(name="cli_test_plugin_g1", description="g1", group="g1")]

    monkeypatch.setattr(catalog_module, "discover_ingestors", fake_discover_ingestors)
    monkeypatch.setattr(catalog_module, "discover_catalog_groups", fake_discover_catalog_groups)
    monkeypatch.setattr(
        catalog_module, "build_catalog_source_specs", fake_build_catalog_source_specs
    )

    catalog_path = tmp_path / "catalog.yaml"
    result = _invoke_intake(
        [
            "cli_test_plugin",
            "-p",
            "s3://bucket/x.zarr",
            "-o",
            catalog_path.as_uri(),
            "--collection-id",
            "c",
        ]
    )

    assert result.exit_code == 0, result.output
    assert catalog_path.exists()
    assert captured == {
        "session_product_uri": "s3://bucket/x.zarr",
        "session_protocol": "s3",
        "session_driver": "fsspec",
    }


def test_passes_typed_storage_session_downstream(
    monkeypatch: Any,
    tmp_local_zarr: Path,
) -> None:
    from firecube.cli import catalog as catalog_module

    class DummyIngestor:
        pass

    def fake_discover_ingestors() -> dict[str, type[DummyIngestor]]:
        return {"cli_test_plugin": DummyIngestor}

    def fake_discover_catalog_groups(
        store_uri: str,
        *,
        storage_session: StorageSession | None = None,
        storage_config: Any | None = None,
    ) -> list[str]:
        assert store_uri == tmp_local_zarr.as_uri()
        assert isinstance(storage_session, StorageSession)
        assert storage_config is None
        return ["g1"]

    def fake_build_catalog_source_specs(
        *,
        plugin_name: str,
        product: str,
        store_uri: str,
        groups: list[str],
        group_info_resolver: Any | None = None,
        storage_session: StorageSession | None = None,
        storage_config: Any | None = None,
    ) -> list[CatalogSourceSpec]:
        assert plugin_name == "cli_test_plugin"
        assert product == tmp_local_zarr.as_uri()
        assert store_uri == tmp_local_zarr.as_uri()
        assert groups == ["g1"]
        assert group_info_resolver is None
        assert isinstance(storage_session, StorageSession)
        assert storage_config is None
        return [CatalogSourceSpec(name="cli_test_plugin_g1", description="g1", group="g1")]

    monkeypatch.setattr(catalog_module, "discover_ingestors", fake_discover_ingestors)
    monkeypatch.setattr(catalog_module, "discover_catalog_groups", fake_discover_catalog_groups)
    monkeypatch.setattr(
        catalog_module, "build_catalog_source_specs", fake_build_catalog_source_specs
    )

    result = _invoke_intake(
        [
            "cli_test_plugin",
            "-p",
            tmp_local_zarr.as_uri(),
            "-o",
            (tmp_local_zarr.parent / "catalog.yaml").as_uri(),
            "--collection-id",
            "test-coll",
        ]
    )

    assert result.exit_code == 0, result.output
