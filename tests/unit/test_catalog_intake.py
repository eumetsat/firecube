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

import sys
import types
from pathlib import Path

import xarray as xr
from click.testing import CliRunner

from firecube.cli.catalog import catalog
from firecube.core.config import StorageConfig
from firecube.core.intake import (
    CatalogGroupInfo,
    build_catalog_source_specs,
    build_intake_catalog,
    discover_catalog_groups,
)
from firecube.core.storage.uri import StorageUri  # pyright: ignore[reportMissingImports]


def _write_group(store: Path, group: str, *, var_name: str = "value") -> None:
    ds = xr.Dataset({var_name: (("time",), [1, 2])}, coords={"time": [0, 1]})
    ds.to_zarr(str(store), group=group, mode="a", consolidated=False, zarr_format=3)


def test_discover_catalog_groups_skips_container_nodes(tmp_path: Path) -> None:
    store = tmp_path / "product.zarr"
    _write_group(store, "group_a")
    _write_group(store, "group_a/derived/coarse")

    groups = discover_catalog_groups(str(store), storage_config=_local_storage_config(tmp_path))

    assert groups == ["group_a", "group_a/derived/coarse"]


def test_build_catalog_source_specs_applies_plugin_annotations(tmp_path: Path) -> None:
    store = tmp_path / "product.zarr"
    _write_group(store, "group_a")
    _write_group(store, "internal_group")

    def resolver(group: str, store_uri: str, storage_config=None):
        _ = (store_uri, storage_config)
        if group == "internal_group":
            return CatalogGroupInfo(include=False)
        return CatalogGroupInfo(
            name=f"custom_{group.lower()}",
            description=f"Readable {group}",
            metadata={"semantic": group},
        )

    specs = build_catalog_source_specs(
        plugin_name="demo",
        product="product.zarr",
        store_uri=str(store),
        groups=["group_a", "internal_group"],
        group_info_resolver=resolver,
        storage_config=_local_storage_config(tmp_path),
    )

    assert len(specs) == 1
    assert specs[0].name == "custom_group_a"
    assert specs[0].description == "Readable group_a"
    assert specs[0].metadata["semantic"] == "group_a"
    assert specs[0].metadata["group"] == "group_a"


def test_catalog_cli_reports_no_catalogable_groups(tmp_path: Path, monkeypatch) -> None:
    store_root = tmp_path / "target"
    product = "empty.zarr"
    (store_root / product).mkdir(parents=True)

    class DummyIngestor:
        pass

    monkeypatch.setattr("firecube.cli.catalog.discover_ingestors", lambda: {"dummy": DummyIngestor})
    monkeypatch.setattr(
        "firecube.cli.catalog.get_storage_config",
        lambda ctx, *, overrides=None, cache=False: _local_storage_config(store_root),
    )
    monkeypatch.setattr("firecube.cli.catalog.observability.init_observability", lambda *_: None)

    runner = CliRunner()
    result = runner.invoke(
        catalog,
        [
            "intake",
            "dummy",
            "--product",
            StorageUri.from_local_path(store_root / product).to_str(),
            "--output",
            StorageUri.from_local_path(store_root / "catalog.yml").to_str(),
            "--collection-id",
            "dummy",
        ],
    )

    assert result.exit_code != 0
    assert "No catalogable dataset groups found" in result.output


def _local_storage_config(target_path: Path) -> StorageConfig:
    config = StorageConfig(storage_type="local")
    config.target_path = str(target_path)  # type: ignore[attr-defined]
    return config


def test_discover_catalog_groups_finds_parquet_leaf_groups(tmp_path: Path) -> None:
    product = tmp_path / "product.parquet"
    (product / "group_a").mkdir(parents=True)
    (product / "group_a" / "part-000.parquet").write_bytes(b"PAR1demoPAR1")
    (product / "group_a" / "derived" / "part-001.parquet").parent.mkdir(parents=True)
    (product / "group_a" / "derived" / "part-001.parquet").write_bytes(b"PAR1demoPAR1")
    (product / ".firecube").mkdir()
    (product / ".firecube" / "ignored.parquet").write_bytes(b"PAR1demoPAR1")

    groups = discover_catalog_groups(str(product), storage_config=_local_storage_config(tmp_path))

    assert groups == ["group_a", "group_a/derived"]


def test_build_catalog_source_specs_for_parquet_sets_data_format(tmp_path: Path) -> None:
    product = tmp_path / "product.parquet"
    (product / "group_a").mkdir(parents=True)

    specs = build_catalog_source_specs(
        plugin_name="demo",
        product="product.parquet",
        store_uri=str(product),
        groups=["group_a"],
    )

    assert len(specs) == 1
    assert specs[0].data_format == "parquet"
    assert specs[0].metadata["data_format"] == "parquet"


def test_build_intake_catalog_for_parquet_uses_intake_parquet_driver() -> None:
    catalog_dict = build_intake_catalog(
        catalog_name="demo",
        catalog_description="Parquet demo",
        collection_id="sentinel3-frp",
        store_uri="s3://bucket/product.parquet",
        sources=[
            build_catalog_source_specs(
                plugin_name="demo",
                product="product.parquet",
                store_uri="s3://bucket/product.parquet",
                groups=["mwir_1km"],
            )[0]
        ],
    )

    source = catalog_dict["sources"]["demo_mwir_1km"]
    assert source["driver"] == "parquet"
    assert source["args"]["urlpath"] == "s3://bucket/product.parquet/mwir_1km"
    assert source["args"]["engine"] == "pyarrow"
    assert source["metadata"]["data_format"] == "parquet"
    assert source["metadata"]["collection_id"] == "sentinel3-frp"
    assert catalog_dict["metadata"]["collection_id"] == "sentinel3-frp"


def test_build_intake_catalog_for_zarr_sets_collection_id() -> None:
    catalog_dict = build_intake_catalog(
        catalog_name="demo",
        catalog_description="Zarr demo",
        collection_id="msg-frm",
        store_uri="s3://bucket/product.zarr",
        sources=[
            build_catalog_source_specs(
                plugin_name="demo",
                product="product.zarr",
                store_uri="s3://bucket/product.zarr",
                groups=["group_a"],
            )[0]
        ],
    )

    source = catalog_dict["sources"]["demo_group_a"]
    # Must be the registered Intake driver short name so the generated catalog
    # is resolvable at open time (a full module path is not).
    assert source["driver"] == "zarr"
    assert source["args"]["urlpath"] == "s3://bucket/product.zarr"
    assert source["args"]["group"] == "group_a"
    assert catalog_dict["metadata"]["collection_id"] == "msg-frm"
    assert source["metadata"]["collection_id"] == "msg-frm"


def test_catalog_cli_requires_collection_id() -> None:
    runner = CliRunner()

    result = runner.invoke(
        catalog, ["intake", "dummy", "--product", "p.zarr", "--output", "catalog.yml"]
    )

    assert result.exit_code != 0
    assert "--collection-id" in result.output


def test_catalog_yaml_failure_fallback_writes_json_not_yml(
    tmp_path: Path,
    monkeypatch,
) -> None:
    output = tmp_path / "catalog.yml"
    store_root = tmp_path / "target"
    product = "product.zarr"
    (store_root / product).mkdir(parents=True)

    class DummyIngestor:
        pass

    fake_yaml = types.ModuleType("yaml")

    def safe_dump(*args, **kwargs):
        _ = (args, kwargs)
        raise RuntimeError("yaml unavailable")

    fake_yaml.safe_dump = safe_dump  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, "yaml", fake_yaml)
    monkeypatch.setattr("firecube.cli.catalog.discover_ingestors", lambda: {"dummy": DummyIngestor})
    monkeypatch.setattr(
        "firecube.cli.catalog.get_storage_config",
        lambda ctx, *, overrides=None, cache=False: _local_storage_config(store_root),
    )
    monkeypatch.setattr("firecube.cli.catalog.observability.init_observability", lambda *_: None)
    monkeypatch.setattr(
        "firecube.cli.catalog.discover_catalog_groups",
        lambda *args, **kwargs: ["group_a"],
    )
    monkeypatch.setattr(
        "firecube.cli.catalog.build_catalog_source_specs",
        lambda *args, **kwargs: [object()],
    )
    monkeypatch.setattr(
        "firecube.cli.catalog.build_intake_catalog",
        lambda **kwargs: {"sources": {}, "metadata": {}},
    )

    runner = CliRunner()
    result = runner.invoke(
        catalog,
        [
            "intake",
            "dummy",
            "--product",
            StorageUri.from_local_path(store_root / product).to_str(),
            "--output",
            StorageUri.from_local_path(output).to_str(),
            "--collection-id",
            "dummy",
        ],
    )

    fallback_output = output.with_suffix(".json")
    assert result.exit_code == 0
    assert fallback_output.exists()
    assert fallback_output.suffix == ".json"
    assert "catalog.json" in result.output
