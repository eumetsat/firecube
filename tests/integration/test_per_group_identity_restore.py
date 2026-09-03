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

"""Mixed-spec per-group identity verification restore.

Preallocate stamps ``firecube_group_identity_hash`` on the bounded group's
coord array; unbounded groups are skipped. Ingest startup re-verifies each
bounded group's stamp against the plugin's current spec; drift raises
``SchemaDriftError`` naming the group.
"""

from __future__ import annotations

import importlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import mixed_bounded_unbounded_test_plugin as _mixed_plugin_module
import pytest
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.api import FIRECUBE_GROUP_IDENTITY_HASH_ATTR
from firecube.core.controlplane import ChunkManager
from firecube.core.errors import SchemaDriftError
from firecube.core.product.identity import ProductIdentity
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.uri import StorageUri
from firecube.ingestor.registry import loader as _loader

pytestmark = pytest.mark.integration

_PLUGIN = "mixed_bounded_unbounded_test"
_PRODUCT = "mixed_bounded_unbounded_test"
_BOUNDED_GROUP = "data"
_UNBOUNDED_GROUP = "aux"
_COORD = "timestamp"


@pytest.fixture(autouse=True)
def _reset_plugin_registry() -> Iterator[None]:
    original_loaded = _loader._LOADED
    original_registry = dict(_loader.AVAILABLE_INGESTORS)
    _loader._LOADED = False
    _loader.AVAILABLE_INGESTORS.clear()
    importlib.reload(_mixed_plugin_module)
    yield
    _loader._LOADED = original_loaded
    _loader.AVAILABLE_INGESTORS.clear()
    _loader.AVAILABLE_INGESTORS.update(original_registry)


def _preallocate_args(target_path: Path) -> list[str]:
    return [
        "zarr",
        "preallocate",
        _PLUGIN,
        "--target",
        f"file://{target_path}",
        "--product-name",
        _PRODUCT,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
        "--option",
        "no_progress=true",
    ]


def _open_root(target_path: Path) -> Any:
    return zarr.open_group(store=str(target_path), mode="a", zarr_format=3)


def _chunk_manager(target_path: Path) -> ChunkManager:
    product_uri = StorageUri.from_local_path(target_path)
    binding = StorageBinding(
        identity=ProductIdentity.from_uri(product_uri, "zarr", product_name=_PRODUCT),
        driver=StorageDriverConfig(driver="fsspec"),
    )
    return ChunkManager(binding=binding, workspace=target_path.parent)


def _plugin_and_resolved(
    chunk_manager: ChunkManager,
) -> tuple[Any, Any]:
    ingestor_cls = _mixed_plugin_module.MixedBoundedUnboundedTestIngestor
    ingestor = ingestor_cls(chunk_manager=chunk_manager)
    ctx = cast(Any, _PluginCtxStub())
    ingestor._bind_index_at_startup(ctx)
    resolved = ingestor.resolved_index(ctx)
    return ingestor, resolved


class _PluginCtxStub:
    _ctx = object()
    run_id = "startup-run"
    storage = None

    def option(self, key: str, default: Any = None) -> Any:
        _ = key
        return default


def test_preallocate_stamps_hash_only_on_bounded_group(tmp_path: Path) -> None:
    target_path = tmp_path / "cube.zarr"
    result = CliRunner().invoke(cli, _preallocate_args(target_path))
    assert result.exit_code == 0, result.output

    root = _open_root(target_path)
    bounded_coord = cast(Any, root[f"{_BOUNDED_GROUP}/{_COORD}"])
    assert FIRECUBE_GROUP_IDENTITY_HASH_ATTR in dict(bounded_coord.attrs)
    stamped = bounded_coord.attrs[FIRECUBE_GROUP_IDENTITY_HASH_ATTR]
    assert isinstance(stamped, str)
    assert len(stamped) == 64

    aux_paths = list((target_path / _UNBOUNDED_GROUP).rglob("zarr.json"))
    for zj in aux_paths:
        text = zj.read_text()
        assert FIRECUBE_GROUP_IDENTITY_HASH_ATTR not in text, (
            f"aux (unbounded) must not carry a stamp; found in {zj}"
        )


def test_per_group_verification_passes_on_matching_stamp(tmp_path: Path) -> None:
    target_path = tmp_path / "cube.zarr"
    result = CliRunner().invoke(cli, _preallocate_args(target_path))
    assert result.exit_code == 0, result.output

    manager = _chunk_manager(target_path)
    try:
        ingestor, resolved = _plugin_and_resolved(manager)
        ingestor._verify_per_group_identity_at_store(f"file://{target_path}", resolved)
    finally:
        manager.close()


def test_per_group_verification_raises_on_divergence(tmp_path: Path) -> None:
    target_path = tmp_path / "cube.zarr"
    result = CliRunner().invoke(cli, _preallocate_args(target_path))
    assert result.exit_code == 0, result.output

    root = _open_root(target_path)
    bounded_coord = cast(Any, root[f"{_BOUNDED_GROUP}/{_COORD}"])
    tampered = "0" * 64
    bounded_coord.attrs[FIRECUBE_GROUP_IDENTITY_HASH_ATTR] = tampered

    manager = _chunk_manager(target_path)
    try:
        ingestor, resolved = _plugin_and_resolved(manager)
        with pytest.raises(SchemaDriftError) as exc_info:
            ingestor._verify_per_group_identity_at_store(f"file://{target_path}", resolved)
        message = str(exc_info.value)
        assert _BOUNDED_GROUP in message
        assert tampered in message
    finally:
        manager.close()


def test_per_group_verification_skips_when_stamp_missing(tmp_path: Path) -> None:
    target_path = tmp_path / "cube.zarr"
    result = CliRunner().invoke(cli, _preallocate_args(target_path))
    assert result.exit_code == 0, result.output

    root = _open_root(target_path)
    bounded_coord = cast(Any, root[f"{_BOUNDED_GROUP}/{_COORD}"])
    existing_attrs = dict(bounded_coord.attrs)
    existing_attrs.pop(FIRECUBE_GROUP_IDENTITY_HASH_ATTR, None)
    for key in list(bounded_coord.attrs):
        del bounded_coord.attrs[key]
    for k, v in existing_attrs.items():
        bounded_coord.attrs[k] = v

    manager = _chunk_manager(target_path)
    try:
        ingestor, resolved = _plugin_and_resolved(manager)
        ingestor._verify_per_group_identity_at_store(f"file://{target_path}", resolved)
    finally:
        manager.close()


def test_per_group_verification_skips_when_coord_array_absent(tmp_path: Path) -> None:
    target_path = tmp_path / "cube.zarr"
    target_path.mkdir()
    _ = zarr.open_group(store=str(target_path), mode="a", zarr_format=3)

    manager = _chunk_manager(target_path)
    try:
        ingestor, resolved = _plugin_and_resolved(manager)
        ingestor._verify_per_group_identity_at_store(f"file://{target_path}", resolved)
    finally:
        manager.close()
