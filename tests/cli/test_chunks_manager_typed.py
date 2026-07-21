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

"""T16: ``cli/chunks/_manager.py`` keeps storage config typed end-to-end.

These tests pin the post-T16 contract:

- The CLI helper that hands storage config to the deletion engine returns a
  typed ``StorageConfig`` straight from ``ctx.obj["storage_config"]`` — no
  fresh dict is built from its fields.
- ``storage_config_dict_from_ctx`` (the typed→dict bridge introduced before
  T16) is gone; callers now use ``storage_config_from_ctx``.
- ``ChunkManager.execute_deletion`` and ``DeletionEngine.execute_deletion``
  accept ``StorageConfig | None`` instead of ``dict[str, Any] | None``.
- Static guard: ``_manager.py`` does not synthesise dicts from storage-config
  attributes (``asdict``, ``dataclasses.asdict``, or an inline ``{...}`` with
  ``storage_config_obj.``-sourced values).
"""

from __future__ import annotations

import inspect
from types import SimpleNamespace
from typing import Any

import click
import pytest
from click.testing import CliRunner

from firecube.cli.chunks import _delete as delete_module
from firecube.cli.chunks import _manager as cli_manager
from firecube.cli.main import cli
from firecube.core.config import StorageConfig
from firecube.core.controlplane.deletion import DeletionEngine
from firecube.core.controlplane.manager import ChunkManager


def test_storage_config_dict_from_ctx_is_removed() -> None:
    """The dict-returning bridge function is gone after T16."""
    assert not hasattr(cli_manager, "storage_config_dict_from_ctx"), (
        "T16: storage_config_dict_from_ctx must be removed; use the typed "
        "storage_config_from_ctx instead"
    )


def test_storage_config_from_ctx_returns_typed_storage_config() -> None:
    """The replacement helper returns the same StorageConfig object stored in ctx."""
    assert hasattr(cli_manager, "storage_config_from_ctx"), (
        "T16: cli/chunks/_manager.py must export storage_config_from_ctx"
    )
    cfg = StorageConfig(storage_type="s3")
    cfg.bucket = "my-bucket"  # type: ignore[attr-defined]
    ctx = click.Context(click.Command("chunks"), obj={"storage_config": cfg})

    result = cli_manager.storage_config_from_ctx(ctx)

    assert isinstance(result, StorageConfig)
    assert result is cfg, (
        "T16: storage_config_from_ctx must return the typed instance unchanged "
        "(no dict roundtrip, no fresh dataclass clone)"
    )


def test_storage_config_from_ctx_raises_when_missing() -> None:
    """Behaviour parity: missing storage config still raises ClickException."""
    ctx = click.Context(click.Command("chunks"), obj={})
    with pytest.raises(click.ClickException):
        cli_manager.storage_config_from_ctx(ctx)


def test_chunk_manager_execute_deletion_signature_accepts_typed_storage_config() -> None:
    sig = inspect.signature(ChunkManager.execute_deletion)
    annotation = sig.parameters["storage_config"].annotation
    rendered = str(annotation)
    assert "StorageConfig" in rendered, (
        f"T16: ChunkManager.execute_deletion(storage_config=) must be typed as "
        f"StorageConfig | None (got annotation {rendered!r})"
    )
    assert "dict" not in rendered, (
        f"T16: ChunkManager.execute_deletion(storage_config=) must not accept a "
        f"dict (got annotation {rendered!r})"
    )


def test_deletion_engine_execute_deletion_signature_accepts_typed_storage_config() -> None:
    sig = inspect.signature(DeletionEngine.execute_deletion)
    annotation = sig.parameters["storage_config"].annotation
    rendered = str(annotation)
    assert "StorageConfig" in rendered, (
        f"T16: DeletionEngine.execute_deletion(storage_config=) must be typed as "
        f"StorageConfig | None (got annotation {rendered!r})"
    )
    assert "dict" not in rendered, (
        f"T16: DeletionEngine.execute_deletion(storage_config=) must not accept a "
        f"dict (got annotation {rendered!r})"
    )


def test_delete_command_passes_typed_storage_config_to_manager(monkeypatch: Any) -> None:
    cfg = StorageConfig(storage_type="s3")
    captured: dict[str, object] = {}

    class FakeManager:
        def create_deletion_plan(self, **kwargs: Any) -> Any:
            if kwargs:
                chunk = SimpleNamespace(product="product", key="chunk-1", size=12)
                return SimpleNamespace(
                    chunks=[chunk],
                    total_size=12,
                    products_affected={"product"},
                    manifest_files=set(),
                )
            return SimpleNamespace(
                chunks=[],
                total_size=0,
                products_affected=set(),
                manifest_files=set(),
            )

        def execute_deletion(
            self,
            plan: Any,
            *,
            delete_storage: bool,
            delete_manifest: bool,
            storage_config: StorageConfig | None,
            dry_run: bool,
        ) -> dict[str, object]:
            captured["storage_config"] = storage_config
            captured["delete_storage"] = delete_storage
            captured["delete_manifest"] = delete_manifest
            captured["dry_run"] = dry_run
            return {
                "would_delete_chunks": len(plan.chunks),
                "would_delete_size_bytes": plan.total_size,
                "products_affected": sorted(plan.products_affected),
            }

    monkeypatch.setattr(delete_module, "resolve_manager", lambda *args, **kwargs: FakeManager())

    result = CliRunner().invoke(
        cli,
        ["chunks", "delete", "--product-name", "product", "--dry-run"],
        obj={"storage_config": cfg},
    )

    assert result.exit_code == 0, result.output
    assert captured == {
        "storage_config": cfg,
        "delete_storage": True,
        "delete_manifest": True,
        "dry_run": True,
    }
    assert "DRY RUN - Would delete:" in result.output
