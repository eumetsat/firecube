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

from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.controlplane.types import ChunkInfo, DeletionPlan


class _FakeDeleteManager:
    def create_deletion_plan(self, **kwargs):
        if not kwargs:
            return DeletionPlan(
                chunks=[],
                total_size=0,
                products_affected=set(),
                manifest_files=set(),
            )
        return DeletionPlan(
            chunks=[
                ChunkInfo(
                    key="chunk-1",
                    product="PRODUCT_A",
                    chunk_type="chunk",
                    size=1024,
                    timestamp=0.0,
                    manifest_path="file:///tmp/wk/.firecube/manifest.jsonl",
                )
            ],
            total_size=1024,
            products_affected={"PRODUCT_A"},
            manifest_files={"file:///tmp/wk/.firecube/manifest.jsonl"},
        )

    def execute_deletion(self, *args, **kwargs):
        return {
            "would_delete_chunks": 1,
            "would_delete_size_bytes": 1024,
            "products_affected": ["PRODUCT_A"],
            "deleted_chunks": 0,
            "deleted_size_bytes": 0,
            "storage_errors": [],
            "manifest_errors": [],
        }


def test_delete_no_scope_exits_nonzero():
    r = CliRunner().invoke(cli, ["chunks", "delete", "--workspace", "/tmp/wk"])

    assert r.exit_code != 0
    assert "MissingScope" in r.output or "product" in r.output.lower()


def test_delete_conflicting_scope_exits_nonzero():
    r = CliRunner().invoke(
        cli,
        [
            "chunks",
            "delete",
            "--product-name",
            "X",
            "--all-products",
            "--workspace",
            "/tmp/wk",
        ],
    )

    assert r.exit_code != 0
    assert "ConflictingScope" in r.output or "mutually exclusive" in r.output.lower()


def test_delete_all_products_non_tty_without_yes_exits_nonzero():
    r = CliRunner().invoke(cli, ["chunks", "delete", "--all-products", "--workspace", "/tmp/wk"])

    assert r.exit_code != 0
    assert "yes-i-really-mean-it" in r.output.lower() or "confirmation" in r.output.lower()


def test_delete_all_products_dry_run_no_confirmation_needed(monkeypatch):
    monkeypatch.setattr(
        "firecube.cli.chunks._delete.resolve_manager",
        lambda *args, **kwargs: _FakeDeleteManager(),
    )
    from firecube.core.config import StorageConfig

    monkeypatch.setattr(
        "firecube.cli.chunks._delete.storage_config_from_ctx",
        lambda *args, **kwargs: StorageConfig(storage_type="local"),
    )

    r = CliRunner().invoke(
        cli,
        ["chunks", "delete", "--all-products", "--dry-run", "--workspace", "/tmp/wk"],
    )

    assert r.exit_code == 0
    assert "DRY RUN - Would delete" in r.output
    assert "MissingScope" not in r.output
    assert "ConflictingScope" not in r.output
    assert "yes-i-really-mean-it" not in r.output.lower()
