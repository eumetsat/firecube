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


class _FakeSpanManager:
    """Stub manager returning a canned ``delete_spans`` result."""

    def __init__(self, result: dict):
        self._result = result
        self.delete_spans_calls: list[dict] = []

    def list_chunks(self, **kwargs):
        return [
            ChunkInfo(
                key="span-1",
                product="PRODUCT_A",
                chunk_type="span",
                size=0,
                timestamp=0.0,
                manifest_path="file:///tmp/wk/.firecube/manifest.jsonl",
            )
        ]

    def delete_spans(self, spans, **kwargs):
        self.delete_spans_calls.append(kwargs)
        return self._result


def _invoke_delete_span(monkeypatch, result: dict, *extra: str):
    manager = _FakeSpanManager(result)
    monkeypatch.setattr(
        "firecube.cli.chunks._delete.resolve_manager",
        lambda *args, **kwargs: manager,
    )
    r = CliRunner().invoke(
        cli,
        [
            "chunks",
            "--workspace",
            "/tmp/wk",
            "delete-span",
            "--product-name",
            "PRODUCT_A",
            "--yes-i-really-mean-it",
            *extra,
        ],
    )
    return r, manager


def test_delete_span_errors_exit_nonzero_after_printing_errors(monkeypatch):
    r, _ = _invoke_delete_span(
        monkeypatch,
        {"deleted_keys": 0, "deleted_spans": 0, "errors": ["boom-1", "boom-2"]},
    )

    assert r.exit_code == 1, r.output
    assert "Deleted 0 chunk keys from storage across 0 spans" in r.output
    assert "Errors: 2" in r.output
    assert "  - boom-1" in r.output
    assert "  - boom-2" in r.output


def test_delete_span_dry_run_with_errors_exits_nonzero(monkeypatch):
    r, manager = _invoke_delete_span(
        monkeypatch,
        {"deleted_keys": 3, "deleted_spans": 1, "errors": ["boom"]},
        "--dry-run",
    )

    assert manager.delete_spans_calls[0]["dry_run"] is True
    assert r.exit_code == 1, r.output
    assert "DRY RUN: would delete 3 chunk keys from storage across 1 spans" in r.output
    assert "Errors: 1" in r.output
    assert "  - boom" in r.output


def test_delete_span_warnings_only_exits_zero(monkeypatch):
    r, _ = _invoke_delete_span(
        monkeypatch,
        {"deleted_keys": 2, "deleted_spans": 1, "warnings": ["careful"], "errors": []},
    )

    assert r.exit_code == 0, r.output
    assert "Warnings: 1" in r.output
    assert "  - careful" in r.output
    assert "Errors:" not in r.output


def test_delete_span_clean_result_exits_zero(monkeypatch):
    r, _ = _invoke_delete_span(
        monkeypatch,
        {"deleted_keys": 2, "deleted_spans": 1, "errors": []},
    )

    assert r.exit_code == 0, r.output
    assert "Deleted 2 chunk keys from storage across 1 spans" in r.output
    assert "Errors:" not in r.output
    assert "Warnings:" not in r.output


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
