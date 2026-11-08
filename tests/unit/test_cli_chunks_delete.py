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

from datetime import datetime
from pathlib import Path

import pytest
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.controlplane.types import ChunkInfo, DeletionPlan
from tests.helpers.chunks import bound
from tests.unit.test_chunk_manager_queries import feb_30_cli_args, stored_span_groups


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
            "records_without_storage": 0,
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
        "firecube.cli.chunks._delete.target_product",
        lambda *args, **kwargs: bound(manager, "PRODUCT_A"),
    )
    r = CliRunner().invoke(
        cli,
        [
            "chunks",
            "--workspace",
            "/tmp/wk",
            "delete-span",
            "--target",
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
            "--target",
            "X",
            "--all-products",
            "--workspace",
            "/tmp/wk",
        ],
    )

    assert r.exit_code == 2
    assert "Provide exactly one of --target or --all-products." in r.output


def test_delete_all_products_non_tty_without_yes_exits_nonzero():
    r = CliRunner().invoke(cli, ["chunks", "delete", "--all-products", "--workspace", "/tmp/wk"])

    assert r.exit_code != 0
    assert "yes-i-really-mean-it" in r.output.lower() or "confirmation" in r.output.lower()


def test_delete_all_products_dry_run_no_confirmation_needed(monkeypatch):
    monkeypatch.setattr(
        "firecube.cli.chunks._delete.scoped_products",
        lambda *args, **kwargs: [bound(_FakeDeleteManager(), "PRODUCT_A")],
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


_CHUNK_PRODUCT = "prod.zarr"
_CHUNK_KEY = "G/FWI/c/0/0/0"


def _seed_chunk_product(root: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[str, Path]:
    """Record one completed run holding one chunk record whose object exists on disk.

    Returns the product URI and the object path. No config file is written and no
    storage setting is left in the environment.
    """
    from firecube.core.controlplane import ChunkManager
    from tests.helpers.storage import make_test_binding

    for name in (
        "FIRECUBE_STORAGE_TYPE",
        "FIRECUBE_STORAGE_DRIVER",
        "FIRECUBE_TARGET_PATH",
        "FIRECUBE_CONFIG",
    ):
        monkeypatch.delenv(name, raising=False)
    product_uri = f"file://{root / _CHUNK_PRODUCT}"
    obj = root / _CHUNK_PRODUCT / _CHUNK_KEY
    obj.parent.mkdir(parents=True)
    obj.write_bytes(b"payload")
    manager = ChunkManager(
        binding=make_test_binding(root, product=_CHUNK_PRODUCT), workspace=root / "ws"
    )
    try:
        run = {
            "product": _CHUNK_PRODUCT,
            "run_id": "run-001",
            "output_path": product_uri,
            "output_format": "zarr",
            "meta": {"plugin": "test"},
        }
        manager.record_run_started(**run, size=0)
        _seed_legacy_chunk_record(manager, "run-001")
        manager.record_run_terminal(**run, size=7, status="complete")
    finally:
        manager.close()
    return product_uri, obj


def _seed_legacy_chunk_record(manager, run_id: str) -> None:
    """Append one chunk-type record to the open run ``run_id``.

    Chunk-type records have no producer in the ChunkManager facade: they exist only in
    legacy manifests, and ``record_span`` writes span records only. This helper is a
    legacy-fixture setup and the only place this file reaches into the WAL writer.
    """
    from firecube.core.controlplane.types import EVENT_RECORD_UPSERT, SCHEMA_VERSION

    manager.repo._writer(_CHUNK_PRODUCT, run_id, resume_existing=True).append(
        EVENT_RECORD_UPSERT,
        {
            "type": "chunk",
            "key": _CHUNK_KEY,
            "size": 7,
            "timestamp": 1.0,
            "schema_version": SCHEMA_VERSION,
        },
        meta={},
        flush=True,
    )


def _stored_chunk_keys(root: Path) -> set[str]:
    """Chunk record keys a fresh ChunkManager reads for the product."""
    from firecube.core.controlplane import ChunkManager
    from tests.helpers.storage import make_test_binding

    manager = ChunkManager(
        binding=make_test_binding(root, product=_CHUNK_PRODUCT), workspace=root / "ws"
    )
    try:
        return {c.key for c in manager.list_chunks(product=_CHUNK_PRODUCT, chunk_type="chunk")}
    finally:
        manager.close()


def _delete_args(product_uri: str, root: Path, *flags: str) -> list[str]:
    return [
        "chunks",
        "--workspace",
        str(root / "ws"),
        "delete",
        "-t",
        product_uri,
        "--pattern",
        "G/*",
        *flags,
    ]


@pytest.mark.contract
def test_repro_chunks_delete_dry_run_without_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    product_uri, obj = _seed_chunk_product(tmp_path, monkeypatch)

    result = CliRunner().invoke(cli, _delete_args(product_uri, tmp_path, "--dry-run"))

    assert result.exit_code == 0, result.output
    assert "DRY RUN - Would delete:" in result.output
    assert "Chunks: 1" in result.output
    assert obj.exists()
    assert _stored_chunk_keys(tmp_path) == {_CHUNK_KEY}


@pytest.mark.contract
def test_repro_chunks_delete_without_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    product_uri, obj = _seed_chunk_product(tmp_path, monkeypatch)

    result = CliRunner().invoke(cli, _delete_args(product_uri, tmp_path, "--yes-i-really-mean-it"))

    assert result.exit_code == 0, result.output
    assert not obj.exists()
    assert "Deleted 1 chunks" in result.output
    assert "Storage errors" not in result.output
    assert _stored_chunk_keys(tmp_path) == set()


@pytest.mark.contract
def test_repro_chunks_delete_config_base_ignored(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    product_uri, obj = _seed_chunk_product(tmp_path, monkeypatch)
    other = tmp_path / "other"
    decoy = other / _CHUNK_PRODUCT / _CHUNK_KEY
    decoy.parent.mkdir(parents=True)
    decoy.write_bytes(b"decoy")
    config = tmp_path / "cfg.toml"
    config.write_text(f'[storage]\ntype = "local"\ntarget_path = "{other}"\n', encoding="utf-8")

    result = CliRunner().invoke(
        cli,
        [
            "--config-file",
            str(config),
            *_delete_args(product_uri, tmp_path, "--yes-i-really-mean-it"),
        ],
    )

    assert result.exit_code == 0, result.output
    assert not obj.exists(), result.output
    assert decoy.read_bytes() == b"decoy"
    assert _stored_chunk_keys(tmp_path) == set()


@pytest.mark.parametrize(
    ("window", "refused", "remaining"),
    [
        ("--end-date=2021-01-01", True, {"G_GOOD", "G_360"}),
        ("--start-date=2021-01-01", False, {"G_360"}),
    ],
    ids=["span-inside-date-window", "span-outside-date-window"],
)
def test_delete_time_range_refuses_feb_30_span_only_inside_date_window(
    monkeypatch, tmp_path: Path, window: str, refused: bool, remaining: set[str]
) -> None:
    # G_360 is recorded in 2020, G_GOOD now; the date window selects one of them.
    argv = feb_30_cli_args(
        monkeypatch,
        tmp_path,
        "delete",
        "--time-range=2024-01-01:2024-12-31",
        window,
        "--yes-i-really-mean-it",
        recorded_at=datetime(2020, 6, 1).timestamp(),
    )
    result = CliRunner().invoke(cli, argv)

    assert (result.exit_code != 0) is refused, result.output
    assert stored_span_groups(tmp_path) == remaining
    if refused:
        assert "G_360" in result.output and "02-30" in result.output, result.output
