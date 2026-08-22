"""End-to-end IntegerAxis persistence and convergence tests."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from types import SimpleNamespace
from typing import Any, cast

import pytest
from click.testing import CliRunner
from index_spec_integer_test_plugin import IntegerAxisIngestor, MixedAxisIngestor

from firecube.cli.main import cli
from firecube.core.api import IntegerAxis, ResolvedIndexRecord
from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.manager import check_legacy_index_record
from firecube.core.controlplane.types import (
    INDEX_CURRENT_FILENAME,
    INDEX_DIRNAME,
    SLOT_INDEX_CURRENT_FILENAME,
    SLOT_INDEX_DIRNAME,
)
from firecube.core.errors import LegacyIndexRecordError
from firecube.core.index_resolve import resolve_index_spec
from firecube.core.index_spec import IndexSpec
from firecube.core.slot_index import SlotAxis, SlotIndexModel
from tests.helpers.storage import make_test_binding

pytestmark = pytest.mark.integration

INTEGER_PLUGIN = "index_spec_integer_test"
INTEGER_PRODUCT = "index_spec_integer_test"
MIXED_PLUGIN = "index_spec_integer_mixed_test"
MIXED_PRODUCT = "index_spec_integer_mixed_test"


def _manager(tmp_path: Path, product: str) -> ChunkManager:
    return ChunkManager(
        binding=make_test_binding(tmp_path, product=product, driver="fsspec"),
        workspace=tmp_path,
    )


def _target_dir(tmp_path: Path, product: str) -> Path:
    return tmp_path / product


def _index_current_path(tmp_path: Path, product: str) -> Path:
    return _target_dir(tmp_path, product) / ".firecube" / INDEX_DIRNAME / INDEX_CURRENT_FILENAME


def _legacy_current_path(tmp_path: Path, product: str) -> Path:
    return (
        _target_dir(tmp_path, product)
        / ".firecube"
        / SLOT_INDEX_DIRNAME
        / SLOT_INDEX_CURRENT_FILENAME
    )


def _preallocate_args(tmp_path: Path, *, plugin: str, product: str) -> list[str]:
    return [
        "zarr",
        "preallocate",
        plugin,
        "--target",
        _target_dir(tmp_path, product).as_uri(),
        "--product-name",
        product,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
    ]


def _read_record(tmp_path: Path, product: str) -> ResolvedIndexRecord:
    return ResolvedIndexRecord.from_json_bytes(_index_current_path(tmp_path, product).read_bytes())


def _integer_record(run_id: str) -> ResolvedIndexRecord:
    spec = IndexSpec(name="integer_test", groups={"data": IntegerAxis(slot_count=144)})
    return resolve_index_spec(spec, time_dim_name="timestamp").as_resolved_index_record(
        run_id=run_id
    )


def _plugin_ctx() -> Any:
    return SimpleNamespace(
        _ctx=object(),
        run_id="integer-axis-e2e-run",
        storage=None,
        option=lambda key, default=None: default,
    )


def test_integer_axis_ingest_writes_resolved_index(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        cli,
        _preallocate_args(tmp_path, plugin=INTEGER_PLUGIN, product=INTEGER_PRODUCT),
    )

    assert result.exit_code == 0, result.output
    assert "resolved index: created" in result.output
    record = _read_record(tmp_path, INTEGER_PRODUCT)
    assert record.index["groups"]["data"]["kind"] == "integer"
    assert not _legacy_current_path(tmp_path, INTEGER_PRODUCT).exists()


def test_integer_axis_record_has_correct_kind(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        cli,
        _preallocate_args(tmp_path, plugin=INTEGER_PLUGIN, product=INTEGER_PRODUCT),
    )

    assert result.exit_code == 0, result.output
    group = _read_record(tmp_path, INTEGER_PRODUCT).index["groups"]["data"]
    assert group["kind"] == "integer"
    assert group["size"] == 144
    assert group["params"] == {}


def test_mixed_axis_record_has_integer_and_regular_time_groups(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        cli,
        _preallocate_args(tmp_path, plugin=MIXED_PLUGIN, product=MIXED_PRODUCT),
    )

    assert result.exit_code == 0, result.output
    groups = _read_record(tmp_path, MIXED_PRODUCT).index["groups"]
    assert groups["data"]["kind"] == "integer"
    assert groups["data"]["size"] == 144
    assert groups["timestamped"]["kind"] == "regular_time"
    assert groups["timestamped"]["size"] == 24


def test_integer_axis_identity_hash_deterministic(tmp_path: Path) -> None:
    first_manager = _manager(tmp_path, INTEGER_PRODUCT)
    second_manager = _manager(tmp_path, INTEGER_PRODUCT)
    first = _integer_record("first-worker")
    second = _integer_record("second-worker")

    stored_first, first_status = first_manager.ensure_resolved_index(
        product=INTEGER_PRODUCT,
        record=first,
        run_id="first-worker",
    )
    stored_second, second_status = second_manager.ensure_resolved_index(
        product=INTEGER_PRODUCT,
        record=second,
        run_id="second-worker",
    )

    assert first_status == "created"
    assert second_status == "matched_existing"
    assert stored_first.identity_hash == stored_second.identity_hash == first.identity_hash
    assert first.identity_hash == second.identity_hash


@pytest.mark.concurrency
def test_two_workers_converge_on_integer_axis(tmp_path: Path) -> None:
    barrier = Barrier(2)

    def worker(run_id: str) -> str:
        manager = _manager(tmp_path, INTEGER_PRODUCT)
        record = _integer_record(run_id)
        barrier.wait(timeout=10)
        stored, _status = manager.ensure_resolved_index(
            product=INTEGER_PRODUCT,
            record=record,
            run_id=run_id,
            max_retries=10,
            initial_backoff_s=0.01,
        )
        return stored.identity_hash

    with ThreadPoolExecutor(max_workers=2) as executor:
        hashes = list(executor.map(worker, ("worker-a", "worker-b")))

    assert hashes == [_integer_record("expected").identity_hash] * 2


def test_legacy_detection_for_integer_axis_plugin(tmp_path: Path) -> None:
    manager = _manager(tmp_path, INTEGER_PRODUCT)
    manager.ensure_slot_index_model(
        product=INTEGER_PRODUCT,
        model=SlotIndexModel(
            name="legacy_only_v1",
            epoch="2024-01-01T00:00:00Z",
            groups={"data": SlotAxis(cadence_s=600, mode="exact")},
        ),
        run_id="legacy-seed",
    )

    with pytest.raises(LegacyIndexRecordError):
        check_legacy_index_record(
            manager,
            product=INTEGER_PRODUCT,
            plugin_name=INTEGER_PLUGIN,
        )
    manager.close()

    result = CliRunner().invoke(
        cli,
        _preallocate_args(tmp_path, plugin=INTEGER_PLUGIN, product=INTEGER_PRODUCT),
    )

    assert result.exit_code != 0, result.output
    assert "Legacy index record detected at" in result.output
    assert "firecube zarr index rebuild" in result.output
    assert _legacy_current_path(tmp_path, INTEGER_PRODUCT).exists()
    assert not _index_current_path(tmp_path, INTEGER_PRODUCT).exists()


def test_fixture_plugin_specs_remain_integer_axis_contract() -> None:
    ctx = cast(Any, _plugin_ctx())
    integer_spec = IntegerAxisIngestor().index_spec(ctx)
    mixed_spec = MixedAxisIngestor().index_spec(ctx)

    assert integer_spec.groups["data"] == IntegerAxis(slot_count=144)
    assert mixed_spec.groups["data"] == IntegerAxis(slot_count=144)
