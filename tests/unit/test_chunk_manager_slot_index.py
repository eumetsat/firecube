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

"""Unit tests for ChunkManager.ensure_slot_index_model and companions."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import pytest
import zarr
from zarr.storage import LocalStore

from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.types import (
    SLOT_INDEX_CURRENT_FILENAME,
    SLOT_INDEX_DIRNAME,
    SlotIndexModelRecord,
    WriteDomain,
)
from firecube.core.errors import (
    ManifestError,
    SlotIndexModelClaimTimeoutError,
    SlotIndexModelConflictError,
)
from firecube.core.filesystem import StorageFilesystem
from firecube.core.product.identity import ProductIdentity
from firecube.core.slot_index import (
    SLOT_INDEX_MODEL_ATTR,
    SLOT_INDEX_MODEL_IDENTITY_HASH_ATTR,
    SlotAxis,
    SlotIndexModel,
)
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.uri import StorageUri


def _make_manager(tmp_path: Path) -> ChunkManager:
    product_uri = StorageUri.from_local_path(tmp_path / "__firecube_controlplane__")
    binding = StorageBinding(
        identity=ProductIdentity.from_uri(product_uri, "zarr", product_name="control_product"),
        driver=StorageDriverConfig(),
    )
    return ChunkManager(binding=binding, workspace=tmp_path)


def _model(name: str = "opera_v1") -> SlotIndexModel:
    return SlotIndexModel(
        name=name,
        epoch="2026-01-01T00:00:00Z",
        groups={"g1": SlotAxis(cadence_s=300, mode="exact")},
    )


def _control_root(tmp_path: Path, product: str) -> Path:
    return tmp_path / product / ".firecube"


def _slot_index_current(tmp_path: Path, product: str) -> Path:
    return _control_root(tmp_path, product) / SLOT_INDEX_DIRNAME / SLOT_INDEX_CURRENT_FILENAME


def _claim_file(tmp_path: Path, product: str) -> Path:
    domain = WriteDomain(product=product, category="slot_index_model", name="current")
    return _control_root(tmp_path, product) / "claims" / domain.claim_name


def _read_zarr_attrs_hash(tmp_path: Path, product: str) -> str | None:
    try:
        store = LocalStore(str(tmp_path / product))
        root = zarr.open_group(store=store, mode="r", zarr_format=3)
    except Exception:
        return None
    value = root.attrs.get(SLOT_INDEX_MODEL_IDENTITY_HASH_ATTR)
    return None if value is None else str(value)


def _start_run(cm: ChunkManager, tmp_path: Path, product: str, run_id: str) -> None:
    cm.record_run_started(
        product=product,
        run_id=run_id,
        output_path=str(tmp_path / product),
        output_format="zarr",
        size=0,
        meta={"plugin": "test"},
    )


@pytest.mark.unit
def test_fresh_store_records_model(tmp_path):
    cm = _make_manager(tmp_path)
    _start_run(cm, tmp_path, "prod1", "run-1")
    model = _model()

    record = cm.ensure_slot_index_model(product="prod1", model=model, run_id="run-1")

    assert record.identity_hash == model.identity_hash
    assert record.model.name == model.name
    cp_file = _slot_index_current(tmp_path, "prod1")
    assert cp_file.is_file()
    on_disk = SlotIndexModelRecord.from_json_bytes(cp_file.read_bytes())
    assert on_disk.identity_hash == model.identity_hash
    assert _read_zarr_attrs_hash(tmp_path, "prod1") == model.identity_hash


@pytest.mark.unit
def test_get_slot_index_model_returns_none_for_nonexistent(tmp_path):
    cm = _make_manager(tmp_path)
    assert cm.get_slot_index_model(product="nonexistent") is None


@pytest.mark.unit
def test_get_slot_index_model_propagates_non_missing_read_errors(
    tmp_path, monkeypatch: pytest.MonkeyPatch
):
    cm = _make_manager(tmp_path)

    class _FakePath:
        def __init__(self, parts: tuple[str, ...] = ()) -> None:
            self.parts = parts

        def join(self, *segments: str) -> _FakePath:
            return _FakePath((*self.parts, *segments))

    class _FailingFilesystem:
        def open(self, path: _FakePath, mode: str):
            _ = (path, mode)
            raise PermissionError("permission denied while reading current.json")

    monkeypatch.setattr(
        cm.repo,
        "get_control_root_uri",
        lambda _product: "file:///control/prod1/.firecube",
    )
    monkeypatch.setattr(
        cm.repo,
        "_get_fs",
        lambda _uri: (_FailingFilesystem(), _FakePath()),
    )

    with pytest.raises(PermissionError, match="permission denied"):
        cm.get_slot_index_model(product="prod1")


@pytest.mark.unit
def test_slot_index_current_record_is_not_published_via_truncating_write(tmp_path):
    cm = _make_manager(tmp_path)
    _start_run(cm, tmp_path, "prod1", "run-1")
    model = _model()
    real_fs, real_root = cm.repo._get_fs(cm.repo.get_control_root_uri("prod1"))
    read_during_write_errors: list[BaseException] = []

    class _ProxyFilesystem:
        def __init__(self, wrapped: StorageFilesystem, on_current_json_open: Callable[[], None]):
            self._wrapped = wrapped
            self._on_current_json_open = on_current_json_open

        def __getattr__(self, name: str) -> Any:
            return getattr(self._wrapped, name)

        def open(self, uri: StorageUri, mode: str = "rb") -> Any:
            handle = self._wrapped.open(uri, mode)
            if mode == "wb" and uri.path.endswith(
                f"/{SLOT_INDEX_DIRNAME}/{SLOT_INDEX_CURRENT_FILENAME}"
            ):
                self._on_current_json_open()
            return handle

    def read_while_current_json_is_truncated() -> None:
        try:
            cm.get_slot_index_model(product="prod1")
        except ManifestError as exc:
            read_during_write_errors.append(exc)

    proxy_fs = _ProxyFilesystem(real_fs, read_while_current_json_is_truncated)
    cm.repo._filesystem = cast(StorageFilesystem, proxy_fs)

    cm.ensure_slot_index_model(product="prod1", model=model, run_id="run-1")

    assert real_root.to_str().endswith("/prod1/.firecube")
    assert read_during_write_errors == []


@pytest.mark.unit
def test_mirror_attrs_for_remote_product_uses_driver_aware_zarr_store(
    tmp_path, monkeypatch: pytest.MonkeyPatch
):
    cm = _make_manager(tmp_path)
    model = _model()
    record = SlotIndexModelRecord(
        model=model,
        identity_hash=model.identity_hash,
        schema_version="v1",
        recorded_at="2026-01-01T00:00:00Z",
        recorded_by_run_id="run-1",
    )
    root_attrs: dict[str, str] = {}
    sentinel_store = object()
    sentinel_storage_options = {"client_kwargs": {"endpoint_url": "https://example.invalid"}}
    create_calls: list[dict[str, object]] = []
    open_calls: list[dict[str, object]] = []

    class _Root:
        def __init__(self) -> None:
            self.attrs = root_attrs

    class _StoreHandle:
        def zarr_kwargs(self) -> dict[str, object]:
            return {
                "store": sentinel_store,
                "storage_options": sentinel_storage_options,
            }

    def fake_create_zarr_store(*, uri: str, storage_config: object, mode: str):
        create_calls.append({"uri": uri, "storage_config": storage_config, "mode": mode})
        return _StoreHandle()

    def fail_create_obstore_store(*args: object, **kwargs: object):
        _ = (args, kwargs)
        raise AssertionError(
            "slot-index root attrs must use create_zarr_store, not create_obstore_store directly"
        )

    def fake_open_group(**kwargs: object):
        open_calls.append(dict(kwargs))
        return _Root()

    monkeypatch.setattr(
        ChunkManager,
        "get_product_root",
        lambda self, product: "s3://bucket/prod1.zarr",
    )
    monkeypatch.setattr(
        "firecube.core.filesystem.store_factory.create_zarr_store",
        fake_create_zarr_store,
    )
    monkeypatch.setattr(
        "firecube.core.filesystem.store_factory.create_obstore_store",
        fail_create_obstore_store,
    )
    monkeypatch.setattr(zarr, "open_group", fake_open_group)

    cm._mirror_attrs("prod1", record)

    assert create_calls == [
        {
            "uri": "s3://bucket/prod1.zarr",
            "storage_config": cm.storage_config,
            "mode": "a",
        }
    ]
    assert open_calls == [
        {
            "store": sentinel_store,
            "storage_options": sentinel_storage_options,
            "mode": "a",
            "zarr_format": 3,
        }
    ]
    assert root_attrs[SLOT_INDEX_MODEL_ATTR] == model.canonical_bytes().decode("utf-8")
    assert root_attrs[SLOT_INDEX_MODEL_IDENTITY_HASH_ATTR] == model.identity_hash


@pytest.mark.unit
def test_read_slot_index_attrs_hash_for_remote_product_uses_driver_aware_zarr_store(
    tmp_path, monkeypatch: pytest.MonkeyPatch
):
    cm = _make_manager(tmp_path)
    expected_hash = _model().identity_hash
    sentinel_store = object()
    sentinel_storage_options = {"client_kwargs": {"endpoint_url": "https://example.invalid"}}
    create_calls: list[dict[str, object]] = []
    open_calls: list[dict[str, object]] = []

    class _Root:
        def __init__(self) -> None:
            self.attrs = {SLOT_INDEX_MODEL_IDENTITY_HASH_ATTR: expected_hash}

    class _StoreHandle:
        def zarr_kwargs(self) -> dict[str, object]:
            return {
                "store": sentinel_store,
                "storage_options": sentinel_storage_options,
            }

    def fake_create_zarr_store(*, uri: str, storage_config: object, mode: str):
        create_calls.append({"uri": uri, "storage_config": storage_config, "mode": mode})
        return _StoreHandle()

    def fail_create_obstore_store(*args: object, **kwargs: object):
        _ = (args, kwargs)
        raise AssertionError(
            "slot-index root attrs must use create_zarr_store, not create_obstore_store directly"
        )

    def fake_open_group(**kwargs: object):
        open_calls.append(dict(kwargs))
        return _Root()

    monkeypatch.setattr(
        ChunkManager,
        "get_product_root",
        lambda self, product: "s3://bucket/prod1.zarr",
    )
    monkeypatch.setattr(
        "firecube.core.filesystem.store_factory.create_zarr_store",
        fake_create_zarr_store,
    )
    monkeypatch.setattr(
        "firecube.core.filesystem.store_factory.create_obstore_store",
        fail_create_obstore_store,
    )
    monkeypatch.setattr(zarr, "open_group", fake_open_group)

    result = cm._read_slot_index_attrs_hash(product="prod1")

    assert create_calls == [
        {
            "uri": "s3://bucket/prod1.zarr",
            "storage_config": cm.storage_config,
            "mode": "r",
        }
    ]
    assert open_calls == [
        {
            "store": sentinel_store,
            "storage_options": sentinel_storage_options,
            "mode": "r",
            "zarr_format": 3,
        }
    ]
    assert result == expected_hash


@pytest.mark.unit
def test_second_call_same_model_verified(tmp_path):
    cm = _make_manager(tmp_path)
    _start_run(cm, tmp_path, "prod1", "run-1")
    model = _model()

    first = cm.ensure_slot_index_model(product="prod1", model=model, run_id="run-1")
    on_disk_first = SlotIndexModelRecord.from_json_bytes(
        _slot_index_current(tmp_path, "prod1").read_bytes()
    )
    second = cm.ensure_slot_index_model(product="prod1", model=model, run_id="run-1")
    on_disk_second = SlotIndexModelRecord.from_json_bytes(
        _slot_index_current(tmp_path, "prod1").read_bytes()
    )

    assert first.identity_hash == second.identity_hash == model.identity_hash
    assert on_disk_first.recorded_at == on_disk_second.recorded_at
    assert on_disk_first.recorded_by_run_id == on_disk_second.recorded_by_run_id


@pytest.mark.unit
def test_different_model_raises_conflict(tmp_path):
    cm = _make_manager(tmp_path)
    _start_run(cm, tmp_path, "prod1", "run-1")
    cm.ensure_slot_index_model(product="prod1", model=_model(name="opera_v1"), run_id="run-1")
    _start_run(cm, tmp_path, "prod1", "run-2")
    other = _model(name="other_v1")
    assert other.identity_hash != _model(name="opera_v1").identity_hash

    with pytest.raises(SlotIndexModelConflictError):
        cm.ensure_slot_index_model(product="prod1", model=other, run_id="run-2")


@pytest.mark.unit
def test_empty_run_id_raises(tmp_path):
    cm = _make_manager(tmp_path)
    with pytest.raises(ValueError):
        cm.ensure_slot_index_model(product="prod1", model=_model(), run_id="")


@pytest.mark.unit
def test_race_window_cp_only_state_raises_timeout(tmp_path):
    cm = _make_manager(tmp_path)
    _start_run(cm, tmp_path, "prod1", "winner")
    model = _model()
    winner_record = SlotIndexModelRecord(
        model=model,
        identity_hash=model.identity_hash,
        schema_version="v1",
        recorded_at="2026-01-01T00:00:00+00:00",
        recorded_by_run_id="winner",
    )
    # Pre-seed current.json so cp_record matches model.identity_hash, but
    # leave the zarr root WITHOUT the identity-hash attr. This simulates the
    # race window where the winner has written CP but not yet stamped attrs.
    cp_file = _slot_index_current(tmp_path, "prod1")
    cp_file.parent.mkdir(parents=True, exist_ok=True)
    cp_file.write_bytes(winner_record.to_json_bytes())
    # Pre-seed an active (non-stale) claim file: forces acquire_claim to raise
    # ClaimConflictError so the loser executes the retry/convergence path.
    claim_file = _claim_file(tmp_path, "prod1")
    claim_file.parent.mkdir(parents=True, exist_ok=True)
    claim_file.write_text(
        json.dumps(
            {
                "product": "prod1",
                "domain": "prod1:slot_index_model:current",
                "owner_id": "winner",
                "claim_path": str(claim_file),
                "acquired_at": 9999999999.0,
                "last_heartbeat_at": 9999999999.0,
                "heartbeat_interval_s": 30,
                "stale_threshold_s": 120,
            }
        )
    )
    assert _read_zarr_attrs_hash(tmp_path, "prod1") is None

    with pytest.raises(SlotIndexModelClaimTimeoutError):
        cm.ensure_slot_index_model(
            product="prod1",
            model=model,
            run_id="loser",
            max_retries=2,
            initial_backoff_s=0.01,
        )
