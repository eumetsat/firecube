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

import dataclasses
import json
import shutil
import threading
from pathlib import Path
from typing import Any

import pytest

from firecube.core import errors
from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.repo import ManifestRepository
from firecube.core.controlplane.types import (
    EVENT_REPLACEMENT_COMMITTED,
    EVENT_RUN_STARTED_WITH_REPLACEMENT,
    Inventory,
    WriteDomain,
)
from firecube.core.filesystem.fsspec_backend import FsspecAtomicWriter
from firecube.core.filesystem.obstore_backend import ObstoreAtomicWriter
from tests.helpers.storage import make_test_binding
from tests.integration import test_obstore_claim_atomicity as s3_env


def _record_run_started(repo: ManifestRepository, *, product: str, run_id: str) -> None:
    assert repo.workspace is not None
    repo.record_run_started(
        product=product,
        run_id=run_id,
        output_path=str(repo.workspace / product),
        output_format="zarr",
        size=0,
        meta={"plugin": "test"},
    )


def _read_run_wal_events(
    temp_workspace: Path, *, product: str, run_id: str
) -> list[dict[str, Any]]:
    run_dir = temp_workspace / product / ".firecube" / "runs" / run_id
    events: list[dict[str, Any]] = []
    for path in sorted(run_dir.glob("events-*.jsonl")):
        events.extend(
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
    return events


def test_run_started_with_replacement_event_written(temp_workspace):
    product = "product"
    run_id = "run-001"
    repo = ManifestRepository(binding=make_test_binding(temp_workspace), workspace=temp_workspace)
    _record_run_started(repo, product=product, run_id=run_id)

    repo.record_run_started_with_replacement(
        product=product,
        run_id=run_id,
        replaces=["run-000", "run-000b"],
    )

    replacement_events = [
        event
        for event in _read_run_wal_events(temp_workspace, product=product, run_id=run_id)
        if event["event_type"] == EVENT_RUN_STARTED_WITH_REPLACEMENT
    ]

    assert len(replacement_events) == 1
    assert replacement_events[0]["record"]["replaces"] == ["run-000", "run-000b"]


def test_replacement_committed_event_written(temp_workspace):
    product = "product"
    run_id = "run-002"
    repo = ManifestRepository(binding=make_test_binding(temp_workspace), workspace=temp_workspace)
    _record_run_started(repo, product=product, run_id=run_id)

    repo.record_replacement_committed(
        product=product,
        run_id=run_id,
        replacing_run_id="run-002",
        replaced_span_keys=["span_old_1", "span_old_2"],
    )

    replacement_events = [
        event
        for event in _read_run_wal_events(temp_workspace, product=product, run_id=run_id)
        if event["event_type"] == EVENT_REPLACEMENT_COMMITTED
    ]

    assert len(replacement_events) == 1
    assert replacement_events[0]["record"]["replacing_run_id"] == "run-002"
    assert replacement_events[0]["record"]["replaced_span_keys"] == ["span_old_1", "span_old_2"]


def test_replacement_committed_idempotent(temp_workspace):
    product = "product"
    run_id = "run-003"
    repo = ManifestRepository(binding=make_test_binding(temp_workspace), workspace=temp_workspace)
    _record_run_started(repo, product=product, run_id=run_id)

    repo.record_replacement_committed(
        product=product,
        run_id=run_id,
        replacing_run_id=run_id,
        replaced_span_keys=["span_old_1"],
    )
    repo.record_replacement_committed(
        product=product,
        run_id=run_id,
        replacing_run_id=run_id,
        replaced_span_keys=["span_old_1"],
    )

    replacement_events = [
        event
        for event in _read_run_wal_events(temp_workspace, product=product, run_id=run_id)
        if event["event_type"] == EVENT_REPLACEMENT_COMMITTED
    ]

    assert len(replacement_events) == 1


def test_run_meta_rewritten_atomically_across_run_lifecycle(temp_workspace):
    """run.json is written at run start and rewritten at terminal state. Both
    writes must go through the atomic-replace path: a create-only write would
    raise FileExistsError on the rewrite, and a plain open("w") would expose a
    0-byte window to concurrent resume guards listing runs."""
    product = "product"
    run_id = "run-001"
    repo = ManifestRepository(binding=make_test_binding(temp_workspace), workspace=temp_workspace)
    _record_run_started(repo, product=product, run_id=run_id)

    run_dir = temp_workspace / product / ".firecube" / "runs" / run_id
    meta_path = run_dir / "run.json"
    started = json.loads(meta_path.read_text(encoding="utf-8"))
    assert started["run_id"] == run_id

    repo.record_run_terminal(
        product=product,
        run_id=run_id,
        output_path=str(temp_workspace / product),
        output_format="zarr",
        size=0,
        meta={"plugin": "test"},
        status="complete",
    )

    completed = json.loads(meta_path.read_text(encoding="utf-8"))
    assert completed["status"] == "complete"
    leftovers = [p.name for p in run_dir.iterdir() if p.name.endswith(".tmp")]
    assert leftovers == [], f"atomic-replace temp file leaked: {leftovers!r}"


def _manager(base, label, workspace):
    identity = dataclasses.replace(base.identity, product_name=label)
    return ChunkManager(binding=dataclasses.replace(base, identity=identity), workspace=workspace)


@pytest.fixture(params=["fsspec", "obstore"])
def new_manager(request, tmp_path):
    """Managers over one local store (or `store=`) that differ only in the product label."""
    opened = []

    def new(label, store="cube.zarr"):
        base = make_test_binding(tmp_path, product=store, driver=request.param)
        opened.append(_manager(base, label, tmp_path))
        return opened[-1]

    yield new
    for manager in opened:
        manager.close()


def _start(manager, label, run_id="r1"):
    manager.record_run_started(
        product=label, run_id=run_id, output_path="", output_format="zarr", size=0, meta={}
    )


def _legacy_runs(tmp_path, **labels):
    for run_id, label in labels.items():
        run_dir = tmp_path / "cube.zarr" / ".firecube" / "runs" / run_id
        run_dir.mkdir(parents=True)
        (run_dir / "run.json").write_text(json.dumps({"product": label, "run_id": run_id}))


def _on_stamp_write(monkeypatch, hook):
    for cls in (FsspecAtomicWriter, ObstoreAtomicWriter):

        def write(self, uri, data, real=cls.write_atomic):
            if uri.path.endswith("product.json"):
                hook()
            return real(self, uri, data)

        monkeypatch.setattr(cls, "write_atomic", write)


def _claim(label):
    return {
        "product": label,
        "domain": WriteDomain(product=label, category="zarr_region", name="g"),
    }


def test_stamp_is_written_once_and_another_label_is_refused_without_a_claims_directory(
    tmp_path, new_manager
):
    firecube = tmp_path / "cube.zarr" / ".firecube"
    new_manager("alpha").write_metadata_blob("alpha", b"x")
    first = (firecube / "product.json").read_bytes()
    assert json.loads(first)["product"] == "alpha"
    new_manager("alpha").write_metadata_blob("alpha", b"y")  # the same label passes
    with pytest.raises(errors.ProductLabelMismatchError) as refused:
        new_manager("beta").acquire_claim(**_claim("beta"), owner_id="o")
    assert (refused.value.given, refused.value.recorded) == ("beta", "alpha")
    assert (firecube / "product.json").read_bytes() == first and not (firecube / "claims").exists()


def test_simultaneous_first_writers_with_different_labels_leave_one_stamp(
    tmp_path, new_manager, monkeypatch
):
    barrier, results, crashed = threading.Barrier(2, timeout=30), {}, []
    _on_stamp_write(monkeypatch, barrier.wait)  # both pass the label check before either writes

    def write(label):
        try:
            _start(new_manager(label), label)
            results[label] = "wrote"
        except errors.ProductLabelMismatchError:
            results[label] = "refused"
        except BaseException as exc:
            crashed.append(exc)

    threads = [threading.Thread(target=write, args=(label,)) for label in ("alpha", "beta")]
    [t.start() for t in threads]
    [t.join(60) for t in threads]
    assert not any(t.is_alive() for t in threads), "a first writer stalled after the barrier"
    if crashed:
        raise crashed[0]
    stamp = json.loads((tmp_path / "cube.zarr/.firecube/product.json").read_text())
    assert sorted(results.values()) == ["refused", "wrote"] and results[stamp["product"]] == "wrote"


def test_corrupt_stamp_fails_closed_and_writes_nothing(tmp_path, new_manager):
    firecube = tmp_path / "cube.zarr" / ".firecube"
    firecube.mkdir(parents=True)
    (firecube / "product.json").write_text("{not json")
    with pytest.raises(errors.ManifestError):
        _start(new_manager("alpha"), "alpha")
    assert not list(firecube.rglob("run.json"))


def test_legacy_label_comes_from_run_json_not_orphans_and_is_stamped_on_first_write(
    tmp_path, new_manager
):
    _legacy_runs(tmp_path, ra="alpha")
    orphan = tmp_path / "cube.zarr" / ".firecube" / "runs" / "orphan"
    orphan.mkdir()
    (orphan / "events-0.jsonl").write_text('{"product": "beta"}\n')
    stamp, alpha = tmp_path / "cube.zarr/.firecube/product.json", new_manager("alpha")
    assert alpha.recorded_product_label() == "alpha" and not stamp.exists()
    _start(alpha, "alpha", "r2")
    assert json.loads(stamp.read_text())["product"] == "alpha"


@pytest.mark.parametrize(
    "call",
    [lambda m: m.recorded_product_label(), lambda m: _start(m, "alpha")],
    ids=["recorded_product_label", "run_creation"],
)
def test_legacy_runs_that_disagree_raise_a_label_conflict_and_stamp_nothing(
    tmp_path, new_manager, call
):
    _legacy_runs(tmp_path, ra="alpha", rb="beta")
    with pytest.raises(errors.ProductLabelConflictError) as conflict:
        call(new_manager("alpha"))
    assert conflict.value.labels == ("alpha", "beta")
    assert not (tmp_path / "cube.zarr/.firecube/product.json").exists()


def test_legacy_store_holding_only_a_claim_refuses_another_label(tmp_path, new_manager):
    with new_manager("alpha").acquire_claim(**_claim("alpha"), owner_id="o"):
        (tmp_path / "cube.zarr/.firecube/product.json").unlink()  # legacy: no stamp
        beta = new_manager("beta")
        assert beta.recorded_product_label() == "alpha"
        with pytest.raises(errors.ProductLabelMismatchError):
            beta.acquire_claim(**_claim("beta"), owner_id="x")


def test_failed_stamp_write_leaves_no_run_and_no_claim(tmp_path, new_manager, monkeypatch):
    def disk_full():
        raise OSError("disk full")

    _on_stamp_write(monkeypatch, disk_full)
    alpha = new_manager("alpha")
    with pytest.raises(OSError, match="disk full"):
        _start(alpha, "alpha")
    with pytest.raises(OSError, match="disk full"):
        alpha.acquire_claim(**_claim("alpha"), owner_id="o")
    assert not list((tmp_path / "cube.zarr").rglob("run.json")) and alpha.list_claims() == []


def test_manager_bound_to_old_label_is_refused_after_store_is_recreated(tmp_path, new_manager):
    alpha = new_manager("alpha")
    _start(alpha, "alpha")  # alpha has seen the stamp record its own label
    shutil.rmtree(tmp_path / "cube.zarr")
    _start(new_manager("beta"), "beta")
    with pytest.raises(errors.ProductLabelMismatchError) as refused:
        _start(alpha, "alpha", "r2")
    assert refused.value.recorded == "beta"
    assert not (tmp_path / "cube.zarr/.firecube/runs/r2").exists()


@pytest.mark.parametrize(
    ("writer", "args", "kwargs"),
    [
        ("take_snapshot", (), {"run_id": None, "trigger": "t", "consistency": "observation"}),
        ("write_metadata_blob", (b"x",), {}),
        ("write_run_journal", ("rb", b"{}"), {}),
        ("write_inventory", (Inventory(kind="digest", objects=()),), {}),
        ("rebuild_compact", (), {}),
    ],
)
def test_writer_under_a_second_label_is_refused_and_leaves_the_tree_unchanged(
    tmp_path, new_manager, writer, args, kwargs
):
    new_manager("alpha").write_metadata_blob("alpha", b"x")

    def tree():
        return {
            p: p.read_bytes() if p.is_file() else None for p in (tmp_path / "cube.zarr").rglob("*")
        }

    before = tree()
    with pytest.raises(errors.ProductLabelMismatchError):
        getattr(new_manager("beta"), writer)("beta", *args, **kwargs)
    assert tree() == before


moto_s3_endpoint, s3_bucket = s3_env.moto_s3_endpoint, s3_env.s3_bucket


@pytest.mark.s3
def test_second_label_is_refused_on_s3_and_the_stamp_keeps_the_first(
    moto_s3_endpoint, s3_bucket, tmp_path
):
    alpha, beta = (
        _manager(s3_env._binding_for_s3(moto_s3_endpoint), n, tmp_path) for n in ("alpha", "beta")
    )
    alpha.write_metadata_blob("alpha", b"x")
    with pytest.raises(errors.ProductLabelMismatchError):
        beta.write_metadata_blob("beta", b"y")
    stamp = s3_bucket.get_object(Bucket="test-bucket", Key="product.zarr/.firecube/product.json")
    assert json.loads(stamp["Body"].read())["product"] == "alpha"
