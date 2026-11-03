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

"""Metadata snapshots under .firecube/metadata/ (new concern: no other test module covers them)."""

import hashlib
import json

import numpy as np
import pytest
import xarray as xr

from firecube.core.controlplane import ChunkManager
from firecube.core.errors import ControlPlaneCorruptionError, ManifestError
from firecube.core.zarr.metadata_documents import ABSENT
from tests.helpers.storage import make_test_binding

pytestmark = pytest.mark.contract
PRODUCT, MAINT = "cube.zarr", "maintenance-run"
NODE_DOCS = {"zarr.json", ".zarray", ".zattrs", ".zgroup", ".zmetadata"}


@pytest.fixture(params=[2, 3], ids=["v2", "v3"])
def store(request, tmp_path):
    ds = xr.Dataset({"v": (("x",), np.arange(4.0))}, coords={"x": np.arange(4)})
    ds.v.attrs["note"] = "before"
    ds.to_zarr(tmp_path / PRODUCT, zarr_format=request.param, consolidated=request.param == 2)
    binding = make_test_binding(tmp_path, product=PRODUCT)
    manager = ChunkManager(binding=binding, workspace=tmp_path)
    yield request.param, tmp_path / PRODUCT, manager
    manager.close()


def _edit(root, rel, mutate):
    doc = json.loads((root / rel).read_text())
    mutate(doc)
    (root / rel).write_text(json.dumps(doc))


def _snap(manager, run_id=None):
    consistency = "observation" if run_id is None else "coherent"
    return manager.take_snapshot(PRODUCT, run_id=run_id, trigger="t", consistency=consistency)


def test_unchanged_store_gives_one_manifest_and_two_records(store):
    _, _, manager = store
    first, second = _snap(manager), _snap(manager)
    assert (first.manifest, first.run_id) == (second.manifest, None)
    assert second.parent == first.snapshot_id != second.snapshot_id
    assert manager.list_snapshots(PRODUCT) == [first, second]


def test_one_attribute_change_diffs_to_that_attribute_on_that_node(store):
    fmt, root, manager = store
    before = _snap(manager)
    rel = "v/.zattrs" if fmt == 2 else "v/zarr.json"
    _edit(root, rel, lambda d: (d if fmt == 2 else d["attributes"]).update(note="after"))
    diff = manager.diff_snapshots(PRODUCT, before.snapshot_id, _snap(manager).snapshot_id)
    assert [n.path for n in diff.nodes] == ["v"]
    node = diff.nodes[0]
    assert dict(node.attributes_changed) == {"note": ("before", "after")}
    assert not (node.attributes_added or node.attributes_removed or node.physical or node.extensions)  # fmt: skip
    assert (node.dimension_names, node.fill_value) == (None, None)
    assert (diff.added, diff.removed, diff.unparsed, diff.changed) == ((), (), (), (rel,))


FACETS = [
    (3, "v/zarr.json", lambda d: d.update(dimension_names=["y"]), "dimension_names", (["x"], ["y"])),
    (3, "v/zarr.json", lambda d: d.update(fill_value=7.0), "fill_value", ("NaN", 7.0)),
    (3, "v/zarr.json", lambda d: d.update(shape=[9]), "physical", {"shape": ([4], [9])}),
    (3, "v/zarr.json", lambda d: d.update(ext={"must_understand": False}), "extensions",
     {"ext": (ABSENT, {"must_understand": False})}),
    (2, "v/.zattrs", lambda d: d.update(_ARRAY_DIMENSIONS=["y"]), "dimension_names", (["x"], ["y"])),
    (2, "v/.zarray", lambda d: d.update(fill_value=7.0), "fill_value", ("NaN", 7.0)),
]  # fmt: skip


@pytest.mark.parametrize(
    ("store", "rel", "mutate", "facet", "expected"),
    [pytest.param(*c, id=f"v{c[0]}-{c[3]}") for c in FACETS],
    indirect=["store"],
)
def test_semantic_facet_change_is_reported_under_its_own_facet(store, rel, mutate, facet, expected):
    _, root, manager = store
    before = _snap(manager)
    _edit(root, rel, mutate)
    diff = manager.diff_snapshots(PRODUCT, before.snapshot_id, _snap(manager).snapshot_id)
    assert [n.path for n in diff.nodes] == ["v"]
    value = getattr(diff.nodes[0], facet)
    assert (dict(value) if isinstance(expected, dict) else value) == expected


def test_unparseable_document_keeps_its_path_level_diff(store):
    fmt, root, manager = store
    rel = "v/.zattrs" if fmt == 2 else "v/zarr.json"
    before = _snap(manager)
    (root / rel).write_text("{not json")
    diff = manager.diff_snapshots(PRODUCT, before.snapshot_id, _snap(manager).snapshot_id)
    assert (diff.changed, diff.unparsed, diff.nodes) == ((rel,), (rel,), ())


def test_coherent_snapshot_needs_the_claim_and_its_holder(store):
    _, root, manager = store
    with pytest.raises(ManifestError):
        _snap(manager, MAINT)
    assert not (root / ".firecube/metadata").exists()
    claim = manager.acquire_exclusive_maintenance(product=PRODUCT, run_id=MAINT)
    try:
        with pytest.raises(ManifestError):
            _snap(manager, "someone-else")
        assert manager.list_snapshots(PRODUCT) == []
        assert not (root / ".firecube/metadata").exists()
        record = _snap(manager, MAINT)
    finally:
        claim.release()
    assert (record.consistency, record.run_id) == ("coherent", MAINT)


EXPECTED_PATHS = {
    2: {".zattrs", ".zgroup", ".zmetadata", "v/.zarray", "v/.zattrs", "x/.zarray", "x/.zattrs"},
    3: {"zarr.json", "v/zarr.json", "x/zarr.json"},
}
CONTROL = {".firecube/schema.json", ".firecube/index/current.json", ".firecube/slot_index/current.json"}  # fmt: skip


def test_snapshot_holds_exactly_the_node_documents_and_blobs_equal_disk_bytes(store, subtests):
    fmt, root, manager = store
    claim = manager.acquire_exclusive_maintenance(product=PRODUCT, run_id=MAINT)
    try:
        for rel in (*CONTROL, ".firecube/runs/r1/events.jsonl", ".firecube/wal/0.jsonl", ".firecube/compact/LATEST.json"):  # fmt: skip
            (root / rel).parent.mkdir(parents=True, exist_ok=True)
            (root / rel).write_text("{}")
        documents = manager.read_snapshot(PRODUCT, _snap(manager, MAINT).snapshot_id).documents
    finally:
        claim.release()
    assert {e.path for e in documents} == EXPECTED_PATHS[fmt] | CONTROL
    for entry in documents:
        with subtests.test(msg=entry.path):
            assert entry.path.rsplit("/", 1)[-1] in NODE_DOCS | {"schema.json", "current.json"}
            blob = manager.read_metadata_blob(PRODUCT, entry.digest.removeprefix("sha256:"))
            assert blob == (root / entry.path).read_bytes()


def test_blob_write_is_create_if_absent_and_a_tampered_blob_is_detected(store):
    _, root, manager = store
    blob_id = manager.write_metadata_blob(PRODUCT, b"payload")
    assert blob_id == hashlib.sha256(b"payload").hexdigest()
    path = root / ".firecube/metadata/blobs/sha256" / blob_id[:2] / blob_id
    before = path.stat()
    assert manager.write_metadata_blob(PRODUCT, b"payload") == blob_id
    after = path.stat()
    assert (after.st_ino, after.st_mtime_ns) == (before.st_ino, before.st_mtime_ns)
    path.write_bytes(b"tampered")
    with pytest.raises(ControlPlaneCorruptionError):
        manager.read_metadata_blob(PRODUCT, blob_id)
