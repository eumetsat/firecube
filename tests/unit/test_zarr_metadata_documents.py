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

"""New module: no existing test covers store-wide capture of Zarr metadata documents."""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path
from typing import Any, Literal

import numpy as np
import pytest
import xarray as xr
import zarr

from firecube.core.filesystem.fsspec_backend import FsspecFilesystem
from firecube.core.storage.uri import StorageUri
from firecube.core.zarr.metadata_documents import (
    ABSENT,
    MetadataDocument,
    MetadataDocumentKind,
    build_consolidated_image,
    enumerate_metadata_documents,
    metadata_document_digest,
    must_understand,
    parse_node_document,
    patch_attributes,
)
from tests.helpers.storage import make_test_binding

pytestmark = pytest.mark.contract

CONTROL = (
    ".firecube/schema.json",
    ".firecube/index/current.json",
    ".firecube/slot_index/current.json",
)
NOISE = (".firecube/runs/r1/run.json", ".firecube/claims/c1.json", ".firecube/wal/1.jsonl")


def _build(tmp_path: Path, fmt: Literal[2, 3]) -> tuple[Path, FsspecFilesystem, StorageUri]:
    """Root group, subgroup, arrays ``a`` and ``sub/b``, consolidated root, plus .firecube files."""
    root = tmp_path / f"v{fmt}.zarr"
    group = zarr.open_group(str(root), mode="w", zarr_format=fmt)
    group.attrs["title"] = "cube"
    names: dict[str, Any] = {"dimension_names": ("x",)} if fmt == 3 else {}
    group.create_array("a", shape=(4,), chunks=(2,), dtype="f4", **names)[:] = 1.0
    group.create_group("sub").create_array("b", shape=(4,), chunks=(2,), dtype="i4", **names)
    zarr.consolidate_metadata(str(root))
    for name in (*CONTROL, *NOISE):
        (root / name).parent.mkdir(parents=True, exist_ok=True)
        (root / name).write_text('{"n": 1}', encoding="utf-8")
    fs = FsspecFilesystem.from_binding(make_test_binding(tmp_path, product=root.name))
    return root, fs, StorageUri.from_local_path(root)


def _doc(path: str, kind: MetadataDocumentKind, content: object) -> MetadataDocument:
    raw = json.dumps(content).encode()
    return MetadataDocument(path=path, kind=kind, raw=raw, digest=metadata_document_digest(raw))


V3 = {"zarr.json", "a/zarr.json", "sub/zarr.json", "sub/b/zarr.json"}
V2 = {".zgroup", ".zattrs", ".zmetadata", "a/.zarray", "sub/.zgroup", "sub/b/.zarray"} | {
    "a/.zattrs",
    "sub/.zattrs",
    "sub/b/.zattrs",
}


@pytest.mark.parametrize(
    ("fmt", "nodes", "images"),
    [
        (3, V3, ()),
        (3, V3 | {".zmetadata", "sub/.zmetadata"}, (".zmetadata", "sub/.zmetadata")),
        (2, V2, ()),
    ],
    ids=["v3", "v3-group-images", "v2"],
)
def test_enumeration_returns_node_and_control_documents_with_exact_bytes(
    tmp_path: Path, subtests, fmt: Literal[2, 3], nodes: set[str], images: tuple[str, ...]
) -> None:
    root, fs, uri = _build(tmp_path, fmt)
    for image in (*images, "a/.zmetadata") if images else ():  # "a/" is an array: not captured
        (root / image).write_text('{"metadata": {}}', encoding="utf-8")

    documents = enumerate_metadata_documents(fs, uri)

    paths = [d.path for d in documents]
    assert paths == sorted(paths)
    assert set(paths) == nodes | set(CONTROL)
    for document in documents:
        with subtests.test(msg=document.path):
            on_disk = (root / document.path).read_bytes()
            assert document.raw == on_disk
            assert document.digest == "sha256:" + hashlib.sha256(on_disk).hexdigest()


def test_walk_never_lists_array_directories(tmp_path: Path) -> None:
    _, fs, uri = _build(tmp_path, 2)
    listed: list[str] = []

    class CountingFs:
        def __getattr__(self, name: str):
            return getattr(fs, name)

        def ls(self, target, *args, **kwargs):
            listed.append(target.path)
            return fs.ls(target, *args, **kwargs)  # pyright: ignore[reportAttributeAccessIssue]

    enumerate_metadata_documents(CountingFs(), uri)  # pyright: ignore[reportArgumentType]

    assert sorted(listed) == sorted([uri.path, f"{uri.path}/sub"])


def test_parse_keeps_absent_null_and_present_apart() -> None:
    base = {"zarr_format": 3, "node_type": "array", "shape": [4]}
    nulls = {**base, "fill_value": None, "dimension_names": None, "attributes": {}}
    named = {**base, "fill_value": 0, "dimension_names": ["x"], "attributes": {"k": 1}}
    views = [
        parse_node_document(_doc("a/zarr.json", MetadataDocumentKind.V3_ARRAY, c))
        for c in (nulls, base, named)
    ]

    seen = [(v.fill_value, v.dimension_names, v.attributes) for v in views]

    assert seen == [(None, None, {}), (ABSENT, ABSENT, ABSENT), (0, ["x"], {"k": 1})]


def test_v2_array_dimensions_attribute_becomes_dimension_names() -> None:
    node = _doc("a/.zarray", MetadataDocumentKind.V2_ARRAY, {"zarr_format": 2, "shape": [4]})
    attrs = _doc("a/.zattrs", MetadataDocumentKind.V2_ATTRS, {"_ARRAY_DIMENSIONS": ["x"]})

    assert parse_node_document(node, attrs=attrs).dimension_names == ["x"]
    assert parse_node_document(node).dimension_names is ABSENT


def test_unmarked_v3_extension_blocks_patch_but_not_enumeration(tmp_path: Path) -> None:
    root, fs, uri = _build(tmp_path, 3)
    sub = root / "sub" / "zarr.json"
    content = json.loads(sub.read_text())
    content["ext_blocking"] = {"payload": 1}
    content["ext_ok"] = {"must_understand": False, "payload": 1}
    sub.write_text(json.dumps(content), encoding="utf-8")

    documents = {d.path: d for d in enumerate_metadata_documents(fs, uri)}

    assert must_understand(parse_node_document(documents["sub/zarr.json"])) == ("ext_blocking",)
    with pytest.raises(ValueError, match="ext_blocking"):
        patch_attributes(documents["sub/zarr.json"], set={"k": 1})
    # zarr-python's root consolidated image says must_understand false, so the root patches.
    assert patch_attributes(documents["zarr.json"], set={"k": 1})[0] == "zarr.json"


def test_v3_patch_keeps_every_other_key_and_value() -> None:
    original = {
        "zarr_format": 3,
        "node_type": "group",
        "attributes": {"keep": [1, 2], "drop": 0},
        "unknown_ext": {"must_understand": False, "deep": {"z": 1, "a": 2}},
    }
    node = _doc("sub/zarr.json", MetadataDocumentKind.V3_GROUP, original)

    path, new = patch_attributes(node, set={"added": None}, remove=["drop"])

    patched = json.loads(new)
    assert path == "sub/zarr.json"
    assert list(patched) == list(original)
    assert patched == {**original, "attributes": {"keep": [1, 2], "added": None}}


def test_v2_patch_rewrites_only_zattrs_and_creates_missing_one(tmp_path: Path) -> None:
    root, fs, uri = _build(tmp_path, 2)
    zarray = root / "a" / ".zarray"
    zarray.write_text(json.dumps({**json.loads(zarray.read_text()), "x_unknown": 1}))
    (root / "a" / ".zattrs").write_text('{"_ARRAY_DIMENSIONS": ["x"], "u": "m"}')
    (root / "sub" / "b" / ".zattrs").unlink(missing_ok=True)
    documents = {d.path: d for d in enumerate_metadata_documents(fs, uri)}

    path, new = patch_attributes(
        documents["a/.zarray"], attrs=documents["a/.zattrs"], set={"s": 1}, remove=["u"]
    )
    created_path, created = patch_attributes(documents["sub/b/.zarray"], set={"s": 1})

    assert (path, json.loads(new)) == ("a/.zattrs", {"_ARRAY_DIMENSIONS": ["x"], "s": 1})
    assert (created_path, json.loads(created)) == ("sub/b/.zattrs", {"s": 1})


@pytest.mark.parametrize("fmt", [2, 3], ids=["v2", "v3"])
def test_array_added_after_consolidation_is_enumerated(tmp_path: Path, fmt: Literal[2, 3]) -> None:
    root, fs, uri = _build(tmp_path, fmt)  # consolidated: its image does not list "late"
    names: dict[str, Any] = {"dimension_names": ("x",)} if fmt == 3 else {}
    zarr.open_group(str(root), mode="r+", use_consolidated=False).create_array(
        "late", shape=(2,), dtype="f4", **names
    )

    paths = {d.path for d in enumerate_metadata_documents(fs, uri)}

    assert ("late/zarr.json" if fmt == 3 else "late/.zarray") in paths


def _xarray_store(tmp_path: Path, fmt: Literal[2, 3]) -> tuple[Path, dict[str, MetadataDocument]]:
    root = tmp_path / f"x{fmt}.zarr"
    data = xr.Dataset({"v": ("x", np.arange(4.0), {"units": "m"})}, coords={"x": np.arange(4)})
    data.to_zarr(root, zarr_format=fmt, consolidated=True, encoding={"v": {"chunks": (2,)}})
    fs = FsspecFilesystem.from_binding(make_test_binding(tmp_path, product=root.name))
    return root, {
        d.path: d for d in enumerate_metadata_documents(fs, StorageUri.from_local_path(root))
    }


def _chunk_files(root: Path) -> dict[str, bytes]:
    return {str(p.relative_to(root)): p.read_bytes() for p in root.rglob("*") if p.name.isdigit()}


@pytest.mark.parametrize("fmt", [2, 3], ids=["v2", "v3"])
def test_built_image_equals_zarr_consolidation_of_the_edited_store(
    tmp_path: Path, fmt: Literal[2, 3]
) -> None:
    root, docs = _xarray_store(tmp_path, fmt)
    array, attrs = (
        (docs["v/zarr.json"], None) if fmt == 3 else (docs["v/.zarray"], docs["v/.zattrs"])
    )
    edited_path, edited = patch_attributes(array, attrs=attrs, set={"units": "km", "added": 1})
    after = {**{p: d.raw for p, d in docs.items()}, edited_path: edited}
    oracle, written = tmp_path / "oracle.zarr", tmp_path / "written.zarr"
    for copy in (oracle, written):
        shutil.copytree(root, copy)
        (copy / edited_path).write_bytes(edited)
    zarr.consolidate_metadata(str(oracle))
    image_name = ".zmetadata" if fmt == 2 else "zarr.json"

    image = build_consolidated_image(after, fmt)
    (written / image_name).write_bytes(image)

    assert json.loads(image) == json.loads((oracle / image_name).read_bytes())
    assert _chunk_files(written) == _chunk_files(root)


def test_image_of_a_foreign_zarray_with_attributes_reads_as_the_live_store(tmp_path: Path) -> None:
    root, docs = _xarray_store(tmp_path, 2)
    zarray = {**json.loads(docs["v/.zarray"].raw), "attributes": {"units": "zarray", "z": 1}}
    zattrs = {**json.loads(docs["v/.zattrs"].raw), "units": "zattrs"}
    (root / "v" / ".zarray").write_text(json.dumps(zarray))
    (root / "v" / ".zattrs").write_text(json.dumps(zattrs))
    after = {p: (root / p).read_bytes() for p in docs}

    (root / ".zmetadata").write_bytes(build_consolidated_image(after, 2))

    live = zarr.open_group(root, mode="r", use_consolidated=False)["v"].attrs.asdict()
    assert zarr.open_group(root, mode="r", use_consolidated=True)["v"].attrs.asdict() == live
    via_xarray = [xr.open_zarr(root, consolidated=c).v.attrs for c in (True, False)]
    assert via_xarray[0] == via_xarray[1]


@pytest.mark.parametrize(
    "extra",
    [
        {".zgroup": {"zarr_format": 2, "shape": [4]}},
        {"v/.zarray": {"zarr_format": 2, "chunks": [2]}},
    ],
    ids=["zgroup-holds-shape", "zarray-lacks-shape"],
)
def test_image_refuses_a_v2_group_with_shape_and_an_array_without(extra: dict[str, Any]) -> None:
    documents = {".zgroup": {"zarr_format": 2}, **extra}

    with pytest.raises(ValueError):
        build_consolidated_image({k: json.dumps(v).encode() for k, v in documents.items()}, 2)


@pytest.mark.parametrize(
    ("path", "content"),
    [("sub/zarr.json", "{not json"), ("a/zarr.json", '{"zarr_format": 3, "node_type": "weird"}')],
    ids=["malformed", "unknown_node_type"],
)
def test_malformed_or_unknown_v3_node_is_captured_as_unparsed(
    tmp_path: Path, path: str, content: str
) -> None:
    root, fs, uri = _build(tmp_path, 3)
    (root / path).write_text(content, encoding="utf-8")

    documents = {d.path: d for d in enumerate_metadata_documents(fs, uri)}

    assert documents[path].kind == MetadataDocumentKind.V3_UNPARSED
    assert documents[path].raw == (root / path).read_bytes()
