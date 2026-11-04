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

"""Generic group-level attributes for DirectZarr schemas (`ZarrGroupSpec.attrs`).

Firecube writes plugin-declared group attrs verbatim onto the group's zarr.json;
it does not interpret them (no convention is assumed). These tests use generic
attribute names on purpose — the core mechanism is convention-agnostic.

The second half covers the metadata authority every zarr.json change goes
through: refusals leave the store untouched, nodes are read from the store,
never from a stale handle or a consolidated index, and an authority lives no
longer than the claim it names.
"""

from __future__ import annotations

import json
import threading
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
import zarr
from zarr.storage import LocalStore

from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.types import WriteDomain
from firecube.core.zarr.guard.metadata_authority import (
    EMPTY_GROUP_DOCUMENT,
    AuthorityBasis,
    DocumentPreconditionError,
    MetadataAuthority,
    MetadataAuthorityError,
    MetadataScope,
    create_or_open_array,
    ensure_group,
    hold_authority,
    publish_document,
    publish_document_if_absent,
    publish_raw,
    set_attributes,
    update_document,
)
from firecube.core.zarr.metadata_documents import ABSENT, metadata_document_digest
from firecube.core.zarr.region_writer import RegionZarrWriter
from firecube.ingestor.api import ZarrArraySpec, ZarrGroupSpec
from firecube.ingestor.templates.direct_zarr import (
    _compute_schema_hash,
    _setup_global_zarr_schema,
)
from tests.helpers.storage import make_test_binding

_ATTRS = {"title": "demo dataset", "owner": "team-x"}


def _strategy(store_uri: str) -> SimpleNamespace:
    return SimpleNamespace(
        _store_uri=store_uri,
        _storage_config=None,
        _session=None,
        _coord_names_by_group={},
    )


def _group_attrs(store_path, group: str) -> dict:
    grp = zarr.open_group(store=str(store_path), mode="r", zarr_format=3)[group]
    return dict(cast(Any, grp).attrs)


@pytest.mark.unit
def test_set_group_attrs_roundtrip(tmp_path) -> None:
    store_path = tmp_path / "g.zarr"
    writer = RegionZarrWriter(str(store_path))
    writer.ensure_group("data/x", shape=(2,), dtype="float64", fill_value=0.0)

    writer.set_group_attrs("data", _ATTRS)

    assert _group_attrs(store_path, "data") == _ATTRS


@pytest.mark.unit
def test_set_group_attrs_is_noop_for_empty(tmp_path) -> None:
    store_path = tmp_path / "g.zarr"
    writer = RegionZarrWriter(str(store_path))
    writer.ensure_group("data/x", shape=(2,), dtype="float64", fill_value=0.0)

    writer.set_group_attrs("data", None)
    writer.set_group_attrs("data", {})

    assert _group_attrs(store_path, "data") == {}


@pytest.mark.unit
def test_set_group_attrs_rejects_reserved_names(tmp_path) -> None:
    store_path = tmp_path / "g.zarr"
    writer = RegionZarrWriter(str(store_path))
    writer.ensure_group("data/x", shape=(2,), dtype="float64", fill_value=0.0)

    with pytest.raises(ValueError):
        writer.set_group_attrs("data", {"firecube_internal": True})


@pytest.mark.unit
def test_group_spec_attrs_rejects_non_mapping() -> None:
    with pytest.raises(ValueError):
        ZarrGroupSpec(group="data", attrs=cast(Any, "not-a-mapping"))


@pytest.mark.unit
def test_group_spec_attrs_defaults_to_none() -> None:
    assert ZarrGroupSpec(group="data").attrs is None


@pytest.mark.unit
def test_schema_hash_unchanged_when_no_group_attrs() -> None:
    """attrs=None and attrs={} must hash identically (backward-compatible: a
    schema declaring no group attrs keeps its pre-feature identity)."""
    arrays = [ZarrArraySpec(name="v", shape=(0, 2), dtype="float32", chunks=(1, 2))]
    expected = {"data": 4}
    h_none = _compute_schema_hash([ZarrGroupSpec(group="data", arrays=arrays)], expected)
    h_empty = _compute_schema_hash([ZarrGroupSpec(group="data", arrays=arrays, attrs={})], expected)
    assert h_none == h_empty


@pytest.mark.unit
def test_schema_hash_changes_with_group_attrs() -> None:
    arrays = [ZarrArraySpec(name="v", shape=(0, 2), dtype="float32", chunks=(1, 2))]
    expected = {"data": 4}
    h_none = _compute_schema_hash([ZarrGroupSpec(group="data", arrays=arrays)], expected)
    h_attrs = _compute_schema_hash(
        [ZarrGroupSpec(group="data", arrays=arrays, attrs={"title": "a"})], expected
    )
    h_other = _compute_schema_hash(
        [ZarrGroupSpec(group="data", arrays=arrays, attrs={"title": "b"})], expected
    )
    assert h_none != h_attrs
    assert h_attrs != h_other
    # Same attrs -> same hash (deterministic).
    h_attrs_again = _compute_schema_hash(
        [ZarrGroupSpec(group="data", arrays=arrays, attrs={"title": "a"})], expected
    )
    assert h_attrs == h_attrs_again


@pytest.mark.unit
def test_setup_global_zarr_schema_writes_group_attrs(tmp_path) -> None:
    store_path = tmp_path / "setup.zarr"
    schema = [
        ZarrGroupSpec(
            group="data",
            arrays=[ZarrArraySpec(name="v", shape=(4, 2), dtype="float32", chunks=(1, 2))],
            attrs=_ATTRS,
        )
    ]

    _setup_global_zarr_schema(
        strategy=_strategy(str(store_path)),
        schema=schema,
        global_expected={"data": 4},
        product="product",
        run_id="run-1",
        chunk_manager=None,
    )

    assert _group_attrs(store_path, "data") == _ATTRS


# --- metadata authority -----------------------------------------------------------------

_ARRAY_KWARGS: dict[str, Any] = {"shape": (2,), "dtype": "int32", "chunks": (2,), "fill_value": 0}
_GROUP_DOC = json.dumps({"zarr_format": 3, "node_type": "group", "attributes": {"t": 1}}).encode()


def _local_root(path: Path) -> zarr.Group:
    """A store holding the array ``g/a``, opened as its root group."""
    root = zarr.open_group(store=LocalStore(path), mode="a", zarr_format=3, use_consolidated=False)
    root.create_array("g/a", **_ARRAY_KWARGS)
    return root


def _on_disk(path: Path, node: str) -> Any:
    return zarr.open_group(store=LocalStore(path), mode="r", zarr_format=3, use_consolidated=False)[
        node
    ]


def _scope(path: Path, prefix: str = "") -> MetadataScope:
    return MetadataScope(str(path.resolve()), prefix)


@pytest.mark.unit
def test_authority_refuses_missing_released_out_of_scope_and_foreign(tmp_path: Path) -> None:
    root = _local_root(tmp_path)

    with pytest.raises(MetadataAuthorityError, match="no MetadataAuthority"):
        set_attributes(None, root, "g/a", {"k": 1})
    with hold_authority(_scope(tmp_path, "g"), claim=None) as auth:
        assert auth.basis is AuthorityBasis.PROCESS_LOCAL
        set_attributes(auth, root, "g/a", {"k": 1})
        with pytest.raises(MetadataAuthorityError, match="outside the scope"):
            set_attributes(auth, root, "h/a", {"k": 1})
    with pytest.raises(MetadataAuthorityError, match="was released"):
        set_attributes(auth, root, "g/a", {"k": 2})
    foreign = MetadataAuthority.process_local(MetadataScope(str(tmp_path / "elsewhere")))
    with pytest.raises(MetadataAuthorityError, match="not the store"):
        set_attributes(foreign, root, "g/a", {"k": 3})

    assert dict(_on_disk(tmp_path, "g/a").attrs) == {"k": 1}


@pytest.mark.unit
@pytest.mark.filterwarnings("ignore:Consolidated metadata:UserWarning")
def test_authority_does_not_trust_consolidated_metadata(tmp_path: Path) -> None:
    _local_root(tmp_path)
    zarr.consolidate_metadata(LocalStore(tmp_path))
    consolidated = zarr.open_group(store=LocalStore(tmp_path), mode="r+", zarr_format=3)
    zarr.open_array(store=LocalStore(tmp_path), path="g/a", mode="r+", zarr_format=3).resize((7,))
    auth = MetadataAuthority.process_local(_scope(tmp_path))

    set_attributes(auth, consolidated, "g/a", {"k": 1})

    assert _on_disk(tmp_path, "g/a").shape == (7,)
    assert dict(_on_disk(tmp_path, "g/a").attrs) == {"k": 1}


@pytest.mark.unit
def test_claim_scope_bounds_the_authority(tmp_path: Path) -> None:
    product = "prod.zarr"
    manager = ChunkManager(binding=make_test_binding(tmp_path, product=product), workspace=tmp_path)
    store_path = tmp_path / product
    root = _local_root(store_path)
    domain = WriteDomain(product=product, category="zarr_schema_global", name="g:setup")

    with hold_authority(
        _scope(store_path, "g"),
        claim=manager.acquire_claim(product=product, domain=domain, owner_id="run-1"),
        claim_domain=domain.identifier,
        owner_id="run-1",
    ) as auth:
        held = [claim.domain for claim in manager.list_claims(product=product)]
        assert auth.active and auth.basis is AuthorityBasis.CLAIM
        set_attributes(auth, root, "g/a", {"k": 1})

    assert held == [domain.identifier]
    assert manager.list_claims(product=product) == []
    assert not auth.active
    with pytest.raises(MetadataAuthorityError, match="was released"):
        set_attributes(auth, root, "g/a", {"k": 2})
    assert dict(_on_disk(store_path, "g/a").attrs) == {"k": 1}


_EVERY_OPERATION: dict[str, Callable[[MetadataAuthority, LocalStore], object]] = {
    "ensure_group": lambda auth, store: ensure_group(auth, store, "g"),
    "create_or_open_array": lambda auth, store: create_or_open_array(
        auth, store, "g/a", create_kwargs=_ARRAY_KWARGS, default_attrs={"_FillValue": 0}
    ),
    "set_attributes": lambda auth, store: set_attributes(auth, store, "", {"k": 1}),
    "publish_document": lambda auth, store: publish_document(auth, store, "zarr.json", _GROUP_DOC),
    "publish_document_if_absent": lambda auth, store: publish_document_if_absent(
        auth, store, "zarr.json", _GROUP_DOC
    ),
    "update_document": lambda auth, store: update_document(
        auth, store, "zarr.json", lambda current: _GROUP_DOC
    ),
}


@pytest.mark.unit
@pytest.mark.parametrize("operation", sorted(_EVERY_OPERATION))
def test_released_authority_leaves_a_fresh_store_without_zarr_json(
    tmp_path: Path, operation: str
) -> None:
    store_path = tmp_path / "fresh.zarr"
    with hold_authority(_scope(store_path), claim=None) as auth:
        pass

    with pytest.raises(MetadataAuthorityError, match="was released"):
        _EVERY_OPERATION[operation](auth, LocalStore(store_path))

    assert not store_path.exists() or list(store_path.rglob("*")) == []


@pytest.mark.unit
def test_ensure_group_creates_missing_parents_and_never_rewrites_one(tmp_path: Path) -> None:
    store = LocalStore(tmp_path)
    auth = MetadataAuthority.process_local(_scope(tmp_path))
    ensure_group(auth, store, "")
    set_attributes(auth, store, "", {"root": "kept"})

    ensure_group(auth, store, "g/h")
    array, created = create_or_open_array(
        auth,
        store,
        "g/h/a",
        create_kwargs={**_ARRAY_KWARGS, "attributes": {"_FillValue": 9, "plugin": "p"}},
        default_attrs={"_FillValue": 0, "firecube_marker": True},
    )

    assert created
    assert dict(array.attrs) == {"_FillValue": 9, "plugin": "p", "firecube_marker": True}
    assert dict(_on_disk(tmp_path, "g/h/a").attrs) == dict(array.attrs)
    stored_root = zarr.open_group(store=store, mode="r", zarr_format=3, use_consolidated=False)
    assert dict(stored_root.attrs) == {"root": "kept"}


@pytest.mark.unit
def test_create_or_open_array_grows_validates_and_adds_absent_defaults(tmp_path: Path) -> None:
    root = _local_root(tmp_path)
    auth = MetadataAuthority.process_local(_scope(tmp_path))
    set_attributes(auth, root, "g/a", {"_FillValue": 7})
    seen: list[tuple[int, ...]] = []

    array, created = create_or_open_array(
        auth,
        root,
        "g/a",
        create_kwargs=_ARRAY_KWARGS,
        grow_to=(6,),
        default_attrs={"_FillValue": 0, "firecube_marker": True},
        validate=lambda arr: seen.append(arr.shape),
    )

    assert not created
    assert seen == [(6,)]
    assert array.shape == (6,)
    assert dict(_on_disk(tmp_path, "g/a").attrs) == {"_FillValue": 7, "firecube_marker": True}
    with pytest.raises(ValueError, match="overwrite"):
        create_or_open_array(auth, root, "g/a", create_kwargs={**_ARRAY_KWARGS, "overwrite": True})


@pytest.mark.unit
def test_publish_document_puts_a_whole_document_inside_its_scope(tmp_path: Path) -> None:
    _local_root(tmp_path)
    store = LocalStore(tmp_path)
    auth = MetadataAuthority.process_local(_scope(tmp_path, "g"))
    before = (tmp_path / "zarr.json").read_bytes()

    publish_document(auth, store, "g/zarr.json", _GROUP_DOC)

    assert (tmp_path / "g" / "zarr.json").read_bytes() == _GROUP_DOC
    assert dict(_on_disk(tmp_path, "g").attrs) == {"t": 1}
    with pytest.raises(MetadataAuthorityError, match="outside the scope"):
        publish_document(auth, store, "zarr.json", _GROUP_DOC)
    with pytest.raises(ValueError, match=r"not a zarr\.json document key"):
        publish_document(auth, store, "g/a/c/0", _GROUP_DOC)
    with pytest.raises(ValueError, match="not a Zarr v3 node document"):
        publish_document(auth, store, "g/zarr.json", b'{"zarr_format": 2}')
    assert (tmp_path / "zarr.json").read_bytes() == before
    assert (tmp_path / "g" / "zarr.json").read_bytes() == _GROUP_DOC


@pytest.mark.unit
def test_publish_document_if_absent_never_replaces_a_document(tmp_path: Path) -> None:
    _local_root(tmp_path)
    store = LocalStore(tmp_path)
    auth = MetadataAuthority.process_local(_scope(tmp_path, "g"))
    existing = (tmp_path / "g" / "a" / "zarr.json").read_bytes()

    assert publish_document_if_absent(auth, store, "g/h/zarr.json", _GROUP_DOC) is True
    assert publish_document_if_absent(auth, store, "g/a/zarr.json", _GROUP_DOC) is False

    assert (tmp_path / "g" / "h" / "zarr.json").read_bytes() == _GROUP_DOC
    assert (tmp_path / "g" / "a" / "zarr.json").read_bytes() == existing
    with pytest.raises(MetadataAuthorityError, match="outside the scope"):
        publish_document_if_absent(auth, store, "x/zarr.json", _GROUP_DOC)
    with pytest.raises(ValueError, match=r"not a zarr\.json document key"):
        publish_document_if_absent(auth, store, "g/x/c/0", _GROUP_DOC)
    with pytest.raises(ValueError, match="not a Zarr v3 node document"):
        publish_document_if_absent(auth, store, "g/x/zarr.json", b'{"zarr_format": 2}')
    assert not (tmp_path / "x").exists()
    assert not (tmp_path / "g" / "x").exists()


_V2_DOCS: dict[str, dict[str, Any]] = {
    ".zattrs": {"k": 1},
    ".zgroup": {"zarr_format": 2},
    ".zarray": {"zarr_format": 2, "shape": [2], "chunks": [2], "dtype": "<i4"},
    ".zmetadata": {"zarr_consolidated_format": 1, "metadata": {}},
}


_WRONG_FORMAT = {
    **{f"v3-as-{n}": (n, json.loads(_GROUP_DOC)) for n in _V2_DOCS},
    "v2-as-zarr-json": ("zarr.json", _V2_DOCS[".zgroup"]),
}


@pytest.mark.unit
def test_publish_raw_writes_only_where_the_stored_digest_is_the_expected_one(
    tmp_path: Path,
) -> None:
    _local_root(tmp_path)
    store = LocalStore(tmp_path)
    auth = MetadataAuthority.process_local(_scope(tmp_path))
    existing = (tmp_path / "g" / "zarr.json").read_bytes()
    for expect in (metadata_document_digest(b"{}"), ABSENT):  # stale digest; ABSENT over a document
        with pytest.raises(DocumentPreconditionError):
            publish_raw(auth, store, "g/zarr.json", _GROUP_DOC, expect=expect)
    assert (tmp_path / "g" / "zarr.json").read_bytes() == existing

    publish_raw(auth, store, "h/zarr.json", _GROUP_DOC, expect=ABSENT)
    publish_raw(auth, store, "g/zarr.json", _GROUP_DOC, expect=metadata_document_digest(existing))

    assert (tmp_path / "h" / "zarr.json").read_bytes() == _GROUP_DOC
    assert (tmp_path / "g" / "zarr.json").read_bytes() == _GROUP_DOC


@pytest.mark.unit
@pytest.mark.parametrize("name", sorted(_V2_DOCS))
def test_publish_raw_admits_each_v2_document_name(tmp_path: Path, name: str) -> None:
    auth = MetadataAuthority.process_local(_scope(tmp_path))
    data = json.dumps(_V2_DOCS[name]).encode()

    publish_raw(auth, LocalStore(tmp_path), f"n/{name}", data, expect=ABSENT)

    assert (tmp_path / "n" / name).read_bytes() == data


@pytest.mark.unit
@pytest.mark.parametrize(("name", "document"), _WRONG_FORMAT.values(), ids=_WRONG_FORMAT)
def test_publish_raw_refuses_a_payload_of_the_wrong_format(
    tmp_path: Path, name: str, document: dict[str, Any]
) -> None:
    auth = MetadataAuthority.process_local(_scope(tmp_path))

    with pytest.raises(ValueError, match="not a"):
        publish_raw(
            auth, LocalStore(tmp_path), f"n/{name}", json.dumps(document).encode(), expect=ABSENT
        )

    assert not (tmp_path / "n").exists()


@pytest.mark.unit
def test_publish_raw_refuses_an_out_of_scope_path_and_a_chunk_key(tmp_path: Path) -> None:
    _local_root(tmp_path)
    auth = MetadataAuthority.process_local(_scope(tmp_path, "g"))
    attrs = b'{"k": 1}'

    with pytest.raises(MetadataAuthorityError, match="outside the scope"):
        publish_raw(auth, LocalStore(tmp_path), "x/.zattrs", attrs, expect=ABSENT)
    with pytest.raises(ValueError, match="not a metadata document key"):
        publish_raw(auth, LocalStore(tmp_path), "g/a/c/0", attrs, expect=ABSENT)

    assert not (tmp_path / "x").exists()
    assert not (tmp_path / "g" / "a" / "c" / "0").exists()


@pytest.mark.unit
def test_a_document_a_writer_creates_meanwhile_is_not_replaced(tmp_path: Path) -> None:
    """A copier finds no document; a writer holding the node lock creates and stamps one.

    Deterministic: the writer holds the node's lock until the copier is
    waiting for it (the lock's user count reaches two), then creates the
    document. Only a copier that decides under the lock keeps the writer's
    document; deciding before it reverts the stamp.
    """
    from firecube.core.zarr.guard import metadata_authority

    store = LocalStore(tmp_path)
    auth = MetadataAuthority.process_local(_scope(tmp_path))
    ensure_group(auth, store, "g")
    copy = json.dumps({"zarr_format": 3, "node_type": "group", "attributes": {}}).encode()
    key = (auth.scope.store, "g/h")
    outcome: list[object] = []

    def copier() -> None:
        try:
            outcome.append(publish_document_if_absent(auth, store, "g/h/zarr.json", copy))
        except BaseException as exc:  # reported below
            outcome.append(exc)

    with metadata_authority._NODE_LOCKS.hold(key):
        thread = threading.Thread(target=copier)
        thread.start()
        for _ in range(1000):
            if metadata_authority._NODE_LOCKS._entries[key].users == 2:
                break
            threading.Event().wait(0.01)
        else:
            pytest.fail("the copier never waited for the node lock")
        ensure_group(auth, store, "g/h")  # re-entrant: this thread holds the lock
        set_attributes(auth, store, "g/h", {"firecube_static_written": True})
    thread.join(10)

    assert outcome == [False]
    assert dict(_on_disk(tmp_path, "g/h").attrs).get("firecube_static_written") is True


@pytest.mark.unit
def test_release_waits_for_an_operation_already_admitted(tmp_path: Path) -> None:
    root = _local_root(tmp_path)
    events: list[str] = []
    errors: list[BaseException] = []
    handed_over, in_operation, finish = threading.Event(), threading.Event(), threading.Event()
    box: dict[str, MetadataAuthority] = {}

    class _Claim:
        def __enter__(self) -> _Claim:
            return self

        def __exit__(self, *exc: object) -> None:
            events.append("claim-out")

    def holder() -> None:
        with hold_authority(
            _scope(tmp_path), claim=_Claim(), claim_domain="p:test:g", owner_id="run-1"
        ) as auth:
            box["auth"] = auth
            handed_over.set()
            in_operation.wait(10)

    def slow_validate(_arr: Any) -> None:
        events.append("op-in")
        in_operation.set()
        finish.wait(10)
        events.append("op-out")

    def worker() -> None:
        try:
            handed_over.wait(10)
            create_or_open_array(
                box["auth"], root, "g/a", create_kwargs=_ARRAY_KWARGS, validate=slow_validate
            )
        except BaseException as exc:  # reported below
            errors.append(exc)

    threads = [threading.Thread(target=holder), threading.Thread(target=worker)]
    for thread in threads:
        thread.start()
    threads[0].join(0.5)  # a release that does not wait finishes here, before the operation
    finish.set()
    for thread in threads:
        thread.join(10)

    assert errors == []
    assert events == ["op-in", "op-out", "claim-out"]


def _refusal(operation: Callable[[], object]) -> BaseException | None:
    """The exception *operation* raised, or ``None``: the assertion names what it got."""
    try:
        operation()
    except Exception as exc:
        return exc
    return None


@pytest.mark.unit
def test_subtree_authority_refuses_to_create_a_missing_ancestor(tmp_path: Path) -> None:
    store_path = tmp_path / "fresh.zarr"
    auth = MetadataAuthority.process_local(_scope(store_path, "g"))

    for operation in (
        lambda: ensure_group(auth, LocalStore(store_path), "g"),
        lambda: create_or_open_array(
            auth, LocalStore(store_path), "g/a", create_kwargs=_ARRAY_KWARGS
        ),
    ):
        error = _refusal(operation)
        assert isinstance(error, MetadataAuthorityError), error
        assert "outside the scope" in str(error)
        assert not store_path.exists() or list(store_path.rglob("*")) == []

    ensure_group(MetadataAuthority.process_local(_scope(store_path)), LocalStore(store_path), "")
    assert ensure_group(auth, LocalStore(store_path), "g").path == "g"


def _memory_stores() -> tuple[Any, Any, Any]:
    from zarr.storage import MemoryStore

    return MemoryStore(), MemoryStore(), None


def _fsspec_stores() -> tuple[Any, Any, Any]:
    import uuid

    from zarr.storage import FsspecStore

    base = f"memory://metadata-authority-{uuid.uuid4().hex}"
    return (
        FsspecStore.from_url(f"{base}/bound"),
        FsspecStore.from_url(f"{base}/foreign"),
        FsspecStore.from_url(f"{base}/bound"),
    )


def _obstore_stores(tmp_path: Path) -> tuple[Any, Any, Any]:
    from obstore.store import LocalStore as ObstoreLocal
    from zarr.storage import ObjectStore

    (tmp_path / "bound").mkdir()
    (tmp_path / "foreign").mkdir()
    return (
        ObjectStore(ObstoreLocal(str(tmp_path / "bound"))),
        ObjectStore(ObstoreLocal(str(tmp_path / "foreign"))),
        ObjectStore(ObstoreLocal(str(tmp_path / "bound"))),
    )


@pytest.mark.unit
@pytest.mark.parametrize("backend", ["memory", "fsspec", "obstore"])
def test_authority_refuses_a_store_it_cannot_verify(tmp_path: Path, backend: str) -> None:
    bound, foreign, equal = {
        "memory": _memory_stores,
        "fsspec": _fsspec_stores,
        "obstore": lambda: _obstore_stores(tmp_path),
    }[backend]()
    zarr.open_group(store=foreign, mode="a", zarr_format=3)
    unbound = MetadataAuthority.process_local(MetadataScope(f"{backend}://product"))

    error = _refusal(lambda: set_attributes(unbound, foreign, "", {"k": 1}))
    assert isinstance(error, MetadataAuthorityError), error
    assert "cannot verify" in str(error)
    bound_auth = MetadataAuthority.process_local(
        MetadataScope(f"{backend}://product", backing=bound)
    )
    error = _refusal(lambda: set_attributes(bound_auth, foreign, "", {"k": 2}))
    assert isinstance(error, MetadataAuthorityError), error
    assert "not the store" in str(error)
    assert dict(zarr.open_group(store=foreign, mode="r", zarr_format=3).attrs) == {}

    ensure_group(bound_auth, bound, "")
    for same in (bound,) if equal is None else (bound, equal):
        set_attributes(bound_auth, same, "", {"k": 1})
    assert dict(zarr.open_group(store=bound, mode="r", zarr_format=3).attrs) == {"k": 1}


class _RecordingClaim:
    """A claim that records the stored attributes of ``g/a`` when it is released."""

    def __init__(self, path: Path, events: list[Any]) -> None:
        self._path = path
        self._events = events

    def __enter__(self) -> _RecordingClaim:
        return self

    def __exit__(self, *exc: object) -> None:
        self._events.append(("claim-out", dict(_on_disk(self._path, "g/a").attrs)))


@pytest.mark.unit
@pytest.mark.parametrize("second", ["revoke", "another-exit"])
def test_a_second_refused_release_keeps_the_queued_claim_exit(tmp_path: Path, second: str) -> None:
    root = _local_root(tmp_path)
    events: list[Any] = []
    held = hold_authority(
        _scope(tmp_path),
        claim=_RecordingClaim(tmp_path, events),
        claim_domain="p:test:g",
        owner_id="run-1",
    )
    auth = held.__enter__()
    errors: list[BaseException | None] = []

    def release_twice_inside(_arr: Any) -> None:
        errors.append(_refusal(lambda: held.__exit__(None, None, None)))
        if second == "revoke":
            errors.append(_refusal(auth.revoke))
        else:
            errors.append(_refusal(lambda: auth._release(lambda: events.append("second-exit"))))

    create_or_open_array(
        auth,
        root,
        "g/a",
        create_kwargs=_ARRAY_KWARGS,
        default_attrs={"marker": True},
        validate=release_twice_inside,
    )

    assert all(isinstance(e, MetadataAuthorityError) for e in errors), errors
    assert "inside one of its own operations" in str(errors[0])
    expected: list[Any] = [("claim-out", {"marker": True})]
    if second == "another-exit":
        expected.append("second-exit")  # both run, the first queued first
    assert events == expected
    assert not auth.active


class _ThreadBoundClaim:
    """A claim held by an RLock: only the thread that entered it can exit it."""

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.exited_on: list[int] = []

    def __enter__(self) -> _ThreadBoundClaim:
        self.lock.acquire()
        return self

    def __exit__(self, *exc: object) -> None:
        self.lock.release()  # raises on any thread but the one that entered
        self.exited_on.append(threading.get_ident())


@pytest.mark.unit
def test_a_claim_exit_refused_on_one_thread_runs_on_that_thread(tmp_path: Path) -> None:
    """A revokes inside its operation, then B leaves its claim scope inside another one."""
    root = _local_root(tmp_path)
    root.create_array("g/b", **_ARRAY_KWARGS)
    claim = _ThreadBoundClaim()
    held = hold_authority(_scope(tmp_path), claim=claim, claim_domain="p:test:g", owner_id="run-1")
    auth_ready, a_queued, b_queued = threading.Event(), threading.Event(), threading.Event()
    box: dict[str, Any] = {}
    refusals: list[BaseException | None] = []
    errors: list[BaseException] = []

    def a_validate(_arr: Any) -> None:
        refusals.append(_refusal(box["auth"].revoke))  # queued with no exit
        a_queued.set()
        assert b_queued.wait(10)

    def b_validate(_arr: Any) -> None:
        assert a_queued.wait(10)
        refusals.append(_refusal(lambda: held.__exit__(None, None, None)))  # the claim's exit
        b_queued.set()

    def thread_a() -> None:
        try:
            assert auth_ready.wait(10)
            create_or_open_array(
                box["auth"], root, "g/a", create_kwargs=_ARRAY_KWARGS, validate=a_validate
            )
        except BaseException as exc:
            errors.append(exc)

    def thread_b() -> None:
        try:
            box["auth"] = held.__enter__()  # the claim is entered on B
            box["b"] = threading.get_ident()
            auth_ready.set()
            create_or_open_array(
                box["auth"], root, "g/b", create_kwargs=_ARRAY_KWARGS, validate=b_validate
            )
        except BaseException as exc:
            errors.append(exc)
        finally:
            a_queued.set()
            b_queued.set()

    threads = [threading.Thread(target=thread_a), threading.Thread(target=thread_b)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(10)

    assert not any(thread.is_alive() for thread in threads), "release deadlocked"
    assert errors == []
    assert len(refusals) == 2
    assert all(isinstance(e, MetadataAuthorityError) for e in refusals), refusals
    assert claim.exited_on == [box["b"]]
    assert claim.lock.acquire(blocking=False), "the claim was left held"
    claim.lock.release()
    assert not box["auth"].active


@pytest.mark.unit
def test_release_inside_an_operation_does_not_deadlock_on_a_waiting_peer(tmp_path: Path) -> None:
    root = _local_root(tmp_path)
    events: list[Any] = []
    held = hold_authority(
        _scope(tmp_path),
        claim=_RecordingClaim(tmp_path, events),
        claim_domain="p:test:g",
        owner_id="run-1",
    )
    auth = held.__enter__()
    peer_admitted = threading.Event()

    def peer() -> None:
        peer_admitted.set()
        set_attributes(auth, root, "g/a", {"peer": True})

    peer_thread = threading.Thread(target=peer, daemon=True)

    def release_with_peer_waiting(_arr: Any) -> None:
        peer_thread.start()
        peer_admitted.wait(10)
        while len(auth._in_flight) < 2:  # the peer is admitted and waits for this node
            threading.Event().wait(0.001)
        _refusal(lambda: held.__exit__(None, None, None))

    owner = threading.Thread(
        target=lambda: create_or_open_array(
            auth, root, "g/a", create_kwargs=_ARRAY_KWARGS, validate=release_with_peer_waiting
        ),
        daemon=True,
    )
    owner.start()
    owner.join(10)
    peer_thread.join(10)

    assert not owner.is_alive() and not peer_thread.is_alive(), "release deadlocked"
    assert events == [("claim-out", {"peer": True})]


@pytest.mark.unit
def test_attributes_written_by_validate_survive_the_defaults(tmp_path: Path) -> None:
    root = _local_root(tmp_path)
    auth = MetadataAuthority.process_local(_scope(tmp_path))

    def write_nested(_arr: Any) -> None:
        set_attributes(auth, root, "g/a", {"nested": True})

    create_or_open_array(
        auth,
        root,
        "g/a",
        create_kwargs=_ARRAY_KWARGS,
        default_attrs={"default": True},
        validate=write_nested,
    )

    assert dict(_on_disk(tmp_path, "g/a").attrs) == {"nested": True, "default": True}


@pytest.mark.unit
def test_update_document_merges_the_current_document_under_the_node_lock(
    tmp_path: Path,
) -> None:
    """The merge sees the stored bytes while the node lock is held; refusals touch nothing.

    Deterministic: inside the merge a peer stamps an attribute and the merge
    waits until the peer has either committed or is waiting for the node lock
    (two users). Under the lock the peer waits, so its stamp lands after the
    put instead of being reverted by it.
    """
    from firecube.core.zarr.guard import metadata_authority

    _local_root(tmp_path)
    store = LocalStore(tmp_path)
    auth = MetadataAuthority.process_local(_scope(tmp_path, "g"))
    path = tmp_path / "g" / "a" / "zarr.json"
    stored = path.read_bytes()
    lock_key = (auth.scope.store, "g/a")
    seen: list[bytes | None] = []
    peer_done = threading.Event()
    peer = threading.Thread(
        target=lambda: (set_attributes(auth, store, "g/a", {"peer": 1}), peer_done.set())
    )

    def merge(current: bytes | None) -> bytes:
        seen.append(current)
        peer.start()
        for _ in range(1000):
            entry = metadata_authority._NODE_LOCKS._entries.get(lock_key)
            if peer_done.is_set() or (entry is not None and entry.users >= 2):
                break
            threading.Event().wait(0.01)
        assert not peer_done.is_set(), "a peer changed the node while the merge held it"
        document = json.loads(current or b"{}")
        document["attributes"] = {**document.get("attributes", {}), "merged": 1}
        return json.dumps(document).encode()

    assert update_document(auth, store, "g/a/zarr.json", merge) is True
    peer.join(10)

    assert seen == [stored]
    assert dict(_on_disk(tmp_path, "g/a").attrs) == {"merged": 1, "peer": 1}
    after = path.read_bytes()
    assert update_document(auth, store, "g/a/zarr.json", lambda current: None) is False
    assert update_document(auth, store, "g/a/zarr.json", lambda current: current) is False
    assert path.read_bytes() == after

    calls: list[bytes | None] = []
    with pytest.raises(MetadataAuthorityError, match="outside the scope"):
        update_document(auth, store, "h/zarr.json", calls.append)
    with pytest.raises(ValueError, match=r"not a zarr\.json document key"):
        update_document(auth, store, "g/a/c/0", calls.append)
    with pytest.raises(ValueError, match="not a Zarr v3 node document"):
        update_document(auth, store, "g/a/zarr.json", lambda current: b'{"zarr_format": 2}')
    auth.revoke()
    with pytest.raises(MetadataAuthorityError, match="was released"):
        update_document(auth, store, "g/a/zarr.json", calls.append)
    assert calls == []
    assert path.read_bytes() == after


def test_empty_group_document_matches_what_zarr_stores() -> None:
    from zarr.buffer import default_buffer_prototype
    from zarr.storage import MemoryStore

    from firecube.core.zarr._zarr_compat import sync

    store = MemoryStore()
    zarr.create_group(store=store, zarr_format=3)
    stored = sync(store.get("zarr.json", prototype=default_buffer_prototype()))
    assert stored is not None
    assert stored.to_bytes() == EMPTY_GROUP_DOCUMENT
