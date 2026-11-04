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

"""Authority to change ``zarr.json``: create, grow, attributes, markers, copies.

In Zarr v3 the shape, the attributes and the markers of one node are a single
JSON document. Every ``Array.resize`` or attribute write rewrites the whole
document from the handle's in-memory copy, so a handle opened before another
writer's change silently reverts that change (shape and markers alike).

This module is the one place a firecube writer changes a node document:

* a `MetadataAuthority` names the store, the node subtree and the claim that
  entitles its holder to change it. Every operation here requires one and
  refuses, before it touches the store, an authority that is missing or
  released, a node outside its scope and a store other than the one it names;
* every operation reads the node from the store under a per-node lock, never
  through a consolidated index or a caller's handle, so no stale handle is
  ever written. A handle an operation returns is for payload I/O and for
  reading back what the operation wrote;
* the editing operations create, grow, and add or replace attributes; none
  deletes a node, and only the growth step of `create_or_open_array` is
  guaranteed never to make an array smaller.
  `update_document` puts whatever its caller's merge returns, and that
  result is not restricted to growth. A missing parent group is created
  empty, only when absent and only inside the scope: an authority over a
  subtree whose ancestor is missing is refused, so it never writes a node
  its claim does not protect. `publish_document` replaces
  one whole document, for code that copies metadata rather than editing it;
  `publish_document_if_absent` puts one only where none exists, deciding
  under the node lock, so it never replaces a document another writer
  created or changed meanwhile; `update_document` reads one, has the caller
  merge into it and puts the result, all under the node lock. `publish_raw`
  replaces one document, v3 or v2, only when its stored bytes still have the
  digest its caller planned against, deciding under the node lock.

The node locks exclude threads of this process only. Between processes the
claim the authority names is the exclusion, and an authority with basis
``PROCESS_LOCAL`` says out loud that there is none; creation's conditional put
is atomic on some stores and not on others (fsspec checks, then writes), so it
relies on that exclusion too. Releasing an authority waits for the operations
already admitted under it, so no write lands after the claim it names was
released; a release from inside one of its own operations is refused and
happens when that operation ends. This module writes nothing under
``.firecube/``.

Lock order: node locks are leaves. A stored-unit lock may be held while a node
lock is taken, never a node lock while a stored-unit lock is taken. Between
node locks, one may be held while an ancestor's is taken, never the reverse.
"""

from __future__ import annotations

import contextlib
import enum
import json
import sys
import threading
from collections.abc import Callable, Iterator, Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import zarr
from zarr.abc.store import Store
from zarr.buffer import default_buffer_prototype
from zarr.errors import GroupNotFoundError, NodeNotFoundError, NodeTypeValidationError
from zarr.storage import FsspecStore, LocalStore, ObjectStore

from firecube.core.errors import FirecubeError
from firecube.core.zarr._zarr_compat import sync
from firecube.core.zarr.guard._unit_locks import _store_identity, canonical_array_path

if TYPE_CHECKING:
    from firecube.core.zarr.metadata_documents import _Absent

__all__ = [
    "AuthorityBasis",
    "DocumentPreconditionError",
    "HeldClaim",
    "MetadataAuthority",
    "MetadataAuthorityError",
    "MetadataScope",
    "adopt_held_claim",
    "create_or_open_array",
    "ensure_group",
    "hold_authority",
    "publish_document",
    "publish_document_if_absent",
    "publish_raw",
    "set_attributes",
    "update_document",
]

_DOCUMENT_NAME = "zarr.json"
_V2_GROUP_NAME = ".zgroup"
_V2_ARRAY_NAME = ".zarray"
_V2_ATTRS_NAME = ".zattrs"
_V2_CONSOLIDATED_NAME = ".zmetadata"
_V3_NAMES = frozenset({_DOCUMENT_NAME})
# The documents `publish_raw` writes: the v3 node document and the four v2 ones.
_RAW_NAMES = frozenset(
    {_DOCUMENT_NAME, _V2_GROUP_NAME, _V2_ARRAY_NAME, _V2_ATTRS_NAME, _V2_CONSOLIDATED_NAME}
)
# The v3 document zarr stores for an empty group; a test compares it with what
# zarr itself writes, so a zarr change fails there instead of drifting here.
EMPTY_GROUP_DOCUMENT = b'{\n  "attributes": {},\n  "zarr_format": 3,\n  "node_type": "group"\n}'


class MetadataAuthorityError(FirecubeError):
    """A ``zarr.json`` change was attempted without a valid authority.

    Raised when no authority is supplied, when it was released, when the node
    lies outside its scope, or when the store is not the one it names. The
    store is untouched when this is raised.
    """


class DocumentPreconditionError(FirecubeError):
    """A stored document lacks the bytes its writer planned against; the write leaves the store as it is.

    Attributes:
        path: The store key of the document.
        expected: The digest the writer expected, or ``ABSENT`` for no document.
        found: The digest of the stored bytes, or ``ABSENT`` when the key holds none.
    """

    def __init__(self, path: str, *, expected: str | _Absent, found: str | _Absent) -> None:
        """Build the error and its sentence from the key and the two digests.

        Args:
            path: The store key of the document.
            expected: The digest the writer expected, or ``ABSENT``.
            found: The digest of the stored bytes, or ``ABSENT``.
        """
        self.path = path
        self.expected = expected
        self.found = found
        wanted = f"digest {expected}" if isinstance(expected, str) else "no document"
        held = f"digest {found}" if isinstance(found, str) else "none"
        super().__init__(
            f"refusing to write {path!r}: expected {wanted}, the store holds {held}; "
            "the document changed since the write was planned"
        )

    def __reduce__(self) -> tuple[Any, ...]:
        return (_restore_precondition_error, (type(self), self.path, self.expected, self.found))


def _restore_precondition_error(
    cls: type[DocumentPreconditionError], path: str, expected: str | _Absent, found: str | _Absent
) -> DocumentPreconditionError:
    """Rebuild a pickled or copied `DocumentPreconditionError` from its fields."""
    return cls(path, expected=expected, found=found)


class AuthorityBasis(enum.Enum):
    """What entitles the holder of an authority to change metadata.

    Attributes:
        CLAIM: A control-plane claim excludes other processes.
        PROCESS_LOCAL: Only the threads of this process are excluded.
    """

    CLAIM = "claim"
    PROCESS_LOCAL = "process_local"


@dataclass(frozen=True, slots=True)
class MetadataScope:
    """The nodes an authority covers: one store, one subtree.

    Attributes:
        store: The store identity, spelled as the stored-unit locks spell it
            (a local directory is its resolved real path).
        prefix: A node path; ``""`` covers the whole store. Stored in zarr's
            canonical spelling (no leading, trailing or repeated slashes).
        backing: The store object the authority is for. A local directory is
            recognised by its path; any other store is accepted only when it
            is this object, or (fsspec, obstore) equal to it by zarr's store
            equality with the same root path. Without it, only a local
            directory can be verified and every other store is refused.

    Raises:
        ValueError: ``prefix`` has a ``.`` or ``..`` segment.
    """

    store: str
    prefix: str = ""
    backing: Store | None = field(default=None, compare=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "prefix", canonical_array_path(self.prefix))

    def covers(self, store: str, path: str) -> bool:
        """Whether the node at *path* of *store* lies in this scope."""
        if store != self.store:
            return False
        node = canonical_array_path(path)
        return not self.prefix or node == self.prefix or node.startswith(self.prefix + "/")


@dataclass(frozen=True, slots=True)
class HeldClaim:
    """Names a claim somebody holds: what a claim scope hands to code inside it.

    Attributes:
        domain_id: The claim's domain identifier (``product:category:name``).
        owner_id: The owner the claim was acquired for.
    """

    domain_id: str
    owner_id: str


class MetadataAuthority:
    """Proof that its holder may change the node documents under ``scope``.

    Built by `hold_authority`, `adopt_held_claim` or `process_local`. It turns
    inactive when the scope that built it exits, and the operations of this
    module then refuse it. Releasing it waits until the operations already
    admitted under it have finished. A release from inside one of its own
    operations raises `MetadataAuthorityError` before anything changes; the
    authority, and the claim `hold_authority` entered, are released when that
    operation ends.

    Attributes:
        scope: The store and subtree it covers.
        basis: What entitles the holder: a claim, or this process only.
        claim_domain: The claim's domain identifier; ``None`` when
            ``basis`` is ``PROCESS_LOCAL``.
        owner_id: The claim's owner; ``None`` when there is no claim.
    """

    __slots__ = (
        "_active",
        "_deferred",
        "_in_flight",
        "_state",
        "basis",
        "claim_domain",
        "owner_id",
        "scope",
    )

    def __init__(
        self,
        scope: MetadataScope,
        basis: AuthorityBasis,
        claim_domain: str | None,
        owner_id: str | None,
    ) -> None:
        self.scope = scope
        self.basis = basis
        self.claim_domain = claim_domain
        self.owner_id = owner_id
        self._active = True
        self._state = threading.Condition()
        self._in_flight: dict[int, int] = {}  # thread ident -> admitted operations
        # Releases refused inside an operation: thread ident -> what to run after
        # it, run by that thread's own release when its operation ends.
        self._deferred: dict[int, Callable[[], object] | None] = {}

    @classmethod
    def process_local(cls, scope: MetadataScope) -> MetadataAuthority:
        """An authority that excludes the threads of this process and nothing else."""
        return cls(scope, AuthorityBasis.PROCESS_LOCAL, None, None)

    @property
    def active(self) -> bool:
        """``False`` once the authority was released."""
        return self._active

    def revoke(self) -> None:
        """Release the authority once the operations admitted under it have finished.

        Raises:
            MetadataAuthorityError: Called from inside one of its own
                operations; it is then released when that operation ends.
        """
        self._release(None)

    def _release(self, then: Callable[[], object] | None) -> None:
        """Deactivate, wait for admitted operations, then run *then* (a claim's exit).

        A release refused inside an operation is queued for the thread that
        asked, and that thread runs it, exit included, when its operation
        ends: a claim exit can be bound to the thread that entered the claim.
        A thread's later refused release never drops an exit it already
        queued: both run, the first queued first.
        """
        me = threading.get_ident()
        with self._state:
            if me in self._in_flight:
                self._deferred[me] = _chain(self._deferred.get(me), then)
                raise MetadataAuthorityError(
                    f"{self!r} cannot be released from inside one of its own operations; "
                    "it is released when that operation ends"
                )
            self._active = False
            self._state.wait_for(lambda: not self._in_flight)
        if then is not None:
            then()

    @contextlib.contextmanager
    def _admitted(self, node: str) -> Iterator[None]:
        """Admit one operation on *node*; refused once released."""
        me = threading.get_ident()
        with self._state:
            if not self._active:
                raise MetadataAuthorityError(
                    f"refusing to change the metadata of {node!r}: {self!r} was released"
                )
            self._in_flight[me] = self._in_flight.get(me, 0) + 1
        try:
            yield
        finally:
            pending, then = False, None
            with self._state:
                remaining = self._in_flight[me] - 1
                if remaining:
                    self._in_flight[me] = remaining
                else:
                    del self._in_flight[me]
                    pending = me in self._deferred
                    then = self._deferred.pop(me, None)
                self._state.notify_all()
            if pending:
                self._release(then)

    def __repr__(self) -> str:
        return (
            f"MetadataAuthority(store={self.scope.store!r}, prefix={self.scope.prefix!r}, "
            f"basis={self.basis.value}, claim={self.claim_domain!r}, active={self._active})"
        )


def _chain(
    first: Callable[[], object] | None, then: Callable[[], object] | None
) -> Callable[[], object] | None:
    """One thread's queued exits: *first*, then *then*; a missing one is skipped."""
    if first is None:
        return then
    if then is None:
        return first

    def both() -> None:
        try:
            first()
        finally:
            then()

    return both


@contextlib.contextmanager
def hold_authority(
    scope: MetadataScope,
    *,
    claim: AbstractContextManager[Any] | None,
    claim_domain: str | None = None,
    owner_id: str | None = None,
) -> Iterator[MetadataAuthority]:
    """Enter *claim* and yield an authority that lasts exactly as long.

    The authority is released before the claim exits, after the operations
    admitted under it have finished. Leaving the block from inside one of the
    authority's own operations raises `MetadataAuthorityError`; authority and
    claim are then released when that operation ends.

    Args:
        scope: The store and subtree the authority covers.
        claim: The claim to enter, e.g. ``ChunkManager.acquire_claim(...)``.
            ``None`` (no control plane: library use, unit tests) yields a
            ``PROCESS_LOCAL`` authority; the caller states that choice by
            passing it.
        claim_domain: The claim's domain identifier; required with a claim.
        owner_id: The owner the claim was acquired for.

    Yields:
        The authority, active until the block exits.

    Raises:
        MetadataAuthorityError: A claim was passed without its domain identifier.
    """
    if claim is None:
        authority = MetadataAuthority.process_local(scope)
        try:
            yield authority
        finally:
            authority.revoke()
        return
    if not claim_domain:
        raise MetadataAuthorityError(
            "hold_authority(claim=...) needs the claim's domain identifier"
        )
    claim.__enter__()
    authority = MetadataAuthority(scope, AuthorityBasis.CLAIM, claim_domain, owner_id)
    try:
        yield authority
    finally:
        # The claim exits after the authority is released: now, or when the
        # operation this release was refused inside ends.
        exc_info = sys.exc_info()
        authority._release(lambda: claim.__exit__(*exc_info))


@contextlib.contextmanager
def adopt_held_claim(scope: MetadataScope, held: HeldClaim) -> Iterator[MetadataAuthority]:
    """Yield an authority for a claim the caller already holds and keeps holding.

    For claims shared across batches (a run's schema claim) or acquired by
    another component. The authority is released on exit; the claim is not
    touched, and that it is really held is the caller's word.
    """
    authority = MetadataAuthority(scope, AuthorityBasis.CLAIM, held.domain_id, held.owner_id)
    try:
        yield authority
    finally:
        authority.revoke()


# --- per-node lock table (threads of this process) ----------------------------------

NodeLockKey = tuple[str, str]
"""``(store identity, canonical node path)``; ``""`` is the root group."""


@dataclass
class _NodeLockEntry:
    """One node's re-entrant mutex and the number of threads holding or waiting for it."""

    lock: threading.RLock = field(default_factory=threading.RLock)
    users: int = 0


class _NodeLocks:
    """Process-wide re-entrant locks per node document; an entry is dropped when idle.

    Separate from the stored-unit lock table: node locks are leaves in the lock
    order, and a node key is never sorted against a stored-unit key.
    """

    def __init__(self) -> None:
        self._guard = threading.Lock()
        self._entries: dict[NodeLockKey, _NodeLockEntry] = {}

    @contextlib.contextmanager
    def hold(self, key: NodeLockKey) -> Iterator[None]:
        with self._guard:
            entry = self._entries.setdefault(key, _NodeLockEntry())
            entry.users += 1
        try:
            with entry.lock:
                yield
        finally:
            with self._guard:
                entry.users -= 1
                if entry.users == 0:
                    del self._entries[key]


_NODE_LOCKS = _NodeLocks()


# --- checks and fresh reads -----------------------------------------------------------


def _location(root: zarr.Group | Store) -> tuple[Store, str]:
    """The store behind *root* and the node path *root* stands for."""
    if isinstance(root, Store):
        return root, ""
    if isinstance(root, zarr.Group):
        return root.store, canonical_array_path(root.path)
    raise TypeError(f"expected a zarr Group or Store, got {type(root).__name__}")


def _node_path(base: str, path: str) -> str:
    return canonical_array_path(f"{base}/{path}")


@contextlib.contextmanager
def _authorized(
    authority: MetadataAuthority | None, store: Store, node: str
) -> Iterator[MetadataAuthority]:
    """Admit one operation on *node* of *store*, or refuse it before any store access.

    A local directory is compared by its resolved path with the scope's
    spelling; any other store must be the scope's ``backing`` store (see
    `MetadataScope`), and is refused when the scope has none.
    """
    if not isinstance(authority, MetadataAuthority):
        raise MetadataAuthorityError(
            f"refusing to change the metadata of {node!r}: no MetadataAuthority was supplied"
        )
    if isinstance(store, LocalStore):
        identity = _store_identity("", store)
        if identity != authority.scope.store:
            raise MetadataAuthorityError(
                f"refusing to change the metadata of {node!r}: store {identity!r} is not the "
                f"store {authority.scope.store!r} the authority names"
            )
    elif authority.scope.backing is None:
        raise MetadataAuthorityError(
            f"refusing to change the metadata of {node!r}: cannot verify that "
            f"{type(store).__name__} is the store {authority.scope.store!r} the authority "
            "names (its scope binds no backing store)"
        )
    elif not _same_backing(authority.scope.backing, store):
        raise MetadataAuthorityError(
            f"refusing to change the metadata of {node!r}: {type(store).__name__} is not the "
            f"store {authority.scope.store!r} the authority names"
        )
    if not authority.scope.covers(authority.scope.store, node):
        raise MetadataAuthorityError(
            f"refusing to change the metadata of {node!r}: outside the scope of {authority!r}"
        )
    with authority._admitted(node):
        yield authority


def _same_backing(bound: Store, store: Store) -> bool:
    """*store* is *bound*, or an fsspec or obstore store equal to it at the same root."""
    if store is bound:
        return True
    if isinstance(store, FsspecStore) and isinstance(bound, FsspecStore):
        return store == bound and store.path.rstrip("/") == bound.path.rstrip("/")
    if isinstance(store, ObjectStore) and isinstance(bound, ObjectStore):
        return store == bound and getattr(store.store, "prefix", None) == getattr(
            bound.store, "prefix", None
        )
    return False


def _node_lock(authority: MetadataAuthority, node: str) -> AbstractContextManager[None]:
    return _NODE_LOCKS.hold((authority.scope.store, node))


def _open_array_fresh(store: Store, node: str) -> zarr.Array | None:
    """The array at *node* read from the store; ``None`` when absent or a group."""
    try:
        return zarr.open_array(store=store, path=node, mode="r+", zarr_format=3)
    except (NodeNotFoundError, NodeTypeValidationError):
        return None


def _open_group_fresh(store: Store, node: str) -> zarr.Group:
    return zarr.open_group(store=store, path=node, mode="r+", zarr_format=3, use_consolidated=False)


def _open_node_fresh(store: Store, node: str) -> zarr.Array | zarr.Group:
    array = _open_array_fresh(store, node)
    return array if array is not None else _open_group_fresh(store, node)


def _document_key(node: str) -> str:
    return f"{node}/{_DOCUMENT_NAME}" if node else _DOCUMENT_NAME


def _require_group(authority: MetadataAuthority, store: Store, node: str) -> zarr.Group:
    """The group at *node*, creating it and any missing parent as an empty group.

    Fast path: one read, under the node's lock, when the group exists.
    Otherwise each level, root first, is read under its own node lock and
    created only when absent (``set_if_not_exists``), so an existing document
    is never rewritten. A missing level outside the authority's scope is
    refused; levels are visited root first and the scope is a subtree, so
    that happens before anything is written.

    Raises:
        MetadataAuthorityError: A missing level lies outside the scope.
        zarr.errors.ContainsArrayError: An array stands at *node* or a parent.
    """
    with _node_lock(authority, node), contextlib.suppress(GroupNotFoundError):
        return _open_group_fresh(store, node)
    parts = node.split("/") if node else []
    empty_group = default_buffer_prototype().buffer.from_bytes(EMPTY_GROUP_DOCUMENT)
    group: zarr.Group | None = None
    for depth in range(len(parts) + 1):
        level = "/".join(parts[:depth])
        with _node_lock(authority, level):
            try:
                group = _open_group_fresh(store, level)
            except GroupNotFoundError:
                if not authority.scope.covers(authority.scope.store, level):
                    raise MetadataAuthorityError(
                        f"refusing to create the missing group {level!r}: outside the scope "
                        f"of {authority!r}"
                    ) from None
                sync(store.set_if_not_exists(_document_key(level), empty_group))
                group = _open_group_fresh(store, level)
    assert group is not None
    return group


# --- operations -------------------------------------------------------------------------


def ensure_group(
    authority: MetadataAuthority | None, root: zarr.Group | Store, path: str
) -> zarr.Group:
    """Return the group at *path* below *root*, creating it and missing parents.

    *root* may be a bare store, so a missing root group can be created under
    an authority (``path=""``).

    Raises:
        MetadataAuthorityError: The authority is missing, released, out of
            scope, or names another store.
        zarr.errors.ContainsArrayError: An array stands at *path* or a parent.
    """
    store, base = _location(root)
    node = _node_path(base, path)
    with _authorized(authority, store, node) as auth:
        return _require_group(auth, store, node)


def create_or_open_array(
    authority: MetadataAuthority | None,
    root: zarr.Group | Store,
    path: str,
    *,
    create_kwargs: Mapping[str, Any],
    grow_to: tuple[int, ...] | None = None,
    default_attrs: Mapping[str, Any] | None = None,
    validate: Callable[[zarr.Array], None] | None = None,
) -> tuple[zarr.Array, bool]:
    """Create the array at *path* or open it from the store; grow it; add absent attributes.

    A new array is created with ``default_attrs`` merged under its
    ``attributes`` (an explicit attribute wins) in the one metadata write that
    creates it, so no crash leaves it without them; missing parent groups are
    created first. For an existing array, in this order: when ``grow_to`` has
    the stored rank and exceeds the stored shape on an axis, the shape becomes
    the elementwise maximum (never smaller); ``validate`` runs on it; the keys
    of ``default_attrs`` it lacks are added in one write. ``validate`` runs
    under the node lock: it must not take a stored-unit lock or change a
    descendant node.

    Args:
        authority: The authority to change the array under.
        root: A group or a store the path is relative to.
        path: The array's path below *root*.
        create_kwargs: ``Group.create_array`` keyword arguments other than
            ``name`` (taken from *path*) and ``overwrite`` (nothing here
            replaces an array).
        grow_to: The shape an existing array must at least have.
        default_attrs: Attributes the array must carry, written when absent.
        validate: Check of an existing array, e.g. a schema drift check.

    Returns:
        ``(array, created)``; the handle is freshly read, for payload I/O.

    Raises:
        MetadataAuthorityError: The authority is missing, released, out of
            scope, or names another store.
        ValueError: *path* is the root, or ``create_kwargs`` carries ``name``
            or ``overwrite``.
    """
    store, base = _location(root)
    node = _node_path(base, path)
    if not node:
        raise ValueError("an array needs a path below the root group")
    refused = sorted({"name", "overwrite"} & create_kwargs.keys())
    if refused:
        raise ValueError(f"create_kwargs must not carry {refused}: the path names the array")
    parent, _, name = node.rpartition("/")
    defaults = dict(default_attrs or {})
    with _authorized(authority, store, node) as auth, _node_lock(auth, node):
        arr = _open_array_fresh(store, node)
        if arr is None:
            kwargs = dict(create_kwargs)
            attributes = {**defaults, **dict(kwargs.get("attributes") or {})}
            if attributes:
                kwargs["attributes"] = attributes
            group = _require_group(auth, store, parent)
            return group.create_array(name=name, **kwargs), True
        if (
            grow_to is not None
            and len(arr.shape) == len(grow_to)
            and any(cur < req for cur, req in zip(arr.shape, grow_to, strict=True))
        ):
            arr.resize(tuple(max(cur, req) for cur, req in zip(arr.shape, grow_to, strict=True)))
        if validate is not None:
            validate(arr)
            fresh = _open_array_fresh(store, node)  # keep what validate wrote
            if fresh is not None:
                arr = fresh
        missing = {k: v for k, v in defaults.items() if k not in arr.attrs}
        if missing:
            arr = arr.update_attributes(missing)
        return arr, False


def set_attributes(
    authority: MetadataAuthority | None,
    root: zarr.Group | Store,
    path: str,
    attrs: Mapping[str, Any],
) -> zarr.Array | zarr.Group:
    """Add or replace the given attributes of the node at *path* (array or group), one write.

    Returns:
        The node as written; read attributes back from it, not from an older handle.

    Raises:
        MetadataAuthorityError: The authority is missing, released, out of
            scope, or names another store.
        zarr.errors.GroupNotFoundError: No node at *path*.
    """
    store, base = _location(root)
    node = _node_path(base, path)
    with _authorized(authority, store, node) as auth, _node_lock(auth, node):
        current = _open_node_fresh(store, node)
        return current.update_attributes(dict(attrs)) if attrs else current


def _document_node(key: str, *, names: frozenset[str] = _V3_NAMES) -> tuple[str, str]:
    """The canonical key and node path of a document key; ValueError when its name is not in *names*.

    The default admits ``zarr.json`` only; `publish_raw` passes the v2 names too.
    """
    canonical = canonical_array_path(key)
    node, _, name = canonical.rpartition("/")
    if name not in names:
        if names == _V3_NAMES:
            raise ValueError(f"{key!r} is not a {_DOCUMENT_NAME} document key")
        raise ValueError(f"{key!r} is not a metadata document key ({', '.join(sorted(names))})")
    return canonical, node


def _is_v3_node(document: object) -> bool:
    return (
        isinstance(document, dict)
        and document.get("zarr_format") == 3
        and document.get("node_type") in ("array", "group")
    )


def _is_v2_group(document: object) -> bool:
    return (
        isinstance(document, dict) and document.get("zarr_format") == 2 and "shape" not in document
    )


def _is_v2_array(document: object) -> bool:
    return (
        isinstance(document, dict)
        and document.get("zarr_format") == 2
        and all(key in document for key in ("shape", "chunks", "dtype"))
    )


def _is_v2_attrs(document: object) -> bool:
    # Any JSON object is a valid attribute set; a v3 node document is refused
    # because it is a node document put under the wrong name.
    return isinstance(document, dict) and not _is_v3_node(document)


def _is_v2_consolidated(document: object) -> bool:
    return (
        isinstance(document, dict)
        and document.get("zarr_consolidated_format") == 1
        and isinstance(document.get("metadata"), dict)
    )


# Per document name: the payload check and what the refusal calls the payload.
_PAYLOAD_CHECKS: Mapping[str, tuple[Callable[[object], bool], str]] = {
    _DOCUMENT_NAME: (_is_v3_node, "Zarr v3 node document"),
    _V2_GROUP_NAME: (_is_v2_group, "Zarr v2 group document"),
    _V2_ARRAY_NAME: (_is_v2_array, "Zarr v2 array document"),
    _V2_ATTRS_NAME: (_is_v2_attrs, "Zarr v2 attributes document"),
    _V2_CONSOLIDATED_NAME: (_is_v2_consolidated, "Zarr v2 consolidated metadata document"),
}


def _checked_document(
    key: str, payload: bytes, *, names: frozenset[str] = _V3_NAMES
) -> tuple[str, str]:
    """The canonical key and node path of a document put; ValueError when key or payload is wrong.

    A ``zarr.json`` payload must be a v3 node document. A ``.zgroup`` or
    ``.zarray`` payload must carry ``zarr_format`` 2 and, for an array,
    ``shape``, ``chunks`` and ``dtype``. A ``.zattrs`` payload is a JSON object
    that is not a v3 node document. A ``.zmetadata`` payload carries
    ``zarr_consolidated_format`` 1 and a ``metadata`` object.
    """
    canonical, node = _document_node(key, names=names)
    try:
        document = json.loads(payload)
    except ValueError as exc:
        raise ValueError(f"payload for {canonical!r} is not JSON: {exc}") from exc
    check, description = _PAYLOAD_CHECKS[canonical.rpartition("/")[2]]
    if not check(document):
        raise ValueError(f"payload for {canonical!r} is not a {description}")
    return canonical, node


def publish_document(
    authority: MetadataAuthority | None, store: Store, key: str, payload: bytes
) -> None:
    """Put one whole ``zarr.json`` document, for code that copies metadata.

    The document replaces whatever the key held, in one put through the store
    (atomic as the store's own writes are: a local store writes a temporary
    file and renames it). Use it to publish documents copied from elsewhere;
    to edit a node, use the other operations, which read it first.

    Args:
        authority: The authority over the node the document describes.
        store: The store to put into.
        key: ``zarr.json`` or ``<node path>/zarr.json``.
        payload: A Zarr v3 node document (JSON with ``zarr_format`` 3 and a
            ``node_type`` of ``array`` or ``group``).

    Raises:
        MetadataAuthorityError: The authority is missing, released, out of
            scope, or names another store.
        ValueError: *key* names no ``zarr.json`` document, or *payload* is
            not a Zarr v3 node document.
    """
    canonical, node = _checked_document(key, payload)
    with _authorized(authority, store, node) as auth, _node_lock(auth, node):
        sync(store.set(canonical, default_buffer_prototype().buffer.from_bytes(payload)))


def publish_document_if_absent(
    authority: MetadataAuthority | None, store: Store, key: str, payload: bytes
) -> bool:
    """Put one whole ``zarr.json`` document where the key holds none yet.

    Checked as `publish_document` is. Whether the key exists is read under
    the node's lock, in the same hold as the put, so a document that another
    writer creates or changes under that lock is never replaced by a copy
    decided on before it. Between processes, as everywhere here, the claim
    the authority names is the exclusion.

    Returns:
        ``True`` when the document was written; ``False`` when one already
        existed, which is left as it was.

    Raises:
        MetadataAuthorityError: The authority is missing, released, out of
            scope, or names another store.
        ValueError: *key* names no ``zarr.json`` document, or *payload* is
            not a Zarr v3 node document.
    """
    canonical, node = _checked_document(key, payload)
    with _authorized(authority, store, node) as auth, _node_lock(auth, node):
        if sync(store.exists(canonical)):
            return False
        buffer = default_buffer_prototype().buffer.from_bytes(payload)
        sync(store.set_if_not_exists(canonical, buffer))
        return True


def update_document(
    authority: MetadataAuthority | None,
    store: Store,
    key: str,
    merge: Callable[[bytes | None], bytes | None],
) -> bool:
    """Read one ``zarr.json``, merge into it and put the result, all under the node's lock.

    Checked as `publish_document` is, before the store is touched. Under the
    node's lock the current document is read (``None`` when the key holds
    none) and handed to *merge*, which returns the document to put, or
    ``None`` to leave the key as it is. *merge* must not do I/O or take
    locks: it runs while the lock is held. Nothing is written when it returns
    ``None`` or the bytes already stored. So a writer that changes the node
    under its lock meanwhile (grows it, stamps an attribute) is either seen
    by *merge* or changes the node after the put, never reverted by it.

    Returns:
        ``True`` when a document was written.

    Raises:
        MetadataAuthorityError: The authority is missing, released, out of
            scope, or names another store.
        ValueError: *key* names no ``zarr.json`` document, or the merged
            document is not a Zarr v3 node document (nothing is written).
        Exception: Whatever *merge* raises; nothing is written.
    """
    canonical, node = _document_node(key)
    with _authorized(authority, store, node) as auth, _node_lock(auth, node):
        stored = sync(store.get(canonical, prototype=default_buffer_prototype()))
        current = None if stored is None else stored.to_bytes()
        payload = merge(current)
        if payload is None or payload == current:
            return False
        _checked_document(canonical, payload)
        sync(store.set(canonical, default_buffer_prototype().buffer.from_bytes(payload)))
        return True


def publish_raw(
    authority: MetadataAuthority | None,
    store: Store,
    path: str,
    data: bytes,
    *,
    expect: str | _Absent,
) -> None:
    """Replace one v3 or v2 metadata document when the store still holds the expected bytes.

    It checks the key and *data*, then the authority, before it touches the
    store. Under the node's lock it reads the stored bytes and compares their
    `metadata_document_digest` with *expect*. It puts *data* in the same lock hold, in
    one put, as `publish_document` does. Under ``ABSENT`` it creates the key
    only where none exists. Between processes, the claim the authority names
    is the exclusion.

    Args:
        authority: The authority over the node the document belongs to.
        store: The store to put into.
        path: ``zarr.json``, ``.zattrs``, ``.zgroup``, ``.zarray`` or
            ``.zmetadata``, at the root or below ``<node path>/``.
        data: The whole document. A ``zarr.json`` must be a v3 node document;
            a ``.zgroup`` or ``.zarray`` must carry ``zarr_format`` 2 (an array
            also ``shape``, ``chunks`` and ``dtype``); a ``.zattrs`` must be a
            JSON object that is not a v3 node document; a ``.zmetadata`` must
            carry ``zarr_consolidated_format`` 1 and a ``metadata`` object.
        expect: The ``sha256:<hex>`` digest the stored bytes must have, or
            ``ABSENT`` when the key must hold no document.

    Raises:
        DocumentPreconditionError: The stored bytes do not have the expected
            digest, or exist under ``ABSENT``. Nothing is written.
        MetadataAuthorityError: The authority is missing, released, out of
            scope, or names another store.
        ValueError: *path* names no metadata document, or *data* does not
            fit its name.
        TypeError: *expect* is neither a string nor ``ABSENT``.
    """
    # Imported here: metadata_documents.py loads the control-plane package, which imports this module.
    from firecube.core.zarr.metadata_documents import ABSENT, metadata_document_digest

    if expect is not ABSENT and not isinstance(expect, str):
        raise TypeError(f"expect must be a digest string or ABSENT, got {type(expect).__name__}")
    canonical, node = _checked_document(path, data, names=_RAW_NAMES)
    with _authorized(authority, store, node) as auth, _node_lock(auth, node):
        stored = sync(store.get(canonical, prototype=default_buffer_prototype()))
        found = ABSENT if stored is None else metadata_document_digest(stored.to_bytes())
        if found != expect:
            raise DocumentPreconditionError(canonical, expected=expect, found=found)
        buffer = default_buffer_prototype().buffer.from_bytes(data)
        if expect is ABSENT:
            sync(store.set_if_not_exists(canonical, buffer))
        else:
            sync(store.set(canonical, buffer))
