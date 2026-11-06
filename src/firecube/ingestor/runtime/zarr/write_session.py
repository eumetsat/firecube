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

"""One run's guarded-write state for a Zarr template.

`ZarrWriteSession` is built on the main thread before any worker and holds
what used to hang off the ingestor instance: the schema claims shared by a
run's concurrent batches, the staged-write ledger and the engine-owned check
flag. Its lifecycle is prepared -> writing -> publishing -> terminal; a call
in the wrong state raises.

Every write runs inside ``with session.admit_write():``, which counts the
batch as in flight. `verify_promotion` and `close` first refuse new
admissions, then wait (up to ``drain_timeout`` seconds, else
`WriteSessionStateError` naming the stuck count) for the in-flight batches to
leave, so no write can begin or continue after coverage verification or
terminal cleanup. A `close` whose drain timed out keeps failing, for every
later caller, while that work is still in flight. The adapter that replaces the ingestor's own bookkeeping
wraps each batch in this context manager. Neither may be called from inside an
admitted block of the same thread (it would wait for itself).

After `close` every operation raises: claims, recording, ledger reads,
`final_root` and the engine-owned accessors. Each of those calls registers
itself as in flight for its whole duration, like a batch, so `close` waits for
it and cannot complete between its open-check and its effect. One limit: a
claim returned by `schema_claim` or `slot_claim` is held beyond the call, and
releasing it after `close` is the caller's cleanup, not an operation.

`admit_write`, `schema_claim` and `verify_promotion` are the session's seams
to the guarded write, the metadata authority and the promotion plan. Today
`admit_write` takes no unit lock (the region writer still does), the scope of
`schema_claim` yields the `HeldClaim` a metadata authority is adopted from,
and `verify_promotion` runs the staged-coverage check. `record_write_result`
is the sink the region writer reports each payload write to.

A staged session built with its workspace URI also holds the authority the
workspace's seeded metadata is written under (`staged_seeding_authority`): a
``PROCESS_LOCAL`` authority over the whole workspace store, which belongs to
this run's process, so no claim stands behind it. It is released when
promotion is verified or the session closes, after the seeding writes already
admitted under it have finished; seeding refuses from then on.

A staged session built with its final target as well holds, from verified
promotion until `close`, a ``PROCESS_LOCAL`` authority over the whole target
store: a staged run holds no cross-process claim at promotion, so only this
process's threads are excluded. `staged_promotion_units` hands promotion the
units the run proved written and the absent ones, as object keys, with a
``zarr.json`` publisher bound to that authority, the arrays whose
uncovered data objects promotion refuses, and the managed time coordinates
whose seeded chunks promotion leaves alone. The publisher never drops an
attribute the target array already has: markers such as engine ownership live
only in the target, and the run's workspace copy may be a shell without them.
"""

from __future__ import annotations

import contextlib
import enum
import functools
import json
import threading
from collections.abc import Callable, Collection, Iterable, Iterator, Sequence
from typing import Any

from zarr.buffer import default_buffer_prototype

from firecube.core.controlplane.claims import ClaimLossLatch
from firecube.core.controlplane.types import WriteDomain
from firecube.core.errors import ConfigurationError, StorageError
from firecube.core.storage.completion import StagedPromotionUnits
from firecube.core.uris import is_remote_target, local_path_from_target
from firecube.core.zarr._coord_lifecycle import (
    CoordLifecycleState,
    raise_if_invalid,
    resolve_coord_lifecycle,
)
from firecube.core.zarr._zarr_compat import sync
from firecube.core.zarr.guard._unit_locks import canonical_array_path
from firecube.core.zarr.guard.guarded_write import Unit, WriteResult, unit_store_key
from firecube.core.zarr.guard.metadata_authority import (
    HeldClaim,
    MetadataAuthority,
    update_document,
)
from firecube.core.zarr.guard.unit_ownership import (
    StagedSlotWrite,
    require_staged_units_fully_written,
    require_target_within_clipped_units,
    staged_unit_object_keys,
)
from firecube.core.zarr.region_writer import RegionZarrWriter


def _target_array_document(store: Any, path: str) -> dict[str, Any] | None:
    """The target's ``<path>/zarr.json`` as a dict; ``None`` when it holds no array document."""
    stored = sync(store.get(f"{path}/zarr.json", prototype=default_buffer_prototype()))
    if stored is None:
        return None
    document = json.loads(stored.to_bytes())
    if document.get("node_type") != "array":
        return None
    return document


def _target_array_shape(store: Any, path: str) -> tuple[int, ...] | None:
    """Shape in the target's ``<path>/zarr.json``; ``None`` when it holds no array document."""
    document = _target_array_document(store, path)
    if document is None:
        return None
    return tuple(int(n) for n in document["shape"])


def _workspace_holds(workspace: str, path: str) -> bool:
    """Whether the local workspace holds a node at *path*; a stat, no listing or read.

    A remote workspace answers ``True``, so the session still classifies its coordinate.
    """
    if is_remote_target(workspace):
        return True
    return (local_path_from_target(workspace) / path).is_dir()


class SessionState(enum.Enum):
    """Lifecycle of a write session."""

    PREPARED = "prepared"
    WRITING = "writing"
    PUBLISHING = "publishing"
    TERMINAL = "terminal"


class WriteSessionStateError(RuntimeError):
    """A session operation was attempted in a state that refuses it."""


def _publish_promoted_document(
    authority: MetadataAuthority, store: Any, key: str, payload: bytes
) -> bool:
    """Publish one promoted ``zarr.json`` under *authority*, reconciled under the node lock.

    The target's current document is read, merged with *payload* by
    `_merge_promoted_document` and put in one `update_document` operation,
    so a writer of this process that grows the array or stamps an attribute
    meanwhile is never reverted. Returns ``False`` when the merge left the
    target's document as it was and nothing was written.
    """
    return update_document(
        authority, store, key, functools.partial(_merge_promoted_document, key, payload)
    )


def _merge_promoted_document(key: str, payload: bytes, current: bytes | None) -> bytes | None:
    """The document to store at *key*: the promoted *payload* over the target's *current* one.

    Pure, no I/O. With no current document, or when either is not an array,
    *payload* as is. For two arrays: the larger axis-0 extent (promotion never
    shrinks an array), the target's attributes plus the keys only *payload*
    has (target markers such as engine ownership live only there), the rest
    from *payload*. ``None`` when that equals the current document, so an
    unchanged document is not rewritten.

    Raises:
        StorageError: The current array is longer on axis 0 and differs in
            anything but ``shape[0]`` and ``attributes``; merging would be a
            guess.
    """
    if current is None:
        return payload
    document, target = json.loads(payload), json.loads(current)
    if document.get("node_type") != "array" or target.get("node_type") != "array":
        return payload
    shape, target_shape = list(document["shape"]), list(target["shape"])
    if shape and target_shape and target_shape[0] > shape[0]:
        ignored = ("shape", "attributes")
        rest = {k: v for k, v in document.items() if k not in ignored}
        if (
            len(target_shape) != len(shape)
            or target_shape[1:] != shape[1:]
            or rest != {k: v for k, v in target.items() if k not in ignored}
        ):
            raise StorageError(
                f"promotion refused for {key}: the target array is longer on axis 0 "
                f"({target_shape} over {shape}) and differs in structure"
            )
        shape[0] = target_shape[0]
    attributes = {**(document.get("attributes") or {}), **(target.get("attributes") or {})}
    merged = {**document, "shape": shape, "attributes": attributes}
    return None if merged == target else json.dumps(merged).encode()


class RunSchemaClaims:
    """Schema claims of one run, shared by its concurrent batches.

    The ``zarr_region:<group>:schema`` claim is exclusive and not re-entrant,
    so ``pipeline_workers`` batches of one run used to fail each other on it.
    The first batch acquires it; batches that arrive while it is held share
    it (refcount) and it is released when the last one leaves. Another run
    still meets the claim and conflicts as before. Schema setup itself runs
    one batch at a time under a lock taken on ``with`` entry.
    """

    def __init__(self) -> None:
        self._guard = threading.Lock()
        self._held: dict[tuple[str, str], list[Any]] = {}

    def acquire(
        self, key: tuple[str, str], acquire_claim: Callable[[], Any], held: HeldClaim
    ) -> contextlib.AbstractContextManager[HeldClaim]:
        """Take or share the claim now (like the claim itself); release on exit.

        The scope hands *held*, the claim's name, to the code inside it.
        """
        with self._guard:
            entry = self._held.get(key)
            if entry is None:
                handle = acquire_claim()  # a conflict raises; nothing is recorded
                handle.__enter__()
                entry = self._held[key] = [handle, 0, threading.Lock()]
            entry[1] += 1
        return self._use(key, entry[2], held)

    @contextlib.contextmanager
    def _use(
        self, key: tuple[str, str], setup_lock: threading.Lock, held: HeldClaim
    ) -> Iterator[HeldClaim]:
        try:
            with setup_lock:
                yield held
        finally:
            self._release(key)

    def _release(self, key: tuple[str, str]) -> None:
        # The claim is released under the guard: a batch arriving meanwhile
        # waits, then acquires afresh, instead of meeting its own run's claim.
        with self._guard:
            entry = self._held[key]
            entry[1] -= 1
            if entry[1] > 0:
                return
            try:
                entry[0].__exit__(None, None, None)
            finally:
                del self._held[key]


class StagedWriteLedger:
    """Slot writes of one staged run: touched before dispatch, written after success.

    Promotion copies whole workspace objects over the target, so before it
    every touched unit must have been written in every cell. ``arrays``
    names the arrays whose every promoted data object a ledger entry must cover.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.store_uri: str | None = None
        self.touched: list[StagedSlotWrite] = []
        self.written: list[StagedSlotWrite] = []
        self.arrays: set[str] = set()

    def record(
        self,
        store_uri: str,
        writes: Sequence[StagedSlotWrite],
        *,
        written: bool,
        arrays: Iterable[str] = (),
    ) -> None:
        """Add *writes* and the ledger *arrays*; ``written`` selects the list."""
        with self._lock:
            self.store_uri = store_uri
            (self.written if written else self.touched).extend(writes)
            self.arrays.update(canonical_array_path(path) for path in arrays)


class ZarrWriteSession:
    """Per-run guarded-write state; construct on the main thread before workers."""

    def __init__(
        self,
        *,
        product: str,
        run_id: str,
        write_mode: str,
        window: tuple[int, int] | None,
        slot_group: str | None,
        acquire_claim: Callable[..., Any],
        open_root: Callable[[str], Any | None],
        open_final_root: Callable[[Any], Any | None],
        drain_timeout: float = 60.0,
        workspace_uri: str | None = None,
        open_final_store: Callable[[], tuple[str, Any]] | None = None,
        claim_latch: ClaimLossLatch | None = None,
    ) -> None:
        """Build the session; no I/O.

        ``open_final_store`` returns the final target's URI and a zarr Store
        over it (built without I/O); a staged session given it takes, at
        verified promotion, the authority promoted ``zarr.json`` documents
        are published under. Without it a staged run reports no promotion
        units.

        ``claim_latch`` is the run's claim loss latch; without it the session makes its own.
        """
        self.product = product
        self.run_id = run_id
        self.write_mode = write_mode
        self.window = window
        self.slot_group = slot_group
        self._acquire_claim = acquire_claim
        self._open_root = open_root
        self._open_final_root = open_final_root
        self._drain_timeout = drain_timeout
        self._workspace_uri = workspace_uri
        self._open_final_store = open_final_store
        # (target URI, target store, authority over it): from verified promotion to close.
        self._publishing: tuple[str, Any, MetadataAuthority] | None = None
        self._cond = threading.Condition()
        self._in_flight = 0
        self._state = SessionState.PREPARED
        self._closed = False  # the first close() has drained and released
        self._drain_failed: str | None = None  # the first close()'s drain timed out
        self._claims = RunSchemaClaims()
        self._claim_latch = claim_latch if claim_latch is not None else ClaimLossLatch()
        self._ledger = StagedWriteLedger()
        self._engine_owned_checked = False
        self._results_lock = threading.Lock()
        self._written_units: dict[str, dict[str, set[Unit]]] = {}
        self._absent_units: dict[str, dict[str, set[Unit]]] = {}
        self._seeded_coordinates: dict[str, set[str]] = {}
        # Store-wide, so seeding may write the workspace root document too.
        self._seeding_authority = (
            MetadataAuthority.process_local(RegionZarrWriter(workspace_uri).metadata_scope())
            if self.staged and workspace_uri is not None
            else None
        )

    # --- lifecycle -------------------------------------------------------------------

    @property
    def state(self) -> SessionState:
        return self._state

    @property
    def claim_latch(self) -> ClaimLossLatch:
        """The latch that records the loss of any claim of this run; it outlives the claims."""
        return self._claim_latch

    @property
    def staged(self) -> bool:
        return self.write_mode == "staged"

    @contextlib.contextmanager
    def admit_write(self) -> Iterator[None]:
        """Admit one batch of writes; refuse outside prepared/writing.

        The batch counts as in flight until the block exits, which is what
        `verify_promotion` and `close` wait for.

        Raises:
            ClaimLostError: A claim of this run was lost; the loss stays for the
                whole run, also after the lost claim was released.
        """
        with self._cond:
            self._claim_latch.raise_if_set()
            if self._state not in (SessionState.PREPARED, SessionState.WRITING):
                raise WriteSessionStateError(
                    f"write refused: session is {self._state.value}, not prepared or writing"
                )
            self._state = SessionState.WRITING
            self._in_flight += 1
        try:
            yield
        finally:
            with self._cond:
                self._in_flight -= 1
                self._cond.notify_all()

    def _drain(self, what: str) -> None:
        """Wait, holding ``_cond``, for in-flight writes to leave."""
        if not self._cond.wait_for(lambda: self._in_flight == 0, timeout=self._drain_timeout):
            raise WriteSessionStateError(
                f"{what}: {self._in_flight} write(s) still in flight after {self._drain_timeout:g}s"
            )

    @contextlib.contextmanager
    def _operation(self) -> Iterator[None]:
        """Register one operational call as in flight; refuse once terminal.

        The open-check and the registration share one lock hold and the effect
        runs inside the block, so `close` (which drains) cannot complete
        between the check and the effect. Registration is a counter, not an
        owner, so a call made inside an admitted batch just adds one.
        """
        with self._cond:
            if self._state is SessionState.TERMINAL:
                raise WriteSessionStateError("session is terminal")
            self._in_flight += 1
        try:
            yield
        finally:
            with self._cond:
                self._in_flight -= 1
                self._cond.notify_all()

    def verify_promotion(self) -> None:
        """Stop admissions, drain in-flight writes, run the staged-coverage check.

        The seeding authority is released after the drain, before the check.
        When the check passes, a staged session that knows its final target
        takes the authority over it that promotion publishes metadata under.

        Raises:
            ClaimLostError: A claim of this run was lost, so the run promotes nothing.
        """
        with self._cond:
            self._claim_latch.raise_if_set()
            if self._state not in (SessionState.PREPARED, SessionState.WRITING):
                raise WriteSessionStateError(
                    f"promotion refused: session is {self._state.value}, not prepared or writing"
                )
            self._state = SessionState.PUBLISHING
            try:
                self._drain("promotion refused")
            finally:
                self._release_seeding_authority()
        ledger = self._ledger
        if ledger.store_uri is not None and ledger.touched:
            require_staged_units_fully_written(
                self._open_root(ledger.store_uri), list(ledger.touched), list(ledger.written)
            )
        self._hold_publishing_authority()

    def _hold_publishing_authority(self) -> None:
        """Take the ``PROCESS_LOCAL`` authority over the whole final target store."""
        if not self.staged or self._open_final_store is None:
            return
        final_uri, store = self._open_final_store()
        scope = RegionZarrWriter(final_uri, store=store).metadata_scope()
        with self._cond:
            if self._state is not SessionState.PUBLISHING:
                raise WriteSessionStateError(
                    f"promotion refused: session turned {self._state.value} while verifying"
                )
            self._publishing = (final_uri, store, MetadataAuthority.process_local(scope))

    def close(self) -> None:
        """Terminal; stops admissions and drains in-flight writes.

        The seeding and publishing authorities are released after the drain,
        also when it timed out: seeding runs before a batch is admitted, and
        the publishing authority exists only after a drain that left nothing
        in flight, so neither release can fail an admitted batch. (Such a
        batch's own later session calls are refused anyway: the session is
        terminal.)

        A later or concurrent call waits until the first call has finished
        draining and releasing. If the first drain timed out and work is
        still in flight, it raises that same failure, so no caller mistakes
        the session for drained; once nothing is in flight it returns, as it
        does after a first drain that completed. It does not wait again.

        Raises:
            WriteSessionStateError: Writes are still in flight: after
                ``drain_timeout`` seconds for the first call, at once for a
                later one.
        """
        with self._cond:
            if self._state is SessionState.TERMINAL:
                self._cond.wait_for(lambda: self._closed)
                if self._drain_failed is not None and self._in_flight:
                    raise WriteSessionStateError(self._drain_failed)
                return
            self._state = SessionState.TERMINAL
            try:
                self._drain("close")
            except WriteSessionStateError as exc:
                self._drain_failed = str(exc)
                raise
            finally:
                try:
                    self._release_seeding_authority()
                finally:
                    try:
                        if self._publishing is not None:
                            self._publishing[2].revoke()
                    finally:
                        self._closed = True
                        self._cond.notify_all()

    def _release_seeding_authority(self) -> None:
        """Release the seeding authority once its admitted writes have finished.

        Called holding ``_cond``: a seeding write never waits for ``_cond``
        while admitted under the authority, so the wait cannot deadlock. The
        same holds for the publishing authority.
        """
        if self._seeding_authority is not None:
            self._seeding_authority.revoke()

    # --- run state -------------------------------------------------------------------

    def staged_seeding_authority(self) -> MetadataAuthority | None:
        """The authority staged seeding writes the workspace ``zarr.json`` documents under.

        ``None`` for a session that is not staged or knows no workspace.
        Once promotion is verified or the session closed it is released, and
        the seeding operations refuse it.
        """
        with self._operation():
            return self._seeding_authority

    def staged_promotion_units(
        self, *, time_coord_name: str = "timestamp"
    ) -> StagedPromotionUnits | None:
        """What promotion publishes: the run's proved units as object keys, and its publisher.

        ``written`` holds the object key of every unit the staged ledger
        proves written in every cell, each of which a guarded write must also
        have reported; ``absent`` the ones the last guarded write of each
        left without an object (all fill), which promotion deletes. A unit a
        guarded write left absent but the ledger does not prove written
        (time coordinate, static, engine-owned arrays) is not listed: it is
        never deleted. ``ledger_arrays`` names the pod-written arrays the
        batches recorded; promotion refuses their changed data objects that
        ``written`` lacks. Keys are relative to the workspace root, which is
        also the target root. ``publish_metadata`` puts a promoted
        ``zarr.json`` under the session's authority over the target, keeping
        the target's attributes of an array it already holds; it is refused
        once the session closed.

        ``managed_coordinates`` names the time coordinates, called
        *time_coord_name*, that the target marks as preallocated or
        coordinate-managed. The session checks each coordinate seeding
        reported in the workspace, and the coordinate of each group holding
        an array this run wrote when the workspace holds it. It reads each
        such coordinate document from the target once.

        Returns:
            ``None`` when the session is not staged or knows no final target
            or workspace: promotion then uploads the whole workspace tree.

        Raises:
            WriteSessionStateError: Promotion was not verified, or the session
                is terminal.
            ConfigurationError: A unit the ledger proves written was never
                reported by a guarded write (a stale workspace object, or a
                ledger wrong about what ran), or a unit the workspace array
                ends inside reaches cells of the target past the workspace
                extent (the target grew into it after seeding, so promoting
                the whole object would erase cells the run never wrote); the
                target is untouched. That growth check reads the target's
                array documents here, before promotion: a target that grows
                between this check and the put is not detected; closing it
                needs a cross-process claim.
            SchemaDriftError: A checked coordinate carries an invalid marker
                state (`raise_if_invalid`); the target is untouched.
        """
        with self._operation():
            if not self.staged or self._open_final_store is None:
                return None
            with self._cond:
                publishing = self._publishing
            if publishing is None:
                raise WriteSessionStateError(
                    f"promotion units refused: session is {self._state.value} and its "
                    "promotion was not verified"
                )
            ledger = self._ledger
            workspace = ledger.store_uri or self._workspace_uri
            if workspace is None:
                return None
            written: frozenset[str] = frozenset()
            absent: frozenset[str] = frozenset()
            final_uri, store, authority = publishing
            if ledger.store_uri is not None and ledger.touched:
                written, absent = self._promotion_keys(
                    ledger.store_uri, list(ledger.touched), store
                )
            return StagedPromotionUnits(
                workspace=workspace,
                target=final_uri,
                written=written,
                absent=absent,
                publish_metadata=functools.partial(_publish_promoted_document, authority, store),
                ledger_arrays=frozenset(ledger.arrays),
                managed_coordinates=self._managed_coordinates(workspace, time_coord_name, store),
            )

    def _managed_coordinates(
        self, workspace: str, time_coord_name: str, target_store: Any
    ) -> frozenset[str]:
        """Paths of the coordinates the target marks managed; see `staged_promotion_units`.

        Candidates are the coordinates seeding reported in *workspace*, and
        the coordinate of each group holding an array the ledger or a guarded
        write recorded there. Seeding copies a coordinate's chunks into the
        workspace; for a managed coordinate that copy goes stale.
        """
        with self._results_lock:
            recorded = set(self._written_units.get(workspace, {}))
            seeded = {
                path
                for path in self._seeded_coordinates.get(workspace, ())
                if path.rpartition("/")[2] == time_coord_name
            }
        recorded.update(write.array_path for write in self._ledger.touched)
        groups = {canonical_array_path(path).rpartition("/")[0] for path in recorded}
        # Only a written group's coordinate that seeding did not report costs a stat.
        written = {canonical_array_path(f"{group}/{time_coord_name}") for group in groups}
        candidates = seeded | {
            coord for coord in written - seeded if _workspace_holds(workspace, coord)
        }
        managed: set[str] = set()
        for coord in sorted(candidates):
            document = _target_array_document(target_store, coord)
            if document is None:
                continue
            state = resolve_coord_lifecycle(document.get("attributes") or {})
            raise_if_invalid(state, coord)
            if state in (CoordLifecycleState.PREALLOCATED, CoordLifecycleState.COORD_MANAGED):
                managed.add(coord)
        return frozenset(managed)

    def _promotion_keys(
        self, store_uri: str, touched: list[StagedSlotWrite], target_store: Any
    ) -> tuple[frozenset[str], frozenset[str]]:
        """``(written, absent)`` object keys of the workspace *store_uri*; see the caller.

        *target_store* is read (array documents only) to refuse a unit the
        workspace ends inside that the target has grown into.
        """
        root = self._open_root(store_uri)
        if root is None:
            raise ConfigurationError(
                f"staged promotion refused: workspace {store_uri} is missing, but the run wrote to it"
            )
        proved = staged_unit_object_keys(root, touched)
        with self._results_lock:
            reported_units = {p: set(u) for p, u in self._written_units.get(store_uri, {}).items()}
            absent_units = {p: set(u) for p, u in self._absent_units.get(store_uri, {}).items()}

        arrays: dict[str, Any] = {}

        def keys(per_array: dict[str, set[Unit]]) -> set[str]:
            out: set[str] = set()
            for path, units in per_array.items():
                arr = arrays.get(path)
                if arr is None:
                    arr = arrays[path] = root[path]
                out.update(unit_store_key(arr, unit) for unit in units)
            return out

        unreported = sorted(proved - keys(reported_units))
        if unreported:
            raise ConfigurationError(
                f"staged promotion refused, target untouched: {len(unreported)} unit(s) the "
                "staged ledger proves written were never reported by a guarded write: "
                f"{unreported[:20]}"
            )
        require_target_within_clipped_units(
            root, touched, functools.partial(_target_array_shape, target_store)
        )
        return proved, frozenset(keys(absent_units) & proved)

    def final_root(self, ctx: Any) -> Any | None:
        """The FINAL target, read-only (engine-owned values live only there)."""
        with self._operation():
            return self._open_final_root(ctx)

    def staged_root(self, store_uri: str) -> Any | None:
        """The staged workspace at *store_uri*, read-only; ``None`` when it does not exist."""
        with self._operation():
            return self._open_root(store_uri)

    def engine_owned_checked(self) -> bool:
        with self._operation(), self._results_lock:
            return self._engine_owned_checked

    def mark_engine_owned_checked(self) -> None:
        with self._operation(), self._results_lock:
            self._engine_owned_checked = True

    def schema_claim(self, group: str) -> contextlib.AbstractContextManager[HeldClaim]:
        """The run-shared schema claim of *group*; its scope yields the `HeldClaim`.

        Code inside the scope builds its metadata authority from the yielded
        claim (`adopt_held_claim`), so the authority lasts no longer than
        this batch's share of the claim.
        """
        domain = WriteDomain(product=self.product, category="zarr_region", name=f"{group}:schema")
        owner_id = f"{self.run_id}:{group}"
        with self._operation():
            return self._claims.acquire(
                (self.run_id, group),
                lambda: self._acquire_with_latch(
                    product=self.product, domain=domain, owner_id=owner_id
                ),
                HeldClaim(domain.identifier, owner_id),
            )

    def slot_claim(self, group: str, ts_index: int) -> Any:
        """The intent-dispatch claim of one group slot."""
        domain = WriteDomain(
            product=self.product, category="zarr_region", name=f"{group}:slot={ts_index}"
        )
        with self._operation():
            return self._acquire_with_latch(
                product=self.product,
                domain=domain,
                owner_id=f"{self.run_id}:{group}:slot={ts_index}",
            )

    def _acquire_with_latch(self, **kwargs: Any) -> Any:
        """Acquire a claim for this run and make its loss a loss of the run."""
        handle = self._acquire_claim(**kwargs)
        attach = getattr(handle, "attach_latch", None)
        if attach is not None:
            attach(self._claim_latch)
        return handle

    def record_touched(
        self, store_uri: str, writes: Sequence[StagedSlotWrite], *, arrays: Iterable[str] = ()
    ) -> None:
        """Record slot writes about to run, and the arrays whose writes the ledger holds.

        Promotion refuses a changed data object of *arrays* that no written
        entry covers.
        """
        with self._operation():
            self._ledger.record(store_uri, writes, written=False, arrays=arrays)

    def record_written(self, store_uri: str, writes: Sequence[StagedSlotWrite]) -> None:
        with self._operation():
            self._ledger.record(store_uri, writes, written=True)

    def record_write_result(self, result: WriteResult, *, store_uri: str) -> None:
        """Take a guarded write's outcome.

        Every unit of the result counts as written; the absent ones are a
        subset of them. A tracked result also settles the absence of each of
        its units afresh, so a unit written again with its object present
        leaves the absent set. An untracked result says nothing about absence.

        Results are applied in the order they arrive, which is the order of
        the writes to a unit only because the region writer calls this while
        it still holds that write's unit locks. It therefore takes no unit
        lock and never waits: it registers as an operation (``_cond``, held
        briefly; `close` and `verify_promotion` drain on it without holding
        unit locks) and then takes ``_results_lock``.
        """
        path = result.footprint.array_path
        units = set(result.units)
        with self._operation(), self._results_lock:
            self._written_units.setdefault(store_uri, {}).setdefault(path, set()).update(units)
            if result.tracked:
                absent = self._absent_units.setdefault(store_uri, {}).setdefault(path, set())
                absent.difference_update(units)
                absent.update(result.absent)

    def note_seeded_coordinates(self, store_uri: str, paths: Collection[str]) -> None:
        """Record coordinate arrays whose chunks seeding left in the workspace *store_uri*.

        `staged_promotion_units` checks each against the target's lifecycle
        marker. No I/O.
        """
        with self._operation(), self._results_lock:
            self._seeded_coordinates.setdefault(store_uri, set()).update(
                canonical_array_path(path) for path in paths
            )

    def written_units(self, store_uri: str) -> dict[str, frozenset[Unit]]:
        """Units recorded as written in *store_uri*, per array path."""
        with self._operation(), self._results_lock:
            return {p: frozenset(u) for p, u in self._written_units.get(store_uri, {}).items()}

    def absent_units(self, store_uri: str) -> dict[str, frozenset[Unit]]:
        """Units recorded as holding no object after a write in *store_uri*, per array path."""
        with self._operation(), self._results_lock:
            return {p: frozenset(u) for p, u in self._absent_units.get(store_uri, {}).items()}
