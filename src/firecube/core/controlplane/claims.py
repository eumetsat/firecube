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

"""Write-domain claim service for ChunkManager v2.

A claim is one JSON record per write domain under `.firecube/claims/`. Every
transition compares against the validator of the record it last read, through
the driver's `ConditionalWrite` capability:

- acquire creates the record, or replaces a released record with a new
  incarnation; a held record refuses, stale or not;
- a heartbeat replaces its own held revision and never creates a record;
- release deletes its own revision where the driver reports `CompareDelete`,
  and otherwise replaces it with a released record;
- `clear_claim` and `release_claim_if_unchanged` retire the revision they read.

A record carries `state` (`held` or `released`), `incarnation` (one id per
acquisition) and `revision` (one id per write). A record without `state` is
held. Listings and reads by domain leave released records out.
"""

from __future__ import annotations

import contextvars
import json
import logging
import math
import threading
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field
from typing import Any, Literal, NoReturn, Protocol, cast

from firecube.core.controlplane.types import (
    CLAIM_STATE_HELD,
    CLAIM_STATE_RELEASED,
    CLAIMS_DIRNAME,
    ClaimInfo,
    ClaimState,
    WriteDomain,
)
from firecube.core.errors import (
    ClaimConflictError,
    ClaimLostError,
    ClaimReleaseError,
    ConfigurationError,
    ControlPlaneCorruptionError,
    Message,
    Param,
)
from firecube.core.filesystem import StorageFilesystemFull
from firecube.core.filesystem.protocol import CompareDelete, ConditionalWrite, PreconditionFailed
from firecube.core.storage.uri import StorageUri

DEFAULT_HEARTBEAT_INTERVAL_S = 30
DEFAULT_STALE_THRESHOLD_S = 120

log = logging.getLogger(__name__)


class ClaimService(Protocol):
    """Pluggable claim backend interface."""

    def acquire(self, *, product: str, domain: WriteDomain, owner_id: str) -> ClaimHandle:
        """Acquire an exclusive write claim for the given domain."""
        ...

    def list_claims(self, *, product: str | None = None) -> list[ClaimInfo]:
        """List active claims, optionally filtered by product."""
        ...

    def list_stale_claims(self, *, product: str) -> list[ClaimInfo]:
        """List stale claims for one product."""
        ...

    def clear_claim(self, *, product: str, domain_id: str, force: bool = False) -> bool:
        """Release a claim by domain ID; force=True overrides stale checks."""
        ...


class ClaimLossLatch:
    """Records that a run lost one of its claims; once set, it stays set.

    A claim handle sets it when its record turns out to belong to someone else.
    The latch keeps the handles attached to it until they are released.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._domain: str | None = None
        self._handles: list[ClaimHandle] = []

    def track(self, handle: ClaimHandle) -> None:
        """Keep `handle` for `confirm_held_claims`; drop the handles already released."""
        with self._lock:
            self._handles = [held for held in self._handles if not held.released]
            if all(held is not handle for held in self._handles):
                self._handles.append(handle)

    def confirm_held_claims(self) -> int:
        """Re-read the record of every tracked handle not yet released, and return how many were read.

        A handle whose record no longer holds its incarnation and revision sets this latch.
        """
        with self._lock:
            held = [handle for handle in self._handles if not handle.released]
            self._handles = held
        return sum(1 for handle in held if handle.confirm())

    def set(self, domain: str) -> None:
        """Record the loss of the claim on `domain`; keep the first domain recorded."""
        with self._lock:
            if self._domain is None:
                self._domain = domain

    @property
    def is_set(self) -> bool:
        """True once a claim loss is recorded."""
        with self._lock:
            return self._domain is not None

    @property
    def domain(self) -> str | None:
        """The domain of the first lost claim, or `None` while none is lost."""
        with self._lock:
            return self._domain

    def error(self) -> ClaimLostError | None:
        """Return the `ClaimLostError` that names the first lost claim, or `None` while none is lost."""
        domain = self.domain
        if domain is None:
            return None
        return ClaimLostError(
            Message(
                "Claim {claim_domain} was taken over by another writer during this "
                "run. The run writes no further batch, because the store may hold "
                "that writer's data.",
                claim_domain=domain,
            ),
            domain=domain,
        )

    def raise_if_set(self) -> None:
        """Raise `ClaimLostError` once a claim loss is recorded.

        Raises:
            ClaimLostError: The run lost the claim on `domain`.
        """
        lost = self.error()
        if lost is not None:
            raise lost


_REPORTING_LATCH: contextvars.ContextVar[ClaimLossLatch | None] = contextvars.ContextVar(
    "firecube_claim_loss_latch", default=None
)


@contextmanager
def reporting_claim_loss(latch: ClaimLossLatch) -> Iterator[None]:
    """Attach `latch` to every claim that `FilesystemClaimService.acquire` creates in this context.

    A thread started inside the block does not inherit the context; attach its claims explicitly.
    """
    token = _REPORTING_LATCH.set(latch)
    try:
        yield
    finally:
        _REPORTING_LATCH.reset(token)


@dataclass(frozen=True, slots=True)
class _ClaimRecord:
    """One parsed claim record and the validator of the generation it was read from."""

    info: ClaimInfo
    state: ClaimState
    validator: str


def _conditional(fs: Any) -> ConditionalWrite:
    """Return `fs` as a `ConditionalWrite` driver; refuse a driver that does not report it.

    Raises:
        ConfigurationError: The driver does not report `ConditionalWrite`.
    """
    if ConditionalWrite not in fs.capabilities():
        raise ConfigurationError(
            f"Write-domain claims need a storage driver with conditional writes; "
            f"{type(fs).__name__} reports none for this target, so no claim is read or written."
        )
    return cast(ConditionalWrite, fs)


def _retire(fs: Any, claim_path: StorageUri, validator: str, info: ClaimInfo) -> None:
    """Retire the record generation `validator`: compare-delete it, else mark it released.

    Raises:
        PreconditionFailed: The record changed since `validator` was read, or is missing.
    """
    if CompareDelete in fs.capabilities():
        cast(CompareDelete, fs).compare_delete(claim_path, validator)
        return
    released = _encode(info, state=CLAIM_STATE_RELEASED, revision=uuid.uuid4().hex, at=time.time())
    _conditional(fs).compare_replace(claim_path, validator, released)


def _encode(info: ClaimInfo, *, state: ClaimState, revision: str, at: float) -> bytes:
    """Return the record bytes of `info` in `state`, written as `revision` at time `at`."""
    payload: dict[str, Any] = {
        "product": info.product,
        "domain": info.domain,
        "owner_id": info.owner_id,
        "claim_path": info.claim_path,
        "acquired_at": info.acquired_at,
        "last_heartbeat_at": at,
        "heartbeat_interval_s": info.heartbeat_interval_s,
        "stale_threshold_s": info.stale_threshold_s,
        "state": state,
        "revision": revision,
    }
    if info.incarnation is not None:
        payload["incarnation"] = info.incarnation
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


def _is_number(value: object) -> bool:
    """Return True for a finite int or float that converts to a float; bool is not a number."""
    if not isinstance(value, int | float) or isinstance(value, bool):
        return False
    try:
        return math.isfinite(value)
    except OverflowError:  # an int too large for a float
        return False


def _parse_record(
    data: bytes,
    *,
    product: str,
    claim_path: StorageUri,
    validator: str,
    default_heartbeat_s: int,
    default_stale_s: int,
) -> _ClaimRecord:
    """Parse and validate one claim record.

    Raises:
        ControlPlaneCorruptionError: The bytes are not JSON, not an object, or a field is
            missing or of the wrong type.
    """
    where = claim_path.to_str()
    try:
        payload = json.loads(data)
    except ValueError as exc:
        raise ControlPlaneCorruptionError(f"Corrupt claim file: {where}") from exc
    if not isinstance(payload, dict):
        raise ControlPlaneCorruptionError(
            f"Corrupt claim file: {where}: the record is {type(payload).__name__}, not an object"
        )

    def corrupt(problem: str) -> ControlPlaneCorruptionError:
        return ControlPlaneCorruptionError(f"Corrupt claim file: {where}: {problem}")

    for name in ("domain", "owner_id"):
        if not isinstance(payload.get(name), str):
            raise corrupt(f"{name} is missing or not a string")
    for name in ("acquired_at", "last_heartbeat_at"):
        if not _is_number(payload.get(name)):
            raise corrupt(f"{name} is missing or not a finite number")
    intervals = {"heartbeat_interval_s": default_heartbeat_s, "stale_threshold_s": default_stale_s}
    for name, default in intervals.items():
        value = payload.get(name, default)
        if not _is_number(value):
            raise corrupt(f"{name} is not a finite number")
        intervals[name] = int(value)
    state = payload.get("state", CLAIM_STATE_HELD)
    if state not in (CLAIM_STATE_HELD, CLAIM_STATE_RELEASED):
        raise corrupt(
            f"state {state!r} is neither {CLAIM_STATE_HELD!r} nor {CLAIM_STATE_RELEASED!r}"
        )
    for name in ("incarnation", "revision"):
        if name in payload and not isinstance(payload[name], str):
            raise corrupt(f"{name} is not a string")
    info = ClaimInfo(
        product=product,
        domain=payload["domain"],
        owner_id=payload["owner_id"],
        claim_path=where,
        acquired_at=float(payload["acquired_at"]),
        last_heartbeat_at=float(payload["last_heartbeat_at"]),
        heartbeat_interval_s=intervals["heartbeat_interval_s"],
        stale_threshold_s=intervals["stale_threshold_s"],
        incarnation=payload.get("incarnation"),
        revision=payload.get("revision"),
    )
    return _ClaimRecord(info=info, state=cast(ClaimState, state), validator=validator)


def _entry_to_uri(entry: Any, base_uri: StorageUri) -> StorageUri:
    """Convert an `ls(detail=False)` entry back to a StorageUri using base context."""
    name = entry.get("name") if isinstance(entry, dict) else entry
    if isinstance(name, StorageUri):
        return name
    raw = str(name or "")
    if "://" in raw:
        return StorageUri.parse(raw)
    if base_uri.protocol == "file" and raw.startswith("/"):
        return StorageUri.from_local_path(raw)
    path = raw
    if base_uri.authority and path.startswith(f"{base_uri.authority}/"):
        path = path[len(base_uri.authority) + 1 :]
    return StorageUri(protocol=base_uri.protocol, authority=base_uri.authority, path=path)


@dataclass(slots=True)
class ClaimHandle:
    """Exclusive claim with a background heartbeat that renews only its own record.

    `validator` names the record generation the handle last wrote. A handle whose
    record turns out to belong to someone else is lost: its heartbeat stops and it
    sets `latch`.
    """

    fs: StorageFilesystemFull
    claim_path: StorageUri
    info: ClaimInfo
    heartbeat_interval_s: int
    stale_threshold_s: int
    validator: str
    latch: ClaimLossLatch | None = None
    _stop: threading.Event = field(init=False, repr=False)
    _thread: threading.Thread | None = field(init=False, default=None, repr=False)
    _release_lock: threading.Lock = field(init=False, repr=False)
    _call_lock: threading.Lock = field(init=False, repr=False)
    _lost: bool = field(init=False, default=False, repr=False)
    _released: bool = field(init=False, default=False, repr=False)
    _retire_uncertain: bool = field(init=False, default=False, repr=False)
    _unconfirmed: set[str] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._stop = threading.Event()
        self._release_lock = threading.Lock()
        # Held by the heartbeat for the whole of one renewal, so release can wait for it.
        self._call_lock = threading.Lock()
        self._unconfirmed = set()
        if self.latch is not None:
            self.latch.track(self)
        self._start_heartbeat()

    def __enter__(self) -> ClaimHandle:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.release()
        return False

    def attach_latch(self, latch: ClaimLossLatch) -> None:
        """Report this handle's loss to `latch`, also a loss that happened already."""
        self.latch = latch
        latch.track(self)
        if self._lost:
            latch.set(self.info.domain)

    @property
    def lost(self) -> bool:
        """True once the handle found its record changed by someone else."""
        return self._lost

    @property
    def released(self) -> bool:
        """True once `release` finished for this handle."""
        return self._released

    def confirm(self) -> bool:
        """Read the record once, without writing; mark the handle lost when the record is not its own.

        The record is its own while it is held with this incarnation and this handle's last
        or an unconfirmed revision. A failed read warns and keeps the claim. So does a
        heartbeat write still in flight after `stale_threshold_s`; release then waits for it.

        Returns:
            True when the record was read; False for a handle released or lost already, or
            when the wait ran out.
        """
        deadline = time.monotonic() + self.stale_threshold_s
        if not self._call_lock.acquire(timeout=self._remaining(deadline)):
            log.warning(
                "claim confirm skipped for %s: its heartbeat write was still in flight after %s s",
                self.claim_path,
                self.stale_threshold_s,
            )
            return False
        try:
            if self._released or self._lost:
                return False
            try:
                data, validator = cast(ConditionalWrite, self.fs).read_snapshot(self.claim_path)
            except FileNotFoundError:
                self._lose("the record is gone")
                return True
            except Exception as exc:
                log.warning("claim confirm read failed for %s: %s", self.claim_path, exc)
                return True
            record = self._parse(data, validator)
            if (
                record is None
                or record.state != CLAIM_STATE_HELD
                or record.info.incarnation != self.info.incarnation
            ):
                self._lose("the record was released, cleared or taken over")
            elif record.info.revision == self.info.revision:
                self.validator = validator
            elif record.info.revision in self._unconfirmed:
                # A heartbeat write whose answer was lost did commit.
                self._adopt(
                    validator, cast(str, record.info.revision), record.info.last_heartbeat_at
                )
            else:
                self._lose("the record holds a revision this handle did not write")
            return True
        finally:
            self._call_lock.release()

    def release(self) -> None:
        """Stop the heartbeat, then retire the handle's own record generation.

        Waits at most `stale_threshold_s` in all, for a concurrent release and for a
        heartbeat write in flight. A lost handle touches nothing. A second call after a
        release does nothing.

        Raises:
            ClaimReleaseError: The wait ran out; the record is untouched.
        """
        deadline = time.monotonic() + self.stale_threshold_s
        self._stop.set()
        if not self._release_lock.acquire(timeout=self._remaining(deadline)):
            self._release_timed_out()
        try:
            if self._released:
                return
            if not self._call_lock.acquire(timeout=self._remaining(deadline)):
                self._release_timed_out()
            try:
                if not self._lost:
                    self._retire_own_record()
                self._released = True
            finally:
                self._call_lock.release()
        finally:
            self._release_lock.release()

    @staticmethod
    def _remaining(deadline: float) -> float:
        return max(deadline - time.monotonic(), 0.0)

    def _release_timed_out(self) -> NoReturn:
        """Set the latch and raise `ClaimReleaseError`; the record stays untouched."""
        self._set_latch()
        raise ClaimReleaseError(
            Message(
                "Claim {claim_domain} of {claim_owner} was not released: its "
                "heartbeat write was still in flight after {wait_s} s. The claim "
                "goes stale; clear it once no process of its owner runs.",
                claim_domain=self.info.domain,
                claim_owner=self.info.owner_id,
                wait_s=self.stale_threshold_s,
                product=self.info.product,
            ),
            domain=self.info.domain,
            owner_id=self.info.owner_id,
        )

    def _retire_own_record(self) -> None:
        """Retire the record generation the handle last wrote; mark the handle lost otherwise.

        A refused retire re-reads once and retries when the record still holds a revision
        this handle wrote. A failed retire propagates and leaves the outcome uncertain.
        """
        validator: str | None = self.validator
        for _attempt in range(2):
            assert validator is not None
            try:
                _retire(self.fs, self.claim_path, validator, self.info)
                return
            except PreconditionFailed:
                validator = self._own_validator_after_refusal()
                if validator is None:
                    return
            except Exception:
                self._retire_uncertain = True
                raise
        self._lose("the record changed again while it was released")

    def _own_validator_after_refusal(self) -> str | None:
        """Re-read the record after a refused retire; return its validator while it is still ours.

        A record gone or released counts as this handle's own retire only after a retire of
        this handle failed without an answer; otherwise the claim is lost. Returns `None`
        when there is nothing left to retire.
        """
        try:
            data, validator = _conditional(self.fs).read_snapshot(self.claim_path)
        except FileNotFoundError:
            if not self._retire_uncertain:
                self._lose("the record is gone")
            return None
        record = self._parse(data, validator)
        if record is None or record.info.incarnation != self.info.incarnation:
            self._lose("the record belongs to another acquisition")
            return None
        if record.state == CLAIM_STATE_RELEASED:
            if not self._retire_uncertain:
                self._lose("the record was released by someone else")
            return None
        if record.info.revision in self._unconfirmed | {self.info.revision}:
            return validator
        self._lose("the record holds a revision this handle did not write")
        return None

    def _start_heartbeat(self) -> None:
        def _loop() -> None:
            while not self._stop.wait(self.heartbeat_interval_s):
                with self._call_lock:
                    if self._stop.is_set() or not self._beat():
                        return

        self._thread = threading.Thread(
            target=_loop,
            name=f"firecube-claim-{self.info.owner_id}",
            daemon=True,
        )
        self._thread.start()

    def _beat(self) -> bool:
        """Renew the record once; return False when the claim is lost.

        The new revision is chosen before the write, so a re-read tells a write that
        committed from one that did not.
        """
        revision = uuid.uuid4().hex
        now = time.time()
        data = _encode(self.info, state=CLAIM_STATE_HELD, revision=revision, at=now)
        try:
            validator = cast(ConditionalWrite, self.fs).compare_replace(
                self.claim_path, self.validator, data
            )
        except PreconditionFailed:
            return self._reconcile(revision, refused=True)
        except Exception as exc:
            log.warning("claim heartbeat write failed for %s: %s", self.claim_path, exc)
            return self._reconcile(revision, refused=False)
        self._adopt(validator, revision, now)
        return True

    def _reconcile(self, attempted: str, *, refused: bool) -> bool:
        """Re-read the record once after a refused or failed heartbeat write.

        Continues with the read validator when the record holds this incarnation at the
        attempted or an earlier unconfirmed revision. After a failed, unrefused write, the
        last confirmed revision or a read error keeps the claim too. Anything else loses it.
        """
        if not refused:
            self._unconfirmed.add(attempted)
        try:
            data, validator = cast(ConditionalWrite, self.fs).read_snapshot(self.claim_path)
        except FileNotFoundError:
            return self._lose("the record is gone")
        except Exception as exc:
            if refused:
                return self._lose(f"the refused record could not be re-read: {exc}")
            log.warning("claim heartbeat re-read failed for %s: %s", self.claim_path, exc)
            return True
        record = self._parse(data, validator)
        mine = (
            record is not None
            and record.state == CLAIM_STATE_HELD
            and record.info.incarnation == self.info.incarnation
        )
        if record is None or not mine:
            return self._lose("the record was released, cleared or taken over")
        if record.info.revision == attempted or record.info.revision in self._unconfirmed:
            self._adopt(validator, cast(str, record.info.revision), record.info.last_heartbeat_at)
            return True
        if not refused and record.info.revision == self.info.revision:
            # A failed write that did not land; the record is still the one this handle wrote.
            self.validator = validator
            return True
        if refused:
            return self._lose("the store refused the write and the record shows no attempted one")
        return self._lose("the record holds a revision this handle did not write")

    def _parse(self, data: bytes, validator: str) -> _ClaimRecord | None:
        """Parse a re-read record; return None when it is unreadable."""
        try:
            return _parse_record(
                data,
                product=self.info.product,
                claim_path=self.claim_path,
                validator=validator,
                default_heartbeat_s=self.heartbeat_interval_s,
                default_stale_s=self.stale_threshold_s,
            )
        except ControlPlaneCorruptionError:
            return None

    def _adopt(self, validator: str, revision: str, heartbeat_at: float) -> None:
        self.validator = validator
        self.info.revision = revision
        self.info.last_heartbeat_at = heartbeat_at
        self._unconfirmed.clear()

    def _lose(self, reason: str) -> bool:
        self._lost = True
        log.warning(
            "claim %s of %s is lost (%s): %s",
            self.info.domain,
            self.info.owner_id,
            self.claim_path,
            reason,
        )
        self._set_latch()
        return False

    def _set_latch(self) -> None:
        if self.latch is not None:
            self.latch.set(self.info.domain)


class FilesystemClaimService:
    """Filesystem/object-store backed claim implementation.

    Every read and write goes through the driver's `ConditionalWrite` capability; a
    driver without it is refused with `ConfigurationError`.
    """

    def __init__(
        self,
        *,
        fs: StorageFilesystemFull,
        control_root_resolver: Any,
        heartbeat_interval_s: int = DEFAULT_HEARTBEAT_INTERVAL_S,
        stale_threshold_s: int = DEFAULT_STALE_THRESHOLD_S,
    ) -> None:
        self._fs = fs
        self._control_root_resolver = control_root_resolver
        self._heartbeat_interval_s = int(heartbeat_interval_s)
        self._stale_threshold_s = int(stale_threshold_s)
        self._log = log

    def acquire(self, *, product: str, domain: WriteDomain, owner_id: str) -> ClaimHandle:
        """Create the claim record, or take over a released one; refuse a held one.

        The handle reports its loss to the latch that `reporting_claim_loss` installed, if any.

        Raises:
            ClaimConflictError: A held record exists, stale or not, or another writer won
                the record first.
            ControlPlaneCorruptionError: The existing record cannot be read or parsed.
            ConfigurationError: The driver does not report `ConditionalWrite`.
        """
        fs = _conditional(self._fs)
        control_path, _control_uri = self._control_root_resolver(product)
        claims_dir = control_path.join(CLAIMS_DIRNAME)
        with suppress(Exception):
            self._fs.makedirs(claims_dir, exist_ok=True)
        claim_path = claims_dir.join(domain.claim_name)
        now = time.time()
        info = ClaimInfo(
            product=product,
            domain=domain.identifier,
            owner_id=owner_id,
            claim_path=claim_path.to_str(),
            acquired_at=now,
            last_heartbeat_at=now,
            heartbeat_interval_s=self._heartbeat_interval_s,
            stale_threshold_s=self._stale_threshold_s,
            incarnation=uuid.uuid4().hex,
            revision=uuid.uuid4().hex,
        )
        data = _encode(info, state=CLAIM_STATE_HELD, revision=cast(str, info.revision), at=now)
        try:
            validator = fs.create_if_absent(claim_path, data)
        except FileExistsError as exc:
            validator = self._take_over_released(claim_path, domain, data, exc)
        return ClaimHandle(
            fs=self._fs,
            claim_path=claim_path,
            info=info,
            heartbeat_interval_s=self._heartbeat_interval_s,
            stale_threshold_s=self._stale_threshold_s,
            validator=validator,
            latch=_REPORTING_LATCH.get(),
        )

    def _take_over_released(
        self, claim_path: StorageUri, domain: WriteDomain, data: bytes, cause: Exception
    ) -> str:
        """Replace an existing released record with `data`; refuse a held or changed one."""
        record = self._read_record(product=domain.product, claim_path=claim_path)
        if record is not None and record.state == CLAIM_STATE_RELEASED:
            try:
                return _conditional(self._fs).compare_replace(claim_path, record.validator, data)
            except PreconditionFailed as exc:
                cause = exc
                record = self._read_record(product=domain.product, claim_path=claim_path)
        held = record is not None and record.state == CLAIM_STATE_HELD
        stale = record.info.stale if record is not None and held else True
        owner = record.info.owner_id if record is not None and held else "unknown"
        raise ClaimConflictError(
            f"Write-domain claim conflict for {domain.identifier} "
            f"(owner={owner}, stale={str(stale).lower()})"
        ) from cause

    def list_claims(self, *, product: str | None = None) -> list[ClaimInfo]:
        """List held claims, sorted by (product, domain); leave released records out.

        An absent claims directory lists as no claims; any other listing error propagates.
        Raises ControlPlaneCorruptionError on a malformed claim file.
        """
        products = [product] if product else sorted(self._list_products())
        claims: list[ClaimInfo] = []
        for item in products:
            control_path, _control_uri = self._control_root_resolver(item)
            claims.extend(self._held_claims(item, control_path.join(CLAIMS_DIRNAME)))
        claims.sort(key=lambda item: (item.product, item.domain))
        return claims

    def list_product_claims(self, *, product: str) -> list[ClaimInfo]:
        """List one product's held claims from a fresh listing, sorted by domain.

        Asks the driver to bypass any listing cache and leaves released records out. An
        absent claims directory lists as no claims; any other listing error propagates.
        Raises ControlPlaneCorruptionError on a malformed claim file.
        """
        control_path, _control_uri = self._control_root_resolver(product)
        claims = self._held_claims(product, control_path.join(CLAIMS_DIRNAME), refresh=True)
        claims.sort(key=lambda item: item.domain)
        return claims

    def list_stale_claims(self, *, product: str) -> list[ClaimInfo]:
        """List stale held claims for one product."""
        return [claim for claim in self.list_claims(product=product) if claim.stale]

    def read_claim_by_domain(self, *, product: str, domain: str) -> ClaimInfo | None:
        """Return the held claim for a canonical write-domain identifier, if present.

        Uses targeted claim-name derivation to avoid listing the claims directory. A
        released record reads as no claim. Raises ControlPlaneCorruptionError on a
        malformed claim file.
        """
        record = self._read_record(
            product=product, claim_path=self._claim_path_for(product=product, domain_id=domain)
        )
        if record is None or record.state != CLAIM_STATE_HELD:
            return None
        return record.info

    def release_claim_if_unchanged(
        self, *, product: str, claim: ClaimInfo, stale_only: bool = False
    ) -> Literal["released", "changed", "missing"]:
        """Retire a listed claim only while its record still holds that same claim.

        Re-reads the record, compares it with `claim`, then retires the generation it read.
        Returns `"missing"` when the record is gone or released, and `"changed"` when its
        owner, acquisition or incarnation differs, or the record changed before the retire.
        With `stale_only`, it also returns `"changed"` when the heartbeat moved since the
        listing or the re-read claim is fresh. Returns `"released"` after the retire.
        Raises ControlPlaneCorruptionError on a malformed claim file.
        """
        claim_path = self._claim_path_for(product=product, domain_id=claim.domain)
        record = self._read_record(product=product, claim_path=claim_path)
        if record is None or record.state != CLAIM_STATE_HELD:
            return "missing"
        current = record.info
        if (
            current.owner_id != claim.owner_id
            or current.acquired_at != claim.acquired_at
            or current.incarnation != claim.incarnation
        ):
            return "changed"
        if stale_only and (
            current.last_heartbeat_at != claim.last_heartbeat_at
            or current.revision != claim.revision
            or not current.stale
        ):
            return "changed"
        try:
            _retire(self._fs, claim_path, record.validator, current)
        except PreconditionFailed:
            return self._after_refused_retire(product=product, claim_path=claim_path)
        return "released"

    def clear_claim(self, *, product: str, domain_id: str, force: bool = False) -> bool:
        """Retire the claim record it reads; refuse a fresh claim unless `force=True`.

        Returns False when no held claim exists, or the record vanished before the retire.

        Raises:
            ClaimConflictError: The claim is fresh and `force` is false, or another writer
                changed the record between the read and the retire.
        """
        claim_path = self._claim_path_for(product=product, domain_id=domain_id)
        record = self._read_record(product=product, claim_path=claim_path)
        if record is None or record.state != CLAIM_STATE_HELD:
            return False
        info = record.info
        if not force and not info.stale:
            raise ClaimConflictError(
                Message(
                    "Refusing to clear active claim {claim_domain}; rerun with {force} to override.",
                    claim_domain=domain_id,
                    force=Param("force", True),
                    product=product,
                    claim_owner=info.owner_id,
                )
            )
        try:
            _retire(self._fs, claim_path, record.validator, info)
        except PreconditionFailed as exc:
            if self._after_refused_retire(product=product, claim_path=claim_path) == "missing":
                return False
            raise ClaimConflictError(
                Message(
                    "Claim {claim_domain} changed after it was read, so it was not cleared; "
                    "list the claims again.",
                    claim_domain=domain_id,
                    product=product,
                    claim_owner=info.owner_id,
                )
            ) from exc
        return True

    def _after_refused_retire(
        self, *, product: str, claim_path: StorageUri
    ) -> Literal["changed", "missing"]:
        """Classify a refused retire by one re-read: the record is gone or released, or changed."""
        record = self._read_record(product=product, claim_path=claim_path)
        if record is None or record.state != CLAIM_STATE_HELD:
            return "missing"
        return "changed"

    def _claim_path_for(self, *, product: str, domain_id: str) -> StorageUri:
        domain_product, category, name = domain_id.split(":", 2)
        control_path, _control_uri = self._control_root_resolver(product)
        claim_name = WriteDomain(product=domain_product, category=category, name=name).claim_name
        return control_path.join(CLAIMS_DIRNAME).join(claim_name)

    def _list_products(self) -> set[str]:
        """List the product names under the base URI; an absent base lists none."""
        products: set[str] = set()
        base_uri = self._control_root_resolver.base_uri
        try:
            entries = self._fs.ls(base_uri, detail=False)  # type: ignore[attr-defined]
        except FileNotFoundError:
            return products
        for entry in entries:
            uri = _entry_to_uri(entry, base_uri)
            parts = uri.path.rstrip("/").split("/")
            if parts and parts[-1]:
                products.add(parts[-1])
        return products

    def _held_claims(
        self, product: str, claims_dir: StorageUri, *, refresh: bool = False
    ) -> list[ClaimInfo]:
        """Read every record in `claims_dir` and return the held ones, unsorted.

        An absent directory lists as no claims; any other listing error propagates.
        """
        kwargs = {"refresh": True} if refresh else {}
        try:
            entries = self._fs.ls(claims_dir, detail=False, **kwargs)  # type: ignore[attr-defined]
        except FileNotFoundError:
            return []
        claims: list[ClaimInfo] = []
        for entry in entries:
            record = self._read_record(product=product, claim_path=_entry_to_uri(entry, claims_dir))
            if record is not None and record.state == CLAIM_STATE_HELD:
                claims.append(record.info)
        return claims

    def _read_record(self, *, product: str, claim_path: StorageUri) -> _ClaimRecord | None:
        """Read one record with its validator; return None when no record exists.

        Raises:
            ControlPlaneCorruptionError: The record cannot be read or parsed.
            ConfigurationError: The driver does not report `ConditionalWrite`.
        """
        fs = _conditional(self._fs)
        try:
            data, validator = fs.read_snapshot(claim_path)
        except FileNotFoundError:
            return None
        except OSError as exc:
            msg = f"Unable to read claim file: {claim_path.to_str()}"
            raise ControlPlaneCorruptionError(msg) from exc
        return _parse_record(
            data,
            product=product,
            claim_path=claim_path,
            validator=validator,
            default_heartbeat_s=self._heartbeat_interval_s,
            default_stale_s=self._stale_threshold_s,
        )
