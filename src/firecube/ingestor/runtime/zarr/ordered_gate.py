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

"""Ordered write gate (turnstile) for append-style Zarr hosts.

The append path must commit batches in planner order so the time axis stays
monotonic when ``pipeline_workers > 1``. A plain lock serialises writes but
admits them in completion order. The gate replaces it: a batch is admitted
only when every lower ``batch_index`` has released, and once a batch fails
no later batch is admitted (fail-stop), so nothing appends past a gap.

The gate is host-owned synchronisation, like the lock it replaces. It is not
a scheduler: the thread pool still decides what runs; the gate only decides
when a running batch may write.
"""

from __future__ import annotations

import threading

_UNORDERED = object()
"""Holder marker for a batch that carries no planner index."""


class OrderedWriteGate:
    """Admit batch writes strictly in ``batch_index`` order, halting on failure.

    Deadlock freedom rests on two facts. First, ``ThreadPoolExecutor`` starts
    futures FIFO in submission order, and batches are submitted in planner
    order, so at any moment the lowest unfinished index is running (or done),
    never stuck behind an unstarted one. Second, every index is released
    exactly once, whether it was admitted, refused because the gate halted,
    or failed before ever asking for its turn (``release`` out of turn is
    recorded and applied when the sequence reaches it). Together these mean
    ``next_index`` always advances to the lowest running batch, which is
    admitted, so no waiter can wait forever. Callers must therefore release
    in a ``finally`` on every path; :meth:`turn` does that.

    Batches without a planner index (direct ``_process_batch`` calls in
    tests) pass ``None`` and are admitted in arrival order, mutually
    exclusive with indexed writers, without moving ``next_index``.

    Attributes:
        next_index: The batch index that will be admitted next.
        halted: ``True`` once a released batch reported failure; every later
            admission request is refused.
    """

    def __init__(self) -> None:
        self._cond = threading.Condition()
        self.next_index = 0
        self.halted = False
        self._holder: object | None = None
        self._released_ahead: dict[int, bool] = {}

    def reset(self) -> None:
        """Return the gate to its initial state before a new run starts.

        Called from the host's ``on_pipeline_start`` on the main thread, before
        any worker exists, so no waiter can be affected.
        """
        with self._cond:
            self.next_index = 0
            self.halted = False
            self._holder = None
            self._released_ahead.clear()
            self._cond.notify_all()

    def acquire(self, index: int | None) -> bool:
        """Block until ``index`` is next in line, then report admission.

        Args:
            index: The batch's planner position, or ``None`` for a batch with
                no planner index (admitted when no other writer is active).

        Returns:
            ``True`` when the caller may write. ``False`` when the gate halted
            after an earlier failure; the caller must still call
            :meth:`release` so the sequence keeps moving.
        """
        with self._cond:
            if index is None:
                self._cond.wait_for(lambda: self._holder is None)
                if self.halted:
                    return False
                self._holder = _UNORDERED
                return True

            self._cond.wait_for(lambda: index == self.next_index and self._holder is None)
            if self.halted:
                return False
            self._holder = index
            return True

    def release(self, index: int | None, *, failed: bool) -> None:
        """Release ``index`` and advance the sequence.

        Args:
            index: The batch's planner position, or ``None`` for an unindexed
                batch. An index that is not yet next in line is recorded and
                consumed when the sequence reaches it, so a batch that failed
                before its turn never stalls later batches.
            failed: ``True`` when the batch did not succeed; halts the gate
                once the sequence reaches this index.

        Raises:
            RuntimeError: If ``index`` was already released.
        """
        with self._cond:
            if index is None:
                if self._holder is _UNORDERED:
                    self._holder = None
                if failed:
                    self.halted = True
            elif index == self.next_index:
                if self._holder == index:
                    self._holder = None
                if failed:
                    self.halted = True
                self.next_index = index + 1
                self._drain_released_ahead()
            elif index > self.next_index:
                self._released_ahead[index] = failed
            else:
                raise RuntimeError(
                    f"batch index {index} already released (next_index={self.next_index})"
                )
            self._cond.notify_all()

    def _drain_released_ahead(self) -> None:
        while self.next_index in self._released_ahead:
            if self._released_ahead.pop(self.next_index):
                self.halted = True
            self.next_index += 1

    def exclusive(self) -> ExclusiveSection:
        """Return a context manager for store access outside a batch turn.

        Host hooks that touch the store from the main thread (for example
        ``on_batch_success`` bookkeeping) must not overlap a worker's write
        turn. The section waits until no writer holds the gate, takes it
        without moving ``next_index``, and ignores the halt flag: bookkeeping
        after a failure is still allowed.
        """
        return ExclusiveSection(self)

    def _acquire_exclusive(self) -> None:
        with self._cond:
            self._cond.wait_for(lambda: self._holder is None)
            self._holder = _UNORDERED

    def _release_exclusive(self) -> None:
        with self._cond:
            if self._holder is _UNORDERED:
                self._holder = None
            self._cond.notify_all()

    def turn(self, index: int | None) -> WriteTurn:
        """Return a context manager that holds this batch's turn at the gate.

        Args:
            index: The batch's planner position, or ``None``.

        Returns:
            A :class:`WriteTurn` that acquires on enter, yields the admission
            flag, and always releases on exit.
        """
        return WriteTurn(self, index)


class WriteTurn:
    """One batch's turn at an :class:`OrderedWriteGate`.

    Entering the context blocks until the batch is next in line and yields
    the admission flag; leaving it releases the index, reporting failure
    when the block raised. :meth:`forfeit` covers the path where the batch
    failed before ever entering (for example during data preparation): it
    releases the index as failed so later batches are neither stalled nor
    admitted past the gap. Calling it after a normal exit is a no-op.

    Args:
        gate: The gate this turn belongs to.
        index: The batch's planner position, or ``None``.
    """

    def __init__(self, gate: OrderedWriteGate, index: int | None) -> None:
        self._gate = gate
        self._index = index
        self._entered = False
        self._admitted = False
        self._released = False

    def __enter__(self) -> bool:
        self._entered = True
        self._admitted = self._gate.acquire(self._index)
        return self._admitted

    def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
        self._finish(failed=self._admitted and exc_type is not None)
        return False

    def forfeit(self) -> None:
        """Release the index as failed if the turn was never entered."""
        if not self._entered:
            self._finish(failed=True)

    def _finish(self, *, failed: bool) -> None:
        if self._released:
            return
        self._released = True
        self._gate.release(self._index, failed=failed)


class ExclusiveSection:
    """Mutual exclusion with batch write turns, without an index.

    Args:
        gate: The gate whose writer slot is taken while the block runs.
    """

    def __init__(self, gate: OrderedWriteGate) -> None:
        self._gate = gate

    def __enter__(self) -> None:
        self._gate._acquire_exclusive()

    def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
        self._gate._release_exclusive()
        return False


__all__ = ["ExclusiveSection", "OrderedWriteGate", "WriteTurn"]
