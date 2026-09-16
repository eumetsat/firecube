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

"""OrderedWriteGate: in-order admission, fail-stop, and release-on-every-path."""

from __future__ import annotations

import concurrent.futures
import threading
import time

import pytest

from firecube.ingestor.runtime.zarr.ordered_gate import OrderedWriteGate

pytestmark = pytest.mark.unit

_TIMEOUT_S = 10.0


def test_out_of_order_arrivals_are_admitted_in_index_order() -> None:
    gate = OrderedWriteGate()
    committed: list[int] = []
    admitted: dict[int, bool] = {}
    started = threading.Barrier(4)

    def worker(index: int) -> None:
        started.wait(_TIMEOUT_S)
        with gate.turn(index) as ok:
            admitted[index] = ok
            committed.append(index)
            time.sleep(0.01)

    threads = [threading.Thread(target=worker, args=(i,)) for i in (3, 1, 2, 0)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(_TIMEOUT_S)

    assert committed == [0, 1, 2, 3]
    assert admitted == {0: True, 1: True, 2: True, 3: True}
    assert gate.next_index == 4
    assert gate.halted is False


def test_failure_inside_turn_halts_and_refuses_every_later_index() -> None:
    gate = OrderedWriteGate()

    with gate.turn(0) as ok:
        assert ok is True

    with pytest.raises(RuntimeError, match="write exploded"), gate.turn(1) as ok:
        assert ok is True
        raise RuntimeError("write exploded")

    assert gate.halted is True
    assert gate.next_index == 2

    with gate.turn(2) as ok:
        assert ok is False
    with gate.turn(3) as ok:
        assert ok is False
    # Refused turns still advance the sequence so nothing behind them stalls.
    assert gate.next_index == 4


def test_exception_releases_in_finally_so_later_turn_is_not_stalled() -> None:
    gate = OrderedWriteGate()
    reached: list[int] = []

    with pytest.raises(ValueError), gate.turn(0):
        raise ValueError("boom")

    def later() -> None:
        with gate.turn(1) as ok:
            reached.append(1 if ok else -1)

    thread = threading.Thread(target=later)
    thread.start()
    thread.join(_TIMEOUT_S)

    assert not thread.is_alive(), "turn(1) deadlocked behind a failed turn(0)"
    assert reached == [-1]


def test_forfeit_before_turn_is_applied_when_sequence_reaches_it() -> None:
    gate = OrderedWriteGate()

    # Batch 2 fails during preparation, before ever asking for its turn.
    early_failure = gate.turn(2)
    early_failure.forfeit()
    assert gate.next_index == 0
    assert gate.halted is False

    with gate.turn(0) as ok:
        assert ok is True
    with gate.turn(1) as ok:
        assert ok is True

    # Reaching index 2 consumes the recorded failure: halt, skip past it.
    assert gate.next_index == 3
    assert gate.halted is True
    with gate.turn(3) as ok:
        assert ok is False


def test_forfeit_after_normal_exit_is_a_noop() -> None:
    gate = OrderedWriteGate()
    turn = gate.turn(0)
    with turn as ok:
        assert ok is True
    turn.forfeit()

    assert gate.next_index == 1
    assert gate.halted is False


def test_double_release_of_an_index_is_refused() -> None:
    gate = OrderedWriteGate()
    with gate.turn(0):
        pass

    with pytest.raises(RuntimeError, match="already released"):
        gate.release(0, failed=False)


def test_reset_clears_halt_and_sequence() -> None:
    gate = OrderedWriteGate()
    with pytest.raises(RuntimeError), gate.turn(0):
        raise RuntimeError("boom")
    assert gate.halted is True

    gate.reset()

    assert gate.next_index == 0
    assert gate.halted is False
    with gate.turn(0) as ok:
        assert ok is True


def test_unindexed_turns_are_exclusive_and_do_not_move_the_sequence() -> None:
    gate = OrderedWriteGate()
    with gate.turn(None) as ok:
        assert ok is True
        assert gate.next_index == 0
    with gate.turn(0) as ok:
        assert ok is True
    assert gate.next_index == 1

    with pytest.raises(RuntimeError), gate.turn(None):
        raise RuntimeError("boom")
    assert gate.halted is True
    with gate.turn(None) as ok:
        assert ok is False


def test_exclusive_section_waits_for_the_active_turn_and_ignores_halt() -> None:
    gate = OrderedWriteGate()
    order: list[str] = []
    turn_entered = threading.Event()
    release_turn = threading.Event()

    def writer() -> None:
        with gate.turn(0):
            turn_entered.set()
            release_turn.wait(timeout=5)
            order.append("turn")

    worker = threading.Thread(target=writer)
    worker.start()
    assert turn_entered.wait(timeout=5)

    def bookkeeping() -> None:
        with gate.exclusive():
            order.append("exclusive")

    hook = threading.Thread(target=bookkeeping)
    hook.start()
    hook.join(timeout=0.2)
    assert hook.is_alive(), "exclusive section must wait while a turn is held"
    release_turn.set()
    worker.join(timeout=5)
    hook.join(timeout=5)
    assert order == ["turn", "exclusive"]
    assert gate.next_index == 1

    gate.halted = True
    with gate.exclusive():
        order.append("after-halt")
    assert order[-1] == "after-halt"


@pytest.mark.parametrize("failing_index", [None, 1])
def test_bounded_pool_completes_without_deadlock(failing_index: int | None) -> None:
    """FIFO pool start + release-on-every-path: no waiter waits forever."""
    gate = OrderedWriteGate()
    committed: list[int] = []
    refused: list[int] = []
    delays = {0: 0.05, 1: 0.0, 2: 0.03, 3: 0.0, 4: 0.02, 5: 0.0}

    def process(index: int) -> None:
        turn = gate.turn(index)
        try:
            time.sleep(delays[index])
            if index == failing_index:
                raise RuntimeError("prepare failed before the write turn")
            with turn as ok:
                if not ok:
                    refused.append(index)
                    return
                committed.append(index)
        finally:
            turn.forfeit()

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(process, i) for i in range(6)]
        done, pending = concurrent.futures.wait(futures, timeout=_TIMEOUT_S)

    assert not pending, "gate deadlocked"
    errors = [f.exception() for f in done if f.exception() is not None]
    if failing_index is None:
        assert not errors
        assert committed == [0, 1, 2, 3, 4, 5]
        assert refused == []
    else:
        assert len(errors) == 1
        assert committed == [0]
        assert sorted(refused) == [2, 3, 4, 5]
        assert gate.halted is True
    assert gate.next_index == 6
