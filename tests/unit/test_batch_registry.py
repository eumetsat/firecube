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

from unittest.mock import MagicMock

import pytest

from firecube.core.batch_registry import BatchResourceRegistry, _IdentityRef

pytestmark = pytest.mark.unit


def test_setup_registers_and_teardown_pops_all() -> None:
    registry = BatchResourceRegistry()
    first = MagicMock()
    second = MagicMock()

    assert registry.register("batch-1", first) is first
    assert registry.register("batch-1", second) is second
    registry.teardown("batch-1")

    first.close.assert_called_once_with()
    second.close.assert_called_once_with()
    assert registry.teardown("batch-1") is None


def test_teardown_after_failure_still_closes_all() -> None:
    registry = BatchResourceRegistry()
    first = MagicMock()
    second = MagicMock()
    third = MagicMock()
    first_error = RuntimeError("first close failure")
    second.close.side_effect = first_error

    for resource in (first, second, third):
        registry.register("batch-1", resource)

    with pytest.raises(RuntimeError) as exc_info:
        registry.teardown("batch-1")

    assert exc_info.value is first_error
    first.close.assert_called_once_with()
    second.close.assert_called_once_with()
    third.close.assert_called_once_with()


def test_idempotent_teardown() -> None:
    registry = BatchResourceRegistry()
    resource = MagicMock()

    registry.register("batch-1", resource)
    registry.teardown("batch-1")
    registry.teardown("batch-1")

    resource.close.assert_called_once_with()


def test_identity_ref_distinguishes_reference_equality() -> None:
    class EqualByValue:
        def __eq__(self, other: object) -> bool:
            return isinstance(other, EqualByValue)

        def __hash__(self) -> int:
            return 1

    first = EqualByValue()
    second = EqualByValue()

    assert _IdentityRef(first) == _IdentityRef(first)
    assert _IdentityRef(first) != _IdentityRef(second)
    assert len({_IdentityRef(first), _IdentityRef(second)}) == 2


def test_teardown_all_closes_every_batch() -> None:
    registry = BatchResourceRegistry()
    first = MagicMock()
    second = MagicMock()

    registry.register("batch-1", first)
    registry.register("batch-2", second)
    registry.teardown_all()

    first.close.assert_called_once_with()
    second.close.assert_called_once_with()


def test_teardown_all_continues_after_failure_and_reraises_first() -> None:
    registry = BatchResourceRegistry()
    failing = MagicMock()
    healthy = MagicMock()
    first_error = RuntimeError("close failure")
    failing.close.side_effect = first_error

    registry.register("batch-1", failing)
    registry.register("batch-2", healthy)

    with pytest.raises(RuntimeError) as exc_info:
        registry.teardown_all()

    assert exc_info.value is first_error
    healthy.close.assert_called_once_with()
