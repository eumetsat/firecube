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

"""Proof of the documented callable-payload failure mode.

The unsafe fixture emits a ``kind="static"`` intent whose ``data`` callable
closes over a file handle that was closed before ``build_write_intents``
returned. The dispatch layer invokes the callable via
``intent.data() if callable(intent.data) else intent.data`` — which raises
``ValueError`` with a ``closed file`` message (Python's ``io`` layer emits
``read of closed file`` for a closed binary read handle). That is the
documented failure mode for lifetime-contract point (2) in the fixture
module docstring; this test asserts on the exception type and message so
regressions in the callable-dispatch surface are caught loudly.
"""

from __future__ import annotations

import pytest
from callable_payload_test_plugin import (
    CallablePayloadUnsafeIngestor,
    _make_unsafe_closed_file_callable,
)


def test_unsafe_closed_file_callable_raises_valueerror_on_invocation() -> None:
    unsafe = _make_unsafe_closed_file_callable()
    with pytest.raises(ValueError, match="closed file"):
        unsafe()


def test_unsafe_ingestor_emits_a_callable_that_raises_at_dispatch_time() -> None:
    ingestor = CallablePayloadUnsafeIngestor()
    intents = ingestor.build_write_intents(batch=None, ctx=None)  # type: ignore[arg-type]

    assert len(intents) == 1
    intent = intents[0]
    assert intent.kind == "static"
    assert intent.group == "data"
    assert intent.array == "lat"
    assert callable(intent.data)

    with pytest.raises(ValueError, match="closed file"):
        intent.data()
