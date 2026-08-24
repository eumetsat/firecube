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

"""Fixture plugins exercising callable ``WriteIntent.data`` payloads.

The dispatch layer (``IndexedRegionStrategy._dispatch_intent`` and
``_dispatch_static_intent``) resolves ``WriteIntent.data`` exactly once at
write time via ``intent.data() if callable(intent.data) else intent.data``.
That contract lets a plugin defer materialization of large arrays until the
last possible moment, but every callable must remain valid until dispatch.

Lifetime contract for callable payloads (three points):

1. **Callables MUST close over stable inputs**: paths (as strings),
   configuration values, or immutable references such as module-level numpy
   constants. These outlive ``build_write_intents`` and stay valid until
   dispatch resolves them.
2. **Callables MUST NOT close over open file handles, DuckDB connections,
   xarray ``Dataset`` handles, or any per-batch scratch object** whose
   lifetime does not extend to dispatch. If the scratch object is closed
   or garbage-collected before dispatch, invocation raises loudly.
3. **If a plugin needs per-batch scratch, use file-backed stable sources**:
   write the scratch bytes to a stable path, close the writer, and *read
   from that path inside the callable*. The callable then only closes over
   an immutable path string; the file itself is the durable source.

``CallablePayloadSafeIngestor`` illustrates point (1): its callables close
over module-level numpy constants that outlive dispatch.

``CallablePayloadUnsafeIngestor`` illustrates the violation of point (2): it
opens a temporary file, closes the handle before returning the intent, then
invocation of the callable raises ``ValueError`` with a ``closed file``
message (Python's ``io`` layer emits ``read of closed file`` for a closed
binary read handle). That is the documented failure mode; the two tests in
``tests/fixtures/callable_payload_test_plugin/tests/`` assert both.
"""

from __future__ import annotations

import tempfile
from collections.abc import Callable, Iterable
from typing import Any, ClassVar

import numpy as np

from firecube.ingestor.api import (
    DirectZarrIngestor,
    PipelineBatch,
    PluginContext,
    WriteIntent,
    ZarrArraySpec,
    ZarrGroupSpec,
    register_ingestor,
)

# Stable module-level constants: safe to close over from callables.
# These are immutable references whose lifetime spans the entire process,
# so they remain valid at dispatch time no matter when build_write_intents
# returned.
_SAFE_REGION_DATA: np.ndarray = np.full((4, 4), 42.0, dtype=np.float32)
_SAFE_STATIC_DATA: np.ndarray = np.arange(4, dtype=np.float64) * 10.0


@register_ingestor("callable_payload_safe")
class CallablePayloadSafeIngestor(DirectZarrIngestor):
    """DirectZarr fixture emitting callable payloads with stable closures.

    Emits one ``kind="region"`` intent and one ``kind="static"`` intent per
    batch. Both intents carry a ``data`` callable that closes over a
    module-level numpy constant, satisfying the lifetime contract.
    """

    PRODUCT_NAME: ClassVar[str] = "callable_payload_safe"

    def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
        # One synthetic item — corresponds to one time slot.
        return [0]

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [
            ZarrGroupSpec(
                group="data",
                arrays=[
                    ZarrArraySpec(
                        name="values",
                        shape=(1, 4, 4),
                        chunks=(1, 4, 4),
                        dtype="float32",
                        fill_value=0.0,
                        expected_time_count=1,
                        time_indexed=True,
                        dimension_names=("timestamp", "y", "x"),
                    ),
                    ZarrArraySpec(
                        name="lat",
                        shape=(4,),
                        chunks=(4,),
                        dtype="float64",
                        fill_value=0.0,
                        time_indexed=False,
                        dimension_names=("y",),
                    ),
                ],
            )
        ]

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        # SAFE: each closure captures a module-level ndarray reference.
        # _SAFE_REGION_DATA and _SAFE_STATIC_DATA outlive this call and
        # remain valid at dispatch time.
        def load_region() -> np.ndarray:
            return _SAFE_REGION_DATA

        def load_static() -> np.ndarray:
            return _SAFE_STATIC_DATA

        return [
            WriteIntent.region(
                group="data",
                array="values",
                index=0,
                data=load_region,
                y_slice=slice(0, 4),
            ),
            WriteIntent.static(
                group="data",
                array="lat",
                data=load_static,
            ),
        ]


def _make_unsafe_closed_file_callable() -> Callable[[], np.ndarray]:
    """Return a callable that violates the lifetime contract.

    Writes payload bytes to a stable path, opens the file for reading,
    closes the handle, then returns a closure that tries to read from the
    closed handle. Invoking the returned callable raises
    ``ValueError: read of closed file`` (from Python's ``io`` layer;
    matches ``closed file``). This is the documented failure mode for
    point (2) of the lifetime contract in the module docstring.

    The file itself is left on disk so that the failure is unambiguously
    about the *handle*, not a missing file — reopening the path inside
    the callable would have satisfied the contract.
    """
    path = tempfile.mkstemp(suffix=".bin")[1]
    with open(path, "wb") as writer:
        writer.write((np.arange(4, dtype=np.float64) * 100.0).tobytes())

    fh = open(path, "rb")  # noqa: SIM115 — the closed-handle pattern IS the demo
    fh.close()  # Handle closed BEFORE the callable is invoked at dispatch.

    def read_from_closed_handle() -> np.ndarray:
        # `fh` is captured but closed; fh.read() raises ValueError.
        buf = fh.read(32)
        return np.frombuffer(buf, dtype=np.float64)

    return read_from_closed_handle


@register_ingestor("callable_payload_unsafe")
class CallablePayloadUnsafeIngestor(DirectZarrIngestor):
    """DirectZarr fixture demonstrating the documented callable failure mode.

    Emits a single ``kind="static"`` intent whose callable closes over an
    already-closed file handle. Invoking the callable at dispatch time
    raises ``ValueError: I/O operation on closed file``.
    """

    PRODUCT_NAME: ClassVar[str] = "callable_payload_unsafe"

    def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
        return [0]

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [
            ZarrGroupSpec(
                group="data",
                arrays=[
                    ZarrArraySpec(
                        name="lat",
                        shape=(4,),
                        chunks=(4,),
                        dtype="float64",
                        fill_value=0.0,
                        time_indexed=False,
                        dimension_names=("y",),
                    ),
                ],
            )
        ]

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        return [
            WriteIntent.static(
                group="data",
                array="lat",
                data=_make_unsafe_closed_file_callable(),
            ),
        ]
