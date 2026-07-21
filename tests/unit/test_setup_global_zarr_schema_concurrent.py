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

"""Concurrent-pod safety for global Zarr schema setup.

Slot-range parallelism launches several ``firecube ingest`` pods at once. Each
runs ``_setup_global_zarr_schema`` at startup. The setup is idempotent, so when
the schema already exists and matches (the ``firecube zarr preallocate``
workflow), no pod should take the exclusive ``zarr_schema_global:<group>:setup``
claim — otherwise simultaneous pods race on it and the losers fail with
``ClaimConflictError``. The exclusive claim is only legitimate when a real
mutation (array creation) is required.
"""

from __future__ import annotations

import contextlib
from types import SimpleNamespace
from typing import Any, cast

import numpy as np
import zarr

from firecube.ingestor.api import ZarrArraySpec, ZarrGroupSpec
from firecube.ingestor.templates.direct_zarr import _setup_global_zarr_schema


def _strategy(store_uri: str) -> SimpleNamespace:
    return SimpleNamespace(
        _store_uri=store_uri,
        _storage_config=None,
        _session=None,
        _coord_names_by_group={},
    )


def _schema() -> list[ZarrGroupSpec]:
    return [
        ZarrGroupSpec(
            group="data",
            arrays=[
                ZarrArraySpec(
                    name="data",
                    shape=(4, 3, 2),
                    dtype=np.float32,
                    chunks=(2, 3, 2),
                    fill_value=0.0,
                ),
            ],
        )
    ]


class _ClaimSpy:
    """Records every exclusive claim acquisition; the claim is a no-op context."""

    def __init__(self) -> None:
        self.acquired: list[str] = []

    def acquire_claim(self, *, product: str, domain: Any, owner_id: str) -> Any:
        self.acquired.append(domain.identifier)
        return contextlib.nullcontext()


def _preallocate(store_path) -> None:
    root = zarr.open_group(store=str(store_path), mode="w", zarr_format=3)
    root.require_group("data").create_array(
        "data", shape=(4, 3, 2), dtype=np.float32, chunks=(2, 3, 2), fill_value=0.0
    )


def test_setup_takes_no_claim_when_schema_already_satisfied(tmp_path):
    # Schema already created (e.g. by `firecube zarr preallocate` or a peer pod).
    store_path = tmp_path / "pre.zarr"
    _preallocate(store_path)

    spy = _ClaimSpy()
    _setup_global_zarr_schema(
        strategy=_strategy(str(store_path)),
        schema=_schema(),
        global_expected={"data": 4},
        product="product",
        run_id="run-1",
        chunk_manager=spy,
    )

    # No exclusive claim => simultaneous pods cannot race on it.
    assert spy.acquired == []


def test_setup_takes_claim_when_array_must_be_created(tmp_path):
    # Fresh store: a real mutation is required, so the exclusive claim is correct.
    store_path = tmp_path / "fresh.zarr"

    spy = _ClaimSpy()
    _setup_global_zarr_schema(
        strategy=_strategy(str(store_path)),
        schema=_schema(),
        global_expected={"data": 4},
        product="product",
        run_id="run-1",
        chunk_manager=spy,
    )

    assert spy.acquired == ["product:zarr_schema_global:data:setup"]
    arr = cast(Any, zarr.open_group(store=str(store_path), mode="r", zarr_format=3)["data/data"])
    assert arr.shape == (4, 3, 2)
