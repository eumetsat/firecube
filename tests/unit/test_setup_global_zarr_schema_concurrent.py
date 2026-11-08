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
import pytest
import zarr

from firecube.core.zarr.guard.metadata_authority import AuthorityBasis, MetadataAuthority
from firecube.core.zarr.region_writer import RegionZarrWriter
from firecube.ingestor.templates.direct_zarr import _setup_global_zarr_schema
from firecube.sdk import ZarrArraySpec, ZarrGroupSpec


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

    def acquire_claim(
        self, *, product: str, domain: Any, owner_id: str, describe_conflict: bool = True
    ) -> Any:
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


class _RecordingClaim:
    """A claim that records when it is entered and left."""

    def __init__(self, events: list[str]) -> None:
        self._events = events

    def __enter__(self) -> None:
        self._events.append("claim-in")

    def __exit__(self, *_exc: object) -> None:
        self._events.append("claim-out")


def _spy_metadata_writes(
    monkeypatch: Any, events: list[str]
) -> list[tuple[str, Any, bool, list[str]]]:
    """Record the authority each writer metadata call receives, and its state then."""
    seen: list[tuple[str, Any, bool, list[str]]] = []
    for name in ("ensure_group", "set_group_attrs"):
        original = getattr(RegionZarrWriter, name)

        def spy(self: Any, *args: Any, _original: Any = original, _name: str = name, **kw: Any):
            authority = kw.get("authority")
            active = isinstance(authority, MetadataAuthority) and authority.active
            seen.append((_name, authority, active, list(events)))
            return _original(self, *args, **kw)

        monkeypatch.setattr(RegionZarrWriter, name, spy)
    return seen


def _schema_with_group_attrs() -> list[ZarrGroupSpec]:
    return [
        ZarrGroupSpec(group="data", arrays=_schema()[0].arrays, attrs={"title": "setup"}),
    ]


@pytest.mark.parametrize("control_plane", ["fake", "none"])
def test_setup_changes_metadata_under_the_authority_naming_its_basis(
    tmp_path, monkeypatch, control_plane
):
    store_path = tmp_path / "fresh.zarr"
    events: list[str] = []

    class _Manager:
        def acquire_claim(
            self, *, product: str, domain: Any, owner_id: str, describe_conflict: bool = True
        ) -> Any:
            events.append(f"acquire {domain.identifier} {owner_id}")
            return _RecordingClaim(events)

    seen = _spy_metadata_writes(monkeypatch, events)
    _setup_global_zarr_schema(
        strategy=_strategy(str(store_path)),
        schema=_schema_with_group_attrs(),
        global_expected={"data": 4},
        product="product",
        run_id="run-1",
        chunk_manager=_Manager() if control_plane == "fake" else None,
    )

    assert [name for name, *_ in seen] == ["ensure_group", "set_group_attrs"]
    for name, authority, active, before in seen:
        assert isinstance(authority, MetadataAuthority), f"{name} ran without an authority"
        assert active
        if control_plane == "fake":
            assert authority.basis is AuthorityBasis.CLAIM
            assert authority.claim_domain == "product:zarr_schema_global:data:setup"
            assert authority.owner_id == "run-1:data:schema_global"
            assert authority.scope.prefix == "data"
            assert before[-1] == "claim-in", f"{name} ran outside the setup claim"
            assert not authority.active  # released with the claim
        else:
            assert authority.basis is AuthorityBasis.PROCESS_LOCAL
            assert authority.claim_domain is None
    if control_plane == "fake":
        assert events == [
            "acquire product:zarr_schema_global:data:setup run-1:data:schema_global",
            "claim-in",
            "claim-out",
        ]
    root = zarr.open_group(store=str(store_path), mode="r", zarr_format=3)
    assert cast(Any, root["data/data"]).shape == (4, 3, 2)
    assert dict(cast(Any, root["data"]).attrs) == {"title": "setup"}
