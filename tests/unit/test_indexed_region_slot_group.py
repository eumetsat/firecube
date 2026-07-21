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

import logging
from pathlib import Path

import pytest

from firecube.ingestor.errors import WriteIntentRangeError
from firecube.ingestor.runtime.zarr.strategies.indexed_region import IndexedRegionStrategy
from firecube.ingestor.templates.direct_zarr import WriteIntent

pytestmark = pytest.mark.unit


class _Writer:
    def ensure_group(self, group: str, **kwargs) -> None:
        _ = (group, kwargs)


def _intent(group: str, ts_index: int) -> WriteIntent:
    return WriteIntent(
        group=group,
        array="values",
        ts_index=ts_index,
        data=None,
        y_slice=slice(0, 1),
    )


def _patch_writer(
    monkeypatch: pytest.MonkeyPatch,
    dispatched: list[tuple[str, int]],
) -> None:
    monkeypatch.setattr(
        "firecube.ingestor.runtime.zarr.strategies.indexed_region.RegionZarrWriter",
        lambda *args, **kwargs: _Writer(),
    )
    monkeypatch.setattr(
        IndexedRegionStrategy,
        "_dispatch_intent",
        staticmethod(lambda writer, intent: dispatched.append((intent.group, intent.ts_index))),
    )


def test_slot_group_set_validates_only_that_groups_intents(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Intents for groups other than slot_group must NOT be validated.

    Otherwise a stray intent for group 'B' (which this pod doesn't own) would
    falsely trip the slot-range assertion meant for group 'A'.
    """
    dispatched: list[tuple[str, int]] = []
    _patch_writer(monkeypatch, dispatched)

    IndexedRegionStrategy(store_uri=str(tmp_path / "test.zarr")).write_groups(
        group_to_intents={
            "A": [_intent("A", 5)],
            "B": [_intent("B", 9999)],
        },
        slot_range=(0, 100),
        slot_group="A",
    )

    assert dispatched == [("A", 5)]


def test_slot_group_set_skips_writes_for_other_groups(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    dispatched: list[tuple[str, int]] = []
    _patch_writer(monkeypatch, dispatched)

    with caplog.at_level(
        logging.WARNING,
        logger="firecube.runtime.zarr.strategies.indexed_region",
    ):
        IndexedRegionStrategy(store_uri=str(tmp_path / "test.zarr")).write_groups(
            group_to_intents={
                "A": [_intent("A", 5), _intent("A", 6)],
                "B": [_intent("B", 7)],
            },
            slot_range=(0, 100),
            slot_group="A",
        )

    assert ("B", 7) not in dispatched
    assert sorted(dispatched) == [("A", 5), ("A", 6)]
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any("slot_group" in r.getMessage() and "'B'" in r.getMessage() for r in warnings)


def test_slot_group_none_validates_all_groups_intents(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Phase 3 regression: when slot_group is None, every group is validated."""
    dispatched: list[tuple[str, int]] = []
    _patch_writer(monkeypatch, dispatched)

    with pytest.raises(WriteIntentRangeError, match=r"group='B'.*slot_range=\[0, 100\)"):
        IndexedRegionStrategy(store_uri=str(tmp_path / "test.zarr")).write_groups(
            group_to_intents={
                "A": [_intent("A", 5)],
                "B": [_intent("B", 9999)],
            },
            slot_range=(0, 100),
            slot_group=None,
        )

    assert dispatched == []


def test_slot_range_none_skips_assertion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """slot_range=None disables the entire assertion path regardless of slot_group."""
    dispatched: list[tuple[str, int]] = []
    _patch_writer(monkeypatch, dispatched)

    IndexedRegionStrategy(store_uri=str(tmp_path / "test.zarr")).write_groups(
        group_to_intents={
            "A": [_intent("A", 5)],
            "B": [_intent("B", 9999)],
        },
        slot_range=None,
        slot_group="A",
    )

    assert sorted(dispatched) == [("A", 5), ("B", 9999)]


def test_slot_group_with_zero_intents_for_that_group_is_noop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    dispatched: list[tuple[str, int]] = []
    _patch_writer(monkeypatch, dispatched)

    with caplog.at_level(
        logging.WARNING,
        logger="firecube.runtime.zarr.strategies.indexed_region",
    ):
        IndexedRegionStrategy(store_uri=str(tmp_path / "test.zarr")).write_groups(
            group_to_intents={
                "A": [],
                "B": [_intent("B", 5)],
            },
            slot_range=(0, 100),
            slot_group="A",
        )

    assert dispatched == []
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any("'B'" in r.getMessage() for r in warnings)


def test_slot_group_set_out_of_range_intent_for_owned_group_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_writer(*args, **kwargs):
        raise AssertionError("writer must not be constructed")

    monkeypatch.setattr(
        "firecube.ingestor.runtime.zarr.strategies.indexed_region.RegionZarrWriter",
        fail_writer,
    )

    with pytest.raises(WriteIntentRangeError, match=r"group='A'.*slot_range=\[0, 5\)"):
        IndexedRegionStrategy(store_uri=str(tmp_path / "test.zarr")).write_groups(
            group_to_intents={
                "A": [_intent("A", 99)],
                "B": [_intent("B", 7)],
            },
            slot_range=(0, 5),
            slot_group="A",
        )
