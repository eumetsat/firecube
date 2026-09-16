# Copyright 2025-2026 EUMETSAT
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Failed-batch repair on the append path.

Faults are injected at the boundary ``append_time_groups`` writes through
(``write_dataset_to_zarr``) or in the caller's ``dataset_for_batch``; every
assertion is on Firecube-visible state: the raised outcome, the timestamp
state array, array lengths, store bytes, and what a real second
``append_time_groups`` run does with the repaired store.
"""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from pathlib import Path
from typing import Any, cast

import numpy as np
import pytest
import xarray as xr
import zarr
from pytest_mock import MockerFixture

from firecube.ingestor.runtime.zarr import append as append_module
from firecube.ingestor.runtime.zarr.append import append_time_groups
from firecube.ingestor.runtime.zarr.append_failure import AppendBatchFailed
from tests.helpers.storage import local_zarr_handle, make_local_session

pytestmark = pytest.mark.integration

_GROUP = "G"
_STATE = "firecube_timestamp_state"


class TailWriteError(RuntimeError):
    """Injected failure for the append tail write."""


class DatasetBuildError(RuntimeError):
    """Injected failure for ``dataset_for_batch``."""


def _days(*days: int) -> list[np.datetime64]:
    return [np.datetime64(f"2024-01-{day:02d}T00:00:00", "s") for day in days]


def _values(timestamps: list[np.datetime64], base: float = 0.0) -> dict[np.datetime64, float]:
    return {ts: base + index for index, ts in enumerate(timestamps)}


def _dataset_for(values_by_time: dict[np.datetime64, float]):
    def _dataset(_group: str, batch_ts: Sequence[Any]) -> xr.Dataset | None:
        timestamps = np.asarray([np.datetime64(item, "s") for item in batch_ts])
        values = np.asarray([values_by_time[np.datetime64(item, "s")] for item in batch_ts])
        return xr.Dataset(
            {"value": (("timestamp",), values.astype(np.float32))},
            coords={"timestamp": timestamps},
        )

    return _dataset


def _append(
    store: Path,
    group_to_timestamps: dict[str, list[np.datetime64]],
    dataset_for_batch: Any,
    *,
    resume_existing: bool = False,
    force_reingest: bool = False,
) -> dict[str, Any]:
    batch_size = max(len(ts) for ts in group_to_timestamps.values())
    return append_time_groups(
        store=str(store),
        zarr_store=local_zarr_handle(store),
        session=make_local_session(str(store)),
        group_to_timestamps=group_to_timestamps,
        dataset_for_batch=dataset_for_batch,
        arrays_for_group=lambda group: [f"{group}/value"],
        chunk_shape={"timestamp": 2},
        resume_existing=resume_existing,
        force_reingest=force_reingest,
        batch_size=batch_size,
    )


def _seed(store: Path, groups: Sequence[str] = (_GROUP,)) -> None:
    timestamps = _days(1, 2, 3, 4, 5)
    _append(store, dict.fromkeys(groups, timestamps), _dataset_for(_values(timestamps)))


def _open_root(store: Path) -> Any:
    return zarr.open_group(str(store), mode="r", use_consolidated=False)


def _state(store: Path, group: str = _GROUP) -> list[int]:
    return [int(v) for v in _open_root(store)[group][_STATE][:]]


def _lengths(store: Path, group: str = _GROUP) -> dict[str, int]:
    zarr_group = _open_root(store)[group]
    return {name: int(zarr_group[name].shape[0]) for name in sorted(zarr_group.array_keys())}


def _values_on_disk(store: Path, group: str = _GROUP) -> list[float]:
    return [float(v) for v in _open_root(store)[group]["value"][:]]


def _store_digest(store: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(item for item in store.rglob("*") if item.is_file()):
        digest.update(str(path.relative_to(store)).encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()


def _is_tail_write(kwargs: dict[str, Any]) -> bool:
    return kwargs.get("mode") == "a" and kwargs.get("region") is None


def _patch_tail_failure(mocker: MockerFixture, *, only_group: str | None = None) -> None:
    real_write = append_module.write_dataset_to_zarr

    def _write_or_fail(ds: xr.Dataset, **kwargs: Any) -> None:
        if _is_tail_write(kwargs) and (only_group is None or kwargs.get("group") == only_group):
            raise TailWriteError("injected tail append failure")
        real_write(ds, **kwargs)

    mocker.patch.object(append_module, "write_dataset_to_zarr", side_effect=_write_or_fail)


def _patch_partial_tail(mocker: MockerFixture, store: Path, *, extra: int) -> None:
    """Extend only the primary array (as a torn append would) then raise."""

    real_write = append_module.write_dataset_to_zarr

    def _write_or_tear(ds: xr.Dataset, **kwargs: Any) -> None:
        if not _is_tail_write(kwargs):
            real_write(ds, **kwargs)
            return
        root = cast(Any, zarr.open_group(str(store), mode="r+", use_consolidated=False))
        value = root[str(kwargs["group"])]["value"]
        current = int(value.shape[0])
        value.resize((current + extra,))
        value[current:] = np.float32(42.0)
        raise TailWriteError("injected failure after the primary array grew")

    mocker.patch.object(append_module, "write_dataset_to_zarr", side_effect=_write_or_tear)


@pytest.mark.parametrize(
    "replay_flag",
    [False, True],
    ids=["resume_existing", "force_reingest"],
)
def test_split_batch_tail_failure_marks_region_state_3_and_resume_refills(
    tmp_path: Path, mocker: MockerFixture, replay_flag: bool
) -> None:
    """(a) Region written, tail raises: region slots carry state 3, no truncation.

    The next run refills the region and appends the tail as one coverage entry
    with both ranges, whether the replay uses ``resume_existing=True`` or
    ``force_reingest=True``.
    """
    store = tmp_path / "split-tail.zarr"
    _seed(store)
    incoming = _days(1, 2, 10)
    values = {incoming[0]: 100.0, incoming[1]: 101.0, incoming[2]: 110.0}
    _patch_tail_failure(mocker)

    with pytest.raises(AppendBatchFailed) as excinfo:
        _append(store, {_GROUP: incoming}, _dataset_for(values), force_reingest=True)

    outcome = excinfo.value.outcome
    assert isinstance(excinfo.value.__cause__, TailWriteError)
    assert "injected tail append failure" in str(excinfo.value)
    assert outcome.failed_group == _GROUP
    assert outcome.committed == []
    assert outcome.not_attempted_groups == []
    assert outcome.failed_entry is not None
    assert outcome.failed_entry["time_index_ranges"] == [[0, 1]]
    assert outcome.failed_entry["write_strategy"] == "append_failed"
    assert outcome.failed_entry["arrays"] == [f"{_GROUP}/value"]
    assert outcome.failed_entry["time_min"] == "2024-01-01T00:00:00"
    assert outcome.failed_entry["time_max"] == "2024-01-02T00:00:00"
    assert outcome.repair.state_marked_ranges == [[0, 1]]
    assert outcome.repair.truncated_to is None
    assert outcome.repair.group_removed is False
    assert outcome.repair.error is None

    assert _state(store) == [3, 3, 1, 1, 1]
    assert _lengths(store) == {"firecube_timestamp_state": 5, "timestamp": 5, "value": 5}
    assert _values_on_disk(store) == [100.0, 101.0, 2.0, 3.0, 4.0]

    mocker.stopall()
    replay_kwargs: dict[str, bool] = (
        {"force_reingest": True} if replay_flag else {"resume_existing": True}
    )
    metrics = _append(store, {_GROUP: incoming}, _dataset_for(values), **replay_kwargs)

    assert [entry["time_index_ranges"] for entry in metrics["coverage"]] == [[[0, 1], [5, 5]]]
    assert metrics["timestamps_skipped"] == 0
    assert _state(store) == [1, 1, 1, 1, 1, 1]
    assert _values_on_disk(store) == [100.0, 101.0, 2.0, 3.0, 4.0, 110.0]
    ds = xr.open_zarr(str(store), group=_GROUP, consolidated=False)
    try:
        np.testing.assert_array_equal(ds["timestamp"].values, np.asarray(_days(1, 2, 3, 4, 5, 10)))
    finally:
        ds.close()


def test_torn_tail_append_truncates_every_array_back_to_the_cursor(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    """(b) A tail append that grew the primary array before raising is undone."""
    store = tmp_path / "torn-tail.zarr"
    _seed(store)
    incoming = _days(6, 7, 8)
    values = _values(incoming, base=200.0)
    _patch_partial_tail(mocker, store, extra=3)

    with pytest.raises(AppendBatchFailed) as excinfo:
        _append(store, {_GROUP: incoming}, _dataset_for(values))

    outcome = excinfo.value.outcome
    assert outcome.failed_entry is None
    assert outcome.repair.truncated_to == 5
    assert outcome.repair.state_marked_ranges == []
    assert outcome.repair.error is None
    assert _lengths(store) == {"firecube_timestamp_state": 5, "timestamp": 5, "value": 5}
    assert _values_on_disk(store) == [0.0, 1.0, 2.0, 3.0, 4.0]
    assert _state(store) == [1, 1, 1, 1, 1]

    mocker.stopall()
    metrics = _append(store, {_GROUP: incoming}, _dataset_for(values), resume_existing=True)

    assert [entry["time_index_ranges"] for entry in metrics["coverage"]] == [[[5, 7]]]
    assert _values_on_disk(store) == [0.0, 1.0, 2.0, 3.0, 4.0, 200.0, 201.0, 202.0]
    assert _state(store) == [1] * 8


def test_dataset_failure_before_any_write_leaves_store_untouched_and_error_typed(
    tmp_path: Path,
) -> None:
    """(c) ``dataset_for_batch`` raising for the only group touches nothing.

    Nothing reached the store, so the caller's own exception propagates (the
    typed contract of pre-write refusals) and the store is byte-identical.
    """
    store = tmp_path / "dataset-failure.zarr"
    _seed(store)
    before = _store_digest(store)

    def _raise(_group: str, _batch: Sequence[Any]) -> xr.Dataset | None:
        raise DatasetBuildError("cannot decode input")

    with pytest.raises(DatasetBuildError, match="cannot decode input"):
        _append(store, {_GROUP: _days(6, 7)}, _raise)

    assert _store_digest(store) == before


def test_dataset_failure_in_second_group_reports_first_group_committed(
    tmp_path: Path,
) -> None:
    """(c) ``dataset_for_batch`` raising after a group committed: no failed entry.

    The first group's entry must reach the WAL even though the batch failed,
    so the outcome carries it; the failed group has nothing to repair.
    """
    store = tmp_path / "dataset-failure-second-group.zarr"
    timestamps = _days(1, 2, 3)
    good = _dataset_for(_values(timestamps))

    def _dataset(group: str, batch: Sequence[Any]) -> xr.Dataset | None:
        if group == "B":
            raise DatasetBuildError("cannot decode input for B")
        return good(group, batch)

    with pytest.raises(AppendBatchFailed) as excinfo:
        _append(store, {"A": timestamps, "B": timestamps}, _dataset)

    outcome = excinfo.value.outcome
    assert isinstance(excinfo.value.__cause__, DatasetBuildError)
    assert outcome.failed_group == "B"
    assert outcome.failed_entry is None
    assert [entry["group"] for entry in outcome.committed] == ["A"]
    assert outcome.committed[0]["time_index_ranges"] == [[0, 2]]
    assert outcome.repair.truncated_to is None
    assert outcome.repair.group_removed is False
    root = _open_root(store)
    assert "A" in root
    assert "B" not in root
    assert _state(store, "A") == [1, 1, 1]


def test_second_group_failure_keeps_first_group_committed_and_names_not_attempted(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    """(d) Two groups written, the second raises: the first stays committed."""
    store = tmp_path / "two-groups.zarr"
    _seed(store, groups=("A", "B", "C"))
    incoming = _days(6, 7)
    values = _values(incoming, base=300.0)
    _patch_tail_failure(mocker, only_group="B")

    with pytest.raises(AppendBatchFailed) as excinfo:
        _append(store, {"A": incoming, "B": incoming, "C": incoming}, _dataset_for(values))

    outcome = excinfo.value.outcome
    assert outcome.failed_group == "B"
    assert [entry["group"] for entry in outcome.committed] == ["A"]
    assert outcome.committed[0]["time_index_ranges"] == [[5, 6]]
    assert outcome.not_attempted_groups == ["C"]
    assert outcome.counters["batch_processing"]["batches_written"] == 1
    assert _lengths(store, "A")["value"] == 7
    assert _values_on_disk(store, "A")[5:] == [300.0, 301.0]
    assert _lengths(store, "B")["value"] == 5
    assert _lengths(store, "C")["value"] == 5

    mocker.stopall()
    metrics = _append(
        store,
        {"A": incoming, "B": incoming, "C": incoming},
        _dataset_for(values),
        resume_existing=True,
    )

    assert {entry["group"]: entry["time_index_ranges"] for entry in metrics["coverage"]} == {
        "B": [[5, 6]],
        "C": [[5, 6]],
    }
    assert metrics["timestamps_skipped"] == 2
    assert all(_lengths(store, group)["value"] == 7 for group in ("A", "B", "C"))


def test_fresh_group_first_write_failure_removes_the_group(
    tmp_path: Path, mocker: MockerFixture
) -> None:
    """(e) A fresh group whose first write raises is deleted, not left half-built."""
    store = tmp_path / "fresh-group.zarr"
    _seed(store, groups=("existing",))
    timestamps = _days(1, 2, 3)
    real_write = append_module.write_dataset_to_zarr

    def _write_then_fail(ds: xr.Dataset, **kwargs: Any) -> None:
        real_write(ds, **kwargs)
        if kwargs.get("group") == _GROUP:
            raise TailWriteError("injected failure after the fresh write landed")

    mocker.patch.object(append_module, "write_dataset_to_zarr", side_effect=_write_then_fail)

    with pytest.raises(AppendBatchFailed) as excinfo:
        _append(store, {_GROUP: timestamps}, _dataset_for(_values(timestamps)))

    outcome = excinfo.value.outcome
    assert outcome.failed_entry is None
    assert outcome.repair.group_removed is True
    assert outcome.repair.truncated_to is None
    root = _open_root(store)
    assert _GROUP not in root
    assert "existing" in root
    assert _lengths(store, "existing")["value"] == 5

    mocker.stopall()
    metrics = _append(store, {_GROUP: timestamps}, _dataset_for(_values(timestamps)))

    assert [entry["time_index_ranges"] for entry in metrics["coverage"]] == [[[0, 2]]]
    assert _state(store) == [1, 1, 1]
