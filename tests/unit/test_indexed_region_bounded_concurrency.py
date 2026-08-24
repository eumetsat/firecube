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

import threading
from collections.abc import Callable
from concurrent.futures import Future
from pathlib import Path
from typing import Any, ClassVar, cast

import numpy as np
import pytest
import zarr

from firecube.core.zarr.region_writer import RegionZarrWriter
from firecube.ingestor.runtime.coverage import CoverageTracker
from firecube.ingestor.runtime.zarr.strategies import indexed_region as indexed_region_module
from firecube.ingestor.runtime.zarr.strategies.indexed_region import IndexedRegionStrategy
from firecube.ingestor.templates.direct_zarr import WriteIntent, ZarrArraySpec, ZarrGroupSpec

pytestmark = [pytest.mark.unit, pytest.mark.concurrency]


def _schema(
    *,
    shape: tuple[int, ...] = (1, 4, 4),
    chunks: tuple[int, ...] = (1, 2, 4),
    shards: tuple[int, ...] | None = None,
    extra_arrays: list[ZarrArraySpec] | None = None,
) -> list[ZarrGroupSpec]:
    arrays = [
        ZarrArraySpec(
            name="values",
            shape=shape,
            dtype=np.float32,
            chunks=chunks,
            fill_value=np.nan,
            shards=shards,
        )
    ]
    if extra_arrays:
        arrays.extend(extra_arrays)
    return [ZarrGroupSpec(group="data", arrays=arrays, coord_names=frozenset())]


def _region(
    *,
    index: int = 0,
    y_slice: slice = slice(0, 2),
    data: Any | None = None,
    array: str = "values",
) -> WriteIntent:
    if data is None:
        data = np.ones((y_slice.stop - y_slice.start, 4), dtype=np.float32)
    return WriteIntent.region(
        group="data",
        array=array,
        index=index,
        data=data,
        y_slice=y_slice,
    )


def _stable_metrics(result: dict[str, Any]) -> dict[str, Any]:
    stable = dict(result)
    stable.pop("duration_s", None)
    return stable


class _FutureOnlyExecutor:
    instances: ClassVar[list[_FutureOnlyExecutor]] = []

    def __init__(self, max_workers: int) -> None:
        self.max_workers = max_workers
        self.submitted: list[Future[None]] = []
        type(self).instances.append(self)

    def __enter__(self) -> _FutureOnlyExecutor:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
        return False

    def submit(self, fn: Callable[..., Any], **kwargs: Any) -> Future[None]:
        _ = fn, kwargs
        future: Future[None] = Future()
        self.submitted.append(future)
        return future


def _complete(futures: set[Future[None]]) -> None:
    for future in futures:
        if not future.done():
            future.set_result(None)


def test_concurrency_one_preserves_dispatch_order_and_coverage(tmp_path: Path) -> None:
    schema = _schema(
        shape=(1, 4, 4),
        chunks=(1, 2, 4),
        extra_arrays=[
            ZarrArraySpec(
                name="timestamp",
                shape=(1,),
                dtype="datetime64[s]",
                chunks=(1,),
            )
        ],
    )
    ts_val = np.datetime64("2025-01-01T00:00:00", "s")
    payload = np.arange(8, dtype=np.float32).reshape(2, 4)
    intents = [
        WriteIntent.coordinate(group="data", index=0, value=ts_val),
        _region(data=payload, y_slice=slice(0, 2)),
    ]

    store_default = str(tmp_path / "default.zarr")
    store_explicit = str(tmp_path / "explicit.zarr")
    default_result = IndexedRegionStrategy(store_uri=store_default, schema=schema).write_groups(
        group_to_intents={"data": intents},
        schema=schema,
    )
    explicit_result = IndexedRegionStrategy(store_uri=store_explicit, schema=schema).write_groups(
        group_to_intents={"data": intents},
        schema=schema,
        region_write_concurrency=1,
    )

    default_root = zarr.open_group(store=store_default, mode="r", zarr_format=3)
    explicit_root = zarr.open_group(store=store_explicit, mode="r", zarr_format=3)
    default_values = cast(Any, default_root["data/values"])
    explicit_values = cast(Any, explicit_root["data/values"])
    default_timestamp = cast(Any, default_root["data/timestamp"])
    explicit_timestamp = cast(Any, explicit_root["data/timestamp"])
    np.testing.assert_array_equal(default_values[:], explicit_values[:])
    np.testing.assert_array_equal(
        default_timestamp[:],
        explicit_timestamp[:],
    )
    assert _stable_metrics(default_result) == _stable_metrics(explicit_result)


def test_concurrency_two_never_exceeds_two_pending_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    max_pending_seen = 0
    _FutureOnlyExecutor.instances = []

    def fake_wait(pending: set[Future[None]], return_when: object):
        nonlocal max_pending_seen
        max_pending_seen = max(max_pending_seen, len(pending))
        if return_when == indexed_region_module.FIRST_COMPLETED:
            first = next(iter(pending))
            _complete({first})
            return {first}, set(pending) - {first}
        _complete(set(pending))
        return set(pending), set()

    monkeypatch.setattr(indexed_region_module, "ThreadPoolExecutor", _FutureOnlyExecutor)
    monkeypatch.setattr(indexed_region_module, "wait", fake_wait)

    intents = [
        _region(y_slice=slice(0, 2)),
        _region(y_slice=slice(2, 4)),
        _region(y_slice=slice(4, 6)),
    ]
    schema = _schema(shape=(1, 6, 4), chunks=(1, 2, 4))

    IndexedRegionStrategy(store_uri=str(tmp_path / "test.zarr"), schema=schema).write_groups(
        group_to_intents={"data": intents},
        schema=schema,
        region_write_concurrency=2,
    )

    assert _FutureOnlyExecutor.instances[0].max_workers == 2
    assert len(_FutureOnlyExecutor.instances[0].submitted) == 3
    assert max_pending_seen <= 2


def test_callable_resolution_remains_serial_and_ordered_at_concurrency_two(
    tmp_path: Path,
) -> None:
    main_thread = threading.current_thread().name
    invocation_log: list[tuple[int, str]] = []

    def payload(tag: int) -> Callable[[], np.ndarray]:
        def _load() -> np.ndarray:
            invocation_log.append((tag, threading.current_thread().name))
            return np.full((2, 4), float(tag), dtype=np.float32)

        return _load

    schema = _schema(shape=(1, 6, 4), chunks=(1, 2, 4))
    intents = [
        _region(y_slice=slice(0, 2), data=payload(1)),
        _region(y_slice=slice(2, 4), data=payload(2)),
        _region(y_slice=slice(4, 6), data=payload(3)),
    ]

    IndexedRegionStrategy(store_uri=str(tmp_path / "test.zarr"), schema=schema).write_groups(
        group_to_intents={"data": intents},
        schema=schema,
        region_write_concurrency=2,
    )

    assert invocation_log == [(1, main_thread), (2, main_thread), (3, main_thread)]


def test_overlapping_physical_chunks_fails_before_executor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_executor(*args: Any, **kwargs: Any) -> _FutureOnlyExecutor:
        _ = args, kwargs
        raise AssertionError("executor must not be constructed")

    monkeypatch.setattr(indexed_region_module, "ThreadPoolExecutor", fail_executor)
    schema = _schema(shape=(1, 4, 4), chunks=(1, 4, 4))
    intents = [_region(y_slice=slice(0, 2)), _region(y_slice=slice(2, 4))]

    with pytest.raises(ValueError, match="overlap one physical chunk"):
        IndexedRegionStrategy(store_uri=str(tmp_path / "test.zarr"), schema=schema).write_groups(
            group_to_intents={"data": intents},
            schema=schema,
            region_write_concurrency=2,
        )


def test_c2_cross_slot_writes_with_time_chunk_gt_1_do_not_overlap(tmp_path: Path) -> None:
    schema = _schema(shape=(2, 4, 4), chunks=(2, 4, 4))
    first = np.full((4, 4), 1.0, dtype=np.float32)
    second = np.full((4, 4), 2.0, dtype=np.float32)
    intents = [
        _region(index=0, y_slice=slice(0, 4), data=first),
        _region(index=1, y_slice=slice(0, 4), data=second),
    ]

    IndexedRegionStrategy(store_uri=str(tmp_path / "test.zarr"), schema=schema).write_groups(
        group_to_intents={"data": intents},
        schema=schema,
        region_write_concurrency=2,
    )

    root = zarr.open_group(store=str(tmp_path / "test.zarr"), mode="r", zarr_format=3)
    values = cast(Any, root["data/values"])
    np.testing.assert_array_equal(values[0, :, :], first)
    np.testing.assert_array_equal(values[1, :, :], second)


def test_sharded_target_fails_before_executor_when_concurrency_gt_1(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_executor(*args: Any, **kwargs: Any) -> _FutureOnlyExecutor:
        _ = args, kwargs
        raise AssertionError("executor must not be constructed")

    monkeypatch.setattr(indexed_region_module, "ThreadPoolExecutor", fail_executor)
    schema = _schema(shape=(1, 4, 4), chunks=(1, 2, 4), shards=(1, 4, 4))

    with pytest.raises(ValueError, match="sharded targets"):
        IndexedRegionStrategy(store_uri=str(tmp_path / "test.zarr"), schema=schema).write_groups(
            group_to_intents={"data": [_region()]},
            schema=schema,
            region_write_concurrency=2,
        )


def test_on_disk_sharded_with_none_spec_fails_before_executor_gt_1(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_executor(*args: Any, **kwargs: Any) -> _FutureOnlyExecutor:
        _ = args, kwargs
        raise AssertionError("executor must not be constructed")

    monkeypatch.setattr(indexed_region_module, "ThreadPoolExecutor", fail_executor)
    store_path = str(tmp_path / "test.zarr")
    root = zarr.open_group(store=store_path, mode="w", zarr_format=3)
    root.require_group("data").create_array(
        name="values",
        shape=(1, 4, 4),
        dtype=np.float32,
        chunks=(1, 2, 4),
        shards=(1, 4, 4),
    )
    schema = _schema(shape=(1, 4, 4), chunks=(1, 2, 4), shards=None)

    with pytest.raises(ValueError, match="sharded on disk"):
        IndexedRegionStrategy(store_uri=store_path, schema=schema).write_groups(
            group_to_intents={"data": [_region()]},
            schema=schema,
            region_write_concurrency=2,
        )


def test_disjoint_writes_succeed_against_real_temp_zarr_store(tmp_path: Path) -> None:
    schema = _schema(shape=(1, 8, 4), chunks=(1, 2, 4))
    intents = [
        _region(y_slice=slice(0, 2), data=np.full((2, 4), 1, dtype=np.float32)),
        _region(y_slice=slice(2, 4), data=np.full((2, 4), 2, dtype=np.float32)),
        _region(y_slice=slice(4, 6), data=np.full((2, 4), 3, dtype=np.float32)),
        _region(y_slice=slice(6, 8), data=np.full((2, 4), 4, dtype=np.float32)),
    ]
    store_path = str(tmp_path / "test.zarr")

    result = IndexedRegionStrategy(store_uri=store_path, schema=schema).write_groups(
        group_to_intents={"data": intents},
        schema=schema,
        region_write_concurrency=4,
    )

    arr = cast(Any, zarr.open_group(store=store_path, mode="r", zarr_format=3)["data/values"])
    for i, expected in enumerate((1, 2, 3, 4)):
        np.testing.assert_array_equal(
            arr[0, i * 2 : i * 2 + 2, :],
            np.full((2, 4), expected, dtype=np.float32),
        )
    assert result["region_write_concurrency_effective"] == 4
    assert result["region_writes_total_count"] == 4
    assert result["region_writes_aligned_count"] == 4
    assert result["coverage"][0]["time_index_ranges"] == [[0, 0]]


def test_worker_threads_never_lazy_initialize_shared_writer_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    main_thread = threading.current_thread().name
    open_threads: list[str] = []
    original_open_root = RegionZarrWriter._open_root

    def spy_open_root(self: RegionZarrWriter) -> Any:
        open_threads.append(threading.current_thread().name)
        return original_open_root(self)

    monkeypatch.setattr(RegionZarrWriter, "_open_root", spy_open_root)
    schema = _schema(shape=(1, 4, 4), chunks=(1, 2, 4))
    intents = [_region(y_slice=slice(0, 2)), _region(y_slice=slice(2, 4))]

    IndexedRegionStrategy(store_uri=str(tmp_path / "test.zarr"), schema=schema).write_groups(
        group_to_intents={"data": intents},
        schema=schema,
        region_write_concurrency=2,
    )

    assert open_threads
    assert set(open_threads) == {main_thread}


def test_non_region_intents_drain_executor_before_dispatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    _FutureOnlyExecutor.instances = []

    def fake_wait(pending: set[Future[None]], return_when: object):
        if return_when == indexed_region_module.ALL_COMPLETED:
            events.append("drain-all")
        _complete(set(pending))
        return set(pending), set()

    original_dispatch = IndexedRegionStrategy._dispatch_intent

    def spy_dispatch(writer: RegionZarrWriter, intent: Any) -> None:
        events.append(f"dispatch-{intent.kind}")
        assert all(
            future.done()
            for executor in _FutureOnlyExecutor.instances
            for future in executor.submitted
        )
        original_dispatch(writer, intent)

    monkeypatch.setattr(indexed_region_module, "ThreadPoolExecutor", _FutureOnlyExecutor)
    monkeypatch.setattr(indexed_region_module, "wait", fake_wait)
    monkeypatch.setattr(IndexedRegionStrategy, "_dispatch_intent", staticmethod(spy_dispatch))

    schema = _schema(
        shape=(1, 4, 4),
        chunks=(1, 2, 4),
        extra_arrays=[ZarrArraySpec(name="quality", shape=(1,), dtype=np.int16, chunks=(1,))],
    )
    intents = [
        _region(y_slice=slice(0, 2)),
        _region(y_slice=slice(2, 4)),
        WriteIntent.slot(
            group="data", array="quality", index=0, data=np.asarray(7, dtype=np.int16)
        ),
    ]

    IndexedRegionStrategy(store_uri=str(tmp_path / "test.zarr"), schema=schema).write_groups(
        group_to_intents={"data": intents},
        schema=schema,
        region_write_concurrency=2,
    )

    assert events == ["drain-all", "dispatch-1d"]


def test_failed_writes_receive_no_coverage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorded_indices: list[int] = []
    original_record_write = CoverageTracker.record_write
    original_write_region = RegionZarrWriter.write_region

    def spy_record_write(self: CoverageTracker, *args: Any, **kwargs: Any) -> None:
        recorded_indices.append(int(kwargs["ts_index"]))
        original_record_write(self, *args, **kwargs)

    def failing_write_region(self: RegionZarrWriter, *args: Any, **kwargs: Any) -> None:
        if kwargs["ts_index"] == 1:
            raise RuntimeError("injected region failure")
        original_write_region(self, *args, **kwargs)

    monkeypatch.setattr(CoverageTracker, "record_write", spy_record_write)
    monkeypatch.setattr(RegionZarrWriter, "write_region", failing_write_region)
    schema = _schema(shape=(2, 4, 4), chunks=(1, 2, 4))
    intents = [_region(index=0), _region(index=1)]

    with pytest.raises(RuntimeError, match="injected region failure"):
        IndexedRegionStrategy(store_uri=str(tmp_path / "test.zarr"), schema=schema).write_groups(
            group_to_intents={"data": intents},
            schema=schema,
            region_write_concurrency=2,
        )

    assert 1 not in recorded_indices


def test_claim_releases_only_after_all_started_writes_complete(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _FutureOnlyExecutor.instances = []
    release_checks: list[bool] = []

    def fake_wait(pending: set[Future[None]], return_when: object):
        _ = return_when
        _complete(set(pending))
        return set(pending), set()

    class Claim:
        def __enter__(self) -> Claim:
            return self

        def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
            release_checks.append(
                all(
                    future.done()
                    for executor in _FutureOnlyExecutor.instances
                    for future in executor.submitted
                )
            )
            return False

    monkeypatch.setattr(indexed_region_module, "ThreadPoolExecutor", _FutureOnlyExecutor)
    monkeypatch.setattr(indexed_region_module, "wait", fake_wait)

    schema = _schema(shape=(1, 4, 4), chunks=(1, 2, 4))
    intents = [_region(y_slice=slice(0, 2)), _region(y_slice=slice(2, 4))]

    IndexedRegionStrategy(store_uri=str(tmp_path / "test.zarr"), schema=schema).write_groups(
        group_to_intents={"data": intents},
        schema=schema,
        claim_for_slot=lambda group, ts_index: Claim(),
        region_write_concurrency=2,
    )

    assert release_checks == [True]


def test_existing_serial_tests_still_pass(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    class Writer:
        def ensure_group(self, group: str, **kwargs: Any) -> None:
            _ = group, kwargs
            calls.append("ensure_group")

        def set_group_attrs(self, group: str, attrs: Any) -> None:
            _ = group, attrs
            calls.append("set_group_attrs")

    class Claim:
        def __enter__(self) -> Claim:
            calls.append("claim_enter")
            return self

        def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
            calls.append("claim_exit")
            return False

    def dispatch_intent(writer: RegionZarrWriter, intent: Any) -> None:
        _ = writer, intent
        calls.append("dispatch")

    monkeypatch.setattr(indexed_region_module, "RegionZarrWriter", lambda *args, **kwargs: Writer())
    monkeypatch.setattr(IndexedRegionStrategy, "_dispatch_intent", staticmethod(dispatch_intent))

    schema = [ZarrGroupSpec(group="data", arrays=[ZarrArraySpec("values", (1, 2), np.float32)])]
    intent = type(
        "Intent",
        (),
        {
            "kind": "region",
            "group": "data",
            "array": "values",
            "ts_index": 0,
            "data": None,
            "y_slice": None,
            "channel_index": None,
            "timestamp_val": None,
        },
    )()

    IndexedRegionStrategy(store_uri="/tmp/test.zarr", schema=[]).write_groups(
        group_to_intents={"data": [intent]},
        schema=schema,
        claim_for_group=lambda group_name: Claim(),
        region_write_concurrency=1,
    )

    assert calls == [
        "claim_enter",
        "ensure_group",
        "set_group_attrs",
        "claim_exit",
        "claim_enter",
        "dispatch",
        "claim_exit",
    ]
