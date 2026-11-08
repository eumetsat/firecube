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
import threading
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import numpy as np
import pytest
import zarr

from firecube.ingestor.runtime.zarr.strategies.indexed_region import (
    IndexedRegionStrategy,
)
from firecube.ingestor.runtime.zarr.write_session import (
    SessionState,
    WriteSessionStateError,
    ZarrWriteSession,
)
from firecube.ingestor.templates.direct_zarr import (
    DirectZarrIngestor,
    WriteIntent,
    ZarrArraySpec,
    ZarrGroupSpec,
)
from firecube.ingestor.types.context import ExecutionParams, PipelineBatch, PluginContext
from tests.helpers.storage import begin_write_session, make_test_context


class _StubDirectIngestor(DirectZarrIngestor):
    PRODUCT_NAME = "stub_direct"
    name = "stub_direct"

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [
            ZarrGroupSpec(
                group="data_1km",
                arrays=[
                    ZarrArraySpec(
                        name="counts",
                        shape=(0, 100, 100),
                        dtype=np.float32,
                        chunks=(1, 100, 100),
                        fill_value=np.nan,
                    ),
                    ZarrArraySpec(
                        name="timestamp",
                        shape=(0,),
                        dtype="datetime64[s]",
                        chunks=(256,),
                    ),
                ],
            )
        ]

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        return [
            WriteIntent(
                group="data_1km",
                array="timestamp",
                ts_index=0,
                data=None,
                kind="timestamp",
                timestamp_val=np.datetime64("2025-01-01T00:00:00", "s"),
            ),
            WriteIntent(
                group="data_1km",
                array="counts",
                ts_index=0,
                data=np.ones((50, 100), dtype=np.float32),
                kind="region",
                y_slice=slice(0, 50),
            ),
        ]


class TestDirectZarrIngestorAbstract:
    def test_cannot_instantiate_without_hooks(self) -> None:
        with pytest.raises(TypeError, match="abstract"):
            DirectZarrIngestor(name="broken")  # type: ignore[abstract]

    def test_subclass_with_hooks_instantiates(self) -> None:
        ingestor = _StubDirectIngestor()
        assert ingestor.name == "stub_direct"

    def test_aggregate_metrics_merges_batch_results(self) -> None:
        ingestor = _StubDirectIngestor()
        ctx = MagicMock()
        ctx.output_format = "zarr"
        batch = PipelineBatch(batch_id="b1", data_path=Path("/tmp"), items=["f1.nc"])
        state = MagicMock()
        state.results = (
            MagicMock(
                success=True,
                metrics={
                    "count": 2,
                    "zarr": {"coverage": [{"group": "data_1km"}]},
                    "storage_handled": True,
                },
                duration_s=1.25,
                cpu_time_s=0.5,
                io_time_s=0.25,
                batch=batch,
            ),
        )

        result = ingestor._aggregate_metrics(ctx, state)

        assert result["count"] == 2
        assert result["zarr"]["coverage"] == [{"group": "data_1km"}]


class TestDirectZarrProcessBatch:
    def test_empty_intents_returns_success_with_zero_count(self) -> None:
        class _EmptyIngestor(DirectZarrIngestor):
            PRODUCT_NAME = "empty"
            name = "empty"

            def zarr_schema(self, ctx):
                return []

            def build_write_intents(self, batch, ctx):
                return []

        ingestor = _EmptyIngestor()
        ctx = MagicMock(spec=PluginContext)
        ctx.execution = ExecutionParams(write_mode="direct")
        ctx.output_name = "test_product"
        ctx.run_id = "run-001"
        ctx.option = MagicMock(return_value="unknown")
        batch = PipelineBatch(batch_id="b1", data_path=Path("/tmp"), items=[])

        with patch.object(ingestor, "resolve_output_uri", return_value="/tmp/test.zarr"):
            begin_write_session(ingestor, ctx)
            result = ingestor._process_batch(batch, ctx)

        assert result.success is True
        assert result.metrics["count"] == 0

    def test_process_batch_error_returns_failure(self) -> None:
        ingestor = _StubDirectIngestor()
        ctx = MagicMock(spec=PluginContext)
        ctx.execution = ExecutionParams(write_mode="direct")
        ctx.output_name = "test_product"
        ctx.run_id = "run-001"
        ctx.option = MagicMock(return_value="unknown")
        batch = PipelineBatch(batch_id="b1", data_path=Path("/tmp"), items=["f1.nc"])

        with patch.object(ingestor, "resolve_output_uri", side_effect=RuntimeError("boom")):
            begin_write_session(ingestor, ctx)
            result = ingestor._process_batch(batch, ctx)

        assert result.success is False
        assert result.error is not None
        assert "boom" in result.error


class TestWriteIntentDataclass:
    def test_defaults(self) -> None:
        intent = WriteIntent(group="g", array="a", ts_index=0, data=np.array([1]))
        assert intent.kind == "region"
        assert intent.y_slice is None
        assert intent.channel_index is None
        assert intent.timestamp_val is None

    def test_frozen(self) -> None:
        intent = WriteIntent(group="g", array="a", ts_index=0, data=np.array([1]))
        with pytest.raises(AttributeError):
            intent.group = "other"  # type: ignore[misc]


class TestZarrSpecDataclasses:
    def test_array_spec_fields(self) -> None:
        spec = ZarrArraySpec(name="counts", shape=(10, 100), dtype=np.float32)
        assert spec.name == "counts"
        assert spec.chunks is None
        assert spec.fill_value is None

    def test_group_spec_defaults(self) -> None:
        spec = ZarrGroupSpec(group="data_1km")
        assert spec.arrays == []
        assert spec.coord_names == frozenset({"y", "x", "channel"})


class TestIndexedRegionStrategy:
    def test_dispatch_unknown_kind_raises(self) -> None:
        writer = MagicMock()
        intent = MagicMock(kind="invalid_kind")
        with pytest.raises(ValueError, match="Unknown WriteIntent kind"):
            IndexedRegionStrategy._dispatch_intent(writer, intent)

    def test_dispatch_region(self) -> None:
        writer = MagicMock()
        intent = WriteIntent(
            group="g",
            array="counts",
            ts_index=0,
            data=np.zeros((10, 10)),
            kind="region",
            y_slice=slice(0, 10),
            channel_index=2,
        )
        IndexedRegionStrategy._dispatch_intent(writer, intent)
        writer.write_region.assert_called_once_with(
            group="g",
            array_name="counts",
            ts_index=0,
            y_slice=slice(0, 10),
            data=intent.data,
            channel_index=2,
        )

    def test_dispatch_1d(self) -> None:
        writer = MagicMock()
        intent = WriteIntent(
            group="g",
            array="calibration",
            ts_index=5,
            data=np.array([1.0, 2.0]),
            kind="1d",
        )
        IndexedRegionStrategy._dispatch_intent(writer, intent)
        writer.write_1d.assert_called_once_with(
            group="g",
            array_name="calibration",
            ts_index=5,
            data=intent.data,
        )

    def test_dispatch_timestamp(self) -> None:
        writer = MagicMock()
        ts_val = np.datetime64("2025-06-15T12:00:00", "s")
        intent = WriteIntent(
            group="g",
            array="timestamp",
            ts_index=3,
            data=None,
            kind="timestamp",
            timestamp_val=ts_val,
        )
        IndexedRegionStrategy._dispatch_intent(writer, intent)
        writer.write_timestamp.assert_called_once_with(
            group="g",
            ts_index=3,
            timestamp_val=ts_val,
        )

    def test_write_groups_returns_coverage_and_duration(self, tmp_path: Path) -> None:
        store_path = str(tmp_path / "test.zarr")
        zarr.open_group(store=store_path, mode="w", zarr_format=3)

        schema = [
            ZarrGroupSpec(
                group="data",
                arrays=[
                    ZarrArraySpec(
                        name="values",
                        shape=(1, 10),
                        dtype=np.float32,
                        chunks=(1, 10),
                        fill_value=0.0,
                    ),
                ],
            )
        ]

        strategy = IndexedRegionStrategy(
            store_uri=store_path,
            schema=schema,
            coord_names_by_group={"data": frozenset()},
        )

        ts_val = np.datetime64("2025-01-01", "s")
        intents = [
            WriteIntent(
                group="data",
                array="timestamp",
                ts_index=0,
                data=None,
                kind="timestamp",
                timestamp_val=ts_val,
            ),
        ]

        result = strategy.write_groups(
            group_to_intents={"data": intents},
            schema=schema,
        )

        assert "coverage" in result
        assert "duration_s" in result
        assert isinstance(result["duration_s"], float)
        assert result["duration_s"] >= 0
        coverage = result["coverage"][0]
        assert coverage["group"] == "data"
        assert coverage["arrays"] == ["timestamp"]
        assert coverage["time_index_ranges"] == [[0, 0]]
        assert coverage["aligned"] is True
        assert coverage["state_array"] == "data/firecube_timestamp_state"
        assert coverage["state_deleted_value"] == 2
        assert str(coverage["time_min"]) == "2025-01-01T00:00:00Z"
        assert str(coverage["time_max"]) == "2025-01-01T00:00:00Z"
        assert coverage["time_dim_name"] == "timestamp"


class TestDirectZarrLifecycleHooks:
    """`_process_batch` wraps intent generation with the four lifecycle hooks.

    Contracts protected:

    - ``batch_setup`` → ``prepare_batch_data`` → ``cleanup_batch_data`` →
      ``batch_teardown`` fire in that order on the success path.
    - ``cleanup_batch_data`` and ``batch_teardown`` still fire on the
      empty-intents short-circuit path.
    - ``cleanup_batch_data`` and ``batch_teardown`` still fire when the
      strategy raises mid-dispatch; ``PipelineResult.success`` is False.
    - ``prepare_batch_data``'s returned metrics merge additively into the
      final metrics dict; template-owned keys are preserved.

    Mirrors ``GenericZarrIngestor._process_batch`` (templates/generic.py:233-303)
    so plugin authors get one lifecycle contract across both templates.
    """

    def _build_ctx(self) -> MagicMock:
        ctx = MagicMock(spec=PluginContext)
        ctx.output_name = "test_product"
        ctx.run_id = "run-001"
        ctx.option = MagicMock(return_value="unknown")
        ctx.storage = None
        ctx.execution = ExecutionParams(write_mode="direct")
        return ctx

    def test_lifecycle_hook_order_on_success(self) -> None:
        calls: list[str] = []

        class _OrderedIngestor(_StubDirectIngestor):
            def batch_setup(self, ctx: PluginContext) -> None:
                calls.append("batch_setup")
                super().batch_setup(ctx)

            def prepare_batch_data(self, batch, ctx):
                calls.append("prepare_batch_data")
                return None

            def cleanup_batch_data(self, batch, ctx) -> None:
                calls.append("cleanup_batch_data")

            def batch_teardown(self, ctx: PluginContext) -> None:
                calls.append("batch_teardown")
                super().batch_teardown(ctx)

        ingestor = _OrderedIngestor()
        ingestor._index_binding = None
        ctx = self._build_ctx()
        batch = PipelineBatch(batch_id="b1", data_path=Path("/tmp"), items=["f1.nc"])

        with (
            patch(
                "firecube.ingestor.runtime.zarr.strategies.indexed_region.IndexedRegionStrategy"
            ) as MockStrategy,
            patch.object(ingestor, "resolve_output_uri", return_value="/tmp/test.zarr"),
        ):
            MockStrategy.return_value.write_groups.return_value = {
                "coverage": [],
                "duration_s": 0.0,
            }
            begin_write_session(ingestor, ctx)
            result = ingestor._process_batch(batch, ctx)

        assert result.success is True, f"expected success; got error={result.error}"
        assert calls == [
            "batch_setup",
            "prepare_batch_data",
            "cleanup_batch_data",
            "batch_teardown",
        ], f"hook order regression: {calls}"

    def test_lifecycle_cleanup_on_empty_intents_batch(self) -> None:
        calls: list[str] = []

        class _EmptyOrderedIngestor(DirectZarrIngestor):
            PRODUCT_NAME = "empty_ordered"
            name = "empty_ordered"

            def zarr_schema(self, ctx):
                return []

            def build_write_intents(self, batch, ctx):
                return []

            def batch_setup(self, ctx):
                calls.append("batch_setup")
                super().batch_setup(ctx)

            def prepare_batch_data(self, batch, ctx):
                calls.append("prepare_batch_data")
                return None

            def cleanup_batch_data(self, batch, ctx):
                calls.append("cleanup_batch_data")

            def batch_teardown(self, ctx):
                calls.append("batch_teardown")
                super().batch_teardown(ctx)

        ingestor = _EmptyOrderedIngestor()
        ingestor._index_binding = None
        ctx = self._build_ctx()
        batch = PipelineBatch(batch_id="b1", data_path=Path("/tmp"), items=[])

        with patch.object(ingestor, "resolve_output_uri", return_value="/tmp/test.zarr"):
            begin_write_session(ingestor, ctx)
            result = ingestor._process_batch(batch, ctx)

        assert result.success is True
        assert result.metrics["count"] == 0
        assert "cleanup_batch_data" in calls, (
            f"cleanup must fire on empty-intents path; observed hooks={calls}"
        )
        assert "batch_teardown" in calls, (
            f"teardown must fire on empty-intents path; observed hooks={calls}"
        )
        assert calls == [
            "batch_setup",
            "prepare_batch_data",
            "cleanup_batch_data",
            "batch_teardown",
        ], f"hook order regression on empty-intents path: {calls}"

    def test_lifecycle_cleanup_on_strategy_failure(self) -> None:
        calls: list[str] = []

        class _FailingIngestor(_StubDirectIngestor):
            def batch_setup(self, ctx):
                calls.append("batch_setup")
                super().batch_setup(ctx)

            def prepare_batch_data(self, batch, ctx):
                calls.append("prepare_batch_data")
                return None

            def cleanup_batch_data(self, batch, ctx):
                calls.append("cleanup_batch_data")

            def batch_teardown(self, ctx):
                calls.append("batch_teardown")
                super().batch_teardown(ctx)

        ingestor = _FailingIngestor()
        ingestor._index_binding = None
        ctx = self._build_ctx()
        batch = PipelineBatch(batch_id="b1", data_path=Path("/tmp"), items=["f1.nc"])

        with (
            patch(
                "firecube.ingestor.runtime.zarr.strategies.indexed_region.IndexedRegionStrategy"
            ) as MockStrategy,
            patch.object(ingestor, "resolve_output_uri", return_value="/tmp/test.zarr"),
        ):
            MockStrategy.return_value.write_groups.side_effect = RuntimeError(
                "strategy dispatch boom"
            )
            begin_write_session(ingestor, ctx)
            result = ingestor._process_batch(batch, ctx)

        assert result.success is False, "PipelineResult must reflect failure"
        assert result.error is not None
        assert "strategy dispatch boom" in result.error, (
            f"error must propagate the underlying strategy error; got {result.error!r}"
        )
        assert "cleanup_batch_data" in calls, (
            f"cleanup must fire on strategy-failure path; observed hooks={calls}"
        )
        assert "batch_teardown" in calls, (
            f"teardown must fire on strategy-failure path; observed hooks={calls}"
        )
        assert calls == [
            "batch_setup",
            "prepare_batch_data",
            "cleanup_batch_data",
            "batch_teardown",
        ], f"hook order regression on failure path: {calls}"

    def test_prep_metrics_merged_not_replaced(self) -> None:
        class _PrepMetricsIngestor(_StubDirectIngestor):
            def prepare_batch_data(self, batch, ctx):
                return {"prep_custom": 42}

        ingestor = _PrepMetricsIngestor()
        ingestor._index_binding = None
        ctx = self._build_ctx()
        batch = PipelineBatch(batch_id="b1", data_path=Path("/tmp"), items=["f1.nc"])

        with (
            patch(
                "firecube.ingestor.runtime.zarr.strategies.indexed_region.IndexedRegionStrategy"
            ) as MockStrategy,
            patch.object(ingestor, "resolve_output_uri", return_value="/tmp/test.zarr"),
        ):
            MockStrategy.return_value.write_groups.return_value = {
                "coverage": [{"group": "data_1km"}],
                "duration_s": 0.0,
            }
            begin_write_session(ingestor, ctx)
            result = ingestor._process_batch(batch, ctx)

        assert result.success is True, f"expected success; got error={result.error}"
        metrics = result.metrics
        assert metrics["prep_custom"] == 42, (
            f"prep_metrics key must survive additive merge; got {metrics}"
        )
        for template_key in ("zarr", "coverage", "count", "storage_handled"):
            assert template_key in metrics, (
                f"template-owned key {template_key!r} missing after merge; got {list(metrics)}"
            )
        assert metrics["count"] == 2, (
            f"template count must not be shadowed by prep merge; got {metrics['count']}"
        )
        assert metrics["coverage"] == [{"group": "data_1km"}], (
            f"template coverage must survive merge; got {metrics['coverage']}"
        )


class _HeldBatch:
    """One admitted batch of a session held open on another thread until released."""

    def __init__(self) -> None:
        self.admitted = threading.Event()
        self.release = threading.Event()
        self.thread: threading.Thread | None = None

    def start(self, session: ZarrWriteSession) -> None:
        def batch() -> None:
            with session.admit_write():
                self.admitted.set()
                self.release.wait(30)

        self.thread = threading.Thread(target=batch, daemon=True)
        self.thread.start()
        assert self.admitted.wait(5), "the held batch was never admitted"

    def finish(self) -> None:
        self.release.set()
        if self.thread is not None:
            self.thread.join(10)
            assert not self.thread.is_alive()


class _StagedRunIngestor(_StubDirectIngestor):
    """One full-chunk slot per run; optionally leaves a second batch in flight."""

    held: _HeldBatch | None = None

    def discover_source_files(self, ctx: PluginContext) -> list[str]:
        return ["f1.nc"]

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        if self.held is not None:
            session = self._write_session()
            session._drain_timeout = 0.05  # the drains below give up on the held batch
            self.held.start(session)
        return [
            WriteIntent(
                group="data_1km",
                array="timestamp",
                ts_index=0,
                data=None,
                kind="timestamp",
                timestamp_val=np.datetime64("2025-01-01T00:00:00", "s"),
            ),
            WriteIntent(
                group="data_1km",
                array="counts",
                ts_index=0,
                data=np.ones((100, 100), dtype=np.float32),
                kind="region",
                y_slice=slice(0, 100),
            ),
        ]


def _staged_run_ctx(tmp_path: Path, workspace: Path) -> Any:
    return make_test_context(
        tmp_path,
        product="stub.zarr",
        options={
            "workspace": str(workspace),
            "pipeline_workers": 1,
            "no_progress": True,
            "cleanup_workspace": True,
        },
        execution=ExecutionParams(write_mode="staged"),
    )


class TestRunStateClose:
    """A run whose write session cannot drain keeps the session and its workspace."""

    def test_host_keeps_its_session_after_a_failed_close(self) -> None:
        ingestor = _StubDirectIngestor()
        ctx = MagicMock(spec=PluginContext)
        ctx.execution = ExecutionParams(write_mode="direct")
        ctx.output_name = "test_product"
        ctx.run_id = "run-001"
        ctx.option = MagicMock(return_value="unknown")
        with patch.object(ingestor, "resolve_output_uri", return_value="/tmp/test.zarr"):
            session = begin_write_session(ingestor, ctx)
            session._drain_timeout = 0.05
            held = _HeldBatch()
            held.start(session)
            try:
                with pytest.raises(WriteSessionStateError, match="still in flight"):
                    ingestor._close_run_state()
                assert ingestor._zarr_write_session is session
                # A retry meets the same session and the same failure.
                with pytest.raises(WriteSessionStateError, match="still in flight"):
                    ingestor._close_run_state()
                # So does the next run on this host: it is refused, the session stays.
                with pytest.raises(WriteSessionStateError, match="still in flight"):
                    begin_write_session(ingestor, ctx)
                assert ingestor._write_session() is session
            finally:
                held.finish()
            ingestor._close_run_state()
        assert ingestor._zarr_write_session is None
        assert session.state is SessionState.TERMINAL

    def test_run_keeps_the_workspace_when_its_session_cannot_drain(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        workspace = tmp_path / "ws"
        host = _StagedRunIngestor()
        host.held = held = _HeldBatch()
        try:
            with (
                caplog.at_level(logging.WARNING),
                pytest.raises(WriteSessionStateError, match="promotion refused"),
            ):
                host.run(_staged_run_ctx(tmp_path, workspace))
            assert (workspace / "stub.zarr").is_dir(), "the workspace was deleted under a write"
            kept = [
                r
                for r in caplog.records
                if r.levelno == logging.ERROR and str(workspace) in r.getMessage()
            ]
            assert len(kept) == 1 and "left in place" in kept[0].getMessage()
            assert host._zarr_write_session is not None
        finally:
            held.finish()
        host._close_run_state()
        assert host._zarr_write_session is None

    def test_run_cleans_the_workspace_when_its_session_closes(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        workspace = tmp_path / "ws"
        host = _StagedRunIngestor()
        with caplog.at_level(logging.WARNING):
            host.run(_staged_run_ctx(tmp_path, workspace))
        assert not workspace.exists()
        assert [r for r in caplog.records if r.levelno >= logging.ERROR] == []
        assert host._zarr_write_session is None


class TestApiReExports:
    def test_direct_zarr_ingestor_importable(self) -> None:
        from firecube.sdk import DirectZarrIngestor as D

        assert D is DirectZarrIngestor

    def test_write_intent_importable(self) -> None:
        from firecube.sdk import WriteIntent as W

        assert W is WriteIntent

    def test_zarr_specs_importable(self) -> None:
        from firecube.sdk import ZarrArraySpec as PublicArraySpec
        from firecube.sdk import ZarrGroupSpec as PublicGroupSpec

        assert PublicArraySpec is ZarrArraySpec
        assert PublicGroupSpec is ZarrGroupSpec
