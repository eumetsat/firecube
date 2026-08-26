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

"""Protocols and shared abstractions for the Firecube ingestion SDK.

This module defines *interfaces* (Protocols) used by the ingestion engine and
plugin implementations. It intentionally contains no runtime logic.

Key concepts:
- `SourceFile`: a local-or-remote, file-like input abstraction.
- `Ingestor`: the minimal plugin contract (run an ingestion with an
  `IngestContext`).
- `PipelineHost`: the contract implemented by an ingestor that can be executed
  by the pipeline runner (callbacks + batch processing).

Plugins should prefer importing public types from `firecube.ingestor.api` rather
than deep-importing from `firecube.ingestor.*`.
"""

from __future__ import annotations

import inspect
import logging
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any, BinaryIO, Protocol, runtime_checkable

from firecube.core.observability.telemetry import IngestionTelemetry
from firecube.ingestor.runtime.zarr.contracts import AppendWriteStrategy, RegionWriteStrategy
from firecube.ingestor.types.context import (
    IngestContext,
    IngestResult,
    PipelineBatch,
    PipelineResult,
    PipelineRunState,
    PluginContext,
    RuntimeIngestContext,
)


@runtime_checkable
class DatasetProducer(Protocol):
    """Protocol for batch templates that produce in-memory datasets.

    Tensogram output is selected by this structural capability rather than by
    requiring a specific Zarr base class.  Implementations share the
    ``build_dataset`` and ``get_batch_groups`` hooks provided by
    ``BaseIngestor``.
    """

    def build_dataset(self, group: str, items: list[Any], ctx: PluginContext) -> Any | None:
        """Convert a sub-batch into a dataset for ``group``."""
        ...

    def get_batch_groups(self, items: Sequence[Any], ctx: PluginContext) -> list[str]:
        """Return logical dataset groups/messages for ``items``."""
        ...


def is_dataset_producer(cls: type) -> bool:
    """Return True iff *cls* genuinely satisfies the ``DatasetProducer`` contract.

    ``@runtime_checkable`` Protocol's ``issubclass`` / ``isinstance`` checks
    validate method-name presence only — Python deliberately does NOT check
    signatures. That is structurally unsound for ``DatasetProducer`` because
    ``GenericParquetIngestor`` declares
    ``build_dataset(self, group, batch: PipelineBatch, ctx)`` which collides
    with the protocol's ``build_dataset(self, group, items: list, ctx)`` by
    name but is invoked incompatibly by ``TensogramWriteStrategy`` (which
    passes a ``list`` of timestamps, not a ``PipelineBatch``).

    This helper adds a signature-shape check: the second positional parameter
    after ``group`` must NOT be named ``batch`` (the parquet convention).
    Plugins using ``*args`` are accepted.
    """
    if not issubclass(cls, DatasetProducer):
        return False
    try:
        sig = inspect.signature(cls.build_dataset)
    except (TypeError, ValueError):
        return False
    positional = [
        p
        for p in sig.parameters.values()
        if p.kind
        in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )
    ]
    if len(positional) < 3:
        return any(p.kind is inspect.Parameter.VAR_POSITIONAL for p in sig.parameters.values())
    return positional[2].name != "batch"


@runtime_checkable
class SourceFile(Protocol):
    """Abstraction for a file-like object that may be local or remote.

    This allows plugins to accept S3 objects as if they were local files,
    supporting standard python file operations (seek, read).
    """

    @property
    def uri(self) -> str:
        """The canonical URI of the source file."""
        ...

    def open(self) -> BinaryIO:
        """Open the file in binary read mode. Must be seekable."""
        ...

    def local_path(self) -> Path | None:
        """Return a local Path if one exists efficiently.

        Returns None for remote files (unless they are explicitly materialized).
        This allows optimizations for libraries that prefer pathlib.Path only when cheap.
        """
        ...


@runtime_checkable
class Ingestor(Protocol):
    """Protocol defining the core contract for an ingestor."""

    def run(self, ctx: IngestContext) -> IngestResult:
        """Execute a full ingest run and return its result.

        This is the top-level orchestration entry point: it opens the run in
        the control plane, drives batching and writes, and closes the run in a
        terminal state. Framework-owned — plugin authors implement the hooks
        it calls, not this method.

        Args:
            ctx: Resolved run context: source, target, storage binding, and
                effective options.

        Returns:
            The run outcome, including output paths and run-level metrics.
        """
        ...

    def ingest(self, ctx: IngestContext) -> IngestResult:
        """Perform the ingest work for *ctx* without run-level orchestration.

        Args:
            ctx: Resolved run context.

        Returns:
            The ingest outcome.
        """
        ...


class PipelineHost(Protocol):
    """Protocol for classes that can be executed by the pipeline runner.

    The runtime pipeline executor expects these attributes and lifecycle hooks
    to exist on the host object (typically an ingestor instance).
    """

    name: str
    _log: logging.Logger

    def _create_batches(
        self, ctx: RuntimeIngestContext, batch_size: int
    ) -> Iterable[PipelineBatch]: ...
    def _aggregate_metrics(
        self, ctx: RuntimeIngestContext, state: PipelineRunState
    ) -> Mapping[str, Any]: ...

    # Lifecycle hooks called by PipelineRunner
    def on_pipeline_start(self, ctx: PluginContext, state: PipelineRunState) -> None:
        """Called once on the main thread before any batch runs.

        The place for shared resource setup that must happen before the first
        write (a persistent database schema, a warmed cache). Raising here
        aborts the run before any batch is attempted.

        Args:
            ctx: Plugin-facing run context.
            state: Snapshot of run state at start, including the effective
                worker count and batch size.
        """
        ...

    def on_batch_success(
        self,
        ctx: PluginContext,
        state: PipelineRunState,
        batch: PipelineBatch,
        result: PipelineResult,
    ) -> None:
        """Called on the main thread after each successful batch.

        Never called from a worker thread, so implementations may touch
        non-thread-safe state. The engine records the batch's control-plane
        span here; a hook failure is logged as a bookkeeping error and does
        not demote the batch to a failure.

        Args:
            ctx: Plugin-facing run context.
            state: Run state as of this batch.
            batch: The batch that completed.
            result: The batch outcome, including its metrics and outputs.
        """
        ...

    def on_batch_failure(
        self,
        ctx: PluginContext,
        state: PipelineRunState,
        batch: PipelineBatch,
        result: PipelineResult,
    ) -> None:
        """Called on the main thread after each failed batch.

        The engine records the failure against the batch's control-plane span
        here. A hook failure is logged and does not mask the original error.

        Args:
            ctx: Plugin-facing run context.
            state: Run state as of this batch.
            batch: The batch that failed.
            result: The batch outcome; ``result.error`` carries the cause.
        """
        ...

    def finalize_pipeline(self, ctx: RuntimeIngestContext, state: PipelineRunState) -> IngestResult:
        """Assemble the run result after the last batch has been processed.

        Framework-owned — it delegates to the pipeline executor. Plugins
        aggregate their own numbers through the metrics hooks instead.

        Args:
            ctx: Runtime run context.
            state: Final accumulated run state.

        Returns:
            The run outcome reported to the caller.
        """
        ...

    def _process_batch(self, batch: PipelineBatch, ctx: PluginContext) -> PipelineResult: ...
    def _resolve_time_dim_name(self) -> str:
        """Resolve the firecube time-dim coordinate name for this ingestor.

        Default ``"timestamp"``; plugins may override via
        ``time_dim_name: ClassVar[str]`` on their BaseIngestor subclass.
        Consumed by the runtime engine to seed coordinate-array chunks
        in staged mode.
        """
        ...


__all__ = [
    "AppendWriteStrategy",
    "DatasetProducer",
    "IngestContext",
    "IngestResult",
    "IngestionTelemetry",
    "Ingestor",
    "PipelineBatch",
    "PipelineHost",
    "PipelineResult",
    "PipelineRunState",
    "PluginContext",
    "RegionWriteStrategy",
    "RuntimeIngestContext",
    "SourceFile",
    "is_dataset_producer",
]
