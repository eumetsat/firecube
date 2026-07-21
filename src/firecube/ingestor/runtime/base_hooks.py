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

"""Reusable lifecycle and slice-metadata hooks for BaseIngestor."""

from __future__ import annotations

import logging
from typing import Any

from firecube.ingestor.types.context import (
    PipelineBatch,
    PipelineResult,
    PipelineRunState,
    PluginContext,
)


def _product_name(ctx: PluginContext, default: str) -> str:
    storage = ctx.storage
    if storage is not None and storage.output is not None:
        return str(storage.output.product.product_name)
    return default


def _canonicalize_slice_value(value: Any) -> Any:
    if isinstance(value, set):
        value = list(value)
    if isinstance(value, tuple):
        value = list(value)
    if isinstance(value, list):
        try:
            return sorted(value)
        except TypeError:
            return list(value)
    return value


class BaseIngestorHookMixin:
    """Shared non-engine hooks used by BaseIngestor implementations."""

    name: str
    _log: logging.Logger
    _span_recorder: Any

    def slice_meta_keys(self) -> list[str]:
        """Option keys that define a logical slice for this plugin."""
        return []

    def slice_meta(self, ctx: PluginContext) -> dict[str, Any]:
        """Return canonical slice metadata for this run."""
        meta: dict[str, Any] = {}
        for key in self.slice_meta_keys():
            if key in ctx.options:
                meta[key] = _canonicalize_slice_value(ctx.options.get(key))
        return meta

    def validation_group(self, ctx: PluginContext) -> str | None:
        """Optional hook to derive a Zarr group path for validate_zarr."""
        _ = ctx
        return None

    def on_pipeline_start(self, ctx: PluginContext, state: PipelineRunState) -> None:
        """Called before pipeline execution starts."""
        _ = ctx
        self._log.info(
            "Starting pipeline execution workers=%d batch_size=%d",
            state.pipeline_workers,
            state.batch_size,
        )

    def on_batch_success(
        self,
        ctx: PluginContext,
        state: PipelineRunState,
        batch: PipelineBatch,
        result: PipelineResult,
    ) -> None:
        """Called on successful batch completion (engine-owned bookkeeping)."""
        _ = state
        slice_meta = self.slice_meta(ctx)
        slice_meta.setdefault("plugin", self.name)

        run_id = ctx.run_id or str(ctx.option("run_id", "unknown"))
        product = _product_name(ctx, self.name)

        self._span_recorder.record_batch_success(
            ctx=ctx,
            batch=batch,
            result=result,
            slice_meta=slice_meta,
            run_id=run_id,
            product=product,
        )

    def on_batch_failure(
        self,
        ctx: PluginContext,
        state: PipelineRunState,
        batch: PipelineBatch,
        result: PipelineResult,
    ) -> None:
        """Called on batch failure (engine-owned bookkeeping)."""
        _ = state
        slice_meta = self.slice_meta(ctx)
        slice_meta.setdefault("plugin", self.name)

        run_id = ctx.run_id or str(ctx.option("run_id", "unknown"))
        product = _product_name(ctx, self.name)

        self._span_recorder.record_batch_failure(
            ctx=ctx,
            batch=batch,
            error=result.error,
            slice_meta=slice_meta,
            run_id=run_id,
            product=product,
        )
