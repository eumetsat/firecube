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

"""Generic ingestor template facade for Tensogram batch pipelines."""

from __future__ import annotations

import contextlib
import types
from abc import abstractmethod
from collections.abc import Sequence
from pathlib import Path
from typing import Any, cast

import xarray as xr

from firecube.core.api import is_remote_target, local_path_from_target, require_tensogram
from firecube.ingestor.api import (
    BaseIngestor,
    OutputPaths,
    PipelineBatch,
    PipelineResult,
    PluginContext,
    TensogramWriteStrategy,
)
from firecube.ingestor.templates.config import TensogramTemplateConfig


def process_tensogram_batch(self: Any, batch: PipelineBatch, ctx: PluginContext) -> PipelineResult:
    """Process a batch through the Tensogram write strategy for a dataset producer."""
    require_tensogram("GenericTensogramIngestor")

    self.batch_setup(ctx)

    strategy: TensogramWriteStrategy | None = None
    try:
        target = str(ctx.target or "output.tgm")
        if is_remote_target(target):
            raise ValueError(
                "GenericTensogramIngestor only supports local .tgm targets; "
                f"got remote target '{target}'."
            )

        telemetry = ctx.telemetry
        prep_ctx = (
            cast(Any, telemetry.span("firecube.batch.prepare"))
            if telemetry is not None
            else contextlib.nullcontext()
        )
        with prep_ctx:
            prep_metrics = self.prepare_batch_data(batch, ctx) or {}

        tgm_config = self.template_config
        compression = (
            tgm_config.tensogram_compression
            if isinstance(tgm_config, TensogramTemplateConfig)
            else "blosc2"
        )
        allow_nan = (
            tgm_config.tensogram_allow_nan
            if isinstance(tgm_config, TensogramTemplateConfig)
            else True
        )
        allow_inf = (
            tgm_config.tensogram_allow_inf
            if isinstance(tgm_config, TensogramTemplateConfig)
            else True
        )
        files = batch.items if batch.items else batch.metadata.get("files", [])
        groups = self.get_batch_groups(files, ctx)
        if not groups:
            groups = ["default"]

        target_path = local_path_from_target(target)
        strategy = TensogramWriteStrategy(
            target=str(target_path),
            compression=compression,
            source_uri=str(ctx.source),
            allow_nan=allow_nan,
            allow_inf=allow_inf,
            time_dim_name=self._resolve_time_dim_name(),
            logger=self._log,
        )

        write_ctx = (
            cast(
                Any,
                telemetry.span(
                    "firecube.batch.tensogram_write",
                    {"firecube.target": str(target_path)},
                ),
            )
            if telemetry is not None
            else contextlib.nullcontext()
        )
        with write_ctx:
            tgm_metrics = strategy.write_groups(
                group_to_timestamps=dict.fromkeys(groups, files),
                dataset_for_batch=lambda g, items: self.build_dataset(g, list(items), ctx),
                batch_size=len(files),
            )

        final_metrics = dict(prep_metrics)
        final_metrics.update(
            {
                "tensogram": tgm_metrics,
                "count": len(files),
                "storage_handled": True,
            }
        )

        return PipelineResult(
            batch=batch,
            output_format="tensogram",
            outputs=OutputPaths(primary=str(target_path)),
            metrics=final_metrics,
            success=True,
        )

    except Exception as exc:
        self._log.exception("Tensogram batch write failed")
        return PipelineResult(
            batch=batch,
            outputs=OutputPaths(primary=Path("")),
            success=False,
            error=str(exc),
        )

    finally:
        if strategy is not None:
            strategy.close()
        try:
            self.cleanup_batch_data(batch, ctx)
        except Exception as exc:
            self._log.warning("Batch cleanup failed: %s", exc)
        self.batch_teardown(ctx)


def bind_tensogram_strategy(ingestor: Any) -> None:
    """Bind Tensogram batch processing to a dataset-producer ingestor instance."""
    ingestor.template_config_class = TensogramTemplateConfig
    ingestor._process_batch = types.MethodType(process_tensogram_batch, ingestor)


class GenericTensogramIngestor(BaseIngestor):
    """Template for plugins that write directly to Tensogram .tgm format."""

    template_config_class = TensogramTemplateConfig

    @abstractmethod
    def build_dataset(self, group: str, items: list[Any], ctx: PluginContext) -> xr.Dataset | None:
        """Convert a sub-batch of items into an xarray Dataset."""

    def get_batch_groups(self, items: Sequence[Any], ctx: PluginContext) -> list[str]:
        """Return list of Tensogram groups/messages to process."""
        return ["default"]

    def _process_batch(self, batch: PipelineBatch, ctx: PluginContext) -> PipelineResult:
        return process_tensogram_batch(self, batch, ctx)
