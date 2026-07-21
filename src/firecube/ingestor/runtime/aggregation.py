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

"""Run-level metric aggregation utilities for plugin and engine integration."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from firecube.ingestor.types.context import PipelineRunState, RuntimeIngestContext

_RESERVED_AGGREGATE_METRICS_KEYS = frozenset({"pipeline"})


def merge_batch_metrics(ctx: RuntimeIngestContext, state: PipelineRunState) -> dict[str, Any]:
    """Merge successful batch metrics into one run-level mapping.

    Default policy:
    - Numeric values are summed.
    - List values are concatenated.
    - Other values keep the first successful value.
    - Zarr coverage is always merged under ``metrics["zarr"]["coverage"]``.
    """
    merged_metrics: dict[str, Any] = {}
    if ctx.output_format == "zarr":
        merged_metrics["zarr"] = {"coverage": []}

    for res in state.results:
        if not res.success or not res.metrics:
            continue

        if "zarr" in res.metrics:
            cov = res.metrics["zarr"].get("coverage")
            if cov:
                if "zarr" not in merged_metrics:
                    merged_metrics["zarr"] = {"coverage": []}
                merged_metrics["zarr"]["coverage"].extend(cov)

        for key, value in res.metrics.items():
            if key == "zarr":
                continue
            if isinstance(value, (int, float)):
                current = merged_metrics.get(key, 0)
                if isinstance(current, (int, float)):
                    merged_metrics[key] = current + value
                else:
                    merged_metrics[key] = value
            elif isinstance(value, list):
                if key not in merged_metrics:
                    merged_metrics[key] = []
                if isinstance(merged_metrics[key], list):
                    merged_metrics[key].extend(value)
            else:
                merged_metrics.setdefault(key, value)

    return merged_metrics


def normalize_plugin_aggregate_metrics(
    raw_metrics: Any,
    *,
    logger: logging.Logger,
    plugin_name: str,
) -> dict[str, Any]:
    """Normalize plugin aggregate metrics and enforce engine-owned key boundaries."""
    if raw_metrics is None:
        return {}
    if not isinstance(raw_metrics, Mapping):
        logger.warning(
            "Plugin '%s' returned non-mapping aggregate metrics (%s); using empty metrics.",
            plugin_name,
            type(raw_metrics).__name__,
        )
        return {}

    normalized: dict[str, Any] = {}
    dropped_keys: list[str] = []
    for key, value in raw_metrics.items():
        if not isinstance(key, str):
            dropped_keys.append(repr(key))
            continue
        normalized[key] = value

    if dropped_keys:
        logger.warning(
            "Plugin '%s' returned non-string aggregate metric keys; ignoring: %s",
            plugin_name,
            ", ".join(sorted(dropped_keys)),
        )

    for reserved_key in _RESERVED_AGGREGATE_METRICS_KEYS:
        if reserved_key in normalized:
            logger.warning(
                "Plugin '%s' returned reserved aggregate metrics key '%s'; ignoring plugin value.",
                plugin_name,
                reserved_key,
            )
            normalized.pop(reserved_key, None)
    return normalized
