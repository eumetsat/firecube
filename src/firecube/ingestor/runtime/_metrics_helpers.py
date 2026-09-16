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

"""Private helpers for reading well-known metric values from typed ``ResultMetrics``.

Callers inside :mod:`firecube.ingestor.runtime` share the same authoritative
typed source for ``timestamps_skipped``: :attr:`ResultMetrics.pipeline.timestamps_skipped`.
Historical dict-shape fallbacks (``metrics["pipeline"]["timestamps_skipped"]``,
``metrics["zarr"]["timestamps_skipped"]``, and top-level ``metrics["timestamps_skipped"]``)
guarded against schemas that no longer exist and have been removed; the batch-level
zarr sub-map is normalized into the typed field by :func:`_coerce_result_metrics`,
so a single typed read covers every producer.

Plugins may still read ``metrics["pipeline"]["timestamps_skipped"]`` through the
compatibility mapping surfaced by :meth:`ResultMetrics.__getitem__` — that view is
rendered from the same typed field.

This module is private: do not import it from outside the runtime package and do
not re-export its symbols from :mod:`firecube.ingestor.api`.
"""

from __future__ import annotations

from typing import Any

from firecube.ingestor.types.result_metrics import ResultMetrics


def _non_negative_int(value: Any) -> int:
    """Coerce a value to a non-negative ``int``, defaulting to ``0`` on any mismatch."""
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int | float):
        return max(int(value), 0)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.isdecimal():
            return int(stripped)
    return 0


def _extract_timestamps_skipped(metrics: ResultMetrics | None) -> int:
    """Return the authoritative ``timestamps_skipped`` count from typed metrics.

    Reads :attr:`ResultMetrics.pipeline.timestamps_skipped` directly. Returns
    ``0`` when ``metrics`` is ``None`` or its ``pipeline`` field is unset.
    """
    if metrics is None:
        return 0
    pipeline = metrics.pipeline
    if pipeline is None:
        return 0
    return _non_negative_int(pipeline.timestamps_skipped)


__all__ = [
    "_extract_timestamps_skipped",
    "_non_negative_int",
]
