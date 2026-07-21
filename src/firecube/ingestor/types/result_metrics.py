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

"""Typed result metrics and outputs for ingestion results."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from firecube.core.controlplane import SpanCoverage


@dataclass(slots=True)
class StorageMetrics:
    """Typed storage summary from plugin results."""

    path: str | None = None
    bytes: int = 0
    files: int = 0
    duration_s: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        """Render storage metrics as a compatibility dictionary."""
        return {
            "path": self.path,
            "bytes": self.bytes,
            "files": self.files,
            "duration_s": self.duration_s,
        }


@dataclass(slots=True)
class PipelineMetrics:
    """Typed pipeline metrics from plugin results."""

    duration_pipeline_s: float = 0.0
    rows_processed: int | None = None
    rows_ingested: int | None = None
    coverage: list[SpanCoverage] = field(default_factory=list)
    duration_upload_s: float = 0.0
    duration_total_s: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        """Render pipeline metrics as a compatibility dictionary."""
        return {
            "duration_pipeline_s": self.duration_pipeline_s,
            "rows_processed": self.rows_processed,
            "rows_ingested": self.rows_ingested,
            "coverage": list(self.coverage),
            "duration_upload_s": self.duration_upload_s,
            "duration_total_s": self.duration_total_s,
        }


@dataclass(slots=True)
class ResultMetrics:
    """Shared metrics shape used by both batch- and run-level results."""

    write_mode: str | None = None
    storage: StorageMetrics | None = None
    pipeline: PipelineMetrics | None = None
    storage_handled: bool = False
    _compat: dict[str, Any] = field(default_factory=dict, init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        """Seed compatibility mapping state from typed fields when needed."""
        if self._compat:
            return
        if self.write_mode is not None:
            self._compat["write_mode"] = self.write_mode
        if self.storage is not None:
            self._compat["storage"] = self.storage.to_dict()
        if self.pipeline is not None:
            self._compat["pipeline"] = self.pipeline.to_dict()
        if self.storage_handled:
            self._compat["storage_handled"] = self.storage_handled

    def to_dict(self) -> dict[str, Any]:
        """Render typed fields as a public dict (no private ``_compat`` leak)."""
        rendered: dict[str, Any] = {}
        if self.write_mode is not None:
            rendered["write_mode"] = self.write_mode
        if self.storage is not None:
            rendered["storage"] = self.storage.to_dict()
        if self.pipeline is not None:
            rendered["pipeline"] = self.pipeline.to_dict()
        if self.storage_handled:
            rendered["storage_handled"] = self.storage_handled
        for key, value in self._compat.items():
            if key not in rendered:
                rendered[key] = value
                continue
            existing = rendered[key]
            if isinstance(existing, dict) and isinstance(value, dict):
                merged = dict(value)
                merged.update(existing)
                rendered[key] = merged
        return rendered

    def get(self, key: str, default: Any = None) -> Any:
        """Return a compatibility-mapped metric value by key."""
        compat = self._compat
        if key == "write_mode":
            return self.write_mode if self.write_mode is not None else default
        if key == "storage_handled":
            return self.storage_handled
        if key == "storage":
            if self.storage is None:
                return compat.get(key, default)
            rendered = self.storage.to_dict()
            compat_storage = compat.get("storage")
            if isinstance(compat_storage, dict):
                for k, v in compat_storage.items():
                    if k not in rendered:
                        rendered[k] = v
            compat["storage"] = rendered
            return rendered
        if key == "pipeline":
            if self.pipeline is None:
                return compat.get(key, default)
            rendered = self.pipeline.to_dict()
            compat_pipeline = compat.get("pipeline")
            if isinstance(compat_pipeline, dict):
                for k, v in compat_pipeline.items():
                    if k not in rendered:
                        rendered[k] = v
            compat["pipeline"] = rendered
            return rendered
        if key in compat:
            return compat[key]
        return default

    def __getitem__(self, key: str) -> Any:
        """Provide mapping-style access for compatibility code paths."""
        value = self.get(key, None)
        if value is None and key not in {"storage_handled"}:
            raise KeyError(key)
        return value

    def __setitem__(self, key: str, value: Any) -> None:
        """Update a compatibility metric entry and keep typed fields in sync."""
        if key == "write_mode":
            self.write_mode = value
            self._compat[key] = value
            return
        if key == "storage":
            self.storage = _coerce_storage_metrics(value)
            self._compat[key] = (
                value
                if isinstance(value, dict)
                else self.storage.to_dict()
                if self.storage
                else None
            )
            return
        if key == "pipeline":
            self.pipeline = _coerce_pipeline_metrics(value)
            self._compat[key] = (
                value
                if isinstance(value, dict)
                else self.pipeline.to_dict()
                if self.pipeline
                else None
            )
            return
        if key == "storage_handled":
            self.storage_handled = bool(value)
            self._compat[key] = self.storage_handled
            return
        raise KeyError(key)

    def items(self):
        """Return compatibility key/value pairs."""
        return self._compat.items() if self._compat else ((key, self[key]) for key in self.keys())

    def values(self):
        """Return compatibility values."""
        return self._compat.values() if self._compat else (self[key] for key in self.keys())

    def update(self, other: Any = None, /, **kwargs: Any) -> None:
        """Update compatibility metrics from another mapping or keyword args."""
        if other is not None:
            if hasattr(other, "items"):
                for key, value in other.items():
                    self[key] = value
            else:
                for key, value in other:
                    self[key] = value
        for key, value in kwargs.items():
            self[key] = value

    def setdefault(self, key: str, default: Any = None) -> Any:
        """Set a compatibility metric only when missing."""
        if key in self._compat:
            return self._compat[key]
        self[key] = default
        return self._compat.get(key, default)

    def keys(self) -> tuple[str, ...]:
        """Return the current compatibility keys in insertion order."""
        if self._compat:
            return tuple(self._compat.keys())
        keys: list[str] = []
        if self.write_mode is not None:
            keys.append("write_mode")
        if self.storage is not None:
            keys.append("storage")
        if self.pipeline is not None:
            keys.append("pipeline")
        if self.storage_handled:
            keys.append("storage_handled")
        return tuple(keys)

    def __iter__(self):
        """Iterate over compatibility metric keys."""
        return iter(self.keys())

    def __len__(self) -> int:
        """Return the number of populated compatibility keys."""
        return len(self.keys())

    def __contains__(self, key: object) -> bool:
        return key in self._compat


@dataclass(slots=True)
class OutputPaths:
    """Shared outputs shape used by both batch- and run-level results."""

    primary: Path | str | None = None
    zarr: Path | str | None = None

    def get(self, key: str, default: Any = None) -> Any:
        """Return an output path by compatibility key."""
        if key in {"primary", "output_path", "path"}:
            return self.primary if self.primary is not None else default
        if key == "zarr":
            if self.zarr is not None:
                return self.zarr
            if self.primary is not None:
                return self.primary
            return default
        return default

    def __getitem__(self, key: str) -> Any:
        value = self.get(key, None)
        if value is None:
            raise KeyError(key)
        return value

    def keys(self) -> tuple[str, ...]:
        """Return the populated output keys in insertion order."""
        keys: list[str] = []
        if self.primary is not None:
            keys.append("primary")
        if self.zarr is not None:
            keys.append("zarr")
        return tuple(keys)

    def __iter__(self):
        return iter(self.keys())

    def __len__(self) -> int:
        return len(self.keys())

    def items(self):
        """Return compatibility key/value pairs."""
        return ((key, self[key]) for key in self.keys())


def _coerce_storage_metrics(value: Any) -> StorageMetrics | None:
    if value is None or isinstance(value, StorageMetrics):
        return value
    if isinstance(value, dict):
        return StorageMetrics(
            path=value.get("path"),
            bytes=int(value.get("bytes", 0) or 0),
            files=int(value.get("files", 0) or 0),
            duration_s=float(value.get("duration_s", 0.0) or 0.0),
        )
    return None


def _coerce_pipeline_metrics(value: Any) -> PipelineMetrics | None:
    if value is None or isinstance(value, PipelineMetrics):
        return value
    if isinstance(value, dict):
        return PipelineMetrics(
            duration_pipeline_s=float(value.get("duration_pipeline_s", 0.0) or 0.0),
            rows_processed=value.get("rows_processed"),
            rows_ingested=value.get("rows_ingested"),
            coverage=_coerce_span_coverage_list(value.get("coverage")),
            duration_upload_s=float(value.get("duration_upload_s", 0.0) or 0.0),
            duration_total_s=float(value.get("duration_total_s", 0.0) or 0.0),
        )
    return None


def _coerce_span_coverage_list(value: Any) -> list[SpanCoverage]:
    if not value:
        return []
    spans: list[SpanCoverage] = []
    for item in value:
        if isinstance(item, SpanCoverage):
            spans.append(item)
        elif isinstance(item, dict) and "group" in item:
            spans.append(
                SpanCoverage(
                    group=item["group"],
                    arrays=list(item.get("arrays", [])),
                    time_index_ranges=(
                        list(item["time_index_ranges"]) if "time_index_ranges" in item else None
                    ),
                    aligned=bool(item.get("aligned", True)),
                    state_array=item.get("state_array"),
                    state_deleted_value=int(item.get("state_deleted_value", 2)),
                    time_min=item.get("time_min"),
                    time_max=item.get("time_max"),
                    region_spec=item.get("region_spec"),
                    write_strategy=item.get("write_strategy"),
                    time_dim_name=item.get("time_dim_name"),
                )
            )
    return spans


def _coerce_result_metrics(value: Any) -> ResultMetrics:
    if value is None:
        return ResultMetrics()
    if isinstance(value, ResultMetrics):
        return value
    if isinstance(value, dict):
        pipeline = _coerce_pipeline_metrics(value.get("pipeline"))
        if pipeline is None and value.get("coverage"):
            pipeline = PipelineMetrics(coverage=_coerce_span_coverage_list(value.get("coverage")))
        metrics = ResultMetrics(
            write_mode=value.get("write_mode"),
            storage=_coerce_storage_metrics(value.get("storage")),
            pipeline=pipeline,
            storage_handled=bool(value.get("storage_handled", False)),
        )
        metrics._compat = dict(value)
        return metrics
    return ResultMetrics()


def _coerce_output_paths(value: Any, *, output_format: str) -> OutputPaths:
    if isinstance(value, OutputPaths):
        outputs = value
    elif isinstance(value, dict):
        outputs = OutputPaths(
            primary=value.get("primary")
            or value.get("output_path")
            or value.get("path")
            or value.get(output_format),
            zarr=value.get("zarr"),
        )
    else:
        outputs = OutputPaths()

    if outputs.zarr is None and output_format == "zarr" and outputs.primary is not None:
        outputs.zarr = outputs.primary
    return outputs


__all__ = [
    "OutputPaths",
    "PipelineMetrics",
    "ResultMetrics",
    "StorageMetrics",
    "_coerce_output_paths",
    "_coerce_result_metrics",
]
