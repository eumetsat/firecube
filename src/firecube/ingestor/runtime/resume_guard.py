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

"""Resume/overwrite safety guard based on ChunkManager manifests (optional Zarr validation)."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, cast

from firecube.core.observability.metrics import ResumeGuardMetrics, resume_guard_span
from firecube.core.zarr.validation import validate_group_with_fs
from firecube.ingestor.errors import RangeOverlapError, ResumeConflictError
from firecube.ingestor.runtime.resume_types import ResumeDecision, ResumeVerdict


def _ranges_overlap(a: tuple[int, int], b: tuple[int, int]) -> bool:
    """Return True if half-open ranges [start, end) overlap.

    Back-to-back ranges like [0,100) and [100,200) are disjoint.
    """
    return a[0] < b[1] and b[0] < a[1]


def _ranges_overlap_inclusive_vs_halfopen(
    inclusive: tuple[int, int], halfopen: tuple[int, int]
) -> bool:
    """Check if INCLUSIVE [a, b] overlaps HALF-OPEN [c, d).

    Boundary cases:
    - [0, 9] vs [10, 20)  → DISJOINT (0 < 20 AND 10 <= 9 → False)
    - [0, 10] vs [10, 20) → OVERLAP  (0 < 20 AND 10 <= 10 → True)
    - [10, 20] vs [0, 10) → DISJOINT (10 < 10 → False)
    - [9, 20] vs [0, 10)  → OVERLAP  (9 < 10 AND 0 <= 20 → True)
    """
    inc_start, inc_end = inclusive
    ho_start, ho_end = halfopen
    return inc_start < ho_end and ho_start <= inc_end


@dataclass(slots=True)
class ResumeGuard:
    """Enforce resume / overwrite safety before an ingest run starts.

    Decision matrix (evaluated in order):

    ┌──────────────────────────┬─────────────────┬───────────────┬─────────────────────┐
    │ non-terminal run exists  │ force_reingest  │ resume_exist. │ outcome             │
    ├──────────────────────────┼─────────────────┼───────────────┼─────────────────────┤
    │ True                     │ False           │ any           │ BLOCK_STALE_RUN     │
    │ True                     │ True            │ any           │ continue (overwrite)│
    │ False                    │ False           │ False         │ RuntimeError(conf.) │
    │ False                    │ False           │ True          │ continue (resume)   │
    │ False                    │ True            │ any           │ continue (overwrite)│
    └──────────────────────────┴─────────────────┴───────────────┴─────────────────────┘

    When ``validate_zarr=true`` is also set, ``_run_optional_validation`` is
    called to check whether the Zarr store actually contains chunks matching
    the manifest.  This can be slow on large stores and should be reserved for
    debugging or post-incident analysis.

    Slice matching: if a plugin declares ``slice_meta_keys`` (e.g.
    ``["msg_region", "forecast_horizons"]``), the guard compares those values
    against existing span records to narrow conflicts to the same logical slice
    rather than the entire product.  Records that lack those keys are treated
    conservatively as potential conflicts.
    """

    plugin_name: str
    chunk_manager: Any
    log: logging.Logger
    slice_meta_keys: Sequence[str]
    last_metrics: ResumeGuardMetrics | None = None

    def _log_decision(self, decision: ResumeDecision) -> None:
        self.log.debug("Resume decision: %s — %s", decision.verdict.value, decision.reason)

    def enforce(
        self,
        *,
        ctx: Any,
        product: str,
        slice_meta: dict[str, Any] | None = None,
        slot_range: tuple[int, int] | None = None,
        slot_group: str | None = None,
        validation_group: str | None = None,
    ) -> None:
        with resume_guard_span() as metrics:
            try:
                with self.chunk_manager.repo.run_entries_cache_scope():
                    self._enforce(
                        ctx=ctx,
                        product=product,
                        slice_meta=slice_meta,
                        slot_range=slot_range,
                        slot_group=slot_group,
                        validation_group=validation_group,
                        metrics=metrics,
                    )
            finally:
                self.last_metrics = metrics
        self.log.info(
            "resume-guard: duration=%.4fs runs_enumerated=%d spans_scanned=%d product=%s",
            metrics.enforce_duration_s,
            metrics.runs_enumerated,
            metrics.spans_scanned,
            product,
        )

    def _record_runs_enumerated(self, *, product: str, metrics: ResumeGuardMetrics) -> None:
        """Read cache size defensively; tolerates MagicMock chunk_managers in existing tests."""
        repo = getattr(self.chunk_manager, "repo", None)
        cache = getattr(repo, "_run_entries_cache", None) if repo is not None else None
        if cache is None or not hasattr(cache, "entries_by_product"):
            return
        try:
            entries = cache.entries_by_product.get(product, [])
        except (AttributeError, TypeError):
            return
        if isinstance(entries, list):
            metrics.runs_enumerated = len(entries)

    def _enforce(
        self,
        *,
        ctx: Any,
        product: str,
        slice_meta: dict[str, Any] | None = None,
        slot_range: tuple[int, int] | None = None,
        slot_group: str | None = None,
        validation_group: str | None = None,
        metrics: ResumeGuardMetrics,
    ) -> None:
        resume_existing = bool(ctx.option("resume_existing", False))
        validate_zarr = bool(ctx.option("validate_zarr", False))
        force_reingest = bool(getattr(ctx, "force_reingest", False)) or bool(
            ctx.option("force_reingest", False)
        )

        slice_meta = dict(slice_meta or {})
        self._check_time_coord_seal(product=product, slot_group=slot_group)

        try:
            self._check_non_terminal_runs(
                product=product,
                force_reingest=force_reingest,
                resume_existing=resume_existing,
                slot_range=slot_range,
                new_slot_group=slot_group,
            )
        except ResumeConflictError:
            self._log_decision(
                ResumeDecision(
                    verdict=ResumeVerdict.BLOCK_STALE_RUN,
                    reason="Non-terminal run(s) exist",
                )
            )
            raise
        finally:
            self._record_runs_enumerated(product=product, metrics=metrics)

        # Phase 3.1 T5: Slot-range-aware completed-span check (split bypass).
        # When slot_range is set, the new pod uses group+range-aware overlap instead of
        # the broad legacy check below (which treats ANY plugin span as a conflict and
        # incorrectly blocks disjoint slot-range pods).
        if slot_range is not None:
            if not force_reingest:
                metrics.spans_scanned = self._check_completed_spans_for_slot_overlap(
                    product=product,
                    slot_range=slot_range,
                    slot_group=slot_group,
                )
            self._log_decision(
                ResumeDecision(
                    verdict=ResumeVerdict.PROCEED_FRESH,
                    reason="Slot-range mode: completed-span overlap check passed "
                    "(or bypassed via force_reingest)",
                )
            )
            return  # CRITICAL: Skip legacy broad completed-span check below

        # Below: legacy/single-pod path (slot_range is None)
        plugin_filter = {"plugin": self.plugin_name}
        run_time_min = slice_meta.get("time_min")
        run_time_max = slice_meta.get("time_max")

        if run_time_min and run_time_max:
            existing_records = list(
                self.chunk_manager.list_chunks(
                    product=product,
                    chunk_type="span",
                    include_replaced=False,
                    meta=plugin_filter,
                    time_overlaps=(run_time_min, run_time_max),
                )
            )
            all_plugin_spans = self.chunk_manager.list_chunks(
                product=product,
                chunk_type="span",
                include_replaced=False,
                meta=plugin_filter,
            )
            seen_keys = {getattr(record, "key", None) for record in existing_records}
            for span in all_plugin_spans:
                meta = getattr(span, "meta", None) or {}
                if (
                    "time_min" not in meta
                    and "time_max" not in meta
                    and getattr(span, "key", None) not in seen_keys
                ):
                    existing_records.append(span)
        else:
            existing_records = self.chunk_manager.list_chunks(
                product=product,
                chunk_type="span",
                include_replaced=False,
                meta=plugin_filter,
            )

        metrics.spans_scanned = len(existing_records)

        if self.slice_meta_keys and not (run_time_min and run_time_max):
            missing_ctx_keys = [k for k in self.slice_meta_keys if k not in slice_meta]
            if missing_ctx_keys and existing_records:
                raise ResumeConflictError(
                    f"Existing entries for product '{product}' (plugin={self.plugin_name}) detected but "
                    f"this run is missing slice options needed for safe matching: {missing_ctx_keys}. "
                    "Provide those options to enable safe slice matching, or rerun with "
                    "--option resume_existing=true / --option force_reingest=true "
                    "(optionally --option validate_zarr=true to inspect storage; may be slow)."
                )

            comparable: list[Any] = []
            unknown: list[Any] = []
            matching: list[Any] = []

            for rec in existing_records:
                meta = getattr(rec, "meta", None)
                if not isinstance(meta, dict):
                    unknown.append(rec)
                    continue

                if all(k in meta for k in self.slice_meta_keys):
                    comparable.append(rec)
                    if all(meta.get(k) == slice_meta.get(k) for k in self.slice_meta_keys):
                        matching.append(rec)
                else:
                    unknown.append(rec)

            # Exact slice match is a conflict.
            if matching:
                existing_records = matching
            elif unknown:
                # Legacy entries exist without full slice meta; be conservative.
                existing_records = unknown
            else:
                existing_records = []

        if not existing_records:
            self._log_decision(
                ResumeDecision(
                    verdict=ResumeVerdict.PROCEED_FRESH,
                    reason="No existing spans",
                )
            )
            return

        if force_reingest:
            if validate_zarr:
                self._run_optional_validation(
                    ctx=ctx,
                    product=product,
                    validation_group=validation_group,
                    warn_only=True,
                )
            self._log_decision(
                ResumeDecision(
                    verdict=ResumeVerdict.PROCEED_RESUME,
                    reason="force_reingest=True",
                )
            )
            return

        if resume_existing:
            if validate_zarr:
                self._run_optional_validation(
                    ctx=ctx,
                    product=product,
                    validation_group=validation_group,
                    warn_only=True,
                )
            self._log_decision(
                ResumeDecision(
                    verdict=ResumeVerdict.PROCEED_RESUME,
                    reason="resume_existing=True",
                )
            )
            return

        if validate_zarr:
            has_chunks = self._run_optional_validation(
                ctx=ctx,
                product=product,
                validation_group=validation_group,
                warn_only=False,
            )
            if has_chunks:
                self._log_decision(
                    ResumeDecision(
                        verdict=ResumeVerdict.BLOCK_CONFLICT,
                        reason="Existing data, no resume flag",
                    )
                )
                raise ResumeConflictError(
                    f"Existing entries for product '{product}' (plugin={self.plugin_name}) detected. "
                    "Rerun with --option resume_existing=true to continue, "
                    "--option force_reingest=true to overwrite."
                )
            self._log_decision(
                ResumeDecision(
                    verdict=ResumeVerdict.BLOCK_CONFLICT,
                    reason="Existing data, no resume flag",
                )
            )
            raise ResumeConflictError(
                f"Manifest entries for product '{product}' (plugin={self.plugin_name}) exist but no chunks were found. "
                "Consider scrubbing or rerun with --option force_reingest=true to overwrite."
            )

        self._log_decision(
            ResumeDecision(
                verdict=ResumeVerdict.BLOCK_CONFLICT,
                reason="Existing data, no resume flag",
            )
        )
        raise ResumeConflictError(
            f"Existing entries for product '{product}' (plugin={self.plugin_name}) detected. "
            "Rerun with --option resume_existing=true to continue, "
            "or --option force_reingest=true to overwrite. "
            "Use --option validate_zarr=true to inspect storage (may be slow)."
        )

    def _check_time_coord_seal(self, *, product: str, slot_group: str | None) -> None:
        """Block ingest when a ConsolidatedTimeCoord WAL event sealed this cube."""

        list_events = getattr(self.chunk_manager, "list_time_coord_consolidations", None)
        if not callable(list_events):
            return
        sealing_events = list(cast(Any, list_events)(product=product))
        if not sealing_events:
            return
        requested_group = slot_group.strip("/") if isinstance(slot_group, str) else None
        if requested_group is None:
            latest = sealing_events[-1]
            group = next(iter(latest.groups), "")
        else:
            matching = [event for event in sealing_events if requested_group in event.groups]
            if not matching:
                return
            latest = matching[-1]
            group = requested_group
        target = self.chunk_manager.get_product_root(product).rstrip("/")
        group_suffix = f"/{group}" if group else ""
        raise ResumeConflictError(
            f"Cube {target}{group_suffix} is sealed "
            f"(consolidated at {latest.timestamp_iso}). Further ingest is blocked."
        )

    def _check_non_terminal_runs(
        self,
        *,
        product: str,
        force_reingest: bool,
        resume_existing: bool = False,
        slot_range: tuple[int, int] | None = None,
        new_slot_group: str | None = None,
    ) -> None:
        """Block if any conflicting non-terminal runs exist for this product+plugin."""
        non_terminal = self.chunk_manager.list_runs(product=product, non_terminal=True)
        if not non_terminal:
            return
        if force_reingest:
            self.log.warning(
                "Non-terminal run(s) exist for product=%s but force_reingest=true, proceeding.",
                product,
            )
            return

        for run in non_terminal:
            existing_slot_range = run.slot_range
            abandon_cmd = (
                f"firecube chunks runs abandon --product-name {product} --run-id {run.run_id} "
                '--reason "<reason>"'
            )

            if slot_range is not None and existing_slot_range is None:
                if run.stale:
                    self.log.info(
                        "Non-range run %s for product=%s is stale; allowing range invocation.",
                        run.run_id,
                        product,
                    )
                    continue
                raise ResumeConflictError(
                    f"Non-range run {run.run_id!r} is active for product '{product}'. "
                    "Abandon it first to start a slot-range parallel run:\n"
                    f"  {abandon_cmd}"
                )

            if slot_range is not None and existing_slot_range is not None:
                existing_slot_group = getattr(run, "slot_group", None)
                if (
                    new_slot_group is not None
                    and existing_slot_group is not None
                    and new_slot_group != existing_slot_group
                ):
                    self.log.info(
                        "Active run %s (slot_group=%s) targets different group than new run "
                        "(slot_group=%s); allowing.",
                        run.run_id,
                        existing_slot_group,
                        new_slot_group,
                    )
                    continue
                if not _ranges_overlap(existing_slot_range, slot_range):
                    self.log.info(
                        "Active run %s (slot_range=%s) is disjoint from new range %s; allowing.",
                        run.run_id,
                        existing_slot_range,
                        slot_range,
                    )
                    continue
                if existing_slot_range == slot_range:
                    if resume_existing:
                        self.log.warning(
                            "Resuming active run %s with same slot_range=%s for product=%s.",
                            run.run_id,
                            slot_range,
                            product,
                        )
                        continue
                    raise ResumeConflictError(
                        f"Run {run.run_id!r} with same slot_range={slot_range} is active "
                        f"for product '{product}'. Resume with --option resume_existing=true or "
                        f"abandon:\n  {abandon_cmd}"
                    )
                raise RangeOverlapError(
                    f"Run {run.run_id!r} with slot_range={existing_slot_range} overlaps "
                    f"new slot_range={slot_range} for product '{product}'. "
                    "Overlapping ranges risk Zarr chunk corruption. "
                    f"Abandon the conflicting run first:\n  {abandon_cmd}"
                )

            raise ResumeConflictError(
                f"Non-terminal run(s) [{run.run_id}] exist for product '{product}'. "
                f"Abandon them first:\n  {abandon_cmd}"
            )

    def _check_completed_spans_for_slot_overlap(
        self,
        *,
        product: str,
        slot_range: tuple[int, int],
        slot_group: str | None,
    ) -> int:
        """Block if any completed span overlaps the pod's slot_range within slot_group.

        Uses inclusive [start, end] vs half-open [start, end) overlap formula.
        Group-aware: when slot_group is set, only considers spans for that group.
        Returns the number of completed spans examined (for telemetry).
        """
        completed_spans = self.chunk_manager.list_chunks(
            product=product,
            chunk_type="span",
            include_replaced=False,
            meta={"plugin": self.plugin_name},
        )
        overlapping = []
        for span in completed_spans:
            if not isinstance(getattr(span, "record", None), dict):
                continue
            span_payload = span.record.get("span") or {}
            if not isinstance(span_payload, dict):
                continue
            span_group = (getattr(span, "meta", None) or {}).get("group")
            # Group-aware: if slot_group set, only check spans for same group;
            # if slot_group is None (covers all groups), check all spans.
            if slot_group is not None and span_group is not None and span_group != slot_group:
                continue
            time_index_ranges = span_payload.get("time_index_ranges") or []
            for range_entry in time_index_ranges:
                if len(range_entry) < 2:
                    continue
                range_start, range_end = int(range_entry[0]), int(range_entry[1])
                if _ranges_overlap_inclusive_vs_halfopen((range_start, range_end), slot_range):
                    overlapping.append(
                        (getattr(span, "key", None), span_group, [range_start, range_end])
                    )
                    break
        if overlapping:
            raise ResumeConflictError(
                f"Completed spans overlap new slot_range={slot_range} "
                f"(slot_group={slot_group!r}) for product '{product}'. "
                f"Overlapping: {overlapping[:5]}{'...' if len(overlapping) > 5 else ''}. "
                "Rerun with --option force_reingest=true to overwrite, "
                "or pass a disjoint --slot-start/--slot-end range."
            )
        return len(completed_spans)

    def _run_optional_validation(
        self,
        *,
        ctx: Any,
        product: str,
        validation_group: str | None,
        warn_only: bool,
    ) -> bool:
        """Probe the Zarr store to check whether chunks actually exist.

        Returns ``True`` if at least one array in the group has written indices
        (``max_index >= 0``), ``False`` otherwise (empty store or error).

        When ``warn_only=True`` (resume or force_reingest path), structural
        issues are logged as warnings but do not block execution.
        When ``warn_only=False`` (blocked path), the caller uses the return
        value to distinguish "manifest without data" from "manifest with data".
        """
        storage = getattr(ctx, "storage", None)
        output = getattr(storage, "output", None) if storage is not None else None
        if output is None:
            return False

        store_uri = output.product.product_uri

        group = str(ctx.option("validate_zarr_group", "")).strip("/")
        if not group and validation_group:
            group = str(validation_group).strip("/")

        try:
            _timeout_raw = ctx.option("validate_zarr_timeout_s", None)
            _max_chunks_raw = ctx.option("validate_zarr_max_chunks", None)
            _on_timeout = str(ctx.option("validate_zarr_on_timeout", "warn"))
            _timeout_s = float(_timeout_raw) if _timeout_raw is not None else None
            _max_chunks = int(_max_chunks_raw) if _max_chunks_raw is not None else None
            report = validate_group_with_fs(
                output.fs(),
                store_uri,
                group,
                timeout_s=_timeout_s,
                max_chunks=_max_chunks,
                on_timeout=_on_timeout,
            )
            has_chunks = any(v >= 0 for v in report.max_indices.values())
            if warn_only and (report.extra_chunks or report.missing_indices):
                self.log.warning(
                    "validate_zarr detected structural issues (extra=%d, missing=%d) for product=%s group=%s",
                    len(report.extra_chunks),
                    sum(len(v) for v in report.missing_indices.values()),
                    product,
                    group or "/",
                )
            return has_chunks
        except Exception as exc:
            self.log.warning(
                "validate_zarr failed for product=%s group=%s: %s",
                product,
                group or "/",
                exc,
            )
            return False
