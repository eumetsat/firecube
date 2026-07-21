# Ideas

Speculative ideas only. Items here are not accepted scope. Promote to [TODO.md](TODO.md) only after the workflow, constraints, and tradeoffs are discussed and the decision is recorded in [DONE.md](DONE.md) with a date.

## Status Tags

- **ACCEPTED-V1** — included in v1 scope, see [DONE.md](DONE.md)
- **REJECTED-V1** — explicitly out, see [DONE.md](DONE.md)
- **DEFERRED-V2+** — plausible, blocked on a prerequisite
- **UNDECIDED** — open for discussion

## Undecided

### §15 Dependency-aware control-plane metadata (pre-native dependent-arrays)

- UNDECIDED

- **Goal:** Capture primary/derived relationships in run/span control-plane records + ChunkManager now, without changing current Zarr physical layout yet.
- **Direction:** Treat this as a control-plane upgrade that paves the way for future dependent-arrays native mode and client adaptation.
- **Current status:** The WAL-backed `.firecube/` control-plane foundation is now in place and documented (`ChunkManager` facade, authoritative WAL under `.firecube/runs/`, derived snapshots, explicit snapshot rebuilds). This item remains open because dependency-aware metadata, queries, and rebuild planning have not been implemented yet.
- **Why now (links to existing items):**
  - Supports **safer resume semantics** (resolved; control-plane-primary `ResumeGuard`, see DONE.md) by making overlap/resume decisions dependency-aware.
  - Extends **meta-aware ChunkManager queries** (resolved; time-range and meta filters shipped, see DONE.md) with dependency filters.
  - Strengthens **control-plane records** (resolved; run/span WAL model shipped, see DONE.md) via role/staleness lifecycle.
  - Improves **time-window maintenance** (resolved; `firecube_timestamp_state` + deletion plans, see DONE.md) with derived-data rebuild targeting.
  - Aligns with **multires as a post-step** (resolved; TODO.md §11 / DONE.md §25) by tracking derived artifact freshness.
  - Adds concrete concurrency checks for **TODO.md §14** around stale/active transitions.
  - De-risks future **TODO.md §5 standards integration** by formalizing dependency metadata shape.
- **Scope (optional for now):**
  - Add dependency metadata fields in span/run `meta` (e.g. `role`, `artifact_kind`, `depends_on`, `staleness`).
  - Add dependency-aware list/filter in ChunkManager/CLI.
  - Add staleness propagation on replace/delete (append-only markers, no manifest rewrite).
  - Add `chunks verify-deps` and `chunks plan-rebuild` commands.
- **Non-goals (for this item):**
  - No native dependent-arrays physical writer yet.
  - No mandatory external client/backend adaptation yet.
  - No breaking schema reset; evolve `v1` with backward-compatible optional fields.

- **Remembered plan (phased, optional):**
  - **Phase A — Safety + interop guardrails (no layout change):**
    - Add dependent-array metadata parser/validator utilities in core Zarr helpers.
    - Add fail-closed write guards when dependency metadata is present but unmanaged.
    - Add read-only CLI inspection/validation commands for dependency metadata.
  - **Phase B — Bridge mode (current layout + explicit dependency control plane):**
    - Keep materialized multires/groups as-is.
    - Record dependency relationships in run/span control-plane metadata (`role`, `artifact_kind`, `depends_on`, `staleness`).
    - Add dependency-aware ChunkManager filters and stale/rebuild planning helpers.
  - **Phase C — Native mode (deferred until client adaptation exists):**
    - Add opt-in experimental native dependent-arrays writer.
    - Enforce strict chunk isolation + coordinated writes + fail-closed behavior.
    - Promote only after cross-repo tests and operational soak.
- **Validation notes (when implemented):**
- Core: `uv run ruff check .`, `uv run pyright`, and `uv run pytest`.
  - Plugin (`firecube-msg-frm`): install local core checkout, then run plugin lint/tests.
  - Runtime smoke: plugin discovery + lightweight ingest path with dependency checks.
- **References discussed:**
  - Dependent arrays proposal: `https://github.com/d-v-b/dependent-arrays`
  - Live TODO anchors: §5, §14, and this item §15. (The other anchors originally listed here — resume semantics, ChunkManager queries, control-plane records, time-window maintenance, multires — are resolved and recorded in DONE.md.)
  - Relevant core modules:
    - `src/firecube/ingestor/runtime/zarr/append.py`
    - `src/firecube/core/controlplane/manager.py`
    - `src/firecube/core/controlplane/repo.py`
    - `src/firecube/ingestor/runtime/recording.py`
    - `src/firecube/cli/zarr.py`

### §20.1 Verify the real failure mode first (pod exit)

- UNDECIDED

- Add an integration-style container check that verifies three cases explicitly:
  - `ClickException` path exits non-zero
  - uncaught exception path exits non-zero
  - `SIGTERM` during active ingest eventually terminates the process
- Capture both stdout/stderr and the container exit code so the failure mode is observable instead of inferred.
- Do not assume Firecube itself is printing structured JSON errors on unhandled exceptions until that path is located and verified.

### §20.3 Secondary checks (TPE lifecycle, claim heartbeat threads)

- UNDECIDED

- **ThreadPoolExecutor lifecycle:** verify pipeline worker threads are always unwound by executor context managers during exceptions. Treat this as a lower-priority audit item, not the primary suspect.
- **Claim heartbeat threads:** already use `daemon=True`; keep this verified but do not treat them as the likely blocker unless evidence changes.
- **Other non-daemon threads:** audit all remaining `threading.Thread` creation and require either explicit join or `daemon=True`.

### §20.4 SIGTERM handling (only if diagnosis proves it necessary)

- UNDECIDED

- Verify the current SIGTERM behavior inside the container before adding a custom handler.
- Only add a Firecube-level SIGTERM handler if default Python/Click shutdown is proven insufficient for:
  - flushing logs/telemetry
  - releasing resources promptly
  - returning a correct non-zero termination signal to the orchestrator
- Avoid adding a handler that bypasses normal cleanup or introduces new shutdown races.

### §20.5 Last-resort safeguard (forced-exit watchdog)

- UNDECIDED

- Consider a process-level forced-exit watchdog only if:
  - the real root cause is understood,
  - cleaner shutdown paths are still insufficient, and
  - the container can otherwise hang indefinitely.
- If added, document clearly that it is an emergency fallback because it bypasses normal finalizers.

### Upstream tensogram feature requests (optional, not blockers)

- UNDECIDED

Two enhancement ideas surfaced while diagnosing the archive-restore fidelity bug (TODO §8, "Verified broken today"). Both would simplify firecube's restore path but the bug is fully fixable on our side without them:

- **tensogram-xarray — first-class variable-attrs convention:** the engine consumes `name`/`dim_names` from each `base` entry and passes the remaining keys through wholesale as variable attrs. A sanctioned `attrs` key (applied as actual variable attrs, with everything else ignored) would stop consumers from having to strip their own plumbing keys after decode.
- **tensogram — logical datetime dtype hint:** descriptors only carry numeric dtypes, so every consumer hand-rolls a datetime64→float64 epoch convention at encode and has no marker to invert it at decode. A logical dtype hint (e.g. `datetime64[ns]`) would let the xarray engine round-trip time coordinates natively.

File upstream at `https://github.com/ecmwf/tensogram` if Phase 1 implementation friction justifies it.

### Phase 4 — AppendStrategy parallel ingestion (speculative)

- UNDECIDED

- **Goal:** Extend slot-range parallelism to `GenericZarrIngestor` / `AppendStrategy` plugins.
- **Blocker:** `AppendStrategy` uses a process-local lock and xarray-append semantics. The append cursor is mutable state that cannot be safely shared across pods without a distributed lock or a fundamentally different write model.
- **Possible direction:** Replace the append cursor with a pre-declared time-index mapping (similar to Phase 3's `timestamp_to_ts_index`) and switch `AppendStrategy` to region writes for the parallel path. This would effectively converge `GenericZarrIngestor` toward `DirectZarrIngestor` for the parallel case.
- **Non-goals:** Do not break the existing single-pod `AppendStrategy` path. Any parallel extension must be opt-in.
- **Prerequisite:** Phase 3 (`DirectZarrIngestor` parallelism) must be stable in production before this is considered.

### §21 Promote shared slot-range machinery to core (cross-plugin dedup)

- UNDECIDED

- **Origin:** Surfaced while reviewing `firecube-opera-seviri-nordlis` and
  `firecube-mtg-fci-l1c`. Both plugins now opt into
  `SUPPORTS_SLOT_RANGE_PARALLELISM` and independently implement the same generic
  slot-anchor machinery (the data-physics differences are legitimate; the
  *generic* anchor/epoch/shard scaffolding is what's duplicated).
- **Idea 1 — epoch/ISO↔seconds helpers as a shared util.** Both plugins convert
  between ISO timestamps and Unix seconds (OPERA used a private `_epoch.py`,
  since deleted in favour of the new `firecube.core.api` epoch helpers; MTG uses
  inline `datetime` math). The core helpers (`iso_to_epoch_s`/`epoch_s_to_iso`/
  `normalize_epoch_iso`) already cover OPERA; confirm MTG adopts them too so
  there's one implementation.
- **Idea 2 — a core `SlotIndexModel` / `SlotRangeSupport` mixin.** OPERA's
  reference-epoch-mismatch guard and MTG's `_ensure_index_model_attrs`
  epoch-stamp/validate solve the identical generic problem (refuse to append
  under a misaligned slot model). The new `ChunkManager.ensure_slot_index_model`
  is the foundation; a thin mixin could let cadence-based plugins declare
  `(epoch, per-group cadence)` and inherit the anchor/append-safety behaviour
  instead of re-deriving it. Prevents drift as more parallel-capable plugins
  appear.
- **Idea 3 — expose `read_chunk_grid_with_shards` on the public API.** OPERA's
  former `compat.py` hand-read shard layout (a real bug source, now deleted);
  the canonical `firecube.core.zarr.validation.read_chunk_grid_with_shards`
  lives below the public `firecube.core.api` line, so any plugin needing
  shard-aware layout must deep-import it. Promoting it (or having core own
  existing-store compat end-to-end) removes the second shard reader.
- **Status:** Plugin-side cleanup is already done (OPERA deleted `compat.py`/
  `_epoch.py`, made `zarr_schema` pure, added `slot_index_model`). These three
  items are *core-side* promotions, only worth doing if a second/third
  parallel-capable plugin keeps the duplication alive — which is now the case.

### §22 Make `firecube zarr preallocate` usable by cadence-based plugins — RESOLVED (2026-06-25)

- RESOLVED (2026-06-25)

- **Origin:** Trying to pre-size an OPERA store for long-horizon parallel
  appends (axis can't grow in parallel mode, so it must be allocated up front).
  `firecube zarr preallocate` is the intended tool but currently does not work
  for `firecube-opera-seviri-nordlis`; the working stand-in is ingest-side
  over-allocation (`--option expected_timesteps_per_group`, wrapped as
  `opera-ingest.sh --horizon YYYYMMDD`).
- **Gap 1 — preallocate ignores plugin `--option`.** `cli/zarr.py:preallocate`
  coerces `--option` pairs into `IngestContext.options` but never builds/applies
  the plugin's typed `plugin_config` from them (unlike `firecube ingest`). So
  hooks that read `self.plugin_config` (`reference_epoch`,
  `expected_timesteps_per_group`, `product_groups`, `cadence_overrides`) see
  only defaults. Net: epoch/horizon can be supplied **only** via `--input-data`
  day directories, not via `--option`. Fix: configure the ingestor from the
  coerced options before calling `slot_index_model`/`global_expected_time_count`/
  `zarr_schema` (mirror the ingest path).
- **Gap 2 — preallocate creates arrays without their declared attributes.** The
  arrays are allocated with shape/dtype/chunks but not the spec's `attrs` (e.g.
  `units`), so a subsequent `firecube ingest` fails in `verify_array_spec` with
  `SchemaDriftError: attrs['units'] existing=None spec='mm h-1'`. Preallocate
  must write the same attrs the ingest path declares, or the two paths will
  never agree for any plugin that sets variable attributes.
- **Why it matters:** Without these, cadence-based plugins cannot use the
  sanctioned preallocate workflow and must fall back to ingest-side
  over-allocation. Fixing both makes `preallocate` the single clean way to size
  a store before a parallel backfill/append campaign.
- See: DONE.md "firecube zarr preallocate — typed-config and spec-attrs parity" (2026-06-25) and `.sisyphus/plans/preallocate-typed-config-and-attrs.md`.

### §F3 Lazy / file-backed payload for `WriteIntent`s

- PROMOTED (2026-07-12) — see TODO.md §F3 for accepted scope, evidence, and design constraints.

### §A — `kind="static"` for `time_indexed=True` arrays (pure-static intent model)

- DEFERRED-V2+

- **Problem:** Plugins wanting a "write-once time-indexed array" must currently use `time_indexed=False` + `kind="static"` as a workaround. The `time_indexed=False` declaration forces an explicit `shape=(N,)` and opts the array out of time-axis preallocation sizing. The TODO §30 union fix (commit 7ff079c) makes this safe: the `time_indexed=False` array is correctly excluded from the time-axis bounds check, and xarray still joins it with `time_indexed=True` data arrays on the shared `time` dim name. Two production callers (MTG FCI L1c and OPERA) both work fine today.
- **What a pure model would look like:** `time_indexed=True` + `kind="static"` — "this array IS time-indexed (its size grows with the time horizon), but its values are deterministic and written once". The `firecube_static_written` marker would still gate write-once semantics: marker absent means write and stamp; marker present means replay-or-raise.
- **Why deferred:** The `time_indexed=False` workaround is non-invasive and self-documenting. No third plugin has surfaced the confusion yet, and no OOM or preallocation mismatch has been observed in practice.
- **Trigger conditions (when to revisit):**
  - (a) A third plugin finds the `time_indexed=False` workaround confusing or error-prone, OR
  - (b) Someone wants `time_indexed=True` static arrays to participate in time-axis preallocation sizing (the workaround skips this, which is currently fine), OR
  - (c) A refactor simplifies `IndexedRegionStrategy._dispatch_static_intent` enough that handling the `time_indexed=True + kind="static"` combination becomes a small, standalone change.
- **Architectural shape if pursued:** A small bookkeeping change in `IndexedRegionStrategy._dispatch_static_intent` (`src/firecube/ingestor/runtime/zarr/strategies/indexed_region.py`, lines 334-367) to handle `time_indexed=True + kind="static"` without assuming the array is non-time-indexed. `RegionZarrWriter.write_static` likely needs no change — the write-once marker logic is already array-content-agnostic.
- **Cross-references:** DONE.md entry added by T4 of `.sisyphus/plans/preallocate-typed-config-and-attrs.md`; `handoff-firecube-core-dense-time.md §A` (repo root, uncommitted).

### §16 — Wave 2: LSM active-run index + completed-slots bitmap for O(range) resume-guard

- UNDECIDED

- **Goal:** Reduce `ResumeGuard.enforce()` from O(N) to O(range) by adding a derived read-model (per DESIGN §27) that captures active runs and completed slots without depending on full run-directory enumeration. Wave 1 (commit `7847254`) added memoization as a short-term fix; Wave 2 replaces the underlying scan with a purpose-built index.

- **Structural design (LSM-style):**
  - **Active-run marker files** under `.firecube/active/<run_id>` — created BEFORE `EVENT_RUN_STARTED` WAL append, deleted AFTER terminal event append. Ordering ensures the marker overhangs both crash windows (prefer false blockers over missed conflicts).
  - **Completed-slots bitmap:** immutable base per `(group, fixed_slot_block)` rebuilt at snapshot time, plus immutable per-completion delta shards. Sharded by STABLE `(group, fixed_slot_block)` — not pod-assigned ranges — so orchestration replay produces identical index layout.
  - **Write-ordering rules:** prefer false blockers over missed conflicts at every crash window. A marker present with no WAL entry is a safe false-positive; a missing marker with a WAL entry is a missed conflict.
  - **Rebuild-from-WAL protocol:** snapshot compaction rebuilds the base bitmap; delta shards can be discarded after inclusion in the next base.

- **Code sites this design addresses:**
  - `_snapshot.py:138` — second `list_run_entries` call that full-scans the run directory; Wave 2's active-run index eliminates this.
  - `resume_guard.py:405-410` — completed-span check with no slot-range predicate; Wave 2's bitmap replaces the linear scan with a range lookup.

- **Test gaps to cover before accepting:**
  - Crash-window ordering: marker written but no WAL entry yet; WAL terminal written but marker delete failed; delta shard written but WAL append failed.
  - Boundary correctness: adjacent inclusive vs half-open slot ranges in the bitmap.
  - Corruption/missing bitmap fallback: must degrade gracefully to full WAL replay, not silent skip.

- **Blocked on:** Wave 2 planning session confirming schema bump is acceptable and that the active-run marker directory does not conflict with any existing `.firecube/` layout invariants.

- **Cross-links:**
  - DONE.md entry closing Wave 1 (added by T16 of this plan).
  - Wave 1 memoization foundation: commit `7847254` (`perf(resume-guard): memoize run-entry scan`).
  - DESIGN.md §27 (derived read-model doctrine).

### §17 — Wave 3: terminal run pruning + auto-rebuild triggers

- UNDECIDED / DEFERRED (until Wave 2 lands)

- **Goal:** Bound the growth of `.firecube/runs/` directory count and eliminate manual-rebuild-on-completion friction. Wave 2 makes reads cheap regardless of directory count, but doesn't shrink it. Wave 3 addresses the accumulation problem.

- **Fix 4 — prune terminal run dirs past a cutoff:**
  - Caveat: `_load_history_records` at `_projection.py:182-193` iterates ALL runs to reconstruct `chunks list --include-replaced` history. Pruning breaks that history for pruned generations.
  - Safe options:
    - (a) Prune only `complete` runs (not `failed` or `abandoned`) past the cutoff, accepting replaced-chunk-history loss for that generation.
    - (b) Archive rather than delete: compress and move to `runs-archive/`, preserving history at the cost of a two-tier scan.
  - Decision deferred: neither option is obviously correct without knowing how often `--include-replaced` history is queried in practice.

- **Fix 5 — auto-rebuild snapshot on ingest completion:**
  - Caveat: at K8s scale, "completion" is one pod ending out of potentially hundreds per campaign. Auto-rebuild on every pod exit would thrash and mostly fail (rebuild is blocked by non-terminal runs from concurrent pods).
  - Need either a "quiet period" heuristic (rebuild only when no active runs remain) or a scheduled maintenance job decoupled from the ingest path.
  - Decision deferred: the right trigger depends on operational topology (single-pod vs multi-pod campaigns) and is not yet known.

- **Code sites this design addresses:**
  - `_projection.py:182-193` — `_load_history_records` that iterates all runs; pruning must not silently break this.

- **Blocked on:** Wave 2 landing (the active-run index and bitmap make it safe to reason about "no active runs" without a full scan, which is a prerequisite for the quiet-period heuristic in Fix 5).

## Notes

Move an idea to [TODO.md](TODO.md) only after the workflow, constraints, and tradeoffs are agreed and the decision is recorded in [DONE.md](DONE.md) with a date.
