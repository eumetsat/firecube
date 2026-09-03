# Design

This document records the architecture rules, the locked design decisions, and the questions that drove them. New decisions are appended to [DONE.md](DONE.md) with a date. The content here is extracted from `AGENTS.md` and kept in sync with it. When a rule changes, update both files.

Firecube is a **batch ingestion worker** for EO datasets. It writes Zarr/Parquet and maintains a product-local `.firecube/` control-plane root used for idempotency, resume safety, cleanup, and write coordination. The design is shaped by two constraints: runs execute inside short-lived containers (Kubeflow/Argo model), and plugins must not reach into engine internals.

## Architectural Invariants

These rules are non-negotiable. Violating any of them requires a `plans/DONE.md` entry explaining the exception.

- **One container == one run** (Kubeflow/Argo/job-runner model). Do not add an internal scheduler/queue.
- Runs must be idempotent over explicit slices, such as horizon, group, time window, or slot range. Concurrency must be planned over disjoint slices. Append-style writes are serialized; direct-Zarr writes may run concurrently only for pre-planned disjoint regions protected by control-plane claims.
- **No shared mutation in workers**: worker threads/processors must only receive immutable context/options.
- **Manifest/cleanup bookkeeping is engine-owned**: plugins should not write manifests directly.
- **No ad-hoc storage wiring**: do not call `fsspec.filesystem(...)` or parse URIs directly in new code; use `src/firecube/core/filesystem/` + `src/firecube/core/uris.py` (or re-exports in `src/firecube/core/api.py`). Use `create_filesystem(config)` factory, never instantiate filesystem objects directly.
- **One driver everywhere**: `StorageConfig.storage_driver` selects fsspec (default) or obstore. If obstore, ALL write-domain I/O (zarr writes, control plane, uploads) uses obstore. No mixing.
- **Source-side fsspec is allowlisted, not a violation**: Input discovery, workspace materialization, and validation read paths intentionally use `_open_fsspec_url()` for arbitrary external inputs. Each call site is listed in `tests/unit/test_no_raw_fsspec_usage.py:_FSSPEC_PERMANENT_ALLOWLIST` with a `PERMANENT:` rationale. Write-domain enforcement is tested in `tests/integration/test_one_driver_invariant.py`.
- **Staged metadata seeding is a runtime-level concern, not a template-level concern.** Templates MUST NOT call `seed_staged_metadata_for_batch` or `seed_staged_store_metadata` directly. The runtime invokes seeding before any Zarr append/cursor logic runs, via `src/firecube/ingestor/runtime/engine.py`'s `_zarr_pre_batch_hook`. This ensures every ZarrIngestor template gets correct daily-append behavior in staged mode without per-template wiring.

## Control-Plane Model

The `.firecube/` directory is the authoritative product-local control-plane root. All state queries and mutations go through `ChunkManager`. Do not bypass this facade.

- `ChunkManager` is the public facade for Firecube chunk state.
- `.firecube/` is the authoritative product-local control-plane root.
- WAL events under `.firecube/runs/<run_id>/events-*.jsonl` are authoritative.
- Snapshots under `.firecube/snapshots/` are derived read models.
- Claims under `.firecube/claims/` are write-domain coordination state, not data records.
- Current records are **run/span** oriented (not per `/c/...` chunk entries).
- Engine/runtime code should write lifecycle state through `SpanRecorder` and `ChunkManager`, not by writing `.firecube/` files directly.
- Cleanup tools (`firecube chunks ...`, `firecube zarr scrub ...`) should use `ChunkManager` queries and deletion helpers; avoid ad-hoc S3 deletes.
- `._firecube_manifest.jsonl` is legacy-only; new code should not read or write it.
- `ChunkManager.list_chunks()` supports time-range filtering via `time_min_after`, `time_max_before`, and `time_overlaps` params; use these instead of post-filtering the full list.
- `ChunkManager.list_runs()` supports `status` and `non_terminal` filtering.
- `ChunkManager.time_coverage_summary()` returns per-group time bounds and span counts, useful for diagnostics and resume overlap detection.
- Resume authority is control-plane-primary: `ResumeGuard` decides whether a run may proceed based solely on WAL state. Non-terminal runs block resume and require explicit `chunks runs abandon`. The data store is not consulted unless `validate_zarr=true`.
- For products with mixed bounded/unbounded groups, the full resolved-index record is not persisted; per-group identity verification for bounded groups happens at ingest startup (see restored per-group verification path).

## Plugin Contract

Plugins interact with the engine through a narrow public surface. Reaching past it is a boundary violation.

- Every concrete `BaseIngestor` subclass must declare `PRODUCT_NAME: ClassVar[str]`, enforced at class-definition time via `__init_subclass__`. Abstract templates (e.g. `GenericZarrIngestor`) are exempt.
- `PipelineResult.metrics` is typed `ResultMetrics` (not a plain dict). `PipelineResult.outputs` is `OutputPaths` (not a plain dict). Both are importable from `firecube.ingestor.api`.
- Plugins must not construct `PipelineResult(output_path=...)`. Use `PipelineResult(outputs=OutputPaths(primary=...))`. Enforced at the constructor: the legacy `output_path=` kwarg was removed from `PipelineResult` and `IngestResult` and raises `TypeError`. The read-only `result.output_path` property remains as a compatibility view of `outputs.primary`. See DONE.md 2026-06-11.
- Parallel DirectZarr plugins declare `index_spec(ctx) -> IndexSpec | None`. `None` means serial mode. A non-`None` spec enables engine-owned slot-index resolution and parallel ingestion.
- Plugins that return an `IndexSpec` must also implement `inspect_item(item, ctx) -> ItemInfo | None`. It maps each source item to a coordinate the engine can place in the resolved index.
- `resolved_index(ctx)` is engine-owned. Plugins use it for read-only lookup, not for deriving their own slot model.
- The old `SUPPORTS_SLOT_RANGE_PARALLELISM` and `slot_index_model()` mixin-era surface is removed.
- Plugins depend on `firecube.ingestor.api` and `firecube.core.api` only. Deep imports into `runtime/` or `core/` internals are not part of the contract.

## Zarr Runtime Surfaces

Zarr is treated as a runtime subsystem with multiple write strategies, not a single helper function. The four-layer architecture lets simple plugins return `xr.Dataset` while advanced plugins declare explicit schema and write intents.

- **Template layer** (`firecube.ingestor.templates`): `GenericZarrIngestor` for xarray-append plugins, `DirectZarrIngestor` for region-write plugins.
- **Strategy layer** (`firecube.ingestor.runtime.zarr.strategies`): `AppendStrategy` wraps the xarray-append path; `IndexedRegionStrategy` executes `WriteIntent` lists via `RegionZarrWriter`. Two split Protocols at `runtime/zarr/contracts.py` are the stable seam: `AppendWriteStrategy` (matched by `AppendStrategy`) and `RegionWriteStrategy` (matched by `IndexedRegionStrategy`), both `@runtime_checkable`.
- **Orchestrator layer** (`runtime/zarr/append.py`): `append_time_groups` coordinates per-group/per-batch iteration.
- **Services layer** (`runtime/zarr/append_services.py`): `AppendResumeService`, `AppendTimestampState`, `AppendWriteExecutor`, `AppendCoverageBuilder`. Each service owns one concern.

Public contract types for direct-write plugins are re-exported from `firecube.ingestor.api`: `WriteIntent`, `ZarrArraySpec`, `ZarrGroupSpec`. Plugins should not import deeper than this surface.

See §4a in [DONE.md](DONE.md) for the rationale and migration notes. The post-§4a follow-ups and the Phase 3 planner are all DONE (see DONE.md): Protocol split (§21), facade thinning (§22), resize-on-write (§23 + §23-AUTO), schema-claim race (§24), ingest-time multires removal (§25), scaffolding fix (§26), per-slot claim granularity (§7), and the §7-sub planner (`firecube zarr slots` / `firecube zarr preallocate`, slot-range flags, K8s env discovery; shipped 2026-05-28).

## Observability Rules

- CLI initializes observability via `src/firecube/core/observability/`.
- Plugins should emit metrics/log-style events via `ctx.telemetry` (injected), not by importing Prometheus/OTel directly.
- This is a batch worker: prefer **Prometheus Pushgateway** style "end-of-run" metrics over a long-lived `/metrics` HTTP endpoint.
- Metric schema is owned by `src/firecube/core/observability/metrics.py`. Domain collectors (`core/filesystem/instrumentation.py`, `core/controlplane/metrics.py`) import key constants from there; never define string-literal summary keys inline.
- Tracing helpers (`span`, `set_current_span_attribute`, `capture_context`, `attach_context`, `detach_context`, `propagated_context`) are the only sanctioned way to use OpenTelemetry in firecube outside `core/observability/`. Direct `opentelemetry` imports are forbidden outside that package and enforced by `tests/unit/test_observability_boundaries.py`.

See the 2026-06-01 entry in [DONE.md](DONE.md) for the full rationale and reviewer findings (F1–F6).

## Decided Questions

Brief answers to recurring design questions. Full history with dates is in [DONE.md](DONE.md).

- **Write mode: staged vs direct?** Explicit `--write-mode [staged|direct]` flag required; no inference from URI scheme or storage type. See §4a in DONE.md.
- **Storage driver selection?** Explicit `--storage-driver [fsspec|obstore]` flag required. One driver per run, no mixing. See §4b in DONE.md.
- **Storage type: local vs S3?** Explicit `--storage-type [local|s3]` flag required; not inferred from `s3://` vs `file://` URI scheme. See §12.1 in DONE.md.
- **Product name source?** Precedence: CLI `--product-name` > config `default_product_name` > plugin `PRODUCT_NAME` > hard fail. `output_name` inference from URI basename was removed.
- **Resume authority?** Control-plane-primary via `ResumeGuard`. WAL state alone decides; the data store is not consulted unless `validate_zarr=true`. See §1 in DONE.md.
- **Concurrency model?** One container, one run. Parallel throughput comes from the orchestrator scheduling disjoint time slices, not from shared writers inside a single run. The §7-sub planner (chunk-aligned slot ranges via `firecube zarr slots` / `firecube zarr preallocate`) shipped 2026-05-28; see DONE.md §7-sub.
- **Plugin metrics?** Plugins use `ctx.telemetry`; they do not import Prometheus or OTel directly. Engine owns `metrics["storage"]` injection. See §16 in DONE.md.
- **Snapshot reads?** Parallel WAL segment reads via `ThreadPoolExecutor(max_workers=8)` in `_snapshot.py`; sequential `apply_events` preserves upsert ordering. See §17 in DONE.md.
- **Zarr write strategy?** Two split Protocols: `AppendWriteStrategy` (matched by `AppendStrategy`; used by `GenericZarrIngestor`) and `RegionWriteStrategy` (matched by `IndexedRegionStrategy`; used by `DirectZarrIngestor`). Both are `@runtime_checkable` and live at `runtime/zarr/contracts.py`. See §4a and §21 in DONE.md.
- **Time-window maintenance?** `firecube_timestamp_state` array tracks per-timestamp state (`unknown`, `present`, `deleted_by_firecube`, `failed_batch`). Scrub uses `ChunkManager.create_deletion_plan()` + `execute_deletion()`. See §8 in DONE.md.
- **Time dimension naming?** Plugin-declared via `time_dim_name: ClassVar[str]` on the `BaseIngestor` subclass (default `"timestamp"`); mirrors `PRODUCT_NAME` pattern. **Not** a `ZarrTemplateConfig` / `PluginConfig` field — config-tier fields are enumerated by `TypedOptionsParam` and would become `--option`-overridable, but the dim name is a property of the plugin/product, not a per-run knob. Plugin declaration is authoritative; existing-cube mismatch fails loudly with migration guidance pointing at the deferred migration framework. Internal `firecube_timestamp_state` array name remains stable. See DONE.md 2026-06-10.
- **DirectZarr schema parity: how do plugins declare per-array CF attrs, sharding, dim names, static arrays?** Via four new optional `ZarrArraySpec` fields: `shards`, `attrs`, `dimension_names`, `time_indexed`. Cold-migration only; see DONE.md entry dated 2026-06-18.
- **Time-coverage decoding on the read side?** Self-describing via `(dtype, attrs)` through `firecube.core.api.decode_time_array`. xarray's CF decoder is a private impl detail; firecube does NOT expose CF as a domain concept.

## Risks To Avoid

These are the anti-patterns most likely to cause silent data corruption, resume failures, or hard-to-debug production incidents.

- **Mixing storage drivers**: calling `fsspec.filesystem(...)` directly in write-domain code bypasses the driver abstraction and breaks the one-driver invariant. Always use `create_filesystem(config)`.
- **Plugins writing manifests**: any plugin that writes `.firecube/` files directly creates a split-brain between the WAL and the data store. Lifecycle state belongs in `SpanRecorder` and `ChunkManager`.
- **Inferring config from context**: the old heuristics (storage type from URI scheme, write mode from locality, product name from basename) caused silent misconfiguration. All flags are now explicit; resist the urge to add inference back.
- **Ad-hoc S3 deletes in cleanup tools**: deleting Zarr chunks without going through `ChunkManager.create_deletion_plan()` leaves the control plane out of sync. Use the deletion helpers.
- **Long-lived metrics endpoints**: this is a batch worker, not a server. A `/metrics` HTTP endpoint that outlives the run is the wrong model; push end-of-run metrics to Pushgateway instead.
- **Deep plugin imports**: plugins that import from `firecube.ingestor.runtime.*` or `firecube.core.*` internals bypass the public contract and break on internal refactors. Use `firecube.ingestor.api` and `firecube.core.api` only.
- **Hardcoded time-dim string lookups in control-plane**: The `deletion.py` silent-fallback pattern (`dim_names.index("timestamp") if "timestamp" in d else 0`) was a latent data-loss hazard — if the cube was written with a different time dim name, deletion would silently target the wrong axis. Always use the typed `time_dim_name` parameter threaded through from the ingestor and raise loudly when absent. Never fall back to index 0. Span records now carry `time_dim_name` at write time; maintenance tooling (`chunks delete-span`) resolves it per span as recorded value > 1-D `firecube_timestamp_state` `dimension_names` discovery > explicit `--time-dim` > default `"timestamp"`, refusing (pre-flight, zero deletion) when an explicit name contradicts a recorded or discovered one. See DONE.md 2026-06-11.
- **Bare `except Exception: pass` in coverage builders** — silent except-swallowing hid the 1970-epoch telemetry bug for an extended period. The bare-except was removed from `AppendCoverageBuilder.record_batch` in 2026-06-18.
- **Iterator-lazy `build_write_intents`**: Do not change the hook to return an iterator or generator as a memory fix. DirectZarr requires the full intent metadata set before write for slot-range validation, `expected_time_count` auto-sizing, `allow_grow`, group presence validation, per-slot claim grouping, and `len(intents)` metrics. Buffering an iterator saves list-container references, not GiB-scale payload bytes. The correct direction for large per-slot payload is metadata-eager / payload-lazy at the intent level (extending the deferred lazy-payload direction to time-indexed intents), not a lazy iterator.
- **Sub-batching a single slot across multiple `PipelineBatch`es**: Do not emit multiple batches carrying WriteIntents with a shared `ts_index`. The per-slot claim (`WriteDomain(category="zarr_region", name=f"{group}:slot={ts_index}")`) is an atomic filesystem file — the second concurrent acquirer raises `ClaimConflictError` immediately, with no retry loop at the dispatch site. Additionally, `CoverageTracker` records writes at `(group, ts_index)` granularity only, so a crash after some but not all sub-batches for a slot succeeded produces a Zarr slot with fill-value regions indistinguishable from a legitimate write — a data-integrity hazard, not just a batch-failure one. Internal-iteration variants that reshape emission order inside a single `build_write_intents` call are safe but do not reduce peak retention: the returned intent list still holds every payload's ndarray until `write_groups()` completes (empirically confirmed 2026-07-12 for the MTG FCI L1C plugin — three sub-batching configurations plateau within noise at the baseline retention level). Per-slot payload retention is instead the target of the deferred lazy-payload direction.

## Accepted Deviations

These entries record audit findings that were reviewed and accepted rather than fixed. Each entry explains why the deviation is intentional and what constraint makes the alternative impractical.

### C9 — Click option-group monkey-patch (2026-06-11)

`install_option_groups_patch()` in `src/firecube/cli/_formatter.py:177-180` globally replaces `click.Command.format_options` with a patched version and sets `click.Context.formatter_class = FirecubeFormatter`. The patch is applied at module import time in `src/firecube/cli/main.py:49`, before any command groups are registered.

**Why the global patch is necessary:** Click has no native option-grouping hook. The only alternative would be a custom `Command` subclass that overrides `format_options`. That approach cannot reach plugin-registered CLI groups: plugins declare their commands under `firecube.plugin_cli` entry points as plain `click.Group` instances, and there is no mechanism to force those groups to use a custom base class. The global patch is the only way to apply consistent help formatting across both core commands and plugin-contributed commands. Revisit if Click gains a native option-group API.

### C2 — ChunkManager facade width (2026-06-11)

`ChunkManager` in `src/firecube/core/controlplane/manager.py` exposes 31 public methods, of which 16 are pass-throughs to the underlying `ManifestRepository` and `DeletionEngine`. This width is intentional.

**Why the width is not bloat:** The DESIGN.md invariant "all control-plane ops via the facade" (see Control-Plane Model above) requires that every caller, including engine code, maintenance tooling, and tests, goes through `ChunkManager` rather than reaching into `repo.py` or `deletion.py` directly. The method count tracks the breadth of the control-plane capability, not a design smell. After the post-C1 repo/deletion split, the facade fronts those split units unchanged; the split reduced internal coupling without shrinking the public surface, which is correct. Thinning the facade would require callers to import from internal modules, violating the boundary.

## Time-coordinate arrays: single-writer invariant

Time-coordinate arrays are engine-owned single-writer surfaces. Ingest pods never write them.

The engine materializes coordinate values during `firecube zarr preallocate`. Pods that run later verify the stored value against the incoming coordinate and raise `SchemaDriftError` on mismatch. They never overwrite. This eliminates the read-modify-write race that the legacy per-slot chunk model exposed under parallel ingestion.

### Axis regime table

Each time-axis declaration falls into one of three regimes. The regime determines what values are stored, which component writes them, and which marker is stamped.

| Regime | Stored values | Single writer | Marker stamped |
|---|---|---|---|
| Regular exact (`mode="exact"`) | Nominal grid: `epoch + slot * cadence` | `preallocate` | `firecube_preallocated` |
| Regular floor (`mode="floor"`) | Per-item observed timestamps from `inspect_item(item).coordinate` | `preallocate` (per window, via `--input-data` discovery) | `firecube_coord_managed` |
| Irregular explicit or discovered (`IrregularTimeAxis`, `values=AUTO`) | Declared or discovered coordinates | `preallocate` | `firecube_preallocated` |

The `floor` regime is an unsealed managed surface: preallocate fills only the slots it discovers in the current window and stamps `firecube_coord_managed`. NaT holes remain for windows not yet processed. Pods verify-or-error; they never fill holes.

### Marker lifecycle

Each coordinate array carries at most one of two mutually exclusive markers: `firecube_preallocated` (grid or irregular values written; pods verify-only) and `firecube_coord_managed` (engine-managed observed surface; pods verify-only). Both markers present is a terminal error: detection raises `SchemaDriftError` before any slot read or write, and the store requires manual inspection. An array with neither marker is a pre-marker legacy cube; current `preallocate` never creates one, because it stamps the marker at array creation, before any value is written.

`firecube_consolidated_at` is a separate timestamp attr stamped by `firecube zarr consolidate-time-coord`. Its presence alongside `firecube_preallocated` means the array was retroactively densified from a legacy per-slot layout; the accompanying `ConsolidatedTimeCoord` WAL event is what `ResumeGuard` reads to block further ingest on the sealed cube.

Materialization is idempotent and window-extensible: a re-run reconciles per slot under the held global materialization claim and refuses on divergent values, for both regimes (grid values are deterministic, so the exact regime reconciles identically). Partial NaT under a marker is a reconcilable state, not corruption. See the DONE.md 2026-09-01 amendment for the per-slot rules.

## Related Files

- [DONE.md](DONE.md) — append-only decision log; read this when you need the rationale behind an existing rule
- [TODO.md](TODO.md) — open work items, including tensogram ingest routing (§8) and plugin context boundary hardening (§13)
- [IDEAS.md](IDEAS.md) — speculative work not yet committed (signal handling, pod exit verification, TPE lifecycle audit)
- [TEST.md](TEST.md) — test discipline, skip policy, CI invocation
- [TESTING_STANDARDS.md](TESTING_STANDARDS.md) — behavior-first testing standards and suite overhaul plan
- [STYLE.md](STYLE.md) — Python style guide and GoF pattern guidance
- [RELEASE.md](RELEASE.md) — package versioning and release rules
- `AGENTS.md` — quick-start commands, CLI flag reference, and "Where things live" module map
