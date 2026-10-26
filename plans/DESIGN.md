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
- **GenericParquet target ownership**: until stable logical-slice replay is supported, the template requires a fresh target with no prior runs or payload. A product claim lasts through promotion and terminal recording; batches retain per-path claims and refuse existing part paths. Resume/force do not bypass this guard.
- **Refusal text carries facts, the CLI renders commands**: a structured refusal's text is a `Message` of typed parts (`src/firecube/core/errors.py`), and a refusal built as plain text has `message` `None` and an empty `context`; a new refusal that names a command or flag must be a `Message`, with the `PERMANENT` rows of the CLI boundary gate and the generated scaffolding as the accepted exceptions. `src/firecube/cli/_recovery.py` renders the flags and commands from the Click definitions for the CLI sites that print a structured library refusal (an exception, warning or result line whose text is a `Message`); `render` leaves a plain string unchanged. A `Message` is joined only with `Message.join`.
- **One CLI URI rule**: one function, `parse_and_validate_uri` in `src/firecube/cli/_uri_policy.py`, admits every storage URI option and argument of the CLI and returns a `StorageUri`; it refuses `stac://`. Source options (the `--input-data` of `ingest`, `zarr preallocate` and `zarr slots`) admit `stac://` through its sibling `parse_and_validate_source_uri` in the same module, which returns a `StacCollectionUri` for `stac://` and otherwise exactly what `parse_and_validate_uri` returns or refuses. `stac://` is a source scheme, never a storage location, so `StorageUri` never holds it; a command without a STAC branch refuses a `StacCollectionUri` in its body with a `click.UsageError` before any I/O. Command code reads the scheme and the path only from that value. `test_cli_parses_uris_only_in_uri_policy` in `tests/architecture/test_cli_boundary.py` fails when another `cli/` module parses a URI. It has three `PERMANENT` rows, each scoped to one function. Two rows cover the `--input-data` handling in the `ingest` function of `cli/main.py`, one call each. The third covers `_storage_uri_from_base` in `cli/chunks/_manager.py`, which parses the base URI of the `[storage]` config.
- **Source-side fsspec is allowlisted, not a violation**: Input discovery, workspace materialization, and validation read paths intentionally use `_open_fsspec_url()` for arbitrary external inputs. Each call site is listed in `tests/unit/test_no_raw_fsspec_usage.py:_FSSPEC_PERMANENT_ALLOWLIST` with a `PERMANENT:` rationale. Write-domain enforcement is tested in `tests/integration/test_one_driver_invariant.py`.
- **Zarr metadata is read live**: every `zarr.open_group` and `zarr.open` in `src/firecube` passes `use_consolidated=False`, and every xarray Zarr open passes `consolidated=False`, at the call or through `ZarrIO.open_group` and `ZarrIO.open_dataset`. A group reached through another group keeps its own snapshot, so code reading below the root reads arrays by full path from a live group, opens a group directly with `use_consolidated=False` before listing its children, and walks by reopening every group that way; zarr 3.2.1's `Group.members` accepts `use_consolidated_for_children` and ignores it. `open_array` is exempt. The two archive validators and the snapshot refresh keep a default open. A write through `ZarrIO.write_dataset` never creates a consolidated snapshot, and `ZarrIO.refresh_consolidated_metadata`, called after `zarr multires` and `.tgm` restore, rewrites the snapshot of each group that already has one and creates none. `test_zarr_metadata_opens_read_live_metadata` in `tests/unit/test_no_raw_fsspec_usage.py` pins opens; its rows are `PERMANENT` and a new open is not given a row.
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
- Resume authority is control-plane-primary: `ResumeGuard` decides whether a run may proceed based solely on WAL state. Non-terminal runs block resume and require explicit `chunks runs abandon`, which also releases the write claims whose owner resolves to that run alone. The data store is not consulted unless `validate_zarr=true`.
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
- Plugins use `firecube.ingestor.api`, `firecube.core.api`, and the optional `firecube.ingestor.extensions` surface. Extensions provide domain-agnostic processing utilities and explicitly supported lifecycle mixins. Plugins own product interpretation, filename conventions, grouping, coordinate meaning, quality rules, and published metadata. Pattern parsing requires a caller-supplied format and a complete match; it does not infer horizons, discover groups, select sources, or assign meaning to time fields. The Regex Guessing prohibition in STYLE.md still applies. Deep imports into `runtime/` or `core/` internals are not part of the contract.

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
- **Storage type: local vs S3?** Smart default on every command that takes a product URI, `ingest` included: `apply_smart_default` (`src/firecube/cli/_uri_policy.py`) fills `--storage-type` from the URI scheme (`file://`→`local`, `s3://`→`s3`) when omitted and rejects an explicit value that contradicts the scheme (`UsageError`). The scheme map is the only source; nothing is sniffed from paths or the environment. `--write-mode` and `--product-name` are never inferred. See §12.1 in DONE.md.
- **Product name source?** Precedence: CLI `--product-name` > config `default_product_name` > plugin `PRODUCT_NAME` > hard fail. `output_name` inference from URI basename was removed.
- **Resume authority?** Control-plane-primary via `ResumeGuard`. WAL state alone decides; the data store is not consulted unless `validate_zarr=true`. See §1 in DONE.md.
- **Concurrency model?** One container, one run. Parallel throughput comes from the orchestrator scheduling disjoint time slices, not from shared writers inside a single run. The §7-sub planner (chunk-aligned slot ranges via `firecube zarr slots` / `firecube zarr preallocate`) shipped 2026-05-28; see DONE.md §7-sub.
- **Append failure and ordering?** Append hosts stop at the first failed batch (`stop_on_batch_failure`, a template capability: `True` on `GenericZarrIngestor`, `False` on `BaseIngestor`, Parquet and DirectZarr; no engine option) so no batch appends past a gap; later batches are reported as not attempted. Appends commit in planner batch order through the ordered write gate, which replaced the append lock: `pipeline_workers>1` parallelises `prepare_batch_data` only. A failed batch's region slots are marked `failed_batch` (state 3), its tail truncated, and one span record per `(run, batch, group)` carries the ranges. See the 2026-09-12 entry in DONE.md.
- **Plugin metrics?** Plugins use `ctx.telemetry`; they do not import Prometheus or OTel directly. Engine owns `metrics["storage"]` injection. See §16 in DONE.md.
- **Snapshot reads?** Parallel WAL segment reads via `ThreadPoolExecutor(max_workers=8)` in `_snapshot.py`; sequential `apply_events` preserves upsert ordering. See §17 in DONE.md.
- **Zarr write strategy?** Two split Protocols: `AppendWriteStrategy` (matched by `AppendStrategy`; used by `GenericZarrIngestor`) and `RegionWriteStrategy` (matched by `IndexedRegionStrategy`; used by `DirectZarrIngestor`). Both are `@runtime_checkable` and live at `runtime/zarr/contracts.py`. See §4a and §21 in DONE.md.
- **Time-window maintenance?** `firecube_timestamp_state` array tracks per-timestamp state (`unknown`, `present`, `deleted_by_firecube`, `failed_batch`). Scrub uses `ChunkManager.create_deletion_plan()` + `execute_deletion()`. See §8 in DONE.md.
- **Time dimension naming?** Plugin-declared via `time_dim_name: ClassVar[str]` on the `BaseIngestor` subclass (default `"timestamp"`); mirrors `PRODUCT_NAME` pattern. **Not** a `ZarrTemplateConfig` / `PluginConfig` field — config-tier fields are enumerated by `TypedOptionsParam` and would become `--option`-overridable, but the dim name is a property of the plugin/product, not a per-run knob. Plugin declaration is authoritative; existing-cube mismatch fails loudly with migration guidance pointing at the deferred migration framework. Internal `firecube_timestamp_state` array name remains stable. See DONE.md 2026-06-10.
- **DirectZarr schema parity: how do plugins declare per-array CF attrs, sharding, dim names, static arrays?** Via four new optional `ZarrArraySpec` fields: `shards`, `attrs`, `dimension_names`, `time_indexed`. Cold-migration only; see DONE.md entry dated 2026-06-18.
- **Time-coverage decoding on the read side?** Self-describing via `(dtype, attrs)` through `firecube.core.api.decode_time_array`. xarray's CF decoder is a private impl detail; firecube does NOT expose CF as a domain concept.
- **Calendar model?** The calendar is a property of the time axis. Gregorian is its default value. `proleptic_gregorian` is the canonical name. `standard` and `gregorian` are aliases. One module selects the storage encoding from the declared calendar: `firecube/core/time/encoding.py`. A Gregorian axis stores `datetime64`. Any other calendar stores a CF-encoded number with `units` and `calendar` attrs. Encoding and decoding use the existing cftime-based time coders. The engine has no calendar arithmetic of its own. See DONE.md 2026-09-23 ("Calendar as a property of every time axis").

## Risks To Avoid

These are the anti-patterns most likely to cause silent data corruption, resume failures, or hard-to-debug production incidents.

- **Mixing storage drivers**: calling `fsspec.filesystem(...)` directly in write-domain code bypasses the driver abstraction and breaks the one-driver invariant. Always use `create_filesystem(config)`.
- **Plugins writing manifests**: any plugin that writes `.firecube/` files directly creates a split-brain between the WAL and the data store. Lifecycle state belongs in `SpanRecorder` and `ChunkManager`.
- **Inferring config from context**: the old ingest heuristics (write mode from target locality, product name from URI basename) caused silent misconfiguration. Keep `--write-mode` and `--product-name` explicit everywhere (`tests/architecture/test_no_cli_inference.py`). The one sanctioned default is `--storage-type` from the product URI scheme via `apply_smart_default`, which is a fixed scheme map rather than a heuristic and still validates an explicit value against the scheme; do not add a second inference path beside it.
- **Ad-hoc S3 deletes in cleanup tools**: deleting Zarr chunks without going through `ChunkManager.create_deletion_plan()` leaves the control plane out of sync. Use the deletion helpers.
- **Long-lived metrics endpoints**: this is a batch worker, not a server. A `/metrics` HTTP endpoint that outlives the run is the wrong model; push end-of-run metrics to Pushgateway instead.
- **Deep plugin imports**: plugins that import from `firecube.ingestor.runtime.*` or `firecube.core.*` internals bypass the public contract and break on internal refactors. Use `firecube.ingestor.api`, `firecube.core.api`, and supported optional `firecube.ingestor.extensions` capabilities.
- **Hardcoded time-dim string lookups in control-plane**: The `deletion.py` silent-fallback pattern (`dim_names.index("timestamp") if "timestamp" in d else 0`) was a latent data-loss hazard — if the cube was written with a different time dim name, deletion would silently target the wrong axis. Always use the typed `time_dim_name` parameter threaded through from the ingestor and raise loudly when absent. Never fall back to index 0. Span records now carry `time_dim_name` at write time; maintenance tooling (`chunks delete-span`) resolves it per span as recorded value > 1-D `firecube_timestamp_state` `dimension_names` discovery > explicit `--time-dim` > default `"timestamp"`, refusing (pre-flight, zero deletion) when an explicit name contradicts a recorded or discovered one. See DONE.md 2026-06-11.
- **Bare `except Exception: pass` in coverage builders** — silent except-swallowing hid the 1970-epoch telemetry bug for an extended period. The bare-except was removed from `AppendCoverageBuilder.record_batch` in 2026-06-18.
- **Iterator-lazy `build_write_intents`**: Do not change the hook to return an iterator or generator as a memory fix. DirectZarr requires the full intent metadata set before write for slot-range validation, `expected_time_count` auto-sizing, `allow_grow`, group presence validation, per-slot claim grouping, and `len(intents)` metrics. Buffering an iterator saves list-container references, not GiB-scale payload bytes. The correct direction for large per-slot payload is metadata-eager / payload-lazy at the intent level (extending the deferred lazy-payload direction to time-indexed intents), not a lazy iterator.
- **Sub-batching a single slot across multiple `PipelineBatch`es**: Do not emit multiple batches carrying WriteIntents with a shared `ts_index`. The per-slot claim (`WriteDomain(category="zarr_region", name=f"{group}:slot={ts_index}")`) is an atomic filesystem file — the second concurrent acquirer raises `ClaimConflictError` immediately, with no retry loop at the dispatch site. Additionally, `CoverageTracker` records writes at `(group, ts_index)` granularity only, so a crash after some but not all sub-batches for a slot succeeded produces a Zarr slot with fill-value regions indistinguishable from a legitimate write — a data-integrity hazard, not just a batch-failure one. Internal-iteration variants that reshape emission order inside a single `build_write_intents` call are safe but do not reduce peak retention: the returned intent list still holds every payload's ndarray until `write_groups()` completes (empirically confirmed 2026-07-12 for a plugin with large per-slot payloads — three sub-batching configurations plateau within noise at the baseline retention level). Per-slot payload retention is instead the target of the deferred lazy-payload direction.

## Accepted Deviations

These entries record audit findings that were reviewed and accepted rather than fixed. Each entry explains why the deviation is intentional and what constraint makes the alternative impractical.

### C9 — Click option-group monkey-patch (2026-06-11)

`install_option_groups_patch()` in `src/firecube/cli/_formatter.py` globally replaces `click.Command.format_options` with a patched version and sets `click.Context.formatter_class = FirecubeFormatter`. The patch is applied at module import time in `src/firecube/cli/main.py`, before any command groups are registered.

**Why the global patch is necessary:** Click has no native option-grouping hook. The only alternative would be a custom `Command` subclass that overrides `format_options`. That approach cannot reach plugin-registered CLI groups: plugins declare their commands under `firecube.plugin_cli` entry points as plain `click.Group` instances, and there is no mechanism to force those groups to use a custom base class. The global patch is the only way to apply consistent help formatting across both core commands and plugin-contributed commands. Revisit if Click gains a native option-group API.

### C2 — ChunkManager facade width (2026-06-11)

`ChunkManager` in `src/firecube/core/controlplane/manager.py` exposes 31 public methods, of which 16 are pass-throughs to the underlying `ManifestRepository` and `DeletionEngine`. This width is intentional.

**Why the width is not bloat:** The DESIGN.md invariant "all control-plane ops via the facade" (see Control-Plane Model above) requires that every caller, including engine code, maintenance tooling, and tests, goes through `ChunkManager` rather than reaching into `repo.py` or `deletion.py` directly. The method count tracks the breadth of the control-plane capability, not a design smell. After the post-C1 repo/deletion split, the facade fronts those split units unchanged; the split reduced internal coupling without shrinking the public surface, which is correct. Thinning the facade would require callers to import from internal modules, violating the boundary.

## Time-coordinate arrays: single-writer invariant

Arrays declared `engine_owned=True` are single-writer surfaces: ingest pods never write them. A time-coordinate array is one once `firecube zarr preallocate` has marked it (`firecube_preallocated` or `firecube_coord_managed`); pods then only verify it. A coordinate that carries neither marker (no preallocate ran, or a store from before the markers) is written by pods per slot, and two slot-disjoint processes sharing one of its chunks can lose a timestamp (see plans/TODO.md).

The engine materializes coordinate values during `firecube zarr preallocate`. Pods that run later verify the stored value against the incoming coordinate and raise `SchemaDriftError` on mismatch. They never overwrite. For a marked coordinate this removes the race of the unmarked case above: there, each pod writes its own slot of the coordinate, a slot write reads, modifies and writes back the whole chunk, and two pods whose slots share a chunk overwrite each other.

The same invariant covers any rank-1 time-indexed plugin array declared `ZarrArraySpec(engine_owned=True)`. Preallocate inspects every source item (any axis kind, `--input-data` required), collects `ItemInfo.values[name]`, refuses missing, unknown and conflicting values, writes the array whole under a whole-extent materialization claim, and stamps `firecube_engine_owned`. Pods drop intents aimed at it and verify the stored value against the final target (also in staged mode, so promotion has nothing of it to write back). Ingest into a target where it is not materialized is refused, and a pod-written array later declared engine-owned is refused.

### Chunk-ownership rule for pod-written arrays

No two concurrent writers may read-modify-write one stored unit. A stored unit is the shard if the array is sharded, else the chunk. A chunk is a read-modify-write unit, so a shared unit silently loses the slot of one writer. A slot window is the range of time slots that one pod writes.

The rule is enforced on the stored layout. It is never inferred from declarations. Four components uphold it.

- Layout at creation (`core/zarr/guard/_layout.py`): it decides the layout of every array that the spec loop, pod startup and the serial path create. It is not the only creator of a coordinate. It never applies a default to an existing array.
  - An undeclared pod-written time-indexed array (no chunks, no shards) keeps the whole-array chunks of zarr if their time chunk is already 1. No warning is logged.
  - If the time chunk is not 1, the array gets time chunk 1, the heuristic of zarr for the per-slot shape, and no shards. One warning is logged.
  - A shards-only declaration keeps the declared shards.
  - A rank-1 time-indexed array (pod-written, engine-owned or the time coordinate) that declares shards and no chunks gets as its chunk the largest divisor of its shard that is at most `min(256, slots)`. A declared `chunks=` is kept as declared.
  - Other declared layouts are used as declared. The time coordinate and engine-owned arrays without shards stay dense.
- Coordinate materialization: it creates its own coordinates with `resolve_coord_chunks`, unsharded, and does not use declared shards. These coordinates are an observed regular coordinate in preallocate, or a coordinate without a spec.
- Slot-window check: a window that covers a stored unit only partly is refused (`ConfigurationError`). A unit wholly inside the window is legal at any size. The check covers the arrays that pods write; the time coordinate, engine-owned arrays and static arrays are exempt from it.
  - At pod startup, the check refuses before any write outside `.firecube/`.
  - Per batch, the check refuses before the writes of that batch. Earlier batches of the run may already have written.
  - The check also refuses an existing store with an unsafe layout, for example an undeclared array that was preallocated dense. The check does not change the store.
- Staged promotion check: promotion replaces whole objects, so every promoted unit must be written completely in all dimensions. Sparse writes into a promoted unit are not supported; they would need seeding.
  - A window run is refused before its batches.
  - A serial run, or a run with partially written rows or channels, is refused just before promotion, after the workspace writes, with the target untouched.

Threads inside one process (`pipeline_workers`) are serialised by a lock table. The table is per process. It covers the read-modify-write of one (store, array, unit). The batches of a run share its schema claim. A static array runs its marker check, write and stamp under one whole-array lock. A concurrent batch with a different payload therefore raises `SchemaDriftError`. Safety across processes comes from the ownership rule, not from locks.

### Guarded writes in DirectZarr flows

- Payload writes: every DirectZarr payload write passes one chokepoint, `guarded_write` (`core/zarr/guard/guarded_write.py`).
  - It takes the unit locks. If this thread already holds a whole-array lock, it reuses that hold and takes no unit lock.
  - It applies `write_empty_chunks` to the array it writes. It never changes `zarr.config`.
  - It writes, then reports the result to the write session of the run before it releases the locks.
  - Staged runs also record which units the write left without an object.
- Metadata: in DirectZarr flows, `zarr.json` changes only under a metadata authority (`core/zarr/guard/metadata_authority.py`).
  - An authority is either a held claim (`CLAIM`, cross-process exclusion) or `PROCESS_LOCAL`.
  - `PROCESS_LOCAL` excludes the threads of this process only. No claim stands behind it.
  - The operations create, grow (never smaller) and add or replace attributes. Each operation checks the authority before it touches the store.
  - The editing operations re-read the node and write under one per-node lock.
  - `publish_document` replaces the whole document without re-reading it, so it can erase a marker that the payload lacks.
  - `publish_document_if_absent` writes only where no document exists.
- Run write session: every DirectZarr run kind (serial, window, staged) builds one write session at pod startup.
  - The session is required and fails closed. A batch or promotion step without a session raises `ConfigurationError`.
  - `close` stops new admissions and drains in-flight writes within `drain_timeout`. Then it ends the session.
  - If the drain times out waiting for in-flight batches or session operations, the first closer raises `WriteSessionStateError` naming the stuck count.
  - The failure is remembered. Every later or concurrent `close` raises the same error while work is in flight, and returns normally once nothing is.
  - The first `close` revokes both authorities (seeding, publishing), also after a timeout.
  - After a failed close, the host keeps its session reference. If workspace cleanup is enabled, the cleanup of the run skips deleting the staged workspace and logs one ERROR naming its path. The operator removes the workspace after the writes stop.
  - If workspace cleanup is disabled (the default), the workspace is never deleted and the failed close is logged as a warning only.
  - The next run on the same host closes the previous session first. It is refused while that session is stuck.
  - The standard engine joins its workers before it closes.
- `PROCESS_LOCAL` authorities cover these cases. They exclude threads of one process and nothing else.
  - An unmarked time coordinate (written per slot by pods under the whole-array lock).
  - The static marker stamp when no claim is held: a window run, setup without a control plane, library use. A run that holds the schema claim of the group stamps under that claim.
  - Group schema creation, attribute changes and the static stamp whenever no claim is held: setup without a control plane (`chunk_manager is None`), and the schema step of the strategy when its claim callback is absent or yields no held claim.
  - The loser recovery mirrors of the control plane (the root attributes that a loser re-mirrors outside the claim of the winner).
  - Staged workspace seeding (the workspace is private to the process of the run).
  - The metadata publication of promotion (a staged run holds no cross-process claim at promotion).
  - Root and ancestor group creation in ingest (`ensure_group_ancestors`), whatever claim the group holds. `firecube zarr preallocate` adopts the materialization claim for its root and ancestor creation when it holds one (`CLAIM`). Without that claim it is `PROCESS_LOCAL`.
- Staged promotion of a DirectZarr run publishes a delta, not the workspace tree.
  - It PUTs every changed object.
  - It DELETEs the target object of a unit that the run wrote entirely as fill, so the old target values do not survive. It deletes only when the array handle does not keep empty chunks and the ledger proves the unit written. It never deletes a time-coordinate, static or engine-owned unit.
  - It publishes the `zarr.json` documents last, and only when every data operation succeeded.
  - It publishes no data object of a managed time coordinate and compares none. Seeding copies the coordinate's chunks into the workspace as a snapshot. `zarr preallocate` may have filled the target since. A PUT of the copy would revert that fill.
  - The plan lists each such object UNCHANGED with reason `engine-managed`. A written unit under a managed coordinate is a planner error. The coordinate's own `zarr.json` still goes through the metadata path.
  - The rule covers only the configured time coordinate (`time_dim_name`), and only when the lifecycle marker in its final-target `zarr.json` says preallocated or coordinate-managed. No other array is classified, marked or not. An unmarked (legacy) time coordinate is written per slot in the workspace, and its staged PUT is how the run's timestamps reach the target.
  - An invalid marker state refuses the promotion with the target untouched.
  - The write session classifies the coordinates that seeding reported and the coordinate of each group the run wrote. It reads the target `zarr.json` once per candidate and lists nothing. Seeding's report covers a group the run never wrote.
  - A target array never shrinks. When the target is longer on axis 0, the larger extent is kept, and a structural difference then refuses the promotion. When the target is equal or shorter, the workspace metadata is published and no structural comparison runs.
  - A unit that the workspace array ends inside (its stored chunk or shard extends past the workspace extent) is proved written only up to that extent. When the promotion units of the run are built, the shape of the target array is read from its `zarr.json`. If the target is longer on an axis where that unit is clipped, the promotion is refused with the target untouched. The target can have grown into the unit after the workspace was seeded, for example under a concurrent force-reingest. The put or delete replaces the whole object and would erase target cells that the run never wrote.
  - A unit wholly inside the workspace extent is never checked. A missing, equal, shorter or different-rank target promotes.
  - Attributes that the target array already has win over the workspace copy.
  - The final merge of each promoted document is atomic under the node lock for the threads of this process only. No other process is excluded.
- GenericZarr is outside the guarded write layer.
  - Its payload writes go through the `to_zarr` of xarray, not through `guarded_write`.
  - Its `zarr.json` writes are outside the metadata authority.
  - It seeds staged metadata itself.
  - It promotes a staged run with `upload_tree`, which uploads the whole workspace tree.
  - It keeps its ordered write gate.

Not guaranteed:

- Write-once of a static array across processes.
- A payload committed by a batch that outlives a timed-out `close` fails at recording (the session is terminal), and promotion is refused. A workspace retained after such a close needs operator cleanup.
- A target that grows into a clipped unit after the promotion check and before the put is not detected. Closing that window needs a cross-process claim.
- Group `zarr.json` documents are published from the workspace as they are. A root attribute written to the final target after the last seed is overwritten by promotion.
- Coordinate-chunk seeding checks whether the workspace already holds a chunk, then copies it from the target. The check and the copy are not atomic, and the chunk of the target can change between them.
- The whole-array lock and the unit locks do not exclude each other. An array is written under unit locks or under a whole-array hold, never both. Static arrays and an unmarked time coordinate use the whole-array hold, and a write inside it takes no unit lock. Pod-written time-indexed arrays use unit locks.
- Repeated staged root seeding replaces the workspace root document, so a workspace-root attribute stamped by a plugin group spec with `group=''` is lost on the next seed.
- Staged metadata seeding writes the workspace through a local path. A non-local workspace is unsupported.
- A plugin that sends both static and region intents to one array breaks the plugin contract. Nothing enforces it.
- Root creation on the `PROCESS_LOCAL` creation paths: on backends whose conditional create is exists-then-put (fsspec), two processes that create the root at once can lose attributes that the peer just published. Root creation is safe only when one process initialises the target. No claim is taken for it.

### Axis regime table

Each time-axis declaration falls into one of three regimes. The regime determines what values are stored, which component writes them, and which marker is stamped.

| Regime | Stored values | Single writer | Marker stamped |
|---|---|---|---|
| Regular exact (`mode="exact"`) | Nominal grid: `epoch + slot * cadence` | `preallocate` | `firecube_preallocated` |
| Regular floor (`mode="floor"`) | Per-item observed timestamps from `inspect_item(item).coordinate` | `preallocate` (per window, via `--input-data` discovery) | `firecube_coord_managed` |
| Irregular explicit or discovered (`IrregularTimeAxis`, `values=AUTO`) | Declared or discovered coordinates | `preallocate` | `firecube_preallocated` |

The `floor` regime is an unsealed managed surface: preallocate fills only the slots it discovers in the current window and stamps `firecube_coord_managed`. NaT holes remain for windows not yet processed. Pods verify-or-error; they never fill holes.

### Marker lifecycle

Each coordinate array carries at most one of two mutually exclusive markers: `firecube_preallocated` (grid or irregular values written; pods verify-only) and `firecube_coord_managed` (engine-managed observed surface; pods verify-only). Both markers present is a terminal error: detection raises `SchemaDriftError` before any slot read or write, and the store requires manual inspection. An array with neither marker is a pre-marker legacy cube; current `preallocate` creates the coordinate array first and then stamps the marker as a separate call through the metadata authority. For observed coordinates (`stamp_coord_managed_marker`) the marker is stamped before any value is written; for grid and irregular coordinates the values are written first and `firecube_preallocated` is stamped afterwards. An interrupted run can therefore leave a created coordinate without its marker (or, for grid and irregular coordinates, with values and no marker).

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
