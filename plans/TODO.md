# TODO

Accepted work only. Speculative ideas go to [IDEAS.md](IDEAS.md) until a design discussion promotes them.
New decisions are recorded in [DONE.md](DONE.md) with a date.

## Active Work

---

### §33 DirectZarr slot-range parallelism - roadmap

- **Landed:** `IndexSpec` + `RegularTimeAxis` + `ResolvedIndex`; byte parity for FCI and OPERA; recursion defect fixed.
- **Landed:** `IntegerAxis` + engine-owned `.firecube/index/current.json` record (`ResolvedIndexRecord`); `firecube zarr index show/verify/rebuild` CLI; atomic reader migration path documented.
- **Landed:** `IrregularTimeAxis` + `AUTO` sentinel + content-addressed item manifest; `--dry-run` for preallocate; `--derived` for index show; closes #27 spirit. See DONE.md 2026-08-24.
- **Landed:** `IndexedWrite` high-level abstraction + `build_indexed_write` hook; documented in `docs/guides/plugins/direct-zarr.md` and the API reference. See DONE.md 2026-08-24.
- **Future deprecation pass:** `SlotIndexModel`, `SlotAxis`, and `as_legacy_slot_index_model()` are kept for byte-parity compatibility with cubes written before the new index model. Schedule a deprecation pass once all known production cubes have been migrated via `firecube zarr index rebuild`. The deprecation should add `DeprecationWarning` on construction, update the allowlist in `test_api_docs_coverage.py`, and remove the symbols in a subsequent major version.

---

### §35 Repo-wide cross-package private-import hardening

**Status**: OPEN
**Scope**: architecture invariant, static test
**Priority**: medium

### Motivation

Post-implementation review during PR #40 preparation found that `test_import_boundaries.py` protects the templates/plugins boundary but not the CLI, and that no test enforces "no cross-package underscore imports" anywhere in the tree. RF-12 (`test_cli_no_private_cross_subsystem_imports.py`, added by PR #40) closes the CLI-specific gap, but the underlying repo-wide invariant is still missing. AST scan of `src/` at PR #40 review time found several cross-subsystem private imports that should be triaged individually:

- `src/firecube/ingestor/runtime/telemetry.py:22` — `from firecube.core.observability.metrics import _to_number, normalize_run_summary`
- `src/firecube/ingestor/runtime/tensogram/strategy.py:28` — `from firecube.core.tensogram.converter import _find_time_dim`
- `src/firecube/core/zarr/validation.py:34` — `from firecube.core.filesystem.ops import _open_fsspec_url  # type: ignore` (already an accepted deviation per DESIGN.md fsspec allowlist)
- `src/firecube/core/tensogram/converter.py:31` — `from firecube.core.filesystem.ops import _open_fsspec_url` (same — accepted deviation)
- Possibly others surfacing when the invariant is added

### Proposed approach

1. AST-scan `src/firecube/**/*.py` for `ImportFrom` nodes with `name` starting with a single underscore and `module` in a different first-level subpackage than the importing file (e.g., `firecube.ingestor.X` importing from `firecube.core.Y` is cross-subsystem; `firecube.core.zarr.X` importing from `firecube.core.zarr._Y` is same-subsystem).
2. Build a `_PERMANENT_ALLOWLIST` seeded from the AST scan, with an explicit rationale per allowed entry (mirror the `test_no_raw_fsspec_usage.py` pattern).
3. For each currently-allowlisted entry, decide: **promote** (rename to public), **accept** (document in DESIGN.md §"Accepted Deviations"), or **defer** (leave in allowlist with a TODO reference).
4. Reject any NEW cross-subsystem private import that is not in the allowlist.

### Not this branch

PR #40 closes the CLI-specific gap only. The repo-wide invariant, allowlist seeding, and individual triage are follow-up work.

### References

- `tests/architecture/test_cli_no_private_cross_subsystem_imports.py` — RF-12, the CLI-scoped precursor
- `tests/architecture/test_import_boundaries.py` — the existing templates/plugins invariant to model after
- `tests/unit/test_no_raw_fsspec_usage.py` — the canonical `_PERMANENT_ALLOWLIST` pattern

### §29 Test suite overhaul

**Goal:** Reduce noisy static/docs failures and raise behavioral bug-detection
power in the default test loop.

**Current state:**
- The default test loop must prioritize runtime behavior over static prose,
  broad snapshots, and implementation-shape checks.
- Test quality rules live in [TESTING_STANDARDS.md](TESTING_STANDARDS.md).
- High-risk missing behavior coverage lives in [TEST_GAPS.md](TEST_GAPS.md).
- Generic tests must use neutral sample product/plugin names unless the test is
  explicitly about an external plugin migration path.

**Direction:**
1. Keep the fast lane focused on behavior and failure modes.
2. Keep docs/static/help checks narrow, semantic, and outside the default
   behavioral signal when practical.
3. Replace mock-heavy or assertion-light tests with real behavior tests around
   deletion, workspace cleanup, storage operations, plugin management, and
   source readers.
4. Prioritize the P0/P1 gaps in [TEST_GAPS.md](TEST_GAPS.md).

**Acceptance criteria:**
- Default development and CI lanes are explicit and documented.
- A one-line help/prose diff does not fail the fast behavioral lane.
- Strict collection has no unexpected dependency skips.
- Every remaining xfail names an accepted TODO item and removal condition.
- The highest-risk control-plane and deletion bugs are covered by behavior
  tests, not only static checks.

---

### §8 Tensogram ingest routing

**Goal:** Clarify whether `GenericTensogramIngestor` is a public plugin base class, an internal write-path adapter, or both.

**Current state:**
- `GenericTensogramIngestor` exists in `src/firecube/ingestor/templates/generic_tensogram.py` and is exported from `firecube.ingestor.api`.
- `firecube ingest --output-format tensogram` currently accepts only plugins based on `GenericZarrIngestor`, then dynamically borrows `GenericTensogramIngestor._process_batch`.
- A plugin that directly subclasses `GenericTensogramIngestor` appears likely to be rejected by the current CLI guard because it is not a `GenericZarrIngestor` subclass.
- `GenericTensogramIngestor` currently supports only local `.tgm` targets.

**Decision (2026-06-11):** Tensogram is BOTH the archive format and a proper public output format, with the same first-class status as `GenericZarrIngestor`. `GenericTensogramIngestor` is a public plugin base class: direct subclassing must be supported and CLI-routable, the runtime `type()` class synthesis in `cli/main.py` must be replaced with explicit strategy/capability routing, and the local-only target restriction must be lifted via staged upload (reusing the `archive create` temp-file + transfer pattern). See `plans/AUDIT.md` (C4).

**Phase 0 — DONE (2026-06-11):** tensogram dependency upgraded `0.17.0` → `0.21.0` (all three packages, pins `>=0.21.0,<0.22`). See DONE.md 2026-06-11 for details, including the legacy-archive validate fix and the checked-in 0.17 compat fixture.

**Verified broken today (evidence for Phase 1, confirmed identical on tensogram 0.17 and 0.21 — pre-existing, not upgrade regressions):**
- `firecube ingest <plugin> --output-format tensogram --target file://.../out.tgm` fails with `[Errno 21] Is a directory`: the control-plane root (`.firecube/`) is created *inside* the `.tgm` target path before the strategy writes, turning the target into a directory. The synthesized-class path inherits zarr-oriented target preparation that is wrong for a single-file format.
- Archive restore loses time-coordinate encoding: CF `units`/`calendar` live in xarray `.encoding` and are never serialized by the converter, so restored cubes get a raw float64 ns-epoch time coord that xarray cannot decode back to datetime64. Restored variables also receive the raw base-entry dict (`name`, `zarr_chunks`, `zarr_compressor`, `zarr_fill_value`, nested `attrs`) as variable attrs — metadata pollution. Fix: serialize variable `.encoding` (at least time units/calendar) into the firecube message metadata and reapply on restore; map only real attrs onto restored variables. **Ownership: this is a firecube bug, not a tensogram one** — the metadata frame is free-form (room to store encoding), the engine's base-entry passthrough is fed by keys we put there, and our restore path already post-processes the decoded dataset. No upstream change required; optional upstream nice-to-haves (a variable-attrs convention in tensogram-xarray base entries, a logical datetime64 dtype hint in tensogram descriptors) are tracked in IDEAS.md, not blockers.

**Phase 1 — DONE (2026-06-11):** `DatasetProducer` protocol defined in `ingestor/contracts/`; CLI `type()` synthesis removed; direct `GenericTensogramIngestor` subclasses are now CLI-routable via protocol-checked strategy selection. `_GenericBatchIngestor` dissolved; `GenericZarrIngestor` and `GenericTensogramIngestor` re-parented directly onto `BaseIngestor`. Evidence: `tests/unit/test_tensogram_routing.py`, `tests/unit/test_template_hierarchy.py`. See DONE.md 2026-06-11 audit dispositions C3, C4.

**Phase 2 plan (separate feature branch) — OPEN:**
1. Fix single-file target handling: control-plane root must not be created inside a `.tgm` path (product-local control plane next to the file, or workspace-local) — resolves the `Is a directory` failure above.
2. Lift the local-only restriction via staged write: `.tgm` to the run workspace, upload through `StorageSession`/`create_filesystem` (extract the existing `archive create` remote pattern). `--write-mode staged` for remote targets; `direct` stays local-only with a loud error. One-driver invariant respected.
3. Control-plane parity: record spans with `SpanCoverage(write_strategy="tensogram", time_dim_name=...)`. Decide ResumeGuard semantics for an overwrite-only format (recommended: each run replaces prior spans for the same slice, `force_reingest`-like).
4. Restore fidelity: fix the time-encoding loss and attrs pollution described above; adopt `verify_hash=True` on decode/restore when frames carry `HASH_PRESENT` (0.21 feature; legacy frames skip with a note, mirroring the validate behavior shipped in Phase 0).
5. Config hygiene: implement or delete the unused `tensogram_message_granularity` field in `TensogramTemplateConfig`.
6. Public surface: scaffolding template, `tests/fixtures/tensogram_capable_test_plugin` (added to the conftest sessionstart guard), remote staged-upload integration test (moto), golden-help regen.
7. Docs + doctrine: output-format page under `docs/concepts/output-formats/` (via `write-user-doc`/`write-plugin-doc` prompts), DONE.md entry closing §8, AGENTS.md "Where things live" update.

---

### §5 Catalog and standards integration

**Goal:** Build a bridge from Firecube's internal view (Zarr + WAL-backed control-plane records) to higher-level discovery standards.

**Direction:**
- Starting from existing Intake support, explore emitting STAC or OGC EDR-style metadata as optional helpers.
- Keep this layer optional and generic. Firecube should not lock users into a single discovery stack.

---

### §7 Safe parallel ingestion patterns

**Goal:** Parallelize ingestion on Kubeflow/Argo safely without ACID/Icechunk complexity.

**Current state:**
- `AppendStrategy` (used by `GenericZarrIngestor`) serializes writes with a process-local lock. `pipeline_workers>1` parallelizes preprocessing but still writes one batch at a time.
- `IndexedRegionStrategy` (used by `DirectZarrIngestor`) writes pre-planned, disjoint index ranges via `RegionZarrWriter`. No appends. `IndexedRegionStrategy.write_groups()` auto-computes `expected_time_count` from `WriteIntent.ts_index`; undersized timestamped arrays hard-fail instead of resizing at write time. This is the building block for safe parallelism.
- `FilesystemClaimService` (`src/firecube/core/controlplane/claims.py`) provides exclusive write claims with heartbeat-based stale detection.
- T3 (§7-GENERIC) complete: `GenericZarrIngestor` now uses `WriteDomain(category="zarr_append", ...)` — the construction lives in `src/firecube/ingestor/runtime/zarr/batch_runner.py` since the §22 facade thinning, wired from `templates/generic.py`; integration fixtures updated in `tests/integration/test_maintenance_claims.py` and `tests/integration/test_obstore_claim_atomicity.py`. Code commit: `6329217`.
- T2 (§7-DIRECT) complete: `DirectZarrIngestor` now uses `WriteDomain(category="zarr_region", name=f"{group}:schema")` for schema setup and `WriteDomain(category="zarr_region", name=f"{group}:slot={ts_index}")` for per-slot intent dispatch via optional `claim_for_slot`. `IndexedRegionStrategy.write_groups()` groups intents by `ts_index` and uses fallback `claim_for_slot → claim_for_group → nullcontext`.

**Verified evidence (from review)**: `DirectZarrIngestor.claim_for_group()` used to build `WriteDomain(product=product, category="zarr_group", name=str(group_name))` — per-group granularity only. §7-DIRECT changed this to schema and per-slot `zarr_region` claims. The remaining safe-parallelism gap is the planner/orchestrator layer that assigns deterministic, disjoint index ranges before dispatch.

**Recommended safe model today:** Append writes serialize to one writer per `(product, group)`. DirectZarr writes may run concurrently only across pods with pre-planned disjoint slot/group ranges via the `firecube zarr slots` planner. Do not rely on intra-pod `pipeline_workers > 1` for write parallelism on DirectZarr templates: the per-slot claim has no retry loop, so any same-slot collision between in-flight batches fails hard (empirically reproduced 2026-07-12). For large per-slot payload plugins (e.g. MTG FCI FDHSI: ~14.8 GiB/slot measured), scale via `pipeline_workers=1` × N disjoint-range pods; total cluster memory is unchanged but sized per-pod it fits standard nodes.

**§7-sub / Phase 3 planner — DONE (2026-05-28):** See DONE.md (section 7-sub) for full deliverables. Engine/template-level planner with deterministic time-to-index mappings, chunk-aligned ranges, `firecube zarr slots` JSON output, `firecube zarr preallocate` schema preflight, `--slot-start/--slot-end/--slot-size` CLI flags, K8s env discovery, 6-row ResumeGuard conflict matrix, and per-pod `run_id` derivation are all shipped. (These commands originally shipped as `firecube plan` / `firecube zarr setup-schema` and were later renamed.)

    **§7-Phase 3.1 hardening — DONE (2026-05-28):** See DONE.md §7-Phase 3.1 for full deliverables. Closes 5 safety gaps surfaced by external review: strict `global_expected` coverage enforcement, intent-group-in-schema hard fail, all-arrays chunk validation, `--slot-group` CLI flag + env var, group-aware ResumeGuard, slot-range-aware completed-span check, split bypass semantics (`resume_existing` vs `force_reingest`), and `--slot-group` propagation through capability gate + plan output. All 4 Final Verification reviewers approved. Commits: C1=94d167e, C2=4a5212e, C3=ba90260.
    **Phase 3.2 follow-up DONE (2026-05-29):** See DONE.md §7-Phase 3.2 for full deliverables. Closes 6 review issues: run_id slot_group isolation, strict schema validation via SchemaDriftError + verify_array_spec, per-group slot_size in firecube plan, _ParallelExecutionState + ctx._ctx escape removal, pod-startup schema verification + audit record, docs drift cleanup. All 4 Final Verification reviewers approved. Commits: C1=37f7521, C2=80583a2, C3=84bb1bb, C4=6b6f3a1, C5=450f8f7, C6=416a4b6.
    **Phase 3.3 external-review follow-up DONE (2026-05-29):** See DONE.md §7-Phase 3.3 for full deliverables. Closes 6 external-review issues: sequential slot filter parity, terminal partial chunk + plan-to-ingest contract, URL-encoded slot_group in run_id + WAL reader, phantom group prevention in capability gate + pod-startup, operator docs refresh, sharding test assertion strengthening. All 4 Final Verification reviewers approved. Commits: C1=9fce649, C2=545689f, C3=40977ca, C4=8fc455d, C5=c6fd238, C6=269acdf.
    **Phase 3.4 external-review follow-up DONE (2026-05-30):** See DONE.md §7-Phase 3.4 for full deliverables. Closes 3 additional review issues: phantom global_expected validation added to the planner CLI preflights (now `firecube zarr slots` + `firecube zarr preallocate`), warn_if_misaligned terminal-partial awareness, sharding test spy on xr.Dataset.chunk. All 4 Final Verification reviewers approved. Commits: C1=604a4d8, C2=a9fc6e8, C3=adf41dd.
    **Phase 3.5 external-review follow-up DONE (2026-05-30):** See DONE.md §7-Phase 3.5 for full deliverables. Closes 2 additional review issues: firecube plan fail-closed on blocked partial-chunk ranges (silent slot-loss bug fixed), zarr_schema() called exactly once per plan invocation. All 4 Final Verification reviewers approved. Commits: C1=2992425, C2=8fda71a. Phase 3.6 follow-up DONE 2026-05-30 (closes 1 additional review issue). Phase 3.7 follow-up DONE 2026-05-30 (closes 2 additional review issues: CLI error remediation text + operator docs refresh). Phase 3.8 follow-up DONE 2026-05-31 (closes 2 additional review issues: delete-span remediation now includes --product/--force/full safety flow + regression test strengthened to lock runnable shape).

    **Builds on:** §4a (DONE.md) — `IndexedRegionStrategy` is the missing primitive that enables disjoint-region writes. §21, §23, §23-AUTO, §7-DIRECT, §7-sub, and Phase 3.1 are all DONE as of 2026-05-28. Safe within-group parallelism is fully delivered for `DirectZarrIngestor` plugins.

---

### §9 Span planning (optional pre-declared spans)

**Goal:** Make spans a stable unit of work for orchestration and maintenance without making ChunkManager product-aware.

**Current state:** Spans are recorded post-hoc from write coverage (`span.time_index_ranges` + `meta.time_min/time_max`).

**Next step:** Optional SDK hook for "span intent":
- Plugins/templates may pre-declare spans (month windows or chunk-aligned index ranges).
- Engine records `status="started"` spans at begin and finalizes to `complete/failed/noop` with actual coverage at end.

**Builds on:** §4a (DONE.md) — Write strategies now produce explicit write intents; pre-declared spans become a natural extension where templates declare spans before executing strategies.

---

### §11 Multires as a post-step — DONE (2026-05-26)

See DONE.md §25 for details.

**Current state (post-§25):** The post-step CLI `firecube zarr multires <target> --storage-type <local|s3> --storage-driver <fsspec|obstore>` exists and wraps `ZarrMultiresBuilder`. Ingest-time multires has been removed: `AppendMultiresHandler` no longer exists (deleted in §25, commit `89b2737`). The `zarr_multi_res` config field is rejected with a `ValueError` pointing to the CLI subcommand.

**Direction:** Use `firecube zarr multires` as a post-step after ingestion completes. No ingest-time multires path remains.

**Builds on:** §4a (DONE.md) — Zarr is now a runtime subsystem with explicit finalize/commit boundaries. §25 completed the removal of the broken ingest-time path.

---

## Harden Implicit Logic and Heuristics

These items identify areas where the system used greedy logic to guess user intentions. The goal is to make these explicit or more robust. Status re-verified against the code on 2026-06-11.

### §12.2 CLI and Execution Engine

- **BASENAME heuristics — DONE:** `output_name` is no longer guessed from the target path basename. `ProductIdentity.from_uri` hard-fails without an explicit product name, and the `default_output_name` config key is rejected at parse time.
- **Magic output detection — DONE:** the CLI and engine read typed `PipelineResult.outputs` / `result.output_path` attributes; no dict key sniffing remains. The legacy `output_path=` constructor kwarg was removed entirely (DONE.md 2026-06-11).
- **Explicit safety — OPEN:** refusal to upload `HOME` or `/` is hardcoded in the engine's upload-source resolution; should be documented or configurable.
- **Free-form option overload — DONE (2026-06-11):** keys owned by dedicated `firecube ingest` flags (`write_mode`, `slot_start`, `slot_end`, `slot_size`, `slot_group`) are hard-rejected at `--option` parse time with remediation naming the owning flag (`_TYPED_FLAG_OWNED_KEYS` in `cli/_typed_options.py`); the silent post-resolution override is closed. All other typed flags were never config fields and were already rejected as unknown keys. Engine options without a dedicated flag (`force_reingest`, `no_progress`, ...) remain the sanctioned `--option` surface.

### §12.3 Configuration Derivation

- **Automatic env resolution — PARTIALLY FIXED:** `${VAR}` expansion is now scoped to `[storage]` values only (not all config strings), but remains unconditional within that scope — a literal `${FOO}` storage value is impossible when `FOO` is set. Remaining work: opt-in flag or escape syntax.
- **DuckDB option tier is disconnected — OPEN:**
  `src/firecube/core/config.py:get_plugin_defaults()` deliberately does not
  merge `[database.duckdb]`, while
  `src/firecube/ingestor/runtime/configure.py:TierConfigurator` allowlists
  `duckdb_max_temp_directory_size`, `duckdb_memory_limit`, and
  `duckdb_threads` only after they are already present in `ctx.options`.
  `src/firecube/cli/_typed_options.py:_enumerate_all_valid_keys()` does not
  include that tier, so `--show-options` omits the keys and typed `--option`
  rejects them. `DuckDbMixin` in
  `src/firecube/ingestor/extensions/duck.py` therefore has no coherent public
  route for receiving its resource settings. Define one typed owner for these
  fields, load `[database.duckdb]` only for plugins that declare the DuckDB
  capability, and expose the same keys through CLI discovery and coercion.
  Acceptance: config-file and `--option` values reach `DuckDbMixin` with typed
  values; `--show-options` lists the keys only for compatible plugins; plugins
  without the capability reject them; public configuration docs describe the
  same surface.
- **`default_product_name` leaks into plugin option validation — OPEN:**
  `src/firecube/cli/main.py` reads the key from plugin defaults to resolve the
  product name, but leaves it in the `options` mapping passed to
  `IngestContext`. `TierConfigurator.configure()` in
  `src/firecube/ingestor/runtime/configure.py` then rejects it because
  `SYSTEM_KEYS` in `src/firecube/ingestor/config/engine.py` does not include the
  key and `PluginConfig` in `src/firecube/ingestor/types/config.py` does not own
  it. Consume this CLI-owned key before tier validation; do not weaken strict
  unknown-key rejection. Acceptance: a config-only `default_product_name`
  completes runtime configuration, while product-name precedence remains CLI
  flag > config `default_product_name` > plugin `PRODUCT_NAME` > hard failure.

### §12.4 Plugin Heuristics

- **Option aliases — DONE (2026-06-11):** `zarr_chunk` deleted (no shim, per STYLE.md); `zarr_chunk_shape` is the single chunking option and `zarr_chunk` now fails strict unknown-key rejection. Locked by `tests/unit/test_zarr_chunk_alias_removed.py`.
- **Regex guessing — FIXED in-repo:** no filename-regex horizon extraction or `F*` folder discovery remains in core or templates. The msg_frm occurrences live in the external plugin repository.
- **Hardcoded defaults — DONE (2026-06-11):** lat/lon soft limits gone; multires `(1.0, 0.5)` single-sourced as `DEFAULT_MULTIRES_RESOLUTIONS` (no silent fallback); `group="FWI"` fallback removed from `core/zarr/layers.py`; `"fire_risk.duckdb"` default removed from `extensions/duck.py`. Evidence: `tests/unit/test_domain_defaults_removed.py`.
- **Typed-vs-free-form drift — DONE (2026-06-11):** strict unknown-key rejection enforced on all declared typed configs; `x_*` experimental namespace implemented — keys matching `x_*` pass through without rejection. Evidence: `tests/unit/test_experimental_options.py`.
- **Discovery knobs unreachable from config — OPEN:**
  `discover_input_files` in `src/firecube/core/formats/discovery.py` accepts
  `exclude`, `include_suffixes`, `recursive`, and `sniff_hdf5`, but
  `EngineConfig` in `src/firecube/ingestor/config/engine.py` exposes only
  `include_patterns` (additive `preferred_globs`). The default
  `discover_source_files` hook in `src/firecube/ingestor/runtime/base.py`
  therefore offers no way to exclude files or narrow the accepted suffix set
  without overriding the hook. Decide whether exclusion belongs in
  `EngineConfig` as a typed option (e.g. `exclude_patterns`) and expose it
  through CLI discovery and coercion alongside `include_patterns`.
  Acceptance: excluding a file that the default suffix set would otherwise
  pick up requires no plugin code; `--show-options` lists the key; public
  discovery documentation describes the same surface.

---

### §13 Harden PluginContext boundary

**Goal:** Make plugin hook context truly read-only and non-bypassable in normal usage.

**Current state (partially addressed):**
- `options` is a detached copy wrapped in `MappingProxyType`. Mutations to `RuntimeIngestContext.options` after `PluginContext` creation are invisible to plugins.
- `option()` reads from the frozen proxy. `ctx.options["k"]` and `ctx.option("k")` are consistent.
- `__getattr__` blocks direct access to `storage`, `_chunk_manager`, `_materializer`.
- 6 boundary tests in `tests/unit/test_plugin_context_boundary.py`.

**Remaining (structural):**
- `_ctx` escape hatch: `pctx._ctx` still gives full access to `RuntimeIngestContext` (see `src/firecube/ingestor/types/context.py:198`). Python cannot enforce true private attributes, but a frozen-snapshot design would eliminate the reference entirely.
- **Phase 3.2 partial closure**: The `_parallel_global_schema` escape hatch (the specific `ctx._ctx._parallel_global_schema` pattern used by `DirectZarrIngestor`) was removed in Phase 3.2 C4 (commit `6b6f3a1`). The broader `_ctx` reference-sharing issue remains open.
- Frozen snapshot blocked by initialization order: `PluginContext` is created in `BaseIngestor.run()` (`src/firecube/ingestor/runtime/base.py`) before `runtime_ctx.telemetry` is assigned a few lines later. Fixing this requires restructuring `run()` so telemetry is assigned before context construction.
- Per-worker snapshots: all workers share one `PluginContext` instance wrapping the same `RuntimeIngestContext`. Safe for reads (options detached), but `_ctx` reference-sharing means a convention-breaking plugin could mutate shared state.
- No tests assert `_ctx` is inaccessible (Python cannot enforce this without structural change).

---

### §31 Parquet template configuration parity

**Goal:** Eliminate accepted Parquet configuration keys that silently have no
effect on persisted output.

**Current state:** `ParquetTemplateConfig` in
`src/firecube/ingestor/templates/config.py` declares `parquet_partition_by` and
`parquet_row_group_size`. `GenericParquetIngestor.write_parquet()` in
`src/firecube/ingestor/templates/generic.py` calls `pyarrow.parquet.write_table`
without applying either value. Both keys therefore pass strict configuration
validation but do not change the output.

**Direction:**
1. Pass `parquet_row_group_size` to the default writer and validate invalid
   values before the first write.
2. Remove `parquet_partition_by` from the accepted config until the template has
   an explicit dataset-directory layout, deterministic part naming, write-domain
   ownership, and resume semantics for partitioned output. Do not emulate
   partitioning inside the current one-file-per-batch contract.
3. Keep `docs/reference/config.md` and
   `docs/guides/plugins/generic-parquet.md` aligned with the implemented surface.

**Acceptance criteria:**
- Every accepted `ParquetTemplateConfig` field changes persisted Parquet output;
  unsupported fields fail as unknown configuration.
- Row-group behavior is verified from real Parquet metadata for local output and
  through the selected storage driver for remote output.
- No plugin needs to override `write_parquet()` to use the advertised row-group
  setting.

---

### §32 Public output-writer contract for custom pipelines

**Goal:** Let an external `BaseIngestor` plugin write through Firecube's selected
storage driver using a stable, typed public contract.

**Current state:** `StorageContext` is exported by
`src/firecube/ingestor/api.py`, and `PluginContext.storage.output` exposes the
bound session at runtime. Its annotation in
`src/firecube/ingestor/types/context.py`, however, names the concrete
`StorageSession` from `src/firecube/core/storage/session.py` under
`TYPE_CHECKING`. That concrete class is not exported by
`src/firecube/ingestor/api.py` or `src/firecube/core/api.py`. A custom
`BaseIngestor._process_batch()` implementation can therefore use the object only
through an internal import, implicit duck typing, or `Any`; none defines a
supported plugin contract.

**Direction:** Define a narrow writer `Protocol` in the plugin contract layer,
type `StorageContext.output` against it, and export it from the appropriate
public `api.py` surface. Keep the concrete `StorageSession` internal. The
protocol must expose only operations required by custom output pipelines and
must preserve the one-driver rule; it must not provide a route around Firecube's
write coordination or control-plane ownership.

**Acceptance criteria:**
- An external `BaseIngestor` plugin can type-check and perform its output writes
  using imports from `firecube.ingestor.api` and `firecube.core.api` only.
- The public protocol has behavior-backed local and S3 driver coverage for its
  declared operations.
- Existing template implementations continue to use the same bound session and
  selected driver without deep imports becoming part of the plugin API.
- Public plugin documentation names only the protocol, not
  `firecube.core.storage.session.StorageSession`.

---

### §14 Expand concurrency and race-condition coverage — DONE (2026-06-11)

**Goal:** Catch production-only failures before deployment.

All four open points closed. Evidence:
- Concurrent ingestor instances (same product, `resume_existing`/`force_reingest` combinations): `tests/integration/test_concurrent_same_product.py`
- Workspace materialization races on the same remote URI: `tests/integration/test_workspace_materialization_race.py`
- Control-plane write ordering/consistency under concurrent batch completion: `tests/integration/test_wal_concurrent_ordering.py`
- OTel/thread context propagation under error and retry scenarios: `tests/integration/test_otel_context_concurrency.py`

---

## Zarr Runtime Follow-ups

These items address gaps and bugs surfaced after §4a landed. They are sequenced as refinements/fixes to work that has already shipped, not as new architectural directions. Items §25 and §26 are runtime-crashing bugs and should be prioritised.

### §21 Strategy Protocol unification — DONE (2026-05-27)

See DONE.md §21 for details.

Follow-up: Check `firecube-msg-frm` external plugin for `ZarrWriteStrategy` import and migrate — out of scope for Phase 1.

---

### §22 GenericZarrIngestor full thinning — DONE (2026-05-27)

**Goal:** Move `GenericZarrIngestor._process_batch` toward a true thin facade.

**Original state (pre-§22):** `_process_batch` was 144 lines and owned five responsibilities inline: staged metadata seeding, lock construction, strategy construction, claim closure construction, and metrics assembly.

**Completed (2026-05-27):** §22 helper extraction is done in `src/firecube/ingestor/runtime/zarr/batch_runner.py` with 5 named helpers and `tests/unit/test_batch_runner.py` (13 tests). T5 wired those helpers into `GenericZarrIngestor._process_batch`, replacing all five inline blocks.

The core write step remains delegated to `AppendStrategy.write_groups()`.

**Result:** `_process_batch` is now a thin orchestrator around preparation, URI/option resolution, batch_runner helper wiring, `strategy.write_groups()`, and final result construction. ~66 lines (down from 143). See DONE.md §22 (helpers) and §22 (wiring complete).

**Builds on:** §4a (DONE.md).

---

### §23 RegionZarrWriter resize-on-write contradicts documented contract — DONE (2026-05-27)

See DONE.md §23 for details.

### §23-AUTO Auto-compute expected_time_count from WriteIntent.ts_index — DONE (2026-05-27)

See DONE.md §23-AUTO for details.

---

---

### §25 AppendMultiresHandler stale kwarg — DONE (2026-05-26)

See DONE.md §25 for details.

Follow-up: Check `firecube-msg-frm` external plugin for `zarr_multi_res` usage and update if needed — out of scope for this PR.

---

### §27 Stale `build_dataset` documentation — DONE (2026-05-26)

See DONE.md §27 for details.

---

### Phase 2.1 Legacy zarr_group claim sweep removal — DONE (2026-05-28)

`_sweep_legacy_zarr_group_claims` removed in Phase 3 (T18). See DONE.md §7-sub.

---

### §30 `ensure_timestamp_slot` should treat `time_indexed=False` arrays as coords — DONE (2026-06-25)

**Goal:** Stop static (non-time-indexed) coordinate arrays from being mistaken for time axes during the time-slot bounds check, so declaring `time_indexed=False` is sufficient on its own.

**Current state:** `RegionZarrWriter.ensure_timestamp_slot` skips an array from the `ts_index < shape[0]` bounds check only if its name is in `coord_names` (constructor param, default `{"y","x","channel"}`) or it is scalar (`ndim == 0`). It does **not** consult the schema's `ZarrArraySpec.time_indexed=False` flag. The strategy builds `coord_names_by_group` from `spec.coord_names` only.

**Bug it caused:** A DirectZarr plugin (OPERA SEVIRI/NORDLIS) declared static 2-D `lat`/`lon` and 1-D `ny`/`nx` projection coords with `time_indexed=False` but did not also list them in `coord_names`. `ensure_timestamp_slot` then read each static coord's dim-0 (the grid size, e.g. 1072/1332) as a time axis and raised "ts_index out of bounds" for any `ts_index` beyond it — i.e. any day far from the reference epoch (slot ~622080 for a 2023 day against a 2018 epoch). Latent for any multi-day ingest; was masked earlier by the axis-size error. The plugin worked around it by adding the names to `coord_names`.

**Direction:** Fold every `time_indexed=False` array into the writer's coord-skip set (the schema already knows which arrays have no time axis), as a union with the existing `spec.coord_names`. Then `time_indexed=False` alone is sufficient and a plugin never has to *also* name the array in `coord_names`.

**Acceptance criteria:**
- A DirectZarr group whose static coords are declared only via `time_indexed=False` (not in `coord_names`) ingests a high-`ts_index` timestamp without a spurious "ts_index out of bounds" error.
- `coord_names` remains honored for back-compat (union, not replacement).

**Surfaced by:** OPERA plugin fix declaring `lat`/`lon`/`ny`/`nx` in `coord_names` (plugin commit `bb9cf7a`).

See: DONE.md "DirectZarr plugin parity — core fixes" (2026-06-25).

---

## Zarr migration framework (deferred from CF-1.8 plan)

**Goal**: a generic Zarr migration framework so future migrations (rename-dim, rename-array, set-default-attrs, schema upgrades, etc.) plug in via a Protocol implementation rather than each shipping its own CLI command. Decided against `migrate-dim` and similar name-coupled one-offs.

**Shape**: `MigrationStrategy` Protocol (`@runtime_checkable`) + in-source registry + a generic safety runner.

**Generic runner responsibilities**:
- Refuse to run if `ChunkManager.list_runs(non_terminal=True)` returns any non-terminal runs.
- Refuse to run if `ChunkManager.list_claims()` returns active claims on target groups.
- Acquire and release a write claim around the migration.
- Provide `--dry-run`.
- Group-explicit `--group <g>` or `--all-groups`.
- Use `ChunkManager` facade + `create_filesystem(config)` — never raw `.firecube/` writes or `fsspec.filesystem(...)`.

**CLI shape**: `firecube zarr migrate <strategy-name> [strategy-args] [--dry-run] [--group <g> | --all-groups]`, with `firecube zarr migrate --list` and per-strategy `--help`.

**First strategy to ship**: `rename-dim` (the operation the CF-1.8 plan would have shipped). Required behavior:
- Walk every Zarr array in the target.
- Rewrite `dimension_names` in each array's metadata.
- Rename the coordinate variable folder (`{group}/timestamp` to `{group}/time`).
- Rebuild consolidated metadata if present.
- Preserve internal control-plane array names (e.g. `firecube_timestamp_state` stays — only its inner dim name changes).

**Out of scope for v1**: multi-resolution pyramid groups; `--force` to bypass safety; auto-migration on append.

**Acceptance criteria for the follow-up plan**:
- Compatibility matrix tests for at least the 8 rows defined in `cf18-compliance` plan.
- E2E `legacy cube -> migrate -> append with new dim -> readback` test.
- Integration tests covering refusal-on-active-claim and dry-run-no-mutation.

---

## CF advisor enhancements (deferred from CF-1.8 plan)

- `firecube advise compliance --profile cf-18 --all-groups`: walk all Zarr groups in a product instead of requiring `--group`.
- Tier 2 — CF Standard Name Table vocabulary validation (check `standard_name` values against the canonical table).
- Tier 3 — UDUNITS-based unit validation. Likely via optional `cf-checker` runtime dependency or a NetCDF temp-export bridge.

---

### Concurrent-metadata-read follow-ups (deferred from the 2026-06-22 `read_bytes` fix)

The 2026-06-22 `StorageFilesystem.read_bytes` change fixed the s3fs 412 crash on the
existing-cube dim-compat read. Two optional hardening items were deliberately NOT
bundled (they change a safety check's timing / touch a separate path):

- **Reduce redundant dim-compat reads.** `_verify_existing_cube_batch_groups` runs
  the dim-compat check once *per batch* (`engine._create_batches_with_parallel_filter`),
  so a pod re-reads the same group `zarr.json` many times. Cache the result per-pod,
  and/or gate the check in parallel slot-range mode where `_verify_schema_at_pod_startup`
  already guarantees the dims from the plugin-declared `time_dim_name`. Care: this
  changes when a public safety check (`verify_dim_compatibility`, DONE.md 2026-06-20)
  runs — preserve the genuine pre-existing-cube append guard.
- **Route `core/zarr/validation.py::_load_array_metadata` through `read_bytes`** for
  consistency (still uses `open().read()`). Lower priority — it's the `firecube zarr
  validate` maintenance path, not the concurrent ingest hot path.

---

### §28 Audit closeout follow-ups (minor)

- **Dead `plugin` marker** — registered in `pyproject.toml`, applied zero times. Decide: delete the registration or apply it to plugin-specific suites.
- **T4 residual** — `tests/unit/test_append_strategy.py` still patches the strategy's own internals; real-store coverage lives in `tests/integration/test_append_strategy_behavior.py`. Candidate for a behavior-based rewrite of the remaining wiring test.
- **A7 watch-item** — the archive-create path (`src/firecube/core/tensogram/converter.py`, `_find_time_dim` call around line 112) does not thread a plugin-declared `time_dim_name`; only the ingest path does.
- **C3 watch-item** — the hierarchy flatten grew `BaseIngestor` to ~815 lines (flatten-by-hoisting); the depth test pins templates only, not plugin-subclassing-plugin chains. Watch for god-base growth; consider extracting batch-lifecycle hooks into a composed collaborator if it keeps growing.

---

### §33 DirectZarr codec parity — extend ZarrArraySpec with codec fields

**Goal:** Give `DirectZarrIngestor` plugins the same codec-configuration surface that the `zarr_codecs` field delivers for `GenericZarrIngestor`, and unify codec resolution across both write paths so custom compressors work identically in staged and direct modes.

**Current state:** `RegionZarrWriter.ensure_group()` in `src/firecube/core/zarr/region_writer.py:309-424` passes zero codec kwargs to `zarr.create_array()`, so `DirectZarrIngestor` plugins write uncompressed arrays regardless of the `zarr_codecs` configuration. `ZarrArraySpec` in `src/firecube/ingestor/templates/direct_zarr.py:337-373` has no codec-related fields. The `zarr_codecs` list shape and `resolve_compressor()` helper in `src/firecube/ingestor/runtime/zarr/write.py` were introduced for the staged/append path in issue #25; direct-path parity was explicitly deferred to this follow-up.

**Direction:**
1. Extend `ZarrArraySpec` with optional per-array codec fields matching the Zarr v3 metadata triple: `filters`, `serializer`, and `compressors`, each accepting `list[dict] | None`. Per-array values override the template-level `zarr_codecs` default.
2. Relax the Phase 1 single-compressor-only validation in `ZarrTemplateConfig` and the equivalent per-array validator to accept full Zarr v3 pipeline chains (zero or more filters, zero or one serializer, zero or more compressors, in the correct order).
3. Extend `resolve_compressor()` or introduce a `resolve_codec_pipeline()` helper that supports the full three-element pipeline for use by both write paths.
4. Wire the pipeline through `RegionZarrWriter.ensure_group()` to `zarr.create_array(filters=..., serializer=..., compressors=...)`.
5. Update `docs/reference/config.md` to describe per-array codec declarations and multi-codec chains.

**Acceptance criteria:**
- A `DirectZarrIngestor` plugin can declare per-array codecs on `ZarrArraySpec` and see them reflected in the written `zarr.json`.
- Multi-element `zarr_codecs` in `ZarrTemplateConfig` no longer triggers the "chains are not supported in this release" parse error.
- Behavior tests cover: per-array codec overrides the template default; correctly-ordered pipeline is accepted; incorrectly-ordered pipeline is rejected with a specific error naming the ordering rule.
- No regression in Phase 1 behavior for `GenericZarrIngestor` plugins that use only the template-level `zarr_codecs`.
- `DirectZarrIngestor` plugins that do not declare codec fields continue to write uncompressed (the default change belongs to a follow-up).

**Builds on:** Issue #25 — the `resolve_compressor()` helper and the `zarr_codecs: list[dict]` shape are the primitives this work extends.

**Prerequisites before merging:**
- Issue #25 shipped and included in the base branch.
- Regression harness for `DirectZarrIngestor` plugins that exercises the write path end-to-end against a real Zarr store.

**Effort:** Medium (2-4 days). Cross-cutting through `ZarrArraySpec`, `RegionZarrWriter`, both strategies, and the shared resolver.

---

### §34 DirectZarr default codec and resume-time codec-drift detection

**Goal:** Give `DirectZarrIngestor` plugins compression by default (parity with `GenericZarrIngestor`) and detect codec drift on resume so a run that changes codec configuration does not silently produce a cube with mixed codec lineage.

**Current state:** After §33 (`DirectZarr codec parity`), `DirectZarrIngestor` plugins can declare codecs on `ZarrArraySpec`, but the default when no codec is declared remains uncompressed. `RegionZarrWriter.ensure_group()` passes no codec kwargs to `zarr.create_array()` unless the spec provides them. The schema hash in `src/firecube/ingestor/templates/direct_zarr.py:84-122` does not include codec configuration, so a resume that changes codec config is silently accepted and no `SchemaDriftError` is raised even though the on-disk cube may have a different codec than the plugin now declares.

**Direction:**
1. Change the `DirectZarrIngestor` default in `RegionZarrWriter.ensure_group()` from no codec kwargs to the same default preset that issue #25 introduced for `GenericZarrIngestor` (Blosc/zstd, clevel=5). Cold migration: existing uncompressed cubes stay uncompressed on append; only fresh arrays get the new default.
2. Include codec configuration in the schema-drift hash in `src/firecube/ingestor/templates/direct_zarr.py`. On resume, a mismatch raises `SchemaDriftError` with a message naming the offending codec fields.
3. Document the migration path in `docs/reference/config.md`: how to keep an existing uncompressed cube uncompressed on append versus how to re-ingest under a new codec.

**Acceptance criteria:**
- A brand-new `DirectZarrIngestor` cube created after this change has Blosc/zstd/5 compression by default (verified by reading `zarr.json`).
- An existing uncompressed `DirectZarrIngestor` cube can be re-opened for append with the same plugin config and continues to write uncompressed arrays. No silent codec upgrade.
- A resume that changes codec config raises `SchemaDriftError` before any write, naming the changed codec fields.
- Behavior test: create cube with codec A; attempt resume with codec B; assert `SchemaDriftError`.
- Behavior test: create uncompressed cube; resume with the same no-codec config; assert append succeeds with uncompressed arrays.
- `docs/reference/config.md` documents the default change and the resume-time drift behavior.

**Builds on:** §33 (`DirectZarr codec parity`) — per-array codec fields on `ZarrArraySpec` must exist before the default change and drift detection can be built on top of them.

**Prerequisites before merging:**
- §33 shipped and included in the base branch.
- Migration-scenario regression harness covering: uncompressed-cube append, codec-change drift error, brand-new-cube default codec.

**Effort:** Medium (3-5 days). Touches `DirectZarrIngestor` schema-hash logic, default codec injection, drift-error surface, and documentation. The cold-migration boundary is subtle and requires explicit tests.
