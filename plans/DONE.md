# Done

## 2026-08-10 — #40 — DirectZarr codec CLI parity + default alignment + RF-11/RF-12 invariants

Closes GitHub #40. Fixes three linked issues in the codec configuration surface: a silent PR #25 regression (default was uncompressed on-disk), the DirectZarr CLI routing gap for codec options, and divergent template defaults between GenericZarr and DirectZarr. Also promotes an internal codec-derivation helper to a public runtime API and closes an adjacent behavioral parity gap in `firecube zarr preallocate`.

### What

**Default flip**: `ZarrTemplateConfig.zarr_compression` now defaults to `True` (was `False`). Restores the pre-PR-25 effective on-disk behavior — where zarr's own default codec (`ZstdCodec(level=0)`) applied — and aligns firecube with zarr-python v3's upstream default. Explicit `zarr_compression = false` still disables compression.

**DirectZarr CLI routing wired**: `DirectZarrIngestor.template_config_class = ZarrTemplateConfig`. Without this declaration, `TierConfigurator` computed `template_keys = set()` and rejected `--option zarr_compression=...` and `--option zarr_codecs='[...]'` as "Unknown configuration options" for every DirectZarr plugin. The internal codec plumbing was correct but unreachable from the CLI.

**Preallocate codec routing parity**: `firecube zarr preallocate` was passing schema kwargs (shape, dtype, chunks, attrs) to `ensure_group()` but not the effective codec pipeline. `--option zarr_compression=false` and `--option zarr_codecs='[...]'` were silently ignored at the preallocate stage even after the TierConfigurator wiring. Fixed by calling `derive_effective_codecs_for_spec(arr_spec, ingestor.template_config)` and forwarding filters/serializer/compressors to `ensure_group()`.

**Preallocate validation parity**: `firecube zarr preallocate` now calls `validate_zarr_specs_against_template` before mutating arrays, matching `DirectZarrIngestor._process_batch`. Previously the CLI could create broken zarr arrays before the validator would have caught the codec conflict; now the failure is fail-fast, no partial mutation.

**`derive_effective_codecs_for_spec` promoted from private to public runtime helper**: the codec-priority derivation helper (per-array > template > default) moved from `templates/direct_zarr.py._derive_effective_codecs_for_spec` to `runtime/zarr/write.py:derive_effective_codecs_for_spec`. New home is next to its sibling public helper `resolve_codec_pipeline`. Shared between `DirectZarrIngestor` (per-batch schema setup and write dispatch) and the `firecube zarr preallocate` CLI command, both of which materialize Zarr arrays from a plugin-declared schema and must apply the same codec precedence rules. The old private name is removed with no alias (per STYLE.md "Option Aliases" rule).

**`cli/zarr.py` imports `DirectZarrIngestor` via `firecube.ingestor.api`**: the CLI command previously did a deep import from `firecube.ingestor.templates.direct_zarr`. Now uses the public re-export. Small correctness fix aligning CLI with the "public surfaces" convention documented in STYLE.md.

**RF-11 architecture invariant** (`tests/architecture/test_template_config_class_declared.py`): AST-based static invariant that enumerates concrete `BaseIngestor` subclasses referencing `self.template_config` and requires each to declare a non-None `template_config_class`. Would have caught the DirectZarr routing gap before implementation. Abstract classes (via `ABC`/`abstractmethod`) are exempt.

**RF-12 CLI private-import invariant** (`tests/architecture/test_cli_no_private_cross_subsystem_imports.py`): AST-based static invariant scoped to `src/firecube/cli/**`, rejects any `ImportFrom` naming a single-underscore-prefixed symbol from a module outside `firecube.cli.*` (i.e., `firecube.ingestor.*` or `firecube.core.*`). Same-package private imports and dunders are allowed. Closes the enforcement gap: prior to RF-12, no static test rejected underscore-prefixed cross-subsystem imports from `src/firecube/cli/**`, so the initial `cli/zarr.py:_derive_effective_codecs_for_spec` leak was not caught before implementation.

### Consequences

- Default codec on-disk for both GenericZarr and DirectZarr default ingests is now `ZstdCodec(level=0)` (matching zarr-python v3). Pre-fix, GenericZarr default was uncompressed on-disk (PR #25 regression); DirectZarr default was already compressed via the fallback in the old `_derive_effective_codecs_for_spec`. The two templates now behave identically.
- Existing cubes written with `zarr_compression=false` remain valid on resume as long as the ingest continues to pass the flag explicitly.
- Codec drift detection at `verify_array_spec()` is spec-gated (line 567): only fires when the plugin's `ZarrArraySpec` declares codec fields. Default resumes (no per-array codec declaration) never trigger drift, so the default flip does not break existing stores.
- `firecube plugins describe direct_zarr_capable_test_plugin` now shows `zarr_compression` and `zarr_codecs` under Template Options.
- `firecube zarr preallocate` behavioral parity with `DirectZarrIngestor` for codec validation and routing.
- Cross-subsystem private imports from `src/firecube/cli/**` are now statically forbidden by RF-12.

### Test coverage

- 7 new test files: CLI matrix for DirectZarr ingest / GenericZarr ingest / `firecube zarr preallocate` (3 × 3 codec cases + 1 malformed-JSON negative + 1 preallocate validation-parity); per-array override preservation (unit); resume-safety regression (integration); RF-11 architecture invariant (AST, mutation-verified); RF-12 CLI private-import invariant (AST, mutation-verified).
- Full test suite (excluding the one pre-existing failure): 2430 passed, 0 new failures. One pre-existing failure (`test_phase3_3_plan_to_ingest_contract::test_plan_remainder_range_accepted_by_ingest`, JSONDecodeError, unrelated to codecs) confirmed present on HEAD before this work.

### Verification

Commit hash: TBD (same commit as this DONE.md entry).

### Source

GitHub #40 — https://github.com/eumetsat/firecube/issues/40

## 2026-08-09 — §33+§34 — Per-array codec overrides and shared codec pipeline module

Closes TODO.md §33 and §34. Firecube no longer injects an opinionated default codec; zarr's own default applies when no codec is declared. Per-array codec overrides are now first-class in the plugin contract.

### What

**`ZarrArraySpec` codec fields**: `filters`, `serializer`, and `compressors` optional fields added to `ZarrArraySpec`. Plugins can declare per-array codec pipelines without touching `ZarrTemplateConfig`.

**`ZarrTemplateConfig.zarr_codecs` validator relaxed**: accepts full Zarr v3 pipelines (flat list, ordering validated: filters first, then serializer, then compressors). Previously only a single-element list was accepted.

**`resolve_compressor()` replaced by `resolve_codec_pipeline()`**: the old helper injected a Blosc/zstd/clevel=5 default when no codec was declared. The new helper has no opinionated default — zarr's own default applies when the pipeline is empty.

**`RegionZarrWriter.ensure_group()` accepts codec kwargs**: `filters`, `serializer`, and `compressors` are forwarded to zarr array creation.

**`_derive_effective_codecs_for_spec()` helper**: resolves the effective codec pipeline for a `ZarrArraySpec` in DirectZarr mode, merging spec-level overrides with template-level defaults.

**`verify_array_spec()` extended**: per-field codec drift checks added. Canonical comparison uses `arr.metadata.codecs` (not a string repr).

**`_compute_schema_hash()` extended**: conditional codec fields included in the hash when present. Backward-compatible: schemas with no codec declarations hash identically to before.

**`validate_zarr_specs_against_template()` cross-config validator**: catches codec conflicts between `ZarrArraySpec`-level overrides and `ZarrTemplateConfig`-level defaults at schema-setup time.

**`src/firecube/core/zarr/codec_pipeline.py`**: new shared module for codec normalization, comparison, and pipeline splitting (filters / serializer / compressors). Used by both the DirectZarr and GenericZarr paths.

### Consequences

- Firecube no longer injects Blosc/zstd/clevel=5 as a default. Existing stores with that codec are unaffected (the codec is stored in `zarr.json`); new stores without a declared codec get zarr's default.
- `GenericZarrIngestor` no longer sets a default compressor. Plugins that relied on the implicit default must now declare it explicitly via `zarr_codecs` or per-array `compressors`.
- `ZarrArraySpec.filters`, `.serializer`, `.compressors` are all optional and default to `None` (no override). Existing plugin code requires no changes.
- Schema hash is backward-compatible: the new codec fields are conditional, so schemas without codec declarations produce the same hash as before.

### Verification

Commit hash: TBD (same commit as this DONE.md entry).

Source: closes TODO.md §33 and §34.

---

## 2026-08-07 — #25 — `zarr_codecs` config field and codec pipeline wiring (recorded retroactively 2026-08-09)

Closes GitHub #25. Shipped the first iteration of user-configurable Zarr codecs for `GenericZarrIngestor` and the staged/append write path.

### What

**`zarr_codecs: list[dict] | None` on `ZarrTemplateConfig`**: new optional config field accepting a single-element list of codec entries in Zarr v3 metadata format (`[{"name": "...", "configuration": {...}}]`). Requires `zarr_compression = true`. Selects any codec registered via zarr's extension mechanism by name.

**`resolve_compressor()` helper**: resolved the active compressor from `zarr_codecs` (when set) or fell back to the Blosc/zstd/clevel=5 default for `GenericZarrIngestor`. (This helper was superseded by `resolve_codec_pipeline()` in §33.)

**Blosc/zstd/clevel=5 default for `GenericZarrIngestor`**: applied when `zarr_compression=true` and no `zarr_codecs` entry was declared. (Removed in §33 — firecube no longer injects an opinionated default.)

**Staged/append write path wiring**: `resolve_compressor()` threaded through the staged metadata seeding path and the append write executor so codec selection was consistent across write modes.

### Test coverage

542 lines across 5 files.

### Commits

- `fa41f78` — main implementation
- `174534b` — follow-up fix

### Source

GitHub #25 — https://github.com/eumetsat/firecube/issues/25

---

Date: 2026-08-05 Task: #27

Decision: Accept GitHub #27 — automate slot allocation for `DirectZarrIngestor` via an opt-in cadence mixin. Promotes IDEAS.md §21 Idea 2 to TODO.md §33 (decision-only entry; implementation follows on a feature branch, milestone v0.2.0).

Context: Plugins opting into `SUPPORTS_SLOT_RANGE_PARALLELISM` must hand-implement `timestamp_to_ts_index` / `global_expected_time_count` / `slot_index_model`, and the same epoch/cadence math is duplicated near-verbatim across the tutorial, ~20 test fixtures, and both production parallel plugins (`firecube-opera-seviri-nordlis`, `firecube-mtg-fci-l1c`) — the trigger condition IDEAS.md §21 set for promotion. `SlotIndexModel` already carries `(epoch, cadence_s, mode)` per group but those fields are inert identity metadata, never consumed for arithmetic anywhere in `src/`; only the derivation behavior is missing. Design decided: an opt-in mixin (working name `CadenceSlotAllocation`) rather than base-class defaults, so the `__init_subclass__` guard at `templates/direct_zarr.py` and its tests stay untouched (the MRO satisfies the guard naturally, and the base never learns about the mixin). Scope is fixed integer-second cadence with a finite horizon in v1; irregular-cadence products (polar orbiters: non-integer periods, drifting overpass times) explicitly keep the manual three-hook contract. `ChunkManager` is unchanged — a derived model is content-addressed identically to a hand-written one. Full design constraints and acceptance criteria in TODO.md §33.

Consequences: TODO.md §33 created (accepted scope + mixin design constraints); IDEAS.md §21 Idea 2 marked PROMOTED (Ideas 1 and 3 remain UNDECIDED); IDEAS.md §34 added for the unrelated gridding-extension boundary findings surfaced by the same evaluation.

Source: GitHub #27 — https://github.com/eumetsat/firecube/issues/27

---

Date: 2026-08-03 Task: #22

Decision: Ship PEP 561 `py.typed` marker for downstream IDE type support

Context: Downstream plugin authors' type checkers (pyright, mypy) could not resolve firecube imports with type information because the package lacked a PEP 561 marker. Root-level placement at `src/firecube/py.typed` is required — subpackage-only triggers mypy #16149. The marker is zero-byte per PEP 561 standard for typed packages (`partial\n` is for stub packages only). Attribution file `py.typed.ABOUT` added per repo convention for files that cannot carry header comments. One contract test added asserting both file existence and zero-byte size.

Files changed:
- `src/firecube/py.typed` (new — zero-byte PEP 561 marker)
- `src/firecube/py.typed.ABOUT` (new — attribution metadata)
- `tests/sdk/test_py_typed_marker.py` (new — contract test)
- `plans/DONE.md` (this entry)

Verification: `uv run pytest tests/sdk/test_py_typed_marker.py --strict-deps -q` → 1 passed. `unzip -l dist/firecube-*.whl | awk '{print $4}' | grep -x 'firecube/py.typed' | wc -l` → 1. `uv run ruff check .` clean; `uv run pyright` clean. Regression: test fails when marker deleted, passes when restored.

Source: GitHub #22 — https://github.com/eumetsat/firecube/issues/22

---

## 2026-07-13 — `write_1d` numpy>=2 one-slot idiom (production blocker)

OPERA-SEVIRI-NORDLIS ingest failed under the pinned `numpy>=2.3.3` runtime with `ValueError: Could not convert object to NumPy datetime` at `RegionZarrWriter.write_1d`. Root cause: `arr[scalar_int] = one_element_ndarray` on a `datetime64[s]` Zarr array is silently accepted by numpy 1.x (broadcast) but rejected by numpy 2.x (strict shape). The failing pattern was `self._open_root()[f"{group}/{array_name}"][ts_index] = data` at `src/firecube/core/zarr/region_writer.py:639`. Reproduced end-to-end against real NORDLIS NetCDFs from CloudFerro S3.

### What

**T1 — `write_1d` numpy-2-safe one-slot idiom** (`3da6468797291b85edec15786fc4d81fff27fc7e`): Replaced scalar-index setitem with the one-element slice-assign idiom `arr[i:i+1] = payload.reshape(1)` for 1-D targets. Explicit `ValueError` for zero-length or multi-element payloads on 1-D targets locks the "write EXACTLY one timestamp slot" contract (DESIGN.md §"Risks To Avoid": coverage tracker records only one `ts_index` per intent; multi-slot payloads would create silent data-integrity gaps). Higher-rank target behavior preserved byte-identical (`arr[ts_index] = payload` with `arr.shape[1:]` validation). Regression tests added covering datetime64 1-elem writes, parametrized numeric dtypes, 0-D scalar payloads (preserves scalar-payload path used by `test_indexed_region_time_1d_coverage.py`), invalid-payload rejection on 1-D targets, and shape-mismatch rejection on higher-rank targets.

### Consequences

- `write_1d` contract now documented explicitly: "write exactly one timestamp slot" — never multi-slot.
- Callers that previously wrote 0-D scalars OR 1-elem ndarrays are unaffected.
- Callers passing multi-element ndarrays to 1-D targets (previously silently mis-writing via numpy 1.x broadcast) now fail loudly with `ValueError`. No such caller was found in firecube-core; external plugins should validate their payload construction.
- Blast radius bounded: only `region_writer.py:639` had the failing pattern; `write_timestamp:693` uses 0-D scalar (safe), `write_region` uses `y_slice` (safe), `write_static` uses `arr[...]` (safe), `state.py:212` uses scalar-to-slice fill (safe).
- Tech debt flagged (out of scope for this fix): `write_1d` is a poorly-named method (a slot payload, not necessarily a 1-D array). Future rename candidate: `write_slot_payload` or `write_time_indexed_intent`.

### Verified

- `uv run pytest tests/unit/test_region_zarr_writer.py -q` → all pass (35 total, 8 in `TestWrite1D`)
- `uv run pytest tests/unit/test_indexed_region_time_1d_coverage.py -q` → all pass (scalar payload path preserved)
- `uv run ruff check src/firecube/core/zarr/region_writer.py tests/unit/test_region_zarr_writer.py` → clean
- `uv run pyright src/firecube/core/zarr/region_writer.py` → 0 errors
- End-to-end smoke: real NORDLIS file → `normalize_string_vars` → `.isel(time=0).values` → `np.atleast_1d` → `write_1d` → `E2E_SMOKE_OK`

### Evidence

- Commit: `3da6468797291b85edec15786fc4d81fff27fc7e` fix(zarr): write_1d writes exactly one slot, numpy>=2-safe idiom
- Source: internal plan `fix-write-1d-numpy-2x-slice-index`.

**Confidence:** HIGH

## 2026-07-11 — Resume-guard perf Wave 1 — repo-boundary memoization + bulk stale sweep CLIs

OPERA cube backfill hit ~4900 runs. Between the "Parallel capability validated" log line and the resume-guard decision, pods paused for roughly 3.5 minutes. Root cause: `ResumeGuard.enforce()` called `_list_run_entries` twice per invocation (once in `_snapshot.py` and once in the guard itself), with no shared cache. At 4900 runs each call enumerated the full `.firecube/runs/` directory, so every pod paid the cost twice. A second axis: after a cluster crash, stale claims and abandoned runs blocked `snapshots rebuild` until an operator manually cleared each record one at a time.

Wave 1 ships two targeted fixes without touching the underlying O(N) scan (deferred to Wave 2, IDEAS.md §16): an enforce-scoped repo-boundary cache that eliminates the redundant enumeration call, and `--all-stale` bulk CLIs that let operators unblock a crashed cluster in one confirmed action instead of N.

### What

**T1 — repo-boundary run-entry cache scaffold** (`12788d6`): Added `_RunEntriesCache` to `src/firecube/core/controlplane/repo.py` — an enforce-scoped ephemeral dict keyed by product. Cache is NOT persisted (DESIGN §27: derived read models only; no hidden state). Lifetime is one `enforce()` call.

**T2 — `list_stale_claims` helper** (`010c6bb`): Added `ManifestRepository.list_stale_claims()` to enumerate claims whose heartbeat has expired. Feeds the bulk-clear CLI (T10) and the crash-recovery workflow (T13).

**T3 — `list_stale_runs` helper** (`e109c49`): Added `ManifestRepository.list_stale_runs()` to enumerate runs stuck in a non-terminal state past their heartbeat window. Feeds the bulk-abandon CLI (T11) and the crash-recovery workflow (T13).

**T4 — resume-guard metrics in `RUN_SUMMARY_SCHEMA`** (`b2bfd1e`): Extended the canonical metric schema with `resume_guard_enforce_duration_s`, `resume_guard_runs_enumerated`, and `resume_guard_spans_scanned`. Collector goes through `TelemetryService` (DESIGN §63: no ad-hoc Prometheus imports).

**T5 — counting-filesystem test harness** (`502adf8`): Added `CountingFilesystem` wrapper in `tests/unit/_helpers/counting_fs.py` for op-count assertions. Used by T6 tests to assert the redundant `_list_run_entries` call is gone.

**T6 — enforce-scoped cache wired into `ResumeGuard`** (`7847254`): `ResumeGuard.enforce()` now scopes the repo-bound `_RunEntriesCache` with `ManifestRepository.run_entries_cache_scope()`. Both `list_runs()` and `_load_current_state()` transparently hit the shared repository cache, so op-count tests (via T5) confirm the directory is enumerated exactly once per enforce call regardless of run count.

**T7 — bulk clear stale claims via manager** (`040af8f`): `ChunkManager.clear_stale_claims(dry_run: bool = True)` iterates `list_stale_claims()` and releases each only when the caller passes `dry_run=False`. The CLI adds the `--yes-i-really-mean-it` confirmation gate on top (DESIGN §36: no silent auto-reclaim).

**T8 — bulk abandon stale runs via manager** (`b6201ce`): `ChunkManager.abandon_stale_runs(dry_run: bool = True)` iterates `list_stale_runs()` and records each as abandoned only when the caller passes `dry_run=False`. The CLI adds the same explicit-confirmation guard as T7.

**T9 — emit resume-guard timing and counters** (`dc5c203`): `ResumeGuard.enforce()` records wall time and scan counts into the run summary via `TelemetryService`. No bare `except:` added (DESIGN §93: fail-loud).

**T10 — `chunks claims clear --all-stale`** (`a084567`): New CLI flag on `firecube chunks claims clear`. Requires `--yes-i-really-mean-it`. Prints a per-claim summary and a final count. Documented in T12.

**T11 — `chunks runs abandon --all-stale`** (`cf1ed29`): New CLI flag on `firecube chunks runs abandon`. Same confirmation gate as T10. Documented in T12.

**T12 — `--all-stale` bulk sweep flags documented** (`e6087b9`): CLI reference updated with flag descriptions, confirmation requirement, and expected output shape.

**T13 — post-crash recovery workflow** (`c7bfeeb`): New `docs/operations/chunk-manager/post-crash-recovery.md` page. Covers the full sequence: identify stale claims, bulk-clear, identify stale runs, bulk-abandon, re-trigger `snapshots rebuild`. One confirmed action per step, not N.

**T14 — Wave 2 (§16) and Wave 3 (§17) captured in IDEAS.md** (`e04e781`): IDEAS.md §16 records the LSM active-run index + completed-slots bitmap design (UNDECIDED). IDEAS.md §17 records terminal run pruning + auto-rebuild triggers (UNDECIDED, blocked on Wave 2).

**T15 — end-to-end crash recovery integration test** (`eb697ae`): `tests/integration/test_crash_recovery_bulk_sweep.py` simulates a mid-run cluster crash, verifies stale claims and runs are detected, bulk-clears both, and confirms `snapshots rebuild` succeeds afterward.

### Guardrails preserved

- **DESIGN §11** (one container == one run): unchanged. The cache is enforce-scoped ephemeral state; it does not alter run identity or lifecycle.
- **DESIGN §27** (derived read models): upheld. `_RunEntryCache` is an in-memory, enforce-lifetime dict. Nothing is persisted to `.firecube/`.
- **DESIGN §36** (explicit abandonment): preserved. `--all-stale` requires `--yes-i-really-mean-it` on every bulk operation. No silent auto-reclaim path exists.
- **DESIGN §63** (observability boundaries): upheld. All new metrics go through `TelemetryService`; no ad-hoc `prometheus_client` imports added.
- **DESIGN §93** (fail-loud): upheld. No bare `except:` blocks added anywhere in this wave.

### Verification

- `uv run pytest tests/unit/ tests/integration/test_crash_recovery_bulk_sweep.py -q` — 52 unit + 2 integration tests green.
- `uv run ruff check .` clean; `uv run ruff format --check .` clean; `uv run pyright` 0 errors on changed files.

### Follow-up

- **IDEAS.md §16** — Wave 2: LSM active-run index + completed-slots bitmap for O(range) resume-guard. UNDECIDED.
- **IDEAS.md §17** — Wave 3: terminal run pruning + auto-rebuild triggers. UNDECIDED, blocked on Wave 2.

---

## 2026-06-29 — fsspec atomic writer is content-atomic on local disk (no zero-length window)

Fixed a flaky high-contention concurrency failure where a loser thread racing for the `slot_index_model:current` claim read `current.json` mid-write and got `ManifestError: slot-index record is not valid JSON: Expecting value: line 1 column 1 (char 0)`. Reproduced ~1-in-40 with `test_concurrent_same_model_high_contention` (20 threads, same model). Root cause: `FsspecAtomicWriter.write_atomic` published via `open(path, "xb")`. On local disk `O_EXCL` makes only the *name* appear atomically — the file is created empty and filled incrementally, so a concurrent reader observes a 0-byte file. Losers in `ChunkManager.ensure_slot_index_model` read `current.json` *outside* the claim the instant they see `ClaimConflictError` (`manager.py:694`), i.e. exactly inside the winner's write window. S3 (s3fs `xb` uploads-on-close) and obstore (`PutMode.Create`, `LocalStore` temp+rename) were already content-atomic; only the local fsspec path had the gap, silently violating the "complete old-or-new object, never partial" invariant the rest of the system already relies on (see the 2026-06-19 `read_bytes` entry).

### What

**Content-atomic local publish via temp-file + `os.link`.** For the local filesystem, `write_atomic` now writes a sibling temp file (`tempfile.mkstemp` in the destination's directory), `fsync`s it, then `os.link(tmp, dst)` to publish the fully-written inode in a single step, and removes the temp in a `finally`. `os.link` raises `FileExistsError` if the target already exists, so the create-if-not-exists `FileExistsError` contract (`protocol.py`) the claim layer depends on is preserved. Non-local backends keep the unchanged `open("xb")` + 412→`FileExistsError` normalization path, selected by a new `_is_local()` probe on the fsspec protocol. `os.link` (not `rename`) is deliberate: `rename` clobbers, which would break create-only semantics — this is the create-only counterpart to the overwrite primitive deferred in the 2026-06-26 orphan-slot entry.

- The fix lives in the fsspec storage driver, NOT the control plane, per DESIGN.md "One driver everywhere / No ad-hoc storage wiring". The `AtomicWriter` Protocol is the seam; `claims.py`/`manager.py` stay driver-agnostic. Because the fix is at the `write_atomic` seam it also closes the same zero-length-read window for concurrently-read **claim** lock files (`list_claims`/heartbeat parse their JSON).

### Files touched

- `src/firecube/core/filesystem/fsspec_backend.py` — `FsspecAtomicWriter.write_atomic` local branch (`_is_local`, `_write_atomic_local`); docstring corrected (`O_EXCL` is name-atomic, not content-atomic).
- `tests/unit/test_fsspec_atomic_writer.py` — 3 local-fs tests: full-payload persist, conflict→`FileExistsError` with no clobber, no temp-file leak.
- `tests/integration/test_slot_index_s3_precondition_retry.py` — forces the writer's remote branch (`_is_local()→False`) so the injected s3fs 412 surface still exercises the normalization now that local no longer routes through `open("xb")`.

### Verification

- `uv run pytest tests/integration/test_slot_index_concurrency.py::test_concurrent_same_model_high_contention -q` — 60 consecutive runs green (was failing ~1/40 before the fix).
- `uv run pytest --strict-deps tests/unit/test_fsspec_atomic_writer.py tests/unit/test_no_raw_fsspec_usage.py tests/integration/test_slot_index_concurrency.py tests/integration/test_slot_index_precedence_matrix.py tests/integration/test_slot_index_s3_precondition_retry.py tests/unit/test_chunk_manager_slot_index.py tests/integration/test_obstore_claim_atomicity.py tests/integration/test_maintenance_claims.py -q` — all green.
- `uv run ruff check .` clean; `uv run ruff format --check .` clean; `uv run pyright` 0 errors on changed files.

---

## 2026-06-26 — OPERA parallel ingest — orphan slot metadata recovery

Eliminated the `ResumeConflictError "Non-range run ... is active"` failure that blocked `--parallelism 8` OPERA backfill runs against a fresh S3 target. Root cause: `WalReader.read_run_entry()` returns an orphan run entry (no `slot_range`, no `slot_group`) when `run.json` is missing during a parallel-start race. The projection faithfully propagated the orphan state into `RunInfo`, and `ResumeGuard` correctly (but unhelpfully) rejected a new pod invocation as a conflicting non-range run. Five targeted fixes were applied: a canonical suffix parser for recovery (T1), projection-layer orphan healing (T2), resume-meta orphan healing (T3), full slot-metadata threading through the three-facade terminal-record chain (T4), and CLI diagnostic exposure (T5). `ResumeGuard` itself is unchanged — the guard's logic was correct; the fix is upstream.

### What

**T1 — `parse_pod_run_id_slot` parser** (`232e3d1`): Added `parse_pod_run_id_slot()` parser to `src/firecube/core/controlplane/repo_utils.py`, alongside the existing `deserialize_slot_*` helpers. Recovery-only inverse of `derive_pod_run_id`; never used at write time. Architecture-independence test kept passing — `firecube.core` does not import `firecube.ingestor`.

**T2 — projection orphan healing** (`ecb8d32`, fixup `1ad2618`): Extended `_run_info_from_entry` in `_projection.py` to fall back to `parse_pod_run_id_slot` when the payload lacks `slot_range` or `slot_group`. Explicit payload values always win; fallback only fires on `None`. Logs a healing notice.

**T3 — resume_meta orphan healing** (`e4d35f6`): Extended `_resume_meta_for_run` in `_wal_writer.py` with the same fallback. Prevents orphan resume metadata from being propagated into `RunEventWriter`, which would otherwise write a slotless `run.json` on the next `_write_run_meta` call (the corruption-persistence vector).

**T4 — terminal slot preservation** (`929505c`): Added `slot_range`/`slot_group` kwargs (defaults `None`) to `record_run_terminal` across all three control-plane facades (`ChunkManager`, `ManifestRepository`, `ManifestWalWriter`) and the engine recording boundary (`SpanRecorder.register_run`). Mirrored the existing `record_run_started` slot-field signature at every layer. The terminal WAL event payload and the final `run.json` now carry slot fields when the caller supplies them.

**T5 — CLI slot field exposure** (`74684b0`): Added `slot_range` and `slot_group` to the JSON output of `firecube chunks runs list -f json`. `slot_range` serializes as a JSON array `[start, end]` or `null`; `slot_group` as a string or `null`. Text output unchanged.

### Files touched

- `src/firecube/core/controlplane/repo_utils.py` — `parse_pod_run_id_slot` parser added.
- `src/firecube/core/controlplane/_projection.py` — `_run_info_from_entry` orphan healing.
- `src/firecube/core/controlplane/_wal_writer.py` — `_resume_meta_for_run` orphan healing + `record_run_terminal` slot kwargs.
- `src/firecube/core/controlplane/repo.py` — `ManifestRepository.record_run_terminal` slot kwargs.
- `src/firecube/core/controlplane/manager.py` — `ChunkManager.record_run_terminal` slot kwargs.
- `src/firecube/ingestor/runtime/recording.py` — `SpanRecorder.register_run` slot kwargs.
- `src/firecube/cli/chunks/_runs.py` — JSON output slot fields.
- `tests/unit/test_run_id_slot_parser.py` — round-trip + malformed parser tests.
- `tests/unit/test_projection_orphan_slot_fallback.py` — orphan projection healing tests.
- `tests/unit/test_resume_meta_orphan_slot_fallback.py` — orphan resume_meta healing tests.
- `tests/integration/test_run_terminal_slot_preservation.py` — terminal slot preservation integration tests.
- `tests/cli/test_runs_list_slot_fields.py` — CLI JSON slot fields test.

### Verification

- `uv run pytest tests/unit/test_run_id_slot_parser.py -v` — round-trip and malformed-input parser cases.
- `uv run pytest tests/unit/test_projection_orphan_slot_fallback.py -v` — orphan projection healing.
- `uv run pytest tests/unit/test_resume_meta_orphan_slot_fallback.py -v` — orphan resume_meta healing.
- `uv run pytest tests/integration/test_run_terminal_slot_preservation.py -v` — terminal slot preservation.
- `uv run pytest tests/cli/test_runs_list_slot_fields.py -v` — CLI JSON slot fields.
- `uv run pytest tests/architecture/test_core_independence.py -v` — `firecube.core` does not import `firecube.ingestor`.
- `uv run pytest --strict-deps -q -m "not slow and not s3" tests/unit tests/integration tests/sdk tests/cli tests/architecture` — full local gate.

### Deferred

Atomic `run.json` overwrites. The existing `AtomicWriter.write_atomic` protocol is create-only (by design, raises `FileExistsError` on existing targets). Replacing the overwrite-heavy `_write_run_meta` calls requires a new `overwrite_atomic`/`replace_atomic` primitive with per-driver implementations (local POSIX `rename`, S3 conditional `PutObject`, obstore `PutMode.Overwrite`). The T2/T3 recovery primitives in this plan render the torn-read window harmless without it; eliminating the underlying race window is a separate, follow-up plan.

Reference: internal plan `opera-parallel-resume-conflict-fix`.

---

## 2026-06-26 — Static-array replay equality — dtype-safe

Fixed a latent `TypeError` in `_dispatch_static_intent`'s replay path: `np.array_equal(..., equal_nan=True)` raises `TypeError: ufunc 'isnan' not supported` when either operand has `dtype.kind` in `{"U", "S", "O", "V"}` (str/bytes/object/structured). No production callers were affected (all static arrays are `float64` today), but the bug would surface for any future plugin using non-float static arrays (e.g. `uint8` cloud masks, `datetime64[s]` scan-time tables, `U3` flag strings).

### What

**T1 — dtype-safe static replay helper** (`9fd9274`): Added private helper `_arrays_equal_missing_aware(a1, a2) -> bool` and constant `_ARRAY_EQUAL_NAN_UNSAFE_KINDS = frozenset({"U", "S", "O", "V"})` to `region_writer.py`, colocated with the existing `_fill_value_is_missing` / `_fill_values_equal` / `_array_is_all_fill` family. The helper uses `np.array_equal(..., equal_nan=True)` for kinds `{i, u, b, f, c, M, m}` (preserving NaT-aware semantics for `datetime64`/`timedelta64`) and falls back to plain `np.array_equal` for the four unsafe kinds. Both operand dtypes are checked. Replaced the direct `np.array_equal(equal_nan=True)` call at `indexed_region.py:365` with `_arrays_equal_missing_aware(existing, intent.data)`.

### Files touched

- `src/firecube/core/zarr/region_writer.py` — new `_ARRAY_EQUAL_NAN_UNSAFE_KINDS` constant + `_arrays_equal_missing_aware` helper.
- `src/firecube/ingestor/runtime/zarr/strategies/indexed_region.py` — import + single call-site replacement.
- `tests/unit/test_indexed_region_static_dispatch.py` — `test_static_replay_dtype_safety` parameterized test (5 dtype variants: int32, uint8, bool, datetime64-with-NaT, str-U3).

### Verification

- `uv run pytest tests/unit/test_indexed_region_static_dispatch.py::test_static_replay_dtype_safety -v` — all 5 parameterized cases pass.
- `uv run ruff check src/firecube/core/zarr/region_writer.py src/firecube/ingestor/runtime/zarr/strategies/indexed_region.py` — clean.
- `uv run pyright src/firecube/core/zarr/region_writer.py src/firecube/ingestor/runtime/zarr/strategies/indexed_region.py` — 0 errors.

Reference: internal plan `static-replay-dtype-safe`.

---

## 2026-06-25 — firecube zarr preallocate — typed-config and spec-attrs parity

Closed two gaps that prevented cadence-based plugins from using `firecube zarr preallocate` as the sanctioned pre-sizing step before a parallel backfill campaign. A third commit records a speculative follow-on idea.

### What

**T1 — typed-config in preallocate** (`ec9bdc8`): `firecube zarr preallocate` now calls `TierConfigurator.configure()` inline before invoking plugin hooks (`slot_index_model`, `global_expected_time_count`, `zarr_schema`). All three typed configs (`engine_config`, `template_config`, `plugin_config`) reach the hooks. Previously, hooks that read `self.plugin_config` (e.g. `reference_epoch`, `expected_timesteps_per_group`, `product_groups`, `cadence_overrides`) saw only defaults, blocking the OPERA dense-time cube rebuild.

**T2 — spec attrs/shards/dimension_names forwarded** (`f8f5168`): `firecube zarr preallocate` now forwards `attrs`, `shards`, and `dimension_names` from each `ZarrArraySpec` to `RegionZarrWriter.ensure_group()`. A subsequent `firecube ingest` against a preallocated store no longer raises `SchemaDriftError: attrs['units'] existing=None spec='K'`.

**T3 — §A idea recorded** (`14b8097`): Pure `kind="static"` for `time_indexed=True` arrays noted as a speculative follow-on in `plans/IDEAS.md §A` (DEFERRED-V2+). No code change.

### Files touched (summary)

- `src/firecube/cli/zarr.py` — T1 `TierConfigurator.configure()` call; T2 `attrs`/`shards`/`dimension_names` forwarding.

### Verification

- `uv run pytest tests/integration/test_preallocate_typed_config.py` — T1 plugin_config reaches hooks.
- `uv run pytest tests/integration/test_preallocate_spec_attrs.py` — T2 no SchemaDriftError on subsequent ingest.

Reference: internal plans `preallocate-typed-config-and-attrs` and `handoff-firecube-core-dense-time`.

---

## 2026-06-25 — DirectZarr plugin parity — core fixes

Closed 4 bugs and one dead-code removal surfaced during OPERA SEVIRI/NORDLIS plugin migration to the DirectZarr write path.

### What

**T1 — §30 union fix** (`7ff079c`): `IndexedRegionStrategy` (in `indexed_region.py`) now unions every `time_indexed=False` array name into the coord-skip set before constructing `RegionZarrWriter`. Previously, `ensure_timestamp_slot` read the dim-0 size of static 2-D arrays (e.g. grid height 1072) as a time-axis length and raised "ts_index out of bounds" for any slot index beyond it. The fix is a union, not a replacement, so existing `coord_names` entries remain honored for back-compat.

**T2 — slot-index ungate** (`7eb0975`): Extracted `_ensure_slot_index_model_at_startup` from `_verify_schema_at_pod_startup` in `direct_zarr.py`. The new method is called from `base.py` on ALL ingest paths (parallel and non-parallel). Non-declaring plugins (no `SUPPORTS_SLOT_RANGE_PARALLELISM`) see zero behavior change via a first-line early return. A `_slot_index_model_stamped` flag provides an idempotent per-pod fast-path.

**T3 — staged-seed marker strip** (`db4752f`): `_seed_group_via_session` in `staged_metadata.py` now strips `firecube_static_written` from seeded array `zarr.json` payloads before writing them into the workspace. All other attrs (`_ARRAY_DIMENSIONS`, `_FillValue`, codecs, shape, dtype) are preserved. The final published target still carries the marker after second-run commit, so write-once enforcement is unaffected.

**T4 — ScratchManager deletion** (`ba5542f`): Deleted `src/firecube/ingestor/runtime/scratch.py` and `tests/unit/test_scratch_manager.py`. Added `tests/unit/test_scratch_module_deleted.py` as a negative guard. Rationale: (a) zero internal callers in `src/`; (b) stdlib `tempfile.TemporaryDirectory` + `zipfile.ZipFile.extractall` is equivalent; (c) SIP heterogeneity (MTG-SIP, EO-SIP, Sentinel SAFE/SIP, EPS-SG ADF/SIP) rules out a generic helper; (d) continues the API minimization trajectory (see P1 entry above). External: the MTG plugin's `_scratch.py` shim needs a follow-up inline stdlib replacement.

### Files touched (summary)

- `src/firecube/ingestor/runtime/zarr/strategies/indexed_region.py` — T1 union fix.
- `src/firecube/ingestor/templates/direct_zarr.py` — T2 method extraction.
- `src/firecube/ingestor/runtime/base.py` — T2 call site.
- `src/firecube/ingestor/runtime/zarr/staged_metadata.py` — T3 marker strip.
- `src/firecube/ingestor/runtime/scratch.py` — T4 deleted.
- `tests/unit/test_scratch_manager.py` — T4 deleted.
- `tests/unit/test_scratch_module_deleted.py` — T4 negative guard added.

### Verification

- `uv run pytest tests/integration/test_direct_zarr_parity_slot_range.py` — T1 static-array slot check.
- `uv run pytest tests/unit/test_slot_index_model_non_parallel.py` — T2 non-parallel path.
- `uv run pytest tests/unit/test_staged_metadata_static_marker_strip.py` — T3 marker strip.
- `uv run pytest tests/unit/test_scratch_module_deleted.py` — T4 deletion guard.
- `uv run pytest --strict-deps` passes on `feat/directzarr-plugin-parity-core-fixes`.

Reference: internal plan `directzarr-plugin-parity-core-fixes`.

---

## 2026-06-22 — CF-1.8 advisor: CF010 exempts grid-mapping container variables

`_check_cf010` (the "data var missing `units`" check in `core/cf/validator.py`) no longer flags variables that are CF grid-mapping containers. A new `_grid_mapping_targets(ds)` collects every variable referenced via a `grid_mapping` attribute; CF010 skips those.

**Why**: a grid-mapping variable carries the CRS in its attributes and, per CF, has no data semantics and no `units` — so requiring units on it is a false positive. The OPERA plugin's `projection_definition` is exactly such a container; this was the last advisor error after the group-globals + time-coordinate work.

**Locked design decisions**:
- Reference-based, not self-tag: a variable IS a grid-mapping container precisely because a data variable references it via `grid_mapping` (that is CF's definition). Keying the exemption on a self-declared `grid_mapping_name` was rejected — it is data-dependent (e.g. geostationary vs LAEA) and would force plugins to hardcode a projection name.
- Narrow: only `grid_mapping` targets are exempted, NOT the other `_REF_ATTRS` kinds (`bounds` share their parent's units, `cell_measures` have units). The exemption only ever removes a false-positive error, so it cannot make previously-compliant data fail.
- Generic/core: lives in `firecube.core.cf`, the sanctioned CF home (DESIGN.md:86 carves the advisor out of the "no CF as a domain concept" rule). Benefits any plugin, not just OPERA. The plugin half (declaring `grid_mapping` references on its georeferenced fields) lives in the OPERA plugin repo.

Evidence: `tests/unit/test_cf_validator.py::test_cf010_exempts_grid_mapping_container` (referenced container exempt; non-referenced unitless var still errors — keeps the exemption narrow). Plugin-side end-to-end advisor check (`tests/test_ingestor.py::test_schema_dataset_is_cf_advisor_error_free`) now asserts zero errors across all 5 groups.

---

## 2026-06-22 — `ZarrGroupSpec.attrs`: generic group-level attributes for DirectZarr

Added an optional `attrs: Mapping[str, Any] | None = None` field to `ZarrGroupSpec` so DirectZarr plugins can declare group-level Zarr attributes (the missing sibling of the existing `ZarrArraySpec.attrs`). Firecube writes the mapping verbatim onto the group's `zarr.json` at schema setup and does not interpret it — **the mechanism is convention-agnostic** (no CF/STAC/ACDD knowledge in core; plugins decide what to publish). Motivation: a DirectZarr plugin had no way to emit dataset-level metadata (e.g. CF `Conventions`/`title`), so the CF-1.8 advisor's CF001-003 fired on OPERA stores; this closes that gap generically.

**Locked design decisions**:
- Group-level, NOT root-level. The CF advisor (and the GenericZarr/append parity path) read attrs from the *group* the data lives in (`xr.open_zarr(group=…).attrs`), not the root. Group attrs also avoid the root entirely, so they never collide with the slot-index identity-hash root attrs (`manager._mirror_attrs`) or need claim coordination. Root-attr writing was considered and rejected as unnecessary complexity.
- Canonical write seam, no parallel path: a new `RegionZarrWriter.set_group_attrs(group, attrs)` (`require_group(...).attrs.update(...)`, guarded by the existing `assert_attrs_safe`) is called once per group from the SAME two schema-setup sites that already stamp array attrs — `_setup_global_zarr_schema` (parallel slot-range startup) and `IndexedRegionStrategy.write_groups` (single-pod). `attrs.update` is idempotent.
- CF-agnostic naming throughout: `attrs` / `set_group_attrs` / `assert_attrs_safe`; the guard rejects only firecube-reserved names. Docstrings describe "convention-agnostic group-level metadata" with CF mentioned at most as one example (matching `ZarrArraySpec.attrs`). Per DESIGN.md:86 "firecube does NOT expose CF as a domain concept"; CF-1.8 lives only in plugins and the standalone `firecube advise compliance` module.
- Schema identity: `_compute_schema_hash` folds group attrs into the hash ONLY when present, so schemas declaring none hash identically to before (backward-compatible); divergent group attrs are caught as drift. Additive optional field per the 2026-06-18 `ZarrArraySpec` parity precedent (cold-migration only).
- NOT addressed here (plugin-side, by design): the CF *time coordinate* (CF004) is already plugin-declarable — a plugin declares a `time` `ZarrArraySpec` with CF attrs and `ensure_group` preserves them through `write_timestamp` resizes (DONE.md 2026-06-18 "Gap C dissolved into Gap B"); no core change needed. `grid_mapping` wiring (CF010 for grid-mapping container vars) remains a separate structural follow-up.

Evidence: `tests/unit/test_zarr_group_attrs.py` (set_group_attrs roundtrip + reserved-name rejection + empty no-op; `ZarrGroupSpec.attrs` non-Mapping rejection + None default; schema-hash unchanged-without-attrs / changed-with-attrs / deterministic; end-to-end `_setup_global_zarr_schema` writes group attrs to the store). Test fake `_Writer` in `tests/unit/test_indexed_region_schema_claim.py` gains `set_group_attrs`. Regression batch (region_writer, setup-global-schema, indexed-region, existing-cube-check, schema-claim-retry) green.

---

## 2026-06-22 — Single-shot `StorageFilesystem.read_bytes` for concurrent-safe metadata reads

Added `read_bytes(uri) -> bytes` to the `StorageFilesystem` Protocol and routed the existing-cube dim-compatibility check (`existing_cube_check._read_json`) through it. This is the **read-side complement** to the same-day `write_atomic` 412 fix: both isolate an s3fs/CloudFerro quirk at the fsspec adapter seam.

**The bug**: in parallel slot-range mode, every pod runs `verify_dim_compatibility` at startup (once *per batch*) and reads each group's `zarr.json` via `fs.open(...,"rb").read()`. On s3fs that takes the range-cached fetch, which adds an `If-Match: <etag>` precondition. While pod A is creating/overwriting a group's `zarr.json` (`_verify_schema_at_pod_startup → ensure_group`, under the `zarr_schema_global` claim), pod B's read of the same object gets HTTP **412 PreconditionFailed** (ETag changed mid-read). CloudFerro returns the 412 with a `None` message, and s3fs crashes in its own handler (`core.py`: `"pre-conditions" in ex.args[1]`, `args[1]` is `None` → `TypeError`). The pod dies at startup. Confirmed the upstream s3fs bug is **still present in s3fs 2026.6.0** (latest at time of fix), so an s3fs bump does NOT fix it.

**Locked design decisions**:
- The fix lives in the storage driver, not the ingestor/control plane. `read_bytes` is a driver-neutral Protocol method; both backends implement a single GET (fsspec `cat_file`, obstore `store.get`) with **no conditional/range-cached fetch**. Per DESIGN.md "One driver everywhere", driver-specific behavior stays at the fsspec adapter seam; the ingestor read path just calls `fs.read_bytes(uri)`.
- Rejected: retry/catch in `_read_json` (wrong layer; the crash is a `TypeError` raised *inside* s3fs — catching it would be exactly the over-broad except STYLE.md forbids). Rejected: reading dims from the control plane (ChunkManager stores `arrays`/`time_dim_name` but NOT `dimension_names`/`shape`, so the data store must be read). Rejected: claim-coordinated read (needs a consistent read, not a lock).
- Correctness rests on object-store PUT atomicity: a single GET returns a complete old-or-new object, never partial. For a dim-*name* check where every pod writes the same plugin-declared `time_dim_name`, old-vs-new is identical → no false positive, no retry needed.
- `read_bytes` is instrumented (`InstrumentedFilesystem`, bytes_read + latency) like `open`.

**s3fs/fsspec bump**: 2026.4.0 → 2026.6.0 (general freshness; does NOT fix this bug — the read_bytes change does).

**Not done** (deliberate scope): `core/zarr/validation.py::_load_array_metadata` still uses `open().read()` — it's the `firecube zarr validate` maintenance path, not the concurrent hot path; route it later for consistency. The redundant per-batch re-read of the same `zarr.json` (and gating the check in parallel mode where schema-verify already covers it) is a separate optional follow-up — left in TODO, not bundled, because it changes a safety check's timing.

Evidence: `tests/unit/test_storage_filesystem.py` (read_bytes roundtrip fsspec+obstore; single-shot-cat_file-not-open spy via isolated fake raw fs — never mutates the shared LocalFileSystem singleton); `tests/unit/test_existing_cube_check_read_bytes.py` (`_read_json` uses `read_bytes`, never `open`); `tests/unit/test_filesystem_instrumentation.py` (read_bytes records bytes_read); `tests/unit/test_storage_filesystem_protocol.py` (mock updated for the new Protocol member). Boundary unchanged: `tests/unit/test_no_raw_fsspec_usage.py` green (`cat_file` on `self._fs` is inside the allowlisted seam).

---

## 2026-06-22 — fsspec atomic writer normalizes S3 412 PreconditionFailed to FileExistsError

`FsspecAtomicWriter.write_atomic` (the `"xb"` exclusive-create primitive) now translates the s3fs lost-race signal to `FileExistsError`, honoring the `AtomicWriter` Protocol contract (`protocol.py`: "Raises FileExistsError if uri already exists") on S3 as it already did on local disk.

**The bug**: s3fs implements exclusive-create as a conditional `PutObject` (`If-None-Match: *`). When a concurrent writer wins the race, S3 returns HTTP 412 `PreconditionFailed`, which s3fs surfaces as `OSError(EINVAL)` (errno 22) with a botocore `ClientError` cause — NOT `OSError(EEXIST)` (errno 17). The writer only translated `EEXIST`, so on S3 the raw `OSError` escaped through `claims.acquire` (which catches only `FileExistsError` → `ClaimConflictError`) and out of `ChunkManager.ensure_slot_index_model`'s `ClaimConflictError` retry/convergence loop. Result: on a fresh S3 store, slot-range parallel pods that lost the schema-claim race crashed at startup (`_verify_schema_at_pod_startup`) instead of converging. Local-disk and obstore runs were unaffected (`ObstoreAtomicWriter` already maps `AlreadyExistsError` → `FileExistsError`).

**Locked design decisions**:
- The fix lives in the fsspec storage driver, NOT the control plane. Per DESIGN.md "One driver everywhere / No ad-hoc storage wiring", `claims.py`/`manager.py` must stay driver-agnostic; the `AtomicWriter` Protocol is the seam that hands them a driver-neutral `FileExistsError`. Teaching the control plane to catch botocore exceptions would have been the actual boundary violation.
- Detection is duck-typed on the botocore response dict (`Error.Code == "PreconditionFailed"` or `HTTPStatusCode == 412`) by walking the `__cause__`/`__context__` chain — no hard botocore import, no keying on the ambiguous `EINVAL` errno. Lives in `fsspec_backend.py`, which is the permanent-allowlisted fsspec adapter seam (`test_no_raw_fsspec_usage.py`).
- The control-plane claim/retry machinery is unchanged; it was already correct and simply never received the signal.
- Stale docstring corrected: s3fs now does native conditional writes (not the old `head_object`+`put`), satisfying the Protocol's "MUST use native primitives" clause; the only gap was exception translation.

Evidence: `tests/unit/test_fsspec_atomic_writer.py` (translation matrix: wrapped s3fs 412, bare ClientError 412, local EEXIST regression, native FileExistsError passthrough, non-conflict errors propagate, happy path); `tests/integration/test_slot_index_s3_precondition_retry.py` (end-to-end: injected one-shot s3fs 412 into the real writer → `ensure_slot_index_model` retries and converges, claim released). Boundary unchanged: `tests/unit/test_no_raw_fsspec_usage.py` green.

---

## 2026-06-21 — Slot-index model promoted to control-plane authority

Plugin-side ad-hoc root-attribute stamping and per-plugin epoch/model-protection mechanisms have been replaced by a single `ChunkManager` service backed by the `.firecube/slot_index/current.json` control-plane record. Both `firecube zarr preallocate` and `DirectZarrIngestor` startup now route through the same shared service, which persists a content-addressed model record under an exclusive claim and mirrors the identity hash as a Zarr root attribute.

**Shipped surfaces**:
- `firecube.core.api.SlotAxis`, `firecube.core.api.SlotIndexModel` (with `canonical_bytes()` and `identity_hash`)
- `firecube.core.api.iso_to_epoch_s`, `firecube.core.api.epoch_s_to_iso`, `firecube.core.api.normalize_epoch_iso`
- `ChunkManager.ensure_slot_index_model` and `ChunkManager.get_slot_index_model` (6-row precedence matrix, scoped retry, full CP+attrs convergence check)
- `DirectZarrIngestor.slot_index_model(ctx)` hook with `__init_subclass__` enforcement when `SUPPORTS_SLOT_RANGE_PARALLELISM = True`
- New error hierarchy: `SlotIndexModelError`, `SlotIndexModelConflictError`, `SlotIndexUnmanagedStoreError`, `SlotIndexModelClaimTimeoutError` (in `firecube.core.errors`)
- New control-plane storage layout: per-product `slot_index/current.json` (schema version `v1`)
- New reserved-root-attr guard module (`_reserved_root_attrs.py`) for user/plugin code paths; slot-index service bypasses it by design
- `firecube zarr preallocate` now accepts `--option` and `--input-data`, routed through the same service as `DirectZarrIngestor` startup

**Locked design decisions**:
- Three-axis discipline: claim names describe the resource (`"current"`), schema versions describe the on-disk record format (`"v1"`), model names describe the plugin algorithm (`"opera_v1"`)
- v1 supports exactly two rounding modes: `"exact"` and `"floor"` — no others
- Per-group axes only in v1; no `default_axis` syntactic sugar
- Hook lives on `DirectZarrIngestor` only; concrete default raises `NotImplementedError` (not `@abstractmethod`)
- Fresh-store precedence matrix is the sole adoption surface; the row "CP absent + attrs present" always raises `SlotIndexUnmanagedStoreError`
- Concurrency retry scoped inside `ensure_slot_index_model` only; `ClaimManager.acquire()` semantics unchanged globally
- Loser threads require full CP+attrs convergence before emitting `EVENT_SLOT_INDEX_MODEL_VERIFIED`; CP-only match is not a convergence signal

**Not done** (deliberate scope):
- No migration, adoption path, legacy-attr reader, or admin command for attrs-only stores — they fail loud
- No core inference of slot epochs or cadences from filenames, store contents, or group names; plugin is the sole source of truth
- No OPERA or MTG plugin-repository migration (those are separate plans in their own repositories)
- No new public docs page or migration guide; this entry is the sole prose artefact
- No second hook name or compatibility alias
- No rounding modes beyond `"exact"` and `"floor"` in v1
- No widening of `_reserved_attrs.py` (array-attrs guard); slot-model root attrs use a new module to avoid conflating the two protection surfaces

Evidence: `tests/sdk/test_public_api_surface.py` (public-API identity + KEPT set); `tests/integration/test_slot_index_precedence_matrix.py` (6-row matrix); `tests/integration/test_slot_index_concurrency.py` (same-model race, different-model race, high-contention, loser-cp-only regression); `tests/unit/test_chunk_manager_slot_index.py` (service unit tests).

---

## 2026-06-20 — Public API: promote three plugin-facing helpers

Three helpers required by `plans/DESIGN.md:45` (plugins use `firecube.ingestor.api` / `firecube.core.api` only) were previously deep-imports only. They are now re-exported through the public API; implementation locations are unchanged.

**Promoted helpers**:
- `decode_time_array` (from `firecube.core.zarr.time_decode`) → `firecube.core.api.decode_time_array`. Already had its own module-level `__all__` signalling export intent.
- `read_chunk_grid_with_shards` (from `firecube.core.zarr.validation`) → `firecube.core.api.read_chunk_grid_with_shards`. Returns `(dim_names, shape, outer_chunk_shape, inner_chunk_shape)` for sharded-Zarr-aware inspection.
- `verify_dim_compatibility` (from `firecube.ingestor.runtime.zarr.existing_cube_check`) → `firecube.ingestor.api.verify_dim_compatibility`. Pre-write time-dim consistency check.

**Not done** (deliberate scope):
- No plugin migrations. OPERA's `compat._read_existing_shards` and MTG's `RegionZarrWriter` deep import remain plugin-side and become separate plans in those repos.
- No compatibility shims, aliases, or `__getattr__` fallbacks. Pure re-exports only (verified by symbol-identity assertion in tests).
- No DESIGN.md broadening — only the now-stale internal path on line 86 was corrected to use the public name. DESIGN.md remains architectural guidance, not a changelog.

Evidence: `tests/sdk/test_public_api_surface.py` `_KEPT_CORE_API`, `_KEPT_INGESTOR_API`, and `_IDENTITY_PAIRS` lock all three names and their identity to the internal implementation.

---

## 2026-06-11 — Audit finding dispositions (A1-A7, S4-S8, C1-C9, T1-T7, P1-P2)

All open findings from the 2026-06-11 repository audit are closed. Resolved-same-day findings (S1, S2, S3, and the guideline-doc drift items) were already recorded in DONE.md on 2026-06-11. The entries below cover the remaining open findings.

**A1 — fsspec lint hole closed.** `open_fsspec_url` removed from `core/api.py` public surface; the raw-fsspec detector in `tests/unit/test_no_raw_fsspec_usage.py` extended to catch the unscored alias. Evidence: `tests/unit/test_no_raw_fsspec_usage.py`.

**A2 — Raw-fsspec fallback removed from `GenericParquetIngestor.write_parquet`.** The `storage_config is None` fallback that called fsspec directly is gone; the method now raises `ConfigurationError` instead. Evidence: `tests/unit/test_parquet_storage_invariant.py`.

**A3 — Basename inference deleted from `ZarrMultiresBuilder._store_uri_for`.** The URI basename heuristic is removed; `firecube zarr multires` requires `--product-name` explicitly. Evidence: `tests/unit/test_cli_zarr_multires.py`.

**A4 — Plugin contract test anchored to repo root.** `ALLOWED_PREFIXES` is now enforced and the vacuous-pass path is closed; `tests/sdk/test_plugin_contract.py` resolves paths from the repo root regardless of cwd. Evidence: `tests/sdk/test_plugin_contract.py`.

**A5 — `_storage_uri_from_target` centralized.** Five copy-pasted locality branches were centralized into a single helper in `core/uris.py`. Evidence: `tests/unit/test_uris.py`.

**A6 — Scheme inference deleted from `core/intake.py`.** The `storage_config` field is now required; scheme-inferred storage options are rejected. Evidence: `tests/unit/test_intake_explicit_storage.py`.

**A7 — `time_dim_name` threaded into tensogram converter.** The `("timestamp", "time")` preference-order guess is replaced by the plugin-declared `time_dim_name`. Evidence: `tests/unit/test_tensogram_time_dim.py`.

**S4 — Dead aliases removed.** `IngestionPipeline = PipelineExecutor` alias deleted from `runtime/engine.py`; `load_plugins` renamed to `discover_ingestors` in `registry/loader.py`. Evidence: `tests/unit/test_single_names.py`.

**S5 — `output_name` hard-rejected.** Removed from `SYSTEM_KEYS`; passing `output_name` in a config file now raises with a migration message pointing to `--product-name`. Evidence: `tests/unit/test_output_name_rejected.py`.

**S6 — Hardcoded domain defaults removed.** `group="FWI"` fallback deleted from `core/zarr/layers.py`; multires `(1.0, 0.5)` default single-sourced as `DEFAULT_MULTIRES_RESOLUTIONS` (no silent fallback on invalid input); `"fire_risk.duckdb"` default removed from `extensions/duck.py`. Evidence: `tests/unit/test_domain_defaults_removed.py`.

**S7 — `[database.duckdb]` scoped to DatabaseDuckDB tier.** Global merge into every plugin's defaults removed from `core/config.py`; the section is now only applied when the plugin declares a DatabaseDuckDB config tier. Evidence: `tests/unit/test_duckdb_tier_scoping.py`.

**S8 — `x_*` experimental namespace implemented.** Keys matching `x_*` are accepted at `--option` parse time and passed through to the engine without strict-unknown-key rejection. All other unknown keys continue to fail. Evidence: `tests/unit/test_experimental_options.py`.

**C1 — `ManifestRepository` split.** The ~1,300-line god class decomposed into `_wal_writer.py` (WAL write path), `_projection.py` (event projection), and `_legacy.py` (legacy migration); `repo.py` becomes a thin coordinator. Evidence: `tests/unit/test_manifest_repo_parity.py`.

**C2 — `ChunkManager` facade width accepted.** The 34-method facade mirrors the repo one-for-one by design: it is the single public surface for all control-plane operations and its width is a consequence of the domain, not a violation. Documented rationale in `plans/DESIGN.md` "Accepted Deviations" §C2. No code change.

**C3 — `_GenericBatchIngestor` dissolved.** The intermediate abstract class removed; `GenericZarrIngestor` and `GenericTensogramIngestor` re-parented directly onto `BaseIngestor`, reducing the inheritance depth to 3 levels. Evidence: `tests/unit/test_template_hierarchy.py`.

**C4 — `type()` synthesis deleted.** Runtime `type()` class synthesis for `--output-format tensogram` replaced by `DatasetProducer` protocol-checked strategy selection; direct `GenericTensogramIngestor` subclasses are now CLI-routable. Evidence: `tests/unit/test_tensogram_routing.py`. See also TODO §8 Phase 1.

**C5 — `SlotRangeCapable` protocol replaces `isinstance(DirectZarrIngestor)`.** Engine capability check at `runtime/engine.py` now uses the `@runtime_checkable` `SlotRangeCapable` protocol instead of a concrete class check. Evidence: `tests/unit/test_slot_range_capability.py`.

**C6 — `WriteModePolicy` replaces string comparisons.** Seven `write_mode == "staged"/"direct"` if-ladders across four layers replaced by a `WriteModePolicy` strategy object. Evidence: `tests/unit/test_write_mode_policy.py`.

**C7 — Storage completion extracted.** ~250 lines of storage completion logic moved from `PipelineExecutor` in `runtime/engine.py` to `core/storage/completion.py`. Evidence: `tests/unit/test_storage_completion.py`.

**C8 — `isinstance(self, DuckDbMixin)` replaces `getattr` probing.** The `# type: ignore` duck-type probe in `templates/generic.py` replaced by a proper `isinstance` check. Evidence: `tests/unit/test_duckdb_capability_check.py`.

**C9 — `install_option_groups_patch()` accepted.** The global Click monkey-patch at import time is a deliberate CLI formatting choice with no runtime side effects on ingestion. Documented rationale in `plans/DESIGN.md` "Accepted Deviations" §C9. No code change.

**T1 — `--strict-markers` active; s3/concurrency markers applied.** `--strict-markers` added to `addopts` in `pyproject.toml`; `s3` marker applied to all moto-backed integration suites; `concurrency` marker applied to concurrency tests. Evidence: `pyproject.toml`, `tests/integration/test_obstore_*.py`.

**T2 — Architecture/contract tests anchored to repo root.** `test_plugin_isolation.py` and `test_plugin_contract.py` now resolve paths from the repo root; vacuous-pass path closed. Evidence: `tests/architecture/test_plugin_isolation.py`, `tests/sdk/test_plugin_contract.py`.

**T3 — All 8 markers registered; `--strict-markers` in addopts.** `architecture`, `contract`, `s3`, `concurrency`, `plugin`, `slow`, `integration`, `unit` all registered in `pyproject.toml`; `--strict-markers` enforced. Evidence: `pyproject.toml`.

**T4 — Real-behavior tests for AppendStrategy and IndexedRegionStrategy.** Real-store behavior tests were added; `tests/unit/test_append_strategy.py` still patches the strategy's own internals. Evidence: `tests/integration/test_append_strategy_behavior.py`, `tests/integration/test_indexed_region_behavior.py`.

**T5 — External-binary skip accepted.** `pytest.skip("uv is not available on PATH")` in `tests/unit/test_version_consistency.py` is a sanctioned skip for a binary dependency; documented in `plans/TEST.md` "Sanctioned skip reasons". No code change.

**T6 — All 5 fixture plugins guarded in `pytest_sessionstart`.** `tests/conftest.py` now raises `UsageError` at session start if any of the 5 required fixture plugins is missing. Evidence: `tests/conftest.py`.

**T7 — 4 concurrency integration tests added.** Covers shared-product contention, WAL write ordering, workspace materialization races, and OTel context propagation under concurrency. Evidence: `tests/integration/test_concurrent_same_product.py`, `tests/integration/test_wal_concurrent_ordering.py`, `tests/integration/test_workspace_materialization_race.py`, `tests/integration/test_otel_context_concurrency.py`.

**P1 — 5 unused exports removed from public API.** `ScratchManager`, `CoverageTracker` removed from `ingestor/api.py`; `RegionZarrWriter`, `fs_kwargs_for_uri`, `open_fsspec_url` removed from `core/api.py`. `RuntimeIngestContext` is deliberately kept. Evidence: `tests/sdk/test_public_api_surface.py`.

**P2 — Internal `.output_path` reads migrated.** All internal reads of `result.output_path` migrated to `outputs.primary`; the compatibility property remains for external readers. Evidence: `tests/architecture/test_no_internal_output_path_reads.py`.

---

## §7-Phase 3.9 — Target/Storage-Type Coherence Validation Hardening (2026-06-01)

Cross-validates `--target` URI scheme against `--storage-type` during CLI parsing so incompatible configurations fail fast before ingestion starts.

**Commits**: C1 = dda65ab, C2 = 16151ef, C3 = c6b174f, C4 = 3c72561, C5 = 4789c37, C6 = 0306085

**Deliverables shipped**:
- `src/firecube/cli/_command_schemas.py`: cross-validation added in `IngestCommandConfig.__post_init__`; mismatched scheme/storage-type is now rejected at parse time
- Help text updated for `--storage-type` and `--target` flags in `firecube ingest`
- Coherence validation section added to plugin migration guidance

Final verification pending.

Full task breakdown captured in internal plan `storage-type-uri-cross-validation`.

## §7-Phase 3.8 — Parallel Ingestion External-Review Fixes #6 (2026-05-31)

Closes 2 verified external-review follow-up issues that surfaced after Phase 3.7 shipped.

**Commits**: C1 = 3c72561, C2 = 4789c37, C3 = <this commit>

**Deliverables shipped**:
- `firecube plan` blocked-range error message uses runnable `delete-span` form (LOW): the Phase 3.7 message at `plan.py:241-248` recommended `firecube chunks delete-span --run-id <id>` — but the CLI requires `-p, --product` (hard-fail) and the blocked-range scenario also requires `--force` (since the trigger IS non-alignment). The recommendation was non-runnable 100% of the time. Updated message now shows the canonical preview/commit pattern: `firecube chunks delete-span --product <product> --run-id <id> --force --dry-run` to preview, then swap `--dry-run` for `--yes-i-really-mean-it` to commit. Docs pointer uses mkdocs anchor (`#fail-closed-planning-behavior`) instead of bare file path. Regression test at `test_phase3_5_blocked_ranges.py::test_plan_error_message_format` strengthened with 4 new substring assertions locking the full runnable shape (`--product`, `--dry-run`, `--force`, `--yes-i-really-mean-it`) — previously only the verb `firecube chunks delete-span` was asserted, so the test passed even when `--product` was missing (commit C1)
- `docs/concepts/parallel-ingestion.md` `delete-span` remediation includes `--product`, `--force`, full safety flow (LOW): the Phase 3.7 fail-closed subsection at lines 266-274 and troubleshooting row at line 413 also showed the non-runnable `--run-id <id>` form. Fail-closed subsection expanded into a 4-step numbered procedure (list → preview → commit → re-ingest) with an explicit explanation of WHY `--force` is required (non-alignment IS the trigger condition). Troubleshooting row updated to be independently paste-runnable with the full command shape, plus a cross-link to the fail-closed subsection. Phase 3.7's `firecube chunks delete --range` clarification preserved (commit C2)

**Final verification approved across all four review phases.**

Full task breakdown captured in internal plan `zarr-phase3.8-followup-fixes`.

---

## §7-Phase 3.7 — Parallel Ingestion External-Review Fixes #5 (2026-05-30)

Closes 2 verified external-review follow-up issues that surfaced after Phase 3.6 shipped.

**Commits**: C1 = a3ee0fe, C2 = 2e464f6, C3 = <this commit>

**Deliverables shipped**:
- `firecube plan` blocked-range error message lists real remediation primitives (LOW): the Phase 3.5 fail-closed message at `plan.py:237-248` previously suggested "extend `global_expected_time_count()` to absorb the gap" as a blanket fix. This works for terminal-stub blocked ranges (e.g. `[950, 1000)` with `total=1000`) but is WRONG for prefix-misalignment cases (e.g. `[73, 100)` with `slot_size=100`) — extending the total cannot fix a misaligned prefix. Updated message now points operators at the actual span-level cleanup command (`firecube chunks delete-span --run-id <id>`) and the re-ingest path (`firecube ingest ... --option force_reingest=true`). `global_expected_time_count()` extension is preserved as a QUALIFIED option for terminal-stub cases only. Note: `firecube chunks delete --range` parses DATE strings, not slot indices — use `delete-span` for slot cleanup. Integration test `test_plan_error_message_format` strengthened with substring assertions for `firecube chunks delete-span` and `force_reingest=true` (commit C1)
- `docs/concepts/parallel-ingestion.md` refreshed for Phase 3.5 + 3.6 plan JSON contract (LOW): JSON example in the operator guide showed group objects WITHOUT `blocked_ranges` (Phase 3.5 added the field) and resume-semantics + troubleshooting sections did not mention Phase 3.5 blocked-partial-ranges fail-closed or Phase 3.6 coverage-lookup fail-closed behavior. Updated JSON example to show `blocked_ranges: []` on every group, added a `Fail-closed planning behavior` subsection documenting both modes with `--no-resume` and `firecube chunks delete-span` remediation cross-refs (plus an explicit note that `chunks delete --range` accepts dates only), and added 2 new troubleshooting table rows. Schema version unchanged; this is doc drift catch-up, not a schema change (commit C2)

**Final verification approved across all four review phases.**

Full task breakdown captured in internal plan `zarr-phase3.7-followup-fixes`.

---

## §7-Phase 3.6 — Parallel Ingestion External-Review Fixes #4 (2026-05-30)

Closes 1 verified external-review follow-up issue that surfaced after Phase 3.5 shipped.

**Commits**: C1 = 2615127, C2 = <this commit>

**Deliverables shipped**:
- `firecube plan` fail-closed on coverage lookup failure (MEDIUM): `_query_coverage` previously swallowed ALL exceptions and returned empty coverage, causing `firecube plan` to silently downgrade resume-aware planning to full planning on auth errors, corrupt control-plane state, driver bugs, or transient S3 errors. Orchestrators would then dispatch a full product worth of pods unnecessarily. First-run no-control-plane case is correctly handled WITHOUT exceptions (via `_load_current_state` returning empty dict), so the broad except was only masking real failures. Fix: removed `try: ... except Exception:` block from `_query_coverage`; caller now wraps the call with `click.ClickException` mapping that names the underlying failure and suggests `--no-resume` as the explicit bypass (commit C1)

**Final verification approved across all four review phases.**

Full task breakdown captured in internal plan `zarr-phase3.6-followup-fixes`.

---

## §7-Phase 3.5 — Parallel Ingestion External-Review Fixes #3 (2026-05-30)

Closes 2 verified external-review follow-up issues that surfaced after Phase 3.4 shipped.

**Commits**: C1 = 2992425, C2 = 8fda71a, C3 = <this commit>

**Deliverables shipped**:
- `firecube plan` fail-closed on blocked partial-chunk ranges (HIGH): `_chunk_aligned_remaining` previously dropped misaligned ranges silently (e.g. `[(950, 1000)] slot=100` → `[]`), so orchestrators ran nothing for the dropped slots. Function now returns `(aligned, blocked)` tuple. `firecube plan` raises `click.ClickException` BEFORE JSON emission when any group has non-empty `blocked_ranges`. Each group's JSON output gains a `blocked_ranges` diagnostic field. Misleading existing tests rewritten to assert the new fail-closed semantics (commit C1)
- `firecube plan` calls `zarr_schema()` exactly once (LOW): Phase 3.4 T1 added a phantom-validation call at `plan.py:156`, but `_resolve_per_group_slot_sizes` was still calling `zarr_schema()` separately. `_resolve_per_group_slot_sizes` signature changes to `(schema, explicit)` — accepts the already-loaded schema. Caller threads the line-156 schema through. Spy test asserts `call_count == 1` (commit C2)

**Final verification approved across all four review phases.**

Full task breakdown captured in internal plan `zarr-phase3.5-followup-fixes`.

---

## §7-Phase 3.4 — Parallel Ingestion External-Review Fixes #2 (2026-05-30)

Closes 3 verified external-review follow-up issues that surfaced after Phase 3.3 shipped.

**Commits**: C1 = 604a4d8, C2 = a9fc6e8, C3 = adf41dd, C4 = <this commit>

**Deliverables shipped**:
- Phantom `global_expected` group rejected at `firecube plan` and `firecube zarr setup-schema` preflights (HIGH): runtime ingest had the validator since Phase 3.3 T4, but CLI preflights silently emitted phantom ranges / reported success. Now both CLI commands call `validate_global_expected_subset_of_schema(global_expected, schema)` BEFORE any range output or zarr writes. `firecube plan` loads `zarr_schema(plugin_ctx)` unconditionally (preflight correctness, not slot-gated) (commit C1)
- `warn_if_misaligned` respects terminal partial chunks (MEDIUM): closes noisy false-positive warning where `validate_chunk_alignment` accepted `[900, 950)` with `global_expected=950` but the adjacent `warn_if_misaligned` call still logged "misaligned, suggested [900, 1000)". `warn_if_misaligned` now accepts optional `global_expected` parameter mirroring the validator's terminal-partial exception. `parallel_gate.py:129` passes `global_expected=global_schema` (commit C2)
- Sharding test no longer tautological (LOW): `test_write_dataset_skips_rechunk_when_chunks_already_match` previously compared the caller's Dask graph before/after `write_dataset_to_zarr()`, but the writer rebinds `ds = ds.chunk(...)` internally — Python rebinding doesn't mutate the caller's reference, so the assertion was always true. Now uses `monkeypatch.setattr(xr.Dataset, "chunk", spy)` with call-through to count actual chunk calls. Asserts `spy.call_count == 0` when shapes match. Test-only fix; production code unchanged (commit C3)

**Final verification approved across all four review phases.**

Full task breakdown captured in internal plan `zarr-phase3.4-followup-fixes`.

---

## §7-Phase 3.3 — Parallel Ingestion External-Review Fixes (2026-05-29)

Closes 6 verified external-review issues that surfaced after Phase 3.2 shipped.

**Commits**: C1 = 9fce649, C2 = 545689f, C3 = 40977ca, C4 = 8fc455d, C5 = c6fd238, C6 = 269acdf, C7 = <this commit>

**Deliverables shipped**:
- `run_sequential` applies same slot filter as `run_pipeline` (HIGH): closes silent default-CLI bug where `--slot-start`/`--slot-end` were ignored in sequential mode (default `pipeline_parallel=False`). Sequential slot-range is now first-class for debugging/recovery (commit C1)
- `validate_chunk_alignment` terminal partial-chunk exception + plan-to-ingest contract test (HIGH): closes mismatch where `firecube plan` emitted `(900, 950)` for length=950/chunk=100 products but the gate rejected it. Now allows ONLY `slot_end == global_expected[group]` as terminal partial; non-terminal misalignment still rejected. New contract test prevents future divergence (commit C2)
- URL-encoded `slot_group` in `run_id` + WAL reader full-subpath extraction (MEDIUM): closes path-safety bug where `slot_group` containing `/` (e.g. `multires/0.5deg` per `scripts/fire_risk_ingest.py`) corrupted WAL paths. `derive_pod_run_id` now URL-encodes via `urllib.parse.quote(safe="")`; WAL reader uses proper subpath extraction; EngineConfig warn-only validation guides operators (commit C3)
- Phantom group prevention in capability gate + pod-startup verification (MEDIUM): closes false-success audit-record bug where `_verify_schema_at_pod_startup` silently wrote success for groups in `global_expected` but absent from `zarr_schema()`. New additive `validate_global_expected_subset_of_schema` in `parallel_gate.py` catches at gate (cheap fast-fail); hard-fail in `_verify_schema_at_pod_startup` is defense-in-depth. Obsolete `extras_in_global` DEBUG log removed (commit C4)
- Operator docs refresh (MEDIUM): `docs/concepts/parallel-ingestion.md` plan-output example now shows per-group `slot_size` and per-range `--slot-group` (Phase 3.1/3.2 features); run_id example shows both single-group and multi-group formats with opacity warning. `docs/concepts/best-practices.md` no longer says "until the Phase 3 engine planner lands" (it landed 2026-05-28) (commit C5)
- Sharding test assertions strengthened (LOW): `test_append_to_sharded_store_preserves_shards` now asserts shard/chunk metadata (not just timestamp count); `test_write_dataset_skips_rechunk_when_chunks_already_match` now compares `_tasks_after == _tasks_before` (the actual Flaw 10 invariant) (commit C6)

**Final verification approved across all four review phases.**

Full task breakdown captured in internal plan `zarr-phase3.3-review-fixes`.

---

## §7-Phase 3.2 — Parallel Ingestion Correctness (2026-05-29)

Closes 6 verified review issues from the Phase 3.1 implementation.

**Commits**: C1 = 37f7521, C2 = 80583a2, C3 = 84bb1bb, C4 = 6b6f3a1, C5 = 450f8f7, C6 = 416a4b6, C7 = <this commit>

**Deliverables shipped**:
- `derive_pod_run_id` includes slot_group for multi-group pod isolation (HIGH): closes run_id collision when two pods share base + slot_range but differ in slot_group (commit C1)
- Strict schema validation via `SchemaDriftError` + `RegionZarrWriter.verify_array_spec` (HIGH): catches dtype/chunks/shape[1:]/rank/fill_value mismatch in setup-schema CLI AND pod-startup verification; NaN-safe fill_value comparison; `spec.fill_value=None` treated as "unspecified — skip check" (preserves idempotent setup); preserves larger-shape[0] tolerance (commit C2)
- Per-group `slot_size` in `firecube plan` JSON (MEDIUM-HIGH): LCM of all chunked arrays per group; chunk-aligned remaining intervals after resume; user `--slot-size N` validated against every group (commit C3)
- `refactor(zarr)`: `_ParallelExecutionState` dataclass introduced; `ctx._ctx._parallel_global_schema` escape hatch removed (MEDIUM): unblocks §13 PluginContext shrinkage; engine-internal state lives on `BaseIngestor.self._parallel_execution_state` (instance attribute, NOT a parameter); `_process_batch` signature and `engine.py` UNCHANGED (commit C4)
- Pod-startup schema verification + ChunkManager audit record (MEDIUM): `_setup_global_zarr_schema` moved out of `_process_batch` hot path; in-memory `schema_verified[group]` flag prevents re-verify within pod; NEW additive method `ChunkManager.record_schema_verification` writes per-pod audit records (each carrying its own `verified_at` — NOT content-idempotent; WAL `events-*.jsonl` per-writer files make this S3-safe); audit observational only — NEVER consulted for skip (commit C5)
- Docs cleanup (LOW): `cli/zarr.py` module docstring no longer claims standalone "read-only"; migration guide accurately describes WriteIntent-only group coverage and capability-gate error class (commit C6)

**Final verification approved across all four review phases.**

Full task breakdown captured in internal plan `zarr-phase3.2-correctness`.

---

## §7-Phase 3.1 — Parallel Ingestion Hardening (2026-05-28)

Closes 5 verified safety gaps in Phase 3 parallel ingestion surfaced by external review.

**Commits**: C1 = 94d167e, C2 = 4a5212e, C3 = ba90260, C4 = f9401f5

**Deliverables shipped**:
- Strict `global_expected_time_count()` coverage enforcement (HIGH): groups receiving WriteIntents MUST be declared in global_expected; previously only warned (commit C1)
- Intent-group-in-schema hard fail (defense in depth): WriteIntent for unknown group → ConfigurationError before writes (commit C1)
- All-arrays chunk validation (MEDIUM-LOW): validate every chunked array per group, not just first; addresses FCI-style heterogeneous chunks within groups (commit C1)
- `--slot-group <name>` CLI flag + `EngineConfig.slot_group` + `FIRECUBE_SLOT_GROUP` env var (MEDIUM): backward-compatible per-group slot range targeting for multi-group plugins (commit C1)
- Multi-group capable test fixture (`multi_group_capable_test_plugin`): 2 writable groups, heterogeneous chunks across + within groups (commit C1)
- Group-aware ResumeGuard for non-terminal runs (refactor): disjoint groups on same slot range no longer conflict (commit C2)
- NEW slot-range-aware completed-span check (HIGH): staggered serial pods on disjoint slot ranges no longer need `resume_existing=true` (commit C2)
- Split bypass semantics: `resume_existing` → non-terminal only; `force_reingest` → completed only (commit C2)
- `_ranges_overlap_inclusive_vs_halfopen` helper extracted + tested at boundaries (commit C2)
- `--slot-group` propagation through capability gate (unknown-group hard fail), post-intent assertion (narrowed validation), and `firecube plan` JSON output (per-range --slot-group emission) (commit C3)
- Migration guide docs cleanup: removed misleading "discards items" wording, added "Required vs Optional" table, updated docstring (commit C4)
- Phase 3 single-group regression integration test (commit C4)

**Final verification approved across all four review phases.**

Full task breakdown captured in internal plan `zarr-phase3.1-hardening`.

---

## §7-sub / Phase 3 — Safe within-group parallel ingestion (2026-05-28)

Closes the final Cat-A item in §7: engine/template-level planner pre-computing
deterministic time-to-index mappings and chunk-aligned ranges.

**Commits**: C1 = 66ce6e0, C2 = be8e248, C3 = 752f7a1, C4 = 7c7f820

**Deliverables shipped**:
- `SUPPORTS_SLOT_RANGE_PARALLELISM` ClassVar + 3 optional hooks on `DirectZarrIngestor`
- `validate_parallel_capability()` fail-fast gate in `BaseIngestor.run()` (before ResumeGuard)
- Chunk-alignment hard failure + post-intent assertion (`WriteIntentRangeError`)
- Global schema setup via `zarr_schema_global` claims (`SchemaSizeMismatchError`)
- `ResumeGuard` 6-row conflict matrix (`RangeOverlapError`)
- Per-pod deterministic `run_id` derivation (`__slot=N-M` suffix)
- `firecube plan <product>` stable JSON output for Argo/Kubeflow orchestrators
- `firecube zarr setup-schema` operator preflight command
- `--slot-start/--slot-end/--slot-size` CLI flags + K8s env discovery
- Phase 2 `_sweep_legacy_zarr_group_claims` compat code removed
- Operator guide: `docs/concepts/parallel-ingestion.md`

---

Date: 2026-05-27 Task: §7-GENERIC

Decision: `GenericZarrIngestor` claim category renamed to `zarr_append` — commit `79682b4` — Files: `src/firecube/ingestor/templates/generic.py` (1-line closure change at ~line 296), `tests/unit/test_generic_zarr_claim_category.py` (new, 3 tests), `tests/integration/test_maintenance_claims.py` and `tests/integration/test_obstore_claim_atomicity.py` (fixture updates). AppendStrategy remains serial (process-local lock); naming now reflects append-mode coordination.

Date: 2026-05-26 Task: §11

Decision: `firecube zarr multires` CLI subcommand implemented — commit 5b4c0c5 — Files: `src/firecube/cli/zarr.py` (added multires subcommand wrapping `ZarrMultiresBuilder`), `tests/unit/test_cli_zarr_multires.py` (new). Verification: `uv run firecube zarr multires --help`. Prerequisite for §25 deletion.

---

This file records accepted decisions and completed work.
New decisions are appended with a date. Read this when context for an existing decision is needed.

---

Date: 2026-05-26 Task: §27

Decision: `build_dataset` signature updated in tutorials and references

Context: Runtime uses `build_dataset(self, group: str, items: list[Any], ctx: PluginContext)` at `src/firecube/ingestor/templates/generic.py:187`. Six doc files still showed the old `batch: PipelineBatch` form, misleading new plugin authors. Updated all six files to the new `items: list[Any]` signature. Added one deprecation-shim note in `docs/guides/subclassing_generic.md` explaining that the legacy signature still works via the shim.

Files changed:
- `docs/tutorials/weather-csv.md` (signature + import updated)
- `docs/tutorials/sentinel3-frp.md` (signature + import updated)
- `docs/tutorials/observability.md` (signature + import updated)
- `docs/guides/subclassing_generic.md` (signature at lines 20, 40, 104 + deprecation paragraph added)
- `docs/concepts/best-practices.md` (signature at lines 14, 21 updated)
- `docs/reference/plugins/msg_frm.md` (Mermaid diagram updated)

Verification: `grep -rn 'build_dataset.*PipelineBatch' docs/` returns exactly 1 match (the deprecation paragraph itself). MkDocs `--strict` build passes.

Evidence: task-5 workspace notes (no-stale grep, new-signature grep, deprecation-paragraph grep, MkDocs `--strict` build, commit hash).

- Source: TODO.md §27

---

Date: 2026-05-26

Decision: Scaffolding templates now generate `PRODUCT_NAME` for all plugin variants

Context: `firecube plugins scaffold` emitted classes that failed `BaseIngestor.__init_subclass__()` at import time because the generated plugin classes omitted the required `PRODUCT_NAME` class attribute. All four templates now emit `PRODUCT_NAME: ClassVar[str] = "{plugin_name}"` beside the existing `name` field.

Consequences:
- Generated base, zarr, parquet, and direct-zarr plugins now satisfy the product-name contract at import time.
- Each emitted template imports `ClassVar` explicitly for the class-level annotation.
- A targeted regression test covers all four scaffold templates.

Verified: `uv run pytest tests/unit/test_scaffolding_product_name.py -q`

Evidence:
- Commit: `76b8b2c`
- Verified by: `src/firecube/ingestor/devtools/scaffolding.py`, `tests/unit/test_scaffolding_product_name.py`
- Source: TODO.md §26

---

Date: 2026-04-12

Decision: Concurrent WAL segment reads in `_load_current_state` via `ThreadPoolExecutor`

Context: `_load_current_state` read run directories and event segments sequentially. For a product with many completed runs past the snapshot cutoff, this was O(N) sequential filesystem opens. On S3, each open is a network round trip, making the read path linearly slower as WAL depth grows. The fix parallelises the I/O phase while preserving deterministic projection ordering.

Consequences:
- `ThreadPoolExecutor(max_workers=8)` at `src/firecube/core/controlplane/_snapshot.py:185` performs parallel WAL segment reads.
- Sequential `apply_events` call after collection preserves upsert-by-key ordering — projection correctness is not affected.
- Single-run fast path bypasses the pool entirely to avoid overhead on the common case.
- `ControlPlaneCorruptionError` from any concurrent reader propagates and aborts the projection rather than being swallowed.
- Note: TODO.md §17 described this as a "candidate direction, not approved design". The implementation has shipped at `src/firecube/core/controlplane/_snapshot.py:185`.

Verified: `grep -n "ThreadPoolExecutor" src/firecube/core/controlplane/_snapshot.py` returns a match at line 185.

Evidence:
- Commit: `c899782` (Confidence: MEDIUM — this is the extraction commit that moved `_snapshot.py` out of `repo.py`; the parallel-WAL implementation itself may predate the file split, but the location is verifiable here)
- Verified by: `src/firecube/core/controlplane/_snapshot.py:185`
- Source: TODO.md §17

---

Date: 2026-04-27

Decision: Time-window maintenance with logical deletion, physical scrub, and targeted window selection

Context: Firecube needed a way to delete and reingest specific time windows without ad-hoc S3 operations. The solution introduces a `firecube_timestamp_state` array for logical deletion tracking, a `scrub.py` module for physical cleanup via `ChunkManager`, and CLI commands for safe, auditable maintenance operations.

Consequences:
- `firecube_timestamp_state` array added with four state values: `{0: unknown, 1: present, 2: deleted_by_firecube, 3: failed_batch}`.
- `attach_timestamp_state_dataset` helper threads state names through write strategies.
- `src/firecube/core/zarr/scrub.py` (186 lines) uses `ChunkManager.create_deletion_plan()` and `execute_deletion()` — no ad-hoc S3 deletes anywhere.
- CLI commands `chunks delete` and `chunks delete-span` added with safety flags.
- `time_overlaps` parameter enables targeted window selection, linking maintenance to the §2 query primitives.

Verified: `grep -rn "firecube_timestamp_state" src/firecube/` returns matches in write strategy and scrub modules.

Evidence:
- Commit: `d8619f3` (Confidence: MEDIUM)
- Verified by: `src/firecube/core/zarr/scrub.py`
- Source: TODO.md §8

---

Date: 2026-04-14

Decision: Zarr write-strategy abstraction with `ZarrWriteStrategy` Protocol, two implementations, and extracted runtime services

Context: The Zarr write path was monolithic, making it hard to add new write strategies or reuse components across templates. The redesign introduces a `ZarrWriteStrategy` Protocol as a stable seam, two concrete implementations for the two write patterns, and extracted `ScratchManager`/`CoverageTracker` services available to all plugin templates.

Consequences:
- `ZarrWriteStrategy` Protocol at `src/firecube/ingestor/runtime/zarr/contracts.py:20` (`@runtime_checkable`).
- `AppendStrategy` at `strategies/append.py:45` wraps `append_time_groups` for xarray-append plugins.
- `IndexedRegionStrategy` at `strategies/indexed_region.py:53` handles direct zarr-python region writes via `RegionZarrWriter`.
- `DirectZarrIngestor` template at `src/firecube/ingestor/templates/direct_zarr.py:96` for region-write plugins.
- `GenericZarrIngestor` refactored to delegate the core append step to `AppendStrategy.write_groups()` at `templates/generic.py:342`; remaining inline ownership (staged metadata seeding, lock construction, strategy construction, claim construction, metrics assembly) tracked as a follow-up in TODO §22.
- `ScratchManager` and `CoverageTracker` extracted and re-exported from `firecube.ingestor.api`.
- `append_time_groups` decomposed from a ~400-line monolith into a 173-line orchestrator at `runtime/zarr/append.py` plus five focused services in `runtime/zarr/append_services.py` (622 lines): `AppendTimestampState` (state array init/updates), `AppendResumeService` (resume cache, cursor inference, overlap detection), `AppendWriteExecutor` (write loop, alignment checks), `AppendCoverageBuilder` (coverage entries, time-range tracking), `AppendMultiresHandler` (multires layer building post-write).
- Public contract types for direct-write plugins: `WriteIntent`, `ZarrArraySpec`, `ZarrGroupSpec` defined in `templates/direct_zarr.py:45-94` and re-exported via `firecube.ingestor.api`. `WriteIntent.kind` ("region" / "1d" / "timestamp") selects the write method; `ts_index: int` is the canonical row key.

Caveats (known gaps surfaced after this entry landed; tracked as TODO follow-ups):
- **Protocol non-conformance**: `ZarrWriteStrategy.write_groups()` declares `(*, group_to_timestamps, dataset_for_batch, batch_size, claim_for_group)` at `runtime/zarr/contracts.py:32-39`. `AppendStrategy.write_groups()` matches. `IndexedRegionStrategy.write_groups()` at `strategies/indexed_region.py:75-81` requires a different parameter set (`group_to_intents`, `schema`). The Protocol is therefore not a real shared interface — it works only for `AppendStrategy`. Tracked as TODO §21.
- **Facade incomplete**: `GenericZarrIngestor._process_batch` is 144 lines and still owns five orchestration responsibilities inline (see TODO §22).
- **Claim granularity is per-group**: `DirectZarrIngestor` builds `WriteDomain(category="zarr_group", name=group_name)` at `templates/direct_zarr.py:179-182`. Per-region/per-slot granularity (needed for safe intra-group parallelism) is tracked as part of TODO §7.

**Status as of 2026-05-27 (Phase 2 completion — supersedes caveats above):**
- Protocol non-conformance addressed by §21 — `ZarrWriteStrategy` was split into `AppendWriteStrategy` + `RegionWriteStrategy`, both `@runtime_checkable` (commit `e16f6b4`).
- Facade complete — §22 DONE (helpers in commit `8ab0fbf`, wiring in commit `e317c2e`). `_process_batch` is now ~66 lines (down from 143), delegating to `runtime/zarr/batch_runner.py` helpers.
- Claim granularity now per-slot for `DirectZarrIngestor` — §7-DIRECT DONE in commit `701ed07`. Schema claims use `name=f"{group}:schema"`, slot claims use `name=f"{group}:slot={ts_index}"`. `GenericZarrIngestor` uses `category="zarr_append"` (§7-GENERIC commit `f332133`). Only the Phase 3 planner/orchestrator (§7-sub) remains open.
- `AppendMultiresHandler` removed in §25 (commit `89b2737`) — ingest-time multires is no longer supported; use `firecube zarr multires` post-step CLI.

Verified: `grep -n "class ZarrWriteStrategy" src/firecube/ingestor/runtime/zarr/contracts.py` returns a match at line 20.

Evidence:
- Commit: `7e7977c` (Confidence: HIGH)
- Verified by: `src/firecube/ingestor/runtime/zarr/contracts.py:20`, `tests/unit/test_coverage_tracker.py`
- Source: TODO.md §4a

---

Date: 2026-04-14

Decision: Storage driver abstraction with `StorageFilesystem` Protocol, `FsspecFilesystem`/`ObstoreFilesystem` implementations, and driver-aware Zarr store construction

Context: Firecube needed to support both fsspec and obstore as storage backends without mixing drivers within a single run. The abstraction introduces a Protocol-based filesystem interface, two concrete implementations, and a factory function for driver-aware Zarr store construction. The `--storage-driver` CLI flag makes the choice explicit.

Consequences:
- `StorageFilesystem` Protocol at `src/firecube/core/filesystem/protocol.py` with `FsspecFilesystem` and `ObstoreFilesystem` implementations.
- `create_zarr_store()` factory function and `ZarrStoreHandle` frozen dataclass in `src/firecube/core/filesystem/store_factory.py` for driver-aware Zarr store construction.
- `--storage-driver [fsspec|obstore]` required CLI flag selects the backend; no mixing within a run.
- `_obstore_compat.py` lazy import guard keeps obstore optional (`uv pip install 'firecube[obstore]'`).
- `tests/integration/test_one_driver_invariant.py` (279 lines) enforces the one-driver-everywhere invariant.
- Note: `ZarrStoreFactory` is not a class. It is `create_zarr_store()` function plus `ZarrStoreHandle` dataclass in `store_factory.py`. The module docstring reads "ZarrStoreFactory" but the implementation is function-based.

Verified: `grep -n "class StorageFilesystem" src/firecube/core/filesystem/protocol.py` returns a match.

Evidence:
- Commit: `1cf537f` (Confidence: HIGH)
- Verified by: `src/firecube/core/filesystem/store_factory.py`, `tests/integration/test_one_driver_invariant.py`
- Source: TODO.md §4b

---

Date: 2026-04-14

Decision: CLI lifecycle migration — engine owns post-execution staging and upload; CLI is purely an entry point

Context: The CLI previously owned staged-output completion, write-mode-specific output resolution, and the upload step after the ingestor returned. That ownership split made the runtime hard to reuse outside the CLI and meant "ingestion complete" was not fully self-contained. The migration moves all post-execution lifecycle into the engine; the CLI now only parses arguments, passes `--write-mode` to the engine, and echoes results.

Consequences:
- Engine owns `complete_output()` at `src/firecube/ingestor/runtime/engine.py:570-616`. It resolves the final output URI, handles staged-vs-direct write modes, and constructs the final manifest.
- Engine owns `_complete_s3_staged()` at `src/firecube/ingestor/runtime/engine.py:717-787`. It performs the actual staged upload to S3.
- `BaseIngestor.run()` calls `engine.complete_output()` before returning to the CLI.
- CLI at `src/firecube/cli/main.py:500-511` only echoes the result manifest and output path.
- `--write-mode [staged|direct]` is a required CLI flag (no inference); passed to the engine via `IngestContext.options["write_mode"]`.

Verified: `grep -n "def complete_output\|def _complete_s3_staged" src/firecube/ingestor/runtime/engine.py` returns matches at lines 570 and 717.

Evidence:
- Commit: `2f23373` (Confidence: HIGH)
- Verified by: `src/firecube/ingestor/runtime/engine.py:570-616`, `src/firecube/cli/main.py:500-511`
- Source: architectural cleanup of the CLI/engine boundary

---

Date: 2026-04-11

Decision: Engine-owned `metrics["storage"]` synthesis — control-plane reporting is core-owned, not plugin-synthesized

Context: Plugins were previously responsible for constructing `metrics["storage"]` themselves, leading to inconsistent or missing control-plane metadata in run summaries. Moving this responsibility to the engine ensures every run gets consistent storage metrics regardless of which plugin ran.

Consequences:
- Engine synthesizes `metrics["storage"]` at `src/firecube/ingestor/runtime/engine.py:535-560`, validating and replacing any non-mapping plugin values.
- Engine injects `control_root` and `latest_pointer` via `describe_control_plane` from `ChunkManager`.
- Scaffolding templates use `merge_batch_metrics()` — no manual storage construction needed in plugins.
- `DirectZarrIngestor` scaffolding added to `scaffolding.py`.
- Plugin docs updated at `docs/concepts/plugins.md:138,158` and `docs/concepts/observability.md:108,146-164`.
- Test `test_finalize_injects_control_plane_storage_metrics` asserts engine ownership.
- Note: TODO.md cites `engine.py:502-527`; actual location is `engine.py:535-560` due to a ~33-line shift from later edits. Same code, just shifted.
- Follow-up: `firecube-msg-frm/src/firecube_msg_frm/metrics.py:152-172` still synthesizes `control_root` and `latest_pointer` locally, labelled "Backward-compatible alias until core owns control-plane reporting" at line 169. Core now owns these fields; the alias can be removed in a future plugin cleanup pass.

Verified: `grep -n "describe_control_plane\|merge_batch_metrics" src/firecube/ingestor/runtime/engine.py` returns matches.

Evidence:
- Commit: `b021ed5` (Confidence: HIGH)
- Verified by: `src/firecube/ingestor/runtime/engine.py:535-560`, `tests/unit/test_engine_resilience.py`
- Source: TODO.md §16

---

Date: 2026-04-10

Decision: Meta-aware `ChunkManager` queries with time-range filtering, status filtering, and `time_coverage_summary`

Context: `ChunkManager` previously required callers to fetch the full chunk list and filter in application code. Adding time-range and status parameters to the query methods moves filtering into the control-plane layer, reducing data transfer and making maintenance and resume logic simpler to express.

Consequences:
- `list_chunks()` accepts `time_min_after`, `time_max_before`, and `time_overlaps` parameters.
- `list_runs()` accepts `status` and `non_terminal` filtering parameters.
- `time_coverage_summary()` returns per-group time bounds and span counts for diagnostics and resume overlap detection.
- `timestamps_written` counter available at both `SpanRecord` and `ChunkInfo` levels.
- CLI `--time-range` and `--status` flags exposed in `firecube chunks` subcommands.

Verified: `grep -n "time_coverage_summary\|time_overlaps" src/firecube/core/controlplane/manager.py` returns matches.

Evidence:
- Commit: `fe7870d` (Confidence: HIGH)
- Verified by: `src/firecube/core/controlplane/manager.py`, `src/firecube/core/controlplane/repo.py`
- Source: TODO.md §2

---

Date: 2026-04-07

Decision: `firecube advise batch-size` CLI command for chunk-aligned batch size recommendations

Context: Choosing a `pipeline_batch_size` that avoids partial Zarr chunks is non-obvious. A misaligned batch size causes extra write overhead and can degrade read performance. The `advise` command reads the existing Zarr metadata and emits a recommendation aligned to the time chunk size.

Consequences:
- `src/firecube/cli/advise.py` (117 lines) implements the command.
- `firecube advise batch-size` registered as a Click subcommand, visible in `--help`.
- Opens the Zarr group via `session.zarr.open_group()` and inspects `arr.chunks[0]` of the first multi-dimensional array via `_find_time_chunk_size()`.
- Emits an aligned `pipeline_batch_size` recommendation with partial-chunk-avoidance rationale.
- Special cases: `time_chunk_size==1` and no-time-dim arrays handled without crashing.

Verified: `grep -n "advise batch-size\|def batch_size" src/firecube/cli/advise.py` returns matches.

Evidence:
- Commit: `66f6bc6` (Confidence: HIGH)
- Verified by: `src/firecube/cli/advise.py`
- Source: TODO.md §10

---

Date: 2026-04-07

Decision: NetCDF-to-Zarr shared utilities in `firecube.core.formats.netcdf` (3 of 4 utilities)

Context: Every NetCDF-based plugin independently handled the same set of NetCDF-to-Zarr V3 incompatibilities: HDF5 encoding cleanup, time dimension renaming, and preparation composition. Centralising these in core eliminates duplication and gives plugins a documented, tested preparation path.

Consequences:
- `clean_netcdf_encoding(ds)` strips `chunks`, `chunksizes`, and `preferred_chunks` from variable encodings to prevent xarray validation errors.
- `rename_time_dim(ds, target="timestamp")` renames the `time` dimension to `timestamp` idempotently.
- `prepare_netcdf_for_zarr(ds)` is a convenience wrapper composing the two primitives in the correct order.
- All three utilities present in `src/firecube/core/formats/netcdf.py` (56 lines total).
- Note: `normalize_string_vars` (the fourth utility for fixed-length Unicode conversion) is absent from the codebase. This entry covers only the three shipped utilities; the remainder is tracked in `plans/TODO.md`.

Verified: `grep -n "def clean_netcdf_encoding\|def rename_time_dim\|def prepare_netcdf_for_zarr" src/firecube/core/formats/netcdf.py` returns three matches.

Evidence:
- Commit: `ef771cc` (Confidence: HIGH)
- Verified by: `src/firecube/core/formats/netcdf.py`
- Source: TODO.md §19

---

Date: 2026-04-06

Decision: Logging consistency hardening in the control-plane layer — f-strings removed, silent suppression replaced

Context: The control-plane layer had several silent failure modes: f-string log calls (deferred formatting even when the log level is disabled), `suppress(Exception)` blocks with no log output, and silent early returns in maintenance commands. These made operational debugging harder than necessary.

Consequences:
- Zero f-string log calls remain in `controlplane/` — all use `%`-style parameterized formatting.
- `events.py:62-69` `makedirs` non-fatal try/except now logs `debug` at each site (commit `9cd1ca9`).
- `discover_manifests` and `_list_run_entries` log `WARNING` with path and exception context before returning empty lists (commit `9fd5309`).
- `rebuild_snapshot` logs `DEBUG` when skipping the local lock in remote mode (commit `29174e9`).
- Timing logs added in `_load_current_state` and `rebuild_snapshot` (commit `29174e9`).
- `abandon_run` logs `DEBUG` when the run is already terminal (commit `29174e9`).

Verified: `grep -rn "f\"" src/firecube/core/controlplane/ | grep "\.log\." | wc -l` returns 0.

Evidence:
- Commits: `9cd1ca9`, `9fd5309`, `29174e9` (Confidence: HIGH)
- Verified by: `src/firecube/core/controlplane/repo.py`, `src/firecube/core/controlplane/events.py`
- Source: TODO.md §18.1

---

Date: 2026-04-06

Decision: Error handling hardening in `ManifestRepository` — assert guards replaced with explicit `ManifestError` raises

Context: `repo.py` used bare `assert self._resolver/self._fs is not None` as internal state guards. These produce `AssertionError` with no context when they fire, and are silently disabled under `python -O`. Replacing them with explicit `ManifestError` raises gives operators a clear error message and a consistent exception type.

Consequences:
- 14+ call sites in `repo.py` now use `if X is None: raise ManifestError("Repository not bound — call bind() first")`.
- `ManifestRepository.close()` logs `WARNING` before suppressing flush failures.
- All `ControlPlaneCorruptionError` raises standardized with product/run context; `self.log.error(msg)` precedes every raise.
- `self.log.error(...)` precedes every `raise ControlPlaneCorruptionError(...)` for torn-tail detection.
- Note: TODO.md claimed "All 26 `assert self._resolver/self._fs is not None` replaced". 5 type-narrowing asserts remain at `repo.py:1069, 1077, 1083, 1240, 1245`. Each is preceded by `self._ensure_bound()` which raises `ManifestError` on bind failure, so the asserts are type-narrowing hints for static checkers and never fire at runtime. The literal claim is overstated; the runtime behavior is correct.

Verified: `grep -rn "ManifestError" src/firecube/core/controlplane/ | grep -v __pycache__ | wc -l` returns 24+.

Evidence:
- Commits: `9fd5309`, `29174e9` (Confidence: HIGH)
- Verified by: `src/firecube/core/controlplane/repo.py:1326-1338`
- Source: TODO.md §18.2

---

Date: 2026-04-06

Decision: WAL observability — corruption metrics, snapshot age warnings, and `firecube chunks snapshots status` CLI

Context: The control-plane layer had no metrics for WAL operations. Corruption events, torn-tail recoveries, and snapshot age were invisible to operators. The fix adds a lightweight `contextvars`-based metrics collector, instruments the key detection sites, and surfaces snapshot age via a new CLI command.

Consequences:
- `src/firecube/core/controlplane/metrics.py` (77 lines) with `WalMetrics` dataclass and `contextvars` collector.
- `record_wal_corruption()` fires at 10 detection sites in `repo.py` and `_wal_reader.py`.
- `firecube_control_plane_corruption_total` metric in `RUN_SUMMARY_SCHEMA`, emitted to Pushgateway per run.
- Pipeline integrates via `collect_wal_metrics()` context manager.
- `firecube chunks snapshots status -p <product>` CLI command prints snapshot age, generation, and record count.
- WARNING log emitted in `_load_current_state` when snapshot age exceeds 24 hours (`_snapshot.py:135-146`).
- Snapshot rebuild duration metric at `repo.py:805`.

Verified: `grep -rn "record_wal_corruption\|WalMetrics" src/firecube/core/controlplane/` returns matches.

Evidence:
- Commits: `fb5d9ff`, `3915150`, `29174e9` (Confidence: HIGH)
- Verified by: `src/firecube/core/controlplane/metrics.py`, `src/firecube/core/controlplane/repo.py`
- Source: TODO.md §18.3

---

Date: 2026-04-06

Decision: OTel `TracerProvider` lifecycle management — explicit shutdown in CLI `finally` block

Context: OpenTelemetry `BatchSpanProcessor` background threads can keep the process alive after ingestion completes if the tracer provider is not explicitly shut down. In a Kubeflow/Argo pod, this causes the step to appear still running after the ingest finishes, preventing the orchestrator from recording the correct outcome.

Consequences:
- `shutdown_tracing()` at `src/firecube/core/observability/tracing.py:43` calls `provider.force_flush(timeout_millis//2)` then `provider.shutdown()` with a 5-second default timeout; both calls are exception-guarded.
- `shutdown_observability()` facade at `core/observability/__init__.py:27` wraps `shutdown_tracing()` and resets the init guard.
- CLI ingest at `cli/main.py:378 try ... :512 finally` invokes `shutdown_observability(timeout_millis=5000)`.
- The `finally` block runs on both success and failure paths — no `except`-and-swallow inside.

Verified: `grep -n "shutdown_observability\|shutdown_tracing" src/firecube/cli/main.py` returns a match in the `finally` block.

Evidence:
- Commits: `82ca7e7`, `c185350` (Confidence: HIGH)
- Verified by: `src/firecube/core/observability/tracing.py:43`, `src/firecube/cli/main.py`
- Source: TODO.md §20.2

---

Date: 2026-02-10

Decision: Plugin development documentation in `docs/concepts/plugins.md` and `docs/guides/subclassing_generic.md`

Context: Plugin authors had no canonical reference for building a `BaseIngestor` subclass, understanding the hook surface, or knowing which public API symbols to import. Without documentation, every new plugin required reading engine internals, and the plugin contract was effectively undiscoverable.

Consequences:
- `docs/concepts/plugins.md` created (200+ lines) covering the plugin contract, hook surface, `PluginContext`, metrics, and engine ownership of storage metrics.
- `docs/guides/subclassing_generic.md` created (126+ lines) with a worked example of subclassing `GenericZarrIngestor`.
- Both files contain substantive content, not stubs.
- Plugin authors can now depend on `firecube.ingestor.api` and `firecube.core.api` as stable public surfaces.

Verified: `wc -l docs/concepts/plugins.md docs/guides/subclassing_generic.md` returns 200+ and 126+ lines respectively.

Evidence:
- Commit: `9b34ae6` (Confidence: MEDIUM)
- Verified by: `docs/concepts/plugins.md`, `docs/guides/subclassing_generic.md`
- Source: TODO.md §3

---

Date: 2026-02-10

Decision: `FIRECUBE_*` as the sole primary environment variable namespace; `AWS_*` as a write-only compat layer

Context: Firecube's configuration was mixing `FIRECUBE_*` and `AWS_*` variable names, making it unclear which namespace drove Firecube behavior and which was a passthrough for underlying libraries. The cleanup establishes `FIRECUBE_*` as the authoritative namespace and confines `AWS_*` to an explicit compat layer.

Consequences:
- `FIRECUBE_*` is the sole primary namespace, read by `core/config.py` and used across 10+ modules.
- `AWS_*` variables are a write-only compat layer confined to `core/runtime.py`, derived FROM `FIRECUBE_*` values, never used to drive Firecube behavior directly.
- `core/runtime.py` contains an explicit rationale docstring explaining the compat layer.
- Zero `AWS_*` reads in Firecube's own config resolution path.

Verified: `grep -rn "AWS_" src/firecube/core/config.py` returns no matches (all AWS reads are in `runtime.py` only).

Evidence:
- Commit: `ac8ddd4` (Confidence: MEDIUM)
- Verified by: `src/firecube/core/config.py`, `src/firecube/core/runtime.py`
- Source: TODO.md §4

---

Date: 2026-01-05

Decision: Safer resume semantics via `ResumeGuard` with control-plane-primary authority

Context: Partial runs left the product in an ambiguous state. Without a formal resume guard, re-running ingestion could silently overwrite or duplicate data. The `ResumeGuard` class makes resume decisions based solely on WAL state, blocking on non-terminal runs and optionally validating the Zarr store when explicitly requested.

Consequences:
- `ResumeGuard` class at `src/firecube/ingestor/runtime/resume_guard.py:16` is the single authority for resume decisions.
- Resume is control-plane-primary: WAL state is consulted first; the data store is only read under explicit `validate_zarr=true`.
- `time_overlaps` parameter enables overlap detection at `manager.py:358` and `repo.py:493`.
- Non-terminal runs block resume and require explicit `firecube chunks runs abandon` before proceeding.
- `validate_zarr`, `validate_zarr_group`, `validate_zarr_timeout_s`, `validate_zarr_max_chunks`, `validate_zarr_on_timeout` budget flags in `src/firecube/ingestor/config/engine.py:64-68`.
- Staged-write metadata seeding via `seed_staged_store_metadata()` at `src/firecube/ingestor/runtime/zarr/staged_metadata.py` copies `zarr.json` from the final target into the temp store before staged writes.
- Note: AGENTS.md "Where things live" cites `ingestor/utils/zarr/staged_metadata.py` but the file is at `ingestor/runtime/zarr/staged_metadata.py`. The functionality is fully present; only the AGENTS.md path reference is stale.

Verified: `grep -rn "class ResumeGuard" src/firecube/` returns `src/firecube/ingestor/runtime/resume_guard.py:16`.

Evidence:
- Commit: `b9017c1` (Confidence: MEDIUM)
- Verified by: `src/firecube/ingestor/runtime/resume_guard.py`, `src/firecube/ingestor/runtime/zarr/staged_metadata.py`
- Source: TODO.md §1

---

Date: 2026-01-05

Decision: WAL-backed run/span control-plane records with strict schema versioning and legacy migration

Context: The legacy `._firecube_manifest.jsonl` format was a flat append-only file with no schema version, making it hard to query by run, filter by status, or perform safe maintenance operations. The run/span model replaces it with a structured WAL under `.firecube/runs/<run_id>/events-*.jsonl`, enabling lifecycle-aware queries and explicit schema evolution.

Consequences:
- WAL events under `.firecube/runs/<run_id>/events-*.jsonl` are the authoritative control-plane records.
- `SpanRecorder` is lifecycle-aware with `register_run_started`, `register_run`, and `register_run_failure` methods.
- Strict `SCHEMA_VERSION = "v2"` enforced at 4 read sites — no silent schema drift.
- Legacy v1 forward-only migration via `migrate_legacy_manifest` for existing products.
- `list_runs(status=..., non_terminal=...)` with CLI surface `firecube chunks runs list --status started`.
- `._firecube_manifest.jsonl` is legacy-only; new code does not read or write it.

Verified: `grep -rn "SCHEMA_VERSION" src/firecube/core/controlplane/` returns matches at 4+ read sites.

Evidence:
- Commit: `b9017c1` (Confidence: MEDIUM); hardening in `bc463d4` (2026-03-09)
- Verified by: `src/firecube/core/controlplane/repo.py`, `src/firecube/core/controlplane/events.py`
- Source: TODO.md §6

---

Date: pre-restructure (Confidence: LOW)

Decision: Storage and URI handling hardened — no substring guessing, no `startswith("s3://")` in business logic

Context: URI handling in the storage layer used ad-hoc `startswith("s3://")` checks and substring guessing to determine storage type. These patterns are fragile and break when URI schemes change or when local paths are passed with `file://` prefixes. The fix routes all URI decisions through `parse_uri()` and `is_remote_target()`.

Consequences:
- `ensure_product_uri` uses `parse_uri()` plus last-segment equality — no substring guessing.
- `path_stats` uses `is_remote_target()` instead of `startswith("s3://")`.
- Zero bare `startswith("s3://")` calls anywhere in `src/firecube/`.
- Bucket validation moved to `StorageUri.parse()` / `__post_init__` in `src/firecube/core/storage/uri.py:65-104`, raising `ValueError` at URI construction time.
- Note: TODO.md §12.1 claims "`test_connection` raises `StorageError` explicitly when no bucket is configured". This claim is stale. `S3Storage`, `LocalStorage`, and `BaseStorage` were deleted entirely in commit `1addd97`. The `test_connection` method no longer exists. The equivalent fail-fast guard now lives in `StorageUri.parse()` and raises `ValueError` (not `StorageError`) at URI construction time. The behavior (no silent `ls("/")` fallback) is preserved; the exception type and layer changed.

Verified: `grep -rn 'startswith("s3://')' src/firecube/` returns no matches.

Evidence:
- Commits: `bb8636a`, `923fc4f` (Confidence: LOW — pre-restructure umbrella commits)
- Verified by: `src/firecube/core/storage/uri.py`, `src/firecube/core/uris.py`
- Source: TODO.md §12.1

---

Date: 2026-05-26

Decision: Schema initialization now happens under the group claim in `IndexedRegionStrategy.write_groups()`

Context: `IndexedRegionStrategy.write_groups()` initialized Zarr arrays with `writer.ensure_group(...)` before entering `claim_for_group(group_name)`. That left a schema-before-claim race window for concurrent direct writers.

Consequences:
- `writer.ensure_group(...)` now runs inside the `with claim_ctx:` block in `src/firecube/ingestor/runtime/zarr/strategies/indexed_region.py`.
- `tests/unit/test_indexed_region_strategy_ordering.py` asserts the claim enters before schema initialization.
- This closes the direct-write schema race without changing claim acquisition or writer behavior.

Verified: `uv run pytest tests/unit/test_indexed_region_strategy_ordering.py -q`

Evidence:
- Commit: `afe60c1`
- Verified by: `src/firecube/ingestor/runtime/zarr/strategies/indexed_region.py`, `tests/unit/test_indexed_region_strategy_ordering.py`
- Source: TODO.md §24

---

Date: 2026-05-26 Task: §25

AppendMultiresHandler removed; zarr_multi_res config field removed — commit 89b2737 — Files: append_services.py (delete AppendMultiresHandler), append.py (remove wiring), strategies/append.py (remove multires param), templates/config.py (remove field + add explicit validator), templates/generic.py (remove mapping), test_append_multires_handler.py (deleted), test_zarr_multi_res_config_rejected.py (new), docs/reference/config.md and msg_frm.md (migration note). Migration: zarr_multi_res now raises ValueError with migration path to firecube zarr multires CLI. External plugin firecube-msg-frm may need follow-up if it sets zarr_multi_res.

---

## Date: 2026-05-27 Task: §21
ZarrWriteStrategy split into AppendWriteStrategy and RegionWriteStrategy — commit e16f6b4 — Files: contracts.py (deleted ZarrWriteStrategy, added AppendWriteStrategy + RegionWriteStrategy with @runtime_checkable), strategies/append.py (docstring), strategies/indexed_region.py (docstring), tests/unit/test_strategy_protocols.py (new). BREAKING CHANGE: external plugins importing ZarrWriteStrategy get ImportError; migrate to AppendWriteStrategy or RegionWriteStrategy. Verification: uv run pytest tests/unit/test_strategy_protocols.py -q. External plugin firecube-msg-frm may need follow-up update if it imports ZarrWriteStrategy.

---

## Date: 2026-05-27 Task: §23
RegionZarrWriter pre-allocates time dimension via ZarrArraySpec.expected_time_count — commit 548f892 — Files: templates/direct_zarr.py (added expected_time_count field + __post_init__ validation to ZarrArraySpec), runtime/zarr/strategies/indexed_region.py (effective shape computation in write_groups), core/zarr/region_writer.py (skip-resize early-out in ensure_timestamp_slot + write_timestamp + DeprecationWarning on fallback), tests/unit/test_region_zarr_writer_preallocation.py (new, 10+ tests covering happy path + edge cases + sparse storage). Phase 1 of no-resize-on-write delivery — opt-in via expected_time_count, fallback preserved with DeprecationWarning. Verification: uv run pytest tests/unit/test_region_zarr_writer_preallocation.py -q. T2.5 (next commit) wires auto-compute.

---

## Date: 2026-05-27 Task: §23-AUTO
IndexedRegionStrategy auto-computes expected_time_count from WriteIntent.ts_index — commit 120cde9 — Files: indexed_region.py (added pre-claim_ctx auto-compute block; dataclasses.replace for immutability), tests/unit/test_region_zarr_writer_preallocation.py (added test_auto_compute_from_write_intents, test_plugin_override_wins, test_empty_intents_skips_auto_compute). Effect: ALL DirectZarrIngestor users now get pre-allocation transparently; plugins keep override option. Phase 1.5 follow-up TODO removed (delivered here). Verification: uv run pytest tests/unit/test_region_zarr_writer_preallocation.py -q.

---

## Date: 2026-05-27 Task: §22 (helpers)
Created runtime/zarr/batch_runner.py with 5 named helpers — commit 6b4827d — Files: src/firecube/ingestor/runtime/zarr/batch_runner.py (new, 5 functions: seed_staged_metadata_for_batch, build_zarr_write_context, build_claim_closure_for_append, build_append_strategy, assemble_batch_metrics), tests/unit/test_batch_runner.py (new, 13 tests). Follows §4a append_services.py precedent. Wiring into GenericZarrIngestor._process_batch completed in T5 (§22 wiring). Verification: uv run pytest tests/unit/test_batch_runner.py -q.

---

## Date: 2026-05-27 Task: §7-COMPAT
Stale-only sweep for legacy zarr_group claims (Phase 2 migration) — commit 804533b — Files: core/controlplane/claims.py (added _sweep_legacy_zarr_group_claims + _legacy_sweep_done_per_product instance attr, invoked from acquire()), tests/unit/test_legacy_claim_sweep.py (new, 6 tests). Sweep is product-scoped, category-filtered (":zarr_group:" substring), and stale-only. Active legacy claims PRESERVED with WARNING; expire via heartbeat (default 120s). Single-release compat code; remove in future release once operators have migrated. Verification: uv run pytest tests/unit/test_legacy_claim_sweep.py -q.

---

## Date: 2026-05-27 Task: §7-DIRECT
DirectZarrIngestor per-slot claims via claim_for_slot callback — commit c18b894 — Files: contracts.py (additive claim_for_slot param on RegionWriteStrategy), indexed_region.py (split intent dispatch by ts_index; schema under claim_for_group; intents under claim_for_slot with fallback chain), direct_zarr.py (schema closure name=f"{group}:schema"; new slot closure name=f"{group}:slot={ts_index}"; both use category="zarr_region"), tests/unit/test_direct_zarr_per_slot_claims.py (new, 10 tests incl. high-slot-count). Phase 1 invariants preserved: §23-AUTO auto-compute, §24 ordering, empty-intents skip. Verification: uv run pytest tests/unit/test_direct_zarr_per_slot_claims.py -q.

---

## Date: 2026-05-27 Task: §22 (wiring complete)
GenericZarrIngestor._process_batch thinned from 143 lines to ~66 lines using batch_runner helpers — commit 785c5c6 — Files: templates/generic.py (replaced 5 inline blocks with batch_runner.* calls), tests/unit/test_generic_zarr_process_batch.py (new, 5 behavior-preservation tests). Phase 1 behavior preserved: exception path, cleanup-on-finally, telemetry spans, conditional staged seeding, resume_existing AND force_reingest AND-logic, metrics dict shape. §22 fully complete. Verification: uv run pytest --strict-deps -m "not slow and not s3" -q.

---

## Date: 2026-06-01 — Observability Boundary Consolidation — schema and tracing helpers in `core.observability`

All metric schema, tracing helpers, and domain-collector key constants now live in `src/firecube/core/observability/`. The runtime telemetry module (`src/firecube/ingestor/runtime/telemetry.py`) no longer owns `RUN_SUMMARY_SCHEMA`, `TelemetryService`, or any `METRIC_*` constants; it imports what it needs from `core.observability.metrics`. The CLI and engine no longer import OpenTelemetry directly; they call the six facade helpers in `core.observability.tracing`.

### Reviewer findings addressed

- **F1 (CLI OTel leak confirmed):** `src/firecube/cli/main.py` imported `opentelemetry.trace` directly. Replaced with `span` and `set_current_span_attribute` from `firecube.core.observability` (T7).
- **F2 (schema in runtime confirmed):** `RUN_SUMMARY_SCHEMA`, `TelemetryService`, and all `METRIC_*` constants lived in `src/firecube/ingestor/runtime/telemetry.py`. Moved to `src/firecube/core/observability/metrics.py` (T4/T6).
- **F3 (compute_run_summary zero-use confirmed):** `compute_run_summary` was re-exported from `firecube.ingestor.api` but had no external callers. Hard-removed from the public surface (T11).
- **F4 (collector key coupling confirmed):** `FilesystemMetrics.as_summary()` and `WalMetrics.as_summary()` used inline string literals for summary keys. Replaced with named constants (`FS_SUMMARY_KEY_*`, `WAL_SUMMARY_KEY_*`) imported from `core.observability.metrics` (T9/T10).
- **F5/F6 (logging/Prometheus already clean):** No violations found at audit time. Boundary tests added to lock the clean state going forward (T13).

### Decisions

- **F4 import constants:** Domain collectors import key constants from `core.observability.metrics`; they do not define string-literal summary keys inline.
- **Hard-remove `compute_run_summary`:** Removed from `firecube.ingestor.api.__all__`. The implementation stays in `runtime/telemetry.py` for internal use; the public re-export is gone.
- **Handwritten architecture tests:** `tests/unit/test_observability_boundaries.py` enforces the OTel, Prometheus, and logging-handler boundaries via AST scanning, mirroring `test_no_raw_fsspec_usage.py`. No CI plugin required.

### Forward links

- Module map: see "Where things live" in `AGENTS.md` (observability bullets).
- Enforcement rules: see "Observability Rules" in `plans/DESIGN.md`.

### Evidence

- Commits: `<sha>` (T1 boundary scaffold), `<sha>` (T4 metrics.py), `<sha>` (T5 tracing facade), `<sha>` (T6 telemetry cleanup), `<sha>` (T7 CLI swap), `<sha>` (T8 engine swap), `<sha>` (T9 FS constants), `<sha>` (T10 WAL constants), `<sha>` (T11 api removal), `<sha>` (T13 boundary lockdown)
- Verified by: `tests/unit/test_observability_boundaries.py` (5 passed), `tests/unit/test_telemetry_schema_snapshot.py`, `tests/unit/test_tracing_facade.py`
- Source: observability-boundary-refactor plan

---

## 2026-06-10 — CF-1.8 compliance: configurable time-dim + Tier-1 advisor

**Decision**: Make firecube's hardcoded `"timestamp"` dimension plugin-configurable via `BaseIngestor.time_dim_name: ClassVar[str]` (default `"timestamp"`). Ship a Tier-1 CF-1.8 advisor under `firecube advise compliance --profile cf-18`. Internal `firecube_timestamp_state` array name stays stable. Migration is deferred to a follow-up plan.

**Rationale**:
- Plugins are forced to call `ds.rename({"time": "timestamp"})` to satisfy firecube's hardcoded dim. This is the workaround we want to eliminate.
- Adding `time_dim_name` as a `ClassVar` (not a config-tier field) mirrors `PRODUCT_NAME` and avoids exposing it via `--option` (TypedOptionsParam enumerates dataclass fields automatically — that's the wrong semantics for a per-product property).
- Tier-1 validator (~150 LOC) is enough for the 80% case; Tier 2 (CF Standard Name Table) and Tier 3 (UDUNITS) are deferred. No external library wrap (`nc-check` too immature, `compliance-checker` requires libnetcdf+NCZarr).
- Migration is a separate design problem: it needs a generic framework (Strategy + Registry + safety runner), not a one-off `migrate-dim` command. Deferred to a follow-up plan with a full brief in TODO.md.

**Implementation summary**:
- Class attribute `time_dim_name: ClassVar[str] = "timestamp"` on `BaseIngestor` + `_resolve_time_dim_name()` helper.
- All 8 hardcoded `"timestamp"` write-domain sites now consume the resolved name as a parameter (host-free at the strategy/helper layer).
- New `firecube.core.cf` package with CF001..CF015 check IDs and Tier-1 validator (191 LOC).
- New `firecube advise compliance --profile cf-18` command with `--format text|json` and `--strict` (exit 0/1/2).
- Group-aware existing-cube mismatch check fails with exact text pointing at `plans/TODO.md` migration framework follow-up.
- Test fixture plugin `cf_time_dim_test_plugin` declaring `time_dim_name="time"` proves the end-to-end path.

**See also**: `plans/DESIGN.md` (Decided Questions, Risks To Avoid).

---

## 2026-06-11 — Span deletion resolves time_dim_name (closes maintenance dead-end)

**Decision**: Maintenance span deletion no longer assumes the default `"timestamp"` dimension. Span records now carry `time_dim_name` (recorded by the engine at write time via `SpanCoverage`), and `DeletionEngine.delete_spans` resolves the dim per span in a pre-flight pass: span-recorded value > discovery from the 1-D `firecube_timestamp_state` array's `dimension_names` > explicit `--time-dim` (new `chunks delete-span` flag) > engine default `"timestamp"`. An explicit name that contradicts a recorded or discovered name aborts with zero chunks deleted.

**Rationale**:
- After the 2026-06-10 `time_dim_name` ClassVar shipped, `firecube chunks delete-span` hard-failed on any conforming cube (e.g. dim `"time"`): the CLI constructed `ChunkManager` with the default dim and offered no override — an operational dead-end that invited re-adding the forbidden index-0 fallback.
- Recorded-then-discovered beats flag-first: the flag is an operator assertion about data layout; trusting it over the cube's own metadata is how chunks get deleted along the wrong axis. The state array is a safe discovery anchor because its name is contractually stable and it is written 1-D along the time dim.
- Resolution runs before any deletion (pre-flight) so a misconfigured flag cannot cause partial deletion; `_resolve_time_dim_index` still raises loudly (now with `--time-dim` remediation) and never falls back to index 0.

**Deliverables**:
- `SpanCoverage.time_dim_name` + WAL span payload field (additive, schema v2 unchanged); threaded from `AppendCoverageBuilder` and `CoverageTracker`/`IndexedRegionStrategy`; parsed back in both dict-to-`SpanCoverage` coercers.
- `DeletionEngine._resolve_span_time_dims` pre-flight resolver with per-state-array discovery cache and conflict guard.
- `chunks delete-span --time-dim` flag; `ValueError` surfaced as clean `ClickException`.
- Tests: `tests/unit/test_deletion_time_dim_resolution.py` (resolution matrix, conflict aborts with zero deletion, WAL round-trip, default-dim regression); e2e delete-span on a `time_dim_name="time"` cube in `tests/integration/test_e2e_time_dim_plugin.py`.
- Operator docs: Custom Time Dimensions section in `docs/operations/chunk-manager/delete.md`.

**See also**: `plans/DESIGN.md` (Risks To Avoid, time-dim entry), DONE.md 2026-06-10 (the feature this closes the gap for).

---

## 2026-06-11 — Legacy `output_path=` result constructor kwarg removed

**Decision**: `PipelineResult` and `IngestResult` no longer accept the legacy `output_path=` constructor kwarg; the only construction path is the documented typed contract `outputs=OutputPaths(primary=...)`. The read-only `result.output_path` property is kept as a compatibility view of `outputs.primary` for readers (engine merge/reporting code still reads it); read-path migration is deferred to a later pass.

**Rationale**:
- The public contract (AGENTS.md, DESIGN.md, `docs/concepts/plugins/contract.md`) said "never `output_path=`" while the constructor silently accepted and coerced it — a contract/code contradiction, and the engine itself kept the deprecated path alive in its failure handlers.
- Removing construction first and keeping the read property is the safe order: a plugin constructing the legacy form now fails loudly at `TypeError`, while reads are harmless and fully derivable from `outputs.primary`.

**Migration performed**:
- All 16 internal construction sites rewritten to typed `outputs=` (engine failure handlers, `base.py` seed/no-batch results, `cli/archive.py` synthetic restore result, all four templates). Values preserved exactly, including empty-path failure results.
- `_coerce_output_paths` lost its `output_path` parameter and override branches; the dict-shape coercion and the zarr-mirror rule (`zarr = primary` when `output_format == "zarr"`) are unchanged, so `outputs=OutputPaths(primary=X)` reproduces the old behavior byte-for-byte.
- Control-plane record fields (`record_run_started(output_path=...)`, WAL/manifest `output_path` keys, `SpanRecorder.register_run*`) are intentionally untouched — persisted operational metadata, not the plugin result contract.
- Contract locked by `tests/unit/test_result_constructor_contract.py` (legacy kwarg raises `TypeError` on both classes; property and zarr-mirror behavior pinned). No test or fixture-plugin call sites used the legacy form.

**Still open**: migrate remaining internal reads of `result.output_path` to `result.outputs.primary`, then decide whether the compatibility property itself can go.

---

## 2026-06-11 — Tensogram dependency upgraded 0.17.0 → 0.21.0

**Decision**: Bump `tensogram`, `tensogram-xarray`, and `tensogram-zarr` pins from `>=0.17.0,<0.18` to `>=0.21.0,<0.22` (Phase 0 of the TODO §8 promotion plan; Phase 1 — first-class output format — is a separate feature branch).

**What 0.18–0.21 changed for firecube** (verified empirically, not just from release notes):
- 0.18 made the CBOR metadata frame free-form and removed `GlobalMetadata.version`. Firecube's metadata builders wrote a top-level `"version": 3` (the removed field) — dropped from `make_data_meta`, `make_controlplane_meta`, and `dataset_to_global_meta`; tests updated to assert the key is absent. Custom top-level keys (`firecube`, `base`) are now sanctioned free-form.
- 0.19's `sp_*` wire-key rename does not affect firecube (zstd/none codecs only; no `simple_packing` usage).
- 0.21's `validate_file(level="default")` attempts per-frame hash verification, which legacy (pre-0.21) frames cannot satisfy (`HASH_PRESENT` clear). `firecube archive validate` now classifies "no inline hash recorded" as a legacy note and stays VALID/exit 0; real hash mismatches and structural issues still fail. New 0.21-written archives validate with hashes verified.
- The Python API surface firecube calls (`TensogramFile.create/open/append/decode_message/file_decode_metadata/message_count/read_message`, `validate_file`) is unchanged; `decode_message` gained `verify_hash` (adoption deferred to Phase 1, needs HASH_PRESENT-aware gating for legacy archives).

**Backward compatibility locked**: `tests/fixtures/data/archive_v1_tensogram_0_17_0.tgm` — a real archive generated with 0.17.0 installed (provenance in `tests/fixtures/data/README.md`) — plus `tests/integration/test_tensogram_archive_compat.py` (validate VALID with legacy note, quick-validate, info, full restore + readback). Do not regenerate the fixture with newer tensogram.

**Smoke-tested e2e on 0.21**: ingest → archive create → info → validate → restore → readback: data values byte-identical, coords and CF dataset attrs preserved, control plane restored.

**Known pre-existing issues surfaced by the smoke test** (confirmed identical on 0.17 via a downgraded probe env — NOT upgrade regressions; logged in TODO §8 for Phase 1): `--output-format tensogram` ingest fails with `Is a directory` (control-plane root created inside the `.tgm` target); archive restore loses time-coordinate CF encoding (`units`/`calendar` never serialized) and writes base-entry dicts as variable attrs.

---

## 2026-06-11 — `zarr_chunk` alias deleted; typed-flag keys hard-rejected in `--option`

**Decision** (closes AUDIT.md S1 + S3; TODO §12.2/§12.4 entries updated):
- `ZarrTemplateConfig.zarr_chunk` is deleted with no shim, per STYLE.md's Option Aliases rule. Its only behavior was `__post_init__` silently nulling an explicit `zarr_chunk_shape` — a hidden-precedence veto between two names for one concern. Zero users existed (no matches in src, tests, docs, or fixtures). `zarr_chunk_shape` is the single chunking option; `zarr_chunk` now fails strict unknown-key rejection.
- `--option` hard-rejects keys owned by dedicated `firecube ingest` typed flags: `write_mode`, `slot_start`, `slot_end`, `slot_size`, `slot_group` (`_TYPED_FLAG_OWNED_KEYS` in `src/firecube/cli/_typed_options.py`, enforced at parse time with remediation naming the owning flag). These five were the entire hole: they are real `EngineConfig` fields, so they passed unknown-key validation, and `cli/main.py` merges free-form options *after* typed-flag resolution — `--option write_mode=direct` silently overrode an explicit `--write-mode staged`. Hard-reject chosen over warn: same doctrine as the explicit-flags migration (silent override is silent misconfiguration). All other typed flags (`--target`, `--storage-type`, `--product-name`, ...) are not config fields and were already rejected as unknown keys. Engine options without a dedicated flag (`force_reingest`, `no_progress`, `pipeline_parallel`, ...) remain the sanctioned `--option` surface, locked by a regression test.

**Method**: TDD — 8 contract tests written first and confirmed red (`tests/unit/test_zarr_chunk_alias_removed.py`, parametrized typed-flag rejection + engine-option acceptance in `tests/unit/test_typed_options_param.py`), then the deletion + parse-time guard turned them green.

---

## 2026-06-18 — DirectZarr write-parity + CF-time telemetry fix

### What

Closed 5 DirectZarr API parity gaps (A–E) identified during OPERA plugin migration:
- Gap A (shards): added `ZarrArraySpec.shards` field and wired through `ensure_group`.
- Gap B (attrs): added `ZarrArraySpec.attrs` field; reserved-attrs guard via `_reserved_attrs.py`.
- Gap C (float64+units time write): dissolved into Gap B — plugins declare CF attrs on the time coord spec.
- Gap D (static arrays): added `ZarrArraySpec.time_indexed=False`, `WriteIntent.kind="static"`, `RegionZarrWriter.write_static`.
- Gap E (dimension_names): added `ZarrArraySpec.dimension_names` field; `ensure_group` passes to Zarr v3 `create_array`.

Fixed 1970-epoch telemetry bug:
- Root cause: `pd.Timestamp(float64)` interpreted values as nanoseconds-since-epoch.
- Fix: new `firecube.core.zarr.time_decode.decode_time_array(values, attrs)` helper decodes via `(dtype, attrs)` dispatch.
- Applied to `AppendCoverageBuilder.record_batch` and `_coerce_append_value`.
- Removed bare `except Exception: pass` from `AppendCoverageBuilder`.

Architecture guard added: AST-walk test banning `pd.Timestamp(<numeric>)` without `unit=` in write-domain modules.

### Files touched (summary)

- `src/firecube/ingestor/templates/direct_zarr.py` — ZarrArraySpec, WriteIntent.kind, _setup_global_zarr_schema.
- `src/firecube/core/zarr/region_writer.py` — ensure_group, verify_array_spec, write_static.
- `src/firecube/core/zarr/time_decode.py` — new module.
- `src/firecube/core/zarr/_reserved_attrs.py` — new module.
- `src/firecube/ingestor/runtime/zarr/append_services.py` — AppendCoverageBuilder CF-time decode.
- `src/firecube/ingestor/runtime/zarr/append.py` — _coerce_append_value CF-time decode.
- `src/firecube/ingestor/runtime/zarr/strategies/indexed_region.py` — kind=static dispatch, 1d coverage constraint, time_indexed guard.

### Verification

- `uv run pytest tests/integration/test_cf_time_dim_telemetry.py` — locks 1970 bug fix.
- `uv run pytest tests/integration/test_direct_zarr_xr_open_roundtrip.py` — dimension_names round-trip.
- `uv run pytest tests/integration/test_direct_zarr_parity_slot_range.py` — slot-range parity with static arrays.
- `uv run pytest tests/architecture/test_no_raw_pd_timestamp_numeric.py` — pd.Timestamp guard.
- `uv run pytest --strict-deps` passes on `feat/directzarr-parity-and-cf-time-decode`.

---

### DirectZarr per-slot payload retention — bite-the-pill + §F3 promotion (2026-07-12)

**Decision:** MTG FCI L1C FDHSI ingest retains ~14.8 GiB per worker under `list[WriteIntent]` semantics; the operator-observed 12-worker × 12-slot 2h ingest reached ~178 GiB total per pod. Sub-batching was investigated as a plugin-side memory reduction path and explicitly rejected. IDEAS.md §F3 (lazy-payload thunk, originally static-only) is promoted to TODO.md as accepted core work, generalized to time-indexed intents.

**Accepted operational path today (no code change):** `pipeline_workers=1` × N disjoint-range pods via the `firecube zarr slots` planner. Per-pod peak stays at ~14.8 GiB; total cluster memory unchanged from an intra-pod approach but sized per-pod it fits standard nodes. Feature knobs (`include_pixel_time=false` → ~5.5 GiB, `pixel_time_dtype=float32` → ~10 GiB) remain available for further per-pod reduction where downstream tolerates.

**Rejected alternatives:**
- Iterator-lazy `build_write_intents` (`-> Iterable[WriteIntent]`): breaks six load-bearing invariants (slot-range validation, `expected_time_count` autosize, `allow_grow`, group presence, per-slot claim grouping, `len(intents)` metrics); saves list-container references only, not payload bytes.
- Plugin-side sub-batching with shared `ts_index` (yield-per-nc_part / tile / channel): per-slot claim raises `ClaimConflictError` on any concurrent second acquirer (no retry loop at the dispatch site, deterministically reproduced 2026-07-12); `CoverageTracker` records at `(group, ts_index)` only, so mid-slot crash-resume produces silently incomplete data (integrity hazard).
- Internal-iteration sub-batching (single batch, chunked emission): safe but empirically ineffective per 2026-07-12 POC — three configurations (baseline / naive / cached) plateau together within noise at the baseline retention level.

**§F3 promotion scope (see TODO.md §F3):** `WriteIntent.data: np.ndarray | Callable[[], np.ndarray] | Any` — additive union, metadata-eager / payload-lazy at intent level, generalized to time-indexed `kind="region"` and `kind="static"` intents. Design constraints named: scratch lifetime, provider caches, resume-check materialization, `kind="1d"` / `kind="timestamp"` scope.

**Doctrine tightening (see DESIGN.md "Risks To Avoid"):** iterator-lazy `build_write_intents` and shared-`ts_index` sub-batching now explicit anti-patterns with rationale.

**Test gaps opened (see TEST_GAPS.md P2):** DirectZarr retained-payload regression harness and `CoverageTracker` sub-slot granularity as prerequisites for accepting any lazy-payload or sub-batching change.

**Evidence trail:** MTG FCI L1C plugin repo — memory diagnosis (2026-07-11, full memray attribution 99.9% accounted, pixel_time 66% dominant); sub-batching POC report (2026-07-12, three-config plateau at baseline, `ClaimConflictError` deterministic reproduction); plugin-author formal review response.

---

## 2026-07-12 — `normalize_string_vars` (§19 remainder closed)

**Date:** 2026-07-12

**Decision:** `normalize_string_vars` — fourth §19 NetCDF-to-Zarr utility

**Context:** Post-concat vlen-string widening (h5netcdf/h5py version-dependent behaviour)
caused downstream Zarr write failures. A generic string-normalization utility was
promised as the fourth member of the §19 set when the first three utilities shipped
(`clean_netcdf_encoding`, `rename_time_dim`, `prepare_netcdf_for_zarr`; DONE.md §19,
commit ef771cc). This entry closes the §19 remainder.

**Consequences:**
- `normalize_string_vars(ds, *, iso_targets=None, logger=None) -> xr.Dataset`
  added to `src/firecube/core/formats/netcdf.py`.
- Strict UTC ISO-8601 parser with precision-preserving datetime64 conversion
  (`src/firecube/core/formats/_iso.py`; `datetime64[s]` vs `datetime64[us]`
  auto-detected from fractional-second presence).
- Context-sensitive predicate: raises ValueError only when a variable is
  explicitly named in `iso_targets` AND is not processable; silently skips
  non-string object vars otherwise (DEBUG log when logger provided).
- Attribute handling matches xarray's decode convention: `units`/`calendar`
  move from `.attrs` to `.encoding` on ISO-converted vars by default. Callers
  can opt in to verbatim preservation via `preserve_cf_time_attrs=True`; then
  firecube's CF advisor may flag the variable. No silent deletion of metadata.
  Source `.encoding` is fully discarded on ISO-converted vars in both modes
  (fresh encoding populated only from moved attrs) — anti-leak invariant
  covered by test C.
- Re-exported via `firecube.core.formats` and `firecube.core.api`.
- 36 unit tests in `tests/unit/test_netcdf_utils.py` (11 ISO helper tests
  + 18 normalize_string_vars tests + 7 pre-existing).

**Verified:**
- `from firecube.core.api import normalize_string_vars` works
- `uv run pytest tests/unit/test_netcdf_utils.py -q` → 36 passed
- `uv run ruff check src/firecube/core/formats/netcdf.py src/firecube/core/formats/_iso.py` → clean
- `uv run pyright src/firecube/core/formats/netcdf.py` → 0 errors

**Evidence:**
- Commits: f4f7594 feat(core/formats): normalize string vars for NetCDF inputs; 0897bc8 feat(core/formats): implement _iso UTC parser with precision-preserving datetime64 conversion; c6084e7 test(core/formats): RED tests for normalize_string_vars and _iso helpers; ae3b98f refactor(test): fix import order and remove noqa suppressor in netcdf utils tests; 3214274 fix(core/formats): reject timezone-naive strings in utc ISO parser; 1553e25 fix(core/formats): move CF time attrs to encoding with preserve opt-in
- Source: plans/TODO.md §19 remainder (removed)
- Follow-up refinement (2026-07-12+): CF time-attr handling pivoted from
  silent DELETE to xarray `pop_to()`-style MOVE-to-encoding; added
  `preserve_cf_time_attrs: bool = False` opt-in for verbatim retention.
  Rationale: silent deletion was neither ecosystem-conventional (xarray
  moves, not deletes) nor advisory-friendly (violated the CF-is-advisory
  principle). Ref: internal plan `normalize-string-vars-cf-attrs-refinement`.

**Confidence:** HIGH
