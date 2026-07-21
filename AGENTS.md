# Firecube ingestor (AGENTS.md)

This repo is a **batch ingestion worker** for EO datasets that writes Zarr/Parquet and maintains a product-local `.firecube/` control-plane root used for idempotency, resume safety, cleanup, and write coordination.

## Quick start

- For full test suite: `uv sync --extra test`
- Install deps: `uv sync`
- Run CLI: `uv run firecube --help`
- Tests: `uv run pytest`
- Lint: `uv run ruff check .`
- Format check: `uv run ruff format --check .`
- Type check: `uv run pyright`

Before running the test suite, install the CLI integration test fixture plugin:

```bash
uv pip install -e tests/fixtures/cli_test_plugin
uv pip install -e tests/fixtures/direct_zarr_capable_test_plugin
uv pip install -e tests/fixtures/direct_zarr_non_capable_test_plugin
uv pip install -e tests/fixtures/multi_group_capable_test_plugin
uv pip install -e tests/fixtures/cf_time_dim_test_plugin
uv pip install -e tests/fixtures/slot_shape_test_plugin
```

This is required for `tests/integration/test_ingest_command_typed.py` and related CLI tests.

## Test Discipline

Test skip policy, CI invocation, and pytest markers. → See [plans/TEST.md](plans/TEST.md)
Behavior-first testing standards, static-test limits, and the test-suite overhaul plan. → See [plans/TESTING_STANDARDS.md](plans/TESTING_STANDARDS.md)

## Documentation prompts

Before editing documentation, read [.prompts/docs-policy.md](.prompts/docs-policy.md).

When editing public docs:

- Identify the primary audience first: user, plugin author, operator, or contributor.
- Prefer commands, examples, expected output, verification steps, and troubleshooting.
- Explain user consequences before architecture.
- Do not add phase history, audit findings, commit labels, reviewer notes, internal service names, private module paths, line numbers, `.sisyphus/`, or `plans/` references to public task pages.
- Use public CLI flags and public SDK imports (`firecube.ingestor.api`, `firecube.core.api`) unless the page is explicitly internal.
- Do not create or substantially rewrite a docs page without choosing the matching prompt from `.prompts/` (`write-user-doc`, `write-plugin-doc`, `write-operator-doc`, or `write-internal-doc`).
- Put implementation history, design rationale, and maintenance evidence in `plans/`, `.sisyphus/`, or an explicitly internal page, not in the user-facing path.

## Workflow prompts

Reusable project workflows live in `.prompts/` (docs fact-checking, review-comment
handling, coverage, edge cases, error handling, release preparation, and PR
preflight). Prefer these prompt files over inventing a new checklist for repeated
maintenance tasks.

## CLI requirements (explicit, no inference)

The CLI infers storage settings from the URI scheme for commands that take a
product URI (`file://`→local, `s3://`→s3; driver defaults to `fsspec`). Pass
`--storage-type` and `--storage-driver` explicitly to override.

**All flags below are required explicitly:**

- `--product-name <name>` — logical product name (or set `PRODUCT_NAME` on your plugin class)
- `--target <uri>` — where to write the output product
- `--write-mode [staged|direct]` — write strategy (NOT inferred from local vs remote)

Write-tier commands require `--write-mode`, except `zarr multires`, which
derives pyramid levels in place and has no staging path.

**Removed behaviors (migration required):**
- `output_name` no longer inferred from target URI basename — use `--product-name` or plugin `PRODUCT_NAME`
- `storage_type` no longer inferred from `s3://` vs `file://` — pass `--storage-type` explicitly
- `write_mode` no longer defaults to `direct` for local targets — pass `--write-mode` explicitly
- Config key `default_output_name` is rejected — use `default_product_name` instead

**Product name precedence:** CLI `--product-name` > config `default_product_name` > plugin `PRODUCT_NAME` > hard fail.

`--source`/`--target` are always strict product URIs; `--archive`/`--output`
are always strict artifact URIs; `--input-data`, `--config-file`,
`--target-dir`, and `--workspace` are exempt from the strict-URI policy
(`--input-data` accepts a local path or an `s3://` prefix, interpreted by the
plugin; the rest are local paths).

## Architectural invariants

Core design rules for this batch ingestor, including control-plane model and observability rules. → See [plans/DESIGN.md](plans/DESIGN.md)

## Where things live

- Plugin contract: `src/firecube/ingestor/runtime/base.py` (`BaseIngestor`) and `src/firecube/ingestor/contracts/interfaces.py` (Protocols).
- Engine/pipeline runner: `src/firecube/ingestor/runtime/engine.py`.
- Public surfaces for plugins: `src/firecube/ingestor/api.py` and `src/firecube/core/api.py` (prefer these over deep imports).
- External plugins: installed via entry points under `firecube.plugins`; plugin-specific usage belongs in each plugin repository.
- Storage/fs: `src/firecube/core/filesystem/` + `src/firecube/core/storage/` (fsspec-based facade).
- Storage driver abstraction: `src/firecube/core/filesystem/` — `StorageFilesystem` Protocol + `FsspecFilesystem` + `ObstoreFilesystem`. Use `create_filesystem(config)` factory. `ZarrStoreFactory` in `store_factory.py`. All I/O goes through the chosen driver — no mixing.
- Optional obstore dep: `src/firecube/core/filesystem/_obstore_compat.py` — import guard. Install: `uv pip install 'firecube[obstore]'`.
- URI helpers: `src/firecube/core/uris.py` (protocol detection, file:// normalization).
- Zarr maintenance: `src/firecube/core/zarr/validation.py`, `src/firecube/core/zarr/scrub.py`, `src/firecube/core/zarr/state.py`, `src/firecube/core/zarr/multires.py`
- Time decode helper: `src/firecube/core/zarr/time_decode.py` — self-describing time-array decode helper (`decode_time_array`). Dispatches on `(dtype, attrs)`; used by `AppendCoverageBuilder` and the append cursor. xarray's CF decoder is a private impl detail. Output preserves the decoded `datetime64` resolution (NOT coarsened to seconds): its values feed coverage bounds and dedup keys, where a second-floor would collapse distinct sub-second timestamps. Decode failures (malformed `units`/`calendar`) propagate loudly — `AppendCoverageBuilder` does NOT swallow them (see DESIGN.md "Risks To Avoid"; the bare-except was removed 2026-06-18 because silent swallowing hid the 1970-epoch coverage bug).
- Reserved attrs guard: `src/firecube/core/zarr/_reserved_attrs.py` — frozenset of reserved array attribute names firecube manages internally; `assert_attrs_safe(attrs)` helper.
- Static array spec field: `ZarrArraySpec.time_indexed` — set `False` for non-time-indexed static arrays (e.g. lat/lon). Mirrors the `PRODUCT_NAME` pattern. Arrays with `time_indexed=False` are created at declared shape; they do NOT participate in time-axis preallocation.
- Static write intent: `WriteIntent.kind="static"` — for static (non-time-indexed) coordinate arrays; dispatches to `RegionZarrWriter.write_static`. Bypasses slot-range validation; divergent data on resume raises `SchemaDriftError`. Write-once is enforced via the reserved `firecube_static_written` marker attr stamped after commit — NOT by inferring freshness from array contents (an all-fill array is indistinguishable from legitimate all-NaN/NaT data, so a contents probe would silently overwrite it on resume). Marker absent ⇒ fresh shell, write+stamp (no full read); marker present ⇒ strict NaN/NaT-aware replay-or-raise.
- ChunkManager / `.firecube/` control-plane ops: `src/firecube/core/controlplane/manager.py` (facade) + `src/firecube/core/controlplane/repo.py`, `src/firecube/core/controlplane/deletion.py`, `src/firecube/core/controlplane/types.py`, `src/firecube/core/controlplane/events.py`, `src/firecube/core/controlplane/claims.py`
- Staged-write metadata seeding: `src/firecube/ingestor/runtime/zarr/staged_metadata.py` (core helper) + `src/firecube/ingestor/runtime/zarr/batch_runner.py` (`seed_staged_metadata_pre_batch` hook). Honored uniformly for any template that runs in `write_mode="staged"` with `seeds_staged_metadata=True` — no per-template wiring required. The runtime engine invokes seeding before `_process_batch()` for all Zarr outputs.
- Metric schema and emission: `src/firecube/core/observability/metrics.py` (canonical schema `RUN_SUMMARY_SCHEMA`, `TelemetryService`, domain-collector key constants).
- CPU/wait accounting: `duration_cpu_s` is process-wide CPU (all threads, user+sys) measured once over the processing window via `time.process_time()` in `PipelineRunner.run_state` — NOT summed from per-batch `time.thread_time()`, which only sees the orchestrating thread and undercounts CPU spent in dask/HDF5/netCDF worker threads (observed ~5× on SEVIRI ingests, making a CPU-bound run look I/O-bound). `non_cpu_wait_s = max(processing_wall_s - cpu_s, 0)`; it is `0` when CPU-bound. Per-batch `PipelineResult.cpu_time_s` stays `thread_time`-based and is a diagnostic lower bound only.
- Tracing facade helpers: `src/firecube/core/observability/tracing.py` (`span`, `set_current_span_attribute`, `capture_context`/`attach_context`/`detach_context`, `propagated_context`).
- Observability boundary enforcement: `tests/unit/test_observability_boundaries.py` (mirrors `test_no_raw_fsspec_usage.py`; covers OTel, Prometheus, logging-handler boundaries).
- CF-1.8 Tier-1 advisor: `src/firecube/core/cf/` — check IDs (`check_ids.py`), report dataclasses (`report.py`), structural validator (`validator.py`). Surfaced via `firecube advise compliance --profile cf-18`.

### Plugin contract requirements
- Every concrete `BaseIngestor` subclass must declare `PRODUCT_NAME: ClassVar[str]` — enforced at class-definition time via `__init_subclass__`. Abstract templates (e.g. `GenericZarrIngestor`) are exempt.
- `PipelineResult.metrics` is typed `ResultMetrics` (not a plain dict). `PipelineResult.outputs` is `OutputPaths` (not a plain dict). Both are importable from `firecube.ingestor.api`.
- Plugins must not construct `PipelineResult(output_path=...)` — use `PipelineResult(outputs=OutputPaths(primary=...))`. The legacy kwarg was removed and raises `TypeError`; the read-only `result.output_path` property remains for readers.
- Plugins may declare `time_dim_name: ClassVar[str]` on their `BaseIngestor` subclass to control the firecube append/index dimension name written into the Zarr store; default is `"timestamp"`. Mirrors the `PRODUCT_NAME` pattern. This is NOT a config-tier field and is NOT exposed via `--option`. The runtime engine consumes this name to seed coordinate-array chunks in staged mode so that plugin value-based timestamp resolvers (e.g. `resolve_timestamp_index`) see real coord values instead of NaT on re-ingest.

### Plugin-specific CLI helpers (optional)
- Core CLI hosts plugin utilities under `firecube plugins <plugin> ...` when a plugin registers a Click group under the `firecube.plugin_cli` entry-point group (e.g. in `pyproject.toml`: `[project.entry-points."firecube.plugin_cli"]` with `my_plugin = "my_plugin.plugin_cli:cli"`). Merely creating a `plugin_cli` module is not enough — the entry point must be declared.

## Gotchas / debugging

- If JSON logs are required, disable progress logging (`--option no_progress=true`) to keep stdout machine-readable.
- If ingestion "hangs" at 0% with parallel execution, try `--option pipeline_parallel=false` or reduce `pipeline_workers` to isolate locking/IO contention (DuckDB, HDF5).

## Style guide

Pythonic principles, GoF patterns, and named anti-patterns. → See [plans/STYLE.md](plans/STYLE.md)

## Where to read more

- [plans/DESIGN.md](plans/DESIGN.md) — architectural invariants, control-plane model, observability rules
- [plans/STYLE.md](plans/STYLE.md) — Python style guide and named anti-patterns
- [plans/TEST.md](plans/TEST.md) — test discipline and skip policy
- [plans/TESTING_STANDARDS.md](plans/TESTING_STANDARDS.md) — behavior-first testing standards and suite overhaul plan
- [plans/TODO.md](plans/TODO.md) — active open work
- [plans/IDEAS.md](plans/IDEAS.md) — speculative ideas with status tags
- [plans/DONE.md](plans/DONE.md) — completed decisions with dates and evidence
- CHANGELOG.md 0.1.2 — DirectZarr write-parity (new `ZarrArraySpec` fields, static arrays, CF-time telemetry)
