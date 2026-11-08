# Firecube (AGENTS.md)

Firecube is a plugin-driven batch ingestion engine for building and maintaining operational analysis-ready data cubes. Plugins own product interpretation. The engine owns writing (Zarr, Parquet) and the product-local `.firecube/` control plane (claims, runs, spans, index, snapshots, releases). The engine also owns the maintenance commands (archive, snapshot, migrate, release) that work on a finished cube.

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
uv pip install -e tests/fixtures/firecube_test_plugins
```

This is required for `tests/integration/test_ingest_command_typed.py` and related CLI tests.

## Maintaining this file

This file is onboarding and a task-to-document router. It holds the commands that test work, the non-obvious repository-wide invariants and traps, and "when working on X, read Y" lines.

- Budget: 200 lines and 12 KiB. No bullet exceeds 60 words. No line references of the form `path:NNN`. Name paths and symbols only. `tests/architecture/test_agents_md_budget.py` enforces the budget.
- Do not append a per-feature design note, a limitation list, an incident or a test-gate description here after you finish work.
- A feature changes its canonical docs, its docstrings, `plans/DONE.md`, `plans/TODO.md` and its tests.
- Add at most one routing line, and only when an agent could not find the destination by reading the repository.
- A change to this file is a reviewed policy change. The pull request must answer three questions.
  1. Which repeated failure or durable repository-wide rule does the change prevent?
  2. Why is a test, a linter or a canonical document not the right place?
  3. What does the change add, and what does it remove?
- The maintainer approves each such change.

## Test Discipline

Test skip policy, CI invocation, and pytest markers. → See [plans/TEST.md](plans/TEST.md)
Behavior-first testing standards, static-test limits, and the test-suite overhaul plan. → See [plans/TESTING_STANDARDS.md](plans/TESTING_STANDARDS.md)

## Documentation prompts

Before editing documentation, read [.prompts/docs-policy.md](.prompts/docs-policy.md).

Choose one page prompt before you write a page: `write-user-doc`, `write-plugin-doc`, `write-operator-doc` or `write-internal-doc`. The gate `tests/sdk/test_reference_pages_stay_generated.py` keeps reference pages generated.

## Workflow prompts

Reusable project workflows live in `.prompts/` (docs fact-checking, review-comment
handling, coverage, edge cases, error handling, release preparation, and PR
preflight). Prefer these prompt files over inventing a new checklist for repeated
maintenance tasks.

## CLI requirements (explicit, no inference)

The CLI infers storage settings from the URI scheme for commands that take a
product URI (`file://`→local, `s3://`→s3; driver defaults to `fsspec`). Pass
`--storage-type` and `--storage-driver` explicitly to override.

**Both flags below are required explicitly:**

- `-t/--target <uri>` — the product URI, the one spelling on every command that takes one
- `--write-mode [staged|direct]` — write strategy (NOT inferred from local vs remote)

A store records its product label once, in `.firecube/product.json`. A command that takes a target reads the label back, and most refuse a store that records none. The CLI never infers it from the URI path. `--product-name` is optional on write commands. Exit codes, the label record and `ExecutionParams` are in `plans/DESIGN.md`.

Write-tier commands require `--write-mode`, except `zarr multires` (which
derives pyramid levels in place) and `zarr consolidate-time-coord` (which
mutates a single existing array in place and has no staging path).

**Product name precedence** (used once, to record the label): CLI `--product-name` > config `default_product_name` > plugin `PRODUCT_NAME` > hard fail.

- URI options: `parse_and_validate_uri` and `parse_and_validate_source_uri` in `src/firecube/cli/_uri_policy.py` admit every storage URI option. `stac://` is a source scheme only, admitted by the `--input-data` of `ingest`, `zarr preallocate` and `zarr slots`. The docstrings there are the spec.

## Architectural invariants

Core design rules for this batch ingestor, including control-plane model and observability rules. → See [plans/DESIGN.md](plans/DESIGN.md)

`GenericParquetIngestor` currently requires a fresh target with no previous runs
or data. Resume and force-reingest do not bypass this restriction. A product
claim covers the run through promotion and terminal recording, and duplicate
part paths are refused. Stable Parquet slice identity and replay remain deferred
in `plans/TODO.md`.

## Where things live

- Plugin contract: `src/firecube/ingestor/runtime/base.py` (`BaseIngestor`) and `src/firecube/ingestor/contracts/interfaces.py` (Protocols).
- Engine/pipeline runner: `src/firecube/ingestor/runtime/engine.py`.
- Public surfaces for plugins: `src/firecube/ingestor/api.py` and `src/firecube/core/api.py` (prefer these over deep imports).
- External plugins: installed via entry points under `firecube.plugins`; plugin-specific usage belongs in each plugin repository.
- Storage: `src/firecube/core/filesystem/` (`StorageFilesystem` Protocol, `FsspecFilesystem`, `ObstoreFilesystem`) and `src/firecube/core/storage/`. Create a driver with `create_filesystem(binding)`. All I/O goes through the chosen driver, no mixing. URI helpers: `src/firecube/core/uris.py`. The optional `obstore` import guard is `_obstore_compat.py`.
- Literal session removal: `StorageSession.delete_literal(uri)` in `src/firecube/core/storage/session.py` removes one object and expands no glob character. `StorageSession.delete` is recursive and is not for this.
- Source discovery: `discover(request)` in `src/firecube/core/discovery/` is the one door. A caller reaches its backend modules only through the package. A `SourceItem` is read by its `href`, never by `SourceItem.scheme`.
- Coordinate materialization: `src/firecube/core/zarr/coord_materialization.py`, engine-owned. Every writing path is in this module. The read-only checks are in `coord_rematerialization.py`, the pure helpers in `_coord_common.py`.
- Coordinate re-materialization checks: `src/firecube/core/zarr/coord_rematerialization.py` (`check_regular_coord_rematerialization`, `check_irregular_coord_rematerialization`). The module writes nothing. A relabel of stored data raises `CoordinateRelabelError`.
- Shared coordinate helpers: `src/firecube/core/zarr/_coord_common.py`. `existing_array` reads by full path from a live root.
- Zarr CLI command package: `src/firecube/cli/zarr/`, one module per command family, shared helpers in `_common.py`.
- Reserved attrs guard: `src/firecube/core/zarr/_reserved_attrs.py` holds the reserved array attribute names. `assert_attrs_safe(attrs)` checks them.
- Static array spec field: `ZarrArraySpec.time_indexed`. Set `False` for a static array such as lat/lon. It is created at the declared shape and skips time-axis preallocation.
- Metric schema and emission: `src/firecube/core/observability/metrics.py` (canonical schema `RUN_SUMMARY_SCHEMA`, `TelemetryService`, domain-collector key constants).
- CF-1.8 Tier-1 advisor: `src/firecube/core/cf/` — check IDs (`check_ids.py`), report dataclasses (`report.py`), structural validator (`validator.py`). Surfaced via `firecube advise compliance --profile cf-18`.
- Fill helpers: `src/firecube/core/zarr/_fill.py`. Compare an array with its fill value only through these helpers.
- Archive: commands in `src/firecube/cli/archive/` register with `@click.command`, not `@archive.command`. Operations are in `src/firecube/ingestor/runtime/archive.py`. ZIP code (`zip/`) and Tensogram code (`tensogram/`) are in `src/firecube/core/archive/`, with `clear_target` in `_common.py`. No Tensogram module imports a ZIP module.
- Guarded layer: DirectZarr payload writes go through `guarded_write` or `guarded_publish` in `src/firecube/core/zarr/guard/guarded_write.py`. Do not write payloads to a DirectZarr store any other way. `zarr.json` changes only under a `MetadataAuthority`. Node locks are leaves. Rules: `plans/DESIGN.md`.
- Staged runs: `StorageSession.plan_promotion` and `publish_plan` in `src/firecube/core/storage/session.py` publish a delta planned by `guard/promotion_plan.py`. GenericZarr keeps `upload_tree`. Rules: `plans/DESIGN.md`.
- Control plane: the `ChunkManager` facade is `src/firecube/core/controlplane/manager.py`. Beside it: `repo.py`, `deletion.py`, `types.py`, `events.py`, `claims.py`. The migrate and release executors are in `src/firecube/ingestor/runtime/`.
- Claims: `FilesystemClaimService` in `claims.py`. Mixed versions are not compatible: stop every older writer of a product before a new writer writes to it. Firecube never removes or replaces the guard file `.firecube-claims.lock`.
- Exclusive maintenance claim: `ChunkManager.acquire_exclusive_maintenance` in `manager.py`. It binds the recorded label, so a second spelling of the URI cannot bypass it. Plugin CLI commands and `ensure_slot_index_model` make no check.
- Content-addressed helpers: `src/firecube/core/controlplane/_content_addressed.py`, internal. The snapshot store, the release records and the inventories import it. A module does not import another store's private helpers.
- Version retention: `src/firecube/ingestor/runtime/release_versions.py`, internal, beside `release_retention.py`. It builds, preflights and verifies `--retain-versions`. `release.py` imports it at call time.
- Migration steps: the built-in step `firecube:set-attrs` is in `src/firecube/ingestor/runtime/migrations/set_attrs.py`, registered under `firecube.migrations` in `pyproject.toml`. `load_migration_steps` in `src/firecube/ingestor/registry/loader.py` reads that group without importing a step.
- Time-coordinate consolidation: `consolidate_time_coords` in `src/firecube/core/zarr/coord_consolidation.py` bypasses `guarded_write` and `MetadataAuthority` and takes no claim. Do not copy that pattern.
- CLI and public operations: a command calls `firecube.operations` in `src/firecube/operations.py`. An operation takes the `Product` that `open_product` returns, in `src/firecube/core/product/open.py`. Reference: `docs/reference/operations.md`. Import gate: `tests/architecture/test_cli_boundary.py`. A new violation is not given a row.

### Plugin-specific CLI helpers (optional)
- Core CLI hosts plugin utilities under `firecube plugins <plugin> ...` when a plugin registers a Click group under the `firecube.plugin_cli` entry-point group (e.g. in `pyproject.toml`: `[project.entry-points."firecube.plugin_cli"]` with `my_plugin = "my_plugin.plugin_cli:cli"`). Merely creating a `plugin_cli` module is not enough — the entry point must be declared.

## Gotchas / debugging

- `--option no_progress=true` suppresses the coarse pipeline progress lines only. Other INFO logs, including parallel Zarr evidence lines, still print (`docs/concepts/observability/logs.md`).
- If ingestion "hangs" at 0% with parallel execution, set `--option pipeline_workers=1` to isolate locking/IO contention (DuckDB, HDF5).

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
