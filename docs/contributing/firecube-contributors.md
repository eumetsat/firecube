# Firecube Contributors

Audience: Firecube contributors and maintainers.

This page is for changes to Firecube internals: the engine, core libraries, CLI,
storage drivers, ChunkManager, observability, tests, or documentation.

Plugin authors do not need this page for normal plugin work. Start with
[Plugin Development](../guides/plugins/index.md) instead.

## Keep The Layers Intact

Firecube depends on a strict import boundary:

- `firecube.core` owns domain objects, storage, filesystem access,
  configuration, ChunkManager, and observability primitives.
- `firecube.ingestor` owns the ingestion SDK, plugin base classes, runtime
  engine, batching, registry, and template implementations.
- plugins import public SDK symbols from `firecube.ingestor.api` and
  `firecube.core.api`.

Core must not import ingestor code. Ingestor code must not import installed
plugin packages except through dynamic plugin discovery. External plugins must
not deep-import runtime internals.

## Preserve Public Surfaces

Public plugin code relies on the API modules:

- `firecube.ingestor.api`
- `firecube.core.api`

If a change moves or renames a public symbol, update the API reference and keep a
compatibility path for the documented deprecation window.

Do not document private module paths in user or plugin-author pages. Contributor
notes may reference internals when the reference is needed to make or verify a
code change.

## Runtime Areas

When changing the engine, identify the runtime area first:

- runtime orchestration: `BaseIngestor`, run setup, teardown, and result
  registration;
- batching and pipeline execution: source batches, workers, result aggregation,
  and finalization;
- write strategies: append Zarr, direct Zarr, Parquet, and Tensogram
  packaging;
- storage and filesystem drivers: local/S3 behavior, fsspec, obstore, and URI
  handling;
- ChunkManager: run records, spans, claims, snapshots, recovery, and cleanup;
- observability: structured logs, OpenTelemetry traces, Pushgateway metrics, and
  plugin telemetry boundaries;
- CLI: command groups, required flags, safety gates, and output formatting.

Keep changes scoped to the layer and runtime area that owns the behavior.

## Thread Safety And Parallel Work

Parallel work must not share mutable state unless the owner makes that state
explicitly safe.

Check these boundaries when changing worker code:

- plugin contexts are read-only from plugin hooks;
- batch workers must not mutate shared plugin state without synchronization;
- append-Zarr writes to one group remain serialized;
- direct-Zarr slot workers require disjoint, chunk-aligned ranges;
- Parquet workers must write distinct output file paths;
- ChunkManager claims must be cleared only through supported operations.

For user-facing parallel models, see [Parallelism](../concepts/parallelism.md).

## Observability Boundary

Firecube centralizes logging, tracing, and metrics. Engine code may use the
observability helpers. Plugin code gets telemetry through `ctx.telemetry`.

Do not add raw OpenTelemetry or Prometheus usage to plugin-facing code paths.
When adding new engine metrics, update the canonical metric schema and generated
reference output together.

## Test Discipline

Install the test and documentation dependencies before relying on the full suite:

```bash
uv sync --extra test --group docs
```

Pytest session startup requires the in-tree fixture plugins to be installed as
editable packages:

```bash
uv pip install -e tests/fixtures/cli_test_plugin
uv pip install -e tests/fixtures/direct_zarr_capable_test_plugin
uv pip install -e tests/fixtures/direct_zarr_non_capable_test_plugin
uv pip install -e tests/fixtures/multi_group_capable_test_plugin
uv pip install -e tests/fixtures/cf_time_dim_test_plugin
```

The CI-style invocation builds the docs first so built-site checks inspect fresh
HTML:

```bash
uv run --group docs mkdocs build --strict
uv run pytest --strict-deps -q --tb=short
```

`--strict-deps` fails collection when optional test dependencies such as
Tensogram, obstore, or moto are missing. Do not hide missing optional Python
packages behind skips; install the test extras instead.

During local development, run a focused subset first:

```bash
uv run pytest -q
uv run pytest --strict-deps -m "not slow and not s3"
uv run pytest tests/unit/test_chunk_manager.py -q
```

Use pytest skips only for real environment limits: platform-specific behavior,
hardware-specific requirements, or unavailable external services. Every skip
needs an explicit reason.

## Contributor Checks

Before marking an internal change done, run the narrow checks that match the
change:

```bash
uv run ruff check .
uv run --group docs mkdocs build --strict
uv run pytest --strict-deps -q --tb=short
```

For CLI changes, run the CLI help and docs completeness checks:

```bash
uv run pytest tests/cli/test_help_conformance.py
uv run pytest tests/unit/test_docs_completeness.py -q
```

For documentation-only changes, the docs build is the primary check:

```bash
uv run --group docs mkdocs build --strict
```

For storage, observability, ChunkManager, or parallel execution changes, include
the matching unit and integration tests rather than only the changed file.

## Next Steps

- **[Plugin Template API](../reference/api.md)** — standard plugin authoring surface
- **[Advanced Plugin API](../reference/advanced-plugin-api.md)** — custom pipeline types
- **[ChunkManager Records](../concepts/chunk-management.md)** — product state, runs, spans, claims, and snapshots
- **[Plugin Development](../guides/plugins/index.md)** — external plugin guides
