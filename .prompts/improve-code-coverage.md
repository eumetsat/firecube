---
description: Measure Firecube coverage and add focused tests for meaningful gaps
agent: build
---

# Improve Code Coverage

Perform a coverage analysis and fill meaningful gaps with focused tests.

## Arguments

`$ARGUMENTS` — optional paths, modules, or test subsets to focus on. If omitted,
start with the full project.

## 1. Measure Current Coverage

Install test dependencies and fixture plugins if needed:

```bash
uv sync --extra test
uv pip install -e tests/fixtures/cli_test_plugin
uv pip install -e tests/fixtures/direct_zarr_capable_test_plugin
uv pip install -e tests/fixtures/direct_zarr_non_capable_test_plugin
uv pip install -e tests/fixtures/multi_group_capable_test_plugin
uv pip install -e tests/fixtures/cf_time_dim_test_plugin
```

Run coverage:

```bash
uv run pytest --strict-deps --cov=firecube --cov-report=term-missing
```

For a focused area:

```bash
uv run pytest --strict-deps tests/path_or_file.py --cov=firecube.module --cov-report=term-missing
```

## 2. Identify Gaps

For each file with significant uncovered lines, read the source and classify gaps:

- **Needs tests**: reachable code paths with no coverage.
- **Error handling**: defensive paths that can be triggered safely.
- **Integration-only**: behavior requiring storage, CLI, plugins, or filesystem setup.
- **Dead code**: unreachable or obsolete code that should be removed.
- **Platform/cloud-specific**: paths requiring a specific OS, S3, or optional dependency.

Do not chase coverage percentages with low-value tests. Prefer tests that lock
behavior users or plugin authors depend on.

## 3. Prioritize

Prioritize:

- resume/idempotency and control-plane behavior
- storage drivers and URI handling
- plugin contract enforcement
- CLI validation and actionable errors
- Zarr/Parquet write correctness
- observability redaction and metric/trace boundaries
- cleanup, deletion, archive, and recovery paths

## 4. Add Tests

- Add tests to existing files when patterns already exist.
- Use fixture plugins for CLI and plugin-contract behavior.
- Test failure paths, not only happy paths.
- Prefer deterministic local filesystem fixtures over remote services.
- Mark slow, S3, and integration tests according to `plans/TEST.md`.

## 5. Remove Dead Code

If a gap appears unreachable:

- Verify with `rg`, tests, and call graph inspection.
- Remove the code rather than excluding it from coverage.
- Update docs if a public surface is removed.

## 6. Verify

Run:

```bash
uv run ruff check .
uv run pyright
uv run pytest <focused tests>
uv run pytest --strict-deps --cov=firecube --cov-report=term-missing
```

Report before/after coverage by file and explain any accepted gaps.
