# Firecube Test Suite

This directory contains Firecube's pytest suite. Before adding or reshaping
tests, read:

- [AGENTS.md](../AGENTS.md)
- [plans/TEST.md](../plans/TEST.md)
- [plans/TESTING_STANDARDS.md](../plans/TESTING_STANDARDS.md)
- [plans/TEST_GAPS.md](../plans/TEST_GAPS.md)

`plans/TEST.md` defines commands, markers, dependency checks, and skip policy.
`plans/TESTING_STANDARDS.md` defines what tests are worth adding or keeping.
`plans/TEST_GAPS.md` tracks missing high-risk behavior coverage.

## Structure

```text
tests/
├── architecture/   repository boundary and static invariant tests
├── cli/            CLI contract, semantic help, and command matrix tests
├── fixtures/       in-tree fixture plugins and test data
├── helpers/        shared test helpers
├── ingestor/       ingestor architecture and configuration tests
├── integration/    cross-component and end-to-end behavior tests
├── sdk/            public API and plugin contract tests
├── unit/           focused unit and component tests
└── test_*.py       root-level regression tests
```

Do not add new tests tied to removed external plugins unless they are explicit
fixture plugins installed from `tests/fixtures/`.

## Setup

Install test dependencies and fixture plugins before running the full suite:

```bash
uv sync --extra test
uv pip install -e tests/fixtures/cli_test_plugin
uv pip install -e tests/fixtures/direct_zarr_capable_test_plugin
uv pip install -e tests/fixtures/direct_zarr_non_capable_test_plugin
uv pip install -e tests/fixtures/multi_group_capable_test_plugin
uv pip install -e tests/fixtures/cf_time_dim_test_plugin
```

## Common Commands

```bash
# Local full suite
uv run pytest -q

# Strict dependency gate
uv run pytest --strict-deps -q --tb=short

# Warning gate used before merge/release
uv run pytest --strict-deps -q --tb=short -W error::DeprecationWarning

# Focused development loop
uv run pytest --strict-deps -m "not slow and not s3 and not docs_static and not snapshot" -q --tb=short

# Docs/help/static lane
uv run pytest --strict-deps -m "docs_static or snapshot" -q --tb=short

# Single file
uv run pytest tests/unit/test_chunk_manager.py -q
```

## Markers

Markers are defined in `pyproject.toml`; use only registered markers.

- `unit`: pure unit tests, no external I/O or services
- `integration`: multiple components together
- `architecture`: repository architecture and boundary invariants
- `contract`: public SDK, plugin, CLI, or persisted-format contracts
- `docs_static`: documentation, help-text, and static prose drift checks
- `snapshot`: reserved for deliberately approved narrow golden snapshots
- `plugin`: specific ingestor plugin behavior
- `s3`: live or mocked S3 endpoint coverage
- `slow`: long-running tests
- `concurrency`: parallel or concurrent behavior

Markers select lanes; they do not make weak tests acceptable.

## Adding Or Changing Tests

New tests must protect a named behavior and risk. Prefer real filesystem/Zarr
objects and fixture plugins over mocks. Static scans and broad snapshots are
allowed only when they protect a current invariant and belong in the correct
lane. Broad prose policing, generated-site checks, and repository-wide example
scans do not belong in the default confidence path. See
[plans/TESTING_STANDARDS.md](../plans/TESTING_STANDARDS.md) for the review
checklist.
