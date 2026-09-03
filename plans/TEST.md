# Test

Validation strategy. The rules below are blocking — a change that violates them is not done.

This file defines how tests are invoked, skipped, and marked. Test quality
standards live in [TESTING_STANDARDS.md](TESTING_STANDARDS.md): behavior-first
test design, static-test limits, snapshot policy, and the suite overhaul plan.
Known high-risk coverage gaps are tracked in [TEST_GAPS.md](TEST_GAPS.md).

## Test Policy

**Policy: NEVER skip a test silently to mask a missing dependency.** Skipping tests is how regressions hide.

## The Three Skip Mechanisms

Each mechanism has a specific purpose. Use the right one, and always supply a reason.

1. **`pytest.importorskip("pkg")`** — module-level guard for optional imports. Skips the entire module if the package isn't installed.

2. **`@pytest.mark.skipif(condition, reason=...)`** — declarative conditional skip. Evaluated at collection time; the reason is visible in the test report.

3. **`pytest.skip("reason")`** — runtime skip inside a test body. Use only when the skip condition can't be determined until the test is already running.

## Legitimate Skip Reasons (the ONLY ones)

- **Platform-specific**: test only runs on Linux, macOS, or Windows.
- **Hardware-specific**: requires GPU, specific CPU features, or a particular filesystem.
- **External service unavailable**: live S3 or network endpoint not reachable in the current environment.

**Missing optional Python packages is NOT a legitimate skip reason.** Install them via `uv sync --extra test` and run with `--strict-deps` in CI. A silent skip on a missing dep is a hidden regression.

**External binaries absent from PATH ARE a legitimate skip reason.** When a test requires an external CLI tool (not a Python package), guard it with `shutil.which` and skip with the reason string `"<binary> is not available on PATH"`. Example: `tests/unit/test_version_consistency.py` skips when `uv` is not on PATH. This is distinct from optional Python packages, which must be installed and never silently skipped.

## CI Invocation (fail-fast)

```bash
uv sync --extra test
uv run pyright
uv run pytest --strict-deps -m "not slow and not s3 and not docs_static and not snapshot and not race" -q --tb=short
uv run pytest --strict-deps -m "docs_static or snapshot" -q --tb=short
uv run pytest --strict-deps -q --tb=short -W error::DeprecationWarning
```

The `--strict-deps` flag (registered in `tests/conftest.py`) causes pytest collection to **FAIL** with `pytest.UsageError` if any test-extra Python package is missing (`tensogram`, `obstore`, `moto`, or `healpix-geo`). This catches dependency drift at collection time instead of producing silent skips.

The first pytest command is the primary behavior gate for agentic workloads. The
second command runs broad documentation/help/static drift checks separately so
their failures are visible without drowning out behavioral regressions.

A CLI `-m` expression REPLACES the `-m 'not race'` filter from `addopts`, so
every explicit `-m` invocation must carry `and not race` itself; omitting it
runs the red-by-design race characterization and fails the lane.

## Local Dev Invocation

```bash
uv run pyright
uv run pytest -q
```

Without `--strict-deps`, legitimate platform skips remain visible, and any optional-dep imports that fail produce **loud ERRORs at runtime** rather than silent skips — making missing extras obvious.

Run a focused subset during development:

```bash
# Fast behavior loop
uv run pytest --strict-deps -m "not slow and not s3 and not docs_static and not snapshot and not race" -q --tb=short

# Docs/help/static lane
uv run pytest --strict-deps -m "docs_static or snapshot" -q --tb=short

# Single file
uv run pytest tests/unit/test_chunk_manager.py -q
```

Run the deprecation-warning gate before merge or release:

```bash
uv run pytest --strict-deps -q --tb=short -W error::DeprecationWarning
```

## Pytest Markers

| Marker | Meaning |
|---|---|
| `unit` | Pure unit test, no I/O or external services |
| `integration` | Exercises multiple components together |
| `architecture` | Repository architecture and boundary invariants |
| `contract` | Public SDK, plugin, CLI, and persisted-format contracts |
| `docs_static` | Documentation, help-text, and static prose drift checks |
| `snapshot` | Reserved for deliberately approved narrow golden snapshots; broad CLI help snapshots are not kept |
| `plugin` | Tests a specific ingestor plugin |
| `s3` | Requires a live or mocked S3 endpoint (moto) |
| `slow` | Long-running; excluded from the default fast loop |
| `race` | Red-by-design characterization of a known legacy race; excluded from the default lane and all CI lanes. Run explicitly via `-m race`. |
| `gate` | Deterministic concurrency-invariant gates (e.g. coordinate-materialization serialization). Run in the default lane; use `-m gate` to run them alone. |
| `concurrency` | Tests parallel/concurrent behaviour |

Mark tests at definition time. Unmarked tests are still collected; markers exist to let you filter, not to gate collection.

## Red-by-Design Tests

Tests marked `race` are intentionally RED: they characterize the legacy per-slot write path losing updates in dense coordinate chunks, as an executable record of why the managed-coordinate path exists. They are excluded from the default lane via the `addopts` `-m 'not race'` filter in `pyproject.toml` and from the CI/publish `-m` filters; run them explicitly with `uv run pytest -m race`, and do not "fix" them to green. `gate` tests are the opposite: always green, run in the default lane, and assert the invariants the fix guarantees (e.g. no overlapping materialization-run intervals).

## Integration Test Fixture Plugins

The integration tests require the in-tree fixture plugins, shipped as one
editable distribution that installs every fixture module and entry point:

```bash
uv pip install -e tests/fixtures/firecube_test_plugins
```

This is required before running the full integration suite. Without it,
plugin-contract and typed-ingest tests will error at import time. The
fixture modules keep their individual names (`cli_test_plugin`,
`regular_axis_test_plugin`, ...); only the packaging is unified.

Plugin-specific compatibility tests belong in each plugin's own repository,
not here: core's test bench must not depend on sibling plugin checkouts.

## Test Suite Options

`--trials <int>` (default 30): repetition count for the multiprocess race
characterization in `tests/integration/test_multiprocess_race_managed_coord.py`.
Only the `race`-marked red test consumes the full trial count; the green `gate`
test uses a small fixed count so the default lane stays fast.

## Architecture And Contract Tests

Architecture and contract tests are invariant gates, not optional style checks.
They should fail loudly when a change violates a repository boundary or public
contract. Do not weaken these tests to unblock unrelated work.

Current invariant suites include:

- `tests/architecture/` — repository-wide architecture and storage-flow rules.
- `tests/sdk/test_plugin_contract.py` — public plugin import-surface contract.
- `tests/unit/test_no_raw_fsspec_usage.py` — write-domain storage boundary.
- `tests/unit/test_observability_boundaries.py` — OpenTelemetry, Prometheus, and logging boundaries.

Use `@pytest.mark.architecture` for new repository-boundary tests and
`@pytest.mark.contract` for public SDK, plugin, CLI, or persisted-format
contracts.

## Historical Evidence

`plans/DONE.md` is an evidence log. Do not rewrite old verification commands to
match current policy. New DONE entries should use the current checks, but old
entries must remain historically accurate unless they are factually wrong.

## Pre-merge Checklist

Before marking a change done:

- [ ] `uv run pytest --strict-deps -m "not slow and not s3 and not docs_static and not snapshot" -q --tb=short` passes with no unexpected skips
- [ ] `uv run pytest --strict-deps -m "docs_static or snapshot" -q --tb=short` passes, or any docs/static drift is intentionally recorded
- [ ] `uv run pytest --strict-deps -q --tb=short -W error::DeprecationWarning` passes
- [ ] No new `pytest.skip(...)` calls without a legitimate reason (platform, hardware, or external service)
- [ ] No new raw `fsspec.filesystem(...)` calls outside the allowlist in `tests/unit/test_no_raw_fsspec_usage.py`
- [ ] `uv run ruff check .` clean
- [ ] `uv run pyright` clean
- [ ] LSP diagnostics clean on changed files
