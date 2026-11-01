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

## Lanes

This is the canonical statement of the lane commands. `README.md`,
`CONTRIBUTING.md`, the PR template, `.prompts/` and `plans/RELEASE.md` repeat
them; when a command changes here, change those copies and
`.github/workflows/` with it. Run from the repository root after
`uv sync --extra test` and `uv pip install -e tests/fixtures/firecube_test_plugins`.

| Lane | Command | Collected |
|---|---|---|
| Fast behavior (primary gate; CI `test` job adds `-n auto` and coverage) | `uv run pytest --strict-deps -m "not slow and not s3 and not docs_static and not snapshot and not race" -q --tb=short` | 3634 |
| Docs/static drift (CI `docs` job first runs `uv run mkdocs build --strict --site-dir /tmp/firecube-mkdocs-check`) | `uv run pytest --strict-deps -m "docs_static or snapshot" -q --tb=short` | 38 |
| Extended storage (S3) | `uv run pytest --strict-deps -m "s3 and not race" -q --tb=short` | 37 |
| Slow | `uv run pytest --strict-deps -m "slow and not race" -q --tb=short` | 3 |
| Full run before merge or release | `uv run pytest --strict-deps -q --tb=short -W error::DeprecationWarning` | 3711 |
| Race (red by design, explicit only) | `uv run pytest -m race` | 30 |

Counts are `--collect-only` totals at the time of writing and move with the
suite; the commands do not.

The `--strict-deps` flag (registered in `tests/conftest.py`) causes pytest collection to **FAIL** with `pytest.UsageError` if a required test-extra package is missing. This catches dependency drift at collection time instead of producing silent skips. This is the canonical list; it mirrors the guard in `tests/conftest.py`: `trollsift`, `tensogram`, `obstore`, `moto`, `healpix-geo`, `griffe` (installed by the `griffelib` package) and `pystac-client`, plus `moto.server` (the `moto[server]` extra, which provides `ThreadedMotoServer`).

`pyproject.toml` sets `addopts = "-ra -q --strict-markers -m 'not race'"`. A CLI
`-m` expression REPLACES that `-m 'not race'` filter, so every explicit `-m`
invocation other than the race lane must carry `and not race` itself wherever a
race test could match the expression; omitting it there runs the red-by-design
race characterization and fails the lane. An explicit `-m` filter replaces the
default exclusion and can select race tests: a negative expression such as
`not slow` matches the race tests, because they carry neither `s3` nor
`slow`. Invocations
without `-m` (the full run, `uv run pytest -q`) keep the `addopts` filter.

The fast lane is the primary behavior gate for agentic workloads. The docs/static
lane runs separately so its failures do not drown out behavioral regressions.
CI also runs `uv run pyright` before the tests.

## Local Dev Invocation

```bash
uv run pyright
uv run pytest -q
```

Without `--strict-deps`, legitimate platform skips remain visible, and any optional-dep imports that fail produce **loud ERRORs at runtime** rather than silent skips — making missing extras obvious. For one lane use its command from the table; for one file:

```bash
uv run pytest tests/unit/test_chunk_manager.py -q
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
| `benchmark` | Remote-reader timing check: the one test carrying it, `test_remote_open_time` in `tests/integration/test_open_time_default_reader.py`, opens the store at `FIRECUBE_OBJSTORE_URL` with default reader settings and asserts elapsed time within a budget; it skips when `FIRECUBE_OBJSTORE_URL` is unset, so it skips in the fast lane unless that variable is set |
| `benchmark_evidence` | Long-form, high-fidelity benchmark evidence; manual, not part of the default CI lane |

`pyproject.toml` registers these 14 markers. `--strict-markers` is on, so an unregistered marker is a collection error.

Mark tests at definition time. Unmarked tests are still collected; markers exist to let you filter, not to gate collection.

## Red-by-Design Tests

Tests marked `race` are intentionally RED: they characterize the legacy per-slot write path losing updates in dense coordinate chunks, as an executable record of why the managed-coordinate path exists. They are excluded from the default lane via the `addopts` `-m 'not race'` filter in `pyproject.toml` and from the CI/publish `-m` filters (see [Lanes](#lanes)); run them explicitly with `uv run pytest -m race`, and do not "fix" them to green. `gate` tests are the opposite: always green, run in the default lane, and assert the invariants the fix guarantees (e.g. no overlapping materialization-run intervals).

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
- `tests/architecture/test_cli_boundary.py` — CLI boundary: no Click, `firecube.cli` or CLI command text below `cli/`, no private module imported by `cli/`, a 500-line ceiling per `cli/` module. Tagged allowlists; a stale row fails.
- `tests/sdk/test_plugin_contract.py` — public plugin import-surface contract.
- `tests/unit/test_no_raw_fsspec_usage.py` — write-domain storage boundary, and live metadata reads (`test_zarr_metadata_opens_read_live_metadata`: Zarr opens pass `use_consolidated=False` or `consolidated=False`; tagged rows, a stale row fails).
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

- [ ] The fast behavior lane (see [Lanes](#lanes)) passes with no unexpected skips
- [ ] The docs/static lane passes, or any docs/static drift is intentionally recorded
- [ ] The full run (`-W error::DeprecationWarning`) passes
- [ ] No new `pytest.skip(...)` calls without a legitimate reason (platform, hardware, or external service)
- [ ] No new raw `fsspec.filesystem(...)` calls outside the allowlist in `tests/unit/test_no_raw_fsspec_usage.py`
- [ ] `uv run ruff check .` clean
- [ ] `uv run pyright` clean
- [ ] LSP diagnostics clean on changed files
