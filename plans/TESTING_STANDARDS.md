# Testing Standards

Contributor and agent guidance for writing, reviewing, pruning, and running
Firecube tests. This document defines what good tests are. `plans/TEST.md`
defines command policy, skip policy, and CI invocation.

## Audience

This is an internal contributor standard for humans and agentic workloads that
change Firecube source, tests, plans, or docs. It is not a user guide.

## Core Rule

Tests must prove behavior that matters to Firecube's contracts. A test is worth
keeping only if it would fail when a real regression breaks one of these things:

- data correctness or persisted output layout
- resume, idempotency, deletion, or cleanup safety
- storage-driver routing and write-domain boundaries
- public CLI, SDK, config, or persisted-format contract
- operator visibility through metrics, logs, traces, or control-plane records

Tests that mainly pin formatting, implementation shape, or old incident history
must be rare, clearly marked, and kept out of the default bug-catching loop
unless they protect a current public contract.

## Required Test Shape

Every new or changed non-trivial behavior needs tests in the smallest useful
combination of these categories.

| Category | Required When | What It Must Prove |
|---|---|---|
| Contract | Public CLI, SDK, config, persisted schema, or plugin contract changes | Accepted inputs, rejected inputs, stable output shape, clear error behavior |
| Boundary | Code handles ranges, URI schemes, dimensions, chunks, empty data, optional fields, or time spans | Zero/empty, one, max, just past max, invalid, and mixed valid/invalid cases |
| Failure mode | Any dependency, filesystem, storage, parser, validator, or writer can fail | The failure is loud, typed when possible, and does not commit partial state |
| State transition | WAL, claims, resume, deletion, staging, or lifecycle state changes | Each transition is explicit; invalid transitions are refused |
| Integration | Behavior crosses engine, storage, CLI, plugin fixture, or Zarr/Tensogram boundaries | At least one realistic end-to-end path with real filesystem/Zarr objects |
| Concurrency | Claims, WAL, workspace materialization, tracing context, or parallel slots are involved | Race-prone paths preserve isolation and deterministic ownership |

Do not use line coverage as proof. Coverage is only a discovery tool for finding
untested behavior.

## Forbidden Test Patterns

Reject these patterns in review:

- **Mirror tests**: recomputing expected values with the same logic as the code
  under test.
- **Happy-path-only tests**: testing a successful run without the adjacent
  invalid, empty, conflicting, or partial state.
- **Mock-first tests**: mocking internal collaborators so heavily that the test
  verifies the mock choreography instead of Firecube behavior.
- **Assertion-light tests**: only checking `exit_code == 0`, `is not None`,
  "does not raise", or "method was called" when a meaningful output, file,
  WAL record, metric, or error can be asserted.
- **Snapshot sprawl**: adding full-output snapshots for broad surfaces like
  help text or docs when a semantic assertion would catch the real contract.
- **Static archaeology**: scanning for historical strings, phase names, or old
  private paths unless the test protects an active architecture invariant.
- **Dead fixture tests**: tests tied to removed plugins, product names, or sample
  paths that no longer represent a current contract.
- **Permanent xfail drift**: adding `xfail` for a known bug without an accepted
  TODO item, owner, and removal condition.

## Static And Architecture Tests

Static tests are allowed only for repository invariants where runtime coverage
is weak or too expensive:

- forbidden write-domain `fsspec` bypasses
- direct OpenTelemetry, Prometheus, or logging-handler imports outside approved
  boundaries
- plugin deep imports into runtime internals
- removed CLI/config aliases reappearing
- public API exports disappearing or coming back unintentionally
- unsafe code patterns that previously caused silent corruption

Static tests must name the invariant they protect and prefer AST or structured
inspection over fragile substring matching. They must not multiply across every
file or command when one focused invariant test is enough.

## CLI, Docs, And Snapshot Tests

CLI and docs tests are contract tests only when they protect behavior a user,
operator, or plugin author depends on.

Prefer:

- command exits and error classes for required/invalid arguments
- JSON or machine-readable output shape checks
- semantic help assertions for required flags and safety warnings
- a small command-contract matrix driven by one source of truth
- docs example checks for removed commands and missing required flags

Avoid:

- full golden snapshots for every command in the default test lane
- checking the same help text through multiple test files
- asserting internal words are absent from every command when only user-facing
  commands need that rule
- treating prose wrapping changes as product regressions

Golden snapshots may exist, but they belong in a docs/CLI contract lane and must
be regenerated deliberately. A one-line prose diff should not block unrelated
runtime bugfixes in the default loop.

## Mocks, Fakes, And Fixtures

Use real objects wherever practical:

- real local temporary directories for storage behavior
- real Zarr stores for writer, resume, deletion, and metadata behavior
- fixture plugins for public plugin contract and CLI routing
- moto or an explicit S3 test environment for S3 behavior

Mocks are acceptable at external boundaries or to force rare failures:

- subprocesses and external CLIs
- network services
- clock/time when deterministic timestamps matter
- low-level filesystem failures that are hard to trigger otherwise

When using a fake, assert on Firecube-visible effects, not only fake method calls.
Examples: output arrays, WAL events, claim files, deletion result fields, metrics,
or user-facing errors.

## Dependency And Skip Rules

The skip policy in `plans/TEST.md` is binding. In short:

- missing Python packages from the test extra must fail under `--strict-deps`
- external binaries may skip with a precise reason
- platform, hardware, and unavailable external services may skip
- parametrized tests may prune non-applicable matrix cells, but those skips must
  not hide missing coverage

If a package is listed in `[project.optional-dependencies].test`, add it to the
strict dependency guard or import it normally. Do not use `pytest.importorskip`
for test-extra packages.

## Test Markers And Lanes

Markers are for selecting lanes, not for hiding tests. Current markers are
defined in `pyproject.toml`; new markers must be added there before use.

Implemented target lanes for the testing overhaul:

| Lane | Purpose | Target Contents |
|---|---|---|
| Fast behavioral | Default PR confidence | unit, contract, core integration; exclude slow, live/mocked S3 stress, docs-static, broad snapshots |
| Static policy | Architecture and public-surface drift | architecture, contract matrix, forbidden import/pattern tests |
| Docs/CLI | Documentation and help drift | focused source docs checks and semantic help conformance |
| Extended storage | Storage-driver and S3 parity | s3, obstore/fsspec remote integration, larger upload paths |
| Release | Full pre-tag evidence | strict deps, warnings as errors, coverage, docs build, package checks |

Primary agentic workload command:

```bash
uv run pytest --strict-deps -m "not slow and not s3 and not docs_static and not snapshot" -q --tb=short
```

Docs/static drift command:

```bash
uv run pytest --strict-deps -m "docs_static or snapshot" -q --tb=short
```

Do not add a new lane marker unless the marker is registered, documented, and
assigned a clear command in `plans/TEST.md`.

## Current Overhaul Plan

### Phase 0 - Establish Standards

- Add this document and link it from `AGENTS.md`, `plans/TEST.md`, and
  `tests/README.md`.
- Keep `plans/TEST.md` focused on command and skip policy.
- Keep this file focused on test quality and portfolio shape.

### Phase 1 - Inventory And Classify

- Generate a collected-test inventory by file, marker, runtime, and purpose.
- Classify tests into behavioral, static policy, docs/CLI, release-only, stale,
  and deletion candidates.
- Identify duplicate assertions across CLI help, docs examples, contract matrix,
  and golden snapshots.
- Record high-risk missing behavior in `plans/TEST_GAPS.md` instead of keeping
  stale tests as placeholders.
- Decide whether the registered `plugin` marker should be used or removed.

Definition of done: every high-volume test file has an owner category and a lane.

### Phase 2 - Fix Policy Drift

- Add every test-extra Python package to the strict dependency guard. Current
  strict guard includes `tensogram`, `obstore`, `moto`, and `healpix-geo`.
- Replace `pytest.importorskip` for test-extra packages with normal imports or a
  strict-deps failure.
- Remove or convert skips that only prune poor parametrization.
- Require each `xfail` to reference an accepted TODO entry and removal condition.

Definition of done: strict collection has no unexpected dependency skips.

### Phase 3 - Consolidate CLI And Docs Static Tests

- Collapse overlapping help tests into one semantic command-contract matrix.
- Delete broad golden snapshots instead of moving them to another default gate.
- Delete repository-wide docs example validation unless a focused docs contract
  proves current public behavior better than the CLI contract matrix.
- Replace full help text snapshots with focused assertions for required flags,
  safety wording, and machine-readable output contracts.

Definition of done: one-line prose wrapping changes do not fail the fast
behavioral lane.

### Phase 4 - Retire Stale Domain Fixtures

- Rename generic control-plane test data away from historical product names when
  the name is not part of the contract.
- Delete empty or misleading plugin/msg test scaffolding that no longer maps to
  an installed fixture plugin.
- Refresh `tests/README.md` whenever test tree structure changes.

Definition of done: no test file advertises a plugin, product, or path that does
not exist in the repo or as an explicit fixture dependency.

### Phase 5 - Replace Low-Signal Tests With Behavior

- Close the deletion-path xfails before adding more static deletion guards.
- Rewrite mock-heavy wiring tests when a real-store or real-CLI test can prove
  the same contract.
- Add focused behavioral coverage for uncovered high-risk branches: deletion
  failures, workspace cleanup failures, filesystem ops, plugin management, and
  source format readers. Prioritize `plans/TEST_GAPS.md`.

Definition of done: the highest-risk bugs are covered by behavioral tests, not
only architecture scans or snapshots.

### Phase 6 - Enforce In Agent Workloads

Before an agent adds or edits tests, it must:

1. Read `AGENTS.md`, `plans/TEST.md`, and this file.
2. Name the behavior and risk category the test protects.
3. Prefer a behavior test over a static scan or snapshot.
4. Check whether an existing test already protects the behavior.
5. Add or update markers so the test lands in the intended lane.
6. Run the smallest relevant test first, then the lane command required by the
   touched surface.

Agents must not add broad snapshot, static grep, or xfail tests to make progress
look measurable. Test count is not a quality metric.

## Review Checklist

Before accepting new or changed tests:

- [ ] The test name describes the behavior and expected outcome.
- [ ] Expected values are independent and concrete.
- [ ] The test would fail for a realistic regression.
- [ ] Failure modes and boundaries are covered when the behavior is risky.
- [ ] Mocks are limited to external boundaries or forced rare failures.
- [ ] The test asserts Firecube-visible effects.
- [ ] Static checks are justified by a current invariant.
- [ ] Snapshot coverage is narrow and assigned to the correct lane.
- [ ] Skips and xfails comply with `plans/TEST.md`.
- [ ] The test does not depend on stale plugin names, sample paths, or removed
  command surfaces.
