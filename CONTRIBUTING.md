# Contributing

Thank you for contributing to Firecube.

Firecube accepts contributions through pull requests. This
document outlines the process to help you prepare a focused change, run the
right checks, and get your contribution reviewed.

Please review and follow the [Code of Conduct](CODE_OF_CONDUCT.md).

## Prerequisites

Use Python 3.12 and `uv`.

```bash
uv sync
uv run firecube --help
```

For test work, install the test extra and the editable fixture plugins used by
CLI and integration tests:

```bash
uv sync --extra test --group dev
uv pip install -e tests/fixtures/cli_test_plugin
uv pip install -e tests/fixtures/direct_zarr_capable_test_plugin
uv pip install -e tests/fixtures/direct_zarr_non_capable_test_plugin
uv pip install -e tests/fixtures/multi_group_capable_test_plugin
uv pip install -e tests/fixtures/cf_time_dim_test_plugin
uv pip install -e tests/fixtures/slot_shape_test_plugin
```

For documentation changes:

```bash
uv sync --group docs
```

## Branches And Scope

Create one branch per logical change. Use a short, conventional prefix:

```text
feat/
fix/
docs/
test/
refactor/
ci/
chore/
perf/
build/
```

Keep pull requests focused. Separate generated artifacts, dependency updates,
and broad refactors from behavior changes when that makes review clearer.

## Development Rules

Prefer small, explicit changes that match the existing module boundaries.

- Use public plugin imports from `firecube.ingestor.api` and `firecube.core.api`.
- Do not add deep plugin imports into runtime or core internals.
- Do not call `fsspec.filesystem(...)` directly in write-domain code; use the
  storage abstractions and factories already in `src/firecube/core/filesystem/`.
- Keep `.firecube/` control-plane state behind `ChunkManager` and runtime-owned
  lifecycle helpers.
- Do not infer product names, storage type, write mode, or other run-critical
  settings from paths or URI shape when the CLI requires explicit flags.
- Validate config early and fail loudly instead of carrying ambiguous state into
  ingestion.

Read `plans/DESIGN.md` and `plans/STYLE.md` before changing storage, control
plane, CLI configuration, plugin contracts, observability, or Zarr write paths.

## Tests

Tests should prove user-visible behavior, persisted state, public contracts, or
important failure modes. Avoid tests that only mirror implementation details.

Use the smallest useful loop first:

```bash
uv run pytest tests/path_or_file.py -q
```

Run the main behavior lane before review:

```bash
uv run pytest --strict-deps -m "not slow and not s3" -q
```

Run the warning gate before merge when the change touches runtime behavior,
dependencies, or public APIs:

```bash
uv run pytest --strict-deps -q --tb=short -W error::DeprecationWarning
```

Follow `plans/TEST.md` for skip policy and `plans/TESTING_STANDARDS.md` for test
quality. Missing Python test dependencies must not be hidden with silent skips.
External binaries, platform-specific requirements, hardware-specific
requirements, and unavailable external services may be skipped with a precise
reason.

## Lint, Format, Types, And Security

The source-level CI gates are:

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run --with bandit bandit -c pyproject.toml -r src scripts -lll
```

Run `uv run ruff format .` to apply Python formatting before submitting.

## Documentation

Before editing public documentation, read `.prompts/docs-policy.md`.

Use the matching prompt in `.prompts/` for substantial documentation work:

- `.prompts/write-user-doc.md` for user tasks.
- `.prompts/write-plugin-doc.md` for plugin author guidance.
- `.prompts/write-operator-doc.md` for production operations.
- `.prompts/write-internal-doc.md` for contributor-only design or maintenance notes.

Public docs should give commands, expected outcomes, and recovery steps. Keep
implementation history, audit notes, and design rationale in `plans/` or an
explicitly internal page.

Build docs after documentation changes:

```bash
uv run mkdocs build --strict --site-dir /tmp/firecube-mkdocs-check
```

## Dependencies, Licenses, And SBOM

When changing dependencies, extras, build tooling, or dependency-license
evidence, update the SBOM artifacts and README tables as needed:

```bash
mkdir -p reports
uv export --format cyclonedx1.5 --all-groups --all-extras --output-file reports/sbom.cdx.json
uv run --isolated --all-groups --all-extras --with hatchling --with pip-licenses pip-licenses --format=json --with-urls > reports/dependency-licenses.json
```

Review `reports/dependency-licenses.json` for GPL, LGPL, AGPL, SSPL, unknown
runtime licenses, and missing license evidence. Do not rely on SBOM license
fields alone.

## Pull Request Checklist

Before opening a pull request:

```bash
git status --short
git diff --stat
git diff --check
uv run ruff check .
uv run ruff format --check .
uv run pyright
```

Then run the relevant focused tests and the CI lane affected by the change.

Use the pull request description to explain:

- what changed;
- why it changed;
- which user, operator, plugin, or maintainer behavior is affected;
- which checks you ran;
- any known follow-up work.

Do not include local virtual environments, caches, generated products, test data
outputs, logs, credentials, or build directories in a pull request.

## Sign Your Work

Sign every commit with your real name and a reachable email address. The
sign-off is a line at the end of the commit message certifying that you wrote
the contribution or otherwise have the right to submit it under this project's
license.

If your Git identity is configured, `git commit -s` adds the sign-off
automatically:

```bash
git config --global user.name "Jane Smith"
git config --global user.email "jane.smith@example.com"
git commit -s -m "fix: reject ambiguous storage configuration"
```

The commit log should include matching author and sign-off identity:

```text
Author: Jane Smith <jane.smith@example.com>
Date:   Thu Feb 2 11:41:15 2026 +0000

fix: reject ambiguous storage configuration

Signed-off-by: Jane Smith <jane.smith@example.com>
```

## AI-Assisted Contributions

AI tools may help draft or review a change, but the contributor remains
responsible for the submitted work. Before opening a pull request, review every
change yourself, remove unused scaffolding, run the relevant checks, and be ready
to explain the design, tests, and failure behavior.

If AI assistance materially shaped the change, disclose that in the pull request
description. Do not use generated responses as a substitute for engaging with
review comments yourself.

If AI assistance materially shaped a commit, record that with an `Assisted-by`
trailer in the commit message. Keep your own `Signed-off-by` line as the final
certification that you have the right to submit the work.

```bash
git commit -s -m "fix: reject ambiguous storage configuration

Assisted-by: GitHub Copilot <copilot@github.com>"
```

The resulting commit log should keep both trailers:

```text
Author: Jane Smith <jane.smith@example.com>
Date:   Thu Feb 2 11:41:15 2026 +0000

fix: reject ambiguous storage configuration

Assisted-by: GitHub Copilot <copilot@github.com>
Signed-off-by: Jane Smith <jane.smith@example.com>
```

## Commit Messages

Use clear conventional-commit-style subjects:

```text
feat: add direct write validation for static arrays
fix: reject ambiguous storage configuration
docs: update plugin migration instructions
test: cover staged resume failure state
ci: add container smoke gate
chore: regenerate SBOM artifacts
```

Each commit should represent one logical unit. If a review asks for changes, it
is fine to add follow-up commits while review is active; maintainers may squash
or ask for cleanup before merge.
