---
description: Prepare a Firecube release with checks, version update, and publishing notes
agent: build
---

# Make Release

Prepare a new Firecube release.

## Arguments

Provide the target package version as `$ARGUMENTS`, for example `/make-release 0.2.0`.
If no version is given, ask the user for the intended version. Do not infer a
major version bump. The package version is bare PEP 440-compatible SemVer; the
git tag is `v<version>`.

## Pre-Release Checks

Run all relevant checks. If any required step fails, stop and report the failure.

### 1. Clean Working Tree

```bash
git status --short
git diff --stat
```

The release should start from a reviewed, intentional state. Do not discard or
overwrite user changes.

### 2. Dependencies And Fixtures

```bash
uv sync --locked --extra test --group docs
uv pip install -e tests/fixtures/cli_test_plugin
uv pip install -e tests/fixtures/direct_zarr_capable_test_plugin
uv pip install -e tests/fixtures/direct_zarr_non_capable_test_plugin
uv pip install -e tests/fixtures/multi_group_capable_test_plugin
uv pip install -e tests/fixtures/cf_time_dim_test_plugin
```

### 3. Lint And Tests

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest --strict-deps -q --tb=short -W error::DeprecationWarning
uv run pytest --strict-deps -m "not slow and not s3" --cov=firecube --cov-report=term-missing --cov-report=xml -q
uv run pytest tests/unit/test_version_consistency.py -q
```

If the full suite requires unavailable credentials or external services, run the
publishable local suite and report what was skipped.

### 4. Documentation

```bash
uv run mkdocs build --strict --site-dir /tmp/firecube-mkdocs-check
```

Run `/doc-fact-check` on changed or high-traffic docs before publishing.

### 5. Package Build

```bash
rm -rf dist
uv build
uv run --with twine twine check dist/*
```

Inspect the generated wheel/sdist filenames and contents before publishing.

### 6. Version And Contract Audit

Check for stale hardcoded package versions after the version bump. If there is a
previous release tag, use its package version as the value to search for:

```bash
PREVIOUS_TAG="$(git describe --tags --abbrev=0 --match 'v[0-9]*' 2>/dev/null || true)"
if [ -n "$PREVIOUS_TAG" ]; then
  PREVIOUS_VERSION="$(git show "${PREVIOUS_TAG}:pyproject.toml" | awk -F'"' '
    /^\[project\]$/ { in_project=1; next }
    /^\[/ { in_project=0 }
    in_project && /^version = / { print $2; exit }
  ')"
  if [ -n "$PREVIOUS_VERSION" ]; then
    rg -n "${PREVIOUS_VERSION}" \
      --glob '!uv.lock' \
      --glob '!CHANGELOG.md' \
      --glob '!dist/**' \
      --glob '!site/**' \
      . || true
  fi
fi
```

Any remaining old package-version hits must be intentional historical references,
not runtime code, docs examples, CI metadata, or release artifacts.

Audit contract/schema versions separately:

```bash
rg -n "schema_version|SCHEMA_VERSION|ARCHIVE_VERSION|PLAN_SCHEMA_VERSION|CURRENT_SCHEMA_VERSION" src tests docs plans
```

Do not change schema or contract versions just because the package version
changed. Change them only when the persisted format or machine-readable output
contract changes, and update `plans/RELEASE.md` if the contract map changes.

## Release Process

### 1. Version Update

Update `pyproject.toml` `[project].version` to the target version and refresh
`uv.lock`.

Do not hardcode the package version in runtime code or public docs. Runtime
version exports and docs examples should derive from the package version.

Update `CHANGELOG.md`.

If `CHANGELOG.md` does not exist, create it using Keep a Changelog format with
an `[Unreleased]` section.

### 2. Changelog Entry

Write a `CHANGELOG.md` entry for the target version following the existing Keep
a Changelog format. Use a dated header:

```markdown
## [X.Y.Z] - YYYY-MM-DD
```

Insert the entry below `[Unreleased]`. Do not leave this as a manual follow-up
unless the user explicitly asks to skip changelog editing.

Find the previous release tag:

```bash
git describe --tags --abbrev=0 --match 'v[0-9]*' 2>/dev/null || true
```

Build the draft from commits since the previous tag. If there is no previous
tag, use all reachable commits:

```bash
PREVIOUS_TAG="$(git describe --tags --abbrev=0 --match 'v[0-9]*' 2>/dev/null || true)"
if [ -n "$PREVIOUS_TAG" ]; then
  git log --oneline "${PREVIOUS_TAG}..HEAD"
else
  git log --oneline
fi
```

Organize into:

- Added
- Changed
- Fixed
- Removed
- Migration Notes
- Stats

Use the section only when it has real content. Keep user-facing behavior first.
Put internal refactors only when they affect users, plugin authors, operators,
release artifacts, or public contracts.

For `Stats`, use the current release checks and CI output. Include:

- test count summary, for example `pytest: 1234 passed`
- type-check result, for example `pyright: 0 errors`
- coverage percentage from the pytest coverage `TOTAL` line
- docs build result
- package/container build result, when run

Do not invent stats. If a stat is unavailable, write the check result and why the
number is unavailable.

### 3. Release Notes

Prepare release notes from the changelog entry. Keep them shorter than the
changelog and focused on user, plugin-author, and operator consequences.

### 4. Commit Release Edits

Only after user approval:

```bash
git add pyproject.toml uv.lock CHANGELOG.md docs README.md
git commit -m "chore: release X.Y.Z"
```

Do not tag from a feature branch. The release commit should be reviewed and
merged to the default branch first.

Do not push without explicit user approval.

### 5. Merge

Push the release branch and open a pull request. Let branch CI pass, review the
release diff, and merge it to `main`.

### 6. Tag And Publish

After the release commit is merged to `main`, tag from a clean, up-to-date local
`main` checkout:

```bash
git switch main
git pull --ff-only origin main
make release VERSION=X.Y.Z
```

`make release` creates and pushes the annotated tag `vX.Y.Z` only when
`VERSION` matches `pyproject.toml`, the working tree is clean, and local `main`
matches `origin/main`.

The tag pipeline should run validation, container build, scan, patch, smoke
test, and push jobs. Watch the tag pipeline and confirm:

- `version_check` accepts `CI_COMMIT_TAG == v<pyproject version>`
- the container image is pushed as `:vX.Y.Z`
- the image label `org.opencontainers.image.version` is the bare package version

Create the GitHub Release manually from the matching `CHANGELOG.md` entry and
include the pushed container image reference.

For package smoke testing in a clean environment:

```bash
uv venv /tmp/firecube-release-smoke
/tmp/firecube-release-smoke/bin/python -m pip install dist/firecube-X.Y.Z-py3-none-any.whl
/tmp/firecube-release-smoke/bin/firecube --help
```

If publishing to a package index, wait for propagation and test install from the
published artifact.

## Report

Return:

- version
- checks run and result
- files changed
- release notes draft
- remaining manual steps
- whether anything was skipped and why
