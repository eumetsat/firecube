# Release

This file is the canonical rulebook for Firecube package versioning and release
checks. Release preparation lives in `.prompts/make-release.md`; local tagging
is guarded by `make release VERSION=<version>`.

## Version Scheme

Firecube package versions use PEP 440-compatible SemVer. Valid examples include
`0.1.1`, `1.0.0`, and `1.2.0rc1`.

`pyproject.toml` `[project].version` is the package-version source of truth.
Runtime code, docs examples, package metadata, and release artifacts must derive
from or agree with that value.

## Contract Versions

Contract/schema versions are separate namespaces from the Firecube package
version. They only change when that specific persisted format or machine-readable
output contract changes.

Current contract versions:

- `.firecube` control-plane schema: `v2`
- Zarr slot-plan output schema: `v1`
- Ingest result manifest schema: `v1`
- Legacy chunk manifest contract: `v1` (migration-only)
- Tensogram control-plane payload schema: `v1`
- Tensogram archive layout: `v1`

## Git Tags

Release tags use a leading `v`, for example `v0.1.1`.

Package metadata never includes the `v` prefix. For a tag pipeline,
`CI_COMMIT_TAG` should equal `v` plus the package version.

The GitHub project should protect `v*` tags so only maintainers can create
release tags.

Release tags should be created from a clean, up-to-date `main` checkout after
the release commit has been reviewed and merged:

```bash
git switch main
git pull --ff-only origin main
make release VERSION=0.1.1
```

The Makefile checks that `VERSION` matches `pyproject.toml`, the working tree is
clean, the current branch is `main`, local `main` matches `origin/main`, and the
release tag does not already exist.

## Pre-1.0 SemVer

While Firecube is in `0.y.z`, the SemVer pre-1.0 rule applies:

- MINOR bumps (`0.y`) may include breaking public-surface changes.
- PATCH bumps (`0.y.z`) must remain backward compatible.

Even before `1.0.0`, release notes should make user, plugin-author, and
operator consequences explicit.

## Public Surface

The SemVer compatibility surface includes:

- CLI commands, flags, exit behavior, and machine-readable output shapes.
- Plugin contract types and attributes, including `BaseIngestor`,
  `PipelineResult`, `OutputPaths`, `ResultMetrics`, `PRODUCT_NAME`, and
  `firecube_core_min_version`.
- Public Python imports from `firecube.ingestor.api` and `firecube.core.api`.
- `firecube.__version__` and `firecube.ingestor.__version__`.
- Config keys consumed by the CLI, runtime, and plugins.
- Manifest and control-plane schemas, which also carry their own schema versions.
- Documented telemetry fields.

Internal modules remain internal unless exported through those public surfaces.

## Tool Versions

Tool versions are not Firecube product versions.

- Python compatibility is controlled by `requires-python`, local interpreter
  setup, CI images, and container base images.
- uv, Ruff, pytest, MkDocs, and related tools are controlled by dependency
  constraints, CI images, and `uv.lock`.
- OCI image tags and labels describe a built artifact. They must not become a
  second source of truth for the Firecube package version.

## Release Checks

At minimum, a release must verify:

- `uv sync --locked --extra test --group docs`
- `uv run ruff check .`
- `uv run ruff format --check`
- `uv run pyright`
- `uv run pytest --strict-deps -q --tb=short -W error::DeprecationWarning`
- `uv run pytest --strict-deps -m "not slow and not s3" --cov=firecube --cov-report=term-missing --cov-report=xml -q`
- `uv run pytest tests/unit/test_version_consistency.py -q`
- `uv run mkdocs build --strict --site-dir /tmp/firecube-mkdocs-check`
- `uv build`
- `uv run --with twine twine check dist/*`

Container release builds should receive the bare package version as
`--build-arg PACKAGE_VERSION=<version>`. The image tag may be `v<version>`, but the OCI
`org.opencontainers.image.version` label should use the bare package version.

Before tagging, scan for stale hardcoded package-version references and audit
schema/contract version constants. Contract versions must change only when their
own persisted format or machine-readable output contract changes.

## Changelog

Maintain `CHANGELOG.md` using Keep a Changelog format.

Each release entry should be placed below `[Unreleased]` with a dated header,
for example `## [0.1.1] - 2026-06-10`. Draft it from `git log
<previous-tag>..HEAD --oneline`, grouped into Added, Changed, Fixed, Removed,
Migration Notes, and Stats sections as applicable.

The Stats section should use measured release-check output, including pytest
test counts, coverage percentage, docs build result, and package/container build
result when available. Do not invent missing stats.

## Tag Pipeline

Tag pipelines validate that `CI_COMMIT_TAG` equals `v` plus the package version.
They build, scan, patch, smoke-test, and push the container image tagged as
`v<version>`.

GitHub Releases are created manually from the matching `CHANGELOG.md` entry for
now. Include the pushed container image reference in the release notes.

## Tag Signing

Signed release tags are encouraged. CI does not require signed tags.
