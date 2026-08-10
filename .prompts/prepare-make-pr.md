---
description: Run Firecube preflight checks and prepare a pull request
agent: build
---

# Prepare And Create Review Request

Final preparation workflow before opening a pull request.

## Arguments

`$ARGUMENTS` — optional branch name, title, or related issue.

## Preflight Checks

Run the relevant checks. If any required step fails, stop and report the failure.

### 1. Worktree Review

```bash
git status --short
git diff --stat
git diff --check
```

Confirm unrelated user changes are not staged or modified by this work.

### 2. Lint

```bash
uv run ruff check .
```

### 3. Type Check

```bash
uv run pyright
```

### 4. Tests

Run focused tests for the change first:

```bash
uv run pytest <focused tests>
```

For broad runtime, storage, or plugin-contract changes:

```bash
uv sync --extra test
uv pip install -e tests/fixtures/cli_test_plugin
uv pip install -e tests/fixtures/direct_zarr_capable_test_plugin
uv pip install -e tests/fixtures/direct_zarr_non_capable_test_plugin
uv pip install -e tests/fixtures/multi_group_capable_test_plugin
uv pip install -e tests/fixtures/cf_time_dim_test_plugin
uv run pytest --strict-deps -q --tb=short
uv run pytest --strict-deps -q --tb=short -W error::DeprecationWarning
```

### 5. Documentation

If docs changed:

```bash
uv sync --group docs
uv run mkdocs build --strict --site-dir /tmp/firecube-mkdocs-check
```

Apply `.prompts/docs-policy.md` to public docs before opening review.

## Review Request Preparation

If all required checks pass:

1. Review files to include.
2. Do not stage build artifacts, `.venv/`, `site/`, local data outputs, credentials, logs, session-local workspace directories, or generated products.
3. Stage only source, tests, docs, config, and prompt files related to the change.
4. Use a clear conventional-commit-style message if committing.
5. Do not push or open a remote review request without explicit user approval.

## Review Body

Use this structure:

```markdown
## Summary
- ...

## Verification
- `uv run ruff check .`
- `uv run pyright`
- `uv run pytest ...`
- `uv run pytest --strict-deps -q --tb=short -W error::DeprecationWarning`
- `uv run mkdocs build --strict --site-dir /tmp/firecube-mkdocs-check`

## Notes
- ...
```

Report the final branch state and verification results.
