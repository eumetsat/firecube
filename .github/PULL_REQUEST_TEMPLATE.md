<!--
Thanks for contributing to Firecube. Please read CONTRIBUTING.md first.
Keep pull requests focused: one logical change per PR.
-->

## What changed

<!-- A clear description of the change. -->

## Why

<!-- The motivation. Link the issue this closes, e.g. "Closes #123". -->

## Affected behavior

<!-- Which user, operator, plugin, or maintainer behavior changes? -->

- User / CLI:
- Plugin authors:
- Operators / production:
- Stored formats or control-plane state:

## Type of change

- [ ] Bug fix (non-breaking)
- [ ] New feature (non-breaking)
- [ ] Breaking change (CLI flags, plugin contract, config, or stored format)
- [ ] Documentation
- [ ] CI / build / chore

## Checks run

<!-- Tick what you ran; paste anything notable. -->

- [ ] `uv run ruff check .`
- [ ] `uv run ruff format --check .`
- [ ] `uv run pyright`
- [ ] `uv run --with bandit bandit -c pyproject.toml -r src scripts -lll`
- [ ] `uv run pytest --strict-deps -m "not slow and not s3" -q`
- [ ] Docs build (if docs changed): `uv run mkdocs build --strict`

## Follow-up work

<!-- Known gaps or planned follow-ups, or "none". -->

## Contributor certification

- [ ] Commits are signed off (`git commit -s`) with my real name and a reachable email.
- [ ] No credentials, generated products, caches, or virtualenvs are included in this PR.
- [ ] If AI assistance materially shaped this change, I disclosed it here and added an `Assisted-by` trailer.
