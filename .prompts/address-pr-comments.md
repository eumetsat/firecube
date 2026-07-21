---
description: Review and resolve unresolved pull request comments
agent: build
---

# Address Review Comments

Review and resolve all actionable feedback on a pull request.

## Arguments

Provide the MR/PR number or URL as `$ARGUMENTS`. If no argument is given, detect
the current branch's open review if possible.

## 1. Fetch Review Feedback

Use the available hosting CLI:

```bash
glab mr view "$ARGUMENTS" --comments
```

or:

```bash
gh pr view "$ARGUMENTS" --comments
gh pr diff "$ARGUMENTS"
```

If threaded inline comments are not visible, use the host API to fetch unresolved
threads/comments directly. Report if authentication or network access prevents
fetching review comments.

## 2. Analyze Each Comment

For each unresolved comment:

- Read the full file and surrounding context, not just the commented line.
- Identify whether the comment is about correctness, API design, tests, docs, security, performance, or style.
- Check the concern against Firecube's local rules in `AGENTS.md`, `plans/STYLE.md`, `plans/TEST.md`, and `.prompts/docs-policy.md` when documentation is involved.
- If the requested fix conflicts with repo architecture or user intent, propose the safer alternative with concrete reasoning.
- If the intent is ambiguous, ask for clarification before making risky changes.

## 3. Address Actionable Issues

For each actionable comment:

- Fix the underlying issue, not only the symptom.
- Update tests when the feedback identifies missing coverage or changed behavior.
- Update public docs when behavior, flags, config, or SDK surfaces change.
- Keep edits scoped to the reviewed change unless a small adjacent cleanup is required.

Do not resolve comments, commit, or push unless the user explicitly asks.

## 4. Verify

Run the smallest relevant verification first, then broaden as needed:

```bash
uv run ruff check .
uv run pyright
uv run pytest <focused tests>
uv run mkdocs build --strict --site-dir /tmp/firecube-mkdocs-check
```

For broad changes, run:

```bash
uv run pytest --strict-deps -q --tb=short
uv run pytest --strict-deps -q --tb=short -W error::DeprecationWarning
```

## 5. Report

Provide a mapping from each review comment to the action taken:

```text
Comment:
Action:
Verification:
Residual risk:
```

List unresolved comments separately with the blocker or question.
