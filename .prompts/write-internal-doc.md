---
description: Create or rewrite Firecube internal architecture documentation
agent: build
---

# Write Internal Documentation

Use this prompt for architecture, design rationale, maintenance policy, and other
contributor-only material.

## Arguments

`$ARGUMENTS` — target doc path and optional internal topic.

## Rules

- Apply `.prompts/docs-policy.md` first.
- Mark the page as internal.
- Keep implementation rationale here, not in user task pages.
- Include verification commands or source evidence when making technical claims.
- Prefer `plans/` for historical evidence and project-management notes.

## Template

````markdown
# Internal: X

Audience: Firecube contributors and maintainers only.

This page is not required to install Firecube, run ingestion, write a normal
plugin, or operate a production job.

## Context

State the internal problem, decision, or subsystem being documented.

## Current Behavior

Describe the behavior as it exists now. Link to public docs only if users need
to understand the outcome.

## Design Constraints

- Constraint 1.
- Constraint 2.

## Decision

Record the chosen approach and the specific tradeoffs accepted.

## Alternatives Considered

| Alternative | Why Not |
|-------------|---------|
| `...` | `...` |

## Verification

List tests, commands, or review checks that prove the behavior.

```bash
uv run pyright
uv run pytest ...
```

## Follow-Ups

- Open work item.
````
