---
description: Create or rewrite a user-facing Firecube how-to page
agent: build
---

# Write User Documentation

Use this prompt for public user tasks: installing, configuring, running, checking,
or recovering from one concrete Firecube workflow.

## Arguments

`$ARGUMENTS` — target doc path and optional task description.

## Rules

- Apply `.prompts/docs-policy.md` first.
- Write for users, not maintainers.
- Start with the task and expected outcome.
- Prefer commands, expected output, verification, and troubleshooting.
- Avoid internal service names, phase history, line numbers, source paths, commit labels, and `.sisyphus/` or `plans/` references.

## Template

````markdown
# Do X

## When To Use This

One or two sentences describing the user goal and when this page applies.

## Prerequisites

- Firecube installed.
- Required plugin installed.
- Required input data or credentials available.

## Steps

1. Run the command:

   ```bash
   uv run firecube ...
   ```

2. Inspect the output:

   ```bash
   uv run firecube ...
   ```

## Verify

Show the smallest command or code snippet that proves the task worked.

```bash
uv run firecube ...
```

Expected result:

```text
...
```

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| Command fails with `...` | Missing required input | Run `...` |

## Next Steps

Link to the next user-facing page. Avoid linking to architecture unless it helps
the reader choose a concrete action.
````
