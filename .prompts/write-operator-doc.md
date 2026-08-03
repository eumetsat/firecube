---
description: Create or rewrite Firecube operator documentation
agent: build
---

# Write Operator Documentation

Use this prompt for production operations: S3, orchestration, observability,
parallel ingestion, cleanup, and recovery.

## Arguments

`$ARGUMENTS` — target doc path and optional operation description.

## Rules

- Apply `.prompts/docs-policy.md` first.
- Write for the person running Firecube, not the person maintaining internals.
- Include preflight, operation, verification, and failure recovery.
- Keep the mental model short and tied to operational decisions.
- Make remediation commands paste-runnable when possible.
- Assume operational competence and organize the page around the real-world
  outcome or problem, including relevant conditions and branches.

## Template

````markdown
# Operate X

## Purpose

State the operational goal and the environment this page assumes.

## Prerequisites

- Required Firecube version or plugin version.
- Required storage credentials.
- Required scheduler or runtime environment.

## Configuration

Show environment variables, config files, and CLI flags that matter for this
operation.

```bash
export FIRECUBE_...
```

```toml
[storage]
...
```

## Procedure

1. Run the preflight command:

   ```bash
   uv run firecube ...
   ```

2. Run the operation:

   ```bash
   uv run firecube ...
   ```

3. Verify the result:

   ```bash
   uv run firecube ...
   ```

## Failure Recovery

| Symptom | Meaning | Recovery |
|---------|---------|----------|
| `...` | `...` | `...` |

## Operational Notes

Keep this section short. Explain only the mental model an operator needs to make
safe decisions.
````
