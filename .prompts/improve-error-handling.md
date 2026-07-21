---
description: Audit Firecube error paths and make failures actionable
agent: build
---

# Improve Error Handling

Perform a comprehensive error-handling audit across the requested Firecube area.

## Arguments

`$ARGUMENTS` — optional paths, modules, commands, or behavior to focus on.

## Checks

### Python Runtime

- No bare `except:` or broad `except Exception` without a deliberate boundary.
- Exceptions preserve the original cause with `raise ... from exc` when wrapping.
- Error messages include enough context: product, group, run id, URI, slot range, batch id, config key, or plugin name.
- Errors do not leak credentials or secrets.
- Retryable/transient errors are not reported as successful no-ops.
- Cleanup failures are logged or surfaced according to severity.

### CLI

- Required flags fail early with actionable messages.
- Invalid choices name the bad value and valid alternatives.
- Destructive commands require explicit confirmation or dry-run.
- Recovery guidance uses real, paste-runnable commands.
- JSON output is not polluted by progress logs when machine-readable output is expected.

### Plugin Contract

- Plugin author mistakes produce `ConfigurationError` or another intentional SDK exception.
- Private runtime errors are not exposed as cryptic stack traces when user action can fix them.
- Validation errors name the plugin and public hook or option involved.

### Storage And Control Plane

- URI, credential, permission, and driver errors keep the useful original message.
- Resume/conflict errors identify the run, product, group, and recovery command.
- Deletion and archive errors distinguish dry-run, partial failure, and completed operations.
- WAL/control-plane corruption errors fail closed where silent fallback would risk data loss.

### Observability

- Logging, metrics, and telemetry failures do not crash ingestion unless configured as critical.
- Credential redaction applies to logs, telemetry metadata, manifests, and exception text.
- Trace/metric flush failures are visible but do not hide the original ingestion error.

## Process

1. Search for broad catches, swallowed exceptions, string-only error handling, and non-actionable messages:

   ```bash
   rg -n "except:|except Exception|raise .+Error\\(|ClickException|UsageError|suppress\\(|pass$" src/firecube tests
   ```

2. Inspect each hit manually; greps are candidates, not automatic failures.
3. Classify each issue: crash, silent failure, poor message, leaked secret, wrong exception type, missing test, or docs drift.
4. Fix only if the user requested implementation; otherwise produce a plan.
5. Add focused tests for changed error paths.
6. Run focused tests, then `uv run ruff check .` and `uv run pyright`.

## Report Format

```text
[HIGH|MEDIUM|LOW] file.py:LINE — Error handling issue
Current behavior:
Risk:
Recommended fix:
Test to add:
```
