---
description: Audit Firecube edge cases across ingestion, storage, CLI, and docs
agent: build
---

# Improve Edge Case Handling

Perform a systematic edge-case audit across the requested Firecube area.

## Arguments

`$ARGUMENTS` — optional paths, modules, commands, or behavior to focus on.

## What To Look For

### Input And Data Edge Cases

- Empty sources, empty files, empty batches, empty groups.
- Single item, single timestamp, scalar-like arrays, one-row tables.
- Duplicate timestamps, out-of-order timestamps, gaps, and overlapping ranges.
- NaN, Inf, negative values, unexpected dtypes, missing columns, missing coordinates.
- Very large batch sizes, many groups, many source files, long names.
- Unicode paths, metadata keys, group names, and product names.

### CLI And Config Edge Cases

- Missing required flags.
- Empty string values for required options.
- Malformed `--option key=value` inputs.
- Unknown config keys and deprecated aliases.
- Conflicting flags such as dry-run plus destructive confirmation.
- Environment-derived slot ranges and invalid Kubernetes-style values.

### Storage And Filesystem Edge Cases

- Local paths with spaces, relative paths, and `file://` normalization.
- Read-only directories, missing parent directories, permission errors.
- Remote URI schemes with explicit `--storage-type` and `--storage-driver`.
- Partial uploads, failed deletions, stale claims, and retryable errors.
- Driver parity between fsspec and obstore where supported.

### Zarr, Parquet, And Parallel Ingestion Edge Cases

- Chunk boundary alignment.
- Slot ranges at start/end of product.
- Misaligned covered ranges and blocked ranges.
- Existing arrays with schema drift or size mismatch.
- Concurrent writers with overlapping and disjoint slots.
- Resume after failed, abandoned, or partially completed runs.

### Observability And Security Edge Cases

- Credentials in logs, errors, metrics, manifests, or telemetry labels.
- High-cardinality telemetry labels.
- JSON logging with progress output disabled.
- Trace shutdown and flush behavior on failure.

## Process

1. Scan the selected area and list likely edge cases.
2. Check whether each edge case has explicit behavior and tests.
3. For ambiguous behavior, ask the user to clarify intended semantics.
4. Fix missing handling only when asked to implement; otherwise report a plan.
5. Add or recommend tests for each accepted behavior.
6. Update public docs only when the behavior affects user action.
7. Run focused tests and summarize residual risk.

## Report Format

```text
[HIGH|MEDIUM|LOW] file.py:LINE — Edge case
Current behavior:
Expected behavior:
Test gap:
Recommended action:
```
