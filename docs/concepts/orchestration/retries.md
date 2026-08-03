# Retries And Recovery

External retries are normal. The external orchestrator decides whether to start
another command after a failure. Firecube checks ChunkManager before writing so
a retry does not blindly overwrite or overlap product state.

## Retry Flow

1. The external orchestrator starts `firecube ingest`.
2. If the command succeeds, the external orchestrator can continue to the next
   step.
3. If the command exits non-zero, the external orchestrator may retry according
   to its own policy.
4. On retry, Firecube checks ChunkManager before writing.
5. If a previous run is still active, a claim is stale, or the requested work
   conflicts with existing records, Firecube fails closed.

At that point, inspect the product before retrying again:

```bash
uv run firecube chunks runs list \
  --product-name file:///data/products/MY_PRODUCT.zarr
```

Use [Recover Runs And Claims](../../operations/chunk-manager/recover.md) when a
failed command leaves a stuck run or stale claim.

## Make Retry Intent Explicit

Use `resume_existing=true` when the retry should continue work that has already
started:

```bash
uv run firecube ingest <plugin> \
  --input-data /data/source \
  --target file:///data/products/MY_PRODUCT.zarr \
  --product-name MY_PRODUCT \
  --storage-type local \
  --storage-driver fsspec \
  --output-format zarr \
  --write-mode staged \
  --option resume_existing=true
```

Use force or deletion workflows only when the operation page tells you to. They
are recovery actions, not generic retry flags.

## Correlate Runs

Pass a stable `--option run_id=...` when you need Firecube run records to line up
with the external orchestrator job:

```bash
uv run firecube ingest <plugin> \
  --input-data /data/source \
  --target file:///data/products/MY_PRODUCT.zarr \
  --product-name MY_PRODUCT \
  --storage-type local \
  --storage-driver fsspec \
  --output-format zarr \
  --write-mode staged \
  --option run_id="$RUN_ID"
```

Metrics, logs, and traces have their own configuration. See
[Observability](../observability/index.md) for correlation details.

## Next Steps

- **[Recover Runs And Claims](../../operations/chunk-manager/recover.md)** — recover a stuck run or stale claim
- **[Inspect ChunkManager State](../../operations/chunk-manager/inspect.md)** — inspect runs, claims, spans, or snapshots
- **[Observability](../observability/index.md)** — connect metrics, logs, and traces to external orchestrator jobs
- **[Production Guide](../production-guide.md)** — choose reliable production defaults
