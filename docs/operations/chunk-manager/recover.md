# Recover Runs And Claims

Use this page when ingestion crashed, a pod was killed, or a writer left a
blocking claim behind.

```bash
PRODUCT_URI="file:///data/products/MY_PRODUCT.zarr"
PRODUCT_NAME="MY_PRODUCT"
```

## Inspect Runs

Start by listing runs:

```bash
firecube chunks runs list --product-name "$PRODUCT_NAME"
```

A stuck run usually appears as `started`:

```text
Run ID            Status   State   Parts Events
------------------------------------------------
docs-started-run  started  active  1     1
```

Confirm the original process is no longer active before abandoning the run.

## Abandon A Stuck Run

Preview the operation:

```bash
firecube chunks runs abandon \
  --product-name "$PRODUCT_NAME" \
  --run-id docs-started-run \
  --reason "process is no longer active" \
  --dry-run
```

Expected output:

```text
[dry-run] Would abandon run 'docs-started-run' for product 'product.zarr' (reason: process is no longer active)
```

Abandon the run in a non-interactive shell:

```bash
firecube chunks runs abandon \
  --product-name "$PRODUCT_NAME" \
  --run-id docs-started-run \
  --reason "process is no longer active" \
  --yes-i-really-mean-it
```

Expected output:

```text
Abandoned run docs-started-run for product.zarr
```

Verify:

```bash
firecube chunks runs list --product-name "$PRODUCT_NAME"
```

Expected output:

```text
Run ID            Status     State   Parts Events
--------------------------------------------------
docs-started-run  abandoned  active  2     2
```

## Inspect Claims

List claims before clearing anything:

```bash
firecube chunks claims list --product-name "$PRODUCT_NAME"
```

Expected output when a claim exists:

```text
Product       State   Owner                 Domain
---------------------------------------------------------------------------
product.zarr  active  docs-started-run:F024 product.zarr:zarr_region:F024
```

The `Domain` value is the exact value to pass to `--domain`.

## Clear A Blocking Claim

Clear a stale claim in non-interactive context:

```bash
firecube chunks claims clear \
  --product-name "$PRODUCT_NAME" \
  --domain product.zarr:zarr_region:F024 \
  --yes-i-really-mean-it
```

Use `--force` only after verifying no writer is active and the claim must be
cleared even though it does not look stale:

```bash
firecube chunks claims clear \
  --product-name "$PRODUCT_NAME" \
  --domain product.zarr:zarr_region:F024 \
  --force \
  --yes-i-really-mean-it
```

Expected output:

```text
Cleared claim for product.zarr:zarr_region:F024
```

Verify:

```bash
firecube chunks claims list --product-name "$PRODUCT_NAME"
```

Expected output:

```text
No claims found.
```

## Resume Ingestion

After the stuck run is abandoned and stale claims are cleared, rerun ingestion
with resume enabled when that is the desired behavior:

```bash
firecube ingest <plugin> \
  --source /data/source \
  --target "$PRODUCT_URI" \
  --product-name MY_PRODUCT \
  --storage-type local \
  --storage-driver fsspec \
  --output-format zarr \
  --write-mode direct \
  --option resume_existing=true
```

## Failure Recovery

| Symptom | Meaning | Recovery |
|---|---|---|
| `started` run blocks resume | Firecube cannot prove the old process is dead. | Verify the process is gone, then use `chunks runs abandon`. |
| Claim remains after crash | The writer did not release its claim. | Verify no writer is active, then use `chunks claims clear`. |
| Claim does not look stale | The heartbeat timestamp is still recent. | Use `--force` only if the writer is gone. |

## Next Steps

- **[Inspect ChunkManager State](inspect.md)** — confirm current runs and claims
- **[Delete And Reingest](delete.md)** — remove a written span or range during recovery
