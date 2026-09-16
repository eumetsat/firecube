# Recover Runs And Claims

Use this page when ingestion failed, a pod was killed, or a writer left a
blocking claim behind.

## Set The Product

Set the full product URI and the logical product name used during ingestion:

```bash
PRODUCT_URI="file:///data/products/MY_PRODUCT.zarr"
PRODUCT_NAME="MY_PRODUCT"
```

Pass the full URI through `--product-name` for every ChunkManager command on
this page. This binds the command directly to the product without a storage
configuration.

## Inspect Runs

Start by listing runs:

```bash
firecube chunks runs list \
  --product-name "$PRODUCT_URI"
```

A stuck run usually appears as `started`:

```text
Run ID            Status   State   Parts Events
------------------------------------------------
docs-started-run  started  active  1     1
```

Confirm the original process is no longer active before abandoning the run.

If the run is already `failed`, go to
[Recover From A Failed Batch](#recover-from-a-failed-batch).

## Abandon A Stuck Run

Preview the operation:

```bash
firecube chunks runs abandon \
  --product-name "$PRODUCT_URI" \
  --run-id docs-started-run \
  --reason "process is no longer active" \
  --dry-run
```

Expected output:

```text
[dry-run] Would abandon run 'docs-started-run' for product 'MY_PRODUCT.zarr' (reason: process is no longer active)
```

Abandon the run in a non-interactive shell:

```bash
firecube chunks runs abandon \
  --product-name "$PRODUCT_URI" \
  --run-id docs-started-run \
  --reason "process is no longer active" \
  --yes-i-really-mean-it
```

Expected output:

```text
Abandoned run docs-started-run for MY_PRODUCT.zarr
```

Verify:

```bash
firecube chunks runs list \
  --product-name "$PRODUCT_URI"
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
firecube chunks claims list --product-name "$PRODUCT_URI"
```

Expected output when a claim exists:

```text
Product       State   Owner                 Domain
---------------------------------------------------------------------------
MY_PRODUCT.zarr  active  docs-started-run:F024 MY_PRODUCT:zarr_region:F024
```

The `Domain` value is the exact value to pass to `--domain`.

## Clear A Blocking Claim

Clear a stale claim in non-interactive context:

```bash
firecube chunks claims clear \
  --product-name "$PRODUCT_URI" \
  --domain MY_PRODUCT:zarr_region:F024 \
  --yes-i-really-mean-it
```

Use `--force` only after verifying no writer is active and the claim must be
cleared even though it does not look stale:

```bash
firecube chunks claims clear \
  --product-name "$PRODUCT_URI" \
  --domain MY_PRODUCT:zarr_region:F024 \
  --force \
  --yes-i-really-mean-it
```

Expected output:

```text
Cleared claim for MY_PRODUCT:zarr_region:F024
```

Verify:

```bash
firecube chunks claims list --product-name "$PRODUCT_URI"
```

Expected output:

```text
No claims found.
```

## Resume Ingestion

After the stuck run is abandoned and stale claims are cleared, rerun ingestion
with `--option resume_existing=true` so the batches that already succeeded are
kept:

```bash
firecube ingest <plugin> \
  --input-data /data/source \
  --target "$PRODUCT_URI" \
  --product-name "$PRODUCT_NAME" \
  --storage-type local \
  --storage-driver fsspec \
  --output-format zarr \
  --write-mode direct \
  --option resume_existing=true
```

## Recover From A Failed Batch

A run recorded as `failed` has already stopped and does not need to be
abandoned. Read the ingestion error and fix its cause before retrying.

For Zarr append ingestion with `--write-mode direct`, rerun the command in
[Resume Ingestion](#resume-ingestion), keeping the original plugin settings and
supplying all inputs needed to finish the run. With
`--option resume_existing=true`, successfully written timestamps are kept,
slots marked as failed are refilled, and new timestamps are appended.

Use `--option force_reingest=true` instead when previously written timestamps
also need replacement. Supply the corrected inputs for those timestamps;
Firecube overwrites them in place.

If a batch failed with `--write-mode staged`, rerun the original staged
command with `--option force_reingest=true` and all required inputs. The failed
run did not publish its staged data, so every batch must be written again.

After the retry, verify the new run:

```bash
firecube chunks runs list \
  --product-name "$PRODUCT_URI"
```

The new run should show `complete`. The earlier run remains `failed` in the
history.

Validate each affected Zarr group, replacing `data` with its group path:

```bash
firecube zarr validate \
  --product "$PRODUCT_URI" \
  --group data
```

Check that the JSON report has `is_valid: true` and the command exits 0. If it
exits 1, resolve the reported validation issues before using the product.

## Failure Recovery

| Symptom | Meaning | Recovery |
|---|---|---|
| `started` run blocks resume | Firecube cannot prove the old process is dead. | Verify the process is gone, then use `chunks runs abandon`. |
| Claim remains after crash | The writer did not release its claim. | Verify no writer is active, then use `chunks claims clear`. |
| Claim does not look stale | The heartbeat timestamp is still recent. | Use `--force` only if the writer is gone. |
| Run ends with `status=failed` and "later batch(es) were not attempted" | Append ingestion stopped before processing all inputs. | Fix the reported error, then [recover the failed batch](#recover-from-a-failed-batch). |
| Rerun refused: "spans from a failed run exist" | The retry needs an explicit choice about keeping earlier work. | Follow [Recover From A Failed Batch](#recover-from-a-failed-batch) for the original write mode. |
| Append refuses an insertion | An incoming timestamp is absent from the target but earlier than its latest timestamp. | Rebuild a new target from all required inputs in chronological order. Resume, force-reingest, and span deletion cannot enable insertion into the existing time coordinate. |

## Next Steps

- **[Inspect ChunkManager State](inspect.md)** — confirm current runs and claims
- **[Delete And Reingest](delete.md)** — remove a written span or range during recovery
