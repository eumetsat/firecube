# Fix A Chunk Ownership Refusal

Use this page when `firecube ingest` refuses a slot range because the range splits a stored chunk or shard. The example causes the refusal on a local store, then runs ranges that fit. For the reasons behind the refusal, see [Chunk Ownership](../concepts/output-formats/zarr/chunk-ownership.md).

The example takes about 5 minutes. It writes 200 small slots under your working directory.

## Prerequisites

The example uses a plugin from the Firecube test fixtures, not a plugin for real data. Work from a clone of the Firecube repository. Use bash. You need `jq`.

Install Firecube and activate the environment:

```bash
uv sync
source .venv/bin/activate
```

Install the test fixture plugin:

```bash
uv pip install -e tests/fixtures/firecube_test_plugins
```

Create an empty working directory and set two variables:

```bash
mkdir chunk-demo && cd chunk-demo
export TARGET="file://$PWD/demo.zarr"
export FLAGS="--target $TARGET --product-name direct_zarr_capable_test_product --storage-type local --storage-driver fsspec --write-mode direct"
```

The output below shows the path of the author's working directory. Your path is different.

## 1. Create A Store With Large Chunks

The plugin declares 1000 time slots. It stores 100 slots in each chunk. Create the empty store:

```bash
firecube zarr preallocate direct_zarr_capable_test_plugin $FLAGS
```

```text
resolved index: created
array data/data: created
array data/lat: created
array data/timestamp: created (regular coord materialization)
{
  "status": "ok",
  "plugin": "direct_zarr_capable_test_plugin",
  "product": "direct_zarr_capable_test_product",
  "target": "file:///data/chunk-demo/demo.zarr",
  "groups": {
    "data": 1000
  }
}
```

Read the chunk shape of the `data` array:

```bash
jq -c '.chunk_grid.configuration.chunk_shape' demo.zarr/data/data/zarr.json
```

```text
[100,10]
```

The first number is the time chunk. One chunk holds slots 0 to 99, the next holds slots 100 to 199, and so on.

## 2. Plan Ranges That Fit

Ask Firecube for slot ranges that fit the chunks:

```bash
firecube zarr slots direct_zarr_capable_test_plugin $FLAGS --slot-size 100 --format table
```

```text
group	slot_start	slot_end
data	0	100
data	100	200
data	200	300
data	300	400
data	400	500
data	500	600
data	600	700
data	700	800
data	800	900
data	900	1000
```

Each range is half-open. The range `[0, 100)` holds slots 0 to 99. Each range covers exactly one chunk.

## 3. Cause The Refusal With A Range That Splits A Chunk

Start a worker for slots 0 to 149. Another worker would take slots 150 to 199. Both workers would then write the chunk of slots 100 to 199.

```bash
firecube ingest direct_zarr_capable_test_plugin $FLAGS \
  --output-format zarr \
  --option pipeline_workers=1 \
  --slot-start 0 \
  --slot-end 150 \
  2> refusal.log
echo $?
grep '^Error' refusal.log
```

```text
1
Error: Group 'data' chunk_shapes=[(100, 10)]; slot range [0, 150) misaligned. Suggested aligned range: [0, 200)
```

Firecube raised a `ConfigurationError` and the command exited with 1. The refusal comes before the worker writes. The log file also holds a JSON warning with the same text.

Check that the store holds no chunk:

```bash
find demo.zarr/data/data -type f | sort
```

```text
demo.zarr/data/data/zarr.json
```

Only the array metadata exists. No worker wrote data.

## 4. Run The Ranges That Fit

Run the first two planned ranges. Each command starts one worker:

```bash
firecube ingest direct_zarr_capable_test_plugin $FLAGS \
  --output-format zarr \
  --option pipeline_workers=1 \
  --slot-start 0 \
  --slot-end 100 \
  2> /dev/null | jq '{files_processed: .metrics.pipeline.files_processed, batches_failed: .metrics.pipeline.batches_failed}'
```

```text
{
  "files_processed": 100,
  "batches_failed": 0
}
```

```bash
firecube ingest direct_zarr_capable_test_plugin $FLAGS \
  --output-format zarr \
  --option pipeline_workers=1 \
  --slot-start 100 \
  --slot-end 200 \
  2> /dev/null | jq '{files_processed: .metrics.pipeline.files_processed, batches_failed: .metrics.pipeline.batches_failed}'
```

```text
{
  "files_processed": 100,
  "batches_failed": 0
}
```

Both workers succeeded. Check the store:

```bash
find demo.zarr/data/data -type f | sort
```

```text
demo.zarr/data/data/c/0/0
demo.zarr/data/data/c/1/0
demo.zarr/data/data/zarr.json
```

Each worker wrote its own chunk object. Ask for the plan again:

```bash
firecube zarr slots direct_zarr_capable_test_plugin $FLAGS --slot-size 100 --format table | head -3
```

```text
group	slot_start	slot_end
data	200	300
data	300	400
```

The plan skips the ranges that completed.

## Next Steps

- **[Chunk Ownership](../concepts/output-formats/zarr/chunk-ownership.md)** - understand why Firecube refuses these ranges
- **[Run Parallel Zarr Writes](parallel-zarr-writes.md)** - run workers against your own product
- **[Chunking](../guides/plugins/configure-zarr-chunking.md)** - choose a chunk layout for your plugin
