# Build a Custom Pipeline Plugin

## Goal

Implement the lower-level batch contract when none of the three template
classes can represent the product. A direct `BaseIngestor` subclass owns batch
processing, output writes, coordination, and result construction.

This is an advanced extension point, not a more flexible default. Recheck the
[Plugin Development Overview](index.md) before choosing it.

## Before You Start

This guide covers a prerequisite check that the Generic template guides do
not, because a direct `BaseIngestor` subclass owns output writing itself
instead of delegating it to a runtime-managed writer.

Use this path only when the plugin already has a supported writer abstraction
for its output. Firecube does not currently expose a general, typed
storage-writer protocol for custom pipelines. A new custom pipeline therefore
cannot implement output writing entirely through stable public Firecube storage
APIs.

Do not work around that gap with imports from Firecube's internal storage
modules. If `GenericZarrIngestor`, `GenericParquetIngestor`, or
`DirectZarrIngestor` can represent the product, use that template and its
runtime-managed writer instead.

Optional processing extensions do not require a custom pipeline. For example,
`DuckDbMixin` can add a managed DuckDB connection to the generic Zarr and
Parquet templates. See [Plugin Extensions](extensions.md) before choosing
`BaseIngestor` only to add reusable processing behavior.

## Implement The Batch Boundary

Follow [Create a Plugin](create-a-plugin.md), select `base`, then
[install the plugin](install-a-plugin.md). Replace the generated
`_process_batch` stub:

```python
from typing import ClassVar

from firecube.ingestor.api import (
    BaseIngestor,
    PipelineBatch,
    PipelineResult,
    PluginContext,
    register_ingestor,
)


@register_ingestor("my_plugin")
class MyPlugin(BaseIngestor):
    PRODUCT_NAME: ClassVar[str] = "my_product"

    def _process_batch(
        self,
        batch: PipelineBatch,
        ctx: PluginContext,
    ) -> PipelineResult:
        ...
```

The implementation must write the batch through the plugin's supported writer
and return a `PipelineResult`. It also owns any coordination needed when more
than one worker can touch the same output domain. The raw base class does not
add the Zarr or Parquet templates' write claims around `_process_batch`.

Construct successful result paths with
`PipelineResult(outputs=OutputPaths(primary=...))`. The removed
`output_path=` constructor argument is not accepted. Use only the declared
fields of `ResultMetrics` when the plugin reports batch metrics.

See the
[Hooks & Lifecycle](../../reference/hooks.md) for the
exact result types and base-class members.

## Use Lifecycle Mixins Deliberately

This guide also states lifecycle-mixin ordering explicitly, unlike the
Generic template guides, because a custom `_process_batch()` owns its own
batch boundary and no template wraps mixin hooks around it automatically.

A custom `_process_batch()` owns its resource boundary. A lifecycle mixin such
as `DuckDbMixin` is not wrapped around that method automatically; call its
cooperative `batch_setup()` and `batch_teardown()` hooks explicitly and keep
teardown in a `finally` block. The mixin manages processing state only. It does
not provide an output writer or write coordination.

See [Plugin Extensions](extensions.md#custom-pipeline-lifecycle) for the call
shape and compatibility limits.

## Verify

After implementing the writer, run one representative batch with the output
format and write mode that the writer supports. A custom Zarr command has this
shape:

```bash
uv run firecube ingest my_plugin \
  --input-data ./path/to/input \
  --target file:///tmp/my_product.zarr \
  --product-name my_product \
  --storage-type local \
  --storage-driver fsspec \
  --output-format zarr \
  --write-mode direct
```

Verify the persisted artifact and its recorded output path, then repeat the
same input. When several workers can touch one output domain, also verify
conflict handling, partial-write cleanup, and recovery before deployment.

## Common Mistakes

| Mistake | Fix |
|---|---|
| Choosing `BaseIngestor` for a standard Zarr or Parquet product | Use the matching template and its smaller hook. |
| Importing a concrete Firecube storage session | Use a plugin-owned supported writer until Firecube exposes a stable public contract. |
| Using `output_path=` | Use `outputs=OutputPaths(primary=...)`. |
| Assuming template write claims apply | Implement coordination appropriate to the custom output. |

## Next Steps

- **[Plugin Extensions](extensions.md)** — add DuckDB or gridding capabilities
- **[Hooks & Lifecycle](../../reference/hooks.md)** — look up the batch and result contract
- **[Package and Register a Plugin](contract.md)** — verify the distribution entry point
- **[Install Your Plugin](install-a-plugin.md)** — verify discovery in the Firecube environment
