# Plugin Contract

## Goal

Every Firecube plugin must satisfy a small set of rules that the engine enforces
at class-definition time and at runtime. This page covers those rules: what you
must declare, how to return results, which imports are allowed, and how the engine
resolves the product name. Get these right and the engine handles idempotency,
resume safety, telemetry, and storage for you.

## Minimal Example

```python
from typing import ClassVar

from firecube.ingestor.api import (
    GenericZarrIngestor,
    OutputPaths,
    PipelineResult,
    PluginContext,
    ResultMetrics,
    register_ingestor,
)


@register_ingestor("my_plugin")
class MyPlugin(GenericZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "my_product"

    def build_dataset(self, group: str, items: list[object], ctx: PluginContext):
        ...

    def _aggregate_metrics(self, ctx, state) -> dict:
        n = sum(b.record_count for b in state.completed_batches)
        return PipelineResult(
            outputs=OutputPaths(primary=str(ctx.target)),
            metrics=ResultMetrics(records_written=n),
        )
```

Key points:

- `PRODUCT_NAME` is a class-level string, not an instance attribute.
- `PipelineResult` takes `outputs=OutputPaths(...)`, not `output_path=`.
- `ResultMetrics` is a typed container, not a plain `dict`.
- All imports come from `firecube.ingestor.api`.

## Required API

| API | Required | Purpose |
|-----|----------|---------|
| `PRODUCT_NAME: ClassVar[str]` | Yes | Logical product identity; feeds ChunkManager bookkeeping, telemetry labels, and `--product-name` |
| `PipelineResult` | Yes | Typed return value from every hook that produces output |
| `OutputPaths` | Yes | Carries the primary output URI and any secondary paths; passed as `outputs=` to `PipelineResult` |
| `ResultMetrics` | Yes | Typed dict for plugin-level counters; passed as `metrics=` to `PipelineResult` |
| `register_ingestor` | Yes | Registers the class under a plugin name so the engine can discover it |
| `PluginContext` | Yes | Read-only context passed to every hook; provides `ctx.storage`, `ctx.telemetry`, `ctx.options` |
| `CatalogGroupInfo` | No | Optional return type for catalog group annotations |

### `PRODUCT_NAME`

Every concrete `BaseIngestor` subclass must declare `PRODUCT_NAME` as a
class-level string:

```python
from typing import ClassVar
from firecube.ingestor.api import GenericZarrIngestor

class MyIngestor(GenericZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "my_product"
```

The engine enforces this at class-definition time via `__init_subclass__`. You
get a clear error at import, not at runtime. Abstract templates like
`GenericZarrIngestor` itself are exempt; only concrete subclasses must declare it.

**Product name precedence:**

```
CLI --product-name  >  config default_product_name  >  plugin PRODUCT_NAME  >  hard fail
```

The CLI flag always wins. If none of the three sources provides a name, the run
fails immediately with a clear error.

### Typed results

Return `PipelineResult` with typed containers. Never use `output_path=` or plain
dicts:

```python
from firecube.ingestor.api import PipelineResult, OutputPaths, ResultMetrics

return PipelineResult(
    outputs=OutputPaths(primary=ctx.storage.output.uri),
    metrics=ResultMetrics(records_written=n),
)
```

`OutputPaths` carries the primary output URI and any secondary paths.
`ResultMetrics` holds plugin-level counters. The engine merges its own storage
metrics after your hook returns, so don't set `metrics["storage"]` yourself.

### Public imports only

Import only from `firecube.ingestor.api` and `firecube.core.api`. Deep imports
into engine internals break across releases and are not covered by the public API
contract.

```python
# Correct
from firecube.ingestor.api import GenericZarrIngestor, OutputPaths, PipelineResult

# Wrong — private internal module, not covered by the contract
from firecube.ingestor._internal.engine import SomeInternalClass
```

### Catalog annotations (optional)

`firecube catalog intake` discovers groups in the product store and creates the
catalog entries. A plugin can rename, describe, annotate, or hide one discovered
group by overriding `catalog_group_info(...)`:

```python
from typing import Any, ClassVar

from firecube.ingestor.api import CatalogGroupInfo, GenericZarrIngestor, register_ingestor


@register_ingestor("my_plugin")
class MyPlugin(GenericZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "my_product"

    def catalog_group_info(
        self,
        group: str,
        store_uri: str,
        storage_config: Any | None = None,
    ) -> CatalogGroupInfo | None:
        if group.startswith("_"):
            return CatalogGroupInfo(include=False)
        return CatalogGroupInfo(
            name=f"my_product_{group}",
            description=f"My product group {group}",
            metadata={"group": group},
        )
```

Return `None` when the default catalog entry is fine. Return
`CatalogGroupInfo(include=False)` to hide a group.

## Configuration

Plugins are discovered via an entry point in `pyproject.toml`:

```toml
[project.entry-points."firecube.plugins"]
firecube_my_plugin = "firecube_my_plugin"
```

After `uv pip install -e .`, the plugin is available as
`firecube ingest firecube_my_plugin`.

To run ingestion, all five flags are required (none are inferred):

```bash
uv run firecube ingest my_plugin \
  --source /data/raw \
  --target /data/output/my_product.zarr \
  --product-name my_product \
  --storage-type local \
  --storage-driver fsspec \
  --write-mode direct
```

Plugin-specific options go in `~/.config/firecube/config.toml`:

```toml
[plugins.my_plugin]
my_option = "value"
```

Inspect all effective options with:

```bash
uv run firecube ingest my_plugin --show-options
```

## Test It

Run the plugin against a small local fixture to confirm the contract is satisfied:

```bash
uv run firecube ingest my_plugin \
  --source tests/fixtures/sample_data \
  --target /tmp/my_product_test.zarr \
  --product-name my_product \
  --storage-type local \
  --storage-driver fsspec \
  --write-mode direct
```

Check that the run completes without errors and the output exists:

```bash
ls /tmp/my_product_test.zarr
```

For automated testing, use `pytest` with the `firecube` test fixtures:

```bash
uv run pytest tests/ -m "not slow and not s3"
```

## Common Mistakes

| Mistake | Fix |
|---------|-----|
| `PipelineResult(output_path=...)` | Use `PipelineResult(outputs=OutputPaths(primary=...))` |
| `metrics={"records_written": n}` (plain dict) | Use `metrics=ResultMetrics(records_written=n)` |
| Missing `PRODUCT_NAME` on a concrete class | Add `PRODUCT_NAME: ClassVar[str] = "my_product"` |
| `PRODUCT_NAME` as an instance attribute | Declare it at class level with `ClassVar[str]` |
| Importing from internal engine modules | Import from `firecube.ingestor.api` instead |
| Setting `metrics["storage"]` in your hook | The engine sets storage metrics; don't override them |
| Constructing `fsspec.filesystem(...)` directly | Use `ctx.storage.output` — the engine binds the right backend |
| Omitting `--storage-type` or `--write-mode` | All five CLI flags are required; none are inferred |

## Next Steps

- **[Create a Plugin](create-a-plugin.md)** — scaffold walkthrough and entry-point setup
- **[BaseIngestor](base-ingestor.md)** — hook signatures and lifecycle details
- **[Public API Reference](../../reference/api.md)** — full symbol reference for `firecube.ingestor.api`
