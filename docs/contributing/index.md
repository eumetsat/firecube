# Contributing

Use this section when changing Firecube itself: the engine, core libraries,
CLI, tests, or documentation. Plugin development is documented separately
because external plugins use the public SDK and do not need engine internals.

## Common Paths

- **[Plugin Development](../guides/plugins/index.md)** — build an external plugin against the public SDK
- **[Firecube Contributors](firecube-contributors.md)** — understand layering,
  public/private boundaries, runtime areas, and contributor checks.

## Boundaries

Plugin code should import public symbols from `firecube.ingestor.api` and
`firecube.core.api`.

Firecube contributor work may touch internals, but it must preserve the public
SDK boundary, CLI contract, storage-driver boundary, ChunkManager behavior, and
observability boundary.

## Next Steps

- **[Plugin Development](../guides/plugins/index.md)** — choose what the plugin produces
- **[Firecube Contributors](firecube-contributors.md)** — internal contribution boundaries
- **[Plugin Templates](../reference/templates.md)** — standard plugin template surface
- **[Hooks & Lifecycle](../reference/hooks.md)** — custom pipeline surface
