# Contributing

Use this section when you want to extend Firecube, either by writing a plugin or
by changing Firecube itself.

Most contributors should start from the path they are working on:

- **Plugin authors** write product-specific code against the public SDK. They
  do not need engine internals.
- **Firecube contributors** change the engine, core libraries, CLI, tests, or
  documentation. They need the project boundaries and verification rules.

## Common Paths

- **[Plugin Authors](plugin-authors.md)** — understand what the engine does
  around plugin hooks and what plugin code must return.
- **[Firecube Contributors](firecube-contributors.md)** — understand layering,
  public/private boundaries, runtime areas, and contributor checks.

## Boundaries

Plugin code should import public symbols from `firecube.ingestor.api` and
`firecube.core.api`.

Firecube contributor work may touch internals, but it must preserve the public
SDK boundary, CLI contract, storage-driver boundary, ChunkManager behavior, and
observability boundary.

## Next Steps

- **[Plugin Authors](plugin-authors.md)** — plugin-facing runtime model
- **[Firecube Contributors](firecube-contributors.md)** — internal contribution boundaries
- **[Plugins](../concepts/plugins/index.md)** — choose a plugin base class
- **[API Reference](../reference/api.md)** — public SDK surface
