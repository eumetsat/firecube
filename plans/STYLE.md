# Style

Engineering and layer principles. The doctrine (architectural invariants) lives in DESIGN.md; this file enumerates the rules and the concrete forbidden patterns.

Anti-patterns are named so reviewers can cite them by name in PR comments without re-explaining the rationale each time.

## Named Anti-Patterns (Forbidden)

These patterns have caused real bugs or maintenance pain. If a change introduces any of them, it must be rejected at review:

- **BASENAME heuristics**: CLI guesses `output_name` from the target path basename when `--product-name` is not provided. Forbidden because implicit naming breaks idempotency and makes runs non-reproducible. Always require `--product-name` explicitly.

- **Magic Output Detection**: `main.py` inspects plugin result dicts for specific keys to resolve local paths. Forbidden because it couples the engine to plugin internals. Plugin results must use `PipelineResult(outputs=OutputPaths(primary=...))`. No key sniffing.

- **Free-form option overload**: Passing common engine or template keys through `--option` instead of typed flags. The `--option` escape hatch exists for experimental or plugin-specific knobs only. Promote any key used in more than one plugin to a typed config field and warn when a typed key is passed via free-form mode.

- **Automatic Env Resolution**: Resolving `${VAR}` placeholders in all config strings unconditionally. Forbidden because it prevents literal `${...}` values and makes config behavior environment-dependent. Env expansion must be opt-in and scoped.

- **Database Leakage**: A plugin (e.g. MSG FRM) pulling global `[database.duckdb]` settings implicitly from the top-level config. Forbidden because it creates invisible coupling between unrelated config sections. Plugin config must be self-contained under its own namespace.

- **Option Aliases**: Dual-naming the same Zarr config key (e.g. `zarr_chunk` vs `chunk_shape`). Forbidden because it creates confusion about which name is canonical and which is deprecated. Pick one name, document it, reject the other at parse time.

  **Note**: internal or verified-zero-caller dual hook names for the same concern (e.g. `groups_for_items` and `get_batch_groups`) are this same anti-pattern at the API surface. When unification is required, the canonical name MUST be declared and the duplicate MUST be DELETED in the same change — no `DeprecationWarning`, no back-compat shim, no transitional period. Carrying two names "for safety" is the anti-pattern this rule exists to prevent.

- **Regex Guessing**: Extracting horizons from filenames via regex or discovering groups by listing `F*` folders. Forbidden because it ties the plugin to a specific naming convention that can silently break. Require explicit configuration for horizon and group discovery.

- **Hardcoded Defaults**: Magic numbers for regrid spacing, lat/lon soft limits, or similar domain constants embedded in source code. Forbidden because they're invisible to operators and impossible to override without a code change. Expose them as named config fields with documented defaults.

- **Typed-vs-free-form drift**: Mixing declared typed config keys with unnamespaced free-form `--option` keys in the same plugin. Forbidden because it makes the effective config surface unpredictable. Reserve a namespace (e.g. `x_*`) for experimental options and enforce strict unknown-key rejection on all declared typed configs.

## Locked Engineering Principles

These apply to all new code in `src/` and to plugin implementations.

- **Pythonic first**: Follow PEP 8. Keep imports tidy. Prefer simple, explicit, readable Python over clever abstractions.
- **Dataclasses + type hints for contracts**: Use dataclasses and type annotations for SDK contracts and config objects. They fail fast and self-document. Plain dicts are not contracts.
- **Small, purpose-driven modules**: Keep modules focused on one concern. Avoid "god files" that accumulate unrelated logic. If a module is growing, split it before adding more.
- **Clear names, straightforward control flow**: Prefer descriptive names and linear logic over metaprogramming or implicit dispatch. A reader should understand what a function does without tracing its ancestors.
- **No side effects at import time**: Especially in `firecube.ingestor`. Keep optional deps lazy. Don't trigger I/O, registration, or network calls on import.
- **Fail fast on bad config**: Validate config at construction time, not at first use. A `ValueError` at startup is better than a silent wrong result halfway through a multi-hour ingest.

## Design Patterns (GoF-inspired, Python-friendly)

Prefer these patterns when structuring new subsystems or refactoring existing ones.

- **Composition over inheritance**: Assemble behavior from small, focused services (workspace, registrar, writers) rather than deep class hierarchies. If you're reaching for a third level of inheritance, stop and compose instead.
- **Facade**: Provide stable, narrow entry points over complex subsystems. `ChunkManager` is the canonical example: it hides manifest/query/deletion internals behind a single coherent API.
- **Strategy**: Drive selectable policies (write modes, batching policies, storage backends) from config rather than `if` ladders. New strategies are added by implementing the protocol, not by editing existing branches.
- **Template Method (sparingly)**: Use for plugin templates where the required hook surface is small and well-documented. If the base class grows beyond a handful of hooks, split into composed services instead.
- **Adapter**: Normalize external APIs (S3/local via fsspec, URI parsing, filesystem ops) behind shared helpers. New code must go through `src/firecube/core/filesystem/` and `src/firecube/core/uris.py`, not raw `fsspec.filesystem(...)` calls.
- **Factory/Registry**: Use for plugin discovery (entry points + registration). Avoid hardcoded imports of plugin classes in engine code.
- **Protocols as interfaces**: Define "ports" as `Protocol` classes. Keep concrete implementations in runtime/core ("adapters"). Plugins depend only on public `api.py` surfaces, never on deep internal imports.

## Principles Carried Forward

These are the short-form rules that cut across all three sections above. They apply to every PR, every plugin, and every new module.

- **Explicit over implicit**: every flag, config key, and behavior must be declared, not inferred from context. If a reader has to trace code to understand what a run will do, the interface is wrong.
- **One driver everywhere**: `StorageConfig.storage_driver` selects fsspec or obstore for the entire write domain. No mixing. Source-side fsspec reads are allowlisted in `test_no_raw_fsspec_usage.py`.
- **Engine owns bookkeeping**: plugins must not write manifests or control-plane files directly. All lifecycle state goes through `SpanRecorder` and `ChunkManager`.
- **No mixin creep, no god base classes**: if `BaseIngestor` or any template grows too large, split into composed services. The base class hook surface must stay small and documented.
- **Public surfaces only**: plugins import from `firecube.ingestor.api` and `firecube.core.api`. Deep imports from internal submodules are a coupling violation and will break on refactor.
- **One container, one run**: do not add internal schedulers or queues. Concurrency comes from orchestrating disjoint slices at the job level, not from shared writers inside a single process.
