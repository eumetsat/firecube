# Changelog

All notable changes to Firecube are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and Firecube package versions follow PEP 440-compatible Semantic Versioning.

## [Unreleased]

## [0.1.5] - 2026-09-03

### Added

- New concepts page `docs/concepts/reproducibility.md`: the tiered
  reproducibility contract (value-identical always; byte-identical only in a
  pinned environment) and where its provenance records live.
- New reference page `docs/reference/control-plane-spec.md`: normative
  specification of the `.firecube/` control-plane layout and file formats
  (run records, WAL events, claims, snapshots, index records, versioning),
  pinned to the code by `tests/sdk/test_control_plane_spec_consistency.py`.
- `preallocate` now materializes `RegularTimeAxis` time coordinates densely at plugin-declared chunk size (default `min(256, T)`); previously-NaT-filled spec-loop arrays are filled and marker-stamped in place.
- New `TimeAxis` intent-named constructors (`TimeAxis.grid`, `TimeAxis.observed`, `TimeAxis.explicit`, `TimeAxis.discovered`) on `firecube.ingestor.api` and `firecube.core.api`: sugar over the raw axis dataclasses, producing identical index identities.
- `firecube zarr preallocate`: new `--slot-start`/`--slot-end` flags bound the coordinate materialization window explicitly (default: the full declared extent).
- New: `firecube zarr consolidate-time-coord` — maintenance command for existing cubes with per-slot time coordinate chunks; `--dry-run` reports proposed changes without mutation; consolidation seals the cube (further ingest to it is refused).
- New reserved attribute names: `firecube_preallocated`, `firecube_coord_managed`, `firecube_consolidated_at`.
- `RegionZarrWriter.write_timestamp` is now marker-aware: sealed time coordinates are drift-checked (`SchemaDriftError` on mismatch) rather than overwritten; legacy (unmarked) cubes retain existing create-on-demand behavior.
- New WAL event: `ConsolidatedTimeCoord` — records time-coordinate consolidation in the control-plane log.
- `extract_all_from_zips` extracts a batch of ZIP archives, serially by
  default or concurrently via `workers=`, returning an `(extracted, failures)`
  pair in which every input archive appears exactly once; a failed archive is
  reported instead of aborting the batch and its partial output is removed.
- New engine option `extract_workers` (default 4): number of parallel
  archive-extraction workers for plugins that extract source-archive batches.
  Independent of `pipeline_workers`; set with `--option extract_workers=N`.
- `IndexedWrite` and `IndexedWriteCompilationError` are now documented in the
  public API reference. `IndexedWrite` appears in `docs/reference/templates.md`
  under Schema And Write Types (both `firecube.ingestor.api` and
  `firecube.core.api` facades). `IndexedWriteCompilationError` appears in
  `docs/reference/exceptions.md`. The DirectZarr plugin guide
  (`docs/guides/plugins/direct-zarr.md`) gained a section on letting the
  engine resolve slots by returning `IndexedWrite` elements from `build_write_intents`.
- `IrregularTimeAxis` axis type for `IndexSpec`: declare an irregular time axis
  with an explicit tuple of coordinate values when items are not evenly spaced.
  Importable from `firecube.core.api` and `firecube.ingestor.api`.
- `AUTO` sentinel: set `IrregularTimeAxis(values=AUTO)` to let the engine
  discover coordinates at planning time by calling `inspect_item` on every
  source item before preallocate. Importable from `firecube.core.api` and
  `firecube.ingestor.api`.
- `MissingIrregularCoordinateError`: raised during `AUTO` discovery when an
  item returns `ItemInfo(coordinate=None)` or when two items share the same
  coordinate. Inherits from `ConfigurationError`. Importable from
  `firecube.core.api` and `firecube.ingestor.api`.
- `NoDiscoveredItemsError`: raised during `AUTO` discovery when no items are
  found for an `IrregularTimeAxis` group. Inherits from `ConfigurationError`.
  Importable from `firecube.core.api` and `firecube.ingestor.api`.
- `ItemManifestEntry` and `validate_manifest_entries`: content-addressed item
  manifest types used by the engine to plan `IrregularTimeAxis` axes and hand
  deterministic per-item work to parallel workers. Importable from
  `firecube.core.api` and `firecube.ingestor.api`.
- `--dry-run` flag for `firecube zarr preallocate`: runs discovery and resolves
  the index without writing any Zarr arrays, claim files, or control-plane
  records. Output matches `firecube zarr index show --json`.
- `--derived` flag for `firecube zarr index show`: computes and prints derived
  coordinate values for `regular_time` groups from the stored epoch, cadence,
  and size. Read-only; no files are written.
- `WriteIntent.data` now accepts `Callable[[], np.ndarray]` in addition to
  `np.ndarray | Any`. The callable is resolved exactly once at dispatch time,
  immediately before the writer call. Eager payloads remain valid; existing
  plugins run unchanged. Supported for `kind="region"` and `kind="static"`;
  other kinds raise `TypeError` at construction.
- `IntegerAxis` axis type for `IndexSpec`: declare a zero-based integer axis
  with a fixed `slot_count` when items map to an integer position rather than a
  timestamp. Importable from `firecube.core.api` and `firecube.ingestor.api`.
  A single `IndexSpec` can mix `IntegerAxis` and `RegularTimeAxis` groups.
- `ResolvedIndexRecord`: on-disk control-plane record written to
  `.firecube/index/current.json` after the engine resolves an `IndexSpec`.
  Stores the `identity_hash` of the resolved spec; subsequent runs verify the
  hash before writing. Importable from `firecube.core.api` and
  `firecube.ingestor.api`.
- `firecube zarr index` CLI command group with three subcommands:
  - `firecube zarr index show --target <uri> --product-name <name>` — print the
    current resolved-index record; add `--json` for machine-readable output.
  - `firecube zarr index verify --target <uri> --product-name <name>` — confirm the
    record can be read and is not a legacy format.
  - `firecube zarr index rebuild --target <uri> --plugin <name> --product-name <name>`
    — regenerate the record from a plugin's `index_spec()` declaration.
    Use this to migrate cubes written by Firecube v0.1.4.post1 and earlier.
- Per-topic API reference pages (templates, hooks, context, exceptions,
  slot-range parallelism, extensions, core utilities) behind a Reference
  overview, each opening with a summary table. Coverage now includes the full
  `BaseIngestor` hook surface, exception types, `PluginContext`, multi-group
  writes via `get_batch_groups`, and the `firecube.core.api` helpers, with
  usage examples on the hooks plugins implement.
- Plugin guide for customizing source discovery.
- `firecube.ingestor.extensions` declares an explicit `__all__`.
- `zarr_codecs` configuration option (introduced by #25 for `GenericZarrIngestor`)
  is now also available to `DirectZarrIngestor` plugins via `--option zarr_codecs='[...]'`.
  Previously the option was declared but unreachable for `DirectZarrIngestor` plugins
  because the template config class was not wired. Closes #25, #40.
- Per-array codec overrides on `ZarrArraySpec` (for `DirectZarrIngestor` plugins):
  optional `filters`, `serializer`, and `compressors` fields accept Zarr v3 codec
  dicts in the same format as `zarr_codecs`. Per-array values take priority over
  the template-level default. `compressors = ()` (empty tuple) explicitly disables
  compression for that array while other arrays use the template default
  ("compress-except-X" pattern). Requires `zarr_compression = true` at the
  template level. Closes #40.
- `zarr_codecs` now accepts full Zarr v3 codec pipelines (filters → serializer →
  compressors in order). Previously only a single compressor entry was accepted;
  multi-element lists were rejected with "chains are not supported". Pipeline
  ordering is validated against zarr's `codecs_from_list`. Closes #40.
- Codec drift detection: `DirectZarrIngestor` schema verification now checks
  declared codec configuration against the on-disk array metadata on resume. A
  mismatch raises `SchemaDriftError` naming the offending field (`filters`,
  `serializer`, or `compressors`). Applies to all arrays including static
  (`time_indexed = false`) arrays. Re-ingest from scratch is the only migration
  path, consistent with existing shape/dtype/chunks drift policy. Closes #40.
- Versioned documentation with `mike`: each stable release publishes docs under
  its full version (for example `/0.1.4/`) with a `latest` alias as the site
  default; pre-releases publish under their own version only and never become
  the default; `main` is tracked as `/dev/`. New `docs-deploy` workflow deploys
  automatically on pushes to `main` and on published GitHub releases.
- PR-time security scanning in CI: trivy (vulnerabilities, secrets, licenses)
  on pushes and pull requests, plus GitHub dependency review with a
  GPL/AGPL/SSPL license deny-list on pull requests.
- `py.typed` marker (PEP 561) so plugin authors get IDE type support for
  `firecube` imports.
- `firecube zarr slots` now accepts `--option` and resolves typed plugin
  config via `TierConfigurator`, matching `firecube zarr preallocate`.
- New `zarr_region_write_concurrency` template config option (integer, default
  1) controls the maximum number of concurrent region writes per slot. Values
  < 1 are rejected at config validation time.
- `firecube zarr compare` subcommand and `compare_zarr_stores()` /
  `ZarrCompareReport` in `firecube.core.api`: read-only equivalence check of
  two Zarr stores (paths, shape, dtype, chunks, dimension names, public attrs,
  static marker, values). Exit `3` on mismatch, one stderr line per
  difference.
- `static_owner` field in the `firecube zarr slots` JSON plan: the range with
  the smallest `slot_start` per group, for schedulers that route static-array
  writes to one worker. Additive to the existing plan schema.
- `--suppress-static-emission-for-non-owner` and `--static-owner-slot-start`
  flags for `firecube ingest` (and the matching `EngineConfig` fields): a
  slot-range worker skips static-array writes unless its `--slot-start`
  equals the owner value from the plan. Requires `slot_start`; serial runs
  reject the flag and always write statics.
- `zarr_write_empty_chunks` typed template option (boolean, default `False`):
  passes Zarr's `array.write_empty_chunks` setting through as a scoped config
  for the write phase only. The effective value is reported in batch metrics
  as `zarr_write_empty_chunks_effective`.
- `firecube zarr validate` reports `static_marker_failures` for the validated
  array when a static array lacks the `firecube_static_written` marker.
- New public exports on `firecube.core.api` (and `firecube.ingestor.api`
  where applicable): `ExtentUnknownError`, `UnboundedAxisError`,
  `RESERVED_ARRAY_ATTRS`, `assert_attrs_safe`, `FIRECUBE_STATIC_WRITTEN_ATTR`,
  `resolve_index_spec`, `extract_all_from_zips`, `BatchResourceRegistry`,
  `physical_chunk_keys_for_region`, `chunk_axis_range`, and
  `axis_selection_is_chunk_aligned`.
- `extract_hdf5_from_zip` and `stream_hdf5_from_zip` accept an explicit
  `member=` argument; archives with several HDF5 candidates are now rejected
  with the candidate list instead of silently picking the first.

### Changed

- `firecube plugins create` generates a complete plugin with one reader
  function unimplemented instead of hook stubs that raise. Each template is
  its guide's own listing: `write_product_item` (base), `read_dataset` (zarr),
  `read_table` (parquet), and `read_product_item` (DirectZarr) raise
  `NotImplementedError` naming the file they were called for; every hook is
  already wired to them. On a fresh DirectZarr scaffold `firecube zarr slots`,
  `preallocate`, and `plugins describe` work immediately, and `ingest` fails
  at the reader. The empty `PluginConfig` subclass is gone (attach one when
  the product needs typed options), the generated `pyproject.toml` carries
  `[tool.ruff]`, `[tool.pyright]`, and a commented `[tool.uv.sources]` hint
  for a local Firecube checkout, the generated test file holds one real
  registration test instead of guidance text, and the generated README states
  that the project has its own environment. Scaffolding tests now render
  every template, drive the hooks, and lint and typecheck the rendered
  project with its own config, so the templates cannot drift from the API
  unnoticed.
- `firecube plugins create` defaults to the `zarr` template (with the `xarray`
  write strategy) instead of `base`, both at the interactive prompt and with
  `--non-interactive`; `base` is the advanced custom-pipeline choice and must
  now be requested explicitly.
- `EngineConfig` rejects `pipeline_workers < 1` and `pipeline_batch_size < 1`
  at construction instead of accepting them silently.
- `ResolvedIndexConflictError` message now includes a field-level diff (groups symmetric difference, per-group axis changes, top-level name/scalar changes) alongside the two truncated hashes -- no more hash-only conflict messages.
- `verify_array_spec` now rejects an on-disk sharded array when the declared
  spec has `shards=None`, even at `zarr_region_write_concurrency=1`. Previously
  this mismatch was silently accepted. Operators with sharded arrays and a
  non-sharded spec must either recreate the array with matching shards or update
  the spec.
- `docstring_style` pinned to `google` in `mkdocs.yml`; numpy-style docstrings
  converted to match.
- CI test lanes follow `plans/TESTING_STANDARDS.md`: the `test` job excludes
  `docs_static` and `snapshot`; the `docs` job runs them.
- `iso_to_epoch_s` now rejects naive ISO 8601 strings and accepts only UTC-
  explicit inputs (`Z`, `+00:00`, or `-00:00`). Consumer action: callers
  passing naive timestamps must append `Z` (or `+00:00`).
- `ZarrTemplateConfig.zarr_compression` now defaults to `True` (was `False`), aligning
  firecube with zarr-python v3's default codec pipeline (`ZstdCodec(level=0)`). Explicit
  `zarr_compression = false` still disables compression. Because PR #25 never shipped
  a release, no user-visible behavior changes vs. the last tagged version (v0.1.4.post1).
- Dependency updates: `cryptography` 50.0.0, `aiohttp` 3.14.3,
  `virtualizarr` 2.7.1, `healpix-geo` 0.2.1,
  `opentelemetry-exporter-otlp` 1.44.0, `mkdocs-material` 9.7.7,
  `setuptools` 83.0.0, `actions/checkout` v7.
- README SBOM dependency tables synced with `uv.lock`; added `mike` to the
  docs dependency group and its SBOM table entry.
- Reorganized plugin documentation and plugin author examples.
- `read_hdf5_array` preserves the source dtype instead of casting everything
  to `float32`; pass `dtype=` for an explicit cast. Callers relying on the
  implicit `float32` cast must now request it.
- A `DirectZarrIngestor` run whose declared regular axis has no fixed extent
  fails with `UnboundedAxisError` (a `ConfigurationError`) naming the group,
  instead of a raw `ExtentUnknownError`, in serial and parallel modes alike.

### Fixed

- `firecube plugins create --template zarr --write-strategy zarr-python` no
  longer generates a plugin that crashes at startup: the DirectZarr scaffold
  passed the removed `end=` keyword to `RegularTimeAxis` (now `end_date=`),
  which every engine startup and `firecube zarr slots`/`preallocate` call hit
  before the author had written a line. Scaffolded plugins now depend on
  `firecube>=0.1.5`, the first release carrying the API the templates use;
  the old `>=0.1.0` floor let `uv sync` resolve a release without
  `IndexedWrite`/`TimeAxis`, so the plugin failed to import and silently
  disappeared from `firecube plugins list`.
- Float-dtype arrays with a finite fill value (for example `-999.0` sentinels)
  are no longer unopenable by xarray: the stamped `_FillValue` attribute now
  carries the base64-encoded IEEE-754 form xarray decodes for float dtypes,
  instead of a bare number that made `xr.open_zarr` raise `TypeError`.
  Integer, bool, and string fills are unchanged; NaN and NaT fills still
  stamp nothing.

- `click` is now declared as a runtime dependency; it was imported by the CLI
  but reached installations only transitively, so a minimal install could not
  run `firecube`.

- Concurrent multi-process ingests no longer crash at startup with
  `ControlPlaneCorruptionError` ("Expecting value: line 1 column 1") when one
  process's resume guard lists runs while a peer is writing its `run.json`:
  run metadata is now published through a new atomic-replace filesystem
  primitive (`AtomicWriter.replace_atomic`; temp file + rename locally, a
  single whole-body PUT on object stores), and the WAL reader skips a
  zero-byte `run.json` with no segments as an in-flight peer write instead of
  raising.

- `firecube zarr compare` and `compare_zarr_stores` no longer crash with an
  IndexError on stores containing a zero-dimensional array (for example a CF
  grid-mapping scalar such as `spatial_ref`); scalar values are now compared
  like any other array.

- `firecube zarr compare` and `compare_zarr_stores` no longer materialize
  whole arrays into memory: values are compared in chunk-aligned slabs
  bounded by a fixed byte budget, so product-scale cubes compare in constant
  memory instead of being killed by the OOM killer. Comparison also treats
  NaT (and complex NaN) like float NaN: two stores holding NaT in the same
  positions — for example the unfilled slots of a partially ingested dense
  time coordinate — now compare equal instead of reporting `values differ`.

- `include_patterns` is documented as additive to the built-in `.zip`/`.h5`/
  `.nc` selection; the reference previously stated that it replaced them.
- `SlotAxis.__post_init__` now rejects `bool` and non-integral `cadence_s`
  values that previously slipped through Python's `int` subclass semantics;
  accepted types are Python `int` and `numpy.integer` subclasses
  (e.g. `np.int64`).
- DirectZarr arrays declaring a `fill_value` now carry the matching
  `_FillValue` attribute, so xarray masks unwritten cells when reading with
  `mask_and_scale=True` (#49). Only JSON-safe scalars are stamped: NaN, NaT,
  and datetime fills are skipped to keep array metadata valid strict JSON and
  the stores openable by xarray. Resuming into an existing store backfills the
  attribute once.

- Bulk stale-sweep operations (`abandon_stale_runs`, `clear_stale_claims`)
  no longer re-list the full runs/ or claims/ directory once per stale item.
  Each mutation-time re-check is now a targeted per-item read. On S3, the
  cost of a sweep with `S` stale items over a product with `N` total items
  drops from `O(N × S)` object reads to `O(N + S)`. Behavior is unchanged
  (race semantics, orphan-handling, and partial-abandon crash recovery are
  all preserved). Fixes #26.

### Security

- ZIP extraction helpers (`extract_hdf5_from_zip`, `stream_hdf5_from_zip`,
  `extract_all_from_zips`) reject member names that could escape the
  extraction root (`..` segments, absolute paths, Windows drive prefixes)
  with `ValueError` instead of extracting them.

### Removed

- Engine option `pipeline_parallel`. Execution mode is now decided solely by
  `pipeline_workers`: 2 or more runs the parallel pipeline, 1 runs
  sequentially. Migration: replace `pipeline_parallel=true` with
  `pipeline_workers=2` (or more) and delete `pipeline_parallel=false`;
  leftover keys in `--option` flags or config files now fail loudly at
  option validation. Previously `pipeline_parallel=false` was silently
  ignored whenever `pipeline_workers` was above 1.
- Removed unused `ItemInfo.key` and `ItemInfo.group` fields. These were placeholders.
- `docs/reference/api.md` and `docs/reference/advanced-plugin-api.md`, replaced
  by the per-topic pages; deep links to their headings no longer resolve.
- `SUPPORTS_SLOT_RANGE_PARALLELISM` class variable and the `SlotRangeCapable`
  Protocol/mixin: DirectZarr parallelism is now declared exclusively via
  `index_spec(ctx) -> IndexSpec | None`. Plugins that still inherit from
  `SlotRangeCapable` should drop the base class and set `PRODUCT_NAME` explicitly.
- `slot_index_model()` hook on `BaseIngestor`: replaced by `index_spec()`.
  The legacy `SlotIndexModel` record is still readable for backwards
  compatibility with cubes written by v0.1.4.post1 (see `firecube zarr index
  rebuild` to migrate).
- `timestamp_to_ts_index()` hook: engine-owned slot resolution now handles
  timestamp→slot mapping via `IndexSpec`'s `resolve_index_spec`.
- `global_expected_time_count()` hook: derivable from `IndexSpec` groups
  directly; no plugin-side callback needed.
- `filter_items_to_slot_range()` hook: replaced by
  `firecube.ingestor.runtime.index_binding.filter_items_by_index`, called by
  the engine after `IndexSpec` resolution.

Migration for 0.1.4.post1 plugins: remove all six symbols from your plugin
class, implement `index_spec()` returning an `IndexSpec`, and implement
`inspect_item(item, ctx) -> ItemInfo | None` for the engine to place each
item in the resolved index. See `docs/operations/firecube-index.md` for
the migration procedure including `firecube zarr index rebuild`.

### Stats

- pytest: full local suite under `-W error::DeprecationWarning` (`not s3 and
  not race`): 3251 collected, 3244 passed, 6 skipped, 1 failed. 
- pyright: 0 errors, 0 warnings.
- ruff: `check` and `format --check` clean.
- docs: `mkdocs build --strict` succeeds; docs-static and snapshot tests green.
- package: `uv build` produces the `0.1.5` wheel + sdist; `twine check` PASSED.

## [0.1.4.post1] - 2026-07-24

### Changed

- Package metadata only: added PyPI project URLs. No code changes relative
  to 0.1.4.

## [0.1.4.post0] - 2026-07-24

### Changed

- Package metadata only: updated the PyPI package description. No code changes
  relative to 0.1.4.

## [0.1.4] - 2026-06-29

### Added

- Project governance docs: `CONTRIBUTING.md` (contributor workflow, lint/test
  gates) and `CODE_OF_CONDUCT.md`.

### Changed

- The `tensogram` optional dependency (and the `test` extra) now requires
  `tensogram`, `tensogram-xarray`, and `tensogram-zarr` `>=0.22,<0.23`
  (previously `>=0.21,<0.22`). Reinstall the extra (`uv sync --extra tensogram`)
  after upgrading.
- Container image now carries an `org.opencontainers.image.vendor="EUMETSAT"`
  label.

### Fixed

- Concurrent slot-index model negotiation on a **local** filesystem no longer
  intermittently fails with `slot-index record is not valid JSON: ... (char 0)`.
  The local control-plane atomic write (`current.json`, write-claim locks) now
  publishes content atomically via a temp file plus `os.link` instead of an
  exclusive-create `open("xb")`, which on local disk made only the filename
  appear atomically and left a zero-length window a concurrent reader could
  observe. Create-if-not-exists semantics are unchanged (`os.link` still raises
  on an existing target); S3 and obstore backends were already content-atomic
  and are unaffected.
- Generated Intake catalogs now reference the registered `zarr` driver short
  name instead of the non-resolvable module path `intake_xarray.zarr.ZarrSource`,
  which made `intake.open_catalog(...).read()` fail at open time with "No plugins
  loaded for this entry". Parquet entries are unchanged.
- `firecube plugins install --editable` now verifies the installed plugin in a
  fresh interpreter. An editable install writes a `.pth` file the current
  process has not loaded, so the post-install check could spuriously report a
  successfully installed plugin as missing.
- The CLI table formatter accepts any `Iterable` of rows, not only a
  materialized `list`, so generator-backed command output renders correctly.

### Stats

- pytest: full suite green under `-W error::DeprecationWarning` (2273 tests
  collected, 0 failures); `not slow and not s3` lane green at 82% line coverage.
- pyright: 0 errors, 0 warnings.
- ruff: `check` and `format --check` clean.
- docs: `mkdocs build --strict` succeeds.
- package: `uv build` produces the `0.1.4` wheel + sdist; `twine check` PASSED.

## [0.1.3] - 2026-06-22

### Added

- `ZarrGroupSpec.attrs` (optional `Mapping[str, Any]`) lets DirectZarr plugins
  declare group-level Zarr attributes, written verbatim onto the group's
  `zarr.json` at schema setup. Convention-agnostic — firecube does not interpret
  the mapping; reserved firecube-internal attribute names are rejected. The
  sibling of the existing per-array `ZarrArraySpec.attrs`. Importable from
  `firecube.ingestor.api`.

### Fixed

- CF-1.8 advisor (`firecube advise compliance`): CF010 ("data var missing
  `units`") no longer flags **grid-mapping container** variables. A variable
  referenced via a `grid_mapping` attribute is a CRS container that carries no
  units by CF design, so requiring units on it was a false positive.
- Parallel slot-range ingestion to an `s3://` target no longer crashes at pod
  startup with `OSError: [Errno 22]` / S3 `PreconditionFailed`. Two distinct
  object-store concurrency hazards are resolved:
  - The control-plane write-claim's exclusive-create now treats S3's
    `412 PreconditionFailed` (returned when a concurrent pod wins the
    create race) as "already exists", so losing pods retry and converge
    instead of aborting the whole run. Local and obstore backends were
    unaffected.
  - Startup metadata reads (the existing-cube dimension-compatibility check)
    now issue a single-shot GET instead of a conditional/range-cached read, so
    a pod reading a group's `zarr.json` while another pod is creating it no
    longer fails with `412 PreconditionFailed`.

## [0.1.2] - 2026-06-22

### Added

- `ZarrArraySpec` gains four new fields for DirectZarr write parity: `shards`
  (per-dimension shard shape), `attrs` (array-level metadata), `dimension_names`
  (explicit Zarr v3 dim labels), and `time_indexed` (set `False` for static
  coordinate arrays such as lat/lon). Schema hash and `verify_array_spec`
  honour all new fields.
- Static array write support: `WriteIntent.kind="static"` dispatches to
  `RegionZarrWriter.write_static`. Static arrays are created at declared shape,
  excluded from time-axis preallocation, and write-once-enforced via the
  `firecube_static_written` marker attribute — resume rejects divergent data
  via `SchemaDriftError`.
- Reserved array attribute guard: `assert_attrs_safe` in
  `firecube.core.zarr._reserved_attrs` prevents plugins from overriding
  firecube-managed internal attributes.
- CF-time decode helper `decode_time_array` (importable from `firecube.core.api`)
  dispatches on `(dtype, attrs)`; output preserves decoded `datetime64`
  resolution without coarsening to seconds.
- Slot-index model: `SlotIndexModel`, `SlotAxis`, and associated record types
  are importable from `firecube.core.api`. `SlotIndexModel.identity_hash` is a
  64-character lowercase hex digest; construction rejects invalid shapes.
  `ensure_slot_index_model` enforces a four-level precedence matrix
  (explicit CLI → persisted → plugin default → absent).
- `DirectZarrIngestor.slot_index_model` hook with abstract-subclass guard;
  concrete plugins may declare a `SlotIndexModel` at class level.
- `firecube zarr preallocate` now accepts `--option` and `--input-data` flags,
  matching the `ingest` command surface.
- `firecube chunks runs` now accepts `--target` to resolve the backend store
  for remote slot-index I/O via the driver-aware Zarr store.

### Changed (breaking)

- CLI now enforces strict URI form everywhere: only `file:///abs/path` and
  `s3://bucket/key` accepted; bare paths rejected with `"URI scheme required
  (file:// or s3://). Did you mean <suggestion>?"`. Inspect-tier commands
  (`zarr validate`, `parquet validate`, `advise batch-size`, `advise compliance`,
  `catalog intake`) accept product URIs without explicit storage flags: the
  smart-default policy infers `--storage-type` from the URI scheme
  (`file://`→local, `s3://`→s3); explicit values override the smart default
  and are rejected on URI/storage-type coherence mismatch. `--storage-driver`
  defaults to `fsspec`, overridable via `FIRECUBE_STORAGE_DRIVER` or
  `[storage].driver` in config. `--write-mode` remains required for write-tier
  commands.
- `archive create --target` (artifact `.tgm` path) renamed to `-a, --archive`.
- `archive restore --source` (artifact `.tgm` path) renamed to `-a, --archive`.
- `archive info`, `validate`, `list`: positional path argument migrated to
  `-a, --archive` URI flag.
- `zarr multires`: positional target URI migrated to `-t, --target` flag.
- `ingest --source` (raw plugin input) renamed to `-i, --input-data`.
  `IngestCommandConfig.source` field renamed to `input_data`.
- `chunks --product` (NAME) renamed to `-n, --product-name` to match ingest/zarr.
- `parquet consolidate --output` and catalog intake `--output` now require
  strict URI form.
- Remote `.tgm` artifact runtime error message updated to `"not yet supported"`.
- Canonical short-flag registry locked: `-p --product`, `-t --target`,
  `-s --source`, `-a --archive`, `-o --output`, `-i --input-data`,
  `-n --product-name`, `-g --group`, `-w --write-mode`, `-f --format`,
  `-r --resolutions`.

### Fixed

- `firecube ingest` now converts `ConfigurationError` to a clean `Error:` line
  when `--input-data` is omitted; no Python traceback is shown to the user.
- Omitting `--input-data` no longer fabricates `"None"` as the source path;
  `IngestContext.source` is set to `""` and the default `discover_source_files`
  raises `ConfigurationError` before any I/O.
- CF-time silent exception swallowing removed from `AppendCoverageBuilder`; the
  bare-except that was masking the 1970-epoch coverage bug is gone — decode
  failures propagate loudly.
- CF-time decode now correctly threads through the append cursor value coercion
  path, fixing silent epoch mismatches in incremental appends.
- Static arrays are properly excluded from time-axis preallocation in the
  sequential schema setup path.
- Raw tracebacks replaced with clean Click errors for missing zarr groups in
  `zarr validate`, `advise batch-size`, and `advise compliance`.
- Internal jargon (`WAL`, `pod`, `PRODUCT_NAME`, `default_product_name`)
  removed from user-facing CLI help.
- Built docs site no longer references the removed `advise cf` command.

### Internal

- `wrap_user_facing_errors` whitelist extended to `ConfigurationError` (in
  addition to the OS-level errors added previously).
- `duration_cpu_s` now uses `time.process_time()` (process-wide, all threads)
  instead of per-batch `time.thread_time()`, giving accurate CPU accounting for
  dask/HDF5/netCDF worker threads.
- Canonical URI scheme policy module (`firecube.cli._uri_policy`) replaces
  duplicated scheme mappings.
- CLI command contract manifest covers every core leaf command.
- Architectural leaks resolved: `catalog intake` passes typed `StorageSession`
  only; `chunks/_manager` removed `dict` roundtrip; `chunks migrate` uses
  public repo API; archive converter receives typed session only.
- `chunks/*` leaf commands added to `CLI_COMMAND_CONTRACTS` as
  `tier="control-plane"`.

### Stats

- `uv run pytest --strict-deps -m "not slow and not s3 and not docs_static and not snapshot" -q`: 2,161 passed, 39 deselected, 122 warnings.
- `uv run ruff check .`: passed.
- `uv run pyright src/`: 0 errors.

## [0.1.1] - 2026-06-10

### Added

- Added a guarded Makefile release flow that tags `v<version>` only from a clean, up-to-date `main` checkout.
- Added package-version consistency checks for runtime exports, docs rendering, CI tag validation, and container metadata.
- Added MkDocs version macros so installation examples render the current Firecube package version.

### Changed

- Standardized Firecube package versioning around `pyproject.toml` using PEP 440-compatible SemVer and `v<version>` git tags.
- Updated CI to validate package/tag consistency, build docs, and pass the bare package version into container OCI labels.
- Normalized Zarr slot-plan output and Tensogram archive contracts to explicit `v1` schema namespaces.
- Updated release preparation guidance to draft reviewed changelog entries in the pull request before tagging from `main`.

### Removed

- Removed unused legacy unversioned Tensogram `.tgm` restore handling and the obsolete archive restore `--group` path.

### Migration Notes

- Tensogram archives now require the `v1` archive layout.
- The Firecube package version is `0.1.1`; runtime code, docs examples, CI checks, and container labels should derive from package metadata instead of hardcoded release strings.

### Stats

- `uv run pytest --strict-deps -m "not slow and not s3" --cov=src --cov-report=term-missing --cov-report=xml -q`: 2,273 passed, 95 skipped; coverage `TOTAL 79%`.
- `uv run mkdocs build --strict --site-dir /tmp/firecube-mkdocs-check`: passed.
- `uv build`: built `firecube-0.1.1.tar.gz` and `firecube-0.1.1-py3-none-any.whl`.
- `uv run --with twine twine check dist/*`: passed.

[Unreleased]: https://github.com/eumetsat/firecube/compare/v0.1.5...HEAD
[0.1.5]: https://github.com/eumetsat/firecube/compare/v0.1.4.post1...v0.1.5
[0.1.4.post1]: https://github.com/eumetsat/firecube/compare/v0.1.4.post0...v0.1.4.post1
[0.1.4.post0]: https://github.com/eumetsat/firecube/compare/v0.1.4...v0.1.4.post0
[0.1.4]: https://github.com/eumetsat/firecube/releases/tag/v0.1.4
