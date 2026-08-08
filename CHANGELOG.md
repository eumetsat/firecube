# Changelog

All notable changes to Firecube are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and Firecube package versions follow PEP 440-compatible Semantic Versioning.

## [Unreleased]

### Added

- `zarr_codecs` configuration field: a single-element list of codec entries in
  Zarr v3 metadata format (`[{"name": "...", "configuration": {...}}]`). Select
  any codec registered via zarr's extension mechanism by name.
  Requires `zarr_compression = true`. Closes #25.
- Versioned documentation with `mike`: each stable release publishes docs under
  its full version (for example `/0.1.5/`) with a `latest` alias as the site
  default; pre-releases publish under their own version only and never become
  the default; `main` is tracked as `/dev/`. New `docs-deploy` workflow deploys
  automatically on pushes to `main` and on published GitHub releases.
- PR-time security scanning in CI: trivy (vulnerabilities, secrets, licenses)
  on pushes and pull requests, plus GitHub dependency review with a
  GPL/AGPL/SSPL license deny-list on pull requests.
- `py.typed` marker (PEP 561) so plugin authors get IDE type support for
  `firecube` imports.

### Changed

- `zarr_compression` now accepts only `bool` (was `bool | str`). Passing a
  string value raises `ValueError` at parse time.

- Dependency updates: `cryptography` 50.0.0, `aiohttp` 3.14.3,
  `virtualizarr` 2.7.1, `healpix-geo` 0.2.1,
  `opentelemetry-exporter-otlp` 1.44.0, `mkdocs-material` 9.7.7,
  `setuptools` 83.0.0, `actions/checkout` v7.
- README SBOM dependency tables synced with `uv.lock`; added `mike` to the
  docs dependency group and its SBOM table entry.
- Reorganized plugin documentation and plugin author examples.

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
