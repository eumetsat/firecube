# Changelog

All notable changes to Firecube are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and Firecube package versions follow PEP 440-compatible Semantic Versioning.

## [Unreleased]

### Added

- `ZarrArraySpec(engine_owned=True)` and `ItemInfo(values={...})` declare
  rank-1 per-slot arrays in `DirectZarrIngestor` plugins. `firecube zarr
  preallocate` inspects every source item, on any time axis, and requires
  `--input-data`. It writes the array whole and marks it materialized. Ingest
  workers only verify the array and raise `SchemaDriftError` on a mismatch.
- `firecube zarr preallocate` refuses per-slot values that the array cannot
  hold. It refuses them before it creates the array.
  - For an integer array, it refuses NaN, infinity, a fraction and an
    out-of-range number.
  - For a bool array, it refuses any value other than `bool` or
    `numpy.bool_`.
  - For a float array, it refuses a finite number too large for the dtype.

  It does not refuse other roundings, such as a large integer into `float32`.
- Ingest into a target where the engine-owned array is not materialized is
  refused with the preallocate command. Ingest into a slot that preallocate
  did not see is refused in the same way.
- Preallocate and ingest never take over an existing array that lacks the
  engine-owned marker, whatever the array holds. Both refuse it. Delete the
  array and run preallocate again.
- If preallocate fails and handles the failure itself, run it again to finish
  the work. If a run was killed outright, take these steps:
  1. Clear its claim with `firecube chunks claims clear`.
  2. Abandon its run with `firecube chunks runs abandon`.
  3. Run `firecube zarr preallocate` again.
- Ingest reads the engine-owned markers from the array's own metadata, so
  stale consolidated metadata does not hide them. Append resume reads the
  stored array metadata when it classifies slots, writes and fills.
- `--archive-format [tensogram|zip]` option on `firecube archive create`.
  `--archive-format zip` packs a Zarr product into a single local
  `.zip` file that opens as a Zarr zip store. The default stays `tensogram`.
  Zip archives contain the whole product, so `--group`, `--start-date`,
  `--end-date`, and `--variables` are rejected.
- `firecube archive info`, `list`, `validate`, and `restore` accept local
  `.zip` archives.
- `firecube archive restore` writes a zip archive through the configured
  storage driver. Before it deletes the target or restores any entry, it
  refuses an archive that holds an entry outside the target. Such an entry has an absolute name,
  a backslash, or an empty, `.` or `..` segment. The error reads `Unsafe entry
  '<name>' in archive <path>`.
  - The restore also refuses a corrupt entry, an archive without a root
    `zarr.json`, and a `zarr.json` entry that is not valid Zarr v3 metadata.
  - With `--overwrite`, the restore deletes the old store but keeps
    `.firecube/claims/`, `.firecube/schema.json` and the records of its own
    run.
  - If the target, or a kept path inside it, is a symlink, the restore refuses
    the overwrite and deletes nothing.
- `firecube archive create` records each run in the source product's control
  plane. The record holds the start time, group, date window, variables and
  size of the archive.
  - If the source has a `.firecube/schema.json` file, the command takes the
    maintenance claim of the product. Use credentials with write access to the
    source.
  - The command refuses anonymous storage (`--storage-anonymous`) and exits
    with code 2. Use credentials that can write to the source.
  - If the command cannot write the record, it exits with code 1. The archive
    file stays in place. The message names the archive path and the record
    error. The run of the failed create can stay `started`. Abandon it with
    `firecube chunks runs abandon` before you retry with `--overwrite
    --yes-i-really-mean-it`.
  - If the source has no `.firecube/schema.json` file, the command writes no claim and no
    record, and does not modify the source.
- Host code outside the engine can now bind the index of a plugin the way the
  engine does, through the public method `BaseIngestor.bind_index_at_startup(ctx)`.
  - When a run preallocates a product or plans slots, the engine calls the method itself.
    A call before either operation is not a prerequisite.
  - The method runs the same binding that ingest runs at start-up. Pass it the plugin context of the run.
  - Each call resolves the index again.
  - If the `index_spec` of a `DirectZarrIngestor` plugin returns `None`, the plugin has no binding.
  - Call the method. Do not override it.
- `PipelineResult.failure` holds the `FirecubeError` that failed a batch. It is
  `None` on success and for any other exception. If the error carries a
  structured message, read its `context` and `recovery`. Otherwise read
  `PipelineResult.error`.
- `TimeAxis.grid`, `TimeAxis.explicit` and `TimeAxis.discovered` accept a
  `calendar` for a source that is not on the Gregorian calendar.
  - The accepted names are `360_day`, `noleap` (alias `365_day`), `all_leap`
    (alias `366_day`) and `julian`.
  - An irregular axis on such a calendar also needs `units`.
  - A regular axis on such a calendar needs `slot_count`. It does not accept
    `mode="floor"` or `end_date`.
  - A monthly `360_day` axis is a grid with `cadence_s=2592000`.
- Firecube stores the time coordinate of a calendar axis as CF-encoded numbers
  with `units` and `calendar` attributes. A reader decodes the numbers in that
  calendar. Firecube refuses a value on any other calendar. It does not
  relabel the value.
- `calendar` defaults to `"proleptic_gregorian"`. The names `"standard"` and
  `"gregorian"` are aliases of it. A Gregorian axis stores `datetime64` with
  no `units` or `calendar` attribute, as before.
- `firecube zarr preallocate` writes the whole coordinate of a calendar axis.
  Run `firecube zarr preallocate` before the first ingest into a calendar
  group.
  - An ingest that runs earlier fails, in direct and in staged mode. The
    error names the command.
  - A new preallocate run that changes the epoch, `units` or calendar of a
    stored coordinate fails. No Zarr data or metadata outside `.firecube/`
    changes. Firecube records the failed run.
  - On a regular calendar axis with a non-empty window, the dry run prints
    `first` and `last` as calendar dates, followed by the calendar name in
    parentheses.
  - An irregular calendar axis prints no `first` or `last` date in the dry
    run. An empty window on a regular calendar axis prints `None` for
    `first` and for `last`.
  - A slot window does not limit the grid of a regular calendar coordinate.
    The dry run adds `; window not applied to calendar coordinate` to the
    line.
  - A regular calendar axis accepts an empty slot window.
- `firecube zarr index show --derived` prints the first and last values of a
  calendar axis as calendar dates. It decodes the dates in the same way as
  the `firecube zarr preallocate` dry run.
- `firecube chunks list --time-range` and `firecube chunks delete
  --time-range` exit with a non-zero code if a recorded span bound does not
  parse as a Gregorian date. An example is 30 February on a `360_day`
  calendar. The error names each span, and `delete` deletes nothing. Spans
  with ordinary bounds filter as before.
- `--input-data stac://<host>/<api-root>/collections/<id>` reads the source
  granules of one STAC API collection. `firecube ingest`, `firecube zarr
  preallocate` and `firecube zarr slots` search the API at
  `https://<host>/<api-root>`, read every result page, and hand the href of
  each asset whose roles contain `data` to the plugin. The default suffix list
  does not apply; `--input-filters` narrows the assets on the basename of the
  href. Requires the new `stac` extra: `uv pip install 'firecube[stac]'`.
  Static `catalog.json` catalogues and reading `https://` hrefs are not
  supported.
- `--stac-search <json>` filters the item search of every `stac://` source
  with the keys `datetime`, `bbox`, `intersects` and `ids`. Any other key,
  `collections` included, exits with code 2, and so does `--stac-search`
  without a `stac://` source. The run record keeps the `stac://` sources,
  the filter as given and the resolved item ids.
- `--input-data` may be repeated, and its schemes may be mixed in one run.
  `PluginContext.sources` holds every source; `PluginContext.source` raises
  `ConfigurationError` when there are several.
- A plugin class that declares `SOURCE_ITEMS = True` receives `SourceItem`
  objects instead of href strings in `filter_item`, `inspect_item` and the
  build hooks. A STAC item carries its collection, item id, asset key,
  datetime, bbox, media type and roles.
- `discover()`, `DiscoveryRequest` and `SourceItem` in `firecube.core.api`
  discover the sources of a run from library code.

### Changed
- The refusal for a legacy index record now names the library function that
  rebuilds the index. A Python caller reads that name in the message. The CLI
  prints the command `firecube zarr index rebuild` instead. The
  `firecube zarr slots` warning about a legacy index record follows the same
  rule. The exception type stays the same.
- A refusal that asks you to run `firecube zarr preallocate` now names the
  library function that does the same for a Python caller. The CLI prints the
  command. Four refusals change:
  - An engine-owned array is not materialized.
  - An engine-owned slot holds no value.
  - A managed time coordinate slot holds no value.
  - A target array is shorter than the declared time axis.

  The exception types stay the same.
- Refusals now carry their facts and their recovery steps as data.
  - A refusal printed by the CLI names only the command. The "from Python"
    note is gone.
  - A refusal raised to a Python caller names parameters and library
    functions. It names no flag and no command. Text that you match on
    changes.
  - `FirecubeError.recovery` lists the operations that the message suggests.
    `FirecubeError.message` holds the message. `FirecubeError.context` holds
    its facts.
  - These three hold data only for an error that carries a structured
    message. For an error built from plain text, `message` is `None`, and
    `context` and `recovery` are empty. The text stays in `str(error)`. Keep
    reading `str(error)` as the fallback.
  - The CLI quotes values that hold spaces in the commands that it prints.
    This holds for the `firecube zarr preallocate` and `firecube ingest` hints.
  - A run error that Firecube records, and `PipelineResult.error`, keep the
    Python wording.
  - If `firecube zarr preallocate` cannot record the end of a run or cannot
    release its claim, it also prints the recovery command on stderr. The log
    line names the library function.
- Two config errors now raise `ConfigurationError` subclasses:
  `ConfigFileError` and `RemovedConfigKeyError`. Before, they raised
  `click.UsageError`. If you catch
  `click.UsageError` around library code, catch `ConfigurationError` instead.
  The CLI still prints the same text and exits with code 2.
- `PipelineFailedBatchesError` and `AppendBatchFailed` are now subclasses of
  `FirecubeError`. A handler for `FirecubeError` now catches them. They are
  still subclasses of `RuntimeError`.
- `firecube zarr slots` refuses a plan of more than 1,000,000 ranges. The
  refusal names the range count, and the range count and slot size of each
  group. It also names the smallest `--slot-size` that gives an executable plan
  within the limit. If no such size exists, the refusal says so. To emit the plan anyway,
  add `--yes-i-really-mean-it`. A plan of exactly 1,000,000 ranges is emitted.
  The JSON costs about 390 bytes per range, so 44 million ranges are about
  17 GB.
- `firecube zarr slots` prints one hint on stderr for a plan of more than
  100,000 ranges. The output format (`v1` JSON, table and CSV) is unchanged.
- `firecube zarr slots` now plans one range per slot by default for a plugin
  that declares neither chunks nor shards. This holds only if no
  worker-written array of the target requires larger ranges. Version 0.1.7 planned the whole axis as one
  range. To choose the range size, pass `--slot-size`.
- A DirectZarr plan covers the stored chunk or shard of every worker-written
  array that the target already has. Plans therefore fit stores that an
  earlier release created. An explicit `--slot-size` that would split one is
  refused. The refusal names the multiple that the slot size needs.
- A `DirectZarrIngestor` run can time out at shutdown while it waits for
  in-flight batches. If workspace cleanup is enabled, Firecube now keeps the
  staged workspace and logs an error that names its path. Before, it deleted
  the workspace under the running writes. Remove the workspace once the writes
  have stopped.
- A staged `DirectZarrIngestor` promotion now refuses a target array that has
  grown into a chunk or shard where the workspace array ends. The target stays
  untouched. Promoting that unit would overwrite cells the run never wrote. If promotion refuses, rerun after the other writer has
  finished.
- The target-growth check runs when promotion starts. If another process grows
  the target after the check and before the files are published, promotion is
  not refused. It can then overwrite cells the run never wrote.
- A staged `DirectZarrIngestor` promotion now refuses a changed data chunk
  that no recorded write of the run covers. The target stays untouched, and the
  error names the chunks. An example is a reused `--workspace` that holds stale
  chunks that differ from the target. Rerun with a fresh workspace. The
  refusal does not apply to the seeded chunks of a managed time coordinate.
- A time-indexed array that ingest workers write, and that declares neither
  `chunks=` nor `shards=`, is now created with one slot per chunk. If Zarr's
  own chunks for the whole array already hold one slot each, Firecube keeps
  them without a warning. Otherwise the array gets time chunk 1, Zarr chooses
  the other dimensions for one slot, and the array is unsharded. Firecube then
  logs a warning that names the chosen chunks. Before, the array got one dense
  chunk.
- An array that declares `shards=` and no `chunks=` keeps its shards. The time
  coordinate and engine-owned arrays keep dense chunks, and existing arrays
  keep their layout.
- Slot-window workers now check the chunk or shard layout stored in the
  target. They refuse a window that covers a stored unit only in part. This
  includes stores that an earlier version created with an undeclared array.
- A slot-window worker now refuses a write to an engine-owned array at a slot
  outside its window. The refusal comes before the worker dispatches the data
  writes of the batch, in direct and staged mode.
- A `DirectZarrIngestor` write intent aimed at an array that `zarr_schema()`
  does not declare now raises `ConfigurationError` before the batch dispatches
  its writes. This holds in direct and staged mode. Setup files under the
  target and `.firecube/` can already exist. Timestamp intents are exempt.
  Before, Firecube wrote such an intent unchecked. Declare the array in
  `zarr_schema()` or stop emitting intents for it.
- `firecube` now requires `zarr>=3.2.1`.
- `firecube` now requires `xarray>=2026.2.0`. Older releases lose values in
  region writes to sharded arrays.
- The dev dependency group now requires pytest 9.0 or later. The test suite
  uses the built-in `subtests` fixture of pytest 9. Update pytest in your
  development environment.
- A staged `DirectZarrIngestor` promotion now publishes a delta instead of the
  workspace tree. It uploads the units the run rewrote and the changed
  auxiliary objects. It neither uploads nor compares the seeded chunks of a
  time coordinate that Firecube manages. The promotion plan marks them
  `engine-managed`.
- A staged `DirectZarrIngestor` promotion never shrinks an array.
- A staged `DirectZarrIngestor` promotion keeps the existing attributes of
  the target when it publishes array metadata.
- A staged `GenericZarrIngestor` promotion is unchanged and still uploads the
  whole workspace tree. A run can rewrite a whole stored unit with only fill
  values. On a local target, the previous data of that unit stays in place.
- A direct-mode `DirectZarrIngestor` run no longer keeps the written and absent unit
  keys of every write in memory. Staged runs record them as before.
- Slot-window (parallel) `DirectZarrIngestor` runs now open each array once
  per writer for region writes, not once per write.
- A slot-window run does not detect growth of an array by another process
  while it runs. An example is `firecube zarr preallocate` run again with a
  larger slot count. Do not change the slot count of a target while a
  slot-window run writes to it.
- On a local target, promotion now replaces each uploaded object whole. The
  replacement is a new file. Its owner is the user that ran the promotion, and
  a symbolic-link destination becomes a regular file.
- A run killed during promotion can leave `.firecube-publish-*.tmp` files in
  the target. Delete them.
- Two failures of concurrent staged runs remain. Claim files that runs
  rewrite while they heartbeat are not replaced whole. A peer that lists claims
  at that moment can still fail with a control-plane corruption error. A
  parallel upload in which one object fails can still hang instead of raising.
- `firecube archive create` no longer prints the codec on the size line of
  its summary. The line now reads `Size: <n> MB`.
- `firecube chunks delete-span` now exits 1 when it reports errors. Before, it
  printed them and exited 0.
- `firecube chunks delete-span` now refuses a CF-encoded time array whose
  dtype cannot represent NaT, unless the array declares a `_FillValue`. This
  covers every integer dtype other than `int64`.
- The dimension check of an existing cube now lists only the top level of each
  group. It runs before every batch of an ingest into an existing cube. Before,
  it listed every object under the group. Its cost no longer grows with the
  number of chunk objects in the store. On a store of 173,283 objects, the
  check fell from about 1.8 s per batch to under 0.01 s. No action is
  required.
- A claim-conflict error now names the owner of the claim. It prints the
  `firecube chunks claims clear` command for the claim. If one recorded run
  owns the claim, it also prints the run, its status and the
  `firecube chunks runs abandon` command for that run. If no recorded run or
  several runs own the claim, the error says so (#86).
- Every `firecube chunks runs abandon` command that Firecube prints now runs
  as printed. The command carries `--product-name` with the product URI and
  `--run-id` with the run. Replace the `<reason>` placeholder. In a script,
  add `--yes-i-really-mean-it` (#86).
- The refusals of `firecube archive create`, `archive restore` and the
  chunk deletion commands now print the `firecube chunks claims clear` command for
  each blocking claim. Before, they named `firecube chunks runs abandon`
  without arguments. If one recorded run owns the claim, they also print the
  status of that run and the abandon command for that run (#86).
- The claim that `firecube zarr preallocate` takes while it sets up the schema
  now carries the id of the preallocate run. Its conflict error names that run.
  If the run cannot record its terminal state, the log names the library
  function that abandons the run, and stderr shows the command. If the run
  cannot release the claim, the log names the library function that clears the
  claim, and stderr shows the command (#86).
- Ingest no longer builds the conflict detail when it retries a claim that
  another writer holds. The intermediate retries of the schema setup skip the
  detail. Its final attempt still builds it, even if the setup then converges
  (#86).
- The reason of `IndexedWriteCompilationError` now names the type and the
  message of the underlying error when a coordinate does not resolve. Update
  any code that matches the old reason text.
- Every option that takes a URI follows one rule. If a script passes a value
  that the rule refuses, change the value.
  - The scheme must be lowercase `file://` or `s3://`.
  - A `file://` URI names no host, not even `localhost`. It names a path below
    the root.
  - An `s3://` URI names a bucket.
  - The command refuses a URI with `?`, `#` or a control character.
  - The command reads a `file://` path as written. It does not decode `%20`.
  - Before, the rule differed between commands. `archive create` read
    `FILE:///tmp/a.tgm` as a path under the working directory. The `chunks`
    commands accepted `gs://` and `file://localhost/`.
- A refused URI names its option. For example, the message reads `Invalid value
  for '--target': URI scheme required (file:// or s3://).` The sentence `Bare
  names are no longer supported` is gone.
- `firecube zarr index show`, `verify` and `rebuild` refuse a `--target`
  without a URI scheme. The other commands that take `--target` already did so.
- `firecube ingest` words its storage-type refusal like the other commands. The
  message now reads `--storage-type '<type>' is incompatible with URI scheme
  '<scheme>'`.

### Deprecated
- The module paths `firecube.core.formats.discovery` and
  `firecube.core.formats.input_filters` emit a `DeprecationWarning` on import.
  Discovery moved to `firecube.core.discovery`; import `discover_input_files`
  from `firecube.core.api`. `firecube.core.formats` still exports
  `discover_input_files` and `KNOWN_EXTENSIONS` without a warning.

### Fixed

- `firecube archive create`, `archive restore`, `archive info`, `archive
  validate` and `archive list` refuse an `--archive` value without a URI
  scheme. They exit with code 2 and name the `file://` form to use. Before,
  `create` and `restore` accepted any bare path. Before, `info`, `validate` and
  `list` accepted a bare path to a `.zip` archive (#17).
- A `file://` URI whose path holds a space, a `%`, a quote or a non-ASCII
  character now names that path. Before, these commands encoded the path and
  used another location, such as a directory named `sp%20ace`:
  - `archive info`, `archive validate` and `archive list`
  - `catalog intake`, `parquet validate` and `parquet consolidate`
  - `advise batch-size`, `advise compliance` and `zarr validate`
- `firecube parquet consolidate` accepts a product path or an output path that
  contains a quote. The command escapes the quote in its SQL statement.
- A URI with a host, a query or a fragment no longer stops `archive create`,
  `archive info` or `zarr index show` with a traceback. These commands exit with
  code 2 and a message.
- An `--archive` value that starts with `~user` for an unknown user no longer
  stops the archive commands with a `RuntimeError`. They exit with code 2 and a
  message.
- Refusals no longer print a recovery command that fails to run. Each command
  now carries the options that it needs and names only options that the
  command has.
  - The refusal for a legacy index record names a working
    `firecube zarr index rebuild` command. The `firecube zarr slots` warning
    for a legacy index record does the same.
  - The refusal of `firecube ingest` for a store shorter than the plugin
    expects names a working `firecube zarr preallocate` command.
  - The advice for an unsorted existing coordinate names a working
    `firecube zarr validate` command.
  - The force advice of `firecube chunks delete-span` names the options that
    the command has.
  - The slot group refusal of `firecube zarr preallocate` no longer names
    `--slot-group`. That command has no such option.
  - The advice of `firecube zarr index verify` names the `firecube zarr index
    rebuild` command with the target, plugin and product name.
  - The advice for a blocked range in `firecube zarr slots` names a working
    `firecube chunks delete-span` command.
- `firecube zarr index rebuild` now prints an error line and exits with code 1
  for an index whose root attrs and record disagree. Before, it printed a
  traceback.
- `firecube zarr index verify` and `firecube zarr slots` now print a recovery
  that works. Before, they told you to run `firecube zarr index rebuild`, and
  that command refuses these states. Each state now has its own text.
  - If the root attrs hold the resolved-index hash and
    `.firecube/index/current.json` is absent, `verify` exits with code 1.
    Restore the file. Or remove the root attrs
    `firecube_resolved_index_identity_hash` and `firecube_resolved_index`,
    then run `firecube zarr index rebuild` with the plugin that wrote the data.
  - If the root attr `firecube_resolved_index_identity_hash` differs from the
    record, `verify` exits with code 1. Remove that attr, then run
    `firecube zarr index rebuild`. The command writes the attr again from the
    record.
  - If the plugin resolves an index that differs from the record,
    `firecube zarr slots` warns. Use the plugin that wrote the product, or a
    new target.
- `firecube zarr consolidate-time-coord` now reports a backfilled
  `ConsolidatedTimeCoord` event only after Firecube records the event. Before,
  it reported the backfill before the record. A failed record could then leave
  a report of work that did not happen.
- `firecube zarr consolidate-time-coord` now refuses to rewrite and reseal a
  coordinate that carries a `firecube_coord_managed` stamp. It refuses even when
  consolidated metadata, at the root or in any group below it, predates the
  stamp. The command now reads the live metadata of the root and of every group
  below it. Before, it could read consolidated metadata that predated the stamp
  and rewrite the coordinate.
- `firecube archive restore` of a Tensogram archive no longer creates
  consolidated metadata. `firecube zarr multires` no longer creates it either.
  Before, both commands wrote it at the root of a store that had none.
  - If the store already has consolidated metadata, both commands rewrite it
    after the last write. The rewritten metadata lists every group and array
    that they wrote.
  - If you open a restored store with the xarray defaults, the open works.
    xarray warns that it fell back to the slower read without consolidated
    metadata. To silence the warning, pass `consolidated=False`.
- These operations now read the own metadata of each array and each group. A
  consolidated snapshot that is older than the store no longer decides what
  they do. Append resume and `firecube archive create` have exceptions, which
  the items below name:
  - Append resume. It reads the live array metadata when it determines the
    initial write cursor. An append into a store whose snapshot predates growth
    no longer resumes at the old length and overwrites slots. Classification of
    the slots can still use the own snapshot of a group.
  - Staged resume. It no longer limits seeding to the length that a snapshot at
    the root records. The own snapshot of a group no longer makes it skip
    seeding. Before, a staged resume that skipped seeding could publish fill
    values over target cells next to the new data.
  - The state update of a span deletion.
  - `firecube zarr validate` and `firecube zarr compare`.
  - `firecube archive validate` on a zip archive. It now reports a child
    document that does not parse.
  - `firecube archive create`. A group whose arrays carry no dimension names
    is still read through the own snapshot of that group, if it has one.
  - The order of a new Tensogram archive. `firecube archive create` now writes
    the groups in name order. In each group it writes the data variables in
    name order, then the coordinates in name order. `firecube archive info` and
    `firecube archive list` show the same order on every run. Archives made
    before keep their order.
  - `firecube advise compliance` and `firecube catalog intake`.

  `firecube archive info` and `firecube archive list` of a zip archive show
  the live metadata. Before, each of these operations could act on the
  snapshot.
- `firecube archive restore --overwrite` of a `.tgm` archive now checks the
  archive before it deletes anything. It reads the archive version and runs the
  quick structural check of every message. If the archive fails a check, the
  command exits with an error. It deletes and replaces no existing data. It
  can already have written the control-plane records of the restore: its claim,
  its run under `.firecube/runs/archive-restore-*/`, and
  `.firecube/schema.json` if that file was absent. Before, it deleted the
  target first and then failed on a bad archive.
  - On a local target, the overwrite keeps `.firecube/claims/`,
    `.firecube/schema.json` and the records of its own run. It deletes the rest
    of the old target. The restore releases its claim when it ends.
  - If the command cannot record a failure or release the claim, it prints a
    `Warning:` line. Before, such an error replaced the restore error. The
    warning does not replace it now. A failure to record the start of the run
    still surfaces as the error itself.
  - If a symlink is the target, or lies on a kept path under `.firecube/`, the
    command refuses the overwrite and deletes nothing.
  - On a local target, a restore that stops during the replacement leaves its
    run in the `started` state. Check the status with `firecube chunks runs
    list`. Abandon the run with `firecube chunks runs abandon` only if it is
    still `started`. Then run the restore again with `--overwrite
    --yes-i-really-mean-it`.
  - The command treats a remote target as before. A remote `.tgm` root restore
    replaces the whole store.
- `firecube chunks runs abandon` now releases the write claims that the run
  alone owns. Before, the claims stayed and a resume after a killed writer
  failed with a claim conflict (#86). The command prints `Released claim
  <domain>` for each claim it deletes. It prints `Left claim <domain>` for a
  claim it keeps, because the owner matches more than one run or the claim
  changed. Clear such a claim with `firecube chunks claims clear`. Abandoning
  a run that is still alive also removes its slot guards. Its heartbeat writes
  the claim again at its next beat. The beat interval defaults to 30 s. The
  claim can therefore return within seconds of the abandon. Stop the run
  before you abandon it.
- If `firecube chunks runs abandon` cannot list the claims, it now exits 1 and
  prints the error. Before, it reported success and left the claims in place.
  Firecube has already recorded the run as abandoned. Run the same command
  again to release the claims (#86).
- `firecube chunks runs abandon` on a run that is already terminal now
  releases the claims that the run still owns. Before, it released nothing.
  The run keeps its status (#86).
- A committed `firecube chunks runs abandon --all-stale --yes-i-really-mean-it`
  now releases the claims of runs that an earlier abandon ended and a crash
  interrupted. It lists the claims once and prints each `Released claim` and
  `Left claim` line, also when it abandons no run. If it cannot list the
  claims, it exits 1 and abandons no run. Run it again (#86).
- The `--all-stale` sweep releases a claim only when the claim is stale. A
  claim is stale 120 s after its last heartbeat. The sweep checks this again
  just before it deletes the claim. It keeps a fresh claim and prints
  `Left claim <domain>`. A process that still renews a claim keeps it. Stop
  that process. Then run `firecube chunks runs abandon` for the run, or
  `firecube chunks claims clear --force` for the claim (#86).
- Without `--yes-i-really-mean-it`, the `--all-stale` sweep is a dry run. It
  deletes nothing and abandons no run. It now also lists the claims. It prints
  `Would release claim <domain>` for each claim that a committed sweep
  releases. It prints `Would leave claim <domain>` for each claim that the
  sweep keeps. It lists only kept claims whose owner includes a terminal run
  or a run in the sweep (#86).
- `firecube zarr slots` no longer holds the whole plan in memory. It writes
  each range as it computes it, so the number of ranges does not change its
  memory use. A plan of 44 million ranges needed more than 100 GB.
  The remaining-coverage intervals and the resolved index stay in memory.
- `DirectZarrIngestor` no longer loses slots silently when writers share a
  chunk or shard along time. The writers are slot-window workers,
  `pipeline_workers` threads in one process, and staged promotion. Firecube
  now refuses windows that would split a stored unit (see Changed).
- A write to a slot outside a 1-D array now raises `IndexError`. Before,
  Firecube ignored the write.
- A window that splits a shard of a group that declares shards and no chunks
  is now refused at the start of the run. The refusal comes before any array
  data or metadata is written to the target, in direct and staged mode.
  `.firecube/` run records can already exist.
- Writers in one process now serialize per stored unit when they address a
  local store in one of these ways:
  - by a file path or a `file://` URI
  - by a symbolic link
  - by a supplied `LocalStore`

  Firecube keys other supplied stores by their URI label. Two spellings of
  one remote store, such as `s3://bucket` and `s3://bucket/`, still take
  separate locks.
- With `pipeline_workers` of 2 or more, two batches of one run could carry
  different payloads for one static array (`WriteIntent.static`). This
  happened in direct and staged mode. Both batches could succeed, and the later one silently
  replaced the earlier. The check, the write and the marker stamp of a static
  array now run under one lock per array in the process. The batch that finds
  the array already written compares its payload. It fails with
  `SchemaDriftError` if the payload differs, as a resume does. Which payload
  stays depends on which batch takes the lock first.
- A `GenericZarrIngestor` refill or overwrite (`force_reingest`) could lose
  values written within one stored chunk. This happened when Dask computed the
  pieces of that chunk as separate tasks, which left some slots with their
  old values. Firecube now aligns the Dask tasks of a region write to the
  stored chunks. The required xarray release aligns them to shards as well, so
  a write whose inner chunks share a shard keeps every value.
- A payload or static write intent aimed at a preallocated or coord-managed
  time coordinate is now verified against the stored values. Identical values
  cause no write. Different values raise `SchemaDriftError`. Before, the
  intent overwrote the stored values.
- Staged runs now refuse to promote a chunk or shard that they do not write
  completely. This covers sparse windows, serial runs, and partially written
  rows or channels. Before, promotion reset the unwritten cells to the fill
  value.
- A staged `DirectZarrIngestor` run can rewrite a whole chunk or shard with
  only the fill value. Promotion now replaces the old data of that unit in the
  target, so it reads back as the fill value. Before, promotion kept the old
  values. This holds only when `zarr_write_empty_chunks` is off, the default.
- `DirectZarrIngestor`'s `zarr_write_empty_chunks` option no longer changes
  the process-wide configuration of Zarr. Concurrent writers with different
  settings no longer receive each other's.
- Concurrent staged runs into one local target no longer fail with Zstd or
  JSON decode errors from reading a half-written file. Each uploaded object
  now replaces the old one whole, so a reader sees the old or the new object.
  S3 and GCS uploads are unchanged. See the "Run Parallel Zarr Writes"
  operations guide.
- `firecube chunks delete` no longer tries to delete span, run, and schema
  records from storage: their keys are not storage objects. On local products
  the attempt failed with "Missing local target base", and on S3 it deleted
  nothing. The command now reports how many matched records have no storage
  objects and points to `firecube chunks delete-span` for span data.
- `firecube chunks delete-span` works on cubes with static data variables
  such as `lat_bnds` and `lon_bnds` (#83). Arrays without the span's time
  dimension are skipped and listed in one warning per span. Before, they
  failed the delete. A span in which no array carries the time dimension is
  refused. An array with unknown dimensions is also refused.
- `GenericZarrIngestor` span records no longer list static data variables.
  They list time-indexed variables only.
- A `delete-span` fill that fails its pre-flight checks now changes nothing.
  The checks find a missing listed array, a range outside an array, and a
  state array that does not fit. Before, the data variables could be
  NaN-filled while the state array still read `present`. A storage failure
  during the write itself is not rolled back.
- `delete-span` fills CF-encoded int64 and float time arrays (`units`
  containing `since`) with NaT. Before, it wrote the Zarr fill value of the
  array, normally `0`. That value decodes as the reference epoch, a valid date.
- Resuming or force-reingesting into a DirectZarr store no longer fails with
  `IndexError` when the plugin writes a 0-d static array. An example is a CF
  `spatial_ref` grid mapping (#84). The replay check now reads static arrays
  of any rank.
- A rank-1 time-indexed array that declares `shards=` but not `chunks=` now
  gets inner chunks that divide its shards, at most 256 slots each. This
  applies to arrays that ingest workers write, to engine-owned arrays
  and to the time coordinate. Existing arrays keep their layout, and
  preallocate still accepts them.
- Before that fix, `firecube zarr preallocate` gave an array of this kind
  `min(256, slots)` slots per chunk. The array was a worker-written array or a
  time coordinate on an exact or irregular axis. This failed with "not
  divisible by the shard's inner chunk size" if that number did not divide the
  shards. An example is 512 slots in shards of 100. Ingest without
  preallocate let Zarr choose, which failed the same way for many shapes.
- An observed time coordinate (`RegularTimeAxis(mode="floor")`) that
  preallocate creates is the exception to that fix. It is unsharded with
  `min(256, slots)` slots per chunk, and its declared shards are not used, as
  before.
- A serial or staged `DirectZarrIngestor` run with `pipeline_workers` above 1
  could overwrite slots that another batch had just written. This happened
  when the time chunk of an array holds more than one slot and the array grew
  during the run. Each region write and each 1-D slot write took its array
  handle before it took its locks. Each now reads the shape of the array while
  it holds its locks. A write to a slot beyond the extent of the array is
  refused with `IndexError` after the locks are taken, and nothing is written.
- In a serial staged run, a write with a negative `ts_index` could overwrite
  target cells the run never wrote, if the array grew afterwards. The
  run recorded the write against the wrong array shape. Firecube now resolves
  a negative `ts_index` once, against the workspace array, before the batch
  records its footprint. A negative `ts_index` into an array that the
  workspace does not hold yet is now refused with `ConfigurationError`. Emit an
  absolute slot instead.
- On S3 with the fsspec storage driver, the dimension check no longer fails
  with `FileNotFoundError`. It failed when another process created an array
  after this process had listed the store. The check now lists each group
  again and reads the `zarr.json` file of each array directly. No action is
  required.
- Firecube now refuses a Gregorian time value outside the `datetime64[ns]`
  range, about 1677-09-21 to 2262-04-11. Before, a value handed to a Gregorian
  axis or written to a Gregorian coordinate wrapped to a wrong date without an
  error. For example, 2500-01-01 became 1915-06-14. The error names the range.
- `firecube archive create --archive-format tensogram` now refuses a store
  with a non-Gregorian time coordinate. The error names the coordinate.
  For a plain Zarr store, the command records nothing. For a Firecube
  product, it attempts to record the run as failed. Before, the command
  dropped the coordinate and wrote an archive without a time axis.
- A second ingest run into a store that holds calendar-valued time objects no
  longer crashes. Before, the run failed and recorded null coverage bounds.
- `firecube zarr consolidate-time-coord` now leaves a preallocated
  calendar time coordinate unchanged.
- `firecube zarr validate` no longer reports the unwritten slots of an encoded
  calendar coordinate as duplicates. It still reports a repeated value on every
  other coordinate.

## [0.1.7] - 2026-09-19

### Added

- `--storage-anonymous` presence-only flag on the storage option group and matching
  `FIRECUBE_S3_ANONYMOUS` environment variable for opting into unsigned S3
  requests. Precedence follows the existing storage-option shape: CLI flag,
  then env var, then `[storage].anonymous` in the config file, then the
  default (`false`). To force authenticated access when env or config
  requests anonymous, unset the env var or edit the config; there is no
  `--no-storage-anonymous` opt-out flag.
- `anonymous` field in the `[storage]` config-file section, and matching
  public `StorageConfig.anonymous` and `StorageDriverConfig.anonymous`
  fields for programmatic runs. Both are importable from
  `firecube.core.api`.

### Changed

- `discover_input_files` and `WorkspaceManager._materialize_remote_uri` now
  route source reads through the configured storage driver via the internal
  `open_source_filesystem` helper. `--storage-driver obstore` now covers
  source-side discovery and remote-input materialization; previously it
  applied only to the write-domain Zarr store.
- Concurrent remote materialization uses a per-URI lock instead of a single
  process-wide lock. Distinct source URIs now materialize in parallel; an
  identical URI is still deduplicated to one download and its cache entry
  is shared with waiters. A failing download releases its per-URI lock so
  a retry can proceed.
- `IngestResult.storage_result` reports `files_written=0` and
  `bytes_written=0` for direct-mode S3 runs, matching direct-mode local
  runs. It previously held totals for the whole store. Manifest, CLI
  output and metrics are unchanged (they were already `null` for direct
  runs).
- With `force_reingest=true` the "Non-terminal run(s) exist ... proceeding"
  warning is no longer logged and the resume-guard log line reports
  `runs_enumerated=0`.

### Fixed

- Direct-mode ingests to S3 no longer list the entire target store when a
  run completes. The listing grew with the number of objects already in
  the store and its result was never reported; on large stores it
  dominated run time.
- The resume guard no longer reads every run record to look for a
  time-coordinate seal; it reads the seal's own record.
- With `force_reingest=true` the resume guard no longer scans all run
  records for non-terminal runs, since the result was ignored in that
  mode.
- `firecube chunks claims clear --domain X` and `--all-stale` now work when
  the store filename differs from the logical product name (e.g. dated or
  versioned stores like `<name>_20260501.zarr`). Previously the command
  compared the logical product name recorded in the claim with the store
  name taken from the URI and failed with
  `ValueError: claim domain '...' does not belong to product '...'`
  whenever the two differed. `firecube chunks claims list` was unaffected.
  Operators can now recover stale claims through the CLI without
  out-of-band recovery workarounds.
- `firecube catalog intake` no longer hides read errors behind
  "No catalogable dataset groups found". If the store cannot be listed at
  all (bad credentials, unreachable endpoint), the command now fails with
  the underlying error. If an individual group cannot be read (e.g.
  malformed metadata), the underlying exception is logged at WARNING level
  with the store URI, so operators can see why it was skipped. Groups
  that are readable but need the plain-Zarr fallback do not produce
  warnings.
- `firecube catalog intake` generated Zarr entries now use `chunks: {}`
  instead of `chunks: "auto"`. Stores with object-dtype variables (e.g.
  string metadata) previously failed to open from the generated catalog
  with `Can not use auto rechunking with object dtype`. Datasets still
  open lazily and dask-backed, now using the store's on-disk chunking;
  pass a different `chunks` value when opening the entry to override.
- Anonymous S3 buckets are now readable without providing empty-string
  credentials or setting `AWS_SHARED_CREDENTIALS_FILE=/dev/null`. With
  `--storage-anonymous` (or the matching env or config setting), fsspec receives
  `anon=True` and obstore receives `skip_signature=True`. Anonymous mode
  applies only to `s3://` URIs; `file://` targets ignore it.
- Concurrent remote input materialization no longer serializes distinct
  downloads through a single process-wide lock, so parallel workers
  fetching different source URIs no longer wait on one another.
- `--storage-anonymous` is now honored through the `ChunkManager` binding
  round-trip; previously `storage_config_from_binding` dropped the flag,
  silently breaking anonymous S3 reads during real ingest.
- `--storage-anonymous` combined with `--storage-type local` and an `s3://`
  source now works; a target-type gate in `StorageDriverConfig.from_storage_config`
  previously zeroed the flag for non-S3 target types.
- `_s3_fs_kwargs_from_storage_config` restored URI-based behavior; legacy
  callers that pass a `storage_type="local"` config with an `s3://` URI
  receive correct fsspec kwargs again.
- `firecube catalog intake --storage-anonymous` generates `storage_options`
  with `anon: true`; the generated YAML previously hardcoded `anon: false`.
- `discover_input_files` with `--storage-driver obstore` on a single-object
  `s3://` source now returns the object; obstore previously returned an
  empty list for single-object prefix queries.
- `discover_input_files` on a `file://` source now returns plain paths;
  previously it returned `file://` URIs, breaking `local_path()` resolution
  and HDF5 content sniffing.
- Concurrent `_materialize_remote_uri` calls for the same URI use
  reference-counted per-URI locks; a failed first download no longer
  causes a race condition where subsequent threads skip the retry.

## [0.1.6] - 2026-09-16

### Added

- Showcase notebooks for Sentinel-3 fire detections, a Firecube 101 NetCDF-to-Zarr
  walkthrough, and MTG FCI L1C parallel ingestion benchmarks, with saved outputs
  and downloadable notebooks.
- Optional `firecube[patterns]` extra and
  `firecube.ingestor.extensions.parse_pattern(pattern, text)` helper for parsing
  filename fields with Trollsift, preserving field names and types.
- `GenericZarrIngestor.write_lock` context manager for plugin hooks that read
  the target store while batches are being written.
- Run metrics report skipped timestamps and batches not attempted after a
  failure. Dashboards calculating batch totals must include
  `batches_not_attempted` alongside succeeded and failed batches.
- Optional coverage fields record the stored chunk length and the ranges and
  repair results of failed writes. Readers of control-plane event files must
  tolerate absent `chunk_len_used`, `time_index_ranges`, and `meta.repair` fields.

### Changed

- `firecube plugins create` rejects plugin names that do not start with a letter
  or that contain characters other than letters, digits, `-`, and `_`. Class names
  derive from the snake-case plugin ID, so `MyCoolPlugin` becomes
  `MyCoolPluginIngestor` and `HTTPServer` becomes `HttpServerIngestor`.
  `--write-strategy` with `--template parquet` or `base` is a usage error.
- Generated plugin projects declare `license` as an SPDX expression string, and
  their registration test loads the `firecube.plugins` entry point. The Zarr
  templates include a `ZarrStorageConfig` class with commented chunking,
  compression, and sharding settings, and the generated README covers input
  filters, reruns, output groups, and, for DirectZarr, parallel slot writes.
- The generated `GenericZarrIngestor` declares the time dimension once as an empty
  `TIME_DIM` constant, and ingestion stops with `NotImplementedError` until the
  author sets it. Its `read_dataset` reader carries a commented two-line NetCDF
  example, and it links the guides for zip archives, filename fields, and paired
  files, which now target that reader.
- Development tooling uses ruff 0.16.5, which also formats Python code blocks in
  Markdown.
- `GenericParquetIngestor` requires a fresh target for each run. Resume and
  force-reingest are refused; preserve existing targets and write to a new URI.
  Runs retain exclusive ownership through promotion, and duplicate or
  out-of-product part paths are rejected.
- Source discovery uses `--input-filters` on ingestion, slot planning, and
  preallocation. Positive patterns add files to built-in discovery; `!` patterns
  exclude files and always take precedence. Explicit patterns are case-sensitive,
  including for single-file inputs; built-in suffix matching remains
  case-insensitive. CLI filters replace saved filters, and `--input-filters '[]'`
  restores the defaults.
- `GenericZarrIngestor` commits batches in order and stops after the first failed
  batch to avoid appending past a gap. With `pipeline_workers>1`, batch
  preparation runs in parallel. Failed and unattempted batches are reported
  separately.
- Retrying a failed run requires `--option resume_existing=true` to keep
  successful writes and retry incomplete data, or `--option force_reingest=true`
  to overwrite matching timestamps. Neither option bypasses timestamp or schema
  constraints.
- Sharding configurations with `zarr_sharding=true` and `zarr_chunk_shape` now
  require `zarr_shard_shape`. Add a shard shape or disable sharding.
- Non-empty `zarr_time_encoding` settings, including `time_encoding` returned by
  `get_zarr_config()`, raise a configuration error because they are unsupported.
- `zarr compare` returns exit code 1 instead of 3 for data mismatches.
  Layout-only differences produce a warning and exit code 0. Update wrappers
  that check the previous mismatch code.
- Direct-mode run summaries report null upload counters (`files_written`,
  `bytes_written`, and `duration_s`), including in `metrics.storage`.
  Consumers must handle these values as optional; staged runs report upload
  measurements.
- Chunk-alignment warnings appear once per group and chunk length, followed by
  a run summary. The final short write is exempt only on the last batch.
- The Tutorials navigation section is now Showcase. Plugin-author guides cover
  source discovery and filters, archive and paired-file readers, and Zarr
  layout configuration. Examples now live under Showcase.
- Firecube now requires `packaging>=24.2` for SPDX license validation.

### Removed

- `include_patterns` CLI, configuration, and SDK settings. Replace
  `--option 'include_patterns=["*.csv"]'` with `--input-filters '["*.csv"]'`
  and rename the configuration or SDK key to `input_filters`. Custom discovery
  hooks must apply these filters explicitly, for example by forwarding
  `self.engine_config.input_filters` to
  `discover_input_files(..., preferred_globs=...)`.
- The private `GenericZarrIngestor._write_lock` attribute. Plugin hooks must use
  the public `write_lock` context manager.

### Fixed

- Generated plugin projects pass `ruff check` with current ruff, and quotes in
  `--author` or `--license` no longer produce an invalid `pyproject.toml`.
- The generated Zarr ingestor raises when `read_dataset()` returns data without
  the `time_dim_name` dimension instead of writing a mostly empty cube, the
  generated DirectZarr ingestor fills unwritten slots with NaN instead of 0.0, and
  the generated base ingestor refuses a missing or remote target instead of
  writing to the current directory.
- Staged ingestion now publishes every batch to local and S3 targets. Previously,
  only the last batch reached the target, causing data loss.
- Staged appends into a chunk that already holds data keep the existing slots.
  Previously, appending or force-reingesting one slot of a multi-slot chunk in
  staged mode replaced its neighbours with fill values and reset their state
  markers.
- Variables without the time dimension, such as bounds, are stored once instead
  of being copied onto every time step. Generated plugins pin
  `data_vars="minimal"`, `coords="minimal"`, `compat="equals"`, and
  `join="exact"` in `xr.concat`. A later run whose static variable differs
  from the stored values, or adds a new one, raises `SchemaDriftError` naming
  the variable, on append and on force-reingest alike; add the time dimension
  to the variable or create a new store.
- Group attributes keep their first-write values across appends. The second
  batch of a fresh staged run previously wrote empty attributes. When a later
  append batch carries different attributes, the run logs one warning listing
  the changed keys and keeps the stored values; tuple-valued attributes no
  longer trigger that warning.
- Chunk seeding is idempotent for arrays that use the dot chunk-key separator
  as well as the default slash form.
- The post-write integrity check verifies the coordinate values of touched
  slots as well as their state markers, so a store whose timestamp coordinate
  was overwritten no longer promotes silently.
- `GenericZarrIngestor` force-reingest overwrites matching timestamps in place,
  refills deleted or failed slots, and appends new tail timestamps. Resume skips
  present timestamps and refills incomplete slots, including batches that combine
  overlap with new data.
- Failed append batches mark overwritten slots as failed, truncate partial
  tails, remove incomplete new groups, and restore the resume position.
  Successfully committed groups remain available, and retries recover both the
  overwritten region and appended tail.
- Append and overwrite operations include time-aligned auxiliary coordinates
  and reject missing or extra arrays before writing. Datetime values that cannot
  round-trip through the stored encoding are rejected.
- Append operations reject duplicate, missing, or unsorted timestamps and
  non-contiguous overlaps. New timestamps must follow the committed maximum,
  including earlier batches in the same staged run; insertion into an existing
  time axis is refused.
- Legacy stores missing the timestamp-state array now report a typed refusal
  with recovery guidance. A direct run with `--option resume_existing=true`
  upgrades the group; read-only use treats existing timestamps as present and
  logs a warning.
- Coordinates retain their source CF encoding when chunk settings are applied.
  The configured time chunk length also applies to the time coordinate and
  timestamp-state array in non-sharded writes.
- Coverage and deletion alignment use stored chunk sizes. Split overwrite and
  append batches produce one coverage record per batch and group.
- `zarr validate` returns exit code 1 for invalid stores, inspects every array
  in the requested group, and accepts unwritten slots in sparse DirectZarr
  stores. Omitted fill-only chunks are reported as informational.
- Failed ZIP extraction preserves pre-existing and shared destination
  directories. Cleanup removes partial output only from fresh destinations
  owned exclusively by the failed extraction; existing or shared directories
  may retain partial writes.
- Span replacement chooses the most recently started run and tolerates omitted
  fill-only chunk keys. `chunks list --include-replaced` includes failed-run
  spans and removes duplicate history records.
- Chunk deletion lists other affected spans before deleting shared chunks and
  requires `--yes-i-really-mean-it`. Completed region fills are skipped unless
  `--force` is passed. Invalid state ranges are reported as errors and leave
  the affected span active.
- Region deletion reads current array metadata, so stale consolidated metadata
  no longer hides arrays. Staged metadata initialization reports unexpected
  failures consistently.
- `chunks delete --time-range` filters by data timestamps. Help and no-match
  messages distinguish data time from record time and show the accepted date
  range format.
- `zarr compare` accepts inferred storage settings without explicit storage
  flags.
- Built-in discovery includes `.hdf`, `.he5`, and `.nc4` files. Empty inputs
  raise a configuration error before ingestion unless empty sources are allowed
  or a slot range is active. Discovery runs once per ingestion run.
- Plugin scaffolds honor author and license arguments and recommend editable
  installation. License values are normalized using SPDX identifiers and
  expressions; unknown values receive a `LicenseRef-` prefix. Plugin installation
  output identifies copied and editable installs.
- CLI errors provide valid JSON-list examples, required write-mode guidance,
  and actionable recovery options for failed runs. Routine schema checks and
  chunk listings produce less log noise.
- Ingestion no longer implies support for `--option dry_run=true`: ingestion
  writes normally. Use `--dry-run` on supported deletion and preallocation
  commands to preview those operations.

### Stats

- pytest: full local suite green under `-W error::DeprecationWarning`
  (`not s3` and `not race` skips only); `not slow and not s3 and not race`
  lane: 3851 passed, 5 skipped, 86% line coverage.
- pyright: 0 errors, 0 warnings.
- ruff: `check` and `format --check` clean.
- docs: `mkdocs build --strict` succeeds.
- package: `uv build` produces the `0.1.6` wheel + sdist; `twine check` PASSED.

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

[Unreleased]: https://github.com/eumetsat/firecube/compare/v0.1.7...HEAD
[0.1.7]: https://github.com/eumetsat/firecube/compare/v0.1.6...v0.1.7
[0.1.6]: https://github.com/eumetsat/firecube/compare/v0.1.5...v0.1.6
[0.1.5]: https://github.com/eumetsat/firecube/compare/v0.1.4.post1...v0.1.5
[0.1.4.post1]: https://github.com/eumetsat/firecube/compare/v0.1.4.post0...v0.1.4.post1
[0.1.4.post0]: https://github.com/eumetsat/firecube/compare/v0.1.4...v0.1.4.post0
[0.1.4]: https://github.com/eumetsat/firecube/releases/tag/v0.1.4
