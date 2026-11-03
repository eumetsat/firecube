# Control-Plane Specification

This page specifies the on-disk format of the `.firecube/` control plane that
Firecube maintains next to every product it writes. It is a format
specification, not a guide: a reader with no Firecube installation can parse
`.firecube/` from this page alone. The key words MUST, MUST NOT, and MAY are
normative.

Two format versions apply, and every JSON document carries its own
`schema_version` field:

| Surface | Version |
|---|---|
| Control-plane envelope: `schema.json`, run records, WAL events, compact cache, `LATEST.json` | `v2` |
| Index records: `index/current.json`, `slot_index/current.json` | `v1` |

## Layout

The control plane lives inside the product root, in a directory named
`.firecube`. All paths below are relative to that directory.

```text
.firecube/
    schema.json                  Control-plane format descriptor, written once
    runs/<run_id>/run.json       Run record, one directory per run
    runs/<run_id>/events-NNNNN.jsonl   WAL segments for that run, NNNNN from 00000
    claims/<digest>.json         Write claims, one file per claimed write domain
    compact/compact-<generation>.jsonl       Projected chunk records
    compact/compact-<generation>.meta.json   Compact cache metadata
    compact/compact.lock         Lock file of a local rebuild
    compact/LATEST.json          Pointer to the newest compact cache generation
    index/current.json           Resolved-index record
    slot_index/current.json      Slot-index model record
```

Only the directories a product has used exist. A serial ingest without a
declared index has no `index/` directory. A product with no compact cache
rebuild has no `compact/` directory.

Earlier releases wrote `snapshots/snapshot-<generation>.jsonl`,
`snapshots/snapshot-<generation>.meta.json` and a `LATEST.json` at the root of
`.firecube/`. Firecube writes none of them now. Firecube ignores them on read.
A reader MUST NOT use them. An operator MAY delete them.

## Atomicity

Writers publish every control-plane JSON document atomically, except the compact
pointer. A reader that can see a file MUST be able to parse it as complete
JSON. Writers publish through
a temporary object followed by an atomic rename (local filesystems) or a
conditional put (object stores). Creation-only writes (`schema.json`, claim
files, `index/current.json`, `slot_index/current.json`) fail instead of
overwriting when the file already exists; `run.json` is replaced atomically in
place because peer processes read it while the run is live.

The compact pointer `compact/LATEST.json` is the exception. A rebuild writes it
in place with a plain write. A reader can see an incomplete pointer during that
write.

## `schema.json`

Written once, when the first run initializes the control plane. Never
rewritten.

| Key | Type | Meaning |
|---|---|---|
| `schema_version` | string | `"v2"` |
| `layout` | string | `"chunkmanager-v2"` |
| `created_at` | number | Unix timestamp (seconds) of first initialization |

## Run Records

Each run owns the directory `runs/<run_id>/`. Its `run.json` is the run's
liveness and resume record, heartbeat-refreshed while the run is active.

| Key | Type | Meaning |
|---|---|---|
| `schema_version` | string | `"v2"` |
| `product` | string | Logical product name |
| `run_id` | string | Run identifier, equals the directory name |
| `status` | string | Run status; terminal states are recorded by WAL events |
| `parts` | integer | Number of sealed WAL segments |
| `events` | integer | Total WAL events written so far |
| `started_at` | number | Unix timestamp (seconds) |
| `updated_at` | number | Unix timestamp of the last heartbeat |
| `run_uri` | string | Absolute URI of the run directory |
| `run_stale_threshold_s` | integer | Seconds after which a silent run counts as stale |
| `error` | string | Present only on failure |
| `completed_at` | number | Present only after completion |
| `slot_range` | array | Present only for slot-range parallel runs: `[start, end)` |
| `slot_group` | string | Present only for slot-range parallel runs |

A run whose `updated_at` is older than `run_stale_threshold_s` MAY be treated
as abandoned by recovery tooling. See
[Recover a Product](../operations/chunk-manager/recover.md).

### Producer record: `meta.producer`

A run records which software produced it. The record is the object
`meta.producer` in the `meta` of the WAL events listed below. It is not a key
of `run.json`. Firecube captures it once, when the run starts.

| Run kind | Events that carry `meta.producer` |
|---|---|
| Ingest | `run_started` and `run_failed` |
| Preallocate | `run_started`, `run_completed` and `run_failed` |

An ingest `run_completed` event does not carry it. No span event carries it.
No other run kind records it. A reader MUST treat an absent `meta.producer` as
unknown.

| Key | Type | Meaning |
|---|---|---|
| `schema_version` | integer | `1` |
| `captured_at` | string | ISO 8601 UTC timestamp of the capture |
| `plugin` | object | The plugin that ran, see below |
| `source` | object | Where the plugin code came from, see below |
| `firecube` | object | `version`: the Firecube version string |
| `config` | object | The digest of the run configuration, see below |

The `plugin` object has these keys:

| Key | Type | Meaning |
|---|---|---|
| `name` | string | Registered plugin name |
| `distribution` | string or null | Name of the installed distribution that provides the plugin |
| `version` | string or null | Version of that distribution |
| `entry_point` | string or null | Value of the `firecube.plugins` entry point that loads the plugin |

The `source` object has these keys:

| Key | Type | Meaning |
|---|---|---|
| `kind` | string | `vcs`, `editable`, `wheel` or `unknown` |
| `commit` | string or null | Commit identifier |
| `requested_ref` | string or null | Branch, tag or revision that the installer requested |
| `dirty` | boolean or null | Whether the working tree has uncommitted changes |
| `evidence` | array of strings | The facts that Firecube read to decide the other keys |

The four values of `source.kind` mean:

- `vcs`: the distribution was installed from a version-control URL. `commit` is the installed commit. `dirty` is `false` when `commit` is set, else `null`.
- `editable`: the distribution is an editable install. `commit` and `dirty` come from Git in the source directory. Each is `null` if Git cannot answer.
- `wheel`: the distribution was installed from an archive or a built directory, or it has no `direct_url.json`. `commit` and `dirty` are `null`.
- `unknown`: Firecube found no distribution for the plugin. It is also the kind when the `direct_url.json` is unreadable, is not JSON, is not an object, or has no recognised source metadata. `commit` and `dirty` are `null`.

The `config` object has these keys:

| Key | Type | Meaning |
|---|---|---|
| `algorithm` | string | `"sha256"` |
| `digest` | string | Hash of the canonical JSON of the engine, template and plugin configuration |
| `canonicalization` | string | `"firecube-config-json-v1"` |
| `excluded` | array of strings | Dotted paths of the values left out of the digest |

The digest leaves out each configuration field that has `repr=False`, each
free-form `x_*` key, and each value with no JSON form. Storage credentials are
not in the digest input.

A `null` value means unknown. It never means false or absent. A `dirty` of
`null` means that Firecube could not check the working tree. Firecube sets
`dirty` to `false` only when it has evidence.

### WAL events

The write-ahead log is the authoritative, append-only record of what happened
to the product. Events are JSON objects, one per line, in segment files
`events-00000.jsonl`, `events-00001.jsonl`, and so on under the run directory.
Segments are immutable once sealed. Every event carries this envelope:

| Key | Type | Meaning |
|---|---|---|
| `schema_version` | string | `"v2"` |
| `event_id` | string | Unique event identifier |
| `event_type` | string | One of the event types below |
| `product` | string | Logical product name |
| `run_id` | string | Emitting run |
| `timestamp` | number | Unix timestamp (seconds) |
| `record` | object | Event-type-specific payload |
| `meta` | object | Optional; omitted when empty |

Event types:

| `event_type` | Emitted when |
|---|---|
| `run_started` | A run begins |
| `run_started_with_replacement` | A run begins and will replace existing chunks |
| `run_completed` | A run ends successfully |
| `run_failed` | A run ends with an error |
| `run_abandoned` | A stale run is marked abandoned |
| `span_committed` | A write span commits |
| `span_failed` | A write span fails |
| `span_noop` | A write span is skipped as already satisfied |
| `record_replaced` | A chunk record is replaced |
| `record_upsert` | A chunk record is inserted or updated |
| `replacement_committed` | A replacement transaction commits |
| `maintenance_started` | A maintenance operation begins |
| `maintenance_completed` | A maintenance operation ends successfully |
| `maintenance_failed` | A maintenance operation fails |
| `schema_verification` | A store schema check is recorded |
| `index_ensured` | A resolved index is written, matched, rebuilt, or refused |
| `slot_index_model_recorded` | A slot-index model record is first written |
| `slot_index_model_verified` | An existing slot-index model record is verified |
| `consolidated_time_coord` | A time-coordinate consolidation rewrites an array |

The `index_ensured` payload records `run_id`, `product`, `identity_hash`, the
sorted `axis_kinds` and `groups` of the declaration, an ISO 8601 `timestamp`,
and an `outcome` that is one of `created`, `matched_existing`,
`conflict_refused`, or `rebuilt`.

## Claims

A claim grants one owner exclusive write access to a write domain, identified
by the string `product:category:name`. The claim file name is the SHA-256 hex
digest of that identifier, so claim lookup never requires listing:
`claims/<sha256(product:category:name)>.json`.

| Key | Type | Meaning |
|---|---|---|
| `product` | string | Logical product name |
| `domain` | string | The `product:category:name` identifier |
| `owner_id` | string | Claim owner |
| `claim_path` | string | Absolute path of this claim file |
| `acquired_at` | number | Unix timestamp (seconds) |
| `last_heartbeat_at` | number | Refreshed by the owner while held |
| `heartbeat_interval_s` | number | Owner's refresh cadence |
| `stale_threshold_s` | number | Staleness cutoff, default 120 |

Semantics:

- Acquisition is the atomic creation of the claim file. If the file already
  exists, acquisition MUST fail with a claim conflict; it MUST NOT take over
  the claim, even a stale one.
- Release deletes the file.
- A claim is stale when `now - last_heartbeat_at > stale_threshold_s`. A
  crashed owner leaves a stale claim behind. Operators clear stale claims with
  `firecube chunks claims clear`, which refuses to clear fresh claims unless
  forced.
- Abandoning a run releases every claim whose owner id resolves to that run
  alone. The release applies to fresh and stale claims. The claim file stays in
  place if its owner or `acquired_at` changed after the claims were listed.
- An owner id resolves through its candidates: the id itself and each prefix
  that ends just before a `:`. The claim is released only if exactly one
  candidate is a recorded run, and that run is the abandoned one.
- A sweep of stale runs also releases the claims of a run that an earlier
  abandon already ended. A sweep that a crash interrupted therefore finishes
  its release when it runs again.
- The `firecube chunks runs abandon --all-stale` sweep releases a claim of a
  terminal run only if the claim is stale when re-read before the delete.
  Its dry run reports the claims it would release and the claims it would leave.
- If the claims of the product cannot be listed, the abandon reports an error
  and does not report success. Run the abandon again to release the claims.
- Acquisition does not change. A claim that the release left in place still
  blocks acquisition of its domain.

Known limitation: a release reads the claim again, then deletes the file. These
are two separate operations. If two releases run at the same time, one can
delete a claim that a successor acquired between the read and the delete.
`firecube chunks claims clear` has the same limitation. Do not run two releases
for the same product at the same time.

### Exclusive maintenance claim

The domain `<product>:maintenance:exclusive` is the exclusive maintenance claim
of a product. It is a claim like any other, and the rules above apply to its
file. Its `owner_id` is the id of the maintenance run.

- A conforming writer MUST NOT write to the product while another owner holds
  this claim. A stale claim counts as held.
- A writer MUST refuse if it cannot read the claim file.
- The check reads the one claim file by name,
  `claims/<sha256(<product>:maintenance:exclusive)>.json`. It passes when the
  file is absent or when its `owner_id` is the id of the writer.
- A writer makes the check when it starts, when it takes its admission claim,
  and when it ensures the resolved index.
- The writer records a run start that the check refuses. The run has a
  `run_started` event and one `run_failed` event with the refusal text, and no
  other terminal event. If the failure record cannot be written, the run has no `run_failed`
  event.
- A maintenance operation that runs under its own exclusive claim records the id
  of the holder in the key `holder_run_id`. The key is in the `meta` object of
  the maintenance event record and in `maintenance.scope`.

See [Write Safety](../concepts/orchestration/write-safety.md) for how claims
coordinate concurrent writers.

## Compact cache

The compact cache is a projection of the WAL into one chunk record per line.
Readers use it to avoid a replay of every run's events. A rebuild writes
`compact/compact-<generation>.jsonl` with the records sorted by key. It then
writes the sibling `compact/compact-<generation>.meta.json`. Last, it updates
`compact/LATEST.json` to point at the new generation. `<generation>` is a
nanosecond timestamp, so generations sort chronologically.

`compact-<generation>.meta.json` and `compact/LATEST.json` both carry
`schema_version` (`"v2"`), `generation`, `completed_before` and `product`.
`completed_before` is a Unix timestamp. The cache folds in the runs that
completed before this instant. The pointer adds `snapshot_path` and
`snapshot_meta_path`. The meta file adds `created_at` and `records`, the line
count. The keys keep their `snapshot_` names. Their values name files under
`compact/`.

A local rebuild holds `compact/compact.lock` while it writes. A rebuild on an
object store takes no lock.

If the pointer is absent, a reader replays the WAL. A store that has only the
earlier layout therefore reads from the WAL. The compact cache is derived data.
An operator MAY delete the `compact/` directory at any time. A rebuild
creates the cache again from the WAL.

## Resolved Index: `index/current.json`

The resolved-index record is the durable form of an engine-resolved
[`IndexSpec`](parallelism.md#firecube.ingestor.api.IndexSpec). It is written
once, after the first successful resolution, and verified against the plugin
declaration on every later run. Inspect it with `firecube zarr index show`;
regenerate it with `firecube zarr index rebuild`. The rendered API reference
for the record type is
[`ResolvedIndexRecord`](parallelism.md#firecube.ingestor.api.ResolvedIndexRecord).

| Key | Type | Meaning |
|---|---|---|
| `schema_version` | string | `"v1"` |
| `recorded_at` | string | ISO 8601 UTC timestamp |
| `recorded_by_run_id` | string | Run that wrote the record |
| `identity_hash` | string | 64-character lowercase hex SHA-256, see below |
| `index` | object | The resolved-index payload |
| `items` | array | Present only for discovered axes: content-addressed item manifest, sorted by coordinate value |

### Identity hash

The identity hash content-addresses the resolved index. Canonical bytes are
the UTF-8 encoding of the payload serialized as JSON with sorted keys,
separators `(",", ":")`, and `ensure_ascii=false`.

- Without items: `identity_hash = sha256(canonical_bytes(index))`.
- With items: the hash covers `{"index": index, "items": items}` with items
  sorted by their per-item `identity_hash`, so the value is invariant under
  input order and changes whenever an item is added or removed.

The same hash is mirrored into the Zarr root attributes
`firecube_resolved_index` (the payload) and
`firecube_resolved_index_identity_hash`, so a store detached from its
control plane still carries its identity.

### Precedence rules

On every run, the engine compares three things: the control-plane record
(`index/current.json`), the mirrored attrs hash on the store, and the index
resolved from the current plugin declaration. The rules are exhaustive and
evaluated in this order:

| Row | Record | Attrs hash | Relation | Outcome |
|---|---|---|---|---|
| 1 | absent | absent | fresh store | write record and attrs, outcome `created` |
| 6 | present | any | record `schema_version` is not `v1` | refuse: manifest error |
| 2 | present | present | both match the declaration | outcome `matched_existing` |
| 3 | present | absent | record matches the declaration | re-mirror attrs, outcome `created` |
| 4 | absent | present | attrs without a record | refuse: manifest error |
| 5 | present | any | record disagrees with the declaration | refuse: resolved-index conflict |
| 7 | present | present | record matches, attrs disagree | refuse: manifest error |

Refusals surface as documented control-plane errors, never as silent
overwrites. Row 5 is the guard against pointing a changed declaration at an
existing store. Use the plugin that wrote the product, or a new target.
`firecube zarr index rebuild` does not change this row.

For row 4, restore `index/current.json`. Or remove the root attrs
`firecube_resolved_index_identity_hash` and `firecube_resolved_index`, then
run `firecube zarr index rebuild` with the plugin that wrote the data. For
row 7, remove the root attr `firecube_resolved_index_identity_hash`, then run
`firecube zarr index rebuild`. The command writes the attr again from the
record. `rebuild` refuses both states while the attrs remain.

## Slot Index: `slot_index/current.json`

Products using slot-index models carry a parallel record with the same
envelope discipline: `schema_version` (`"v1"`), `recorded_at`,
`recorded_by_run_id`, `identity_hash`, and `model`. The model's canonical
bytes cover `name`, `epoch`, `time_unit`, and the per-group axes (cadence and
mode) with sorted keys; the identity hash is the SHA-256 of those bytes and is
mirrored to store attributes the same way as the resolved index.

## Reserved Zarr array attributes

Firecube stamps a small set of reserved attribute names on the arrays it
manages, including the write-once static-array marker
`firecube_static_written`. The authoritative list and the guard helper are in
[Core Utilities](core-utilities.md#reserved-array-attributes). Plugins MUST
NOT write these names.

## Co-location

`.firecube/` and the readable data store are one product. When copying or
moving a product manually, the control plane MUST move with the data for
inspection, resume, recovery, and cleanup to keep working. Firecube's archive
and restore commands preserve it automatically.

## Versioning

This specification is versioned by the `schema_version` fields above, and
each documented file names its version explicitly. The compatibility rule:

- Within a version, readers MUST ignore keys they do not recognize; keys MAY
  be added without a version change, and documented keys keep their meaning.
- A change that would break a conforming reader (removing a key, changing a
  type or the canonical-bytes rule) increments the `schema_version` of the
  affected document.
- The move of the cache to `compact/`, the key `meta.producer` and the
  exclusive maintenance claim change no `schema_version`. The new keys are
  additive. The compact cache is derived data, and a reader that finds no
  pointer replays the WAL.
- Firecube refuses to act on an `index/current.json` whose `schema_version`
  it does not support (precedence row 6) rather than guessing.

## See Also

- [Inspect a Product](../operations/chunk-manager/inspect.md): read these
  records with the CLI instead of by hand
- [Recover a Product](../operations/chunk-manager/recover.md): stale runs and
  claims
- [Resolved Index Operations](../operations/firecube-index.md): `show`,
  `verify`, and `rebuild`
- [Storage Concepts](../concepts/storage.md): staged and direct writes, and
  where the product root lives
