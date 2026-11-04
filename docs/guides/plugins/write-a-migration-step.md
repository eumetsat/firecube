# Write A Migration Step

## Goal

Ship a metadata migration step in a plugin package. An operator then runs it on
finished products with `firecube migrate run`. A step changes attributes in the
Zarr metadata documents of a product. A step never moves or rewrites a chunk.
A change that moves chunks, such as a renamed dimension, is not a step.

The public types are in `firecube.ingestor.api`. Each link in this guide goes
to the generated entry of one type in the
[Metadata Migration reference](../../reference/migration.md). The operator runs
the step as described in [Migrate Metadata](../../operations/maintenance/migrate.md).

Package the plugin as in [Package and Register a Plugin](contract.md). The
distribution must have a name and a version. Firecube refuses a step whose entry
point has no installed distribution.

## Minimal Example

A step is a class. Its constructor takes one mapping, the step options. Its
`plan` method takes a `StoreView` and returns a tuple of `AttributePatch`
objects.

```python
from collections.abc import Mapping
from typing import Any

from firecube.ingestor.api import ABSENT, AttributePatch, MetadataPlan, StoreView


class NameVariables:
    """Give every array that has no `long_name` one, built from its path."""

    id = "my-plugin:0.1.0->0.2.0"
    depends_on: tuple[str, ...] = ()
    from_release = None
    to_release = None
    descriptive_keys = frozenset({"processing_level"})

    def __init__(self, options: Mapping[str, Any]) -> None:
        self.level = str(options.get("level", "L2"))

    def plan(self, view: StoreView) -> MetadataPlan:
        patches = [AttributePatch("", set={"processing_level": self.level})]
        for node in view.nodes():
            if node.node_type != "array":
                continue
            attrs = {} if node.attributes is ABSENT else node.attributes
            if "long_name" not in attrs:
                patches.append(
                    AttributePatch(node.path, set={"long_name": node.path.replace("_", " ")})
                )
        return tuple(patches)
```

Declare the five class attributes of
[`MigrationStep`](../../reference/migration.md#firecube.ingestor.api.MigrationStep).
Name the `id` `<namespace>:<from>-><to>`, as in `my-plugin:0.1.0->0.2.0`.
Firecube enforces one rule on the `id`: it must equal the entry-point name.
The form is a naming convention.

Read the store only through the
[`StoreView`](../../reference/migration.md#firecube.ingestor.api.StoreView).
A field that the document lacks holds
[`ABSENT`](../../reference/migration.md#firecube.ingestor.api.ABSENT).
Test `node.attributes is ABSENT` before you read it.

Return the changes as
[`AttributePatch`](../../reference/migration.md#firecube.ingestor.api.AttributePatch)
objects. Keep [`plan`](../../reference/migration.md#firecube.ingestor.api.MigrationStep.plan)
pure.

If the store already holds the values that the step sets, the plan changes
nothing. Firecube then writes only its own stamp. The run reports the outcome
`adopted`.

Add the step to the `firecube.migrations` entry-point group in `pyproject.toml`.
Quote the key, because it contains a colon and a `>`:

```toml
[project.entry-points."firecube.migrations"]
"my-plugin:0.1.0->0.2.0" = "my_plugin:NameVariables"
```

Install the package again after you change the entry points. If the key differs
from the `id` of the class, Firecube refuses the run:

```text
Error: migration step 'my-plugin:other' names itself 'my-plugin:0.1.0->0.2.0'
```

## Choose The Attributes A Step Changes

Firecube sorts every attribute that a plan sets or removes into one class. The
class decides who may change it.

| Class | Attribute names | Rule |
|---|---|---|
| Physical | Every name Firecube reserves (`_ARRAY_DIMENSIONS`, `_FillValue`, the array markers and the six reserved root attributes) and every name that starts with `firecube_` | Always refused. |
| Interpretation | `scale_factor`, `add_offset`, `units`, `calendar`, `grid_mapping`, `crs`, `missing_value`, `valid_min`, `valid_max`, `valid_range` | Refused unless the operator passes `--allow interpretation`. |
| Descriptive | `title`, `summary`, `history`, `institution`, `source`, `references`, `comment`, `license`, `Conventions`, `long_name`, `standard_name`, `description`, `keywords`, `creator_name`, `creator_email`, `creator_url`, `publisher_name`, `publisher_email`, `publisher_url`, `project`, `acknowledgment`, `product_version` | Allowed. |
| Unknown | Any other name | Refused unless the operator passes `--allow unknown`. |

To change a name that is not in the built-in descriptive list, add it to
`descriptive_keys`. This extends the descriptive class only. A physical or
interpretation name stays in its class whatever `descriptive_keys` holds.

Firecube refuses the whole plan if one attribute is refused. It changes no Zarr
document.

Firecube sets the root attribute `firecube_last_modifier` in every run. If
`to_release` is set, it also sets `firecube_release` and creates that release in
the same run. Do not set either name in a plan.

## Know The Identity Rule

Firecube identifies an applied step by the step id. This has three
consequences:

- A rerun of the same id writes nothing, even with other options.
- The same id at another version of the distribution is refused. An upgrade
  does not allow the id again.
- A change that must reach stores where an older step ran needs a new id.

Treat each step as a change that runs once for each store.

## Verify

Create a small store and run the step through the installed entry point. The
commands use `/tmp/rain.zarr`. The activated virtual environment must contain
the package.

```bash
python - <<'EOF'
import numpy as np
import zarr

group = zarr.open_group("/tmp/rain.zarr", mode="w", zarr_format=3)
group.attrs["title"] = "Daily rainfall"
rate = group.create_array("precip_rate", shape=(4,), chunks=(2,), dtype="f4", dimension_names=["time"])
rate[:] = np.arange(4)
rate.attrs["units"] = "mm/h"
temp = group.create_array("temp", shape=(4,), chunks=(2,), dtype="f4", dimension_names=["time"])
temp.attrs["long_name"] = "Air temperature"
EOF
firecube migrate list
```

Each line shows a step, its distribution and its version. The list includes this
line:

```text
my-plugin:0.1.0->0.2.0  my-plugin  0.2.0
```

`migrate run` refuses a store that has no control plane. Adopt the store first:

```bash
firecube migrate adopt \
  --target "file:///tmp/rain.zarr" \
  --product-name rain
```

```text
adopted the store as product rain (run adopt-aee4c651-d814-4963-8179-dffaa476710b, Zarr format 3)
```

Run the plan without a write. Quote the step id, because the shell reads `>`:

```bash
firecube migrate run 'my-plugin:0.1.0->0.2.0' \
  --target "file:///tmp/rain.zarr" \
  --product-name rain \
  --option level=L3 \
  --dry-run
```

```text
set /:processing_level
set precip_rate:long_name
class /:processing_level: descriptive, allowed
class precip_rate:long_name: descriptive, allowed
write precip_rate/zarr.json
write zarr.json
planned outcome: executed
```

The step names `precip_rate` only, because `temp` has a `long_name`. Run it
without `--dry-run`:

```bash
firecube migrate run 'my-plugin:0.1.0->0.2.0' \
  --target "file:///tmp/rain.zarr" \
  --product-name rain \
  --option level=L3
```

```text
set /:processing_level
set precip_rate:long_name
class /:processing_level: descriptive, allowed
class precip_rate:long_name: descriptive, allowed
write precip_rate/zarr.json
write zarr.json
outcome: executed
before snapshot 53b6222a58a412d5b6899d87bea59283a7a026319b65b2d52865d4e037387d51
wrote 2 of 2 planned documents
after snapshot 2047b9419a35cc78b605adca184ffadf761de7d08330ed596a64067c0995247a
step my-plugin:0.1.0->0.2.0 executed in run migrate-a257d5f7-7288-45ff-b6ce-2d8ee95b44fd
```

The run ids and the snapshot ids differ on your machine. Run the same command
again with `--option level=L1`:

```text
step my-plugin:0.1.0->0.2.0 already applied by run migrate-a257d5f7-7288-45ff-b6ce-2d8ee95b44fd
```

The root group now holds `processing_level` `L3` and the attribute
`firecube_last_modifier`.

## Common Mistakes

| Mistake | Fix |
|---|---|
| An entry-point key that differs from the class `id` | Use the same string for both. |
| An unquoted `>` in a shell command | Quote the step id: `'my-plugin:0.1.0->0.2.0'`. |
| Reading `node.attributes` without a test for `ABSENT` | Test `node.attributes is ABSENT` first. |
| Setting `units`, `scale_factor` or another interpretation name | Tell the operator to pass `--allow interpretation`, or leave the change out. |
| Setting a name that starts with `firecube_` | Remove it. Firecube never lets a step change these names. |
| Fixing a released step and keeping its `id` | Publish the fix under a new `id`. The same id at a new version is refused. |
| Reading the clock or a file in `plan` | Use `view.now` and the `StoreView`. |

## Next Steps

- **[`MigrationStep`](../../reference/migration.md#firecube.ingestor.api.MigrationStep)**: the contract of a step
- **[Migrate Metadata](../../operations/maintenance/migrate.md)**: run, resume and inspect a migration
- **[Create A Release](../../operations/maintenance/create-a-release.md)**: what `to_release` creates
- **[Declare The Schema And Index](direct-zarr.md)**: the root attributes that a plugin must not declare
