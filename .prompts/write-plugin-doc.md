---
description: Create or rewrite Firecube plugin-author documentation
agent: build
---

# Write Plugin Author Documentation

Use this prompt for public SDK and plugin-author documentation.

## Arguments

`$ARGUMENTS` — target doc path and optional plugin-author task.

## Rules

- Apply `.prompts/docs-policy.md` first.
- Write for plugin authors using the public SDK.
- Use imports from `firecube.ingestor.api` unless the page is explicitly about an advanced public API.
- Explain required methods, config, and verification commands.
- Avoid private runtime modules, internal service names, and implementation history.

## Template

````markdown
# Implement X In A Plugin

## Goal

Describe the plugin capability the reader will implement and the public API it
uses.

## Minimal Example

Use imports from `firecube.ingestor.api` unless the page is explicitly about an
advanced public API.

```python
from typing import ClassVar

from firecube.ingestor.api import GenericZarrIngestor, PluginContext, register_ingestor


@register_ingestor("my_plugin")
class MyPlugin(GenericZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "my_product"

    def build_dataset(self, group: str, items: list[object], ctx: PluginContext):
        ...
```

## Required API

List only public methods, attributes, flags, or config keys the plugin author
must use.

| API | Required | Purpose |
|-----|----------|---------|
| `PRODUCT_NAME` | Yes | Logical product identity |

## Configuration

Show the config or CLI options needed to exercise the feature.

```toml
[plugins.my_plugin]
...
```

## Test It

Give one local command that validates the plugin behavior.

```bash
uv run firecube ingest my_plugin ...
```

## Common Mistakes

| Mistake | Fix |
|---------|-----|
| Importing private runtime modules | Import from `firecube.ingestor.api` |

## See Also

Link to task-oriented or reference pages. Avoid internal design notes unless the
reader is implementing Firecube itself.
````
