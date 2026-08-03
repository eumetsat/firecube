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
- Classify the page with the action/cognition and acquisition/application
  compass before choosing its structure.
- Avoid private runtime modules, internal service names, and implementation history.
- Inspect the predecessor, successor, and comparable sibling pages before
  writing. Preserve established headings, command prefixes, verification
  style, and transition-link formatting when they remain consistent with the
  selected type and current documentation policy.
- Keep Concepts explanations separate from Guides procedures. A write-model
  explanation and an implementation guide may cover the same public class when
  they develop context and tradeoffs versus direct action, respectively.

Plugin authors need all four documentation types:

- Tutorials use one concrete plugin and one controlled, reliable learning path.
- How-to guides address a real plugin-author goal, assume competence, and keep
  only the API facts needed for the action.
- Reference describes exact classes, hooks, fields, defaults, and constraints.
- Explanation develops the mental model, context, and tradeoffs without a task
  sequence or exhaustive API table.

Landing pages orient and route among those types without duplicating them. For
plugin lifecycle and implementation how-tos, use the scaffold below as a local
presentation convention, removing sections the task does not need.

## How-To Template

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

Explain only the public API facts required to perform this task. Link complete
signatures, fields, defaults, and constraints to Reference.

## Verify

Give one local command that validates the plugin behavior.

```bash
uv run firecube ingest my_plugin ...
```

## Common Mistakes

| Mistake | Fix |
|---------|-----|
| Importing private runtime modules | Import from `firecube.ingestor.api` |

## Next Steps

Link to task-oriented or reference pages. Avoid internal design notes unless the
reader is implementing Firecube itself.
````
