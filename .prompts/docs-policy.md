---
description: Apply Firecube's documentation audience and public/private boundary rules
agent: build
---

# Documentation Policy

Use this prompt before creating, reviewing, or substantially rewriting Firecube
documentation.

## Goal

Keep published Firecube documentation useful for users and keep implementation
history out of the default reading path.

## Arguments

`$ARGUMENTS` — optional doc paths to review or rewrite. If omitted, apply this
policy to the current documentation task.

## 1. Identify The Audience

Choose one primary audience before writing:

- **User**: installs Firecube, installs plugins, runs ingestion, checks outputs.
- **Plugin author**: implements a plugin against the public SDK.
- **Operator**: runs Firecube in production, CI, Argo, KFP, cron, or S3-backed environments.
- **Contributor**: changes Firecube internals, tests, migrations, or architecture.

If a page serves more than one audience, split it or move the lower-level detail
behind a short "Learn more" link.

## 2. Public Docs Standard

Public docs explain what a reader can do, what command or code to use, what result
to expect, and how to recover from common failures.

Public docs should usually answer at least one of these questions:

- What can I do?
- What do I need before starting?
- What command or code do I run?
- What output should I expect?
- How do I verify it worked?
- What do I do when it fails?

Architecture belongs in public docs only when it changes a user decision. Explain
the user consequence first, then add the smallest necessary mental model.

## 3. Internal Detail Boundary

Do not put these in public task pages unless they are required for a user action:

- phase history, audit findings, reviewer names, commit labels, or evidence logs
- line numbers, private module paths, or source-file archaeology
- internal service names such as `SpanRecorder`, `PipelineExecutor`, `ManifestRepository`, or private facades (`RuntimeIngestContext` is exported from `firecube.ingestor.api` and documented in the API reference, so it is not on this list)
- design invariants, rationale, tradeoff matrices, or implementation debates
- `.sisyphus/`, `plans/`, or other project-management references

Use these homes instead:

- `docs/quickstart/`: first-run user path
- `docs/tutorials/`: guided examples with concrete inputs and outputs
- `docs/guides/`: task-oriented plugin-author and operator guides
- `docs/reference/`: complete factual surfaces such as CLI, config, and API
- `docs/concepts/`: user-facing mental models only
- `plans/` and `.sisyphus/`: implementation history, audits, design rationale, evidence

## 4. Page Types

Use one page type explicitly:

- **Tutorial**: teaches through a working example.
- **How-to**: solves one practical task.
- **Reference**: lists the complete surface without narrative.
- **Explanation**: gives a user-facing mental model.
- **Internal note**: records architecture, design rationale, or maintenance policy.

Do not mix tutorial, reference, and internal design history in one page.

## 5. Writing Rules

- Start with the task or decision, not with architecture.
- Prefer runnable commands, short code examples, expected output, and recovery steps.
- Use public CLI flags and public SDK imports only.
- Name required flags explicitly when a command will fail without them.
- Keep troubleshooting entries actionable and paste-runnable where possible.
- Link to internals only after the user-facing path is complete.
- Keep section names consistent across related public pages. When a recurring
  section has an established heading, reuse it instead of inventing synonyms.

## 6. Template Selection

Use the prompt templates under `.prompts/` when creating or rewriting pages:

- `/write-user-doc` for user tasks.
- `/write-plugin-doc` for SDK/plugin pages.
- `/write-operator-doc` for production operations.
- `/write-internal-doc` for architecture and design rationale.

Templates are scaffolds. Remove empty sections before publishing.
