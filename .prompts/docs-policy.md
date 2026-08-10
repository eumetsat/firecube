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

Choose one primary audience for each content page before writing:

- **User**: installs Firecube, installs plugins, runs ingestion, checks outputs.
- **Plugin author**: implements a plugin against the public SDK.
- **Operator**: runs Firecube in production, CI, Argo, KFP, cron, or S3-backed environments.
- **Contributor**: changes Firecube internals, tests, migrations, or architecture.

If a content page serves more than one audience, split it or move the
lower-level detail behind a short "Learn more" link. A landing page may route
multiple audiences when it separates their destinations clearly.

## 2. Public Docs Standard

Match the content to the reader's need:

- Tutorials provide a controlled learning experience with concrete actions and
  expected results.
- How-to guides help a competent reader accomplish one real-world goal or solve
  one problem.
- Reference provides neutral, precise, complete lookup facts for a declared
  public surface.
- Explanation develops understanding through context, connections, reasons,
  implications, and tradeoffs.

Commands, expected output, verification, and recovery belong primarily in
tutorials and how-to guides. Do not add them to reference or explanation merely
to make a page seem practical.

## 3. Internal Detail Boundary

Do not put these in public task pages unless they are required for a user action:

- phase history, audit findings, reviewer names, commit labels, or evidence logs
- line numbers, private module paths, or source-file archaeology
- internal service names such as `SpanRecorder`, `PipelineExecutor`, `ManifestRepository`, or private facades (`RuntimeIngestContext` is exported from `firecube.ingestor.api` and documented in the API reference, so it is not on this list)
- internal design invariants, implementation rationale, decision history,
  tradeoff matrices, or implementation debates
- `plans/` or other project-management references

Use these homes instead:

- `docs/quickstart/`: first-run user path
- `docs/tutorials/`: guided examples with concrete inputs and outputs
- `docs/guides/`: task-oriented plugin-author and operator guides
- `docs/reference/`: complete factual surfaces such as CLI, config, and API
- `docs/concepts/`: user-facing context, reasons, implications, tradeoffs, and
  mental models
- `plans/`: implementation history, audits, internal design rationale, and evidence

## 4. Page Types

For a content page, classify the dominant need with two questions: does it
inform action or cognition, and does it serve skill acquisition/study or skill
application/work?

- **Tutorial**: action plus acquisition; a controlled lesson through doing.
- **How-to**: action plus application; a practical goal or problem.
- **Reference**: cognition plus application; authoritative lookup facts.
- **Explanation**: cognition plus acquisition; bounded understanding and
  reflection.
- **Internal note**: records architecture, design rationale, or maintenance policy.

Landing/overview pages introduce and route to content; compatibility pages route
old URLs. They are page roles, not additional documentation types.

Do not infer type from a directory or title. Secondary material may support a
content page's dominant need, but it must not interrupt or change that need.
Move interrupting sections to their canonical owner and link them.

## 5. Writing Rules

- Start with the reader's question and dominant need.
- Use public CLI flags and public SDK imports only.
- Link to internals only after the user-facing path is complete.
- Keep section names consistent across related public pages. When a recurring
  section has an established heading, reuse it instead of inventing synonyms.
- For tutorials, use one concrete path, show results early and often, and omit
  alternatives and extended explanation.
- For how-to guides, name the user outcome, assume competence, keep the flow
  action-oriented, and include only reference or explanation needed for the
  task.
- For reference, describe neutrally and mirror the public machinery.
- For explanation, connect and contextualize ideas without adding a procedure
  or exhaustive contract table.

## 6. Template Selection

Use the prompt templates under `.prompts/` when creating or rewriting pages:

- `/write-user-doc` for user tasks.
- `/write-plugin-doc` for SDK/plugin pages.
- `/write-operator-doc` for production operations.
- `/write-internal-doc` for architecture and design rationale.

Templates are scaffolds. Remove empty sections before publishing.
