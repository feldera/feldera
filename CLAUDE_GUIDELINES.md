# CLAUDE.md Authoring Guide (for Claude Code)

This repo uses layered `CLAUDE.md` docs so Claude Code can navigate context quickly. Follow these governing principles.

## Purpose and placement

* Use `CLAUDE.md` as the entry point for humans and Claude Code within each directory.
* Write only what’s needed to work effectively in that directory; avoid project history; concise underlying reasoning is OK.

## Layering and scope

* Top level provides a high-level overview: the project’s purpose, key components, core workflows, and dependencies between major areas.
* In the top level, include exactly one short paragraph per important subdirectory describing what lives there, why it exists, and where to start. Keep it concrete but brief.
* Subdirectory docs add progressively more detail the deeper you go. Each level narrows to responsibilities, interfaces, commands, schemas, and caveats specific to that scope.

## DRY information flow

* Do not repeat what a parent `CLAUDE.md` already states about a subdirectory. Instead, link up to the relevant section.
* Put cross-cutting concepts at the highest level that owns them, and reference from below.
* Keep a single source of truth for contracts, schemas, and commands; everything else links to it.

## Clarity for Claude Code

* Prefer crisp headings, short paragraphs, and tight bullets over prose.
* Name files, entry points, public interfaces, and primary commands explicitly where they belong.
* Call out constraints, feature flags, performance notes, and “gotchas” near the workflows they affect.

## Maintenance rules

* Update the highest applicable level first; ensure lower levels still defer to it.
* Remove stale sections rather than letting them drift; shorter and correct beats exhaustive and outdated.
* When adding a new directory, add its paragraph to the top level and create its own `CLAUDE.md` that deepens—never duplicates—the parent’s description.

## Quality checklist

* Top level gives a true overview and one concise paragraph per important subdirectory.
* Every subdirectory doc increases detail appropriate to its scope.
* No duplication across levels; links replace repetition.
* Commands, interfaces, and data shapes are precise and current. It is OK to document different arguments for the same command for different use-cases.
* Formatting is skim-friendly and consistent across the repo.
