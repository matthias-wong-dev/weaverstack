# Design documentation stocktake

This is the maintenance map for the current design tree. It records each
document's role and the action taken during the writing uplift. The Markdown
layout, stable headings, and relative links are suitable for a later MkDocs
migration; this repository does not build a documentation site yet.

## Current documents

| Document | Type | Status | Action |
| --- | --- | --- | --- |
| [weaver-architecture.md](weaver-architecture.md) | Overview | Update | Product authority. It retains command responsibilities and now directs syntax and interactive detail to CLI usage. |
| [code-architecture.md](code-architecture.md) | Architecture | Update | Four-doer and handoff authority. Source modules now link here instead of carrying layer essays. |
| [build-philosophy.md](build-philosophy.md) | Contract | Update | Kept the invariants and review checklist; removed duplicated rationale from the lifecycle guide. |
| [how-does-build-work.md](how-does-build-work.md) | Lifecycle | Update | Retained the operational sequence and replaced its repeated principles with a link to the contract. |
| [catalogue.md](catalogue.md) | Architecture | Update | Kept catalogue ownership and addressing; removed the obsolete CLI-plan reference and simplified the repeated planning explanation. |
| [validation.md](validation.md) | Architecture | Retain | Current validation contract with a coherent scope. CLI invocation details remain in CLI usage. |
| [weaver-repository.md](weaver-repository.md) | Contract | Retain | Current repository-layout and ownership contract; distinct from build execution. |
| [sql-execution.md](sql-execution.md) | Contract | Retain | Narrow desktop and in-Fabric SQL boundary with no competing command guide. |
| [cli-usage.md](cli-usage.md) | How-to | Update | Command reference. Removed architecture arguments and linked to the code architecture where needed. |
| [how-to-add-an-artefact.md](how-to-add-an-artefact.md) | How-to | Update | Retained the implementation sequence; condensed reviewer-facing rationale and linked test policy. |
| [test-architecture.md](test-architecture.md) | Testing | Update | Claim-to-layer policy. Fabric setup remains in the companion Fabric guide. |
| [fabric-testing.md](fabric-testing.md) | Testing | Update | Real-workspace prerequisites and platform behaviour; links to test architecture for layer selection. |
| [history/interactivity-baseline.md](history/interactivity-baseline.md) | Historical | Archive | Moved from `todo/`; implementation record, not a current contract. |

## Authority and duplication

`weaver-architecture.md` defines product behaviour. `code-architecture.md`
defines how this repository implements that behaviour. The remaining documents
own a narrower contract or procedure and should link to those two instead of
repeating their full rationale.

Use runtime text for the observed condition, a comment for a local constraint,
a docstring for a callable contract, and a design document for system-wide
reasoning. Pull requests and Git history record how the implementation changed.

## Future site direction

Keep authored documentation in Markdown under `design/`. New material should
use descriptive headings, relative links, simple tables, and code fences. Keep
maintainer design, user how-to material, and generated reference material
separate so a future MkDocs navigation can adopt the existing structure without
copying or transforming the source documents.
