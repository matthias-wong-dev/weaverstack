# Design documentation stocktake

This is the maintenance map for the current design tree. It records each
document's role and the action taken during the writing uplift. The Markdown
layout, stable headings, and relative links are suitable for a later MkDocs
migration; this repository does not build a documentation site yet.

## Current documents

| Document | Type | Status | Action |
| --- | --- | --- | --- |
| [weaver-architecture.md](weaver-architecture.md) | Overview | Authoritative | Keep and update when product behaviour changes. |
| [code-architecture.md](code-architecture.md) | Architecture | Authoritative | Keep; it owns the four-doer implementation structure. |
| [build-philosophy.md](build-philosophy.md) | Contract | Current | Keep; it owns build invariants. |
| [how-does-build-work.md](how-does-build-work.md) | How-to | Current | Keep; it explains the build lifecycle. |
| [catalogue.md](catalogue.md) | Architecture | Current | Keep; it owns central catalogue design. |
| [validation.md](validation.md) | Architecture | Current | Keep; it owns Test and Assumption design. |
| [weaver-repository.md](weaver-repository.md) | Contract | Current | Keep; it owns repository structure. |
| [sql-execution.md](sql-execution.md) | Contract | Current | Keep; it owns Warehouse execution boundaries. |
| [cli-usage.md](cli-usage.md) | How-to | Current | Keep and update alongside CLI changes. |
| [local-setup.md](local-setup.md) | How-to | Current | Keep and update with supported local tooling. |
| [how-to-add-an-artefact.md](how-to-add-an-artefact.md) | How-to | Current | Keep; it owns extension guidance. |
| [test-architecture.md](test-architecture.md) | Testing | Current | Keep; it owns test-layer selection. |
| [fabric-testing.md](fabric-testing.md) | Testing | Current | Keep; it owns real-workspace setup and test markers. |
| [todo/interactivity-baseline.md](todo/interactivity-baseline.md) | Historical | Historical | Retain as an implementation record; do not treat it as a current contract. |

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
