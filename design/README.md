# Weaver design documentation

This directory describes Weaver for maintainers. Read the documents by purpose
rather than treating them as one linear specification.

See the [documentation stocktake](documentation-stocktake.md) for the complete
classification, authority, and future site direction.
See the [source prose review](source-prose-review.md) for the runtime and source
boundaries covered by the writing uplift.

## Overview and architecture

| Document | Type | Use it for |
| --- | --- | --- |
| [Weaver architecture](weaver-architecture.md) | Overview | Product model, document model, lifecycle, and authoritative design decisions. |
| [Code architecture](code-architecture.md) | Architecture | The Session, Builder, Installer, and Runner responsibilities and their handoffs. |
| [Build philosophy](build-philosophy.md) | Contract | Invariants that build planning and installation must preserve. |
| [Central catalogue](catalogue.md) | Architecture | Catalogue ownership, its projected and runtime tables, reconciliation, and certification. |
| [Validation](validation.md) | Architecture | Tests, Assumptions, runtime artefacts, and result handling. |
| [The keyed table load](keyed-load.md) | Architecture | Reconciliation for a table with a primary key, in both engines. |
| [Warehouse SQL execution](sql-execution.md) | Contract | SQL execution across desktop and Fabric positions. |

## Repository and command use

| Document | Type | Use it for |
| --- | --- | --- |
| [Weaver repository](weaver-repository.md) | Contract | Repository layout, item ownership, and source migration. |
| [How Weaver build works](how-does-build-work.md) | How-to | The build lifecycle and its generated bundle. |
| [CLI usage](cli-usage.md) | How-to | Workspace resolution, sessions, commands, and control-plane tasks. |
| [Adding an artefact](how-to-add-an-artefact.md) | How-to | Extending Weaver with a new runtime artefact. |

## Testing

| Document | Type | Use it for |
| --- | --- | --- |
| [Test architecture](test-architecture.md) | Testing | Which layer should prove a claim. |
| [Fabric integration tests](fabric-testing.md) | Testing | Workspace setup, declarations, telemetry, and Fabric-specific behaviour. |

## Historical notes

[Interactivity baseline](history/interactivity-baseline.md) records measurements
and decisions from the Run and Build decomposition. It is historical context,
not a current implementation contract.

When behaviour changes, update the document that owns the relevant contract.
Avoid adding a second explanation of an existing architectural decision.
