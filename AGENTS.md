# Weaverstack Agent Guide

Guidance for coding agents working **on weaverstack itself**.

## Repository role

`weaverstack` is a data-engineering runtime for Microsoft Fabric built around a
central control plane. One mandatory **Weaver Lakehouse** holds the installed
SES repositories and the authoritative catalogue; destination Lakehouses and
Warehouses hold only materialised output.

The distribution is `weaverstack`; the import is `weaver`.

## The sibling `weaver` repository is reference-only

The repositories sit side by side:

```text
dwg-platform/
├── weaver/        reference implementation — DO NOT MODIFY
└── weaverstack/   this repository
```

`weaver` is consulted for proven algorithms, Fabric/OneLake/Spark/Warehouse edge
cases, SES fixtures and behavioural intent. Never change it as part of
weaverstack work, and never import from it. Where the two disagree, the
architecture in [the implementation plan](backlog/weaverstack-step-by-step-implementation-plan.md)
is authoritative.

Reference baseline: `a97ba8a0b00dd66dff1b2c5e818403694562fd30` (the plan's
reviewed snapshot). The sibling checkout has since advanced; confirm which
revision you are reading before treating it as the baseline.

## Working protocol

Construction follows numbered checkpoints in
[backlog/weaverstack-step-by-step-implementation-plan.md](backlog/weaverstack-step-by-step-implementation-plan.md).
This is not a document to implement in one pass. For each checkpoint:

1. read only that section and its listed reference files;
2. say what should be ported and what should be replaced;
3. raise the decisions that need Matthias's judgement, and wait;
4. implement only that checkpoint;
5. present the resulting structure and observable behaviour;
6. wait for approval before starting the next.

`backlog/weaver-architecture-summary.md` is the architectural companion.

## Architecture invariants

These hold from checkpoint 0 and are enforced by `tests/test_core_boundary.py`:

- **Core never imports the CLI.** `weaver_cli` is an optional extra; a core
  import of it would break a Fabric Environment install. The dependency runs one
  way, CLI → core.
- **The core is importable without PySpark and without Fabric credentials.**
  PySpark, `azure-identity` and `mssql-python` are lazy imports confined to the
  modules that execute against those systems.
- **One error hierarchy.** Everything derives from `weaver.errors.WeaverError`,
  including CLI errors. Add a subclass at the checkpoint that first raises it.
- **The CLI owns no semantics.** It parses arguments and prints results. Command
  functions return plain serialisable structures.

These become enforceable as the corresponding code lands:

- **Static discovery.** Discovery never imports object modules.
- **Objects never mutate the target.** `read()` proposes; Weaver owns mutation,
  CRUD accounting, staging and logging.
- **Every target root is explicit.** No destination Lakehouse is assumed to be
  attached to the notebook.
- **The central catalogue is authoritative.** No target-local catalogue, no
  target-local runtime, no target-local logging authority.
- **Certification is per object.** Before a rebuild, the selected objects and
  their descendants stop being certified; each returns only after it builds.

## Environment neutrality

Weaverstack must contain no defaults for product, workspace, Lakehouse,
Warehouse, endpoint, repository or notebook names, no production endpoints and
no local platform paths. Allowed defaults are generic technical values (Fabric
API URLs, auth scopes, Livy version, timeouts, polling intervals, parallelism).

## Layout

```text
weaverstack/
├── pyproject.toml
├── AGENTS.md
├── backlog/          architecture summary and the checkpoint plan
├── src/
│   ├── weaver/       the core framework
│   └── weaver_cli/   the optional desktop CLI
└── tests/
```

## Dependencies

Base install is deliberately minimal. A dependency is declared at the checkpoint
that first needs it, not in advance. See the comment in `pyproject.toml`.

## Development

```bash
python3.11 -m venv .venv
.venv/bin/pip install -e '.[dev]'
.venv/bin/python -m pytest
.venv/bin/weaver --help
```
