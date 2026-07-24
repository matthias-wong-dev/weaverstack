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

## Read this first

[docs/journal.md](docs/journal.md) is the **record** of what weaverstack is and
why. The backlog plan is the **guide**, written before construction started.
Where they disagree, the journal is right and the plan is stale.

The journal also carries the context that matters most: the underlying system
has run in production on SQL Server for years and weaver proved it works on
Fabric. This is implementation, not invention. Port proven algorithms rather
than re-deriving them; spend design attention on the control plane, which is
the genuinely new part.

Add to the journal as part of the work, not afterwards — a checkpoint that
changes a decision and leaves the journal stale is incomplete.

## The core abstraction

This is the thing that is hard to hold in your head, and the thing most likely
to be got wrong by someone reading only the code in front of them.

**Weaver is a system that runs inside Microsoft Fabric.** We develop it on a
laptop against a local proxy, and we test it at both levels. Two axes, kept
strictly apart:

```text
    WHERE THINGS ARE                 WHERE THE CODE RUNS
    the host                         the executor

    LocalHost   a root directory     in-process, on a laptop
    FabricHost  one workspace        in-process, inside a Fabric session
                                     submitted from outside, over Livy
```

They are independent, and three of the combinations are real:

| | host | code runs | what it is |
|---|---|---|---|
| 1 | Local | laptop | development, and most of the test suite |
| 2 | Fabric | laptop | the desktop CLI reaching into a workspace |
| 3 | Fabric | **in Fabric** | **the product** — `pip install weaverstack` in a notebook |

**The foundational rule:** *Weaver core operates within the host where it is
executing. Only the CLI and the Fabric test infrastructure cross from one host
into another.*

```text
core running locally        → operates within LocalHost
core running inside Fabric   → operates within FabricHost, session-native
CLI or pytest running locally → may cross into Fabric over REST, DFS and Livy
```

A `FabricHost` identifies the workspace the resources live in. It does **not**
say whether access happens through desktop HTTP clients or inside a session.

So the storage picture has two parts, and they must not be conflated:

*Within-host execution* — the store Weaver uses where it runs:

| execution | host | store |
|---|---|---|
| local process | `LocalHost` | `LocalStore` |
| Fabric session | `FabricHost` | `FabricStore` over `notebookutils.fs` |

*Cross-boundary access* — a local caller reaching into a workspace:

| caller | destination | client |
|---|---|---|
| CLI | Fabric workspace | `OneLakeDfsClient` |
| Fabric integration tests | Fabric workspace | `OneLakeDfsClient` |

`OneLakeDfsClient` (ADLS Gen2 DFS over HTTPS) is **not** the Fabric equivalent of
`LocalStore`. It is how the desktop crosses in, constructed explicitly by the
caller that crosses. Inside Fabric, `store_for(FabricHost)` returns the
session-native `FabricStore`; from a desktop that construction fails rather
than silently substituting DFS.

Above resolution and the store, nothing knows which host it is talking to. An
`if isinstance(host, …)` in core operation code means the abstraction is being
broken; the fix belongs in the factories, or in the CLI that does the crossing.

**Credential choice is a caller's policy, not the core's.** Core accepts an
injected credential and otherwise uses the library default without pinning the
chain. The CLI and the Fabric test infrastructure call `prefer_cli_credential()`
themselves; importing or using the core imposes no credential choice.

### The local host is a proxy, not a toy

`.local/Sales_LH/Files` and `.local/Sales_LH/Tables` mirror the shape a Fabric
Lakehouse presents through OneLake, deliberately, so the same resolution
arithmetic serves both. It exists so that most development and most of the test
suite need no tenant, no capacity and no credentials — not because local is a
lesser case.

### Row 3 is the claim, and it is the least tested

A user should be able to open a Fabric notebook, `pip install weaverstack`, and
work. That is the product, and it is what distinguishes Weaver from tools that
demand an orchestration environment of their own. **A Fabric test that runs
Weaver on the laptop and reaches into a workspace over HTTP tests row 2, not
row 3.** Both are worth having, but only row 3 is the promise.

Row 3 is delivered by installing Weaver into a Fabric Environment: `weaver
install --workspace <ws> --environment <env>` builds a wheel from the checkout,
stages it and Weaver's dependencies, and publishes. A Livy session (and a Fabric
notebook) then attaches that Environment via `fabric_environment` on the host and
imports the installed package — nothing is copied into the workspace. Rerun
`weaver install` whenever Weaver Python changes; an unchanged source tree builds
the same version and the install skips the republish.

### What this means when you add a feature

Ask, in order:

1. Does it work against a `LocalHost`, with a test that needs no tenant?
2. Does it work against a `FabricHost` from the laptop?
3. Does it work with Weaver *running inside* Fabric?

Answer all three, and answer them with tests that call the real function —
not with test code that reproduces what the function would have done. That
mistake has already been made once here: the first Fabric suite deleted files
through the store directly and looked like it was testing `wipe`.

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
- **Level-three identity is host + type + name.** An item name is unique per
  *type*, not across types — a Lakehouse and its generated SQL endpoint share a
  display name. Resolution is typed: the slot supplies the type (a `DeltaTarget`
  is a Lakehouse, a `WarehouseTarget` a Warehouse), so core never asks the
  workspace what a bare name "is". A destructive operation must not depend on
  name inference.
- **The central catalogue is authoritative.** No target-local catalogue, no
  target-local runtime, no target-local logging authority.
- **Certification is per object.** Before a rebuild, the selected objects and
  their descendants stop being certified; each returns only after it builds.

## Environment neutrality

Weaverstack must contain no defaults for product, workspace, Lakehouse,
Warehouse, endpoint, repository or notebook names, no production endpoints and
no local platform paths. Allowed defaults are generic technical values (Fabric
API URLs, auth scopes, Livy version, timeouts, polling intervals, parallelism).

This covers **examples, docstrings and test fixtures**, not just code paths. Use
neutral item names — `Sales`, `Inventory`, `Reporting`.

Weaver also has no opinion about data architecture: Folder, Delta and SQL are
materialisation forms, not tiers. `T0`/`T1`/`T2` naming is house jargon and is
rejected by `tests/test_neutrality.py`; widely-understood naming such as
bronze/silver/gold is fine where it aids a reader.

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
.venv/bin/python -m pytest              # core only — no JVM, under a second
.venv/bin/python -m pytest -m spark     # local Spark/Delta, needs Java 17
.venv/bin/weaver --help
```

Spark tests are deselected by default (`addopts = ["-m", "not spark"]`) and skip
themselves if PySpark or a supported JDK is missing, so a contributor without a
JVM is never blocked. `weaver doctor` reports what is present and what to
install; see [docs/local-setup.md](docs/local-setup.md).

Versions are declared as ranges, not pins — Spark 3.5.x with delta-spark 3.2.x,
on Java 11 or 17 — so an existing local install is not disturbed.

The `spark` fixture is **session-scoped** and the `lakehouses` fixture is
**per-test**, because those costs differ by four orders of magnitude: a session
takes ~1.2 s plus ~4.3 s of JVM warm-up on its first Delta operation, while a
local Lakehouse skeleton takes 0.2 ms. Only one `SparkSession` may be active per
process in any case. Tests stay isolated through their own `tmp_path`, not
their own session — safe because Weaver addresses Delta by explicit path rather
than through a metastore.
