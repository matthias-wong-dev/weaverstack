# Weaverstack Agent Guide

Guidance for coding agents working **on weaverstack itself**.

## Repository role

`weaverstack` is a data-engineering runtime for Microsoft Fabric built around a
central control plane. One mandatory **Weaver Lakehouse** holds the workspace
declaration under `Files/weaver_items` and the authoritative catalogue;
destination Lakehouses and Warehouses hold only materialised output.

The distribution is `weaverstack`; the import is `weaver`.

## The sibling `weaver` repository is reference-only

The repositories sit side by side:

```text
dwg-platform/
├── weaver/        reference implementation — DO NOT MODIFY
└── weaverstack/   this repository
```

`weaver` is consulted for proven algorithms, Fabric/OneLake/Spark/Warehouse edge
cases, Weaver document fixtures and behavioural intent. Never change it as part of
weaverstack work, and never import from it. Where the two disagree, the
architecture in [the master CLI plan](design/weaver_master_cli_plan.md) is
authoritative.

Reference baseline: `a97ba8a0b00dd66dff1b2c5e818403694562fd30` (the plan's
reviewed snapshot). The sibling checkout has since advanced; confirm which
revision you are reading before treating it as the baseline.

## Implementation authority

[The master CLI plan](design/weaver_master_cli_plan.md) is the authoritative plan
for the current CLI, Workspace, repository parsing and catalogue reconciliation
work. The underlying system has run in production on SQL Server for years and
the sibling `weaver` implementation proved it works on Fabric. This is
implementation, not invention: port proven algorithms rather than re-deriving
them, and spend design attention on the control plane, which is the genuinely
new part.

## The core abstraction

This is the thing that is hard to hold in your head, and the thing most likely
to be got wrong by someone reading only the code in front of them.

**Weaver is a system that runs inside Microsoft Fabric.** Fabric is its one real
workspace. We develop it on a laptop against a local emulator, and test it at both
levels. Resource location and code execution are separate axes:

```text
    WHERE RESOURCES ARE              WHERE THE CODE RUNS

    local emulator root              in-process, on a laptop
    Fabric workspace                 in-process, inside a Fabric session
                                     submitted from outside, over Livy
```

These give three useful execution paths:

| | resources | code runs | what it is |
|---|---|---|---|
| 1 | local emulator | laptop | development, and most of the test suite |
| 2 | Fabric | laptop | the desktop CLI reaching into a workspace |
| 3 | Fabric | **in Fabric** | **the product** — `pip install weaverstack` in a notebook |

**The foundational rule:** *Weaver core operates within the environment where it
is executing. Only the CLI and Fabric test infrastructure cross into Fabric.*

```text
core running locally        → operates against the local emulator
core running inside Fabric   → operates within FabricWorkspace, session-native
CLI or pytest running locally → may cross into Fabric over REST, DFS and Livy
```

A `FabricWorkspace` identifies the workspace the resources live in. It does **not**
say whether access happens through desktop HTTP clients or inside a session.
`LocalWorkspace` remains the name of the in-process emulator configuration in Python;
it is not a second deployment workspace and must not leak into durable contracts.
In particular, a build bundle binds target kind and item identifiers, never a
deployment-kind discriminator.

So the storage picture has two parts, and they must not be conflated:

*In-environment execution* — the store Weaver uses where it runs:

| execution | environment configuration | store |
|---|---|---|
| local process | `LocalWorkspace` emulator | `LocalStore` |
| Fabric session | `FabricWorkspace` | `FabricStore` over `notebookutils.fs` |

*Cross-boundary access* — a local caller reaching into a workspace:

| caller | destination | client |
|---|---|---|
| CLI | Fabric workspace | `OneLakeDfsClient` |
| Fabric integration tests | Fabric workspace | `OneLakeDfsClient` |

`OneLakeDfsClient` (ADLS Gen2 DFS over HTTPS) is **not** the Fabric equivalent of
`LocalStore`. It is how the desktop crosses in, constructed explicitly by the
caller that crosses. Inside Fabric, `store_for(FabricWorkspace)` returns the
session-native `FabricStore`; from a desktop that construction fails rather
than silently substituting DFS.

Above resolution and the store, nothing knows which environment it is using. An
`if isinstance(workspace, …)` in core operation code means the abstraction is being
broken; the fix belongs in the factories, or in the CLI that does the crossing.

**Credential choice is a caller's policy, not the core's.** Core accepts an
injected credential and otherwise uses the library default without pinning the
chain. The CLI and the Fabric test infrastructure call `prefer_cli_credential()`
themselves; importing or using the core imposes no credential choice.

### The local environment is an emulator, not a peer workspace

`.local/Sales_LH/Files` and `.local/Sales_LH/Tables` mirror the shape a Fabric
Lakehouse presents through OneLake, deliberately, so the same resolution
arithmetic serves both. It exists so that most development and most of the test
suite need no tenant, no capacity and no credentials.

### Fabric is the reference; local emulates it — never the reverse

This is the direction of the whole system, and the mistake most worth naming
because it has already been made once. **Weaver is Fabric-first.** The behaviour
that must be right is the behaviour *inside* Fabric; the local emulator exists so
that behaviour can be developed and tested quickly on a laptop. Design against
what Fabric does, then make local reproduce it. Do not design against what is
convenient locally and then contort Fabric to fit — if local and Fabric disagree,
Fabric is right and local is the thing to fix.

Concretely, for anything with two phases (as the build bundle has *generate* then
*install*): **both phases run in the target environment.** Inside Fabric that
means in the session, against the native Spark catalogue; in the emulator it
means in-process against the local catalogue. A workflow that plans on the
desktop and only executes in Fabric is a *different, lesser* architecture (row 2
dressed as row 3), and it silently loses capabilities the authoritative
catalogue provides — build bundle generation, done on the desktop with
`spark=None`, could not see catalogue views and so could not prune them. The fix
was to move generation into the session, not to accept the gap as inherent to
Fabric. When a Fabric behaviour is awkward, the question is "how does local
emulate this?", never "how does Fabric bend to what local already does?".

### Row 3 is the claim, and it is the least tested

A user should be able to open a Fabric notebook, `pip install weaverstack`, and
work. That is the product, and it is what distinguishes Weaver from tools that
demand an orchestration environment of their own. **A Fabric test that runs
Weaver on the laptop and reaches into a workspace over HTTP tests row 2, not
row 3.** Both are worth having, but only row 3 is the promise.

Row 3 is delivered by installing Weaver into a Fabric Environment: `weaver
install --workspace <ws> --environment <env>` builds a wheel from the checkout,
stages it and Weaver's dependencies, and publishes. A Livy session (and a Fabric
notebook) then attaches that Environment via `environment` on the workspace and
imports the installed package — nothing is copied into the workspace. Rerun
`weaver install` whenever Weaver Python changes; an unchanged source tree builds
the same version and the install skips the republish.

### What this means when you add a feature

Ask, in order:

1. Does it work in the `LocalWorkspace` emulator, with a test that needs no tenant?
2. Does it work against a `FabricWorkspace` from the laptop?
3. Does it work with Weaver *running inside* Fabric?

Answer all three, and answer them with tests that call the real function —
not with test code that reproduces what the function would have done. That
mistake has already been made once here: the first Fabric suite deleted files
through the store directly and looked like it was testing `wipe`.

## Architecture invariants

These are enforced by `tests/test_core_boundary.py`:

- **Core never imports the CLI.** `weaver_cli` is an optional extra; a core
  import of it would break a Fabric Environment install. The dependency runs one
  way, CLI → core.
- **The core is importable without PySpark and without Fabric credentials.**
  PySpark, `azure-identity` and `mssql-python` are lazy imports confined to the
  modules that execute against those systems.
- **One error hierarchy.** Everything derives from `weaver.errors.WeaverError`,
  including CLI errors. Add a subclass when the operation that raises it lands.
- **The CLI owns no semantics.** It parses arguments and prints results. Command
  functions return plain serialisable structures.

These become enforceable as the corresponding code lands:

- **Static discovery.** Discovery never imports object modules.
- **Objects never mutate the target.** `read()` proposes; Weaver owns mutation,
  CRUD accounting, staging and logging.
- **Every target is named, not inherited.** No destination Lakehouse is assumed to
  be attached to the notebook, and that covers *names* as well as paths: a
  generated statement says which Lakehouse it means. On Fabric that is the native
  four-part `workspace.lakehouse.schema.object`; the local emulator folds the
  Lakehouse into its one namespace level. A bare `Schema.Object` resolves through
  whatever the session is attached to — which is the Weaver Lakehouse — so it is
  the ambient-context anti-pattern in disguise.
- **Level-three identity is workspace + type + name.** An item name is unique per
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
├── src/
│   ├── weaver/       the core framework
│   └── weaver_cli/   the optional desktop CLI
└── tests/
```

## Dependencies

Base install is deliberately minimal. A dependency is declared when the feature
that first needs it lands, not in advance. See the comment in `pyproject.toml`.

## Development

```bash
python3.11 -m venv .venv
.venv/bin/pip install -e '.[dev]'
.venv/bin/python -m pytest              # core only — no JVM, under a second
.venv/bin/python -m pytest -m spark     # local Spark/Delta, needs a JDK
.venv/bin/weaver --help
```

### Codex cloud: run Spark without rediscovering the setup

Codex cloud workspaces usually already have `.venv` and a supported JDK, but
the virtual environment may contain only the core test dependencies. The JVM
also does not automatically use the HTTP proxy variables that `pip` and
`curl` understand. Use this sequence rather than diagnosing Spark from
scratch:

```bash
.venv/bin/pip install -e '.[dev]'
.venv/bin/weaver doctor
JAVA_TOOL_OPTIONS="$(.venv/bin/python - <<'PY'
import os
from urllib.parse import urlparse

proxy = urlparse(os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy", ""))
if proxy.hostname:
    print(
        f"-Dhttp.proxyHost={proxy.hostname} -Dhttp.proxyPort={proxy.port or 80} "
        f"-Dhttps.proxyHost={proxy.hostname} -Dhttps.proxyPort={proxy.port or 80}"
    )
PY
)" .venv/bin/python -m pytest -m spark
```

Why the third command matters: `delta-spark` asks Spark's Ivy resolver to fetch
Delta JVM artefacts from Maven on the first session start. Without Java proxy
properties, a proxied cloud workspace can report an unresolved
`io.delta#delta-spark_2.12` dependency followed by `JAVA_GATEWAY_EXITED`, even
though `pip install` worked. Ivy caches the download under `~/.ivy2`, so later
runs in the same workspace normally start without another download. See
[the cloud-workspace notes](design/local-setup.md#codex-cloud-workspaces) for
individual commands and troubleshooting.

Spark tests are deselected by default (`addopts = ["-m", "not spark"]`) and skip
themselves if PySpark or a supported JDK is missing, so a contributor without a
JVM is never blocked. `weaver doctor` reports what is present and what to
install; see [design/local-setup.md](design/local-setup.md).

Versions are declared as ranges, not pins — Spark 3.5.x with delta-spark 3.2.x,
on Java 11 or 17 — so an existing local install is not disturbed.

The `spark` fixture is **session-scoped** and the `lakehouses` fixture is
**per-test**, because those costs differ by four orders of magnitude: a session
takes ~1.2 s plus ~4.3 s of JVM warm-up on its first Delta operation, while a
local Lakehouse skeleton takes 0.2 ms. Only one `SparkSession` may be active per
process in any case. Tests stay isolated through their own `tmp_path`, not their
own session.

One shared session does need help with that, and the reason is worth knowing
before you add a Spark test. Delta caches a `DeltaLog` — and through it a
`Snapshot`, a query execution and its encoder — per table *path*, so a suite that
builds every table under a fresh `tmp_path` accumulates the retained state of
every Lakehouse it has already deleted. Left alone that exhausted the default 1 GB
driver heap partway through a combined `-m spark` run, and the failure surfaced as
an unreadable `Py4JJavaError` blamed on whichever test was running. An autouse
fixture in `tests/conftest.py` clears Delta's log cache and Spark's plan cache
after each test; a test that registers a *schema* still has to drop it, because
a schema is not a cache.
