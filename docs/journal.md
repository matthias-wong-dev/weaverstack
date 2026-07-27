# Build journal

A running record of what weaverstack actually is, and why.

## How this relates to the plan

[`backlog/weaverstack-step-by-step-implementation-plan.md`](../backlog/weaverstack-step-by-step-implementation-plan.md)
is a **guide**, written before construction started. This journal is the
**record**, written as construction happens.

Where the two disagree, this journal is right and the plan is stale. The
checkpoint numbering is kept because it is a useful spine, but scope has already
moved and will keep moving.

## What is actually being built

Weaver is not a new idea being invented here. The underlying system —
the object contract, the backing table and view shape, the generated load
procedure, the reconciliation semantics — has run in production on SQL Server
for years and is battle-tested. The first weaver implementation established that
the same model works on Microsoft Fabric: OneLake, Spark, Delta and Warehouse.

**Weaverstack is therefore implementation, not invention.** Two consequences
worth holding onto:

1. Where a proven algorithm exists, port it. The SQL DDL and ETL generation
   especially encode years of accumulated correctness. The plan's caution
   against "blindly preserving every legacy detail" means *don't carry
   incidental structure*; it does not mean the semantics are open for redesign.
2. What is genuinely new is the **control plane** — central catalogue,
   central repository installation, one global dependency graph, per-object
   certification. That is where design attention belongs.

---

## Standing architecture

**Four levels, named as SQL names them.**

| Level | Fabric | Local |
|---|---|---|
| 4 | workspace | root directory |
| 3 | Lakehouse / Warehouse / Environment | subdirectory |
| 2 | schema | schema directory |
| 1 | table, view, folder, procedure | table or folder |

Level 4 is the only level written down. Level 3 needs no configuration because
an item is uniquely identifiable within its host — so it is named directly.
Uniqueness, **not** invariance: promoting one Lakehouse to another inside one
workspace is ordinary, so level-3 names are always supplied explicitly at the
call site and never inferred.

**The host decides where work executes**, not where it was requested.
`--to MyFabric` runs in that workspace whether invoked from a notebook or a
desktop shell; only the transport differs.

**Three transports, each with one jurisdiction.**

| What | How |
|---|---|
| files and directories | OneLake DFS REST — identical from desktop and inside Fabric |
| Delta tables | Spark with explicit `abfss://` roots |
| Warehouse | `mssql-python` |

The Fabric FUSE mount (`/lakehouse/default/…`) is never used. It only exposes
the *attached* Lakehouse, which is precisely the dependence being removed.
weaver's `runtime/load.py:415` documents relying on it for Folder I/O; that is
the coupling weaverstack breaks.

**Nothing to learn until you need it.** Weaver should feel natural at whatever
stage a developer is at, and must not impose a learning curve ahead of the
problem it solves. That makes the naming story a progression, not a
prerequisite:

1. Two-part names inside one repository. Works immediately, no configuration.
2. Three-part names across repositories and targets — a Warehouse reaching a
   Lakehouse by naming it in full. Fabric supports this natively, so it also
   needs no configuration. Not portable across a rename, which is fine until
   it isn't.
3. `_shortcuts` bindings, adopted when portability across environments starts
   to matter.

Each step is opt-in and earns its place. A guard or a config that would force
step 3 on someone at step 1 is a design error, not rigour.

**Two kinds of validation, held to different standards.**

*Critical path* — if it passes, behaviour is wrong. A mistyped `Primary Key`
parsing as no key silently turns an upsert into a full replacement; a column
reference that is not in the schema fails deep inside Spark. Enforce hard, and
prefer a false rejection to a false acceptance.

*Fail early* — it would fail at build anyway, just later and less clearly. The
result-set count, permanent DDL in a body. Enforce only where a false positive
is impossible; otherwise record the observation and let the build be the
authority. Trading a clear build error for a wrong rejection is a bad trade.

Most upfront validation is the second kind. Being thorough there is a courtesy,
not a guarantee, and it must not cost anyone a working object.

**Config is a convenience, never a layer.** Every host is constructible in
Python. The `hosts:` file is a named lookup that can express nothing the
constructors cannot — asserted by test.

---

## Log

### Checkpoint 0 — skeleton

`weaverstack` distribution, `weaver` import, Python 3.11, hatchling.

**Core and CLI are separate top-level packages**, CLI behind an optional extra.
The one-way dependency is then enforced by packaging: a core import of
`weaver_cli` breaks any install that did not ask for the CLI. A convention plus
a lint rule would not have that property.

**One error hierarchy.** weaver had two unrelated roots (`CommandError(ValueError)`
and `WeaverError(Exception)`); everything here descends from `WeaverError`.

**Dependencies are declared at the checkpoint that needs them**, not in advance.
Base install is `pyyaml`.

### Checkpoint 1 — vocabulary

The correction that mattered: level 3 is *uniquely identifiable*, not
*invariant*. An earlier draft claimed level-3 names stay the same across
environments, which would have forbidden same-workspace promotion — a normal
deployment. Uniqueness is what removes the need for configuration; invariance
was never required and would have been a real constraint.

**Kind comes from the slot, never the string.** `DeltaTarget.parse("Shared")`
and `WarehouseTarget.parse("Shared")` produce the same `ItemRef`. What an item
must *be* is decided by the parameter it is passed to.

**`Files` is written, `Tables` is implicit.** Asymmetric on purpose: `Files` is
what a user sees in the Fabric UI, and a folder target may carry a subpath
beneath it. A Delta target names a Lakehouse and the area follows from the
object kind.

**Host entries are keyword-argument bags.** `configurable_keys()` derives from
the record, so a new host field is configurable with no parser change, while an
unknown key is still refused by name. Open in what it accepts, closed against
typos.

### Checkpoint 2 — resolution and transport

Wider than the plan's version, deliberately: the plan scoped this to local path
arithmetic, but the *type* that crosses host boundaries had to be settled before
anything consumed it.

**`Location`, because `pathlib` cannot be the currency.**
`Path("abfss://ws@host/lh")` collapses the double slash and returns a broken
root with no error. Locations always join by string; `.path` is available only
when the location is genuinely a filesystem path. There is a test asserting the
corruption, so the reason survives.

**`Store` is transport, never policy.** `move_within_store` is one operation,
not read + write + delete. Within a Lakehouse a move is a metadata rename, and
an implementation can only choose that if the intent survives the call.
Listing returns size and modification time, because every incremental strategy
needs them.

**No generic `sync()`.** Push, deployment and Folder reconciliation differ in
*deletion policy* — push owns its destination subtree and deletes what is
missing; reconciliation deletes only within its `File key` scope, and under
`Incremental` deletes nothing. Collapsing those into `sync(delete_missing=…)`
puts a data-correctness decision behind a transport flag. weaver keeps them in
separate modules (`fabric/transfer.py`, `runtime/folders.py`) and that line
holds.

**Staging, provisionally.** The author writes into a real local temp directory
(true `Path`, any library), Weaver uploads to the lakehouse staging sibling,
then promotes staging → destination by rename. Three legs, three mechanisms.
The lifecycle is not settled; the *paths* are.

### Checkpoint 3 — the SES contract

The heart of the system: a contract validated to exhaustion before anything
physical happens.

**Unknown keys are refused by name.** The highest-value guard and absent from
weaver. A mistyped `Primary Key` previously parsed as *no primary key*, which
silently converts an upsert into a full replacement — data loss presenting as
"why did the table shrink".

**References are whole-value or nothing.** `$Sales.Order` and
`$Sales.Order[Order date]` resolve to the target's corresponding field, so the
field being resolved decides what is fetched and no direction marker is needed.
`See $Sales.Order` is refused: a contract that is only sometimes
machine-readable is not a contract. `$$` escapes a literal dollar. Resolution
itself needs sibling documents and waits for the repository reader — including
cycle detection, since the lookup is recursive.

**Column sets are comma-separated; column lists are YAML lists.** `Primary key`
and `Comparison columns` are *sets* — one key, one comparison tuple. `Not null`
is several independent facts. The distinction is semantic, so the syntax marks
it.

**Audit columns follow the representation.** `Row insert/update/delete datetime`
are never authored. Warehouse keeps the spaced form already in weaver's
`sql/ddl.py`; Delta uses underscores. A live row carries a sentinel delete
datetime, hence not-null. `schema` stays exactly what the author wrote;
`effective_schema` adds them.

**Validation deferred is recorded, not skipped.** A Warehouse object infers its
shape from its query, so its column references cannot be checked here.
`defers_column_validation` says so rather than leaving the distinction in
someone's head.

**`Load mode` retired.** Behaviour follows from `Incremental` and `Primary key`.

**Layout convention:** a blank line between subsections. Unenforced, but
documented and followed by fixtures — the header is the contract a reader meets
first.

### Checkpoint 4 — the authoring surface

**Dependencies are imports, not string keys.** weaver used
`self.repo["Schema.Object"]`. Weaverstack declares a dependency by importing
the other object's module:

    from Sales__Customer import Sales__Customer as Customer
    ...
    customers = Customer.dataframe()

Discoverable from the AST without executing anything, no strings to mistype,
and — the real gain — an IDE can autocomplete and navigate to the object being
depended on. `self.repo` is gone entirely.

**Accessors are classmethods, not properties.** A class-level property needs a
metaclass, since Python no longer chains `classmethod` and `property`. The
method form is the plainer construction, and being inherited from the base
class it is visible to tooling. Under it is a registry lookup against the
running workflow, held in a `ContextVar` so concurrent steps cannot see each
other's dependencies, and raising clearly when called outside one.

**Two consequences of import-as-dependency**, both accepted:

- an unused import is a phantom dependency — a real ordering constraint with no
  data flow. Correct-by-declaration beats trying to prove usage from the AST.
- object module names and helper module names must not collide, or a helper
  import silently becomes a dependency. A repository-level guard.

**Spark SQL is in, not deferred.** Fabric Lakehouse views persist in the
metastore, so `.spark.sql` with `View ID` is a real object. The ID names the
object, not the engine, so there is no `Spark table ID`: routing is already
`(language, kind) -> target` and Spark SQL adds rows to that table.

A Spark SQL object **must declare `Dependencies`**. Its query may read by path,
and a path cannot be resolved back to a managed object — Weaver's graph is over
logical IDs, and reverse-mapping physical locations would be fragile and only
work for objects already built. Discovery still runs and is additive; the
declaration is the floor, not the ceiling. Declared dependencies can only ever
widen the graph, because a missing edge is a wrong build order, which is silent
data corruption.

A Spark SQL table declares `Schema` like Python does, since it materialises
Delta and the declared shape is what lets every column guard run up front.

### Checkpoint 5 — the repository reader

Reading a folder of object files and checking the structural contract, without
executing anything.

**File, ID and class must all agree.**

| Language | File | ID | Class |
|---|---|---|---|
| Python | `Sales__Order.py` | `Sales.Order` | `Sales__Order` |
| Spark SQL | `Sales.Order.spark.sql` | `Sales.Order` | — |
| T-SQL | `Reporting.Order.sql` | `Reporting.Order` | — |

Python uses `__` because a module name cannot contain a dot; SQL has no such
constraint and uses the dot. The class carries the *full* name rather than just
the object part, so `from Sales__Order import Sales__Order` says exactly which
object it names at the call site — explicit over short.

**The read contract.** Python: exactly one class *inheriting a Weaver base*,
named for the file, with the base matching the declared kind and exactly one
`def read(self)`. Ordinary helper classes may sit beside it — only the Weaver
class must be unique. Candidates are found by direct base name, so an object
inheriting through an intermediate class of its own is not recognised; that is
the price of never importing the module. Two `read` definitions is an error
rather than a shrug — the later silently replaces the earlier. SQL: exactly one
result-producing statement.

**The result-set check abstains rather than guesses.** Intermediate work is
fine — `select … into #tmp`, `create or replace temp view` — only one statement
may return rows. But on seeing `exec` or `sp_executesql` the check stands down
and records why. A wrong rejection blocks a legitimate object; a miss merely
fails at build the way it does today, so the asymmetry decides the calibration.

**Hashing normalises line endings and drops a BOM.** The hash answers "has this
changed since it was certified", and a checkout with `autocrlf` is not a
changed file. The repository signature is one hash over sorted
`(path, content hash)` pairs, covering support files too.

**The author writes the query; Weaver writes the `CREATE`.** A permanent
`create view` or `create table` in a body usually means the wrapper has been
written by hand. It is *recorded* on the analysis, not refused — fail-early, and
there may be a legitimate reason to create something durable inside a body.
Temporary scratch (`create temp view`, `create table #tmp`) is not even noted.

**A View is one statement.** It is checked for a single result set like any
other SQL object, and additionally may not carry preceding statements: Weaver
wraps the body in `CREATE VIEW`, and a view definition cannot contain a script.
A Table may do as much intermediate work as it likes.

**Objects live at the root; subdirectories are support.** `_`-prefixed root
files are not objects. A helper may not be importable *under an object's
module name*, because an import of it would be read as a dependency on that
object — compared on the complete dotted path, so `parsers/Sales__Order.py` is
`parsers.Sales__Order` and collides with nothing.

**`self.path` and `Folder.folder_path()` are deliberately different names.**
The dependency accessor was first written as `Folder.path()`, which replaced
the inherited `self.path` property on every Folder — silently, because a bound
method is truthy, so the failure surfaced later as a confusing `TypeError`. An
object reaches its own destination through `self.path`; it reaches a
dependency's through that object's classmethod.

**Parses are kept.** `SourceDocument` holds the Python AST and the SQL split
beside the `SesDocument`, so later checkpoints read the repository once rather
than once per question. `SesDocument` stays pure — the AST is on the wrapper,
excluded from comparison.

Reading goes through a `Store`, so the same reader will serve a repository
installed in the Weaver Lakehouse once the Fabric store exists.

### Checkpoint 6a — dependency extraction

Extraction only. Whether a name *resolves* — to an object, to a shortcut, or to
nothing — is deferred to build, where the external-dependency configuration is
supplied. Getting the names out accurately is its own piece of work.

**Python declares a dependency by importing.** The marker is structural: one
`__` in an absolute import name, neither side empty or underscore-prefixed. So
`from Sales__Order import Sales__Order` is a reference to `Sales.Order`;
`from weaver import Table` has no `__`; a helper reached as `_helpers.dates`
contributes its package name and is likewise not one. Extraction does not care
about case — `sales__order` extracts as `sales.order`, and whether that matches
an object is a build-time question.

**SQL declares them by relation position** — after `from`, `join`, `apply`,
`using` or `merge`. The elegant part is inherited from weaver: **single-part
names are never relations.** A CTE, a temp view, a temp table and a table alias
are all single-part, so requiring two parts excludes every one of them without
tracking scope.

Part count carries the meaning:

| Parts | Meaning |
|---|---|
| 2 | Weaver's namespace — an object or a shortcut |
| 3 or 4 | a physical target the author named; captured, never resolved |

**`Dependencies` replaces discovery** rather than adding to it. That gives the
author a way to *remove* an edge — the phantom dependency an unused import
creates — as well as add one. Both sets are recorded on the document, so a lint
can later report a declaration that omits something the query plainly reads.

Two things that needed changing from weaver's extractor: backticks as an
identifier delimiter for Spark, and `cross apply` — sqlparse keywords `cross`
but not `apply`, so the pair arrives as two tokens and the original never
matched it. `merge` targets were not captured either.

Spark path reads (``delta.`abfss://…` ``) parse as two parts but are a format
and a path, not schema and object, so those prefixes are excluded. Whether they
could ever be resolved is left open until tested.

**DML targets are relations too.** `insert into`, `update`, `merge into` and
`delete from` all name something that must exist. The first three were missed
entirely — `insert into` and `merge into` arrive as one keyword token or two
depending on dialect, so the intervening `into` has to be skipped. Weaver does
not restrict what an author writes; intermediate statements, temp tables and
deletion against the current table are all permitted. The obligation is only to
read them accurately.

**The test suite is organised by dialect**, over realistic complete statements
rather than snippets — `tests/test_ses_dependencies_spark.py` and
`tests/test_ses_dependencies_tsql.py`, each ending with one full file that
exercises everything together and asserts that nothing was invented.
`tests/test_ses_repository_end_to_end.py` asserts the whole chain over the
example repository: filename classification, metadata, structural checks, SQL
analysis and discovered references.

### Checkpoint 6b — the graph

**Nodes are `target:Schema.Object`, not `Schema.Object`.** An object ID is
unique *within* a physical target, not across them: `Sales.Order` may be a
folder, a Delta table and a Warehouse table at the same time, because those are
three different places. Filenames already encode part of this —
`Sales__Order.py`, `Sales.Order.spark.sql` and `Sales.Order.sql` coexist
happily — but a Python table and a Spark SQL table sharing an ID both claim
`Tables/Sales/Order` and collide. Uniqueness is enforced per target.

Routing is inferred from language and kind: a Folder goes to the folder target,
anything in a Delta language goes to the Delta target, and SQL goes to the
Warehouse. Never configured, which is what removed the old paired
source-and-target build command.

**The graph knows nothing about what an edge means**, because there is more
than one graph over the same objects. Load order follows every dependency.
Build order is nearly flat: building a Folder is a directory and building a
Delta table is a `CREATE` from its declared `Schema`, so neither needs a single
upstream object to exist. Only a Warehouse object has build dependencies,
because its shape is inferred from its query. So a build is every Folder and
every Delta table in one parallel wave, then the Warehouse objects in order,
with a SQL endpoint refresh where the first of them reads Delta. Same
primitives, different edge sets.

That boundary stays visible because node identity carries the target: an edge
from `delta:` to `sql:` is exactly where the refresh barrier belongs.

**Order is deterministic.** Ties break by name, so the same repository always
produces the same plan and two plans can be diffed — which the catalogue will
need.

**A two-part name resolves in the namespace of whoever wrote it.** T-SQL binds
inside the Warehouse, Spark SQL inside the Lakehouse, a Python import against a
file. So `join Sales.Customer` in a Warehouse query means the *Warehouse's*
`Sales.Customer` when one exists, because that is what the SQL would actually
bind to. Failing that, a single candidate anywhere is the answer, and it may
cross a boundary — a Warehouse query reading a Delta table is the ordinary
case. Two candidates in neither position is left for the build.

That rule settles almost everything without any configuration. In the fixture,
`Sales.Customer` exists as both a Delta table and a Warehouse table:
`sql:Reporting.OrderReport` resolves it to the Warehouse one, while
`sql:Sales.Customer` — reading its own namesake — resolves to the Delta one,
which is the ordinary shape of surfacing a Lakehouse table into a Warehouse.

`build_internal_graph(..., external_names=…)` is the seam the shortcut bindings
will use: a name declared external is a boundary rather than an edge. The
parameter exists now so wiring the configuration in later changes no signature
downstream. The file format waits for the build package, because a shortcut's
role — an operation that creates something, and a node with no upstream — is
only concrete once that exists.

**A cross-boundary read is written in three parts, and that is enough.** Fabric
lets a Warehouse reference a Lakehouse table as `Lakehouse.Schema.Table`
directly, so no shortcut is required — a repository building bronze into a
Lakehouse and another building silver into a Warehouse can simply name the
Lakehouse. The fixture does exactly that, and `_shortcuts` is what you reach
for later, when the Lakehouse name should stop being hard-coded.

The consequence for the graph: a three-part read *cannot* resolve at parse
time, because whether `Sales_LH` names this repository's own Delta target is
only known once the build is handed its targets. So those references are
recorded as pending rather than turned into edges, and the cross-boundary edges
appear when the build resolves them. What remains after that is genuinely
outside — in the fixture, a table-valued function nobody defines.

Cycles are refused when the repository is read. A repository whose graph cannot
be ordered is not a repository worth handing on.

### Local test substrate

Local build and load come before any Fabric work, so the suite needed a way to
stand up Lakehouses without a JVM being mandatory.

Measured, because the fixture scoping follows from it:

| | cost |
|---|---|
| Spark session start | 1.24 s |
| first Delta write+read (warm-up) | 4.31 s |
| later Delta write+read | ~0.75 s |
| a local Lakehouse skeleton | 0.0002 s |
| session stop | 0.42 s |

So the `spark` fixture is session-scoped and the `lakehouses` fixture is
per-test. Only one `SparkSession` may be active per process anyway, and the
warm-up is not worth paying twice; the directories are free enough that sharing
them would only invite contamination.

Sharing one session across tests is safe **because Weaver addresses Delta by
explicit path rather than through a metastore** — a session carries no state
between tests. That is the same property that lets a Fabric notebook write to a
Lakehouse it is not attached to, showing up as a testing convenience.

Two environment traps, both handled in the fixture rather than in a shell
profile: `PYSPARK_PYTHON` defaults to the system interpreter and fails deep
inside a task with a version mismatch, so it is pinned to `sys.executable`; and
`JAVA_HOME` is discovered when unset. Missing PySpark or Java skips rather than
fails, so the default run needs neither.

**Versions are ranges, not pins.** The first cut wrote
`pyspark==3.5.1, delta-spark==3.2.0` — one machine's install mistaken for a
requirement. Spark 3.5.x with delta-spark 3.2.x is the real compatibility
window, since the two are released in lockstep, and Spark 3.5 runs on Java 8, 11
or 17. The first Java discovery asked `/usr/libexec/java_home -v 17`
specifically, which would have skipped every Spark test on a Java 11 machine —
a working setup reported as an unsupported one.

`weaver doctor` reports Python, PySpark, delta-spark and Java in one pass, with
the command that fixes whatever is missing and a non-zero exit so it can gate a
script. It exists because the alternative way to discover a missing JDK is a
Java stack trace, and it is the CLI's first real command: the check lives in
`weaver.diagnostics`, the CLI only prints it.

### The build command, as it actually works

Correcting two things the plan and an earlier draft got wrong.

**Checkpoints 11–16 are one piece of work, not six.** The plan's granularity
does not match the shape of the thing.

**The build package is a folder of scripts**, not a set of declarative
operations. Generated, ordered, inspectable before anything runs, and runnable
separately.

The sequence:

```text
weaver build --from MyRepo --to LocalHost --weaver_lakehouse … \
             --folder_target … --spark_target … --sql_target … --config env.yml
```

1. Copy the repository into the Weaver Lakehouse at `Files/repos/MyRepo`.
   Locally a copy; on Fabric a push. After this the host holds the source.
2. On the host — where Weaver is installed or importable — call
   `generate_build_package(weaver_lakehouse=…, repo="MyRepo", folder_target=…, …)`.
3. That writes a folder of scripts in dependency order, to read before running.
4. Run it, or run it later with `install_build(package_directory)`.

**Each target is independently optional**, though at least one is required. An
absent target means those objects are assumed to exist already — deliberate
developer latitude, possibly withdrawn later.

The intricate part — incremental build driven by signature comparison — is
explicitly deferred.

### Wipe

Per physical target, because the three are different places with different
mechanics.

**Delta needs no catalogue.** Weaver addresses tables by explicit path and never
registers them, so a table is a directory and wiping is removing it. There is
nothing to enumerate from and nothing left dangling — the same property that
lets a Fabric notebook write to an unattached Lakehouse, showing up again as a
simplification. On Fabric the Lakehouse auto-discovers what appears under
`Tables/`, so removal should de-register too; worth confirming against a real
workspace.

**Folders** keep the configured root and lose its contents. A folder target may
be a root *within* `Files`, and a wipe respects that scope.

**Warehouse raises `NotImplementedError`.** It wants one dynamic statement built
from the catalogue views, and there is no local SQL to develop it against.

A wipe clears the *target*, not merely what Weaver manages — which suits a
development loop and makes it something the CLI has to gate. `dry_run` reports
without removing, and a guard refuses any location outside the host root. That
guard should be unreachable, since locations are derived rather than supplied,
which is exactly why it is worth having.

**The initial command-line design used one generic target with no kind.**
Its *shape* decided: `Sales_LH` named an item and
`Sales_LH/Files/Extracts` named a folder root. That was later replaced by typed
target flags when Fabric's per-type name identity became concrete.

```bash
weaver wipe --lakehouse-target Sales_LH --host MyLocal --hosts env.yml --dry-run
weaver wipe --warehouse-target MyWarehouse --host MyFabric --hosts env.yml
```

The plan is always printed first, then acted on. Without `--yes` it asks; with
no terminal to ask on it refuses and says so, so a script cannot destroy
something by omission. `--root` builds a local host without a config file, since
nothing should require a file to be expressible.

`_add_host_args` and `_resolve_host` are shared, so `build` and `load` inherit
the same `--host`/`--hosts`/`--root` handling.

### Where code runs

Three positions, not two — and the third is needed far less than expected.

| | | |
|---|---|---|
| **A** | local process, local effects | the core and Spark suites |
| **B** | local process, remote effects | pure HTTP: ARM, Fabric REST, OneLake DFS, TDS |
| **C** | code executing inside Fabric | a Spark session there |

What actually needs **C**: Delta table creation and load, Spark SQL objects,
and catalogue writes. Everything else — capacity, item CRUD, pushing a
repository, creating folder destinations, folder load, Warehouse DDL and load,
SQL endpoint refresh — is **B**, and B runs from a laptop with nothing shipped
anywhere.

**So `pytest` is not shipped to Fabric.** Testing is a development activity.
Remote pytest gives poor feedback, most of what wants testing is position B and
already reachable, and where a test genuinely needs code inside Fabric it can
submit a program and assert on the JSON result — which is the mechanism build
and load need regardless. Don't invent a second way to get code into Fabric;
use the one build already requires, so the transport is tested by being used.

**Host says where; executor says how.** "Remote Fabric" and "native Fabric" are
not two hosts — they are the same workspace reached differently.

| position | host | executor |
|---|---|---|
| desktop → local | `LocalHost` | in-process |
| desktop → Fabric | `FabricHost` | Livy submit |
| notebook → Fabric | `FabricHost` | in-process |

Detected rather than declared. A host left unnamed inside Fabric means "the
workspace I am running in", because the notebook path is meant to be primary and
making someone name the workspace they are sitting in is friction without
safety.

The wheel is 62 KB of pure Python with two pure-Python dependencies, so getting
Weaver into a session is not the hard part. The proven route is to copy it into
the Weaver Lakehouse Files area and `sys.path.insert` it — the architecture
summary already anticipated this as the bridge until PyPI.

### OneLake as a Store

`FabricStore` satisfies the same protocol as `LocalStore`, so everything above
it is written once. Verified against real OneLake: write, read, exists,
is_directory, shallow and recursive listing with sizes and timestamps, move,
delete, idempotent make_directory.

Two things OneLake does differently, both now encoded:

**A write is three calls** — create the file, append the bytes, flush at the
final offset.

**There is no directory rename.** `move_within_store` copies and deletes here.
The operation stays whole so an implementation inside a Fabric session can use
`notebookutils.fs.mv` instead — the caller never learns which happened, which is
the point of having made it one operation rather than three.

### Getting Weaver into Fabric

The product claim is that someone opens a Fabric notebook, installs Weaver, and
works. Everything built so far runs Weaver *on a laptop* and reaches into a
workspace — which proves the modules and not the claim. See the core abstraction
in `AGENTS.md`: that is row 2, and row 3 is the promise.

Until PyPI, row 3 needs the package shipped into the workspace.

**`weaver_install` is a host key**, not the derived convention argued for
earlier. The convention would have been fine as plumbing, but this is the thing
under test — a test that says "Weaver came from here and ran there" wants the
location visible rather than inferred. The convention remains the default when
the key is absent.

**Sync compares content hashes, not timestamps.** "Periodically" leaves a window
where the laptop's Weaver is newer than the workspace copy and results diverge
silently, which is the worst kind of bug because everything appears to work. At
62 KB the comparison is cheap enough to make every time.

**The bootstrap tries `import weaver` first.** The day a Fabric Environment
carries Weaver, the fallback is dead code and the host key can be deleted with
nothing else changing.

**A Livy session has no FUSE mount.** This was the assumption the whole
bootstrap rested on, and it is false: `/lakehouse` exists inside a Livy session
but is *empty*, unlike a notebook where the default Lakehouse appears at
`/lakehouse/default`. Putting the Lakehouse path on `sys.path` therefore cannot
work, and the first two versions of the bootstrap both did exactly that.

What does work, verified: copy the package into the session from its explicit
`abfss` root with `notebookutils.fs.cp`, then insert the local parent.

```python
notebookutils.fs.cp(
    "abfss://…/Files/weaver", "file:/tmp/weaver_runtime/weaver", recurse=True
)
sys.path.insert(0, "/tmp/weaver_runtime")
import weaver
```

That works in a notebook too, so there is one bootstrap rather than two, and it
needs nothing attached — the same reason every destination root is explicit.

The bootstrap belongs to the session, not to its callers:
`LivySession.for_host(host)` builds it from the host config and runs it once on
start, so a caller submits its work and nothing else.

Also fixed on the way: `import weaver` searches `sys.path` for a *directory
named weaver*, so the parent goes on the path, not the package itself.

**Livy sessions are expensive to start and cheap to reuse**, so a session is
held open across a batch rather than paid for per statement. A submitted program
returns a value by printing a tagged JSON line, so a result can be told from
whatever else was logged — printed output and returned values are otherwise the
same channel.

### Two Fabric behaviours, found by running it

**A Lakehouse grows a `SQLEndpoint` sibling of the same name**, a little after
creation. So item names are unique *per type*, not across types — a weaker
guarantee than level three appeared to rest on. Resolving a name without a type
ignores that facet, since a SQL endpoint is not something anyone addresses
directly.

**Fabric holds a deleted item's name for minutes**, answering
`409 ItemDisplayNameNotAvailableYet`. Neither of these was guessable, and both
were found within an hour of actually running against a tenant.

### Build is the act of locking in

Getting the SES source to Fabric looked like a delivery problem with several
answers — a desktop push, a manual upload, or Fabric's own Git integration via
notebook resources, which is the only place in a workspace where source files
get versioning and pull requests, since Git integration covers *items* and not
Lakehouse file contents.

They are not competing answers, because **the copy into
`<weaver-lakehouse>/Files/repos/<name>` is not logistics — it is certification.**

That snapshot is the artifact the catalogue certifies against. The source hash
recorded at build refers to *that copy*, so:

- editing the original after a build changes nothing, and load keeps running
  what was certified. That is the safety property the catalogue exists for: an
  object is safe against the versions of its dependencies that were certified,
  and its own source is one of those versions;
- every delivery route converges at build. A local checkout, a OneLake push,
  notebook resources — they differ only in what `--source` points at, and build
  normalises all of them into one central snapshot;
- load never learns where the source came from. One code path, not three.

The repository `signature` becomes load-bearing here: build records it, load
verifies the installed copy still matches, and a mismatch means somebody edited
installed source instead of rebuilding.

This also settles a question left open at checkpoint 2. Build *does* move files
— exactly one thing, the repository snapshot — and that movement is the point
rather than a side effect.

**The parent directory is free.** Weaver has no opinion about where a
repository folder sits, so authoring it *inside* a notebook's resources folder
in a local checkout costs nothing and keeps the Fabric Git route open. Every
delivery route is then the same command with a different `--source`. Documented
in `docs/ses-repository.md` as a convenience rather than a requirement — the
only argument for starting early is that starting early is free and starting
late means a move.

### A notebook can carry everything

A Fabric notebook holding the SES repo as resources needs no Livy: it invokes
Weaver directly, cell by cell, and can be scheduled or triggered through the
notebook job API. That adds a fourth executor and costs nothing, because the
notebook is only a caller of the Python API.

It carries one constraint worth stating as a rule rather than leaving as a
habit: **in that mode there is no CLI at all**, so every command must be a
Python function with a signature good enough to call by hand in a cell, and the
CLI must stay a thin printer over it. The moment logic grows inside
`weaver_cli`, notebook users lose it silently.

The notebook can carry the wheel too — 62 KB beside the SES folder, `%pip
install`ed from the resource path — which for a real user today may be a better
story than `weaver_install`. That leaves a clean division: `weaver_install` is a
development tool for shipping a working tree on every change; the
notebook-carries-everything pattern is the interim answer for users. Both
disappear when `pip install weaverstack` works.

### Review corrections

A round of review corrected several places where the code proved more than it
supported, or expressed the wrong foundation. All accepted.

**Within-host is the foundation; host and executor are independent.** A core
operation stays within the one host it was given. Cross-host movement is CLI
orchestration and must not shape the `Store`. The claim that "a host determines
where work executes" is removed from `hosts.py`. `FabricStore` over DFS is
reframed as the *desktop's* transport into a workspace, not the canonical
in-Fabric path.

**Credential policy leaves the core.** `auth.credential()` no longer pins
`AZURE_TOKEN_CREDENTIALS` as a side effect of asking for a token. The CLI and
the Fabric test infrastructure call `prefer_cli_credential()` themselves. Using
the core imposes no credential choice.

**`move_within_store` is gone.** I added it speculatively at checkpoint 2 on the
reasoning that a same-store move is a cheap rename — but OneLake has no directory
rename, `LocalStore` fell back to `shutil.move` across filesystems, and
`FabricStore` copied every byte. The name promised metadata-rename intent that
neither implementation kept. Staging promotion, destination replacement and
atomic publication are real needs; their contract comes from the load algorithm,
not a guess. Load introduces what it actually needs.

**OneLake listing fails loudly when paged.** It silently returned the first page,
which would truncate a wipe, a sync or a reconciliation. It now raises on an
`x-ms-continuation` header. Full pagination is deferred; silent truncation is
not acceptable in the meantime. Proven by a mocked test — no tenant needed.

**Identity is host + type + name, not host + name.** The earlier "unique within
a workspace" was too strong and the code already knew it — a Lakehouse grows a
same-named SQL endpoint. Resolution is now typed: the slot supplies the type,
and core never asks the workspace what a bare name "is". `wipe_item` — which did
untyped discovery and chose destructive behaviour from the answer — is replaced
by `wipe_lakehouse`, resolved explicitly as a Lakehouse. The CLI constructs the
typed selection before calling core. `artifact_segment` is renamed
`lakehouse_artifact_segment`,
because its `.Lakehouse` suffix was always Lakehouse-specific.

**The Fabric wipe test is gone; the local vertical is stronger.** The deleted
test built files that merely *resembled* a Delta table (`part-0000.parquet`, a
hand-written `_delta_log/*.json`) and ran wipe from the laptop over DFS — it
looked like in-Fabric validation and was not. Real in-Fabric wipe coverage
returns when it can create a genuine Delta table inside a session and call the
actual implementation there. Meanwhile the **local** lifecycle is a real
build → load → wipe → rebuild, executing saved `build.spark.sql` and
`load.spark.sql` rather than `createDataFrame`, and asserting the environment
recovers on a second pass.

A build constraint that lifecycle surfaced: **Delta rejects spaces in column
names** unless column mapping is enabled, and Weaver's declared schemas have
spaced names (`Order id`). The build DDL will have to carry
`delta.columnMapping.mode = name`. Found by running it, not by reasoning.

Verified against the tenant after the refactor: 21 resource/store tests and 7
in-Fabric Livy tests still pass under typed resolution.

### Sharpening the host boundary

A follow-up review pushed on the storage naming, which still blurred within-host
execution and desktop access into Fabric. The foundational rule, stated plainly:

> Weaver core operates within the host where it is executing. Only the CLI and
> the Fabric test infrastructure cross from one host into another.

A `FabricHost` identifies the workspace the resources live in; it does not say
whether access is a desktop HTTP client or an in-session mechanism. So storage
has two separate pictures, and conflating them was the error:

- **within-host** — `LocalHost` → `LocalStore`; `FabricHost` → a future
  session-native store;
- **cross-boundary** — CLI or test → workspace → the DFS client.

**`FabricStore` is renamed `OneLakeDfsClient`.** The old name read like the store
Weaver uses inside Fabric; it is specifically an ADLS Gen2 DFS client used *from
outside* Fabric. It still satisfies the `Store` protocol so the CLI can hand it
to the same code a `LocalStore` drives, but it is cross-boundary access, not the
in-host Fabric store.

**`store_for(FabricHost)` now raises** rather than returning the DFS client. A
within-host factory returning a desktop transport was exactly the conflation:
the CLI (and the test infra) construct `OneLakeDfsClient` explicitly and inject
it, so desktop DFS is never the default Fabric storage path. Verified against the
tenant after the rename — the DFS store and the in-Fabric Livy sync both pass.

The AGENTS.md abstraction section now carries the two-table separation as the
authoritative statement of the boundary.

### Fabric absence is explicit

Environment installation now distinguishes absence from failure at both
lookups that can legitimately return nothing. A zero-match item lookup raises
`ItemNotFoundError`; only that error permits creation of a missing Environment.
Other lookup failures — including ambiguity and Fabric API errors — propagate.

`FabricError` retains the HTTP status returned by Fabric. Reading an
Environment's published libraries maps only status 404 to the valid
"never published" empty state; authentication, throttling and server failures
propagate instead of triggering a restage and republish.

### Wipe proves the within-host Fabric path

The local populated-Lakehouse wipe test now has a Fabric parameter with the
same body and the same saved Spark SQL DDL/DML. Its Fabric fixture creates one
disposable target, populates it through an Environment-backed Livy session,
runs the actual `wipe_delta_target` inside that session, and deletes the target
in `finally`. Desktop OneLake access is used only to set up shared file content
and independently inspect the result.

That test made the deferred within-host storage path due. `FabricStore` now has
its original, correct meaning: session-native directory operations backed by
`notebookutils.fs`. `FabricSessionResolver` obtains the current workspace from
NotebookUtils and resolves Lakehouse names there, producing native `abfss`
locations. `resolver_for(FabricHost)` selects it only when NotebookUtils is
present; desktop callers retain the REST resolver and explicitly inject
`OneLakeDfsClient`. Binary session reads and writes remain unimplemented until
a proven binary NotebookUtils contract is needed; wipe uses only exists, list
and recursive delete.

### Warehouse SQL keeps the two caller boundaries separate

Warehouse wipe is the first consumer of the installed SQL runtime. One common
`PooledSqlExecutor` now owns parameters, result sets, cursor lifecycle, commit,
rollback, and error translation. It is backed by small endpoint-specific pools;
every replacement physical connection requests current authentication material.

The authentication paths remain intentionally different. Desktop callers
inject an Azure credential and request `SQL_SCOPE`; installed Weaver uses
`notebookutils.credentials` for the SQL resource audience. A `FabricHost` alone
never selects desktop SQL from inside Fabric or Fabric-native SQL from a laptop.
The desktop factory is explicit, and the production factory fails outside a
Fabric session.

The Fabric resolver obtains the typed Warehouse item and its dedicated
connection-string endpoint. Both execution positions converge on a
`SqlEndpoint` carrying workspace ID, Warehouse item ID, server, and database;
the full connection string is not used as pool identity.

The legacy dynamic wipe was ported as the pure
`generate_warehouse_wipe_sql()`. Its sources and the two proven connection
patterns are recorded in [`sql-execution.md`](sql-execution.md). Warehouse wipe
does not acquire a `Store`, return a wipe report, or offer dry-run behaviour.

The opt-in vertical creates a uniquely named disposable Warehouse, waits
separately for its REST endpoint and successful SQL query, populates it through
desktop `mssql-python`, invokes installed Weaver through Environment-backed
Livy, independently verifies the catalogue is empty, confirms the Warehouse
item survived the wipe, and deletes it in fixture cleanup. Capacity remains an
external prerequisite. The fixture prints stage timings so the disposable
approach can be judged from measured provisioning cost.

The first complete run against the `Weaver` test workspace passed. The
Environment-backed Livy session started in 42.03 seconds. The disposable
Warehouse then took 7.68 seconds to create, 0.70 seconds to expose its endpoint,
2.01 seconds for the first SQL connection, 0.05 seconds for the first
`select 1`, 5.37 seconds to populate, 4.61 seconds to wipe from installed
Weaver, and 0.29 seconds to delete. Six fixture objects were independently
observed before wipe and none afterwards. Its total fixture lifetime was 21.80
seconds; the whole pytest, including Livy startup, took 68.93 seconds. That
measurement does not justify a permanent shared Warehouse fixture.

### The wipe CLI exposes typed target slots

The CLI no longer infers a destructive operation from a generic `--target`.
`--lakehouse-target`, `--warehouse-target`, and `--folder-target` are repeatable
and may be mixed. This is necessary rather than cosmetic: a Fabric workspace
can contain a same-named Lakehouse and Warehouse, and level-three identity is
workspace + type + name.

The handler remains an adapter over core. It constructs `ItemRef`,
`WarehouseTarget`, or `FolderTarget`, obtains desktop DFS only for Lakehouse and
folder selections, obtains desktop SQL only for Warehouse selections, and calls
the existing core wipe functions. A Warehouse-only CLI call never asks for a
`Store`. Underscore aliases are accepted for shell callers while help presents
the canonical hyphenated flags.

The desktop path was then exercised through the real command:

```bash
weaver wipe --warehouse_target "Play Warehouse" \
  --host Weaver --hosts env.yml --yes
```

`Play Warehouse` was independently inspected as empty before the command, the
CLI completed the core Warehouse wipe, and a separate desktop catalogue query
confirmed it remained empty afterwards. The test changed no pre-existing user
objects.

### Declared schemas, explicit aliases, and a closed repository

CP6b left cross-engine access resting on two conveniences: a two-part name that
found a single candidate anywhere resolved to it even across a boundary, and a
three-part physical read was the sanctioned way to reach the other engine. Both
worked, but both inferred meaning the repository never stated. This checkpoint
replaces the inference with declaration, so the repository closes: every
ordinary two-part reference resolves *within* it, or it is an error.

**Schemas are declared, one file per schema, under `_schemas`.** `_schemas/DWG.yml`
declares `Schema ID: DWG`; the filename must match the ID exactly, case included.
Every schema an object ID or an alias implies must be declared — nothing is
conjured because a two-part name happens to use it. Unused declarations are
fine; an undeclared one is refused with the file it should live in. Schema files
are a distinct repository resource, read separately from object files and kept
out of the support-file set, but covered by the repository signature like
anything else that travels with it.

**Cross-engine access is an explicit alias, not an inference.** A Lakehouse
object (a Delta table or Spark view) may publish a `Warehouse alias`; a Warehouse
object (a SQL table or view) may publish a `Lakehouse alias`. Folders publish
nothing — they are Files, not a table namespace — but still need a declared
schema. The alias is a deliberate export: Weaver does not surface every object
into both engines. It may rename — `Staging.Customer` can surface as
`Sales.Customer` — so it is parsed through the same two-part model as an ID.
Each published name must be unique in its destination and must not collide with a
native object already living there, so every name in a namespace has exactly one
owner before resolution begins. A Lakehouse-native object and its *own*
same-named Warehouse alias are fine, because they live in different namespaces.

**Resolution is namespace-strict and closes through aliases.** A Warehouse query
naming `Sales.Customer` resolves against Warehouse natives, then Warehouse
aliases — never against a Delta table by proximity. When it lands on an alias,
the dependency edge points at the *native* object that declared it, and records
how it resolved (`native`, `lakehouse_alias`, `warehouse_alias`). That provenance
is what a later planner needs to know where an alias must be materialised. A
Python import still resolves to the exact module it names, so a Folder and a
Delta table of one ID stay distinguishable. The old loose-candidate rule is
gone.

**A name may be shared across targets, but a Folder and a Delta table may not
share one.** Native identity is partitioned by target, not merged into two
namespaces, so a Delta table and a Warehouse table of one name — the ordinary
cross-engine case — coexist; the `cross-engine` fixture carries `Sales.Ledger`
as both. What is refused is a Folder and a Delta table of one name: both are
Lakehouse, and a Python Folder and a Python Delta table of that ID would be the
very same `.py` file, so the pairing cannot even be written. It is rejected
explicitly rather than left as an impossible case a reader must reason out, or a
silent resolution ambiguity. What is also forbidden is *alias ambiguity*: an
alias may not land on a name a native object already owns in that namespace.

**One spelling per name, one spelling per schema.** Identities are compared
case-insensitively, so the same name written two ways — `Sales.Ledger` as a
Delta table and `sales.ledger` as a Warehouse table, or `_schemas/Abc.yml`
beside `_schemas/abc.yml` — is two names for one thing that the model cannot
tell apart. A repository must not contain such a pair; whichever it is, one must
change. (The schema pair is only reachable on a case-sensitive file system,
which is exactly where it must be caught.)

**Two-part names are managed; three-part names are not.** An unresolved two-part
reference is now a repository error — a missing object, a missing alias, a typo,
or a cross-engine read that was never published. A three-part (or four-part)
name is left exactly as before: it addresses something physical and often
outside the repository — genuine source data — so it is recorded as external,
never refused. Table-valued functions read like two-part relations
(`cross apply Sales.Split(…)`), so the extractor now tags a name abutting a `(`
as a call and exempts it, the way CTEs and temp tables were already exempt. This
is the one parser change the strict rule required.

The `cross-engine` fixture demonstrates the whole contract with no hard-coded
target: `Raw.Customer` (Folder) → `Sales.Customer` (Delta, publishes a Warehouse
alias) → `Reporting.CustomerSummary` (Warehouse, publishes a Lakehouse alias) →
`Sales.CustomerFeature` (Delta), a loop that crosses Lakehouse → Warehouse →
Lakehouse and back. Cross-engine cycles through aliases are refused like any
other. `schemas_by_namespace` derives which schemas each engine will eventually
need to materialise, though creation is deferred.

`build_internal_graph(..., external_names=…)` survives for the lower-level graph
tests, but the repository reader no longer uses it: the alias-closed graph is
built and stored during the read, and `.graph`/`.unresolved` expose it. The
`_shortcuts` idea from CP6b is now only about reaching *another repository's*
objects; within a repository, an alias is the answer, and the sales-etl fixture
keeps its three-part reads to prove those are still first-class.

### The build bundle — plan once, install anywhere

The first complete build vertical: an installed repository is read and
validated, projected onto the supplied physical targets, and turned into a
**fully bound bundle** that a separate installer runs without ever re-reading or
re-planning the source. The whole of interpretation lives in
`generate_build_bundle`; the whole of execution lives in `install_bundle`. That
separation is the point — the same bundle can later run locally, through Fabric
Livy, or from a notebook, and no installer surface gets to reinterpret the
repository.

`weaver.build_bundle` carries it — named for the subsystem, and to sidestep the
conventional `build/` gitignore rule that would otherwise swallow the package. A
kept bundle lands under `<weaver-lakehouse>/Files/build_bundles/<name>` (normally
a timestamp); a bundle bound straight for the installer can instead use a
throwaway directory. The manifest is a flat, ordered hierarchy — plan →
sequence → batch → action — serialised to a canonical `plan.yml`. **Sequences
are barriers**, run in order; **a batch is bound to exactly one target**, so a
physical destination is named once rather than repeated inside actions;
**actions are independent units**, each with its own payload, hash and result.
`bundle_id` is a content hash over the manifest and its payload hashes, no
timestamp, so equal inputs give equal identity. Loading validates structure
first (ids, targets, executors, ordering, omissions, payload paths) then payload
presence and hashes, before any action can run.

**The source owns its create syntax.** `SourceDocument.create_ddl()` generates
the installable definition: a Spark SQL view wraps in `CREATE OR REPLACE VIEW`, a
Python object becomes a small deterministic wrapper that imports the object and
hands the class to the runtime, T-SQL raises at a deliberate v1 boundary. The
payload embeds no physical path — Spark SQL binds by two-part name, Python reads
the installer's ambient context — so a bundle is independent of where it installs.

**Projection is maximal and coherent.** Keep every node whose target kind is
bound, drop the unbound, then drop to a fixpoint anything stranded above a
dropped producer, so no retained node is ever planned with a missing dependency.
Every omission carries a typed reason, so a missing Warehouse binding is visible
rather than a mysterious absence — a Lakehouse-only build of a repo with a
Warehouse leaf installs the Lakehouse chain and records the leaf as
`target_unbound`. A *supplied* Warehouse binding over T-SQL work raises the
explicit v1 `NotImplementedError` instead of silently omitting.

**Build creates structure; load populates it — and build does neither twice.**
The first cut of this work mixed the two: a Python object's payload imported the
object and ran its `read()` to write rows. That was wrong. Build reads the
repository *once* to freeze a bundle and then only creates structure —
`create_ddl()` returns pure Spark DDL: a Delta table is `CREATE OR REPLACE TABLE`
over its **declared** columns, a view is `CREATE OR REPLACE VIEW` over its query
body, a folder is an empty directory. Nothing runs `read()`, reads a CSV, or
writes a row; populating a table is *load*, a separate phase, and the whole load
runtime (`materialise`, context injection, the dependency-resolver binding) was
removed. Schema is therefore always declared, never inferred from data — which
is deliberate, because inferring a Delta schema from a CSV or spreadsheet is too
risky to do silently.

**Local Delta is registered into the catalog, path-free.** In Fabric, declaring
a Lakehouse table makes `Schema.Table` queryable by name immediately, and local
must mirror that or the model diverges. The one physical path a build resolves is
a schema's storage: the schema-create action is `CREATE DATABASE … LOCATION
'<Tables>/<schema>'`, so a managed table created under it lands where Weaver
addresses it while the table DDL stays a path-free `CREATE OR REPLACE TABLE
Schema.Object (…)`. A Spark SQL view then binds its inputs by two-part name. A
spike (`tests/spark/test_local_persisted_view.py`) proved table → view →
view-on-view resolves in the shared session; durable cross-process catalog
persistence is deliberately not a prerequisite.

**Build is reconciliation, and the prune is frozen.** A build removes anything in
the target it does not manage — an unmanaged schema, folder, table or view —
before creating the managed set. The critical property, and a correction to a
first cut that computed the prune *dynamically at install*: the drops are frozen
into the bundle at **build** time. The build inspects the visible target now,
diffs it against the managed set, and bakes a concrete drop per orphan — a `DROP
TABLE`/`VIEW`/`DATABASE` as a Spark SQL payload, an unmanaged folder as a
directory-removing action. The installer runs exactly those and enumerates
nothing. This is the whole point of a frozen bundle: you can read `plan.yml` and
the drop payloads and see precisely what an install will remove *before* it
runs — a dynamic prune would instead delete whatever the live catalog happened to
say at install, so a registration glitch could quietly wipe production.
Reconciliation is scoped to the one bound Lakehouse's own storage — a table is a
directory under `Tables/`, a folder under `Files/` (never the reserved
`repos`/`build_bundles` areas) — so a shared Spark catalog cannot make a build
reach into another Lakehouse. It is on by default; `generate_build_bundle(prune=
False)` opts out when the target is not visible to inspect, and a Spark session
lets the inspection also see catalog views.

**The installer trusts nothing it has not just checked.** It validates the
bundle, resolves targets through an injected environment, and runs sequences as
barriers with one recorded result per action; a failure fails its sequence and
no later sequence starts, with skipped sequences recorded rather than omitted.
Because build executes only generated DDL and folder/prune actions, the installer
never puts the snapshot on the import path or imports object code. Concurrency
starts serial — one shared local Spark session gives no useful parallel DDL —
while the manifest still models independent actions, so a Fabric installer can
add session concurrency later without changing bundle semantics.

The proof is `tests/fabric/test_build_bundle.py`, written environment-neutrally
and run in Fabric and its local emulator. It generates a bundle, **deletes the
source repository**, then installs from the bundle alone and verifies a real
Folder directory, an *empty* Delta table of the declared shape, a persistent view,
and a view-on-view — structure, not data. A second test seeds the target with
unmanaged objects, asserts the build froze a drop for each, and asserts they are
gone after install while the managed set is present; a third rebuilds a bundle
with an invalid view payload (hash matching) and asserts the barrier; a fourth
checks the report is written into the bundle.

**Execution environment and executor stay independent, and the Fabric path is
proven on a real workspace.** A `BuildEnv` (in `tests/fabric/conftest.py`) hides
transport behind callables the way `PopulatedLakehouse` does for wipe. Generation
and installation both run in the target environment: in-process against the
shared Spark session for the emulator, and as Livy programs against the
session-native store, resolver and Spark for Fabric. The desktop only stages the
repository and reads results. Thus Fabric planning and installation execute
*inside* Fabric (position C) against the authoritative catalogue. The same four
behavioural tests pass in the emulator and Fabric (`pytest -m fabric` against the
Weaver workspace, capacity resumed for the run and suspended after).

**Fabric is the reference; local emulates it — do not invert.** A first cut
generated the bundle on the desktop with `spark=None` and only installed
in-session. That is a *different, lesser* architecture (row 2 dressed as row 3),
and it silently lost catalogue view pruning: planning ran outside the Fabric Spark
catalogue, so it could not see views to freeze a `DROP` for them. The fix was to
move generation into the session, not to accept the gap as inherent to Fabric —
the rule now stated plainly in `AGENTS.md`.

Making the Fabric side green taught the platform's real contract, one live run
per fact:

- **Trident Spark refuses `CREATE DATABASE` on a Lakehouse** — use `CREATE
  SCHEMA` (and `DROP SCHEMA`). Local keeps a `LOCATION` clause so a managed table
  lands under the resolver's Tables path; a **schema-enabled** Fabric Lakehouse
  manages the location itself, so a managed `CREATE TABLE Schema.Object` lands at
  `Tables/<schema>/<table>` and views bind by two-part name. Path-addressed
  ``delta.`…` `` table creation, which works locally, is *rejected* on Fabric —
  the reverse of the initial guess.
- **The bundle is one physical place under two URL schemes**: generation writes
  it in-session through `FabricStore` as `abfss://…`; the desktop test handle
  addresses that same place over OneLake DFS (`https://…dfs…`). The installer
  re-resolves the bundle by name in-session, so it reads the abfss form.
- **`FabricStore` gained byte `read`/`write`** (over `notebookutils.fs`
  head/put) — the installer reads `plan.yml` and payloads from OneLake and writes
  the report back. UTF-8 text, which a bundle is.
- **`dbo` is a reserved schema** — a schema-enabled Lakehouse's default `dbo`
  cannot be dropped, so a prune never touches it, as it never touches
  `repos`/`build_bundles` under Files.
- **A Fabric managed table's physical path is host-chosen (lower-cased)** while
  the resolver produces the declared case, so table existence is asserted by name
  through the catalog, not by a case-exact path; a Folder, which Weaver `mkdir`s
  itself, is still path-checked.
- **`SHOW DATABASES` returns qualified `Workspace.Lakehouse.schema` names** on
  Fabric, so the prune enumerates catalogue views per surviving schema with `SHOW
  VIEWS IN <bare-schema>` (session-relative) rather than matching bare names
  against `SHOW DATABASES` — which now freezes a `DROP VIEW` for an unmanaged
  catalogue view in Fabric and the local emulator.

With generation in-session, prune reconciles catalogue views as well as storage,
so nothing about the Fabric path is a lesser case than local.

**A bundle binds destinations, not hosts.** `BoundTarget` retains the target
kind and the workspace, item and SQL endpoint identifiers an installer may need,
but no longer carries `host_kind`. Fabric is Weaver's real host; local execution
emulates it for development and testing. The resolver, store and Spark supplied
to generation or installation already define that runtime context, so serialising
`local` or `fabric` into the bundle was redundant and misleading. The
`LOCAL_HOST`/`FABRIC_HOST` bundle constants and installer validation based on
them were removed. Resolver-owned `schema_location` preserves the only behavior
that had depended on the field: the emulator emits an explicit schema
`LOCATION`, while Fabric lets the Lakehouse manage it.

---

### SQL-backed tables, and what only Fabric could tell us

Tables built from a query, both engines. A SQL-backed table's physical shape is
a deterministic property of its query, but only the target engine authoritatively
knows the query's output types — so inference is **deferred into one
self-contained install action** (build-philosophy §7.2/§7.3), never a plan-time
materialisation of ancestors and never a chatty round-trip. A Spark SQL table
freezes a `spark_table` JSON instruction the executor completes in one pass; a
T-SQL table freezes one self-contained script that shapes a `WHERE 1=0` temp
table, reads its metadata back and creates the table server-side. Declared or
inferred: a declaration controls the physical types and the query is analysed
only for case-exact column-set equivalence; without one, the query's own shape
is the schema. Only the authored main table is built — no view, no `_Current`,
no `_History` (history is a later load-time artefact). Nullability is a Weaver
contract from the SES header (primary key, `Not null`), not the query's own
nullability, so both engines agree; all three audit columns are physically not
null. The planner now targets **one physical side per bundle** — Lakehouse *or*
Warehouse — because crossing that boundary needs a SQL-endpoint refresh; the
alias-based loop is the next branch.

**A Warehouse reconciles too.** Prune is part of the build, so the Warehouse got
its own: the planner reads `sys.objects`/`sys.schemas` at *plan* time and freezes
one explicit T-SQL drop per unmanaged table, view and schema. Two differences
from the Lakehouse are worth holding onto. T-SQL has no `DROP SCHEMA … CASCADE`,
so the frozen order has to be dependency-safe by construction — views before the
tables they read, a schema only once it is empty — rather than relying on the
engine to cascade. And the catalogue reading is **Fabric-native by default**,
exactly as `wipe_sql_target` already was: Weaver runs in Fabric, so it inspects
the target through its own session identity (`fabric_sql_executor`), and only a
desktop caller crossing in injects `desktop_sql_executor` explicitly. The first
cut had this backwards — it *required* injection, which made the only usable path
a desktop-compiled bundle uploaded to Fabric. That inverts the architecture:
Weaver is installed in Fabric and does the work there, so reading target state
and compiling the bundle belong in the same place as the install. Reconciliation
still fails closed — off a Fabric session there is no identity to read the
catalogue with, so generation raises rather than emitting drops from an inventory
nobody could read (§6) — and `prune=False` remains the explicit opt-out.

**Identity is treated separately.** The `Identity` header names a Weaver-managed
surrogate — a plain not-null `bigint` build creates and a later load populates,
never engine-autogenerated. It is not a business column: not declared in Schema,
not produced by the query, but the primary key may name it. This is provisional
and will be revisited on its own.

The value of running it. Local generation tests check text, not SQL validity, so
the Warehouse T-SQL was qualified against a real Fabric Warehouse. Three failure
modes surfaced that **only server-side execution could show**, each now pinned
by a regression assertion:

1. an inferred identity table led its dynamic `all_columns` CTE with an unnamed
   `select 0, N'…'` — a CTE takes its column names from its first SELECT, so
   T-SQL refused it (`No column name was specified`). The injected identity
   SELECT must name its columns.
2. a primary key on the identity surrogate failed the in-SQL metadata check
   (`Primary key CustomerKey does not exist`): the surrogate is not a query
   column. The validation now unions the identity into its available set,
   mirroring the Python validator in `weaver.ses.columns`.
3. a Fabric Warehouse uses a **case-sensitive collation**, so `INFORMATION_SCHEMA`
   must be referenced in exact upper case — a test-helper bug, not a Weaver one;
   the generated SQL uses `sys.*`, which is really lowercase.

The test framework earned this cheaply. One reusable `BuildEnv` drives the same
transport-neutral body against local Spark and Fabric; each SES estate is
provisioned and installed **once per module** (module-scoped `lakehouse_estate`
/ `warehouse_estate`, one disposable Warehouse rather than one per test), so a
whole module of Fabric assertions costs one Lakehouse or Warehouse and one
install. All four build paths — existing Lakehouse, Spark SQL, mixed estate and
the Warehouse, reconciliation included — are green on real Fabric.

One property of the harness is worth recording because it nearly cost us a false
positive: the Warehouse catalogue helper originally allow-listed the schemas it
looked at, so the "the orphan schema is gone" assertion would have **passed
vacuously** whether or not prune ever dropped it. It now excludes system schemas
instead of naming user ones. A reconciliation test that cannot see the thing it
expects to be absent is not testing anything.

### Catalogue checkpoint 0 — the SES gaps the catalogue exposed

Before the central catalogue could project anything, the authoring model had to
be able to say what the catalogue records. Planning the projection found four
oversights and two rules that were broader than their reason.

**Logical keys were missing.** SES had `Primary key` and nothing else, so a
catalogue's key and relationship dictionaries would have been empty by
construction. Added: `Unique keys`, a YAML list of comma-separated column sets;
and `Foreign keys`, a list of `- Col A, Col B: Parent.Object[P A, P B]`.

Both are **semantic, not physical** — nearer an ER diagram than DDL. Nothing is
built and nothing is enforced. That decision is what removes their names: a
relationship is identified by its own columns and its parent, so several may run
between the same pair of objects and an object may reference itself (a hierarchy
in one table). A row in the catalogue *is* the edge.

**Views declare logical keys too.** A view stores no rows, so a key on one
describes the shape of its result. `Primary key`, `Unique keys` and `Foreign
keys` are now accepted on a view; everything implying storage — `Identity`,
`Not null`, `Incremental`, `Comparison columns` — is still refused.

**The Delta audit columns are snake case.** `row_insert_datetime`, not
`Row_insert_datetime`. The Warehouse keeps the spaced `Row insert datetime` the
SQL backend has always used, so the divergence stays exactly where it was
justified — spaces in Spark column names need quoting everywhere they appear.
Every spelling, retired ones included, stays reserved against a declaration.

**Two rules were over-broad, and the catalogue is what proved it.**

Weaver's own catalogue lives in schema `_`, declared as ordinary SES and built by
the ordinary build path — that recursion is the point of it (plan §4). Two rules
made it unauthorable:

1. *A root file beginning with `_` was support, never an object.* The underscore
   convention is about **directories** — `_schemas`, `_helpers` — and
   subdirectories were already excluded a line earlier. So the rule now demotes
   an underscored root file only when its stem does not name a schema and an
   object: `_scratch.py` is still private, `_.Registry.spark.sql` is an object.
   A file *without* an underscore is still judged on its suffix alone, so
   `Sales.Order.py` remains a reported error rather than a silent demotion —
   that distinction is the whole value of the guard.

2. *A Spark SQL object had to declare a non-empty `Dependencies`.* The rule's
   reason is that a Spark query may read by path, which cannot be resolved back
   to a managed object, so the graph is declared rather than discovered. What it
   actually wants is for the author to be **explicit**, and `Dependencies: []`
   is explicit. A catalogue table's body is literals and depends on nothing.
   An empty declaration also *suppresses* discovery, because a declaration has
   always replaced discovery rather than adding to it — otherwise
   `Dependencies: []` would quietly mean "discover them for me".

Note that Python cannot express schema `_` at all: the separator is `__`, so
`_.Registry` would be `___Registry`, whose first part is empty. The catalogue
tables are therefore Spark SQL, which is also what gives them a declared schema
and a body that returns no rows.

**A metadata reference is not a dependency.** `Description: $Sales.Order` means
*the text over there is the text here* — one sentence, written once, pointed at
from everywhere it applies. Resolving it is a copy, and `weaver.ses.references`
performs it: chains are followed to the literal at the end, and a cycle is an
error because it can never produce text.

Resolution deliberately does **not** work like dependency resolution. A
dependency binds in its consumer's execution namespace, because that is what the
SQL will bind to. A reference is a logical pointer, and the case it exists for is
the cross-target one — `tests/fixtures/sales-etl/Sales.Customer.sql` is a
Warehouse table whose `Lineage: $Sales.Customer` means the *Delta* table sharing
its ID. It cannot sensibly mean itself, so resolution excludes the referrer and
prefers its namespace only to break a tie.

An unresolved reference is **not** an error: it may legitimately name another
repository's object, and refusing it would cost someone a working object over a
documentation nicety. The pointer is recorded and the text is absent, which is
why the catalogue keeps `description` and `description_reference` side by side.

### Catalogue checkpoint 1 — one authority for the catalogue's shape

`weaver.catalogue` now owns the fixed representation of all ten tables and the
rendering of their DML. The point of concentrating it is that the schema is a
contract between four things that must agree — the built-in SES that materialises
the tables, the tolerant reader, the projection that fills them, and the SQL that
writes them. Four independent lists would drift, and drift here fails subtly
rather than loudly.

**Installation scope is in the key, not beside it.** Every table opens its key
with `repository` and `target_type`. That is not a convention; it is what makes a
partial-target build safe *by construction*. There is no way to name a row without
naming the installation it belongs to, so a comparison or a delete cannot span
both target types by omission. `CatalogueTable.__post_init__` refuses a definition
whose key does not open with the scope, and the renderer refuses rows from an
installation other than the one it was given.

**The one thing not frozen is the clock.** `current_timestamp()` is rendered as a
call rather than as an instant. A rendered time would change the payload — and so
the bundle id — on every run, destroying the identity that review and
certification rest on (§10). The engine supplies the moment; the payload stays
stable. Rows are sorted by key before rendering for the same reason: a mapping's
iteration order must not be able to change a bundle.

**Values are typed, and the reason is a Spark detail worth recording.** A `MERGE`
source is a `SELECT … UNION ALL SELECT …` whose schema comes from its first
branch, so an uncast `NULL` would type a column by accident and a later branch
could then fail to match the target. Every value is therefore cast to its declared
type. Strings escape the backslash as well as the quote, because Spark's default
parser treats a backslash as an escape.

**Two shapes fell out of the decisions rather than being designed.** A logical key
has no name — nothing physical is built, so a name would have to be invented — so
`index_type` and `column_set` are part of its identity. A relationship has no name
either, and *every* column of `ForeignKeyDictionary` is therefore key: the row is
the edge. Its only comparison column is the signature, which is right, because an
edge that points somewhere else is a different edge, not a changed one.

**A live row's delete datetime is a sentinel, not a null.** All three audit
columns are physically not null, so catalogue DML has to supply
`9999-12-31 23:59:59.999999`. `AUDIT_LIVE_DELETE_DATETIME` lives with the audit
columns in `weaver.ses.metadata` rather than in the catalogue, because it is the
row convention and load will need the same one. It is the SQL Server original's
choice, and its merit is that "as at" becomes one range predicate instead of a
null check. Worth revisiting deliberately when load lands rather than inheriting
by accident.

### Catalogue checkpoint 2 — Weaver builds its own catalogue

`src/weaver/builtin/catalogue/` is an SES repository shipped as package
resources. Setup materialises it into the Weaver Lakehouse and the *ordinary*
planner and installer build it — there is no catalogue-specific create, no
privileged executor, no special-cased schema. `tests/spark/` proves it end to end
on local Delta: ten Delta tables, every column and type matching the
representation, declared keys physically not null, and no rows, because build
creates structure.

**The text is committed, and a test regenerates it.** Generating the SES at
runtime would remove any chance of drift but also remove the reviewable
declaration, which is the more valuable half — a reader should be able to read
`_.Registry` as SES like any other object. So `render_sources()` produces the
canonical text from the table definitions and a test asserts the shipped
resources match byte for byte. Add a column to a definition and the suite fails
until the resource is regenerated.

**The tables describe themselves.** Each declares its catalogue key as its SES
`Primary key`, which is what makes those columns physically not null — so the
not-null guarantee the representation asserts is the one Delta actually enforces.
A test also checks that SES's *default* comparison columns equal the set the
renderer compares in its MERGE guard: two independent definitions of "what makes
a row different", which would let an unchanged row be written or a changed one
skipped if they diverged.

**`builtin/catalogue` is deliberately not a Python package.** An `__init__.py`
sitting in it would travel into the Weaver Lakehouse as a support file and into
that repository's signature. Resources are reached through
`importlib.resources.files("weaver.builtin") / "catalogue"`, which is what works
from an installed wheel rather than only from a source tree.

**A `$` in a description had to be escaped.** `description_reference` is described
as "the $Schema.Object the description was copied from" — which SES would parse as
a reference and refuse. The generator escapes it as `$$`, and a test asserts the
note comes back as prose. A small thing, but it is the authoring contract applying
to Weaver's own declarations, which is the point of them being ordinary.

**Two local-emulator divergences, recorded rather than designed around.** The
local Hive metastore lowercases both a registered table name and its managed
directory; Fabric preserves case. The assertions are therefore
case-insensitive — which is the behaviour Weaver relies on anyway, since the
reader already refuses two names differing only by case. And one Spark catalog is
shared by every test while each gets its own Lakehouse directory, so the fixture
drops schema `_` on the way out; a schema left registered makes the next test's
`CREATE SCHEMA IF NOT EXISTS` a no-op and sends its tables into this test's
`tmp_path`. That is the established convention here, not a new one.

**What this branch does not yet claim.** Build still emits `CREATE OR REPLACE
TABLE`, so re-running the bootstrap is idempotent in *shape* and destructive of
*rows*. Dropping only what changed needs the signatures the catalogue is being
built to hold, plus the drop policy that reads them — the next branch. Only setup
rebuilds schema `_`, so the exposure is a setup re-run, not an ordinary build.

### Catalogue checkpoint 3 — tolerant reading, and the projection boundary

**The reader's tolerance is deliberately asymmetric.** Two absences are ordinary
and read as data: a table that does not exist yet (the build that writes the
catalogue is the build that creates it) and a column an older Weaver never wrote.
An unexpected extra column is ignored, which is the mirror — a newer catalogue must
not break an older Weaver. But a permission error, a corrupt Delta log or a broken
session must **propagate**, because read as "no rows" it would tell the next build
that nothing is catalogued, and once drop policy lands that is a licence to remove
an estate. So absence is recognised only by Spark's own
`TABLE_OR_VIEW_NOT_FOUND` error class — not by message text, which a Spark upgrade
could reword into a catastrophe. Tolerance is cheap when it is specific and
dangerous when it is a bare `except`.

A present column is cast to its expected type as well as a missing one, which is
not obvious: an older catalogue may have stored a boolean as a string, and an
uncast comparison would rewrite an unchanged row on every build.

**The reader names no Spark API.** `tests/test_core_boundary.py` forbids the core
from even mentioning `pyspark` in source, lazily or not, and it caught the first
draft using `pyspark.sql.functions`. The projection is rendered as SQL text
instead, and the session is duck-typed — which is the existing convention in the
executors and has the side benefit that a tolerant read is inspectable as a
statement.

**Projection reads nothing physical.** Every value comes from the validated
declaration or the repository's own resolved graph. The consequence worth naming:
because `ColumnDictionary` is descriptive rather than a physical column list, an
*inferred* Spark SQL or T-SQL table projects completely at plan time even though
its columns are not known until install. That is what allows the whole catalogue
to be frozen into the bundle rather than half of it waiting on the engine — and it
is why keeping ordinals and types out of that table was the right call rather than
a simplification.

**A cross-engine alias is not a dependency, and the fixture proves it.** Where an
edge resolved through a `Lakehouse alias`, the dependency row records the *alias*
name — the name that binds in the consumer's own namespace — and `_.Alias` records
what it points at. Dependency rows are therefore same-namespace by construction,
and crossing engines requires the join. `tests/fixtures/catalogue-estate` carries
the awkward cases together: `Sales.Customer` as both a Delta and a Warehouse
table, an alias-resolved edge, a three-part physical reference, a
self-referencing relationship, an identity column, and a column note that points
at another object's note.

**Schema rows are projected only for schemas the installation uses.** A schema the
repository declares but this side never created would otherwise be claimed as part
of an installation that does not contain it.

### Catalogue checkpoints 4–6 — scoped DML, the build tail, and the bootstrap

**The statements do not depend on reading the catalogue.** This is the decision
worth recording, because the plan asked for the read and the read is still done —
just not for the reason it looked like. Reconciliation for one installation is a
scoped delete of everything the projection does not claim, plus an idempotent
merge of everything it does. That pair is correct against *any* prior state,
including a state the planner could not see. A build that derived its deletes from
an inventory would have its deletion scope widened by a failed read, which is
exactly what build-philosophy §6 exists to prevent; here nothing is derived from
the read, so a failed read cannot widen anything. The read produces the *summary*
a reviewer sees (§3, §17), and its absence degrades the report rather than the
correctness — which is why generation still works with no session at all.

**`_.Installation` has no obsolete row to delete, and finding that out was a
bug.** Its key *is* the installation scope, so the "keys beyond the scope"
predicate was a predicate over no columns and rendered `NOT ( () )`. Spark caught
it. The fix is not a special case so much as the honest consequence: there is at
most one such row per scope, so the merge alone keeps it current, and rendering a
delete would have removed the row about to be written. A second, smaller bug came
out of the same shape — catalogue actions were being named by position, so
Installation's only statement was labelled `delete`. Names now come from what a
statement *is*.

**The bundle names the Weaver Lakehouse as a second bound target.** Catalogue work
writes to the control plane, not to the destination, and a bundle must name every
physical destination it touches (§9). When the destination *is* the Weaver
Lakehouse — precisely the case when Weaver builds its own catalogue — the existing
binding is reused rather than duplicated. `BoundTarget` also gained `item_name`,
because on Fabric `item_id` is a GUID and `_.Installation.target_name` has to be
readable; it is a record, never identity.

**A Warehouse build now writes Spark SQL.** There is one central catalogue and it
lives in a Lakehouse, so a Warehouse build carries Spark SQL catalogue actions
against the Weaver Lakehouse. Three existing tests asserted a Warehouse plan was
all-T-SQL; they now scope that claim to the *physical* actions, which is what they
always meant. Accepted consequence of a central catalogue rather than a wart.

**Sequence numbers 9000/9010/9020, with a guard.** The SQL Server system this
ports from ran past thirty dependency layers, so the tail sits far above them —
about 896 layers of headroom — and `check_sequence_headroom` fails generation
rather than letting a deep repository silently reorder its own catalogue.

**One test needed sharpening in a way worth noting.** "A Warehouse build's
statements never say `lakehouse`" is false, and correctly so: an `_.Alias` row
records `lakehouse` in `alias_target_type`, because a Warehouse object's Lakehouse
alias is a fact about its declaration. Scope is now asserted on the *predicate*,
not on the presence of a word — a value and a claim about which rows are touched
are different things.

**The bootstrap is one bundle.** Setup materialises the built-in repository and
builds it normally; the barriers already order it — schema, then the ten tables,
then the catalogue's own DML writing into the tables that same bundle just
created. No first-run mode, and the only thing making it possible is the tolerant
reader, since planning reads a catalogue that does not exist yet. Setup never
prunes: the Weaver Lakehouse belongs to the installation, not to this repository,
so a reconciling build would treat a user's own schema there as an orphan. There
is a test that a user's schema survives.

**No `weaver setup` CLI command, and no local session factory needed.** The obvious
adapter would construct a Spark session, and `tests/test_core_boundary.py` forbids
the CLI from naming PySpark — `[cli]` does not install it. That boundary is correct
and stays. The CLI, when it lands, will *submit* the operation to Fabric rather than
run Spark locally:

```text
local CLI → Fabric submission → remote session attached to the Weaver Lakehouse
          → initialise_weaver_lakehouse(...)
```

So the callable function is the whole of what this branch needs.
`weaver.initialise_weaver_lakehouse` takes the session it is given.

### Two things only running it on Delta could find

**A merge source cannot be a chain of unioned selects.** The obvious construction —
one `SELECT` of cast literals per row, joined by `UNION ALL` — broke the bootstrap
with Janino's `Code grows beyond 64 KB`. Spark generates Java for the plan and a
method's bytecode is capped, so a union of ~90 projections exceeds it. The
catalogue's own `_.ColumnDictionary` has a row per column of every catalogue table,
which was enough. It is now one `VALUES` relation — a single plan node however many
rows it carries — with the casts moved outward into one enclosing projection. The
values are bare literals; `VALUES` unifies a column's type across rows (all-null
becomes void) and the enclosing `CAST` settles it either way, which is what keeps
the source's schema exactly the target's. Smaller text, too.

**Row counts must not go in a sequence description.** They briefly did, so a
reviewer could see the effect of a bundle before running it. But a description is
part of the hashed plan, so two runs of the *same* repository produced different
bundle identities purely because the catalogue's state had changed — breaking the
property that lets a reviewer compare environments and certify an artefact (§10).
Counting rows is a report about state, not part of a frozen contract.

Generation therefore no longer reads the catalogue at all, which is the honest end
of the reasoning that started with "the statements do not depend on the read". The
tolerant reader and `summarise` are still the API for asking what a build would
change, and they are what the drop policy will run its signature comparison
through — but nothing in the frozen plan depends on them, so nothing in the plan
can vary with the state of the thing being written.

Both are the kind of defect local Spark exists to catch: neither is visible in
generated text, and both would have surfaced first in a workspace.

### The execution model: the session is attached to Weaver

This was recorded as an open question — whether the catalogue should be addressed
by explicit path — and the question was based on a wrong model. Matthias settled
it, and it is worth writing down properly because everything about addressing
follows from it.

**The Spark session is attached to the Weaver Lakehouse.** That is the fixed
control-plane context, not an accident of whatever a notebook happened to open. So
`_.Registry`, `_.Installation` and the rest being reached as ordinary two-part
names in schema `_` is the *defined* execution context — not the ambient-catalogue
anti-pattern of build-philosophy §16, which is about a destination resolving
through whatever context happens to be current.

Destination Lakehouses are the **variable data plane**. They are reached through
roots resolved from their target bindings, and a build never switches the session's
current catalogue to reach one:

```text
Spark session
└── attached: Weaver            control plane, fixed
    ├── _.Installation
    ├── _.Registry
    └── …
Build targets                   data plane, resolved explicitly
├── Lakehouse A → tables_root, files_root
├── Lakehouse B → tables_root, files_root
└── Lakehouse C → tables_root, files_root
```

That separation is what makes one invocation building several Lakehouses possible.
Switching the current catalogue between targets would make `Sales` in Lakehouse A
and `Sales` in Lakehouse B indistinguishable; resolved roots keep them apart by
construction.

`LakehouseSparkLocation` is that resolution, provided by the host adapter —
`resolver.lakehouse_spark_location(item)` — with `table_path()` and
`folder_path()` on it. The responsibilities stay separated: `ItemRef` identifies
the logical item, the host resolves the physical roots, the plan carries the item,
the installation context resolves it once per target, and the executor uses it. An
executor deriving its own path would be re-deciding where an action lands, which is
a planning decision it is not allowed to make.

**One distinction nearly got conflated, and it matters.** On Fabric a Lakehouse has
*two* addresses: the DFS location the store lists through, and the `abfss://` root
Spark reads and writes through. `LakehouseSparkLocation` carries the second. Prune
inspection *lists* a target, so it keeps using the store's DFS location — a first
attempt at routing everything through one type would have had inspection trying to
enumerate a URL Spark cannot walk.

**A resolved root is deliberately not in the bundle.** On Fabric it embeds
workspace and item ids; locally it embeds a temporary directory. A bundle whose
identity moved with a temporary path would not be comparable between environments
(§10), and one carrying a stale root would install somewhere the caller no longer
means. The bundle names the item; the installer resolves it.

**The local `_` fixture churn is not evidence about production.** The local fixture
runs one long-lived session while presenting a succession of temporary directories
as though each were *the* Weaver Lakehouse, so `CREATE SCHEMA IF NOT EXISTS _` is a
no-op after the first and the next test's tables would land in the previous test's
directory. That is a session-isolation problem in the harness, which the catalogue
suites handle by dropping `_`. Production has one Weaver Lakehouse attached for the
life of the session and reaches destinations through resolved roots. The
architecture does not follow the lifecycle of a test fixture.

### What the settled model exposes: destination DDL is still two-part

Fixing the *catalogue's* addressing settles the catalogue. It also makes visible
that **destination** addressing has the problem the catalogue was suspected of.

Today a destination table is built as a two-part name:

```sql
CREATE SCHEMA IF NOT EXISTS `Sales` LOCATION '<destination>/Tables/Sales'   -- local
CREATE SCHEMA IF NOT EXISTS `Sales`                                        -- Fabric
CREATE OR REPLACE TABLE Sales.Customer (…) USING delta
```

A two-part name resolves through the session's current catalogue, and the session
is attached to **Weaver**. So:

- **Locally** it works, but only for one destination. `LocalResolver.schema_location`
  pins `Sales` to that destination's `Tables` area with an explicit `LOCATION`, and
  `IF NOT EXISTS` means the *first* Lakehouse to register `Sales` wins. Two
  destinations sharing a schema name would silently share a location.
- **On Fabric** `schema_location` returns None by design — a schema-enabled
  Lakehouse pins managed tables itself — so `CREATE SCHEMA IF NOT EXISTS Sales`
  creates `Sales` in the *attached* Lakehouse, and the table lands in Weaver rather
  than in the destination.

This is the work `LakehouseSparkLocation` exists for and does not yet do: the build
DDL has to address a destination by its resolved root, not by a bare two-part name.
It touches the schema sequence and every Lakehouse executor, so it is its own piece
of work rather than an addition to this one.

**Why the Fabric build tests did not catch it.** They generate *and* install in one
session and then verify through `spark.table("DWG.Customer")` — the same session
catalogue the write went through. A table written to the wrong Lakehouse is then
read back from the wrong Lakehouse, and the assertion passes. This is the same shape
as the prune helper that once allow-listed the schemas it inspected: an assertion
that cannot see the thing it claims about is not testing it. The fix is to assert on
the destination's *resolved path* — `location.table_path(schema, name)` — as well as
on the name.

### The Spark suite no longer fits one process here

The catalogue roughly doubled the Spark suite, from 41 tests to ~93. Every file
passes on its own — built-in build 9/9, tolerant reader 14/14, reconciliation 19/19,
bootstrap 18/18, and the physical build set 26/26. The *combined* run does not: at
around stage 7000 the shared JVM starts failing with `Py4JJavaError: <exception
str() failed>`, and whatever runs last collects the failures. In one run that was
three `test_catalogue_setup` assertions; in the next it was
`test_local_lifecycle`, `test_local_persisted_view` and
`test_diagnostics::test_the_report_agrees_with_a_session_actually_starting` — a test
that only starts a session and cannot be affected by catalogue code. All seven of
those pass in 39 seconds when run alone.

This is **a test-harness limitation**, recorded as such: cumulative degradation of
one shared JVM and its session state. The catalogue increased the total Spark
workload enough to expose it; that does not make the catalogue the cause, and the
catalogue design must not be distorted to work around it. Process isolation, or a
session per test group, can be considered on its own.

Costs were cut where it was free to do so — the reconcile fixture creates its tables
directly instead of building a bundle per test, and the bootstrap is shared by every
read-only assertion, taking that file from 5:13 to 1:39 — so the run completes in
~17 minutes rather than timing out. That is mitigation, not a fix.

### It was not the harness: Delta was keeping every Lakehouse the tests threw away

The entry above recorded the combined `-m spark` failure as a harness limitation —
cumulative degradation of one shared JVM — and proposed process isolation. That
was a guess, and it was wrong in an instructive way.

Measure the heap *after a forced collection* and the guess collapses. A reading
taken without one shows garbage, which proves nothing; taken after one it shows
retention, and the live set climbs about 5.6 MB per test and never comes down.
Under the default 1 GB driver heap the run reaches the ceiling around test 50,
after which everything is GC thrash until the JVM gives out — which is why the
failure attached itself to whichever test happened to be running, including one
that only starts a session.

A class histogram named it. The heap is Catalyst expression trees of exactly the
shape an `ExpressionEncoder` builds — `Invoke`, `GetExternalRowField`,
`ValidateExternalType`, `AssertNotNull` — plus the bytecode generated for them.
Those belong to Delta: `DeltaLog` caches one instance per table **path**, each
holding a `Snapshot` whose state is a `Dataset`, and therefore a whole query
execution and its encoder. Every test builds under a fresh `tmp_path`, so every
table is a new path, and the cache keeps the snapshot of each one alive long
after the directory it describes has been deleted.

So the tests were isolated and the session's memory of them was not. The harness
now clears Delta's log cache and Spark's plan cache after each test. Both are
caches; the cost is re-reading a transaction log and no answer changes.

| | result | wall clock | live heap |
|---|---|---|---|
| before | 2 failed, 6 errors | 5:04 | pinned at the 1 GB ceiling |
| after | 92 passed | 3:42 | 68–219 MB |

Two things were tried and are *not* in the fix, which is worth recording so they
are not tried again. Turning Spark's status retention down to the minimum
(`spark.sql.ui.retainedExecutions` and friends, whose defaults are sized for a
long-lived cluster with someone watching it) looked like the answer because live
heap tracked retained executions almost exactly — and made no measurable
difference, because the correlation was with work done, not with what the
listeners kept. Raising the heap was never the fix; it moves the ceiling.

### What Fabric actually does with a four-part name

Everything below rests on Fabric's namespace, so it was asked rather than
recalled. One Livy session, attached to `Play_Lakehouse_1`, driving both
Lakehouses:

```text
current_catalog()   spark_catalog
current_database()  chimcobldhq2alr5c5r6ash5a1m62uav9hgmmpb8dtqn6pav64im8ojf
SHOW SCHEMAS        Weaver.Play_Lakehouse_1.TestSchema
                    Weaver.Play_Lakehouse_1.dbo
```

A Fabric schema is a **three-level name under `spark_catalog`**, so an object is
four parts. Against a Lakehouse the session was *not* attached to, all of these
work: `CREATE SCHEMA`, `CREATE OR REPLACE TABLE`, `INSERT`, `SELECT`,
`CREATE OR REPLACE VIEW` (including a view in one Lakehouse over a table in
another), `MERGE`, `DELETE`, `DROP TABLE`/`VIEW`/`SCHEMA … CASCADE`,
`SHOW TABLES IN`, `SHOW VIEWS IN`, `DESCRIBE`, and
`spark.catalog.tableExists`/`databaseExists`. Nothing needs attaching and nothing
needs switching.

Three findings shaped the design rather than merely confirming it:

- **`SHOW SCHEMAS IN `ws`.`lh`` is refused.** It encodes the pair and looks it up
  as a schema. A bare `SHOW SCHEMAS` answers for the attached Lakehouse only. So
  there is *no* SQL way to enumerate another Lakehouse's schemas, and schema
  discovery must read storage — which prune already did, and which the journal
  had already defended for a different reason.
- **`SHOW TABLES` returns views too.** Tables are `SHOW TABLES` minus
  `SHOW VIEWS`.
- **The anticipated failure is real and exact.** An unqualified
  `CREATE TABLE WvProbe2.Stray` landed in `Weaver.Play_Lakehouse_1.WvProbe2` — the
  attached Lakehouse — with no error.

`spark.catalog.listDatabases()` and `listTables(...)` are broken in Fabric (they
re-encode an already-qualified name and fail), so nothing uses them.

### Local Spark cannot be given a catalogue per Lakehouse

The obvious local proxy is one Spark catalogue per Lakehouse:
`spark.sql.catalog.Sales_LH`. It does not work, and it fails deep rather than
cleanly. Delta's `DeltaCatalog` extends `DelegatingCatalogExtension`, whose
delegate is only ever set for `spark_catalog`; registered as an ordinary named
catalogue its delegate is null, and every statement dies inside the analyzer with
`INTERNAL_ERROR` or a bare `NullPointerException`. Tried, and recorded so it is
not tried again.

Local Spark therefore has exactly one namespace level, and the proxy folds the
Lakehouse into it:

```text
Fabric   `Weaver`.`Play_Lakehouse_1`.`Sales`.`Customer`
local    `sales_lh__sales`.`Customer`
```

That is not Fabric syntax and is not meant to be. What it reproduces is the one
property Fabric's namespace provides and a bare `Sales.Customer` does not: two
destinations declaring a schema of the same name stay apart. The fold is in the
*name* only — the local database still carries an explicit `LOCATION` of
`<lakehouse>/Tables/<schema>`, so a managed table lands exactly where the Fabric
layout puts it and the emulator keeps mirroring OneLake. Local simplification does
not reach the Fabric model.

### The payload names the object; the installer says which Lakehouse

The open question this branch inherited was how destination DDL should be
addressed. Two answers were available and both are wrong.

Freezing the qualified name into the payload — `CREATE TABLE
`Weaver`.`Play_LH`.`Sales`.`Customer`` — loses §10. Two bundles of one repository
generated against dev and prod would then differ in *every payload*, and
comparison between environments is one of the four things canonical hashing
exists for. Keeping the bare two-part name loses §9 and §16, which is what was
already happening.

So the payload names the object logically and the executor resolves it against
the batch's target:

```sql
CREATE OR REPLACE VIEW {{object:DWG.ActiveCustomer}} AS
SELECT * FROM {{object:DWG.Customer}} WHERE IsActive
```

This is the substitution §16 permits — "strictly transport-level values whose
meaning was already bound and validated" — and not the template it forbids. The
object, its schema, the statement, and which item the batch targets are all fixed
before the bundle is written. What is supplied late is only how that
already-chosen destination spells a name, which is the one thing generation
cannot know without tying the bundle to the environment it ran in. A reviewer
reading the payload sees the object; the manifest's target block says where it
goes. A bare two-part name said neither.

View bodies are rewritten in place, so a view built in one destination reads its
inputs from the same one. Only ordinary two-part references are touched, which is
exactly the set the reader guarantees resolves inside the repository — anything
left is a physically-qualified name or a table-valued function, both of them the
author naming something Weaver does not manage. Nothing else about the text moves:
same whitespace, same comments, same casing, same delimiters.

### Freezing a schema create was freezing a temporary directory

Following the addressing through turned up a §10 violation that had been sitting
in every local bundle. `CREATE SCHEMA` needs a `LOCATION` on local Spark, the
planner asked the resolver for one, and the resolved path went into the payload:

```sql
CREATE SCHEMA IF NOT EXISTS `Sales` LOCATION '/var/folders/…/pytest-42/Sales_LH/Tables/Sales'
```

A temporary directory, inside the hashed plan, deciding where a managed table
lands. On Fabric the same code froze the opposite mistake — no clause and a bare
two-part name, so the schema was created in the attached Lakehouse.

The action now names the schema and nothing else, and the destination decides how
to make one. That is not the installer filling in a semantic decision: which
schema, in which Lakehouse, is settled and in the manifest. It is the same
transport-level resolution every other action gets, applied to a clause that is
purely about storage.

### The Fabric build tests were structurally unable to fail

The previous entry predicted this and it held. The fixture attached its Livy
session to the **target** Lakehouse, so a two-part name happened to land in the
right place; the write and the read then went through one session catalogue, and
a table written to the wrong Lakehouse would have been read back from the wrong
Lakehouse with the assertion passing.

The fixture now attaches to the **Weaver** Lakehouse, which is the production
model, and both Lakehouses are schema-enabled — the Weaver one because the
catalogue lives in a schema called `_` and a Lakehouse without schemas cannot
hold one. Every assertion names the Lakehouse it is about, and the build-bundle
test checks the resolved *path* as well as the name.

Two tests make the whole claim directly. One session builds the destination,
writes the catalogue into the Weaver Lakehouse, reads `_.Registry` and
`_.Installation` back through their fully-qualified names, and checks schema `_`
is *not* in the destination. Locally, two Lakehouses each declaring `DWG` get two
separate tables — a row written to one is not in the other, which is precisely
what `CREATE SCHEMA IF NOT EXISTS DWG` could not deliver when the first Lakehouse
to register the name won.

Both pass on Fabric. The catalogue read there is a four-part
`` `Weaver`.`weavertest_weaver_…`.`_`.`Registry` `` against a Lakehouse the
session is attached to, while the objects it describes are in one it is not.

One assertion had to be written differently than the previous entry proposed. It
said the fix was to assert on the destination's resolved path, and a path is too
precise: Fabric lowercases a managed table's directory (`Tables/DWG/customer`),
exactly as the local metastore does. The physical name is the host's to choose, so
the assertion lists the schema directory and matches case-insensitively — which
still proves the bytes are in the destination's own storage, and which is how
Weaver treats identity anyway.

### The Fabric suite is now long enough to be refused a session

Worth recording because it is not a code failure and will be misread as one. A
full `-m fabric` run is 39 minutes and starts a Livy session per function-scoped
build environment plus one per module-scoped estate. At the tail of one such run,
the two estate modules failed identically:

```text
LivyError: Livy session did not reach 'idle' within 600s
```

Seven errors, all of them a session that never started. Run on their own the same
seven tests pass in four minutes. So the capacity was saturated, not the code —
and the count of sessions has not changed, only their length, because generation
and installation both happen in them and there is now more of both.

**Done, and it took two attempts to find the right shape.**

Sharing the session alone was not enough. The Fabric build context was creating
and deleting its *own* Weaver Lakehouse and its own target per test; pointing it
at the run's session left the Lakehouse churn in place, and Fabric's namespace
resolver then intermittently reported `Artifact not found` for a target that
demonstrably existed. That was checked directly rather than assumed: a Lakehouse
created *after* a session has started is addressable straight away, so lateness is
not the problem — churning artifacts underneath a long-lived session is.

So nothing is re-provisioned. One Weaver Lakehouse, one target Lakehouse and one
session for the whole run, all created before the session starts; a build context
*empties* the target on its way in, which is what a freshly created one used to
provide and what the local context has always done. It also models the
architecture rather than contradicting it — a real installation does not get a new
Lakehouse per build.

One consequence had to be fixed rather than tolerated. Two modules install
different fixtures under one repository name, and with a shared Weaver Lakehouse a
file-by-file upload *merged* them: the Warehouse estate inherited a
Lakehouse-reading table from a repository it had never heard of. Installing a
repository now replaces it, which is what the words already meant.

| | result | wall clock | sessions |
|---|---|---|---|
| before | 41 passed, 7 errors | 39:16 | ~9 |
| after | **48 passed** | **10:06** | 1 |

`close()` also waits for the session to report itself gone. `DELETE` returns when
the request is accepted, not when the capacity slot is free, so a caller that
closed and immediately opened another was asking for a second session while the
first still held the only one.

### Building the mixed estate into a real workspace, and what it cost

The multi-target work was exercised on a workspace that is not disposable —
`Play_Lakehouse_1` as the Weaver Lakehouse, `Play_Lakehouse_2` as the destination,
`Play Warehouse` as the Warehouse — with prune on, which is how three things came
out that a disposable fixture could not have shown.

**Weaver's Warehouse prune deleted nine schemas it does not own.** Every Fabric
Warehouse carries a schema per fixed database role (`db_owner`, `db_datareader`
and seven more) and the prune treated each as an orphan. On SQL Server
`drop schema [db_owner]` fails; on Fabric it *succeeds*. All nine went. The roles
survive, so `create schema [db_owner] authorization [db_owner];` puts them back,
and it did. They are now excluded by ownership — asked of the server through
`sys.database_principals.is_fixed_role` — rather than by adding nine names to a
reserved list, because the reserved list says what Weaver declines to manage and
this says what the database owns on its own behalf.

This is the sharpest argument yet for the rule that a build is planned against a
*read* target: the drops were visible in the bundle before anything ran, and were
read only after the fact.

**A Lakehouse table's physical name is lower-cased and the Warehouse is
case-sensitive.** Spark creates `Sales.Customer`; Fabric stores
`Tables/Sales/customer`; the SQL endpoint exposes `Sales.customer`; and a Fabric
Warehouse, whose collation is case-sensitive, cannot see `[Sales].[Customer]` at
all. The failure is `Invalid object name`, which is exactly what an unsynced
endpoint reports — so the obvious diagnosis is the wrong one, and five minutes
were spent waiting for a sync that had already happened. Weaver passes a
three-part name through untouched by design, so matching the physical spelling is
the author's job; the trap is worth knowing rather than working around.

**A Lakehouse SQL endpoint exposes tables, not Spark views.** `Sales.ActiveCustomer`
is a Spark-catalogue object, queryable from Spark in any Lakehouse, and simply
absent from the endpoint. A Warehouse object cannot read one.

What worked, first time and unremarkably, is the part this branch was about. One
repository installed into two physical sides, one session, three Lakehouse-scoped
destinations in play, and the catalogue in the Weaver Lakehouse recording both:

```text
_.Installation      MixedEstate  lakehouse  -> Play_Lakehouse_2
                    MixedEstate  warehouse  -> Play Warehouse
                    _weaver      lakehouse  -> Play_Lakehouse_1
```

Read back, as ever, through ```Weaver`.`Play_Lakehouse_1`.`_`.`Registry```.

### The next architecture: repositories own items, and items own documents

The catalogue and multi-target work made the remaining false abstraction plain.
The repository is still flat and each document still chooses one of three target
kinds, while the thing being declared is a Fabric workspace containing several
Lakehouses and Warehouses. The accepted next architecture makes that containment
the source of identity:

```text
Weaver repository
└── Weaver item
    └── Weaver document
```

One Weaver Lakehouse is one control plane and holds one source-controlled Weaver
repository. Weaver's catalogue is its built-in `Lakehouse/_weaver` item, not a
second repository. Another developer may run another Weaver Lakehouse in the same
workspace, install the same repository, and choose different physical bindings.

**Binding is exact-item and deliberately sparse.** A build needs at least one
binding and usually leaves most items unbound. A consumer in a bound item is not
dropped merely because a producer item is unbound: the producer is assumed static
for this build and the catalogue records the dependency exactly as the consumer
declared it. A declared-schema Python table can therefore build its empty
structure and discover a missing producer only at load; an action that needs the
producer during build still fails honestly at the engine.

**The repository layout carries ownership.** `Lakehouse/<name>` and
`Warehouse/<name>` are logical items. A Lakehouse item owns both its Tables
documents and its `Files/` Folder documents; `schemas/` is item-owned and `lib/`
holds helper code. Folder stops being a target. A Delta and Folder document of one
`Schema.Object` may coexist because `Files/Schema.Object` is a distinct logical
namespace.

`_ignore/` is the only ignored authored directory. It and its complete subtree do
not travel, do not contribute to the repository signature and are not discovered.
Every other underscore path goes through ordinary parsing and validation; there is
no general underscore reservation.

**Logical identity is exact-case.** Item, schema, object and logical reference
spellings must match exactly. Case-only duplicate declarations remain invalid
because a physical engine may collapse them even though Weaver can tell them
apart. The repository name is its directory name.

**Aliases move to `alias.yml`, destination first.** A declaration says which
consumer-facing canonical name is supplied by which canonical source:

```yaml
aliases:
  Warehouse/Reporting/Sales.Customer: Lakehouse/Curated/Sales.Customer
```

One destination has one source; one source may serve several destinations; any
item types may be connected. The declaration participates in exact-case
validation, graph resolution and catalogue projection. `_.Alias` reproduces the
file. `_.Dependency` belongs to the consumer item, preserves two-part names as
two-part and physical three-/four-part names as authored, and renames
`is_within_repository` to `is_within_item`.

Only the logical half lands in this re-architecture. A physical three-part name is
already supported and may coordinate items when it happens to match their bound
names — the low-ceremony, environment-locked path. Physical alias behaviour is a
later branch. If retained work actually uses an alias, planning fails before
mutation with `NotImplementedError: Alias usage is not yet supported`; it never
lets the two-part name bind to the wrong destination. Python imports may still
resolve statically through an alias destination, since their declared-schema build
does not use the producer until load.

**Build, prune, wipe and rebind keep their distinct meanings.** A multi-item build
is one coordinated unit with one catalogue tail after all retained physical work.
Prune remains frozen build reconciliation and removes target structure not declared
by the bound item. Two logical items may be pointed at the same physical item, but
with prune on they will consume one another's structure; Weaver need not add a
special prohibition, and `prune=False` is the explicit unsafe escape hatch. Wipe
still wipes the named physical item. Rebinding updates the current Installation
row and leaves the old physical item behind until someone names it for a wipe.

Catalogue evolution remains intentionally destructive while the system is this
young. It is rebuilt from its repository; preserving history becomes important
when incremental build begins to depend on it. Schema declarations remain a
separate document type, ready to carry item-specific security policy later.

The accepted target is now in
[`backlog/weaver-architecture-summary.md`](../backlog/weaver-architecture-summary.md),
and implementation is divided into R0–R9 in
[`backlog/weaver-repositories-items-documents-checkpoints.md`](../backlog/weaver-repositories-items-documents-checkpoints.md).
R0 records the architecture only; no implementation changed with it.

### R1 — logical identity without physical binding

The first implementation seam is deliberately pure Python. `WeaverItemId`,
`WeaverSchemaId` and `WeaverDocumentId` now carry the exact-case canonical
grammar, including the separate Lakehouse `Files/` namespace. `WeaverItem` and
`WeaverRepository` enforce exact duplicates, reject case-only collisions and do
exact lookup. They contain no host or target value, so changing a physical
`DeltaTarget` cannot change logical equality.

The validated metadata declaration is canonically named `WeaverDocument`.
`SesDocument` remains an alias until R8 migrates the compatibility surface; it
is not a second model. The flat `SesRepository` similarly remains the old
execution model while discovery and planning move checkpoint by checkpoint.

R1 is covered by pure unit tests only. The full lightweight suite finished with
1,082 passing, one skipped and 146 Spark tests deselected.

### R2 — static item-owned discovery

`read_weaver_repository()` is the canonical item-layout reader. It traverses a
`Store`, takes the repository name from the root directory, discovers exact
`Lakehouse/<name>` and `Warehouse/<name>` items, and assigns each schema and
source document an item-qualified identity. Lakehouse `Files/` documents must be
Folders, Lakehouse root documents must be Delta/Spark materialisations, and
Warehouse root documents must be T-SQL materialisations. Every object's schema
must be declared by that same item.

Discovery parses Python through AST and never imports it. `lib/` is snapshotted
as support, while a user-authored `__init__.py` is rejected. `_ignore/` is the
only invisible directory: even invalid Python beneath it is absent from parsing,
installation input and the repository signature. Every other underscore path is
ordinary input and therefore validated or refused according to where it sits.

The end-to-end `Estate` test is entirely pure Python and contains two
Lakehouses, two Warehouses, independently owned `Sales` schemas, a same-name
Delta/Folder pair, a helper module and parked invalid content. After R2 the full
lightweight suite finished with 1,089 passing, one skipped and 146 Spark tests
deselected.

### R3 — one exact logical-reference grammar and `alias.yml`

Metadata pointers now preserve short item-relative forms and canonical
item-qualified forms, including `Files/` and `[Column]`. The item reader eagerly
follows description, lineage and column-note chains and validates foreign-key
targets. Missing names, casing mismatches and cycles are repository errors. The
flat reader retains its old tolerant resolver only as part of the R8 transition.

Repository-level `alias.yml` is parsed destination first into immutable
`RepositoryAlias` values. Both sides must be canonical logical document
identities; the source must exist exactly, the destination item and schema must
exist, and a destination cannot collide with native content. Several
destinations may name one source. Metadata may resolve through an alias
destination, but no physical alias work is generated. The new item reader rejects
the retired document-local alias headers.

All parsing, chains, cycles, casing failures, collisions and one-to-many source
tests are pure Python. After R3 the lightweight suite finished with 1,101
passing, one skipped and 146 Spark tests deselected.

### R4 — item-owned dependencies, graph and sparse projection

The static Python parse now retains relative import level and module path. From
an item root, `.Files.Schema__Object` resolves to a Folder; from `Files/`,
`..Schema__Object` resolves to a table-style document and
`.Schema__Object` to another Folder. Imports below the item's `lib/` are helper
imports and produce no graph edge. Absolute object-shaped imports remain a
low-friction item-root spelling during the transition.

SQL two-part names resolve exactly in the consumer item's table namespace or
through an alias destination. Authored three- and four-part names remain
unchanged physical declarations with no invented logical producer. Each
`ItemDependency` records the consumer, the spelling the consumer wrote, the
resolved producer when logical, resolution provenance and `is_within_item`.

The repository now carries one global exact-case DAG. Cross-item cycles fail at
read time. Sparse projection accepts exact item identities and selects only
documents those items own; a producer in an unbound item remains a graph ancestor
but is not silently added to physical work or used to discard its bound consumer.

The focused fixture proves Delta-to-Folder, Folder-to-Delta,
Folder-to-Folder, lib exclusion, cross-item alias resolution, physical-name
preservation, an unbound producer and a cross-item cycle. After R4 the pure
suite finished with 1,108 passing, one skipped and 146 Spark tests deselected.

### R5 — the multi-item manifest seam

The existing immutable manifest and mechanical installer are being retained.
`ItemBinding` now binds one exact `WeaverItemId` to a typed physical Lakehouse or
Warehouse, and the serialised `BoundTarget` carries both identities. Manifest
target ids include the logical item, so two logical items may deliberately name
one physical item without colliding inside the bundle.

`generate_item_build_bundle()` freezes any non-empty sparse set of bindings into
one plan. Schema and dependency-layer barriers may contain several item-specific,
target-bound batches. Action ids and payload paths include item identity, the
repository snapshot is certified, and loading/installing the result performs no
source interpretation. Authored physical SQL remains byte-preserved. If any
retained consumer resolves through `alias.yml`, generation stops before writing
with `NotImplementedError: Alias usage is not yet supported`.

At its first seam the planner refused `prune=True` rather than reuse an unsafe
target-kind scope. R6 then supplied the item-scoped catalogue tail and R7 supplied
item-owned physical reconciliation, completing the coordinated manifest without
changing its immutable plan/installer boundary. The original multi-binding,
deterministic identity, alias preflight, physical-name and repository-independent
install tests remain pure Python. At the initial seam the lightweight suite
finished with 1,117 passing, one skipped and 146 Spark tests deselected.

### R6 — item-scoped catalogue and built-in `_weaver`

The item path now has its own authoritative catalogue representation. All ten
tables carry `(repository, item_type, item_name)` scope, and object tables add an
explicit `object_namespace` so `Tables/Sales.Order` and `Files/Sales.Order` do not
collide. Dependency rows belong to the consumer and retain the exact authored
name plus `is_within_item`; Alias rows reproduce the canonical destination/source
pair from `alias.yml`. The legacy renderer was generalised around a scope value,
so its existing target-kind output remains byte-compatible while the item path
uses the stricter identity.

`read_weaver_repository()` injects `Lakehouse/_weaver` from generated schema and
source bytes. Those bytes are parsed by the ordinary readers, included in the
repository signature and certified snapshot, and an authored item of that name is
rejected. Binding the built-in item to the control Lakehouse therefore emits ten
ordinary table actions followed by item-scoped dictionary, Installation and
Registry barriers. Registry remains last. Rebinding changes only the physical
`target_name` attribute of that logical item's Installation row; scoped delete and
merge cannot reach another item of the same type.

The test pyramid is deliberate. Pure tests cover definitions, projection,
deterministic DML, scope isolation, rebinding, aliases, dependencies, planner
barriers and bootstrap shape. The full core suite passed with 1,129 tests and one
skip. A focused real local install built and populated the catalogue, and the
complete runnable Spark suite passed 99 tests in 4m14s. Finally, Weaver
`0.1.1.dev11157825816` was published to the Fabric `weaver` Environment; the
installed wheel discovered, generated, installed and independently read the same
built-in item inside an Environment-backed Livy session. That row-3 proof passed
in 6m13s.

Two endgame observations are recorded in R9 rather than widening R6. Fabric's
managed Delta directory is host-lowercased, but the catalogue table's display name
should still preserve the PascalCase definition and needs an explicit proof. Also,
the Livy sessions collection endpoint exposes scheduler/plugin/Livy states,
timestamps and cancellation reasons. The Fabric harness should report those
before requesting a capacity's single Spark slot, so a queued session is visible
rather than looking like a silent start.

### R7 — one Lakehouse item owns Tables, Files, prune and wipe

The item planner now ports the proven fail-closed target inventories rather than
inventing another reconciliation path. For each bound item it derives a keep-set
from exactly that item's retained documents, inspects the named physical target at
generation time and freezes every drop into the bundle. Several logical items
produce independent batches and namespaced action/payload identities even when
their schemas and object names repeat. Warehouse items use either the
Fabric-native connector from the host or an item-keyed injected executor. A
Lakehouse item uses one binding and one keep-set for Delta tables, Spark views and
Files folders together.

Prune is on by default. `prune=False` emits no destructive action and remains the
explicit unsafe escape hatch for someone deliberately jamming declarations into a
shared physical item. Rebinding only changes which physical item the current plan
inspects; the old target is not inferred from catalogue history and is untouched.
The existing `wipe_lakehouse()` is the deliberately separate blunt operation: it
resolves a typed physical Lakehouse and clears both Files and Tables, regardless
of Weaver declarations. Compatibility functions for independently named Folder
and Delta targets remain only until the public R8 migration.

Pure tests cover the default/escape hatch, combined Tables/Files inventory,
same-type multi-item batches, Warehouse pruning and old-binding isolation. The
full lightweight suite passed 1,133 tests with one skip. The full local Spark
suite passed 100 tests in 4m09s; its R7 vertical created a real orphan Delta table
and folder, installed the retained item, independently inspected both areas, then
called the real Lakehouse wipe and found both empty. The same behavior ran through
the published `0.1.1.dev21230656620` wheel inside Fabric: after one test-fixture
metadata correction, the installed code passed the prune/build/wipe proof in 70s.

### R8 — item vocabulary at the public and CLI boundaries

The root package now presents the item model as the primary surface:
`WeaverRepository`, `WeaverItem`, `WeaverDocument`, exact logical identities,
`read_weaver_repository()`, item bindings and the item bundle generator/installer.
The old Folder/Delta target and standalone setup functions remain directly
importable for the isolated flat-planner compatibility suite, but are no longer
advertised through `__all__`. Flat-layout input to the item reader fails early
with concrete moves for Delta/Spark, Warehouse, Folder, `_schemas/` and helper
files; it is never guessed into an item layout.

`weaver build` now takes one installed repository, an optional bundle record and one
or more repeatable `--bind ItemType/LogicalName=PhysicalName` declarations. Prune
and catalogue publication default on, with explicit `--no-prune` and
`--no-catalogue` escape hatches. The desktop adapter requires a Fabric host and
submits one Environment-backed program in which the installed Weaver performs
static discovery, generation and installation. It does not plan on the laptop.
The local emulator remains available through the public Python API, where the
caller owns its Spark session. On a new control plane the first coordinated build
binds generated `Lakehouse/_weaver` once; later builds leave it unbound unless an
explicit destructive catalogue rebuild is intended.

Wipe stays physical and typed. The CLI accepts whole Lakehouse and Warehouse
targets only; independently wiping a Folder was removed because Files belongs to
the Lakehouse item. The compatibility underscore spellings for the two remaining
flags stay cheap and isolated. Repository and CLI documentation now use the
item-owned layout and logical/physical binding vocabulary.

R8 verification is intentionally pure Python rather than another heavy Fabric
test. Parser, typed binding, serialisable result, one-session program syntax,
migration error, help text, public API, boundary and neutrality tests cover the
interface. The underlying item planner/installer transport was already exercised
through the installed wheel at R6 and R7.

### R8a/R8b — item certification and one local Fabric operation

The installation row had accidentally retained the repository signature from the
flat model. That made an edit to one logical item invalidate every installation,
which is the wrong basis for incremental build. Certification now has three
deliberate grains: the repository signature covers the complete source and
coordinated snapshot; each `WeaverItem` signs its own identity, schemas,
documents, support files and destination-keyed aliases; Registry and dictionary
rows keep their declaration-level signatures. An alias belongs to the destination
that consumes it, so it changes only that item's signature. Physical bindings and
producer contents do not leak into the consumer's signature.

The build path now matches the deployment architecture rather than a desktop
optimisation. Weaver running inside Fabric makes one session-native recursive copy
from the repository in OneLake to a driver-local temporary directory. Static
discovery, validation, all three signature levels, planning and repository
snapshot generation read that local tree. The bundle is generated locally and its
payload store is independent of the target store, so its mechanical installer can
read local payloads while mutating named Fabric Lakehouses and Warehouses.
Temporary work is removed at the end of the call.

Persisting a bundle is no longer compulsory. An ordinary developer build installs
the temporary bundle directly and performs no bundle round trip through OneLake.
When a durable record or handover is useful, Weaver packages the complete bundle
as one deterministic `<timestamp>.weaver.zip` after generation or installation
and uploads that one file. A receiver copies one archive to its own temporary
directory, rejects unsafe ZIP paths, extracts it and applies the existing manifest
and payload-hash validation before installation. The archive is an internal
transport form, not a new package trust boundary.

Pure tests carry most of this proof. They mutate unrelated items, support files,
schemas and aliases independently; count one source read per remotely shaped
repository file; prove the direct path has no remote bundle reads; round-trip a
bundle including payloads and its repository snapshot; and prove archive install
reads payloads from the extraction rather than the target store. The complete
non-Spark suite passed 1,160 tests with one skip. The full shared-session Spark
suite passed all 101 tests; its public-workflow vertical creates a real Delta
table and Folder from the driver-local bundle and explicitly drops its registered
schema so the expensive session remains isolated for the following prune proof.

The first live run of the published workflow exposed one Fabric-local detail the
emulator cannot manufacture: ``notebookutils.fs.cp`` writes Hadoop
``.filename.crc`` checksum sidecars beside files copied to ``file:/tmp``. Strict
discovery rejected the sidecar before target mutation, as it should. The
``FabricStore`` copy adapter now snapshots the remote tree's real checksum-shaped
paths, removes only sidecars generated by the copy, and preserves any such file
that was actually authored in OneLake. File archive downloads remove their one
adjacent generated sidecar directly. A pure fake reproduces both directory and
binary-file behavior without weakening the rule that only ``_ignore`` is absent
from repository discovery.

The review deployment then exercised the whole coordinated path with a staged
``MixedEstate`` repository: the built-in control item in ``Play_Lakehouse_1``, a
Lakehouse item in ``Play_Lakehouse_2`` and a Warehouse item in ``Play Warehouse``.
One bundle built all three bindings, published three item-signature installation
rows, registered four operational documents, ten control-plane documents and two
reporting documents, and left both Warehouse objects and both Delta tables empty
as a build should. The Lakehouse SQL endpoint exposes its physical table names in
lower case and its collation-sensitive Warehouse consumer therefore authors that
exact three-part spelling; this is distinct from Weaver's canonical logical and
catalogue display names.

That deployment also exposed the remaining catalogue-case transition rather than
a clean-create defect. ``CREATE OR REPLACE`` under case-sensitive analysis keeps
the registered spelling when a case-insensitive predecessor already exists, so an
old ``registry`` did not become ``Registry`` merely because the new DDL was exact.
The table executor now inventories the destination inside the same case-sensitive
scope and drops the single case-only predecessor before replacement. It refuses
multiple case-colliding predecessors. This follows the existing build contract:
build owns and replaces structure, while a later load owns rows. An existing
control plane now converges to the same PascalCase contract as a fresh one.

### R9 underway — visible session contention and canonical table names

Fabric's Livy collection is now a public read-only diagnostic rather than a fact
known only to a one-off probe. `list_livy_sessions()` preserves session identity,
submitter, scheduler/plugin/Livy states, timestamps, result and cancellation
reason; `list_workspace_livy_sessions()` joins those per-Lakehouse collections
across the workspace and can retain only sessions still occupying or awaiting a
capacity slot. Both the desktop build adapter and Fabric pytest harness call it
before requesting their session. They report contention and continue; they never
cancel a session whose lifecycle they do not own. A live read against the Weaver
workspace found no active session, while its most recent ended entry carried the
user cancellation, all three states and the submitter exactly as the model
records them.

The catalogue casing observation was real and was not merely OneLake path
presentation. `SHOW TABLES` returned all ten existing names lower-cased even
though their definitions and quoted DDL are PascalCase. A disposable Fabric
probe isolated the cause: the session default
`spark.sql.caseSensitive=false` folds a table identifier at creation, while
temporarily setting it to `true` preserved `CamelSql`, `CamelSave` and
`RenamedSql` through SQL creation, `saveAsTable` and rename. The table executor
now applies that setting only for a Fabric destination's create DDL and restores
the caller's value in `finally`. It is destination capability data, not an
executor host branch. Applying it to the local emulator was tested and rejected:
local's intentionally folded schema had been registered case-insensitively, so a
case-sensitive lookup could not find it under the display-case spelling.

Pure tests prove session parsing/filtering, preflight-before-start, configuration
restoration after both successful and failed DDL, and the Fabric/local destination
split. The lightweight suite passed 1,146 tests with one skip. The complete local
Spark suite then passed all 100 tests in 4m15s. Weaver
`0.1.1.dev40734786769` was published from commit `d1dd3c8`; a fresh installed
row-3 build then returned every raw, not case-folded, `SHOW TABLES` name in
canonical PascalCase.

That same build corrected a second stale assumption. The managed directories
were PascalCase too: lower-case was not an immutable host choice once the session
was case-sensitive, but another consequence of the default setting. Physical
inventory remains an independent case-insensitive assertion because its spelling
is not Weaver identity; the required display contract is exact. Fabric's
`schema.json.gz` also sits alongside a schema's table directories and must be
excluded from a physical table inventory.

The first full PR matrix exposed two Windows portability assumptions rather than
runtime defects. Local Spark roots are deliberately URI-style strings with
forward slashes, so their tests now derive expected roots with `Path.as_posix()`
instead of interpolating a `WindowsPath`. More importantly, the committed
built-in catalogue is byte-significant repository input: a Windows checkout had
converted its LF resources to CRLF and therefore changed both the drift check and
the repository signature. `.gitattributes` now pins those shipped catalogue
resources to LF on every platform; generated and reviewed bytes remain identical.

The same matrix then exposed a Linux-only case defect that macOS's default
case-insensitive filesystem concealed. Local Spark had physically created
``customer`` while the emulator asserted ``Customer``; macOS treated those as the
same path. A per-create case-sensitive override was insufficient locally because
returning the session to case-insensitive analysis made the exact-case table
unresolvable. The local destination now uses the session catalogue's canonical
lower-case folded schema (for example ``sales_lh__sales``) and establishes
case-sensitive analysis as the emulator's session policy. Declared table names and
their managed directories therefore remain PascalCase, while Fabric continues to
scope and restore the setting around creation so Weaver does not take ownership of
the user's session policy. A filesystem-name assertion prevents macOS from hiding
the regression again. The focused two-Lakehouse proof passed all four cases. The
complete local Spark run then passed 98 real cases and exposed two deliberately
minimal error-path fakes that lacked the newly required session configuration
surface; after giving those fakes that surface, both focused error cases passed.

### R10 — the workspace is the durable boundary, not a repository name

The remaining repository dimension was generality without a use case. One Weaver
control plane describes one Fabric workspace declaration, so a repository name
cannot distinguish two live catalogue rows there. The source repository remains
valuable as a development and certification unit—its signature still certifies
the complete snapshot and coordinated bundle—but its directory name is not
logical installation identity.

The physical source contract is now singular:

```text
<Weaver Lakehouse>/Files/weaver_items/
├── Lakehouse/
│   ├── Raw/
│   └── _weaver/       generated and managed by Weaver
└── Warehouse/
```

The CLI therefore has no `--repository` selector, and both local and Fabric
resolvers expose one `weaver_items_root`. Before the one remote-to-local copy, the
ordinary build replaces the reserved `_weaver` subtree with the installed
package's canonical sources. The static reader accepts those exact bytes inside
the ordinary item hierarchy and rejects partial, unexpected or modified managed
content. Authored and generated items consequently travel through one source
tree, one signature and one bundle.

Catalogue installation scope is now exactly `(item_type, item_name)`. Object rows
add only `(schema_name, object_name)`, giving the four-part declaration identity
`ItemType/ItemName/Schema.Object`. A Lakehouse Folder uses
`schema_name = Files/<declared-schema>`; for example a Table and Folder both
authored as `Sales.Customer` become `Sales/Customer` and
`Files/Sales/Customer`. This removes `object_namespace` without losing identity.
Alias endpoints and foreign-key references use the same schema spelling, so the
catalogue also needs no destination, source or reference namespace columns.

The cheap proof is intentionally structural: every one of the ten table keys
opens with only the exact item pair, no table carries a repository or namespace
column, same-name Table/Folder rows remain distinct, Folder schema declarations
project as `Files/<schema>`, and item-scoped reconciliation cannot name another
item. Spark and Fabric remain verification of physical DDL and DML, not substitutes
for these interface tests.

The public `weaver.catalogue` module now exports only that item-scoped model. The
pre-item repository/target catalogue is quarantined under
`weaver.catalogue.legacy` while the deprecated flat planner remains available for
compatibility; it is not an alternative catalogue contract. Setup likewise
reports the generated `Lakehouse/_weaver` item rather than inventing a repository
identity for it.

Exact case must cover consumption as well as creation. Fabric's default analysis
could create `CustomerEnriched` in one action and then fold the next action's
reference to it, so the table and view executors now analyze each complete action
inside the same temporary exact-case scope used by its DDL and restore the user's
session policy afterwards. The pure executor tests cover success and failure;
the full lightweight suite passes 1,164 tests with one skip.

The local Spark multi-destination test made one thing plain that the pure tests
could not: a flat repository fixture cannot be remapped into an item tree at test
setup. The layout is the smaller half of the difference. The *content* differs
too — a Table consuming a Folder writes `Lineage: $Files/Raw.CustomerCsv` and
imports `.Files.Raw__CustomerCsv`, neither of which the flat spelling can carry,
and a bare `data/` directory has no home beneath an item, where non-object files
belong in `lib/`. So the item verticals get their own authored fixture,
`tests/fixtures/build-lakehouse-item`, declaring one `Lakehouse/Raw`. The flat
`build-lakehouse` fixture stays exactly where it is, feeding the deprecated
planner's compatibility tests. Two models, two fixtures, neither pretending to
be the other.

### R11 — the directory already said it

Two pieces of a declaration were still repeating what their own location says.

The first was `.spark.sql`. That suffix earned its place in the flat layout,
where every document sat in one directory and nothing but the filename could say
which engine would run it. Items removed that problem without anyone noticing:
a Lakehouse materialises Delta through Spark, a Warehouse materialises through
T-SQL, so by the time a reader sees the file it has already passed the directory
that answers. A document is now `Schema.Object.sql` and the item picks the
dialect:

```text
Lakehouse/Raw/DWG.Customer.sql          Spark SQL
Warehouse/Reporting/DWG.Customer.sql    T-SQL
```

The deprecated flat reader keeps `.spark.sql`, and that is not an inconsistency
to tidy away later — it is the point. The flat model has no item to ask, which is
exactly why it needed the suffix. Inside an item the suffix is rejected outright
with the rename to make, rather than quietly accepted as a synonym.

The second was `alias.yml`. It sat at the declaration root and keyed every entry
by a full four-part destination, which meant every line named the item it was
already going to be read for. An alias is a name *one item* wants for a document
another item owns, so it now lives in that item's own directory:

```yaml
# Warehouse/Reporting/alias.yml
aliases:
  DWG.Customer: Lakehouse/Raw/DWG.Customer
```

The file's location is the destination item. Only the source stays four-part,
because the source genuinely is elsewhere.

This paid for itself immediately in the signature code. Certifying a root
alias.yml meant hashing each declaration separately under a synthetic
`alias:<destination>` key, so that an entry for one item would not disturb
another item's signature — machinery whose only job was to undo the file's
misplacement. An item's alias.yml sits under the item's own prefix, so it is
certified as one of its support files and that special case is deleted. Adding an
alias still moves exactly one item signature, now because of where the file is
rather than despite it.

---

### R12 — the second architecture goes

R10 and R11 left Weaver carrying two of everything: two readers, two planners,
two catalogues, two builtins. The item model was the public surface and the flat
model was "isolated compatibility", which is a comfortable phrase for code that
still has to compile, still has to be understood, and still gets read by the next
person as though it were a live option.

Matthias's call was that there is no legacy here — this framework has not shipped
to anyone who needs a migration path — so the second architecture is not a seam
to maintain, it is just the first draft. It went:

- `weaver.catalogue.legacy`, the re-export shim;
- the flat `read_repository` and `SesRepository`, and with them the root-file
  conventions (`_schemas/`, `_helpers/`, private root files) that only made sense
  when every document lived in one directory;
- the flat planner and its projection, and the repository/target-scoped
  catalogue tables;
- the committed `weaver/builtin/catalogue` SES resources, whose drift test existed
  only because the text was committed separately from the definitions it mirrored;
- `.spark.sql` everywhere, including as a rejection: with the item choosing the
  dialect there is no special case left to write, and `Sales.Rollup.spark` is
  simply a filename that does not name Schema.Object;
- the migration error that told an author how to convert a flat repository.

What survived moved rather than being copied. The target inspectors the flat
planner owned became `weaver.build_bundle.prune`, which is what they always were
— reconciling a bound physical item against what an item declares, the half of a
build that can destroy data. The item modules then took the plain names, because
`item_planner` beside no other planner is a qualifier that distinguishes nothing.

The signature machinery got smaller twice over in two checkpoints, which is the
tell that both changes were removing something the design had been paying for
rather than adding to it.

The cost is honest and worth writing down: about a hundred and sixty tests went
with the flat reader, and not all of them were testing the flat reader. The whole
path over a realistic multi-object fixture — classification through metadata,
structural checks, SQL analysis and discovered references, asserted together —
was written against `sales-etl` in the flat layout, and it is gone rather than
ported. The parse, DDL, graph and dependency layers keep their own unit tests and
the item reader has its own end-to-end file, so the loss is integration breadth
rather than a hole over specific behaviour. It should come back as an
item-shaped fixture; it has not yet.

---

## Open questions

| Question | Raised | Status |
|---|---|---|
| Which `weaver` revision is the port baseline — the plan's `a97ba8a` or current `fee2025`? | CP0 | open |
| Path-like *reader* for Folder dependencies during ETL. | CP2 | settled at CP4: `Folder.folder_path()` on the depended-on class; realised as a `Path` at load. Load itself is deferred. |
| The whole load phase: running `read()`, upsert/incremental merge, audit-column accounting, applying proposed deletes. | build | deferred — build creates structure only; load populates it, and is the next body of work after the Fabric seam. |
| Should the local emulator stand up a durable (cross-process) metastore, or is in-session catalog registration enough? | build | open — build registers each Delta table into the session catalog (via a schema database with a `Tables/<schema>` LOCATION) so views bind by name; cross-process persistence is not a prerequisite. |
| Build reconciliation scope: prune drops what a Lakehouse holds that the bundle does not manage, scoped to that Lakehouse's `Tables/`/`Files/` storage. Is per-Lakehouse physical scope the right boundary once Fabric catalogs are per-item? | build | **settled at R0: yes, now reached through the exact owning Weaver item.** Prune removes physical structure absent from that item's retained documents. Two logical items may bind one physical item, but with prune on they consume one another; `prune=False` is the explicit unsafe exception, not a shared-ownership model. |
| Does OneLake DFS implement ADLS Gen2 `x-ms-rename-source`? Determines whether desktop-initiated moves are cheap. Ten-minute experiment. | CP2 | open, due CP7 |
| Should `Identity` imply `Incremental: true`? Left free deliberately. | CP3 | still open — build now materialises the surrogate (a Weaver-managed not-null `bigint`, not autogenerated), but identity is provisional and treated separately, load semantics included. |
| Control-table names, and whether they sit under a schema. | CP2 | due CP16 |
| Shortcut / external-dependency config: `_shortcuts/*.yml`, selected as `--shortcuts prod.yml`. Names are logical and belong to the repository; targets are physical and belong to the build. Deferred. | CP6 | **superseded at R0.** One control plane contains one Weaver repository. Portable cross-item names are destination-keyed declarations in repository-level `alias.yml`; authored three-/four-part names remain the physical, environment-locked route. Physical alias behaviour is still deferred and retained alias use fails explicitly. |
| Is the third target called `delta_target` or `spark_target`? The command sketch says Spark; the internal target kind is `delta`. | CP11 | **superseded at R0.** Bindings are logical `Lakehouse/<name>` or `Warehouse/<name>` items. Delta and Folder are object kinds owned by a Lakehouse, not binding types. |
| Does `build` move any files at all? | CP2 | settled: yes, exactly one — the repository snapshot, and that movement is certification rather than a side effect. |
| Should the catalogue be addressed by explicit path rather than by two-part name? | catalogue | **settled: no.** The Spark session is attached to the Weaver Lakehouse — that is the fixed control-plane context, so two-part names in schema `_` are the defined execution context rather than ambient resolution. Destination Lakehouses are the data plane and are addressed through roots resolved from their bindings (`LakehouseSparkLocation`). The local `_` churn is fixture isolation, not architecture. |
| Destination Delta objects are still built as two-part names, which resolve through the session's catalogue — and the session is attached to Weaver. | catalogue | **settled: a payload names its object, the installer names the Lakehouse.** A generated statement carries `{{object:Schema.Name}}` and the executor resolves it against the batch's target — Fabric's four-part name, or the local proxy's folded database name. Freezing the qualified name would have made two bundles of one repository differ in every payload between environments (§10); keeping the two-part name kept the defect. Schema creation stopped being frozen SQL for the same reason: its `LOCATION` was a resolved temporary directory inside the hashed plan. |
| How should `-m spark` run now that it is ~93 tests? | catalogue | **settled, and it was not a harness limitation.** Delta's `DeltaLog` cache holds a `Snapshot`, and therefore a query execution and its encoder, per table *path*; every test builds under a fresh `tmp_path`, so the cache kept every Lakehouse the suite had thrown away. Clearing it between tests took the run from failing at the 1 GB ceiling in 5:04 to 92 passing in 3:42 with live heap between 68 and 219 MB. No process isolation needed. |
| Can a destination Lakehouse's schemas be enumerated through Spark? | multi-target | **settled: no, and storage answers instead.** Fabric refuses `SHOW SCHEMAS IN `ws`.`lh`` and a bare `SHOW SCHEMAS` answers only for the attached Lakehouse, so schema discovery reads the destination's `Tables/` area through the store — which prune already did. Views are catalogue-only and are asked of the destination by its four-part name. |
| Should the local emulator give each Lakehouse its own Spark catalogue? | multi-target | **settled: it cannot.** `DeltaCatalog` extends `DelegatingCatalogExtension` and its delegate is only set for `spark_catalog`; registered as a named catalogue every statement dies in the analyzer. The proxy folds the Lakehouse into the one namespace level Spark offers (`sales_lh__sales`), keeping the isolation and leaving storage layout untouched. |
| Should the Fabric build modules share one Livy session? | multi-target | **settled: one session, one Weaver Lakehouse and one target Lakehouse for the whole run**, all created before the session, with the target emptied between contexts. Sharing the session alone was not enough — the remaining per-test Lakehouse churn made Fabric's namespace resolver report `Artifact not found` for a target that existed. 41 passed with 7 errors in 39:16 became 48 passed in 10:06. |
| Does `%pip install` from a notebook resource path work in a Fabric session? | CP7 | open, cheap to check |
| Can a Livy session see a notebook's resources? If so, delivery and runtime source need not be separated at all. | CP7 | open |

## Divergences from the plan

| Checkpoint | Divergence |
|---|---|
| 2 | Widened to include the location type and the file-transport protocol. |
| 5 | Reader goes through `Store` rather than `Path`; result-set guard added (not in the plan). |
| 4 | `self.repo` removed; dependencies become imports; Spark SQL supported rather than deferred. |
| 6b | Superseded at 6c: cross-engine two-part resolution is now an explicit `Warehouse alias`/`Lakehouse alias`, not a single-candidate inference; an unresolved two-part name is refused rather than recorded. Three-part reads and the CP6b sales-etl fixture stay valid. |
| new | Schema SES files under `_schemas` (not in the original plan's checkpoint order) — every object and alias schema must be declared; no on-the-fly schema. |
| build | Build is strictly structure, never load: `create_ddl()` returns Spark DDL (`CREATE OR REPLACE TABLE` over the declared schema, `CREATE OR REPLACE VIEW`, an empty folder directory) and the repository is read once into a frozen bundle. The plan's Python-payload-runs-`read()` shape is dropped; a load phase comes later. |
| build | Build registers each Delta table into the Spark catalog by giving its schema database a `Tables/<schema>` LOCATION, so a managed table lands at Weaver's path and a Spark SQL view binds inputs by two-part name — mirroring Fabric, where a declared table is immediately queryable. |
| build | Build reconciles: prune sequences run upfront and drop any schema/folder/table/view the bundle does not manage, scoped to the bound Lakehouse's own `Tables/`/`Files/` storage. |
| 3 | Substantially extended: references, `Prohibit rebuild`, `Not null`, `Identity`, `Comparison columns`, `Column notes`, `Notes`, `Revision notes`, audit columns, unknown-key rejection. `Load mode` removed. |
| build | SQL-backed tables added, both engines: a Spark SQL or T-SQL table takes its shape from its query, declared or inferred, via install-time query-shape inference (§7.2/§7.3). Only the main table is built (no view/`_Current`/`_History`). A bundle targets one physical side (Lakehouse *or* Warehouse); the alias-based cross-database loop is the next branch. `Identity` materialises a Weaver-managed `bigint` surrogate, but is provisional and treated separately. Qualified end to end on a real Fabric Warehouse. |
