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

**On the command line the target is a positional, so it carries no kind.**
Rather than three kind-flags, the *shape* decides: `Sales_LH` names an item and
clears all of it, `Sales_LH/Files/Extracts` names a folder root and clears only
that. What an item *is* comes from the host — locally every item is
Lakehouse-shaped, on Fabric it must be asked for, which is why a Fabric wipe
raises until item resolution exists.

```bash
weaver wipe Sales_LH --host MyLocal --config env.yml --dry-run
weaver wipe MyWarehouse --host MyFabric --config env.yml
```

The plan is always printed first, then acted on. Without `--yes` it asks; with
no terminal to ask on it refuses and says so, so a script cannot destroy
something by omission. `--root` builds a local host without a config file, since
nothing should require a file to be expressible.

`_add_host_args` and `_resolve_host` are shared, so `build` and `load` inherit
the same `--host`/`--config`/`--root` handling.

---

## Open questions

| Question | Raised | Status |
|---|---|---|
| Which `weaver` revision is the port baseline — the plan's `a97ba8a` or current `fee2025`? | CP0 | open |
| Path-like *reader* for Folder dependencies during ETL. | CP2 | settled at CP4: `Folder.path()` on the depended-on class; materialised to a real local `Path` at load. |
| Does OneLake DFS implement ADLS Gen2 `x-ms-rename-source`? Determines whether desktop-initiated moves are cheap. Ten-minute experiment. | CP2 | open, due CP7 |
| Should `Identity` imply `Incremental: true`? Left free deliberately. | CP3 | deferred until identity is implemented |
| Control-table names, and whether they sit under a schema. | CP2 | due CP16 |
| Shortcut / external-dependency config: `_shortcuts/*.yml`, selected as `--shortcuts prod.yml`. Names are logical and belong to the repository; targets are physical and belong to the build. Deferred. | CP6 | due at build |
| Is the third target called `delta_target` or `spark_target`? The command sketch says Spark; the internal target kind is `delta`. | CP11 | open |
| Does `build` move any files at all? In the central architecture, source stays central and load imports it — the case may be empty. | CP2 | due CP12 |

## Divergences from the plan

| Checkpoint | Divergence |
|---|---|
| 2 | Widened to include the location type and the file-transport protocol. |
| 5 | Reader goes through `Store` rather than `Path`; result-set guard added (not in the plan). |
| 4 | `self.repo` removed; dependencies become imports; Spark SQL supported rather than deferred. |
| 3 | Substantially extended: references, `Prohibit rebuild`, `Not null`, `Identity`, `Comparison columns`, `Column notes`, `Notes`, `Revision notes`, audit columns, unknown-key rejection. `Load mode` removed. |
