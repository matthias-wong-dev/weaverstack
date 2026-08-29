# Tests and Assumptions

## Purpose

This document defines Weaver validation: how Tests and Assumptions are authored,
installed, run, and recorded. It owns validation architecture; command use is
documented in [CLI usage](cli-usage.md).

Weaver validates an estate with two kinds of authored declaration. Both are
schema-qualified, dependency-resolved, compiled to independently runnable
primitives, and installed by an ordinary build. Neither materialises a data
object.

## The two kinds

A **Test** compares an expected relation with an actual one and passes when the
symmetric difference is empty:

```text
missing     = expected EXCEPT actual
unexpected  = actual   EXCEPT expected
```

An **Assumption** returns the rows that contradict it, and passes when there are
none. It has no second side, so there is nothing to correlate, so it may not
declare a primary key.

## Where they are authored

Beneath the item that owns them, in two directories accepted under both a
Lakehouse and a Warehouse item:

```text
Lakehouse/Sales/
    schemas/Sales.yml
    Sales__Order.py
    tests/
        Sales__OrdersReconcile.py
        Sales.OrderSummaryReconciliation.sql
    assumptions/
        Sales__OrdersUpToDate.py
        Sales.NoOrphanOrders.sql
```

The owning item continues to choose the SQL dialect — a `.sql` file is Spark SQL
in a Lakehouse and T-SQL in a Warehouse — and Python validation runs through
Spark, so it belongs to a Lakehouse item.

The directory identifies the validation kind. A file in `tests/` that declares
an Assumption is refused and directed to the appropriate directory.

## The metadata contract

Keys are grouped by their purpose, and each validation kind composes only the
groups that apply to it:

| group | keys | who has it |
|---|---|---|
| document | `Description`, `Notes`, `Revision notes` | everything |
| dependency | `Dependencies` | everything |
| data lineage | `Lineage` | data objects |
| build behaviour | `Static`, `Prohibit rebuild` | data objects |
| shortcut | `shortcuts.py`, `shortcuts.yml` | data objects |

A Test adds `Primary key`. An Assumption adds nothing.

Refusing a data key on a validation says which kinds *do* have it and why this
one cannot, because `Lineage: ...` on a Test reads as plausible until you ask
what it would do.

## Identity: one namespace, two collections

A validation carries the item's ordinary item-qualified `Schema.Object`
identity. It is held in the same `source_documents` keyspace as the objects,
which is what makes a Test and a Table that both claim `Sales.Order` inside one
item an ambiguous duplicate — refused by the machinery that already refuses two
tables of the same name, rather than by a second rule.

It is listed separately on the item:

```python
WeaverItem.documents  # what this item materialises
WeaverItem.validations  # what this item validates
WeaverItem.declarations  # both, for the readers that genuinely span them
```

Dependency resolution, reference checking and the item signature use
`declarations`. Everything that projects DDL, dictionaries or Registry rows uses
`documents`, and therefore cannot be handed a Test on the strength of a Test
having an object identity.

## The comparison

`weaver.runtime.test_compare` is the one definition, and the T-SQL renderer
emits the same two `EXCEPT` statements against the same contract. A Test written
in Python, compiled from Spark SQL or rendered as a Warehouse procedure means one
thing, or "the Sales tests pass" means nothing across an estate.

**The counting is physical.** `failure_count = missing_count + unexpected_count`,
so one changed entity contributes two discrepancy rows — an expected-side row
and an actual-side row. A second, logical counting model would have to decide
what "changed" means without a key, and a Test is not required to have one.

**The key correlates and does not count.** Diagnostic rows carry two reserved
columns ahead of the Test's own:

```text
_weaver_side   _weaver_sk   OrderId   Amount
expected       1            10        100
actual         1            10        110
expected       2            20        200
actual         3            30        300
```

Same key on both sides is *changed*; expected only is *missing*; actual only is
*unexpected*. That classification is presentation. Remove the declared key and
the same Test reports the same number of failures, with every row keyed on its
own.

`_weaver_sk` is runtime correlation information, not a serialized copy of the
declared key: a reader needs to know which rows pair, not which columns paired
them.

### Failure is not execution failure

Keep these apart, always:

| | what it is |
|---|---|
| validation failure | it ran, and found evidence — rows |
| execution failure | it could not be evaluated |

A duplicate or blank declared key, two sides that cannot be compared, a missing
installed primitive and a SQL error are all the second kind, and raise
`ValidationError`. Reporting them as zero discrepancies would say a Test nobody
could run had passed.

Key validity reuses the load layer's own rule — see
`weaver.runtime.delta_sql.blank_key_predicate` — rather than inventing a second
idea of what a key is.

## The authoring surface

```python
class Sales__OrdersReconcile(Test):
    def expected(self):
        return Sales__OrderSource(self).dataframe()

    def actual(self):
        return Sales__Orders(self).dataframe()
```

`read()` is Weaver's and is **not authorable**. A Test that could redefine it
would still be called a Test while meaning something else, and the one thing a
reader must be able to assume about every Test in an estate is what passing
means. The override is refused by `__init_subclass__` *and* by the repository
parser, so it fails whether the class is written in a notebook or committed.

An Assumption authors `read()` directly, and what it returns is the evidence.

### Compiled from SQL

A Spark SQL validation compiles to a Python module carrying the authored SQL,
exactly as a Spark SQL table does — the authored header becomes the docstring
that is the contract, the SQL becomes `SQL`, and a Weaver base supplies the
rest:

```text
Lakehouse/Sales/tests/Sales.OrdersReconcile.sql   what the developer writes
Files/_/Load/tests/Sales__OrdersReconcile.py      what a build deploys
```

The program's shape is its contract: after any setup, a Test's first query is
expected and its second is actual; an Assumption's one query is the violating
rows. Setup is unrestricted and may be dynamic — but it comes **first**, and
nothing may follow the contract queries.

That last rule is not tidiness. A Spark SQL `SELECT` is lazy: the frame is built
where it is written and materialised later, so a setup statement running after
the first contract query changes what that query will read by the time anyone
reads it. T-SQL does the opposite, capturing each contract query into a temp
table at its authored position. The same body would mean two different things on
the two engines, so both refuse it rather than each answering its own way.

**Both sides come from one execution.** `Test.read()` reaches its two relations
through a `_sides()` hook rather than by calling `expected()` and `actual()`
separately, so a compiled Test runs its program once. Running it twice would
compare two different snapshots of anything the setup materialised, and report
the difference between them as failure.

There is no installed `.sql` validation program and nothing that runs one. The
comparison, key validation, correlation and diagnostics stay in
`test_compare` rather than being emitted a second time in SQL.

### Compiled to a Warehouse procedure

A T-SQL validation compiles to an installed procedure, and — unlike a load — the
payload *is* the procedure. A load procedure has to name its target's physical
columns, which are not knowable while the target is a declaration, so what a
load generates is a script that reads `sys.columns` and assembles the procedure
server-side. A validation names no target: its columns are whatever its own
queries return, materialised into temp tables, and `EXCEPT` and `d.*` are
column-agnostic.

```sql
create or alter procedure [_].[Test Sales.OrdersReconcile]
    @missing_count       bigint = null output
  , @unexpected_count    bigint = null output
  , @suppress_result_set bit = 0
```

The counts are in the signature, not in a result set, for the reason a load's
are: authored setup may run `EXEC` and return rows of its own, so "the result
set this produced" is a question with no answer. They are optional so
`exec [_].[Test Sales.OrdersReconcile];` still works typed by hand.

**Only the contract queries are rewritten.** A single offset-exact pass over the
authored body diverts each into a temp table with the same transform the
shape-only build uses — so a CTE gets its `INTO` on the body `SELECT` — and
everything else the author wrote travels verbatim and in place.

**`_weaver_sk` ranks keys, not rows.** The distinct declared-key values across
both sides are ranked once into a table of their own and joined back. Ranking
over a union of the *rows* is the obvious shape and does not work: the rows are
projected with `*`, since Weaver does not know a Test's columns, so a
`_weaver_side` added to the union would appear in the output twice. Without a
key, each side is numbered within itself and the actual side is offset past
`@missing_count`.

**Working tables are dropped at both ends of a run.** The body drops each temp
table it names before it captures anything and again once the counts and the
diagnostics are out, so a connection is left as it was found. The closing drops
are what the direct `--file` batch relies on, having no procedure invocation to
be scoped to; an installed procedure is also released by the engine when the
invocation ends. A run that threw is the case neither covers: a guard stops
before the closing drops, so its tables stay in that connection's session, named
for the validation that made them, until the opening drops of the next run on
that connection remove them.

**One body, two wrappers.** The installed procedure and the direct `--file`
batch share the rendered body exactly — the batch declares the same locals and
projects them at the end. Two renderers would be two contracts, and the promise
of `--file` is that it runs what an install would have run.

Dependencies are ordinary imports resolved by the ordinary AST machinery, and
`Sales__Orders(self)` inherits the session and the resolved Lakehouse exactly as
it does inside a Table. Nothing about validation dependencies is new — a second
dependency language for validation is exactly what this design does not build.

### One dependency rule, and one thing a validation may not depend on

A validation uses the rule everything else uses: a declaration replaces
inference, and `Dependencies: []` is a declaration, so an explicit none means
none. There is deliberately no second dependency semantic to learn.

What differs is only whether a kind is *required* to declare. A Spark SQL
**object** is, because its query may read by path and a load ordered by a
half-known graph builds things in the wrong order. A Spark SQL **validation** is
not, and does not have to carry a `Dependencies:` header at all.

That exemption rests on two facts, and they are the reason it is safe:

- **Validation installs last**, with the load artefacts, so the objects a
  validation reads are already in place by the time it runs.
- **Nothing depends on a validation.** A Test reads the estate and produces
  nothing, so there is nothing for anything else to read. A declaration naming
  one is refused rather than resolved — if a validation could depend on another,
  ordering among validations would start to matter, silently, and the exemption
  above would stop holding.

So an edge inference missed on a validation costs an ordering nicety, not a
wrong estate.

## What the catalogue records

Two rows, describing two different things:

| | describes |
|---|---|
| `_.TestDictionary` | the **logical** authored declaration |
| `_.Registry` | the **physical** procedure or module it compiles to |

There is **no Registry row under the logical Test ID**, because nothing is
materialised there. `TestDictionary` carries `test_type` (`test` /
`assumption`), the description and its reference, and the declared
`primary_key` — null for a Test that declares none, and always null for an
Assumption.

One dictionary holds both kinds, deliberately: a reader asks the same questions
of each, and the two share one logical namespace so they cannot both claim a
key.

Validation dependencies belong to the **logical** identity:

```text
_.Dependency     schema_name = Sales,  object_name = OrdersUpToDate
_.Registry       _.[Assumption Sales.OrdersUpToDate]
```

Current load-estate reading recovers consumers through Registry. That does not
hold for validation, so catalogue reading derives logical validations from
`TestDictionary` and associates their `Dependency` rows with those IDs. A fake
logical Registry row to reuse the load helper is not an option.

## The runtime artefact

`LoadArtefact` became `RuntimeArtefact` with a `role`, rather than acquiring a
parallel class. Loads and validations have one lifecycle — claimed from source,
signed, selected incrementally, installed, registered, pruned — and only the
producers differ, because what a load *is* and what a validation *is* are
different questions:

```text
load_artefacts(repository)        validation_artefacts(repository)
              \                  /
            runtime_artefacts(repository)
```

Identity is deterministic from the owning item, the kind and the logical ID:

```text
Warehouse/Reporting/procedure:_/Test Sales.IncrementalCount
Warehouse/Reporting/procedure:_/Assumption Sales.OrdersUpToDate

Lakehouse/Sales/file:_/Load/tests/Sales__IncrementalCount.py
Lakehouse/Sales/file:_/Load/assumptions/Sales__OrdersUpToDate.py
```

**Under the existing runtime root, not beside it.** That root is the item's
Python import root, so `from Sales__Order import Sales__Order` resolves from a
validation exactly as it does from a load — no second import root, no duplicated
object modules. The folder is named `_/Load`; renaming it to `_/Runtime` is a
cosmetic change outside this work.

A generated validation's signature is salted with its generator version
(`SPARK_VALIDATION_VERSION`, `TSQL_VALIDATION_VERSION`), so an edit to a
renderer rebuilds exactly the artefacts it changed. A Python validation is
deployed verbatim and signed by its own bytes, because nothing generated it.

**An item that only validates still gets its infrastructure** — the generated
`_` schema in a Warehouse, the runtime tree in a Lakehouse — or its primitive
would have nowhere to land.

## Role, not shape

Registry roles are `data`, `load`, `test` and `assumption`.

The old shortcut was that a file or a stored procedure *was* a load artefact,
because a load layer installed the only files and procedures there were. A Test
compiles to a module and a procedure of its own, so the shape now answers
nothing:

- reading installed state asks `RegisteredDocument.object_role`, which is
  preserved rather than dropped after Registry parsing;
- during a build, where nothing is installed yet, membership is asked of the
  repository's claimed runtime artefacts.

`WeaverDocumentId.is_load_artefact` is gone. This matters because a Test
procedure that inferred its way into the load DAG would be run by `weaver load`.

## Running it

Four modes, and they mean the same thing:

| | what it runs | evidence |
|---|---|---|
| `weaver.test(item)` | every installed validation the item owns | counts |
| `weaver.test(item, name=…)` | one installed validation | counts **and** rows |
| `weaver.test(item, file=…)` | a source file, installing nothing | counts and rows |
| `Sales__X(spark).read()` | one authored class, directly | rows |

The first three read the **installed catalogue** and never reopen the
repository, except `file=`, which is the point of it. The fourth opens no
catalogue, invokes no orchestrator and writes no task log — it is the notebook
loop, and it stays deliberately outside all of this.

### One failure does not stop the rest

A validation is read-only, so a Test that failed has told you something and the
next can still tell you something else. An early exit would throw that away for
no safety in return, which is why a run reports every node and takes the worst
status.

### Suppression is about size, not speed

A whole-target run never materialises a diagnostic row —
`@suppress_result_set = 1` on a Warehouse, a count that never collects on Spark.
A targeted run asks for the rows and gets them **from the same execution** as
the counts, because running a Test twice compares data that could have moved and
may cost a great deal.

### What is persisted

Counts and execution metadata. Never a discrepancy row, a violation row, a
`_weaver_sk` value or a serialized key: those are interactive evidence, and a
node's mapping — which is what a task log and a transported report are built
from — simply has no field for them.

A run's completion document aggregates planned, executed, passed, failed and
invalid counts, plus total `missing_count`, `unexpected_count` and
`violation_count`. There is no manufactured "changed row" count, which would
need a key a Test is not required to have.

## Not in scope

Severity levels, warning-only Tests, tolerances, per-Test scheduling, a separate
`weaver assumption` command, persisted diagnostic rows, multiset comparison, a
changed-row count distinct from the symmetric difference, a second dependency
system, a second Spark SQL engine, a second T-SQL splitter, and blanket
rejection of dynamic T-SQL.

Diagnostic rows are interactive evidence, not task-log metadata: they may be
large and may carry sensitive business data. Task logs persist counts.

`weaver load --include-test` is designed but not built. Embedding validation in
the load DAG touches working load orchestration, and the requirement is
emphatically not "run all tests at the end" — validation nodes have to sit in
the real physical graph behind their dependencies, including shortcut and
endpoint-refresh barriers. It is deferred so that everything above it can land
without risk to `weaver load`.
