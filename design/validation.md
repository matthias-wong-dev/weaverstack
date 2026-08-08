# Tests and Assumptions

Weaver validates an estate with two kinds of authored declaration. Both are
ordinary Weaver declarations — schema-qualified, dependency-resolved, compiled to
independently runnable primitives, installed by an ordinary build — and neither
is a data object. That last clause is what this document is mostly about,
because it is the thing the rest of the system was not previously shaped for.

This is the authoritative design for validation. Where it and the code disagree,
the code is wrong.

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

**The directory names the kind rather than merely grouping it.** A file in
`tests/` declaring an Assumption is refused and told where it belongs. Two
directory names beat one directory plus a header nobody reads without opening
the file.

## The metadata contract

The old `_COMMON_KEYS` said what every document got and left each kind to add to
it. That is the wrong shape for a declaration that is not a data object: almost
everything in it — `Lineage`, `Static`, `Prohibit rebuild`, the aliases — is not
a key a Test *happens not to use*, it is a key that could not mean anything on
one.

So the keys are grouped by what they are about, and each kind composes the
groups that apply to it:

| group | keys | who has it |
|---|---|---|
| document | `Description`, `Notes`, `Revision notes` | everything |
| dependency | `Dependencies` | everything |
| data lineage | `Lineage` | data objects |
| build behaviour | `Static`, `Prohibit rebuild` | data objects |
| alias | `Warehouse alias`, `Lakehouse alias` | data objects |

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
WeaverItem.documents      # what this item materialises
WeaverItem.validations    # what this item validates
WeaverItem.declarations   # both, for the readers that genuinely span them
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
rows. Setup is unrestricted and may be dynamic.

**Both sides come from one execution.** `Test.read()` reaches its two relations
through a `_sides()` hook rather than by calling `expected()` and `actual()`
separately, so a compiled Test runs its program once. Running it twice would
compare two different snapshots of anything the setup materialised, and report
the difference between them as failure.

There is no installed `.sql` validation program and nothing that runs one. The
comparison, key validation, correlation and diagnostics stay in
`test_compare` rather than being emitted a second time in SQL.

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

## Not in scope

Severity levels, warning-only Tests, tolerances, per-Test scheduling, a separate
`weaver assumption` command, persisted diagnostic rows, multiset comparison, a
changed-row count distinct from the symmetric difference, a second dependency
system, a second Spark SQL engine, a second T-SQL splitter, and blanket
rejection of dynamic T-SQL.

Diagnostic rows are interactive evidence, not task-log metadata: they may be
large and may carry sensitive business data. Task logs persist counts.
