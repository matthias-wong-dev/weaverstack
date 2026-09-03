# Health

## Purpose

`weaver health` reports what the installed estate's operational state adds up
to. It answers one question an operator asks every morning: is what Weaver
installed doing what it was installed to do.

This document defines the model. See [CLI usage](cli-usage.md) for the command
and its output, and [Central catalogue](catalogue.md) for the tables health
reads.

---

## Three sections and one status

```text
Load    every loadable node, its current _.LoadStatus, and its freshness
Tests   every Test and Assumption, its current _.TestStatus, and its freshness
Build   what installed state contradicts about itself
```

Each section carries findings, and each finding a severity:

```text
Green   Amber   Red
```

A section's status is its worst finding, and overall health is the worst
section. No status is stored, so nothing can disagree with the findings under
it.

Findings keep the runtime vocabulary alongside the severity, so a consumer reads
how bad something is and what actually happened without either word standing in
for the other:

```text
severity   green | amber | red
status     succeeded | rejected | failed | error | blocked | pending
```

Each finding also carries a stable `code`, so a JSON consumer never parses
prose.

---

## Subjects come from the installed graph

Health overlays evidence onto `Catalogue.dag()`. What is installed, what depends
on what, and what validates it are that graph's answers, and health reads them
rather than reconstructing them. See
[Code architecture](code-architecture.md#one-graph-implementation-three-topologies).

```text
Load subjects    dag.loadables()
Test subjects    dag.validations()
Build subjects   every node
```

Subjects come from the graph, never from the presence of a status row. A freshly
rebuilt loadable has no `_.LoadStatus` row, because the rebuild ended the
incarnation the old one described, and that absence is the finding.

A View participates in dependency traversal and is not a load subject: it owns
no load work. A generated runtime artefact is not a subject at all: it is what
runs a subject, and the graph holds it beside the node rather than as one.

---

## Two clocks

Freshness is two questions, so health reads two instants.

**Overdue** is a question about the wall clock. `as_of` is compared with
`_.LoadStatus`'s completion instant, and a load that settled before it reads as
stale. `as_of` defaults to 24 hours before the report started.

**Behind its sources** is a question about data movement. `_.Bookmark` is
compared instead, on both sides:

```text
bookmark(ancestor) > bookmark(node)   →  the node is behind
```

A bookmark advances for a clean load that established an instant. A rejecting
load keeps the one it had, and a Static skip moves nothing, so neither reads as
newer data. A failed, errored or blocked load establishes nothing and advances
nothing.

That is why a Static object skipped at 09:00 does not make a descendant loaded
at 08:00 stale: its `_.LoadStatus` says the load succeeded a moment ago, and its
bookmark says its data stood still.

Ancestry is transitive over the whole managed graph, so it reaches through a
View, through a logical shortcut, and across items.

A Test or Assumption is stale when managed data in its ancestry moved after it
passed. Time alone does not make a validation stale: its freshness is tied to
whether the data it reads moved.

### Static objects

A Static object is loaded once. It is normally a small reference dataset that is
meant to sit still, so neither freshness question applies to it:

```text
Static + never successfully loaded   Amber / pending
Static + failed, error or blocked    Red
Static + rejected                    Amber
Static + successfully loaded         Green
```

Neither `as_of` nor an ancestor that moved is asked of one. A reference table
loaded months ago stays Green.

Its bookmark still counts against everything downstream. That is the lineage
evidence a consumer needs:

```text
Static Ref.Country loaded January     Fact.Customer loaded February   Green
Static Ref.Country loaded August      Fact.Customer loaded February   Amber
```

A genuine reload of a reference table is exactly what puts its consumers behind.

`is_static` reaches the evaluator on `InstalledNode`, carried from the Table or
Folder dictionary row when the graph is built. Health reads the graph and never
a dictionary row.

---

## Build health

Build health reports what installed state contradicts about itself:

```text
missing_validation_artefact   a declared validation with no Registry row for its artefact
not_installed                 a dictionary row Registry does not certify
certified_missing             a certified object the physical target does not hold
dependency_unresolved         a declared read that names nothing installed
ambiguous_installation        two logical objects at one physical address
```

A load artefact's absence is not among them. A table may declare `Has load
procedure: false`, and the catalogue records no column separating that from an
artefact that failed to install.

A Lakehouse view is not proven present. It exists in the Spark catalogue and
nowhere in storage, and health reads a Lakehouse over storage so that a report
never starts a Spark session.

Where catalogue state cannot be read as a managed graph at all, a Registry row
whose item has no Installation row for instance, the operation fails through the
ordinary command error path rather than fabricating a finding. See
`weaver.errors.CatalogueStateError`.

---

## Current state, and the history that explains it

Health is a view of current state, so its history read starts from current
state and never infers current state from history.

`_.LoadStatus` is the current state: one row per loadable object, holding how
that object's load ended, when, and the workflow that ran it. The whole summary
comes from that table alone.

```text
_.LoadStatus                     the current-state summary
    counts
    workflow_ids
    started_at
    completed_at
```

It has to be that table alone, because not every current state has a statistic.
A Blocked load settles a `_.LoadStatus` row and appends no `_.LoadStatistic`
row, so a summary built from the statistics would omit exactly the objects an
operator most needs counted.

`_.LoadStatistic` then enriches those states with what each load moved, matched
on the workflow and the object's four-part logical identity:

```text
_.LoadStatus
    + Workflow ID
    + Item type
    + Item name
    + Schema name
    + Object name
         ↓
_.LoadStatistic              what those loads moved
```

All five, because neither half identifies a load on its own. The workflow alone
carries every other object that workflow loaded. The object alone carries every
historical execution of it, and `_.LoadStatistic` accumulates them.

Current state spans as many workflows as it took to reach. A workflow that loads
84 objects, followed by one that loads 2 of them, leaves 82 objects explained by
the first and 2 by the second, and health reports all 84. Reading the globally
latest workflow out of `_.Log` and taking its statistics would discard the 82,
because a later partial load is not a later account of the whole estate.
`CurrentLoad.workflow_ids` therefore holds every workflow behind current state.
They are sorted, which is an order for reading and not a chronology. Health
reads `_.Log` for nothing.

`_.LoadStatistic` accumulates, so it is never read whole. The match is what
bounds it, rendered as a semi-join in the statement so the engine does the work:
one row per current loadable object at most, whatever the estate's age. There is
no row cap and no prefix, so `load_activity` is every statistic behind current
state. A semi-join rather than an inner one, because `_.LoadStatus` is keyed by
the object and the read stays one table's projection.

The window is acquired behind `read_installed_catalogue(..., load_history=True)`
and carried on the catalogue as `Catalogue.load_history`. It sits apart from
`rows`, because `Catalogue.table_rows` answers for a materialised table and a
window is not one.

```text
Warehouse
    │
    read_installed_catalogue(...)
    ├─ current catalogue state
    └─ the statistics behind it
    │
Catalogue
    ├─ dag()
    ├─ runtime status
    ├─ bookmarks
    └─ load_history
    │
  health
```

One operation reads the installed estate into a `Catalogue` and everything
reasons from that `Catalogue`. Health asks the Warehouse nothing further.

The runtime records orchestrated `weaver.load()` runs and standalone object
`.load()` calls under the same `load` task type, so the surface is phrased as
**latest load activity** and claims nothing about who started it.

The terminal shows a small subset, being the slowest loads and the rows that
moved. JSON retains the whole window.

---

## Resources

Each catalogue table is a round trip over TDS, so health materialises the ten it
consults and no others:

```text
Installation  Registry  TableDictionary  FolderDictionary  TestDictionary
Dependency    Shortcut  Bookmark         LoadStatus        TestStatus
```

The dictionaries describing an object's columns and keys are absent: nothing
health decides consults one. So are the history tables, read as one window
instead. See `weaver.operations.health.HEALTH_TABLES`.

Health executes no authored load or test Python. It takes no Environment, and:

```text
weaver health Warehouse/Reporting   → auth, resolver, tds
weaver health                       → auth, resolver, tds, onelake
```

A Warehouse answers over TDS and a Lakehouse over its storage, so no request
starts a Livy session.

---

## Where to look in the code

```text
weaver/health.py                the report model and the evaluator, pure
weaver/catalogue/history.py     the bounded window of _.Log and _.LoadStatistic
weaver/operations/health.py     the operation that reads the estate into a Catalogue
weaver_cli/main.py              the parser, the terminal renderer and --json
```
