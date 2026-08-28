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

## Bounded history

`_.Log` and `_.LoadStatistic` grow with the estate's age, so health reads one
window: the most recent load workflow, and the statistics that workflow's loads
appended.

The runtime records orchestrated `weaver.load()` runs and standalone object
`.load()` calls under the same `load` task type, so the surface is phrased as
**latest load activity** and claims nothing about who started it.

The terminal shows a small subset, being the slowest loads and the rows that
moved. JSON retains the whole window.

---

## Resources

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
weaver/catalogue/history.py     the bounded reads of _.Log and _.LoadStatistic
weaver/operations/health.py     the operation that gathers what the evaluator takes
weaver_cli/main.py              the parser, the terminal renderer and --json
```
